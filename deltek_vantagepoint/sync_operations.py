"""Per-table sync operations for the Deltek Vantagepoint connector.

This module handles:
1. Incremental cursor logic - per-table ModDate cursors, with an optional bounded first sync
2. The monthly full truncate-and-resync - trigger logic, per-table completion tracking, and the
   truncate-then-re-pull mechanics that let source deletions be reflected at the destination
3. Hub table sync - page-number pagination plus fan-out to detail/child tables and Billing
4. General/Configuration, Workflow, and Third-Party Integration table sync - full truncate +
   replace every sync, since these endpoints carry no audit columns and don't paginate
"""

# For URL-encoding parent record IDs used in child-collection paths
import urllib.parse

# For clamping the monthly full-resync trigger day to the current month's actual length
from calendar import monthrange

# For evaluating the current date against the monthly full-resync trigger day
from datetime import datetime, timezone

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Incremental-sync and monthly full-resync constants
from sync_limits import (
    __CURSOR_FIELD,
    __DEFAULT_CURSOR,
    __DEFAULT_FULL_RESYNC_DAY_OF_MONTH,
    __PAGE_SIZE,
    __STATE_MONTH_FORMAT,
)

# The detail/child fan-out configuration and the Billing invoice header table
from table_registry import __HUB_CHILDREN, __INVOICE_HEADER_TABLE

# The shared HTTP GET wrapper with retry and token refresh
from rest_client import get_api_response

# --------------------------------------------------------------------------------------
# Incremental cursor logic
# --------------------------------------------------------------------------------------


def get_initial_cursor(configuration):
    """
    Determine the starting ModDate cursor for a Hub table's very first sync (i.e. before any
    cursor has been checkpointed to state). Defaults to the epoch (full history) unless the
    optional "initial_sync_start_date" configuration value is set, in which case only records
    modified on or after that date are pulled on the first sync. This is the recommended way to
    run a small first test sync against a live instance instead of pulling full history - set it
    to a recent date (e.g. "2026-08-01T00:00:00") to bound both the Hubs data volume and, since
    child fan-out (see __HUB_CHILDREN in table_registry.py) only fires for parents returned by this
    sync, the number of child API calls made on that first run too.
    Only affects the FIRST sync of each table - once a real cursor is checkpointed to state, this
    value is no longer consulted; subsequent syncs continue incrementally from where they left off
    regardless of what this configuration value is set to.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        str: The starting cursor value in the same "%Y-%m-%dT%H:%M:%S"-style format Vantagepoint
            returns for ModDate.
    """
    return configuration.get("initial_sync_start_date") or __DEFAULT_CURSOR


# --------------------------------------------------------------------------------------
# Monthly full truncate-and-resync
# --------------------------------------------------------------------------------------


def get_full_resync_day_of_month(configuration):
    """
    Determine which day of the month triggers the monthly full resync (see
    should_run_full_resync()). Configurable via the optional "full_resync_day_of_month"
    configuration value (e.g. "15"); defaults to __DEFAULT_FULL_RESYNC_DAY_OF_MONTH if that value
    is blank or not a valid positive integer.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        int: The configured day of month (at least 1) - should_run_full_resync() further clamps
            this to the current month's actual length, since not every month has, say, 31 days.
    """
    raw_value = configuration.get("full_resync_day_of_month")
    try:
        day = int(raw_value)
    except (TypeError, ValueError):
        return __DEFAULT_FULL_RESYNC_DAY_OF_MONTH
    return day if day >= 1 else __DEFAULT_FULL_RESYNC_DAY_OF_MONTH


def should_run_full_resync(state, configuration):
    """
    Decide whether this sync should fully truncate and re-pull every Hub table (and its detail/
    Billing tables), so records deleted at the source - which incremental sync alone can never
    detect, since this API has no deletion feed - eventually disappear from the destination too.
    Anchored to the calendar, not a rolling interval: True on the very first sync ever (harmless -
    nothing exists yet to truncate), and again on the first sync that lands on or after the
    configured day of the month (see get_full_resync_day_of_month()), provided a full resync
    hasn't already completed that same calendar month. A sync that runs a few days late (or isn't
    scheduled daily) still catches up on the next sync at or after that day - it just runs once
    per month, not on a fixed rolling countdown that could drift.
    Args:
        state: a dictionary that holds the state of the connector.
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        bool: True if a full truncate-and-resync should run this sync.
    """
    last_full_resync_month = state.get("last_full_resync_month")
    if not last_full_resync_month:
        return True

    now = datetime.now(timezone.utc)
    days_in_current_month = monthrange(now.year, now.month)[1]
    trigger_day = min(get_full_resync_day_of_month(configuration), days_in_current_month)

    if now.day < trigger_day:
        return False

    return last_full_resync_month != now.strftime(__STATE_MONTH_FORMAT)


def truncate_table_and_details(hub_table_name):
    """
    Truncate a Hub table and every detail/Billing table fed by it, as the first step of a monthly
    full resync (see should_run_full_resync()). Detail tables normally only ever receive
    op.upsert() calls (see sync_children_for_record()), so without this, a record deleted at the
    source (e.g. a removed Employee Skill) would never be removed from the destination even once
    force_full re-pulls everything - truncating here, before the full re-pull begins, is what lets
    that re-pull's upserts stand in for a real deletion diff.
    Args:
        hub_table_name: The Hub table name (e.g. "employee", "firm", "project") about to be
            fully re-synced.
    """
    op.truncate(table=hub_table_name)

    hub_config = __HUB_CHILDREN.get(hub_table_name)
    if hub_config:
        for child in hub_config["children"]:
            op.truncate(table=child["table"])

    if hub_table_name == "project":
        op.truncate(table=__INVOICE_HEADER_TABLE["table"])


# --------------------------------------------------------------------------------------
# Hub table sync
# --------------------------------------------------------------------------------------


def sync_table(
    base_url, access_token, configuration, state, cursors, table_config, failed_tables, force_full
):
    """
    Sync a single Hubs table using page-number pagination and server-side incremental filtering
    on the ModDate audit column. For Hubs with child collections configured (see __HUB_CHILDREN in
    table_registry.py), also fans out to those children for every record returned by this sync -
    i.e. only parents that are new or changed since the last cursor, not the full parent roster
    every time.
    Args:
        base_url: The Vantagepoint API base URL.
        access_token: OAuth2 bearer access token.
        configuration: a dictionary that holds the configuration settings for the connector.
        state: the full connector state dictionary (checkpointed after every page).
        cursors: a dict mapping table name to its last-synced ModDate cursor value.
        table_config: a dict describing the table (table, endpoint, primary_key).
        failed_tables: a set of table names that have already failed with an unrecoverable error
            this sync - shared across all parent records so a permission gap on one child table is
            only attempted (and logged) once per sync, not once per parent record.
        force_full: if True, ignore any checkpointed cursor (and "initial_sync_start_date") and
            fully re-pull this table's entire history, after truncating it and its detail/Billing
            tables - see should_run_full_resync(). If False, sync incrementally as usual.
    Returns:
        str: The current (possibly-refreshed) access_token, to carry forward to the next table.
    """
    table_name = table_config["table"]
    endpoint = table_config["endpoint"]
    cursor = (
        __DEFAULT_CURSOR
        if force_full
        else cursors.get(table_name, get_initial_cursor(configuration))
    )

    if force_full:
        truncate_table_and_details(table_name)
        log.info(f"Truncated table '{table_name}' (and its detail/Billing tables) for full resync")

    hub_config = __HUB_CHILDREN.get(table_name)
    if hub_config:
        number_children_tables = len(hub_config["children"])
        child_tables = ", ".join(child_table["table"] for child_table in hub_config["children"])
    else:
        number_children_tables = 0
        child_tables = "none"

    url = f"{base_url}/{endpoint}"
    page = 1
    max_cursor_seen = cursor
    total_synced = 0

    log.info(f"Starting sync for table '{table_name}' from cursor: {cursor} with GET {url}")

    while True:
        params = {
            "page": page,
            "pageSize": __PAGE_SIZE,
            "order": __CURSOR_FIELD,
            "filterHash[0][name]": __CURSOR_FIELD,
            "filterHash[0][opp]": ">=",
            "filterHash[0][value]": cursor,
        }

        log.info(
            f"Processing table '{table_name}', page: {params['page']}, pageSize: {params['pageSize']}"
        )

        try:
            records, access_token = get_api_response(
                url, params, access_token, base_url, configuration
            )
        except RuntimeError as error:
            if force_full:
                raise RuntimeError(
                    f"Table '{table_name}' (and its detail/Billing tables) was truncated for the "
                    f"monthly full resync but could not be fully re-synced - its destination data "
                    f"may now be incomplete. Failing the sync rather than reporting it as "
                    f"successful. Underlying error: {error}"
                ) from error
            raise

        number_response_records = len(records)
        log.info(
            f"Table '{table_name}', page: {params['page']}, response has {number_response_records} records"
        )

        if not records:
            log.info(f"No more records for table '{table_name}' in page: {params['page']}")
            break

        log.info(
            f"Table '{table_name}', processing {number_children_tables} children: {child_tables} "
            f"for each of {number_response_records} records"
        )

        for record in records:
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table=table_name, data=record)
            record_cursor = record.get(__CURSOR_FIELD)
            if record_cursor and record_cursor > max_cursor_seen:
                max_cursor_seen = record_cursor

            access_token = sync_children_for_record(
                base_url,
                access_token,
                configuration,
                table_name,
                record,
                failed_tables,
                force_full,
            )

        total_synced += number_response_records
        log.info(f"Table '{table_name}', records synced: {total_synced}")

        cursors[table_name] = max_cursor_seen
        state["cursors"] = cursors

        # Save the progress by checkpointing the state. This is important for ensuring that the
        # sync process can resume from the correct position in case of next sync or
        # interruptions. You should checkpoint even if you are not using incremental sync, as it
        # tells Fivetran it is safe to write to destination. For large datasets, checkpoint
        # regularly (e.g., every N records) not only at the end. Learn more about how and where
        # to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state)

        if number_response_records < __PAGE_SIZE:
            break
        page += 1

    log.info(f"Completed sync for table '{table_name}'. Records synced: {total_synced}")
    return access_token


# --------------------------------------------------------------------------------------
# Detail/child collections and Billing
# --------------------------------------------------------------------------------------


def normalize_records(payload):
    """
    Normalize a Vantagepoint child-endpoint response into a list of record dicts. Some child
    endpoints return a bare list, but samples show at least one (e.g. Employee Skills, Firm
    Addresses) can return a single JSON object instead of a one-item array. Treat a dict as a
    single-record result and anything else unexpected as no records.
    Args:
        payload: The parsed JSON response body from a child endpoint.
    Returns:
        list: A list of record dicts (possibly empty).
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return [payload]
    return []


def sync_children_for_record(
    base_url, access_token, configuration, hub_name, record, failed_tables, force_full
):
    """
    Fan out to every configured child collection for a single parent Hub record (see __HUB_CHILDREN
    in table_registry.py), and to the Billing invoice header for "main" project records. Called
    once per parent record returned by sync_table(), so children are only fetched for parents that
    are new or changed in this sync - see the ASSUMPTION note on __HUB_CHILDREN about what this does
    and doesn't catch.
    A child table that fails with an unrecoverable error (e.g. a 403 - the service account isn't
    authorized for that specific sub-resource, confirmed live even when the parent Hub itself is
    accessible) is logged once, added to failed_tables, and skipped for every subsequent record
    this sync - one denied permission should not abort the whole sync, and shouldn't be retried
    (and re-logged) once per parent record either. EXCEPTION: during force_full, this child table
    was already truncated (see truncate_table_and_details()), so the same failure now means its
    destination data is incomplete, not just stale - this raises instead, failing the whole sync.
    Args:
        base_url: The Vantagepoint API base URL.
        access_token: OAuth2 bearer access token.
        configuration: a dictionary that holds the configuration settings for the connector.
        hub_name: The parent Hub table name (e.g. "employee", "firm", "project").
        record: The parent record just upserted.
        failed_tables: a set of table names that have already failed with an unrecoverable error
            this sync; mutated in place as new failures occur.
        force_full: True if this hub (and its children) were truncated for the monthly full
            resync this sync - see should_run_full_resync().
    Returns:
        str: The current (possibly-refreshed) access_token.
    """
    hub_config = __HUB_CHILDREN.get(hub_name)

    if hub_config:
        parent_id = hub_config["id_template"](record)

        if parent_id:
            parent_id_encoded = urllib.parse.quote(parent_id, safe="")

            for child in hub_config["children"]:
                child_table = child["table"]

                if child_table in failed_tables:
                    continue
                child_path = hub_config["url_template"].format(
                    parent_id=parent_id_encoded, path=child["path"]
                )
                url = f"{base_url}/{child_path}"

                try:
                    payload, access_token = get_api_response(
                        url, {}, access_token, base_url, configuration
                    )
                except RuntimeError as error:
                    error_msg = str(error)

                    # 400 = ASP.NET request validation (e.g., ampersand & character in path
                    # like: R&KDE200600000000490)
                    # 404 = ASP.NET routing failure (e.g., ID pattern ending in a period in
                    # path like: JR.)
                    # NOTE: left lenient here even during force_full - this is a one-off malformed-ID
                    # issue on a single parent record, not a systemic failure of the whole child
                    # table, so treating it as fatal would make the monthly resync fragile against
                    # known-odd IDs. It does mean this one parent's children stay incomplete after
                    # the truncate, which is a real (if narrow) accuracy gap - flagging as a judgment
                    # call rather than deciding it's fine unilaterally.
                    if "400" in error_msg or "404" in error_msg:
                        log.warning(
                            f"Skipping children for {hub_name} '{parent_id}' - "
                            f"API returned error: {error_msg}"
                        )
                        break  # Skip all remaining children for this parent, move to next parent
                    elif force_full:
                        raise RuntimeError(
                            f"Table '{child_table}' was truncated for the monthly full resync but "
                            f"failed while re-syncing - its destination data may now be incomplete. "
                            f"Failing the sync rather than reporting it as successful. Underlying "
                            f"error: {error}"
                        ) from error
                    else:
                        log.warning(
                            f"Table '{child_table}' failed with an unrecoverable error; skipping it "
                            f"for the rest of this sync: {error}"
                        )
                        failed_tables.add(child_table)
                        continue  # Skip this child table for ALL remaining parents

                for child_record in normalize_records(payload):
                    # The 'upsert' operation is used to insert or update data in the destination
                    # table.
                    # The first argument is the name of the destination table.
                    # The second argument is a dictionary containing the record to be upserted.
                    op.upsert(table=child_table, data=child_record)

    if hub_name == "project":
        access_token = sync_invoice_header_for_project(
            base_url, access_token, configuration, record, failed_tables, force_full
        )

    return access_token


def sync_invoice_header_for_project(
    base_url, access_token, configuration, project_record, failed_tables, force_full
):
    """
    Fetch Billing invoice header records for a single top-level project. Only runs for "main"
    project records (WBS2 and WBS3 blank), since the underlying endpoint is scoped by :WBS1 alone,
    not the full WBS1/WBS2/WBS3 key - phase/task rows within the same project would just repeat the
    same invoices.
    Args:
        base_url: The Vantagepoint API base URL.
        access_token: OAuth2 bearer access token.
        configuration: a dictionary that holds the configuration settings for the connector.
        project_record: The parent project record just upserted.
        failed_tables: a set of table names that have already failed with an unrecoverable error
            this sync; mutated in place as new failures occur.
        force_full: True if "project" (and its children) were truncated for the monthly full
            resync this sync - see should_run_full_resync().
    Returns:
        str: The current (possibly-refreshed) access_token.
    """
    if project_record.get("WBS2") or project_record.get("WBS3"):
        return access_token

    table_name = __INVOICE_HEADER_TABLE["table"]
    if table_name in failed_tables:
        return access_token

    wbs1 = project_record.get("WBS1")
    if not wbs1:
        return access_token

    wbs1_encoded = urllib.parse.quote(wbs1, safe="")
    url = f"{base_url}/InteractiveDetail/{wbs1_encoded}/{__INVOICE_HEADER_TABLE['path']}"

    try:
        payload, access_token = get_api_response(url, {}, access_token, base_url, configuration)
    except RuntimeError as error:
        if force_full:
            raise RuntimeError(
                f"Table '{table_name}' was truncated for the monthly full resync but failed while "
                f"re-syncing - its destination data may now be incomplete. Failing the sync rather "
                f"than reporting it as successful. Underlying error: {error}"
            ) from error
        log.warning(
            f"Table '{table_name}' failed with an unrecoverable error; skipping it for the rest "
            f"of this sync: {error}"
        )
        failed_tables.add(table_name)
        return access_token

    for record in normalize_records(payload):
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table=table_name, data=record)

    return access_token


# --------------------------------------------------------------------------------------
# General/Configuration, Workflow, and Third-Party Integration tables
# --------------------------------------------------------------------------------------


def sync_config_table(base_url, access_token, configuration, state, table_config):
    """
    Sync a single General/Configuration, Workflow, or Third-Party Integration reference table.
    These endpoints return their entire result set in one call (no filterHash/pagination support
    and no audit columns), so every sync fully replaces the destination table: truncate, then
    upsert every record returned.
    Args:
        base_url: The Vantagepoint API base URL.
        access_token: OAuth2 bearer access token.
        configuration: a dictionary that holds the configuration settings for the connector.
        state: the full connector state dictionary (checkpointed after this table completes).
        table_config: a dict describing the table (table, endpoint, primary_key).
    Returns:
        str: The current (possibly-refreshed) access_token, to carry forward to the next table.
    """
    table_name = table_config["table"]
    endpoint = table_config["endpoint"]

    url = f"{base_url}/{endpoint}"

    log.info(f"Starting full sync for config table '{table_name}' with GET {url}")

    records, access_token = get_api_response(url, {}, access_token, base_url, configuration)

    # Soft-delete all previously synced rows for this table before loading the fresh full set,
    # so records removed from Vantagepoint since the last sync don't linger in the destination.
    op.truncate(table=table_name)

    for record in records or []:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table=table_name, data=record)

    # Save the progress by checkpointing the state. This is important for ensuring that the
    # sync process can resume from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it
    # is safe to write to destination. For large datasets, checkpoint regularly (e.g., every N
    # records) not only at the end. Learn more about how and where to checkpoint by reading our
    # best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state)

    log.info(f"Completed sync for table '{table_name}'. Records synced: {len(records or [])}")
    return access_token
