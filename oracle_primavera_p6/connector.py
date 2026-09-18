"""Fivetran Connector SDK connector for the Oracle Primavera P6 EPPM 'Data Service' REST API.

Authenticates with HTTP Basic Auth, discovers tables/columns via the metadata endpoints,
and pulls data one table at a time via the `runquery` endpoint (SYNC mode), paginating with
nextKey/nextTableName and using a per-table `sinceDate` cursor for tables that expose an
update-timestamp column.
"""

import base64
import re
import time
from datetime import datetime, timezone

import requests

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# --------------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------------

__DEFAULT_CONFIG_CODE = "ds_p6adminuser"
__VALID_CONFIG_CODES = {"ds_p6adminuser", "ds_p6reportuser", "ds_unifier"}

__PAGE_SIZE = "5000"
__SYNC_MODE = "SYNC"

# dataType/physicalDataType values that runquery's SYNC mode cannot return.
__LOB_TYPES = {"BLOB", "CLOB", "NCLOB", "LONG RAW"}

# Column names (case/underscore-insensitive) that indicate a table can be synced incrementally.
__INCREMENTAL_CURSOR_COLUMNS = {"updatedate", "lastupdatedate", "changedate"}

__TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S +00:00"

__MAX_ATTEMPTS = 5
__DEFAULT_RETRY_AFTER_SECONDS = 60
__REQUEST_TIMEOUT_SECONDS = 180
__CHECKPOINT_EVERY_PAGES = 1


class FatalAuthError(Exception):
    """Raised when credentials/config_code are rejected (401/403). Should abort the whole sync."""


# --------------------------------------------------------------------------------------
# Configuration helpers
# --------------------------------------------------------------------------------------


def validate_configuration(configuration: dict):
    """Validate required configuration keys and fail fast on an invalid config_code.

    Args:
        configuration: dictionary of configuration values provided by the user.

    Raises:
        ValueError: if a required field is missing or config_code is not a recognized value.
    """
    for key in ("username", "password", "base_url"):
        if not configuration.get(key):
            raise ValueError(f"Missing required configuration value: '{key}'")

    config_code = configuration.get("config_code") or __DEFAULT_CONFIG_CODE
    if config_code not in __VALID_CONFIG_CODES:
        raise ValueError(
            f"Invalid 'config_code' value: '{config_code}'. Must be one of: "
            f"{', '.join(sorted(__VALID_CONFIG_CODES))}"
        )


def get_base_url(configuration: dict) -> str:
    """Return the configured base URL, normalizing a trailing slash."""
    base_url = configuration["base_url"]
    if not base_url.endswith("/"):
        base_url += "/"
    return base_url


def get_config_code(configuration: dict) -> str:
    """Return the configured config_code, defaulting to ds_p6adminuser."""
    return configuration.get("config_code") or __DEFAULT_CONFIG_CODE


def get_configured_table_filter(configuration: dict) -> list:
    """Return the lowercase list of table names the user restricted syncing to (may be empty)."""
    raw = configuration.get("tables") or ""
    return [name.strip().lower() for name in raw.split(",") if name.strip()]


def get_configured_incremental_tables(configuration: dict):
    """Return the lowercase set of table names explicitly designated incremental, or None if unset.

    When the `incremental_tables` configuration value is set (even to an empty string after
    trimming individual entries), it takes full precedence over the column-based
    auto-detection heuristic: any in-scope table listed here syncs incrementally, and every
    other in-scope table is fully resynced every run, regardless of what columns it has.
    Returns None when the configuration key itself is absent, meaning "fall back to
    auto-detection" (used for discovery runs where the table scope isn't finalized yet).
    """
    if "incremental_tables" not in configuration:
        return None
    raw = configuration.get("incremental_tables") or ""
    return {name.strip().lower() for name in raw.split(",") if name.strip()}


def build_headers(configuration: dict) -> dict:
    """Build the HTTP Basic Auth + JSON headers required by every dataservice request."""
    username = configuration.get("username", "")
    password = configuration.get("password", "")
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("utf-8")
    return {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


# --------------------------------------------------------------------------------------
# Name sanitization
# --------------------------------------------------------------------------------------


def sanitize_name(name: str) -> str:
    """Normalize a P6 table/column name (which may contain spaces/mixed case) to
    lowercase_snake_case for use as a Fivetran destination table/column name.
    """
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", (name or "").strip()).strip("_").lower()
    normalized = re.sub(r"_+", "_", normalized)
    if not normalized:
        normalized = "col"
    if normalized[0].isdigit():
        normalized = f"_{normalized}"
    return normalized


# --------------------------------------------------------------------------------------
# HTTP request handling with retry/error classification
# --------------------------------------------------------------------------------------


def _backoff_sleep(attempt: int):
    """Exponential backoff sleep between retry attempts."""
    time.sleep(min(60, 2**attempt))


def request_with_retries(
    method: str, url: str, headers: dict, params: dict = None, json_body: dict = None
):
    """Execute an HTTP request, retrying transient failures and failing fast on permanent ones.

    Retries (up to __MAX_ATTEMPTS, exponential backoff): connection errors, timeouts,
    chunked-encoding errors, HTTP 429 (honors Retry-After), HTTP 500, HTTP 503.

    Fails fast (raises immediately, no retry): HTTP 400/404/405/406/415 (raise RuntimeError)
    and HTTP 401/403 (raise FatalAuthError, which callers must propagate to abort the run).
    """
    last_exception = None
    for attempt in range(1, __MAX_ATTEMPTS + 1):
        try:
            response = requests.request(
                method,
                url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=__REQUEST_TIMEOUT_SECONDS,
            )
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ) as exc:
            last_exception = exc
            log.warning(
                f"Transient network error on attempt {attempt}/{__MAX_ATTEMPTS} calling {url}: {exc}"
            )
            if attempt >= __MAX_ATTEMPTS:
                raise RuntimeError(
                    f"Request to {url} failed after {__MAX_ATTEMPTS} attempts: {exc}"
                ) from exc
            _backoff_sleep(attempt)
            continue

        status = response.status_code

        if status in (200, 201):
            return response

        if status in (401, 403):
            log.severe(
                f"Authentication/authorization failure (HTTP {status}) calling {url}: {response.text}"
            )
            raise FatalAuthError(
                f"HTTP {status} calling {url}. Check the username/password and that the account has "
                f"the Data Service role, and that config_code is correct."
            )

        if status in (400, 404, 405, 406, 415):
            hint = ""
            if status == 400:
                hint = (
                    " Hint: this usually means a bad table/column name or a malformed request body. "
                    "Check the 'tables' configuration value, or consider a manual P6 metadata refresh."
                )
            log.severe(f"Non-retryable HTTP {status} calling {url}: {response.text}.{hint}")
            raise RuntimeError(f"HTTP {status} calling {url}: {response.text}.{hint}")

        if status == 429:
            retry_after_header = response.headers.get("Retry-After")
            wait_seconds = __DEFAULT_RETRY_AFTER_SECONDS
            if retry_after_header and retry_after_header.strip().isdigit():
                wait_seconds = int(retry_after_header.strip())
            log.warning(
                f"Rate limited (HTTP 429) calling {url} on attempt {attempt}/{__MAX_ATTEMPTS}; "
                f"waiting {wait_seconds}s before retrying"
            )
            if attempt >= __MAX_ATTEMPTS:
                raise RuntimeError(
                    f"HTTP 429 calling {url} after {__MAX_ATTEMPTS} attempts: {response.text}"
                )
            time.sleep(wait_seconds)
            continue

        if status in (500, 503):
            log.warning(
                f"Server error (HTTP {status}) calling {url} on attempt {attempt}/{__MAX_ATTEMPTS}: {response.text}"
            )
            if attempt >= __MAX_ATTEMPTS:
                raise RuntimeError(
                    f"HTTP {status} calling {url} after {__MAX_ATTEMPTS} attempts: {response.text}"
                )
            _backoff_sleep(attempt)
            continue

        # Any other unexpected status code: treat as a fail-fast / code-path bug.
        log.severe(f"Unexpected HTTP {status} calling {url}: {response.text}")
        raise RuntimeError(f"Unexpected HTTP {status} calling {url}: {response.text}")

    if last_exception:
        raise RuntimeError(
            f"Request to {url} failed after {__MAX_ATTEMPTS} attempts: {last_exception}"
        )
    raise RuntimeError(f"Request to {url} failed after {__MAX_ATTEMPTS} attempts")


# --------------------------------------------------------------------------------------
# Metadata endpoints
# --------------------------------------------------------------------------------------


def fetch_tables_metadata(configuration: dict) -> list:
    """Call GET metadata/tables and return the raw list of table metadata dicts."""
    url = f"{get_base_url(configuration)}metadata/tables"
    params = {"configCode": get_config_code(configuration)}
    response = request_with_retries("GET", url, build_headers(configuration), params=params)
    data = response.json()
    return data if isinstance(data, list) else []


def fetch_columns_metadata(configuration: dict, table_name: str) -> list:
    """Call GET metadata/columns/{tableName} and return the raw list of column metadata dicts."""
    url = f"{get_base_url(configuration)}metadata/columns/{table_name}"
    params = {"configCode": get_config_code(configuration)}
    response = request_with_retries("GET", url, build_headers(configuration), params=params)
    data = response.json()
    return data if isinstance(data, list) else []


def _is_blacklisted(table_metadata: dict) -> bool:
    """Parse isBlackListed defensively: P6 returns it as the STRING "true"/"false"."""
    value = table_metadata.get("isBlackListed")
    return str(value).strip().lower() == "true"


def _is_lob_column(column_metadata: dict) -> bool:
    """Return True if a column's dataType/physicalDataType is a LOB type unsupported by SYNC mode."""
    data_type = str(column_metadata.get("dataType") or "").strip().upper()
    physical_type = str(column_metadata.get("physicalDataType") or "").strip().upper()
    return data_type in __LOB_TYPES or physical_type in __LOB_TYPES


def _is_incremental_capable(columns_metadata: list) -> bool:
    """A table is incremental-capable if any column name (normalized) matches a known cursor column."""
    for column in columns_metadata:
        normalized = str(column.get("columnName") or "").strip().lower().replace("_", "")
        if normalized in __INCREMENTAL_CURSOR_COLUMNS:
            return True
    return False


def resolve_sync_type(
    configuration: dict, physical_table_name: str, columns_metadata: list
) -> bool:
    """Return True if this table should sync incrementally.

    If `incremental_tables` is configured, it fully overrides auto-detection: membership in
    that list (case-insensitive) determines incremental vs. full-resync for every in-scope
    table. Otherwise, falls back to column-based auto-detection (`_is_incremental_capable`).
    """
    configured_incremental = get_configured_incremental_tables(configuration)
    if configured_incremental is not None:
        return physical_table_name.strip().lower() in configured_incremental
    return _is_incremental_capable(columns_metadata)


def get_in_scope_tables(configuration: dict, tables_metadata: list) -> list:
    """Filter table metadata to non-blacklisted tables, intersected with the configured `tables` CSV.

    Matching against the configured CSV is case-insensitive on physicalTableName, falling back to
    displayTableName. Configured names that match nothing are logged and skipped rather than
    failing the whole run.
    """
    configured = get_configured_table_filter(configuration)
    matched = set()
    in_scope = []

    for table_metadata in tables_metadata:
        if _is_blacklisted(table_metadata):
            continue

        physical_name = str(table_metadata.get("physicalTableName") or "").strip()
        display_name = str(table_metadata.get("displayTableName") or "").strip()

        if configured:
            physical_key = physical_name.lower()
            display_key = display_name.lower()
            if physical_key in configured:
                matched.add(physical_key)
            elif display_key in configured:
                matched.add(display_key)
            else:
                continue

        if not physical_name and not display_name:
            continue

        in_scope.append(table_metadata)

    if configured:
        unmatched = [name for name in configured if name not in matched]
        for name in unmatched:
            log.warning(
                f"Configured table '{name}' did not match any known non-blacklisted P6 table; skipping it"
            )

    return in_scope


# --------------------------------------------------------------------------------------
# runquery pagination parsing
# --------------------------------------------------------------------------------------


def _next_table_is_falsy(value) -> bool:
    """True if nextTableName should be treated as "no more pages"."""
    if value is None:
        return True
    if isinstance(value, str):
        stripped = value.strip()
        return stripped == "" or stripped == "-1"
    if isinstance(value, (int, float)):
        return value == -1
    return False


def _next_key_is_zero(value) -> bool:
    """True if nextKey should be treated as the "0"/0 sentinel."""
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == "0"
    if isinstance(value, (int, float)):
        return value == 0
    return False


def parse_pagination(payload: dict, physical_table_name: str):
    """Defensively extract (next_key, next_table_name, has_more) from a runquery response.

    Lookup order: response["data"]["pagination"] (list -> match this table, else the sole entry)
    -> response["data"]["nextTableName"]/["nextKey"] -> top-level response["nextTableName"]/["nextKey"].

    Pagination stops when no pagination info is found at all, OR nextTableName is falsy/"-1"/-1,
    OR nextKey is "0"/0 alongside an absent/-1 nextTableName.
    """
    next_key = None
    next_table_name = None
    found = False

    data = payload.get("data") if isinstance(payload, dict) else None

    if isinstance(data, dict):
        pagination = data.get("pagination")
        if isinstance(pagination, list) and pagination:
            entry = None
            for item in pagination:
                table_name = (
                    str(item.get("tableName", "")).lower() if isinstance(item, dict) else None
                )
                if table_name == physical_table_name.lower():
                    entry = item
                    break
            if entry is None:
                entry = pagination[0]
            if isinstance(entry, dict):
                next_key = entry.get("nextKey")
                next_table_name = entry.get("nextTableName")
                found = True
        elif isinstance(pagination, dict):
            next_key = pagination.get("nextKey")
            next_table_name = pagination.get("nextTableName")
            found = True

        if not found and ("nextTableName" in data or "nextKey" in data):
            next_key = data.get("nextKey")
            next_table_name = data.get("nextTableName")
            found = True

    payload_has_pagination_keys = isinstance(payload, dict) and (
        "nextTableName" in payload or "nextKey" in payload
    )
    if not found and payload_has_pagination_keys:
        next_key = payload.get("nextKey")
        next_table_name = payload.get("nextTableName")
        found = True

    if not found:
        return None, None, False

    if _next_table_is_falsy(next_table_name):
        return None, None, False

    if _next_key_is_zero(next_key) and _next_table_is_falsy(next_table_name):
        return None, None, False

    return next_key, next_table_name, True


def sync_table(
    configuration: dict,
    physical_table_name: str,
    destination_table: str,
    columns: list,
    since_date: str,
    state: dict,
) -> int:
    """Page through runquery for a single table and upsert every row. Returns the row count.

    Checkpoints periodically (every __CHECKPOINT_EVERY_PAGES pages) using the unmodified `state`
    passed in, so large tables get flushed to the destination incrementally instead of
    accumulating an unbounded backlog of unflushed upserts across many tables in one run.
    This does not advance this table's own incremental cursor (that only happens once the
    whole table finishes, in update()) — it only bounds how much unflushed data can build up.
    """
    base_url = get_base_url(configuration)
    headers = build_headers(configuration)
    url = f"{base_url}runquery"
    params = {"configCode": get_config_code(configuration)}

    next_key = None
    next_table_name = None
    is_first_page = True
    total_rows = 0
    page_count = 0

    while True:
        body = {
            "name": f"fivetran_sync_{physical_table_name}",
            "mode": __SYNC_MODE,
            "pageSize": __PAGE_SIZE,
            "sqlQueriesAndTotalRecordCount": False,
            "originalDateFormat": True,
            "tables": [{"tableName": physical_table_name, "columns": columns}],
        }
        if since_date:
            body["sinceDate"] = since_date
        if not is_first_page:
            if next_key is not None:
                body["nextKey"] = str(next_key)
            if next_table_name is not None:
                body["nextTableName"] = str(next_table_name)

        response = request_with_retries("POST", url, headers, params=params, json_body=body)
        payload = response.json()

        data = payload.get("data") if isinstance(payload, dict) else None
        rows = data.get(physical_table_name) if isinstance(data, dict) else None
        if not isinstance(rows, list):
            rows = []

        for row in rows:
            if isinstance(row, dict):
                op.upsert(
                    destination_table,
                    {sanitize_name(key): value for key, value in row.items()},
                )
                total_rows += 1

        next_key, next_table_name, has_more = parse_pagination(payload, physical_table_name)
        is_first_page = False
        page_count += 1

        if not has_more:
            break

        if page_count % __CHECKPOINT_EVERY_PAGES == 0:
            op.checkpoint(state=state)

    return total_rows


# --------------------------------------------------------------------------------------
# Fivetran Connector SDK entry points
# --------------------------------------------------------------------------------------


def schema(configuration: dict):
    """Define the destination schema by discovering in-scope tables and their columns.

    Independently calls metadata/tables and metadata/columns/{tableName} (no reliance on
    module-level state persisting between schema() and update()). Excludes LOB-typed columns.
    Declares only `table` and `primary_key` (when a table has PK columns) so the SDK can infer
    column types and the schema can evolve.
    """
    validate_configuration(configuration)

    tables_metadata = fetch_tables_metadata(configuration)
    in_scope_tables = get_in_scope_tables(configuration, tables_metadata)

    schema_list = []
    for table_metadata in sorted(
        in_scope_tables,
        key=lambda t: (t.get("physicalTableName") or t.get("displayTableName") or "").lower(),
    ):
        physical_name = table_metadata.get("physicalTableName") or table_metadata.get(
            "displayTableName"
        )
        if not physical_name:
            continue

        try:
            columns_metadata = fetch_columns_metadata(configuration, physical_name)
        except FatalAuthError:
            raise
        except Exception as exc:
            log.warning(f"Skipping table '{physical_name}' during schema discovery: {exc}")
            continue

        destination_table = sanitize_name(physical_name)
        primary_key_columns = []
        logged_lob_columns = set()

        for column_metadata in columns_metadata:
            column_name = str(column_metadata.get("columnName") or "")
            if _is_lob_column(column_metadata):
                if column_name not in logged_lob_columns:
                    log.info(
                        f"Excluding LOB column '{column_name}' from table '{physical_name}' "
                        f"(SYNC mode does not support LOB types)"
                    )
                    logged_lob_columns.add(column_name)
                continue
            if column_metadata.get("isPK") is True:
                primary_key_columns.append(sanitize_name(column_name))

        incremental = resolve_sync_type(configuration, physical_name, columns_metadata)
        log.info(
            f"Table '{physical_name}' classified as "
            f"{'incremental' if incremental else 'full-resync-only'}"
        )

        entry = {"table": destination_table}
        if primary_key_columns:
            entry["primary_key"] = primary_key_columns
        schema_list.append(entry)

    return schema_list


def update(configuration: dict, state: dict):
    """Sync every in-scope P6 table, one runquery call per table, one table at a time.

    For incremental-capable tables (those with an UPDATE_DATE/LASTUPDATEDATE/CHANGEDATE/UPDATEDATE
    column), a `sinceDate` cursor is stored in state and only advanced after that table's entire
    pagination loop finishes successfully. Fully resynced tables never get a stored cursor.
    An error on one table's metadata/columns or runquery call is logged and that table is skipped,
    except for auth failures or an invalid config_code, which abort the whole run.
    """
    validate_configuration(configuration)

    state = state or {}
    state.setdefault("tables", {})

    tables_metadata = fetch_tables_metadata(configuration)
    in_scope_tables = get_in_scope_tables(configuration, tables_metadata)
    in_scope_tables_sorted = sorted(
        in_scope_tables,
        key=lambda t: (t.get("physicalTableName") or t.get("displayTableName") or "").lower(),
    )

    for table_metadata in in_scope_tables_sorted:
        physical_name = table_metadata.get("physicalTableName") or table_metadata.get(
            "displayTableName"
        )
        if not physical_name:
            continue

        destination_table = sanitize_name(physical_name)

        try:
            columns_metadata = fetch_columns_metadata(configuration, physical_name)
        except FatalAuthError:
            raise
        except Exception as exc:
            log.warning(
                f"Skipping table '{physical_name}': failed to fetch column metadata: {exc}"
            )
            continue

        sync_columns = [
            str(column_metadata.get("columnName"))
            for column_metadata in columns_metadata
            if column_metadata.get("columnName") and not _is_lob_column(column_metadata)
        ]
        if not sync_columns:
            log.warning(f"Table '{physical_name}' has no syncable (non-LOB) columns; skipping")
            continue

        incremental = resolve_sync_type(configuration, physical_name, columns_metadata)
        sync_start_ts = datetime.now(timezone.utc).strftime(__TIMESTAMP_FORMAT)
        since_date = (
            state["tables"].get(destination_table, {}).get("last_sync_at") if incremental else None
        )

        try:
            row_count = sync_table(
                configuration=configuration,
                physical_table_name=physical_name,
                destination_table=destination_table,
                columns=sync_columns,
                since_date=since_date,
                state=state,
            )
        except FatalAuthError:
            raise
        except Exception as exc:
            # Flush whatever rows this table already emitted before moving on. Without this,
            # a mid-table failure leaves an unbounded partial batch buffered until some later
            # table's checkpoint tries to commit it, which surfaces as a confusing
            # "failed to upsert" against the *failed* table long after it was skipped.
            log.warning(
                f"Error syncing table '{physical_name}': {exc}. Skipping table and continuing."
            )
            op.checkpoint(state=state)
            continue

        log.info(
            f"Synced {row_count} row(s) for table '{physical_name}' -> destination table '{destination_table}'"
        )

        if incremental:
            state["tables"][destination_table] = {"last_sync_at": sync_start_ts}

        # Checkpoint after every table (incremental or not) so upserts are flushed to the
        # destination in bounded batches rather than accumulating across many tables in one run.
        op.checkpoint(state=state)


# Global connector object required by the Fivetran Connector SDK.
connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    connector.debug()
