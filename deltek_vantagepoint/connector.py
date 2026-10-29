"""This connector syncs core Hubs master data (Employees, Firms, Contacts, Projects, Activities,
Marketing Campaigns, and Boilerplates) plus General/Configuration, Workflow, and Third-Party
Integration reference data from the Deltek Vantagepoint REST API to a Fivetran destination.
See the Technical Reference documentation
(https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation
(https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For formatting the monthly full-resync checkpoint into state
from datetime import datetime, timezone

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# The calendar-month format used for the monthly full-resync checkpoint
from sync_limits import __STATE_MONTH_FORMAT

# The Hubs and General/Configuration table/endpoint registries
from table_registry import __CONFIG_TABLES, __TABLES

# Configuration validation and OAuth2 token acquisition
from authentication import validate_configuration, get_access_token

# The schema() entry point
from schema_builder import schema

# The per-table sync logic, including the monthly full resync
from sync_operations import should_run_full_resync, sync_config_table, sync_table


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: Source Examples : Deltek Vantagepoint")

    # Validate the configuration to ensure all required fields are present
    validate_configuration(configuration)

    # "base_url" is the tenant root (e.g. "https://<firm>.deltekfirst.com/<Firm>") - every actual
    # API call, including the token endpoint, lives under an additional "/api" path segment on top
    # of that root. Confirmed against a live instance's working setup, not just public API docs.
    base_url = configuration.get("base_url").rstrip("/") + "/api"

    # Obtain an OAuth2 access token via the password grant. Vantagepoint access tokens are valid
    # for 12 hours (per the API's token response), comfortably longer than a single sync.
    access_token = get_access_token(base_url, configuration)

    cursors = state.get("cursors", {})
    log.info(f"Starting sync with these cursors: {cursors}")

    # Tracks tables that have already failed with an unrecoverable error (e.g. a 403 permission
    # denial) THIS sync, so a permission gap on one resource is logged once and then skipped for
    # every remaining parent record, instead of being re-attempted - and re-logged - per record.
    # Confirmed live: a service account can be authorized for a Hub's base record but denied on a
    # specific child collection (e.g. Employee Skills), so this has to be tracked per table, not
    # assumed to apply to an entire Hub.
    failed_tables = set()

    force_full = should_run_full_resync(state, configuration)

    # Tables that have already completed a full truncate-and-resync THIS monthly cycle. Persisted
    # to state and checked per table (not just a single all-or-nothing flag) so that if the cycle
    # fails partway through - see the raise below - the next sync only retries the tables that
    # didn't finish, instead of re-truncating and re-pulling full history for ones that already
    # succeeded. Cleared once every table has completed (see the end of this function).
    full_resync_completed_tables = set(state.get("full_resync_completed_tables", []))

    if force_full:
        remaining = [
            t["table"] for t in __TABLES if t["table"] not in full_resync_completed_tables
        ]
        log.info(
            f"Running the monthly full truncate-and-resync (last one: "
            f"{state.get('last_full_resync_month', 'never')}) - tables still needing a full "
            f"truncate-and-resync this cycle: {remaining or '(none - all already completed)'}"
        )

    for table_config in __TABLES:
        table_name = table_config["table"]
        # Only truncate-and-fully-resync this specific table if the cycle is running AND this
        # table hasn't already completed it - a table that finished on an earlier, since-retried
        # attempt this cycle is synced normally/incrementally instead of being redone from scratch.
        table_needs_full_resync = force_full and table_name not in full_resync_completed_tables

        if table_needs_full_resync:
            # Do NOT catch errors here. This table (and its detail/Billing tables - see
            # truncate_table_and_details()) was already truncated for the monthly resync, so a
            # failure partway through leaves its destination data actively wrong (some still-valid
            # records now look deleted), not just stale. Letting this propagate fails the whole
            # sync loudly instead of silently reporting incomplete data as a success, and leaves
            # this table out of full_resync_completed_tables so the next sync retries it (and only
            # it, plus any other table that also hasn't completed yet).
            access_token = sync_table(
                base_url,
                access_token,
                configuration,
                state,
                cursors,
                table_config,
                failed_tables,
                table_needs_full_resync,
            )
            full_resync_completed_tables.add(table_name)
            state["full_resync_completed_tables"] = sorted(full_resync_completed_tables)

            # Save the progress by checkpointing the state. This is important for ensuring that
            # the sync process can resume from the correct position in case of next sync or
            # interruptions. You should checkpoint even if you are not using incremental sync, as
            # it tells Fivetran it is safe to write to destination. For large datasets, checkpoint
            # regularly (e.g., every N records) not only at the end. Learn more about how and
            # where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(state)
        else:
            try:
                access_token = sync_table(
                    base_url,
                    access_token,
                    configuration,
                    state,
                    cursors,
                    table_config,
                    failed_tables,
                    table_needs_full_resync,
                )
            except RuntimeError as error:
                log.error(
                    f"Skipping table '{table_config['table']}' after an unrecoverable error: {error}"
                )
                failed_tables.add(table_config["table"])

    for table_config in __CONFIG_TABLES:
        try:
            access_token = sync_config_table(
                base_url, access_token, configuration, state, table_config
            )
        except RuntimeError as error:
            log.error(
                f"Skipping table '{table_config['table']}' after an unrecoverable error: {error}"
            )
            failed_tables.add(table_config["table"])

    if failed_tables:
        log.warning(
            f"Sync completed with {len(failed_tables)} table(s) skipped due to unrecoverable "
            f"errors (likely permission gaps - check the service account's role): "
            f"{sorted(failed_tables)}"
        )

    if force_full:
        # Only reached if every table that still needed a full truncate-and-resync this cycle
        # (whether completed just now or on an earlier, since-retried attempt) finished without
        # raising - see the comment in the __TABLES loop above. Marking completion here (not
        # unconditionally) is what guarantees a partially-failed resync gets retried next sync
        # instead of being treated as done for this calendar month. Clearing the per-table tracking
        # here (rather than leaving stale entries around) is what lets next month's cycle start
        # clean instead of thinking every table is already done.
        state["last_full_resync_month"] = datetime.now(timezone.utc).strftime(__STATE_MONTH_FORMAT)
        state["full_resync_completed_tables"] = []

        # Save the progress by checkpointing the state. This is important for ensuring that the
        # sync process can resume from the correct position in case of next sync or
        # interruptions. You should checkpoint even if you are not using incremental sync, as it
        # tells Fivetran it is safe to write to destination. For large datasets, checkpoint
        # regularly (e.g., every N records) not only at the end. Learn more about how and where
        # to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state)

    log.info("Deltek Vantagepoint sync completed for all Hubs and configuration/reference tables")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the
# command line or IDE 'run' button.
#
# IMPORTANT: The recommended way to test your connector is using the Fivetran debug command:
#   fivetran debug
#
# This local testing block is provided as a convenience for quick debugging during development,
# such as using IDE debug tools (breakpoints, step-through debugging, etc.).
# Note: This method is not called by Fivetran when executing your connector in production.
# Always test using 'fivetran debug' prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
