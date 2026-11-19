"""
This connector replicates large BigQuery tables with the Connector SDK. The tables to sync are
listed in TABLE_SPECS (table_specs.py). It reads their partitions, primary keys and sizes
automatically, reads data with the BigQuery Storage Read API, and uses table partitions to make both
the initial historical sync and incremental syncs efficient.
See the Technical Reference documentation (https://fivetran.com/docs/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connector-sdk#connectordevelopmentbestpractices) for details
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector, ConfigurationForm, Test, form_field

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

from concurrent.futures import ThreadPoolExecutor, as_completed  # For parallel execution
from bigquery_client import (
    begin_sync,
    check_connection,
    get_context,
    get_table_plans,
    report_skipped,
    save_final_state,
    sync_table,
)  # BigQuery client utilities
from progress import RunProgress  # Progress logging
from table_specs import MAX_PARALLEL_TABLES  # Number of tables synced in parallel


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the schema and update methods.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or empty.
    """
    required_configs = ["service_account_key", "project_id"]
    missing = [key for key in required_configs if not (configuration.get(key) or "").strip()]
    if missing:
        raise ValueError(f"Missing required configuration value(s): {', '.join(missing)}")


def configuration_form():
    """
    Define the setup form that collects the connection settings in the Fivetran dashboard.
    Values entered here are stored securely and passed to schema() and update() in the
    configuration dictionary. `fivetran configuration` uses the same form to create a local
    configuration.json for testing.
    """
    form = ConfigurationForm()
    form.add_field(
        form_field.TextField(
            name="service_account_key",
            label="Service account key",
            field_type=form_field.TextField.password,
            description="The full JSON key of a Google service account that can read the tables.",
            required=True,
        )
    )
    form.add_field(
        form_field.TextField(
            name="project_id",
            label="Project",
            description=(
                "The Google Cloud project that the service account belongs to. It must match the "
                "project in the key. Reads and queries are billed to this project. The tables to "
                "sync are listed in table_specs.py."
            ),
            required=True,
        )
    )
    form.add_test(label="Test connection", func=connection_test)
    return form


def connection_test(configuration: dict):
    """
    Check the settings entered in the setup form, including that the project matches the key. Check
    the form of every entry in TABLE_SPECS and that the first listed table can be read. That every
    table exists, is a plain table and has a primary key is checked when the schema is read and
    when a sync starts. Failure messages never contain the service account key.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    test = Test()
    try:
        validate_configuration(configuration=configuration)
        check_connection(configuration=configuration)
    except (ValueError, RuntimeError) as error:
        return test.failure(str(error))
    return test.success()


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    validate_configuration(configuration=configuration)

    # The tables are the ones listed in TABLE_SPECS, with their metadata read from BigQuery
    schema_list = []
    for plan in get_table_plans(configuration=configuration):
        # Each BigQuery dataset becomes a schema in the destination
        # Every synced table has a primary key, see _require_primary_keys
        table = {"schema": plan.dataset, "table": plan.stream, "primary_key": plan.primary_keys}
        if plan.explicit_columns:
            # Only columns whose type cannot be inferred from the value are declared
            table["columns"] = plan.explicit_columns
        schema_list.append(table)
    return schema_list


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-methods#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: Connectors : BigQuery")

    # Relative filter values such as today-90d are resolved against the start of this sync
    begin_sync()

    # Fail fast on configuration problems before any BigQuery call
    validate_configuration(configuration=configuration)

    plans = get_table_plans(configuration=configuration)
    ctx = get_context(configuration=configuration)
    workers = min(MAX_PARALLEL_TABLES, len(plans))
    run = RunProgress(len(plans))
    log.info(f"Sync started: {len(plans)} table(s), {workers} at a time")

    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(sync_table, ctx, plan, state, run) for plan in plans]
            try:
                for future in as_completed(futures):
                    # result() re-raises any error from a worker so the sync fails instead of
                    # skipping a table
                    future.result()
            except BaseException:
                # Do not start the tables that are still queued after one has failed
                for future in futures:
                    future.cancel()
                raise
    finally:
        # Save the state of the tables that finished, including those whose own checkpoint was
        # skipped, and tell the user on the dashboard about any data that was skipped or read in a
        # degraded way. This also happens when a table failed, so the warnings are not lost. A
        # table's state only moves forward when the table finished, so this cannot skip data.
        save_final_state(state)
        report_skipped()
    log.info(run.summary())


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema, configuration_form=configuration_form)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
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
