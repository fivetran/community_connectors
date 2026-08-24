"""AVEVA PI JDBC connectivity check.

Scope: this connector validates ONLY that a JRE plus AVEVA's PI SQL Client JDBC
driver can be installed and authenticated against, from inside the Hosted
Connector SDK's Linux container. It does not sync any PI data (elements,
attributes, event frames, recorded values, etc.) — see the REST-based `aveva_pi`
connector in this repo for a full data sync implementation over PI Web API.

This exists to de-risk the JDBC connectivity path (JRE install via
drivers/installation.sh, driver loading, non-SSPI authentication) before
building a full JDBC-based table sync on top of it.

See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For recording when the connectivity check ran
from datetime import datetime, timezone

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling logs in the connector
from fivetran_connector_sdk import Logging as log

# For supporting data operations like upsert() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Local module: JDBC connection setup and driver metadata lookup
from client import connect, get_driver_and_server_info


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the
    connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or blank.
    """
    required = ("das_host", "af_server", "af_database", "username", "password")
    for key in required:
        val = configuration.get(key, "")
        if not val:
            raise ValueError(f"Missing or empty required configuration key: '{key}'")
        if val.startswith("<"):
            raise ValueError(
                f"Required configuration key '{key}' still contains a placeholder value. "
                "Replace it with a real value before running the connector."
            )


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    # A single diagnostic table proving the JDBC connection was established and
    # authenticated. Not a real data table.
    return [
        {
            "table": "jdbc_connection_check",
            "primary_key": ["checked_at"],
            "columns": {
                "checked_at": "UTC_DATETIME",
                "connected": "BOOLEAN",
            },
        },
    ]


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
    log.warning("Example: Connectors Example : Aveva PI (JDBC connectivity check)")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    # Open an authenticated JDBC connection via the AVEVA-supplied driver.
    conn = connect(configuration)
    try:
        info = get_driver_and_server_info(conn)
        log.info(
            f"Connected via {info['driver_name']} {info['driver_version']} "
            f"to {info['database_product_name']} {info['database_product_version']}"
        )
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(
            table="jdbc_connection_check",
            data={
                "checked_at": datetime.now(timezone.utc),
                "connected": True,
                **info,
            },
        )
    finally:
        conn.close()

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    op.checkpoint(state)


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

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
