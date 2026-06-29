"""AVEVA PI connector that syncs asset hierarchy and time-series data from AVEVA PI
(formerly OSIsoft PI) using the PI Web API REST interface.
No proprietary ODBC drivers are required — the connector communicates over HTTPS
using standard Basic authentication.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling logs in the connector
from fivetran_connector_sdk import Logging as log

# For supporting data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op  # noqa: F401 — re-exported for SDK

# Local modules split by responsibility:
#   client.py  — HTTP session, API calls, pagination, database discovery
#   models.py  — record extraction and timestamp helpers
#   sync.py    — per-table sync strategies (full reimport and incremental)
from client import build_session, base_url, get_database_web_id
from sync import sync_elements, sync_attributes, sync_event_frames, sync_recorded_values

# Default start date for first incremental sync (Unix epoch)
__EPOCH_ISO = "1970-01-01T00:00:00Z"


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
    required = ("base_url", "username", "password")
    for key in required:
        if not configuration.get(key):
            raise ValueError(f"Missing or empty required configuration key: '{key}'")

    # Validate start_date format if provided
    start_date = configuration.get("start_date")
    if start_date:
        try:
            from datetime import datetime
            datetime.fromisoformat(start_date.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(
                f"Invalid start_date format '{start_date}'. "
                "Expected ISO 8601, e.g. '2020-01-01T00:00:00Z'."
            )

    # Validate sync_recorded_values flag if provided
    sync_rv = configuration.get("sync_recorded_values", "false")
    if sync_rv.lower() not in ("true", "false"):
        raise ValueError(
            f"Invalid sync_recorded_values value '{sync_rv}'. Expected 'true' or 'false'."
        )


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    validate_configuration(configuration)

    # Four tables mapping directly to PI AF object types exposed by PI Web API.
    # Column types are not declared here; the SDK auto-detects them from upserted data.
    return [
        {"table": "elements",        "primary_key": ["web_id"]},
        {"table": "attributes",      "primary_key": ["web_id"]},
        {"table": "event_frames",    "primary_key": ["web_id"]},
        {"table": "recorded_values", "primary_key": ["_fivetran_id"]},
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
    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    # Build the HTTP session and resolve the target database WebId
    session = build_session(configuration)
    base = base_url(configuration)
    database_name = configuration.get("database_name")
    start_date = configuration.get("start_date", __EPOCH_ISO)
    do_recorded = configuration.get("sync_recorded_values", "false").lower() == "true"

    db_web_id = get_database_web_id(session, base, database_name)

    # 1. Full reimport of the PI AF element hierarchy
    sync_elements(session, base, db_web_id, state)

    # 2. Full reimport of element attributes; also returns PI Point WebIds for step 4
    pi_point_web_ids = sync_attributes(session, base, db_web_id, state)

    # 3. Incremental sync of event frames by start_time cursor
    sync_event_frames(session, base, db_web_id, state, start_date)

    # 4. Incremental sync of recorded values (opt-in — can produce very large volumes)
    if do_recorded:
        sync_recorded_values(session, base, pi_point_web_ids, state, start_date)
    else:
        log.info(
            "Skipping recorded_values sync. "
            "Set sync_recorded_values = \"true\" in configuration to enable. "
            "Warning: this can generate very large data volumes on large PI deployments."
        )


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
