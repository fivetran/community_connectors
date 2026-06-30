# This connector syncs data from an IBM DB2 for i (IBM i / AS400) database using the IBM i Access ODBC Driver.
# It defines an `update` method, which incrementally syncs the CUSTOMER table using the UPDATE_TIMESTAMP column.
# The first sync is a full load; subsequent syncs fetch only rows where UPDATE_TIMESTAMP is greater than
# the highest value seen in the previous sync.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk.
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import pyodbc for connecting to IBM i via the IBM i Access ODBC Driver.
# Requires the IBM i Access ODBC Driver to be installed — see drivers/installation.sh.
import pyodbc

# For handling incremental sync timestamps
from datetime import datetime, timezone

# For reading configuration from a JSON file
import json

# For validating schema names before SQL interpolation
import re

# For testing TCP connectivity before opening the ODBC connection
import socket

# For measuring query and fetch performance
import time

# Number of rows to fetch per batch from the database
__BATCH_SIZE = 1000
# Set the checkpoint interval to 10000 rows
__CHECKPOINT_INTERVAL = 10000
# Timeout in seconds for the initial TCP connectivity check
__DEFAULT_TIMEOUT_SECONDS = 60
# Default start timestamp for the first full sync
__DEFAULT_SYNC_START = "1990-01-01T00:00:00"


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "customer",  # Name of the table in the destination, required.
            "primary_key": ["c_d_id", "c_id"],  # Primary key column(s) for the table, optional.
        }
    ]


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required fields with valid values.
    This function checks if the necessary parameters for connecting to the IBM i database are present
    and that key values are of the correct type.
    If any required parameter is missing or invalid, it raises a ValueError with an appropriate message.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or invalid.
    """
    required_keys = ["hostname", "port", "database", "user_id", "password"]
    for key in required_keys:
        if key not in configuration:
            raise ValueError(f"Missing required configuration key: {key}")

    # Validate that port is a valid integer
    port_str = configuration.get("port", "")
    try:
        int(port_str)
    except (ValueError, TypeError):
        raise ValueError(f"Configuration key 'port' must be a valid integer, got: {port_str!r}")


def parse_state_timestamp(timestamp_str: str):
    """
    Parse a timestamp string from the state dictionary into a datetime object.
    If the string is missing or unparseable, returns a default datetime of 1990-01-01.
    Args:
        timestamp_str: A string representing the timestamp stored in state.
    Returns:
        A timezone-aware datetime object representing the last processed timestamp.
    """
    if not timestamp_str:
        return datetime(1990, 1, 1, tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return datetime(1990, 1, 1, tzinfo=timezone.utc)


def test_tcp(host: str, port: int, timeout_seconds: float):
    """
    Test TCP connectivity to the IBM i host before establishing the ODBC connection.
    Args:
        host: The hostname or IP address of the IBM i system.
        port: The port number to connect to.
        timeout_seconds: The connection timeout in seconds.
    """
    started = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
            log.info(f"TCP connectivity succeeded to {host}:{port} in {elapsed_ms} ms")
    except OSError as exc:
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        raise RuntimeError(
            f"TCP connectivity failed to {host}:{port} after {elapsed_ms} ms: {exc}"
        ) from exc


def connect_to_db2i(configuration: dict):
    """
    Connect to the IBM DB2 for i database using the IBM i Access ODBC Driver.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        conn: A pyodbc connection object if the connection is successful.
    """
    hostname = configuration.get("hostname")
    port = configuration.get("port")
    database = configuration.get("database")
    user_id = configuration.get("user_id")
    password = configuration.get("password")

    conn_str = (
        f"Driver=IBM i Access ODBC Driver;"
        f"System={hostname};"
        f"UID={user_id};"
        f"PWD={password};"
        f"Database={database};"
        f"Port={port};"
    )

    started = time.perf_counter()
    try:
        conn = pyodbc.connect(conn_str)
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        log.info(f"ODBC connection succeeded in {elapsed_ms} ms")
        return conn
    except Exception as exc:
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        raise RuntimeError(f"ODBC connection failed after {elapsed_ms} ms: {exc}") from exc


def fetch_and_upsert_data(conn, db_schema: str, last_update_timestamp: str):
    """
    Fetch rows from the CUSTOMER table updated after last_update_timestamp and upsert them.
    On the first sync (empty state), fetches all rows. Subsequent syncs fetch only rows where
    UPDATE_TIMESTAMP is greater than the highest value seen in the previous sync.
    Processes rows in batches of __BATCH_SIZE and checkpoints every __CHECKPOINT_INTERVAL rows.
    Args:
        conn: A pyodbc connection object to the IBM i database.
        db_schema: The library/schema name containing the CUSTOMER table.
        last_update_timestamp: ISO-format timestamp string from state; rows updated after this
            value are fetched. On first sync this defaults to __DEFAULT_SYNC_START.
    Returns:
        tuple: A tuple of (row_count, total_fetch_ms) summarising the sync.
    """
    # Validate db_schema to prevent SQL injection via configuration values
    if not re.match(r"^[A-Za-z0-9_]+$", db_schema):
        raise ValueError(
            f"Invalid schema name: {db_schema!r}. Only alphanumeric characters and underscores are allowed."
        )

    cursor = conn.cursor()
    # Filter by UPDATE_TIMESTAMP to enable incremental syncs; ORDER BY ensures rows arrive in
    # ascending timestamp order so the running maximum is always the last row seen.
    sql = f"SELECT * FROM {db_schema}.CUSTOMER WHERE UPDATE_TIMESTAMP > '{last_update_timestamp}' ORDER BY UPDATE_TIMESTAMP"

    query_start = time.perf_counter()
    cursor.execute(sql)
    log.info(f"Query executed in {(time.perf_counter() - query_start) * 1000:.0f} ms")

    columns = [desc[0].lower() for desc in cursor.description]
    # Track the highest UPDATE_TIMESTAMP seen so it can be saved to state
    latest_timestamp = parse_state_timestamp(last_update_timestamp)
    row_count = 0
    batch_num = 0
    total_fetch_ms = 0

    while True:
        batch_start = time.perf_counter()
        rows = cursor.fetchmany(__BATCH_SIZE)
        fetch_ms = (time.perf_counter() - batch_start) * 1000
        total_fetch_ms += fetch_ms

        if not rows:
            break

        batch_num += 1
        for row in rows:
            data = dict(zip(columns, row))

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table="customer", data=data)
            row_count += 1

            # Update the running maximum of UPDATE_TIMESTAMP.
            # pyodbc may return a datetime object or a string depending on the driver version.
            row_ts = data.get("update_timestamp")
            if row_ts is not None:
                if not isinstance(row_ts, datetime):
                    row_ts = parse_state_timestamp(str(row_ts))
                elif row_ts.tzinfo is None:
                    row_ts = row_ts.replace(tzinfo=timezone.utc)
                if row_ts > latest_timestamp:
                    latest_timestamp = row_ts

            if row_count % __CHECKPOINT_INTERVAL == 0:
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
                # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
                op.checkpoint(
                    state={"customer_last_update_timestamp": latest_timestamp.isoformat()}
                )

        log.info(f"Batch {batch_num}: {len(rows)} rows in {fetch_ms:.0f} ms")

    avg_ms = total_fetch_ms / batch_num if batch_num > 0 else 0
    log.info(
        f"DONE: {row_count} rows, {batch_num} batches, "
        f"{total_fetch_ms:.0f} ms total, avg {avg_ms:.0f} ms/batch"
    )

    # Final checkpoint saves the highest UPDATE_TIMESTAMP seen, which becomes the cursor
    # for the next incremental sync. Also ensures Fivetran receives the safe-to-write signal
    # when total row count is not an exact multiple of __CHECKPOINT_INTERVAL.
    op.checkpoint(state={"customer_last_update_timestamp": latest_timestamp.isoformat()})

    return row_count, total_fetch_ms


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
    log.warning("Example: Source Examples: IBM DB2 for i")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    hostname = configuration.get("hostname")
    port = int(configuration.get("port"))
    timeout_seconds = float(configuration.get("timeout_seconds", __DEFAULT_TIMEOUT_SECONDS))
    database = configuration.get("database")

    # Load the last update timestamp from state; defaults to __DEFAULT_SYNC_START for the first sync
    last_update_timestamp = state.get("customer_last_update_timestamp", __DEFAULT_SYNC_START)
    log.info(f"Current cursor: {last_update_timestamp}")

    # Verify TCP connectivity before opening the ODBC connection
    test_tcp(hostname, port, timeout_seconds)

    conn = connect_to_db2i(configuration)
    try:
        fetch_and_upsert_data(conn, database, last_update_timestamp)
    finally:
        conn.close()
        log.info("Connection to IBM DB2 for i closed")


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
