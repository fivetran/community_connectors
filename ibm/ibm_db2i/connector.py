# This connector syncs data from an IBM DB2 for i (IBM i / AS400) database using the IBM i Access ODBC Driver.
# It defines an `update` method, which upserts data from the CUSTOMER table via pyodbc.
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

import json
import socket
import time

# Number of rows to fetch per batch from the database
BATCH_SIZE = 1000
# Set the checkpoint interval to 10000 rows
CHECKPOINT_INTERVAL = 10000
# Timeout for the initial TCP connectivity check
DEFAULT_TIMEOUT_SECONDS = 60


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "customer",  # Name of the table in the destination, required.
            # Set primary_key to the primary key column(s) of your CUSTOMER table.
        }
    ]


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required fields.
    This function checks if the necessary parameters for connecting to the IBM i database are present.
    If any required parameter is missing, it raises a ValueError with an appropriate message.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_keys = ["hostname", "port", "database", "user_id", "password"]
    for key in required_keys:
        if key not in configuration:
            raise ValueError(f"Missing required configuration key: {key}")


def test_tcp(host: str, port: int, timeout: float):
    """
    Test TCP connectivity to the IBM i host before establishing the ODBC connection.
    Args:
        host: The hostname or IP address of the IBM i system.
        port: The port number to connect to.
        timeout: The connection timeout in seconds.
    """
    started = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
            log.info(f"TCP connectivity succeeded to {host}:{port} in {elapsed_ms} ms")
    except Exception as exc:
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


def fetch_and_upsert_data(conn, db_schema: str):
    """
    Fetch all rows from the CUSTOMER table and upsert them to the destination.
    Processes rows in batches and checkpoints every CHECKPOINT_INTERVAL rows.
    Args:
        conn: A pyodbc connection object to the IBM i database.
        db_schema: The library/schema name containing the CUSTOMER table.
    Returns:
        tuple: A tuple of (row_count, total_fetch_ms) summarising the sync.
    """
    cursor = conn.cursor()
    sql = f"SELECT * FROM {db_schema}.CUSTOMER"

    query_start = time.perf_counter()
    cursor.execute(sql)
    log.info(f"Query executed in {(time.perf_counter() - query_start) * 1000:.0f} ms")

    columns = [desc[0].lower() for desc in cursor.description]
    row_count = 0
    batch_num = 0
    total_fetch_ms = 0

    while True:
        batch_start = time.perf_counter()
        rows = cursor.fetchmany(BATCH_SIZE)
        fetch_ms = (time.perf_counter() - batch_start) * 1000
        total_fetch_ms += fetch_ms

        if not rows:
            break

        batch_num += 1
        for row in rows:
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table="customer", data=dict(zip(columns, row)))
            row_count += 1

            if row_count % CHECKPOINT_INTERVAL == 0:
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
                # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
                op.checkpoint(state={})

        log.info(f"Batch {batch_num}: {len(rows)} rows in {fetch_ms:.0f} ms")

    avg_ms = total_fetch_ms / batch_num if batch_num > 0 else 0
    log.info(
        f"DONE: {row_count} rows, {batch_num} batches, "
        f"{total_fetch_ms:.0f} ms total, avg {avg_ms:.0f} ms/batch"
    )
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

    # Validate configuration before attempting any connection
    validate_configuration(configuration)

    hostname = configuration.get("hostname")
    port = int(configuration.get("port"))
    timeout_seconds = float(configuration.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS))
    database = configuration.get("database")

    # Verify TCP connectivity before opening the ODBC connection
    test_tcp(hostname, port, timeout_seconds)

    conn = connect_to_db2i(configuration)
    try:
        fetch_and_upsert_data(conn, database)
    finally:
        conn.close()
        log.info("Connection to IBM DB2 for i closed")


# This creates the connector object that will use the update function defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug(configuration=configuration)
