"""This connector demonstrates loading a compiled native C library with ctypes and calling it
for every row during a sync. A synthetic stub library is used, so no credentials or external
systems are needed.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For loading the native shared library and passing C-compatible buffers to it
import ctypes

# For reading configuration.json when running locally
import json

# For resolving the library path relative to this file
import os

# For measuring the per-call overhead of the native function
import time

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

__DEFAULT_LIBRARY_PATH = "lib_src/libstublibrary.so"
__DEFAULT_TOTAL_ROWS = 50000
__DEFAULT_BATCH_SIZE = 1000
__INIT_DIRECTORY = b"/tmp/stub-init-dir"


def __positive_int(configuration: dict, key: str, default: int) -> int:
    """
    Read an optional positive integer from the configuration.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        key: name of the configuration value.
        default: value used when the key is missing.
    Raises:
        ValueError: if the value is present but is not a positive integer.
    """
    raw = configuration.get(key)
    if raw in (None, "") or str(raw).strip().startswith("<"):
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise ValueError(f"{key} must be a positive integer")
    if value <= 0:
        raise ValueError(f"{key} must be a positive integer")
    return value


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary and confirm the native library exists.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if a value is invalid or the library file cannot be found.
    """
    __positive_int(configuration, "total_rows", __DEFAULT_TOTAL_ROWS)
    __positive_int(configuration, "batch_size", __DEFAULT_BATCH_SIZE)
    if not os.path.exists(__library_path(configuration)):
        raise ValueError(
            "Native library not found. Build it from lib_src/stub_library.c as described in the README."
        )


def __library_path(configuration: dict) -> str:
    """Resolve the shared library path relative to this file."""
    relative = configuration.get("library_path")
    if relative in (None, "") or relative.startswith("<"):
        relative = __DEFAULT_LIBRARY_PATH
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), relative)


def __load_library(path: str):
    """
    Load the shared library and declare the argument and return types of each function.
    Declaring the types is required so ctypes passes pointers and integers correctly.
    Args:
        path: absolute path of the shared library.
    Returns:
        The loaded ctypes library.
    """
    lib = ctypes.CDLL(path)
    lib.stub_init.argtypes = [ctypes.c_char_p]
    lib.stub_init.restype = ctypes.c_int
    lib.stub_transform.argtypes = [ctypes.c_char_p, ctypes.c_int]
    lib.stub_transform.restype = ctypes.c_int
    lib.stub_free.argtypes = []
    lib.stub_free.restype = ctypes.c_int
    return lib


def schema(configuration: dict):
    """
    Define the table schema for the connector.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        A list of table definitions with primary keys and column types.
    """
    return [
        {
            "table": "transformed_rows",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "raw_value": "STRING",
                "transformed_value": "STRING",
            },
        }
    ]


def transform_value(lib, raw_value: str) -> str:
    """
    Pass one value through the native library and return the transformed text.
    The library edits the buffer in place, so a mutable buffer is created for each call.
    Args:
        lib: the loaded ctypes library.
        raw_value: the value to transform.
    Raises:
        RuntimeError: if the native function returns a non-zero code.
    """
    buf = ctypes.create_string_buffer(raw_value.encode(), len(raw_value))
    rc = lib.stub_transform(buf, len(raw_value))
    if rc != 0:
        raise RuntimeError(f"stub_transform failed for value {raw_value} rc={rc}")
    return buf.value.decode()


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector, used to resume after the last committed batch.
    """
    log.warning("Example: Common Patterns For Connectors : Native Library With Ctypes")

    validate_configuration(configuration)
    total_rows = __positive_int(configuration, "total_rows", __DEFAULT_TOTAL_ROWS)
    batch_size = __positive_int(configuration, "batch_size", __DEFAULT_BATCH_SIZE)

    # Native libraries often document init and free as expensive, so they are called once per
    # sync rather than per row. update() runs in one long-lived process, which makes this safe.
    lib = __load_library(__library_path(configuration))
    rc = lib.stub_init(__INIT_DIRECTORY)
    if rc != 0:
        raise RuntimeError(f"stub_init failed with rc={rc}")
    log.info("Native library initialized once for this sync")

    start_row = int(state.get("last_row", 0))
    call_count = 0
    elapsed = 0.0

    try:
        for batch_start in range(start_row, total_rows, batch_size):
            batch_end = min(batch_start + batch_size, total_rows)

            for row_id in range(batch_start, batch_end):
                raw_value = f"ROW{row_id:012d}"

                started = time.perf_counter()
                transformed = transform_value(lib, raw_value)
                elapsed += time.perf_counter() - started
                call_count += 1

                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(
                    table="transformed_rows",
                    data={"id": row_id, "raw_value": raw_value, "transformed_value": transformed},
                )

            # Save the progress by checkpointing the state. This is important for ensuring that
            # the sync process can resume after the last committed batch in case of interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state={"last_row": batch_end})

        avg_ms = (elapsed / call_count) * 1000 if call_count else 0
        log.info(f"Processed {call_count} native calls, avg {avg_ms:.4f} ms/call")
        # Reset the cursor so the next sync starts over from the first row.
        op.checkpoint(state={"last_row": 0})
    finally:
        log.info(f"stub_free returned {lib.stub_free()}")


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
