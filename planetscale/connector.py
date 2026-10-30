"""This connector replicates tables from a PlanetScale (Vitess) database by reading VStream, the Vitess change stream.
See the Technical Reference documentation (https://fivetran.com/docs/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connector-sdk#connectordevelopmentbestpractices) for details
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For importing the Vitess protobuf modules after they are compiled
import importlib

# For encoding the saved copy cursors and the gRPC credentials as base64 text
import base64

# For locating the proto directory next to this file
import os

# For recognizing the names of internal tables that Vitess creates
import re

# For adding the compiled protobuf modules to the import path while they are loaded
import sys

# For the temporary directory that receives the compiled protobuf modules
import tempfile

# For retry delays, pass timeouts and checkpoint throttling
import time

# For the TLS gRPC channel that carries the VStream change stream
import grpc

# For compiling the Vitess .proto files into Python modules when the connector starts
from grpc_tools import protoc

# For the MySQL protocol connection that discovers shards, tables and primary keys
import pymysql

# For the CA certificate bundle that verifies PlanetScale's TLS certificate
import requests

# Configuration parameters that must be set
__REQUIRED_CONFIG_KEYS = ["host", "database", "username", "password"]
# First character of a configuration value that is still an unedited placeholder, such as <YOUR_PASSWORD>
__PLACEHOLDER_PREFIX = "<"
# Last character of an unedited placeholder
__PLACEHOLDER_SUFFIX = ">"

# Folder next to this file that holds the Vitess .proto files
__PROTO_DIRECTORY_NAME = "proto"
# The Vitess .proto files that are compiled when the connector starts
__PROTO_FILES = [
    "vtgateservice.proto",
    "vtgate.proto",
    "binlogdata.proto",
    "query.proto",
    "topodata.proto",
    "vtrpc.proto",
    "vtorcdata.proto",
    "vttime.proto",
    "querythrottler.proto",
]
# The compiled modules the connector uses, in the order load_vitess_modules() returns them
__VITESS_MODULE_NAMES = [
    "binlogdata_pb2",
    "query_pb2",
    "topodata_pb2",
    "vtgate_pb2",
    "vtgateservice_pb2_grpc",
]

# Port of the MySQL protocol connection that discovers shards and tables
__MYSQL_PORT = 3306
# Port of the TLS gRPC connection that carries the VStream change stream
__GRPC_PORT = 443
# Seconds to wait for the MySQL connection to open
__MYSQL_CONNECT_TIMEOUT_SECONDS = 20
# Seconds to wait for a MySQL query to return
__MYSQL_READ_TIMEOUT_SECONDS = 60
# Rows fetched per round trip while reading discovery results
__FETCH_BATCH_SIZE = 1000

# Seconds allowed to read the shard's current position
__PEEK_TIMEOUT_SECONDS = 45
# Length of one VStream pass in seconds; a longer copy continues in the next pass from the saved cursor
__READ_TIMEOUT_SECONDS = 900
# Passes in a row without progress before the connector gives up on a table and shard
__MAX_STALLED_PASSES = 3
# Least seconds between two throttled checkpoints; the SDK advises against checkpointing more often
__MIN_CHECKPOINT_SECONDS = 60
# Seconds between progress messages while a table is read
__PROGRESS_LOG_SECONDS = 60

# Attempts for each call that can fail transiently, including the first one
__RETRY_ATTEMPTS = 4
# Seconds to wait before the first retry
__RETRY_BASE_SECONDS = 2
# Each retry waits this many times longer than the previous one
__BACKOFF_MULTIPLIER = 2
# Longest error description written to the retry log, in characters
__MAX_ERROR_TEXT_CHARS = 150
# gRPC error codes worth retrying: the service is unavailable, asks the client to slow down, or did not answer in time
__TRANSIENT_GRPC_CODES = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
    grpc.StatusCode.DEADLINE_EXCEEDED,
}
# MySQL error codes worth retrying: too many connections, cannot connect, server gone away, connection lost
__TRANSIENT_MYSQL_CODES = {1040, 2003, 2006, 2013}
# Text of a TLS certificate failure, which is permanent and is not retried
__CERTIFICATE_ERROR_TEXT = "CERTIFICATE_VERIFY_FAILED"

# Largest gRPC message accepted from PlanetScale, in bytes (256 MiB)
__MAX_MESSAGE_BYTES = 256 * 1024 * 1024
# Most table names listed in the warning about tables without a primary key
__MAX_TABLES_IN_WARNING = 20
# Vitess cell that PlanetScale serves VStream from, as in the partner-built Go connector
__PLANETSCALE_CELL = "planetscale_operator_default"
# Table type reported by SHOW FULL TABLES for real tables; views are excluded
__BASE_TABLE_TYPE = "BASE TABLE"
# Text of the error PlanetScale returns when the saved position has been purged from its binary logs
__BINLOG_EXPIRED_TEXT = "Cannot replicate because the source purged required binary logs"
# Position name that asks VStream for the shard's latest position
__CURRENT_POSITION = "current"

# How a VStream pass ended: the table is up to date
__OUTCOME_CAUGHT_UP = "caught up"
# How a VStream pass ended: the initial copy of the table finished
__OUTCOME_COPY_COMPLETED = "copy completed"
# How a VStream pass ended: the pass reached its time limit
__OUTCOME_TIMEOUT = "timeout"
# How a VStream pass ended: the connection dropped with a transient error
__OUTCOME_INTERRUPTED = "interrupted"

# Vitess creates internal tables for table GC and online schema changes. They never hold user data, so they
# are skipped. These are the same patterns as the partner-built Go connector (schema_builder.go).
__VITESS_INTERNAL_TABLE = re.compile(
    r"_[0-9a-zA-Z]{8}_[0-9a-zA-Z]{4}_[0-9a-zA-Z]{4}_.*|_vt_.*|\b_(\w+|\d+)_\d+_vrepl\b"
)

# VStream column types that are converted to Python integers
__INTEGER_TYPES = {
    "INT8",
    "UINT8",
    "INT16",
    "UINT16",
    "INT24",
    "UINT24",
    "INT32",
    "UINT32",
    "INT64",
    "UINT64",
    "YEAR",
}
# VStream column types that are converted to Python floats
__FLOAT_TYPES = {"FLOAT32", "FLOAT64"}
# VStream column types that are kept as bytes
__BINARY_TYPES = {"BLOB", "BINARY", "VARBINARY", "BIT", "GEOMETRY"}
# VStream column types that are passed through unconverted. ENUM was checked against PlanetScale and
# arrives as its text label, so only SET and GEOMETRY need the warning.
__UNCONVERTED_TYPES = {"SET", "GEOMETRY"}


def load_vitess_modules() -> tuple:
    """
    Compile the Vitess .proto files with grpcio-tools and import the generated Python modules.
    The generated files are written to a temporary directory that is removed once the modules are imported,
    so the compiled code always matches the grpcio and protobuf versions of the runtime.
    Returns:
        (binlog, query, topo, vtgate, vtgate_grpc): the VStream event, row and field, tablet type and request
        messages, and the gRPC client stub module.
    Raises:
        RuntimeError: if protoc cannot compile the proto files.
    """
    proto_directory = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), __PROTO_DIRECTORY_NAME
    )
    with tempfile.TemporaryDirectory() as output_directory:
        exit_code = protoc.main(
            [
                "protoc",
                f"-I{proto_directory}",
                f"--python_out={output_directory}",
                f"--grpc_python_out={output_directory}",
                *__PROTO_FILES,
            ]
        )
        if exit_code != 0:
            raise RuntimeError(
                f"protoc could not compile the Vitess proto files in {proto_directory} "
                f"(exit code {exit_code})"
            )
        # The generated modules import each other by name, so the directory must be importable while they load
        sys.path.insert(0, output_directory)
        try:
            importlib.invalidate_caches()
            return tuple(importlib.import_module(name) for name in __VITESS_MODULE_NAMES)
        finally:
            sys.path.remove(output_directory)
            sys.path_importer_cache.pop(output_directory, None)


# The Vitess protobuf modules, compiled from the .proto files when the connector starts (see the README)
binlog, query, topo, vtgate, vtgate_grpc = load_vitess_modules()


class Cursor:
    """
    Where the VStream resumes for one table and shard: a GTID position, the last primary key copied, and whether
    the initial copy of the table is still in progress. A table counts as caught up only after VStream reports
    that its copy is complete, never because no key has been seen yet.
    """

    def __init__(self, position: str = "", last_primary_key=None, is_copy_in_progress=None):
        """
        Create a cursor.
        Args:
            position: The GTID set the stream has reached, or an empty string if the table has not synced yet.
            last_primary_key: A query.QueryResult holding the last primary key copied, or None if no row has been
                copied yet.
            is_copy_in_progress: True until VStream reports that the initial copy of the table is complete. A
                cursor without a position has copied nothing yet, so the default is True in that case.
        """
        self.position = position
        self.last_primary_key = last_primary_key
        if is_copy_in_progress is None:
            is_copy_in_progress = not position
        self.is_copy_in_progress = is_copy_in_progress

    @classmethod
    def from_state(cls, saved):
        """
        Rebuild a cursor from the dictionary stored in state.
        Args:
            saved: The dictionary written by to_state(), or None if this table and shard have not synced yet.
        Returns:
            A Cursor.
        """
        if not saved:
            return cls()
        last_primary_key = None
        if saved.get("last_primary_key"):
            last_primary_key = query.QueryResult.FromString(
                base64.b64decode(saved["last_primary_key"])
            )
        position = saved.get("position", "")
        # State saved before the flag existed: a saved key, or no position at all, means the copy was in progress
        is_copy_in_progress = saved.get(
            "is_copy_in_progress", last_primary_key is not None or not position
        )
        return cls(position, last_primary_key, is_copy_in_progress)

    def to_state(self) -> dict:
        """
        Return the cursor as a JSON-serializable dictionary for state.
        Returns:
            A dictionary with the position, the base64 encoded last primary key (or None), and the
            copy-in-progress flag.
        """
        last_primary_key = None
        if self.last_primary_key is not None:
            last_primary_key = base64.b64encode(self.last_primary_key.SerializeToString()).decode()
        return {
            "position": self.position,
            "last_primary_key": last_primary_key,
            "is_copy_in_progress": self.is_copy_in_progress,
        }

    def get_resume_position(self) -> str:
        """
        Give the GTID position a stream request starts from. While the initial copy has not copied a row yet there
        is nothing to resume, so the position is empty and the copy starts over.
        Returns:
            The GTID position, or an empty string to start the copy of the table from the beginning.
        """
        if self.is_copy_in_progress and self.last_primary_key is None:
            return ""
        return self.position

    def has_progress(self) -> bool:
        """
        Tell whether this table and shard have a saved position or are part-way through the initial copy.
        Returns:
            True if the cursor has a position or a last primary key.
        """
        return bool(self.position) or self.last_primary_key is not None


class PassProgress:
    """What one VStream pass has seen so far: the cursor, the row changes applied and the current columns."""

    def __init__(self, cursor: Cursor):
        """
        Start tracking a pass.
        Args:
            cursor: The cursor the pass starts from.
        """
        self.cursor = cursor
        self.rows = 0
        self.fields = None
        self.is_watching = False  # True once a position event matches the stop position
        self.latest_position = (
            cursor.position
        )  # Newest position seen, which a finished copy resumes from


class Checkpointer:
    """
    Records each cursor in state and writes state with op.checkpoint(). Cursors are written at most once every
    min_interval_seconds, and flush() writes whatever is still pending. Work since the last checkpoint is
    replayed after a failure, which is safe because upserts and deletes are idempotent.
    """

    def __init__(self, state: dict, min_interval_seconds: int):
        """
        Create a checkpointer.
        Args:
            state: The state dictionary of this sync. It is updated in place.
            min_interval_seconds: The least time between two throttled checkpoints.
        """
        self.state = state
        self.min_interval_seconds = min_interval_seconds
        self.last_checkpoint_time = time.monotonic()
        self.has_pending_changes = False

    def save(self, table: str, shard: str, cursor: Cursor):
        """
        Store the cursor in state, and checkpoint if enough time has passed.
        Args:
            table: The table the cursor belongs to.
            shard: The shard the cursor belongs to.
            cursor: The cursor to store.
        """
        table_state = self.state["tables"].setdefault(table, {"shards": {}})
        cursor_state = cursor.to_state()
        if table_state["shards"].get(shard) != cursor_state:
            table_state["shards"][shard] = cursor_state
            self.has_pending_changes = True
        if time.monotonic() - self.last_checkpoint_time >= self.min_interval_seconds:
            self.flush()

    def flush(self, is_forced: bool = False):
        """
        Write state with op.checkpoint() if anything changed since the last checkpoint.
        Args:
            is_forced: Checkpoint even if nothing changed, which tells Fivetran it is safe to write to the destination.
        """
        if not (self.has_pending_changes or is_forced):
            return
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(self.state)
        self.last_checkpoint_time = time.monotonic()
        self.has_pending_changes = False


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if a required configuration parameter is missing, is still a placeholder, or has an invalid format.
    """
    for key in __REQUIRED_CONFIG_KEYS:
        value = configuration.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Missing required configuration value: {key}")
        if is_placeholder(value):
            raise ValueError(
                f"Configuration value '{key}' is still a placeholder; replace it with your own value"
            )
        if value != value.strip():
            raise ValueError(f"Configuration value '{key}' has leading or trailing whitespace")

    host = configuration["host"].strip()
    if "://" in host or "/" in host or ":" in host:
        raise ValueError(
            "Configuration value 'host' must be a host name such as aws.connect.psdb.cloud, "
            "without a scheme, port or path"
        )

    tables = configuration.get("tables")
    if tables is not None and not isinstance(tables, str):
        raise ValueError(
            "Configuration value 'tables' must be a comma-separated string of table names"
        )
    if any(is_placeholder(name) for name in parse_table_list(tables)):
        raise ValueError(
            "Configuration value 'tables' is still a placeholder; list your table names "
            "or remove 'tables' to replicate every table"
        )


def is_placeholder(value: str) -> bool:
    """
    Tell whether a configuration value is an unedited placeholder such as <YOUR_PASSWORD>.
    Args:
        value: The configuration value to check.
    Returns:
        True if the value is wrapped in angle brackets.
    """
    text = value.strip()
    return text.startswith(__PLACEHOLDER_PREFIX) and text.endswith(__PLACEHOLDER_SUFFIX)


def quote_identifier(name: str) -> str:
    """
    Quote a table or database name for SQL and VStream filters, escaping embedded backticks.
    Args:
        name: The identifier to quote.
    Returns:
        The identifier wrapped in backticks.
    """
    return "`" + name.replace("`", "``") + "`"


def is_transient_grpc(error: Exception) -> bool:
    """
    Tell whether a gRPC error is worth retrying: the service is unavailable, asked us to slow down, or timed out.
    Args:
        error: The exception that was raised.
    Returns:
        True if the error is a transient gRPC error.
    """
    return isinstance(error, grpc.RpcError) and error.code() in __TRANSIENT_GRPC_CODES


def is_transient_mysql(error: Exception) -> bool:
    """
    Tell whether a MySQL protocol error is worth retrying: connection failures, but not bad credentials or bad TLS.
    Args:
        error: The exception that was raised.
    Returns:
        True if the error is a transient connection error.
    """
    if isinstance(error, pymysql.err.OperationalError):
        code = error.args[0] if error.args else None
        return code in __TRANSIENT_MYSQL_CODES and __CERTIFICATE_ERROR_TEXT not in str(error)
    return isinstance(error, (ConnectionError, TimeoutError))


def describe_error(error: Exception) -> str:
    """
    Build a short, readable description of an error. gRPC errors become 'CODE: details'.
    Args:
        error: The exception to describe.
    Returns:
        The description text.
    """
    if isinstance(error, grpc.RpcError) and hasattr(error, "code"):
        return f"{error.code().name}: {error.details()}"
    return str(error)


def retry(description: str, operation, is_transient):
    """
    Call operation(), retrying with exponential backoff when is_transient(error) is true.
    Permanent errors, such as bad credentials, are raised at once, and so is the last transient one.
    Args:
        description: What the operation does, for the retry log.
        operation: A function with no arguments that does the work.
        is_transient: A function that takes the exception and returns True if the call is worth retrying.
    Returns:
        Whatever operation() returns.
    Raises:
        grpc.RpcError, pymysql.MySQLError, OSError: the error from the last attempt, or a permanent error.
    """
    for attempt in range(1, __RETRY_ATTEMPTS + 1):
        try:
            return operation()
        except (grpc.RpcError, pymysql.MySQLError, OSError) as error:
            if attempt == __RETRY_ATTEMPTS or not is_transient(error):
                raise
            delay = __RETRY_BASE_SECONDS * __BACKOFF_MULTIPLIER ** (attempt - 1)
            log.warning(
                f"{description} failed ({describe_error(error)[:__MAX_ERROR_TEXT_CHARS]}); "
                f"retrying in {delay}s (attempt {attempt}/{__RETRY_ATTEMPTS})"
            )
            time.sleep(delay)


def connect_mysql(configuration: dict):
    """
    Open a MySQL protocol connection that verifies PlanetScale's TLS certificate. It is only used for discovery.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        A pymysql connection.
    """
    return pymysql.connect(
        host=configuration["host"],
        port=__MYSQL_PORT,
        user=configuration["username"],
        password=configuration["password"],
        database=configuration["database"],
        autocommit=True,
        connect_timeout=__MYSQL_CONNECT_TIMEOUT_SECONDS,
        read_timeout=__MYSQL_READ_TIMEOUT_SECONDS,
        ssl_ca=requests.certs.where(),
        ssl_verify_cert=True,
        ssl_verify_identity=True,
    )


def fetch_rows(cursor):
    """
    Yield the rows of an executed query, fetching them in batches instead of all at once.
    Args:
        cursor: A pymysql cursor on which execute() has been called.
    Returns:
        A generator of result rows.
    """
    while True:
        rows = cursor.fetchmany(__FETCH_BATCH_SIZE)
        if not rows:
            return
        yield from rows


def discover_shards(connection, database: str) -> list:
    """
    List the shards of the keyspace, as the partner-built Go connector does (SHOW VITESS_SHARDS).
    Args:
        connection: An open pymysql connection.
        database: The PlanetScale database (keyspace) name.
    Returns:
        The shard names, for example ["-"] for an unsharded keyspace.
    """
    with connection.cursor(pymysql.cursors.SSCursor) as cursor:
        cursor.execute("SHOW VITESS_SHARDS LIKE %s", (f"{database}/%",))
        names = [row[0] for row in fetch_rows(cursor)]
    return [name.split("/", 1)[1] for name in names if name.split("/", 1)[0] == database]


def discover_tables(connection, database: str) -> tuple:
    """
    List the base tables of the database (no views, no Vitess internal tables) and their primary keys.
    Args:
        connection: An open pymysql connection.
        database: The PlanetScale database (keyspace) name.
    Returns:
        (tables, skipped). tables maps each table that has a primary key to [(column name, MySQL column type)] in
        key order; skipped lists the tables without a primary key. Both are sorted by table name.
    """
    with connection.cursor(pymysql.cursors.SSCursor) as cursor:
        cursor.execute(f"SHOW FULL TABLES FROM {quote_identifier(database)}")
        names = [
            row[0]
            for row in fetch_rows(cursor)
            if row[1] == __BASE_TABLE_TYPE and not __VITESS_INTERNAL_TABLE.match(row[0])
        ]
        cursor.execute(
            "SELECT table_name, column_name, column_type, ordinal_position "
            "FROM information_schema.columns WHERE table_schema = %s AND column_key = 'PRI'",
            (database,),
        )
        key_rows = list(fetch_rows(cursor))
    keys = {}
    for table_name, column_name, column_type, _ in sorted(
        key_rows, key=lambda row: (row[0], row[3])
    ):
        keys.setdefault(table_name, []).append((column_name, column_type))
    tables = {name: keys[name] for name in sorted(names) if name in keys}
    skipped = sorted(name for name in names if name not in keys)
    return tables, skipped


def discover_source(configuration: dict) -> tuple:
    """
    Connect over the MySQL protocol, list the shards and tables, and disconnect, retrying on transient failures.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        (shards, tables, skipped) as returned by discover_shards() and discover_tables().
    Raises:
        RuntimeError: if PlanetScale cannot be reached or refuses the credentials.
    """

    def read_source():
        """Open a connection, read shards and tables, and always close the connection."""
        connection = connect_mysql(configuration)
        try:
            tables, skipped = discover_tables(connection, configuration["database"])
            return discover_shards(connection, configuration["database"]), tables, skipped
        finally:
            connection.close()

    try:
        return retry(
            "Reading shards and tables over the MySQL protocol", read_source, is_transient_mysql
        )
    except (pymysql.MySQLError, OSError) as error:
        raise RuntimeError(
            f"Unable to read from PlanetScale over the MySQL protocol at "
            f"{configuration['host']}:{__MYSQL_PORT}: {error}"
        ) from error


def parse_table_list(value) -> list:
    """
    Split the comma-separated "tables" setting into names, trimmed, without blanks or repeats.
    Args:
        value: The setting's value, or None if it is not set.
    Returns:
        The table names in the order they were listed.
    """
    names = []
    for name in (value or "").split(","):
        name = name.strip()
        if name and name not in names:
            names.append(name)
    return names


def select_tables(tables: dict, skipped: list, requested: list, database: str) -> tuple:
    """
    Apply the "tables" setting to the discovered tables.
    Args:
        tables: The tables with a primary key, as returned by discover_tables().
        skipped: The tables without a primary key.
        requested: The table names from the "tables" setting, empty to replicate everything.
        database: The PlanetScale database name, for error messages.
    Returns:
        (tables to replicate, skipped tables to warn about). With an explicit list nothing is skipped quietly.
    Raises:
        RuntimeError: if no table can be replicated, or a listed table is missing, a view, or has no primary key.
    """
    if not requested:
        if not tables:
            raise RuntimeError(
                f"No tables with a primary key were found in database '{database}' "
                f"({len(skipped)} table(s) without a primary key were skipped). "
                "This connector needs a primary key to apply deletes."
            )
        return tables, skipped
    problems = []
    missing = [name for name in requested if name not in tables and name not in skipped]
    keyless = [name for name in requested if name in skipped]
    if missing:
        problems.append(
            f"not found as base tables (views are not supported): {', '.join(missing)}"
        )
    if keyless:
        problems.append(f"no primary key (needed to apply deletes): {', '.join(keyless)}")
    if problems:
        raise RuntimeError(
            f"Table(s) in the 'tables' setting cannot be replicated from database '{database}': "
            f"{'; '.join(problems)}"
        )
    return {name: tables[name] for name in sorted(requested)}, []


def describe_skipped_tables(skipped: list) -> str:
    """
    Describe the tables skipped for lack of a primary key, for the logs and the dashboard.
    Args:
        skipped: The names of the skipped tables.
    Returns:
        The warning text.
    """
    shown = ", ".join(skipped[:__MAX_TABLES_IN_WARNING])
    hidden_count = len(skipped) - __MAX_TABLES_IN_WARNING
    more = f" and {hidden_count} more" if hidden_count > 0 else ""
    return (
        f"Skipped {len(skipped)} table(s) without a primary key "
        f"(deletes cannot be applied without one): {shown}{more}"
    )


def primary_key_sdk_type(column_type: str):
    """
    Map the MySQL type of a primary key column to an SDK data type.
    Only primary key columns are ever declared; every other column's type is left for the SDK to infer.
    Args:
        column_type: The MySQL column type, for example "bigint unsigned" or "varchar(32)".
    Returns:
        The SDK type name, or None if it is not a common key type.
    """
    kind = column_type.lower()
    if kind.startswith("bigint") or (kind.startswith("int") and "unsigned" in kind):
        return "LONG"
    if kind.startswith(("int", "mediumint", "smallint", "tinyint")):
        return "INT"
    if kind.startswith(("varchar", "char", "text", "tinytext", "mediumtext", "longtext")):
        return "STRING"
    if kind.startswith(("binary", "varbinary")):
        return "BINARY"
    return None


def open_channel(configuration: dict):
    """
    Open the gRPC channel to PlanetScale (TLS on port 443) and create the VStream client stub.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        (channel, stub). The caller closes the channel.
    """
    with open(requests.certs.where(), "rb") as ca_bundle_file:
        root_certificates = ca_bundle_file.read()
    channel = grpc.secure_channel(
        f"{configuration['host']}:{__GRPC_PORT}",
        grpc.ssl_channel_credentials(root_certificates=root_certificates),
        options=[("grpc.max_receive_message_length", __MAX_MESSAGE_BYTES)],
    )
    return channel, vtgate_grpc.VitessStub(channel)


def build_auth_metadata(configuration: dict) -> tuple:
    """
    Build the HTTP Basic credentials sent as gRPC metadata, which is how PlanetScale's own Go client authenticates.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        The metadata tuple for a gRPC call.
    """
    credentials = f"{configuration['username']}:{configuration['password']}"
    token = base64.b64encode(credentials.encode()).decode()
    return (("authorization", f"Basic {token}"),)


def build_request(configuration: dict, table: str, shard: str, cursor: Cursor):
    """
    Build the VStreamRequest the Go connector builds: one table, PRIMARY tablet, MinimizeSkew, PlanetScale cell.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        table: The table to stream.
        shard: The shard to stream.
        cursor: Where to resume. A copy that has not copied a row yet starts over from an empty position.
    Returns:
        A vtgate.VStreamRequest.
    """
    shard_gtid = binlog.ShardGtid(
        keyspace=configuration["database"], shard=shard, gtid=cursor.get_resume_position()
    )
    if cursor.last_primary_key is not None:
        shard_gtid.table_p_ks.add(table_name=table, lastpk=cursor.last_primary_key)
    # The filter's SELECT is the stream's column list, not a table scan: Vitess copies the table in primary key order
    rule = binlog.Rule(match=table, filter=f"SELECT * FROM {quote_identifier(table)}")
    return vtgate.VStreamRequest(
        tablet_type=topo.PRIMARY,
        vgtid=binlog.VGtid(shard_gtids=[shard_gtid]),
        filter=binlog.Filter(rules=[rule]),
        flags=vtgate.VStreamFlags(minimize_skew=True, cells=__PLANETSCALE_CELL),
    )


def find_shard_position(response, database: str, shard: str):
    """
    Find the shard's GTID position in a VStream response.
    Args:
        response: A vtgate.VStreamResponse.
        database: The PlanetScale database (keyspace) name.
        shard: The shard name.
    Returns:
        The GTID position, or None if the response does not carry one for this shard.
    """
    for event in response.events:
        if event.type != binlog.VGTID:
            continue
        for shard_gtid in event.vgtid.shard_gtids:
            if shard_gtid.keyspace == database and shard_gtid.shard == shard and shard_gtid.gtid:
                return shard_gtid.gtid
    return None


def peek_position(stub, configuration: dict, table: str, shard: str) -> str:
    """
    Read the shard's current GTID position (a VStream that starts at 'current'), which is where a pass stops.
    Args:
        stub: The VStream client stub.
        configuration: a dictionary that holds the configuration settings for the connector.
        table: The table the pass is for.
        shard: The shard the pass is for.
    Returns:
        The GTID position.
    Raises:
        RuntimeError: if PlanetScale does not return a position or cannot be reached.
    """

    def read_position():
        """Open a short stream, return the first position it reports, and close the stream."""
        request = build_request(configuration, table, shard, Cursor(__CURRENT_POSITION))
        stream = stub.VStream(
            request, metadata=build_auth_metadata(configuration), timeout=__PEEK_TIMEOUT_SECONDS
        )
        try:
            for response in stream:
                position = find_shard_position(response, configuration["database"], shard)
                if position:
                    return position
        finally:
            stream.cancel()
        raise RuntimeError(
            f"PlanetScale did not return a current position for table '{table}' shard '{shard}'"
        )

    try:
        return retry(
            f"Reading the current position of table '{table}'", read_position, is_transient_grpc
        )
    except grpc.RpcError as error:
        raise RuntimeError(
            f"Could not read the current position of table '{table}' shard '{shard}' from "
            f"{configuration['host']}:{__GRPC_PORT}: {describe_error(error)}"
        ) from error


def to_python(field, raw):
    """
    Convert one VStream value (text-encoded bytes) into a Python value the SDK stores sensibly.
    Args:
        field: The query.Field that describes the column.
        raw: The value as bytes, or None.
    Returns:
        An int, float, bytes, str or None.
    """
    if raw is None:
        return None
    kind = query.Type.Name(field.type)
    if kind in __INTEGER_TYPES:
        return int(raw)
    if kind in __FLOAT_TYPES:
        return float(raw)
    if kind in __BINARY_TYPES:
        return bytes(raw)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin-1")  # same fallback as the Go connector's CleanStringValue


def to_record(fields: list, row) -> dict:
    """
    Split a query.Row into a column dictionary. All values are packed into one byte string and a length of -1 means NULL.
    Args:
        fields: The query.Field list of the table.
        row: The query.Row to split.
    Returns:
        A dictionary of column name to value.
    """
    record = {}
    offset = 0
    for field, length in zip(fields, row.lengths):
        if length < 0:
            record[field.name] = None
        else:
            record[field.name] = to_python(field, row.values[offset : offset + length])
            offset += length
    return record


def has_key_changed(before: dict, after: dict, primary_key: list) -> bool:
    """
    Tell whether an update changed the primary key of the row.
    Args:
        before: The row as it was before the change.
        after: The row as it is after the change.
        primary_key: The primary key column names.
    Returns:
        True if any primary key column has a different value.
    """
    return any(before[key] != after[key] for key in primary_key)


def delete_row(table: str, before: dict, primary_key: list):
    """
    Delete the row that the before image describes.
    Args:
        table: The destination table.
        before: The row as it was before the change.
        primary_key: The primary key column names.
    """
    # The 'delete' operation marks the row as deleted in the destination table.
    # The first argument is the name of the destination table.
    # The second argument is a dictionary containing the primary key columns and values of the row to delete.
    op.delete(table=table, keys={key: before[key] for key in primary_key})


def emit_row_changes(row_event, fields: list, table: str, primary_key: list) -> int:
    """
    Turn a VStream row event into SDK operations. A primary key change deletes the old key and upserts the new row.
    Args:
        row_event: The binlog.RowEvent from the stream.
        fields: The query.Field list of the table.
        table: The table being synced; events for other tables are ignored.
        primary_key: The primary key column names.
    Returns:
        The number of row changes handled.
    """
    if strip_keyspace(row_event.table_name) != table:
        return 0
    count = 0
    for change in row_event.row_changes:
        before = to_record(fields, change.before) if change.HasField("before") else None
        after = to_record(fields, change.after) if change.HasField("after") else None
        if before is not None and (after is None or has_key_changed(before, after, primary_key)):
            delete_row(table, before, primary_key)
        if after is not None:
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table=table, data=after)
        count += 1
    return count


def strip_keyspace(table_name: str) -> str:
    """
    Remove the keyspace prefix from the table name of a VStream event, such as "mydb.orders".
    A keyspace name never contains a dot, but a table name can.
    Args:
        table_name: The table name as VStream reports it.
    Returns:
        The table name without the keyspace.
    """
    return table_name.split(".", 1)[-1]


def find_shard_gtid(vgtid, database: str, shard: str):
    """
    Find the entry of one shard in a VGtid.
    Args:
        vgtid: The binlog.VGtid from an event.
        database: The PlanetScale database (keyspace) name.
        shard: The shard name.
    Returns:
        The binlog.ShardGtid of the shard, or None if the VGtid has no entry for it.
    """
    for shard_gtid in vgtid.shard_gtids:
        if shard_gtid.keyspace == database and shard_gtid.shard == shard:
            return shard_gtid
    return None


def apply_vgtid(cursor: Cursor, vgtid, database: str, table: str, shard: str) -> Cursor:
    """
    Apply a VGTID event, which carries the new position and, during a copy, the last primary key copied.
    While a copy is in progress only a position that comes with a last primary key is kept, because it is the only
    one the copy can resume from. VStream sends a position without a key before the first rows of a copy and again
    after the last ones, and neither says that the table is up to date.
    Args:
        cursor: The current cursor.
        vgtid: The binlog.VGtid from the event.
        database: The PlanetScale database (keyspace) name.
        table: The table being synced.
        shard: The shard being synced.
    Returns:
        The updated cursor, or the same one if the event is for another shard or cannot be resumed from.
    """
    shard_gtid = find_shard_gtid(vgtid, database, shard)
    if shard_gtid is None:
        return cursor
    position = shard_gtid.gtid or cursor.position
    if not cursor.is_copy_in_progress:
        return Cursor(position, None, False)
    entry = next((item for item in shard_gtid.table_p_ks if item.table_name == table), None)
    if entry is None or not entry.lastpk.rows:
        return cursor
    return Cursor(position, entry.lastpk, True)


def apply_last_primary_key(cursor: Cursor, event, table: str) -> Cursor:
    """
    Apply a LASTPK event, which reports copy progress. A completed copy ends the copy-in-progress state.
    Args:
        cursor: The current cursor.
        event: The binlog.LastPKEvent from the stream.
        table: The table being synced.
    Returns:
        The updated cursor, or the same one if the event is for another table.
    """
    if strip_keyspace(event.table_last_p_k.table_name) != table:
        return cursor
    if event.completed:
        return Cursor(cursor.position, None, False)
    last_primary_key = event.table_last_p_k.lastpk
    if not last_primary_key.rows:
        return cursor
    return Cursor(cursor.position, last_primary_key, True)


def read_fields(field_event, table: str) -> list:
    """
    Read the column list from a FIELD event and warn once if a column type is passed through unconverted.
    Args:
        field_event: The binlog.FieldEvent from the stream.
        table: The table being synced, for the warning.
    Returns:
        The query.Field list.
    """
    fields = list(field_event.fields)
    unconverted = [
        field.name for field in fields if query.Type.Name(field.type) in __UNCONVERTED_TYPES
    ]
    if unconverted:
        log.warning(
            f"[{table}] SET or GEOMETRY columns are passed through unconverted: {unconverted}"
        )
    return fields


def process_response(
    response,
    progress: PassProgress,
    database: str,
    table: str,
    shard: str,
    primary_key: list,
    stop_position: str,
) -> bool:
    """
    Apply every event of one VStream response to the destination and to the pass progress.
    Args:
        response: A vtgate.VStreamResponse.
        progress: The progress of the running pass, updated in place.
        database: The PlanetScale database (keyspace) name.
        table: The table being synced.
        shard: The shard being synced.
        primary_key: The primary key column names.
        stop_position: The shard position the pass should bring the table up to.
    Returns:
        True if the response contained the copy-completed event.
    """
    is_copy_completed = False
    for event in response.events:
        if event.type == binlog.FIELD:
            progress.fields = read_fields(event.field_event, table)
        elif event.type == binlog.ROW:
            progress.rows += emit_row_changes(event.row_event, progress.fields, table, primary_key)
        elif event.type == binlog.VGTID:
            shard_gtid = find_shard_gtid(event.vgtid, database, shard)
            if shard_gtid is not None and shard_gtid.gtid:
                progress.latest_position = shard_gtid.gtid
                # Checked for every event, because one response can carry several positions
                progress.is_watching = progress.is_watching or shard_gtid.gtid == stop_position
            progress.cursor = apply_vgtid(progress.cursor, event.vgtid, database, table, shard)
        elif event.type == binlog.LASTPK:
            progress.cursor = apply_last_primary_key(progress.cursor, event.last_p_k_event, table)
        elif event.type == binlog.COPY_COMPLETED:
            # Only now is the table up to date as of the newest position VStream reported
            progress.cursor = Cursor(progress.latest_position, None, False)
            is_copy_completed = True
    return is_copy_completed


def classify_stream_error(error: grpc.RpcError, table: str, shard: str) -> tuple:
    """
    Decide what a gRPC error means for the running pass.
    Args:
        error: The gRPC error raised while reading the stream.
        table: The table being synced, for error messages.
        shard: The shard being synced, for error messages.
    Returns:
        (outcome, detail): "timeout" when the pass reached its time limit, or "interrupted" with a description
        of a transient error.
    Raises:
        RuntimeError: if PlanetScale no longer has the saved position, or the error is permanent.
    """
    details = error.details() or ""
    if error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
        return __OUTCOME_TIMEOUT, ""
    if __BINLOG_EXPIRED_TEXT in details:
        raise RuntimeError(
            f"PlanetScale no longer has the saved position for table '{table}' shard '{shard}' "
            "(its binlogs expired). Run Resync All Data so the tables are copied again."
        ) from error
    if is_transient_grpc(error):
        return __OUTCOME_INTERRUPTED, f"{error.code().name}: {details}"
    raise RuntimeError(
        f"VStream failed for table '{table}' shard '{shard}': {error.code().name}: {details}"
    ) from error


def stream_pass(
    stub,
    configuration: dict,
    table: str,
    shard: str,
    cursor: Cursor,
    stop_position: str,
    primary_key: list,
    checkpointer: Checkpointer,
) -> tuple:
    """
    Stream from the cursor until the copy completes, the stream moves past stop_position, the pass times out, or
    the connection drops. Progress is saved in the cursor as it goes and checkpointed about once a minute.
    Args:
        stub: The VStream client stub.
        configuration: a dictionary that holds the configuration settings for the connector.
        table: The table to sync.
        shard: The shard to sync.
        cursor: Where to resume.
        stop_position: The shard position the pass should bring the table up to.
        primary_key: The primary key column names.
        checkpointer: Saves the cursor and checkpoints state while the pass runs.
    Returns:
        (cursor, row changes, outcome, detail). The outcome is "copy completed", "caught up", "timeout" or
        "interrupted" (a transient error, described in detail).
    Raises:
        RuntimeError: if PlanetScale rejects the stream or no longer has the saved position.
    """
    database = configuration["database"]
    request = build_request(configuration, table, shard, cursor)
    stream = stub.VStream(
        request, metadata=build_auth_metadata(configuration), timeout=__READ_TIMEOUT_SECONDS
    )
    progress = PassProgress(cursor)
    last_logged_time = time.monotonic()
    outcome, detail = __OUTCOME_CAUGHT_UP, ""
    try:
        # The stream is read one response at a time, so memory stays flat however large the table is
        for response in stream:
            is_copy_completed = process_response(
                response, progress, database, table, shard, primary_key, stop_position
            )
            checkpointer.save(table, shard, progress.cursor)
            if time.monotonic() - last_logged_time >= __PROGRESS_LOG_SECONDS:
                log.info(f"[{table}:{shard}] {progress.rows} row change(s) so far in this pass")
                last_logged_time = time.monotonic()
            if is_copy_completed:
                outcome = __OUTCOME_COPY_COMPLETED
                break
            is_past_stop = progress.cursor.position != stop_position
            if progress.is_watching and not progress.cursor.is_copy_in_progress and is_past_stop:
                break
        else:
            # The server closed the stream before the copy finished or the stop position was reached
            outcome = __OUTCOME_INTERRUPTED
            detail = "the stream ended before the table was up to date"
    except grpc.RpcError as error:
        outcome, detail = classify_stream_error(error, table, shard)
    finally:
        stream.cancel()
    return progress.cursor, progress.rows, outcome, detail


def handle_stalled_passes(table: str, shard: str, outcome: str, detail: str, stalled_passes: int):
    """
    Give up on a table and shard whose passes keep ending without progress.
    A stream that keeps dropping is an error; one that keeps timing out is a warning and the next sync continues.
    Args:
        table: The table being synced.
        shard: The shard being synced.
        outcome: How the last pass ended.
        detail: The description of the transient error, if the pass was interrupted.
        stalled_passes: How many passes in a row made no progress.
    Raises:
        RuntimeError: if the stream keeps dropping.
    """
    if outcome == __OUTCOME_INTERRUPTED:
        raise RuntimeError(
            f"The VStream for table '{table}' shard '{shard}' keeps dropping without making progress "
            f"({detail}); giving up after {stalled_passes} attempts"
        )
    message = (
        f"Table '{table}' shard '{shard}' made no progress in {stalled_passes} passes of "
        f"{__READ_TIMEOUT_SECONDS}s; its saved position is kept and the next sync continues"
    )
    log.warning(message)
    op.warning(message)


def pause_before_next_pass(table: str, shard: str, outcome: str, detail: str, stalled_passes: int):
    """
    Log why another pass follows, and back off first if the stream was interrupted.
    Args:
        table: The table being synced.
        shard: The shard being synced.
        outcome: How the last pass ended.
        detail: The description of the transient error, if the pass was interrupted.
        stalled_passes: How many passes in a row made no progress, which sets the backoff delay.
    """
    if outcome == __OUTCOME_INTERRUPTED:
        delay = __RETRY_BASE_SECONDS * __BACKOFF_MULTIPLIER**stalled_passes
        log.warning(f"[{table}:{shard}] stream interrupted ({detail}); resuming in {delay}s")
        time.sleep(delay)
    else:
        log.info(
            f"[{table}:{shard}] pass timed out after {__READ_TIMEOUT_SECONDS}s; "
            "continuing from the saved cursor"
        )


def sync_shard(
    stub,
    configuration: dict,
    table: str,
    shard: str,
    cursor: Cursor,
    primary_key: list,
    checkpointer: Checkpointer,
) -> int:
    """
    Bring one table's shard up to the position it has when the pass starts. A copy or catch-up that times out or
    drops after making progress carries on from the saved cursor, however many passes that takes.
    Args:
        stub: The VStream client stub.
        configuration: a dictionary that holds the configuration settings for the connector.
        table: The table to sync.
        shard: The shard to sync.
        cursor: Where to resume.
        primary_key: The primary key column names.
        checkpointer: Saves the cursor and checkpoints state.
    Returns:
        The number of row changes applied.
    """
    total_rows = 0
    stalled_passes = 0
    while True:
        stop_position = peek_position(stub, configuration, table, shard)
        is_up_to_date = not cursor.is_copy_in_progress and cursor.position == stop_position
        if is_up_to_date and cursor.position:
            return total_rows
        cursor_before = cursor.to_state()
        cursor, rows, outcome, detail = stream_pass(
            stub, configuration, table, shard, cursor, stop_position, primary_key, checkpointer
        )
        total_rows += rows
        checkpointer.save(table, shard, cursor)
        if outcome in (__OUTCOME_CAUGHT_UP, __OUTCOME_COPY_COMPLETED):
            return total_rows
        has_made_progress = rows > 0 or cursor.to_state() != cursor_before
        stalled_passes = 0 if has_made_progress else stalled_passes + 1
        if stalled_passes >= __MAX_STALLED_PASSES:
            handle_stalled_passes(table, shard, outcome, detail, stalled_passes)
            return total_rows
        pause_before_next_pass(table, shard, outcome, detail, stalled_passes)


def sync_table(
    stub,
    configuration: dict,
    table: str,
    primary_key: list,
    shards: list,
    checkpointer: Checkpointer,
):
    """
    Sync every shard of one table, truncating it first if this is its first sync, then checkpoint if it changed.
    Args:
        stub: The VStream client stub.
        configuration: a dictionary that holds the configuration settings for the connector.
        table: The table to sync.
        primary_key: The primary key column names.
        shards: The shard names of the keyspace.
        checkpointer: Saves the cursors and checkpoints state.
    """
    table_state = checkpointer.state["tables"].setdefault(table, {"shards": {}})
    cursors = {shard: Cursor.from_state(table_state["shards"].get(shard)) for shard in shards}
    is_first_sync = not any(cursor.has_progress() for cursor in cursors.values())
    if is_first_sync:
        # The 'truncate' operation marks every existing row of the destination table as deleted. It runs once per
        # table, before any shard is read, so that reading shard 2 cannot delete what shard 1 just copied.
        op.truncate(table=table)
    rows = 0
    for shard in shards:
        rows += sync_shard(
            stub, configuration, table, shard, cursors[shard], primary_key, checkpointer
        )
    log.info(
        f"[{table}] {'first sync' if is_first_sync else 'incremental sync'}: {rows} row change(s)"
    )
    # Multi-table syncs checkpoint after each table that was copied or changed. A table with no changes only
    # moves its position forward, which is saved with the next checkpoint instead of checkpointing once per table.
    if is_first_sync or rows > 0:
        checkpointer.flush()


def prepare_state(state: dict, database: str, table_names: list):
    """
    Make the state ready for this sync: replace state that belongs to another database, and forget dropped tables.
    Saved positions only make sense for the database they came from.
    Args:
        state: The state dictionary from the previous sync, updated in place.
        database: The PlanetScale database name.
        table_names: The names of the tables that will be replicated.
    """
    source = {"database": database}
    if state.get("source") not in (None, source):
        log.warning(
            "Saved state is for a different database or an older version of this connector; "
            "starting a fresh copy of every table"
        )
        state.clear()
    state["source"] = source
    state.setdefault("tables", {})
    for dropped in [name for name in state["tables"] if name not in table_names]:
        log.info(f"Table '{dropped}' is no longer replicated; dropping its saved position")
        del state["tables"][dropped]


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    validate_configuration(configuration=configuration)
    _, tables, skipped = discover_source(configuration)
    requested = parse_table_list(configuration.get("tables"))
    tables, _ = select_tables(tables, skipped, requested, configuration["database"])
    definitions = []
    for table, key_columns in tables.items():
        definition = {"table": table, "primary_key": [name for name, _ in key_columns]}
        # Key types are declared because a sync whose first operation is a delete has no upsert to infer them from
        # (the SDK fails with "Primary key with unknown data type"). Other columns are left out on purpose, so the
        # SDK infers them and the schema can evolve with the source.
        key_types = {}
        for name, column_type in key_columns:
            sdk_type = primary_key_sdk_type(column_type)
            if sdk_type:
                key_types[name] = sdk_type
        if key_types:
            definition["columns"] = key_types
        definitions.append(definition)
    return definitions


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
    log.warning("Example: Databases : PlanetScale")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    # Discover the shards and tables on every sync, so that tables added to or dropped from the database are picked up.
    # Without the "tables" setting every base table with a primary key is replicated.
    database = configuration["database"]
    shards, tables, skipped = discover_source(configuration)
    if not shards:
        raise RuntimeError(f"No shards found for database '{database}'")
    requested = parse_table_list(configuration.get("tables"))
    tables, skipped = select_tables(tables, skipped, requested, database)
    if skipped:
        log.warning(describe_skipped_tables(skipped))
        op.warning(describe_skipped_tables(skipped))
    log.info(f"Replicating {len(tables)} table(s) across shard(s) {shards}")

    # The state holds one cursor (position and last primary key) per table and shard, no row keys:
    # {"source": {"database": ...},
    #  "tables": {"<table>": {"shards": {"<shard>": {"position": "<GTID set>", "last_primary_key": "<base64 or null>"}}}}}
    prepare_state(state, database, list(tables))
    checkpointer = Checkpointer(state, __MIN_CHECKPOINT_SECONDS)

    channel, stub = open_channel(configuration)
    try:
        for table, key_columns in tables.items():
            primary_key = [name for name, _ in key_columns]
            sync_table(stub, configuration, table, primary_key, shards, checkpointer)
    finally:
        channel.close()

    # The final checkpoint is always written, so that Fivetran knows the sync finished even when nothing changed.
    checkpointer.flush(is_forced=True)


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
