"""AVEVA PI Connector for Fivetran Connector SDK.

Syncs data from AVEVA PI (formerly OSIsoft PI) via the PI SQL Data Access Server
(PI SQL DAS) using an ODBC connection. Supports full reimport and cursor-based
incremental syncs with time-windowed queries and automatic window backoff.

Prerequisites:
  - AVEVA PI SQL Client (ODBC driver) installed on the host machine.
    Download from: https://customers.osisoft.com (AVEVA Customer Portal).
  - pyodbc Python library (see requirements.txt).
  - PI SQL DAS service running and reachable on the configured host/port.

See the Technical Reference: https://fivetran.com/docs/connectors/connector-sdk/technical-reference
See Best Practices:          https://fivetran.com/docs/connectors/connector-sdk/best-practices
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone, timedelta
from typing import Generator

import pyodbc

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_NAME = "AVEVA PI"

# Maximum retry attempts for transient connection failures before raising
MAX_RETRIES = 3

# Default PI SQL DAS port
DEFAULT_PI_PORT = 5461

# Column name that flags a table as supporting cursor-based incremental sync
INCREMENTAL_COLUMN = "Modified"

# The Archive table uses a different incremental column than standard PI tables
ARCHIVE_TABLE = "Archive"
ARCHIVE_INCREMENTAL_COLUMN = "TimeStamp"

# Only these two columns contribute to the hash ID for Archive rows
ARCHIVE_HASH_COLUMNS: frozenset[str] = frozenset({"AttributeID", "TimeStamp"})

# Starting size of the time window for incremental queries (days)
INITIAL_WINDOW_DAYS = 365

# If the adaptive window shrinks below this threshold, give up and surface the error
MIN_WINDOW_HOURS = 1

# Roll back the start cursor by this many hours for Archive to catch late-arriving records
LATE_ARRIVAL_ROLLBACK_HOURS = 2

# Emit a checkpoint every N rows during full reimport to signal liveness to Fivetran
CHECKPOINT_INTERVAL = 10_000

# Error message substrings that identify an authentication failure (not worth retrying)
_AUTH_ERROR_PATTERNS = [
    "403 Forbidden",
    "401 Unauthorized",
    "Unknown PI SQL DAS",
    "Connection failed. Server returned HTTP response code: 400",
    "Please make sure the PI SQL DAS service is running",
]

# Maps AVEVA PI SQL type names to Fivetran Connector SDK type strings.
# Types not present in this map are skipped with a warning.
_PI_TYPE_MAP: dict[str, str] = {
    "AnsiString":   "STRING",
    "AnsiStringCs": "STRING",
    "TimeSpan":     "STRING",
    "Guid":         "STRING",
    "String":       "STRING",
    "StringCs":     "STRING",
    "Variant":      "STRING",
    "Boolean":      "BOOLEAN",
    "DateTime":     "UTC_DATETIME",
    "Double":       "DOUBLE",
    "Single":       "DOUBLE",
    "Int8":         "SHORT",
    "Int16":        "SHORT",
    "UInt8":        "SHORT",
    "Int32":        "INT",
    "UInt16":       "INT",
    "Int64":        "LONG",
    "UInt32":       "LONG",
    "UInt64":       "LONG",
}


# ---------------------------------------------------------------------------
# Configuration validation
# ---------------------------------------------------------------------------

def validate_configuration(configuration: dict) -> None:
    """
    Validate the configuration dictionary to ensure it contains all required keys.
    Raises ValueError if any required key is absent.
    Args:
        configuration: a dictionary containing connection settings for the connector.
    """
    for key in ("host", "username", "password"):
        if key not in configuration:
            raise ValueError(f"Missing required configuration key: '{key}'")


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

def _build_conn_str(configuration: dict) -> str:
    """Build the ODBC connection string from configuration values."""
    # Users may need to change 'odbc_driver' to match their PI SQL Client installation.
    # Common values: "PI ODBC", "PI SQL Client"
    driver   = configuration.get("odbc_driver", "PI ODBC")
    host     = configuration["host"]
    port     = int(configuration.get("port", DEFAULT_PI_PORT))
    database = configuration.get("database", "")
    username = configuration["username"]
    password = configuration["password"]
    return (
        f"DRIVER={{{driver}}};"
        f"Server={host};"
        f"Port={port};"
        f"Database={database};"
        f"UID={username};"
        f"PWD={password};"
        f"Time Zone=UTC;"
    )


def get_connection(configuration: dict) -> pyodbc.Connection:
    """
    Open a pyodbc connection to PI SQL DAS, retrying up to MAX_RETRIES times.

    Raises ValueError immediately on authentication errors (no retry — the
    user must fix credentials before the next sync).
    Raises ConnectionError after MAX_RETRIES consecutive transient failures.
    """
    conn_str = _build_conn_str(configuration)
    last_exc: Exception = RuntimeError("No connection attempt made")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            conn = pyodbc.connect(conn_str, autocommit=True)
            log.info(f"Connected to {SOURCE_NAME}")
            return conn
        except pyodbc.Error as exc:
            msg = str(exc)
            if any(p in msg for p in _AUTH_ERROR_PATTERNS):
                raise ValueError(f"Authentication failed for {SOURCE_NAME}: {msg}") from exc
            last_exc = exc
            log.warning(f"Connection attempt {attempt}/{MAX_RETRIES} failed: {msg}")

    raise ConnectionError(
        f"Could not connect to {SOURCE_NAME} after {MAX_RETRIES} attempts."
    ) from last_exc


# ---------------------------------------------------------------------------
# Metadata discovery
# ---------------------------------------------------------------------------

def _map_type(pi_type: str) -> str | None:
    """Map a PI SQL type name to a Fivetran SDK type. Returns None for unknown types."""
    mapped = _PI_TYPE_MAP.get(pi_type)
    if mapped is None:
        log.warning(f"Unsupported PI type '{pi_type}' — column will be skipped.")
    return mapped


def discover_tables(conn: pyodbc.Connection) -> dict[str, list[str]]:
    """
    Return all TABLE-type objects grouped by schema: {schema_name: [table_name, ...]}.
    Equivalent to JDBC DatabaseMetaData.getTables() in the original Java connector.
    """
    tables: dict[str, list[str]] = {}
    for row in conn.cursor().tables(tableType="TABLE"):
        tables.setdefault(row.table_schem, []).append(row.table_name)
    return tables


def get_columns(conn: pyodbc.Connection, table: str, schema: str) -> list[dict]:
    """
    Return [{"name": column_name, "type": fivetran_type}, ...] for each supported column.
    Columns with PI types not in _PI_TYPE_MAP are omitted (a warning is logged).
    """
    columns = []
    for row in conn.cursor().columns(table=table, schema=schema):
        fivetran_type = _map_type(row.type_name)
        if fivetran_type:
            columns.append({"name": row.column_name, "type": fivetran_type})
    return columns


def get_primary_keys(conn: pyodbc.Connection, table: str, schema: str) -> list[str]:
    """Return primary key column names for the given table."""
    pks = []
    try:
        for row in conn.cursor().primaryKeys(table=table, schema=schema):
            pks.append(row.column_name)
    except pyodbc.Error as exc:
        log.warning(f"Could not fetch primary keys for {schema}.{table}: {exc}")
    return pks


# ---------------------------------------------------------------------------
# SQL query helpers
# ---------------------------------------------------------------------------

def _dq(name: str) -> str:
    """Double-quote a SQL identifier, escaping any embedded double-quotes."""
    return '"' + name.replace('"', '""') + '"'


def _fmt_dt(dt: datetime) -> str:
    """Format a datetime as a PI SQL timestamp literal (UTC, 'YYYY-MM-DD HH:MM:SS')."""
    return "'" + dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S") + "'"


def _col_list(columns: list[dict]) -> str:
    """Build a comma-separated, double-quoted column list for use in SELECT."""
    return ", ".join(_dq(c["name"]) for c in columns)


# ---------------------------------------------------------------------------
# Row streaming (generator-based to avoid loading large tables into memory)
# ---------------------------------------------------------------------------

def _iter_rows(cursor: pyodbc.Cursor, columns: list[dict]) -> Generator[dict, None, None]:
    """
    Yield one {column_name: value} dict per cursor row.
    pyodbc returns naive datetime objects; this attaches UTC timezone info to them.
    """
    col_names = [c["name"] for c in columns]
    for row in cursor:
        record: dict = {}
        for name, value in zip(col_names, row):
            # Attach UTC tz info to naive datetimes returned by the ODBC driver
            if isinstance(value, datetime) and value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            record[name] = value
        yield record


def stream_full_table(
    conn: pyodbc.Connection, table: str, schema: str, columns: list[dict]
) -> Generator[dict, None, None]:
    """Stream every row from a table via a full SELECT (no WHERE clause)."""
    query = f"SELECT {_col_list(columns)} FROM {_dq(schema)}.{_dq(table)}"
    cursor = conn.cursor()
    cursor.execute(query)
    yield from _iter_rows(cursor, columns)


def stream_epoch_records(
    conn: pyodbc.Connection,
    table: str,
    schema: str,
    columns: list[dict],
    ts_col: str,
) -> Generator[dict, None, None]:
    """
    Stream Archive rows where ts_col equals Unix epoch (1970-01-01 00:00:00 UTC).
    This seeds the Archive table on the first sync so no historical records are missed.
    """
    epoch_literal = _fmt_dt(datetime.fromtimestamp(0, tz=timezone.utc))
    query = (
        f"SELECT {_col_list(columns)} FROM {_dq(schema)}.{_dq(table)} "
        f"WHERE {ts_col} = {epoch_literal}"
    )
    cursor = conn.cursor()
    cursor.execute(query)
    yield from _iter_rows(cursor, columns)


def stream_incremental_window(
    conn: pyodbc.Connection,
    table: str,
    schema: str,
    columns: list[dict],
    incremental_col: str,
    since: datetime,
    until: datetime,
) -> Generator[dict, None, None]:
    """Stream rows where incremental_col is within the closed interval [since, until]."""
    query = (
        f"SELECT {_col_list(columns)} FROM {_dq(schema)}.{_dq(table)} "
        f"WHERE {incremental_col} >= {_fmt_dt(since)} "
        f"AND {incremental_col} <= {_fmt_dt(until)}"
    )
    cursor = conn.cursor()
    cursor.execute(query)
    yield from _iter_rows(cursor, columns)


# ---------------------------------------------------------------------------
# Hash ID generation (for tables without a natural primary key)
# ---------------------------------------------------------------------------

def generate_hash_id(record: dict, hash_columns: list[str]) -> str:
    """
    Produce a deterministic hex string from the values of the specified columns.
    Used as _fivetran_id when a table has no natural primary key.

    Args:
        record: the row dict.
        hash_columns: column names whose values contribute to the hash.
    Returns:
        A 32-character lowercase hex string (MD5).
    """
    parts = [str(record.get(col, "")) for col in sorted(hash_columns)]
    return hashlib.md5("|".join(parts).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Per-table sync strategies
# ---------------------------------------------------------------------------

def _upsert(record: dict, dest_table: str, use_hash_id: bool, hash_columns: list[str]) -> None:
    """Inject a hash ID when needed, then upsert the record."""
    if use_hash_id:
        cols = hash_columns or list(record.keys())
        record["_fivetran_id"] = generate_hash_id(record, cols)
    op.upsert(dest_table, record)


def sync_reimport(
    conn: pyodbc.Connection,
    table: str,
    schema: str,
    columns: list[dict],
    dest_table: str,
    use_hash_id: bool,
    state: dict,
) -> None:
    """
    Full reimport: upsert every row from the source table.

    Checkpoints every CHECKPOINT_INTERVAL rows to signal liveness to Fivetran.

    Note on deletes: rows removed from PI are NOT automatically soft-deleted in the
    destination because PI does not expose a deleted-records feed. If delete reflection
    is required, enable Fivetran's truncate-and-reload mode for these tables.
    """
    log.info(f"Full reimport: {schema}.{table} → {dest_table}")
    col_names = [c["name"] for c in columns]
    row_count = 0

    for record in stream_full_table(conn, table, schema, columns):
        _upsert(record, dest_table, use_hash_id, col_names)
        row_count += 1
        # Checkpoint periodically so Fivetran knows we are making progress
        if row_count % CHECKPOINT_INTERVAL == 0:
            op.checkpoint(state)
            log.info(f"  … {row_count} rows processed")

    op.checkpoint(state)
    log.info(f"Full reimport done: {dest_table} ({row_count} rows)")


def sync_incremental(
    conn: pyodbc.Connection,
    table: str,
    schema: str,
    columns: list[dict],
    dest_table: str,
    incremental_col: str,
    use_hash_id: bool,
    hash_columns: list[str],
    state: dict,
) -> None:
    """
    Cursor-based incremental sync with adaptive time-window backoff.

    Algorithm:
      1. Read the last cursor from state (defaults to Unix epoch on first sync).
      2. For the Archive table on first sync: seed epoch records before the
         windowed loop (epoch records have a special timestamp that falls outside
         the normal incremental range).
      3. For the Archive table on subsequent syncs: roll back the start cursor by
         LATE_ARRIVAL_ROLLBACK_HOURS to recapture records that arrived late.
      4. Walk from start to now in windows of up to INITIAL_WINDOW_DAYS.
      5. On a transient query failure: halve the window and retry the same slice.
         If the window shrinks below MIN_WINDOW_HOURS, raise immediately.
      6. Checkpoint state (with updated cursor) after every successfully processed window.
    """
    cursor_key = f"{schema}.{table}"
    cursors = state.setdefault("incremental_cursor", {})
    start_str = cursors.get(cursor_key)
    is_first_sync = start_str is None
    start = (
        datetime.fromisoformat(start_str)
        if start_str
        else datetime.fromtimestamp(0, tz=timezone.utc)
    )
    now = datetime.now(timezone.utc)

    # Archive first sync: seed epoch-timestamp records before the windowed pass
    if table == ARCHIVE_TABLE and is_first_sync:
        log.info(f"Archive: seeding epoch records for {schema}.{table}")
        seed_count = 0
        for record in stream_epoch_records(conn, table, schema, columns, incremental_col):
            _upsert(record, dest_table, use_hash_id, hash_columns)
            seed_count += 1
        log.info(f"Archive epoch seed complete ({seed_count} rows)")

    # Archive subsequent syncs: roll back to catch late-arriving records
    if table == ARCHIVE_TABLE and not is_first_sync:
        start = start - timedelta(hours=LATE_ARRIVAL_ROLLBACK_HOURS)
        log.info(f"Archive: adjusted start to {start.isoformat()} (late-arrival rollback)")

    window = timedelta(days=INITIAL_WINDOW_DAYS)
    log.info(
        f"Incremental: {schema}.{table} | "
        f"from {start.isoformat()} | initial window={window.days}d"
    )

    while start < now:
        end = min(start + window, now)
        max_seen: datetime | None = None
        row_count = 0

        try:
            for record in stream_incremental_window(
                conn, table, schema, columns, incremental_col, start, end
            ):
                _upsert(record, dest_table, use_hash_id, hash_columns)
                row_count += 1
                ts = record.get(incremental_col)
                if isinstance(ts, datetime) and (max_seen is None or ts > max_seen):
                    max_seen = ts

            # Advance cursor: take the max of the previous cursor and the latest
            # timestamp seen in this window. If the window was empty, advance to end.
            prev_str = cursors.get(cursor_key)
            prev_cursor = datetime.fromisoformat(prev_str) if prev_str else start
            new_cursor = max(prev_cursor, max_seen) if max_seen else end
            cursors[cursor_key] = new_cursor.isoformat()

            # Save progress after each successful window
            op.checkpoint(state)
            log.info(
                f"  Window {start.isoformat()} → {end.isoformat()}: "
                f"{row_count} rows | cursor → {new_cursor.isoformat()}"
            )
            start = end

        except Exception as exc:
            msg = str(exc)

            # Authentication errors are not transient — surface immediately
            if any(p in msg for p in _AUTH_ERROR_PATTERNS):
                raise

            # Transient query error: halve the window and retry the same time slice
            window = window / 2
            if window < timedelta(hours=MIN_WINDOW_HOURS):
                log.warning(
                    f"Window for {schema}.{table} is below the {MIN_WINDOW_HOURS}h minimum "
                    f"and cannot be reduced further. Last error: {msg}"
                )
                raise RuntimeError(
                    f"Incremental sync failed for {schema}.{table}: {msg}"
                ) from exc

            log.warning(
                f"Query failed [{start.isoformat()} → {end.isoformat()}]. "
                f"Halving window to "
                f"{int(window.total_seconds() // 3600)}h. Error: {msg}"
            )


# ---------------------------------------------------------------------------
# Fivetran Connector SDK entry points
# ---------------------------------------------------------------------------

def schema(configuration: dict) -> list[dict]:
    """
    Discover AVEVA PI tables and return their schema for Fivetran.

    Destination table name convention: "<source_schema>_<table_name>"
    This preserves the source schema namespace within a single Fivetran destination
    schema, matching the behaviour of the original Fivetran-native AVEVA PI connector.

    Tables without a natural primary key receive a synthetic '_fivetran_id' primary key.
    The value is generated at sync time from an MD5 hash of the relevant column values.

    Args:
        configuration: a dictionary containing connection settings.
    Returns:
        A list of table definitions for Fivetran.
    """
    validate_configuration(configuration)
    conn = get_connection(configuration)
    try:
        result = []
        for src_schema, tables in discover_tables(conn).items():
            for table_name in tables:
                pks = get_primary_keys(conn, table_name, src_schema)
                columns = get_columns(conn, table_name, src_schema)
                dest_table = f"{src_schema}_{table_name}"

                table_def = {
                    "table": dest_table,
                    "primary_key": pks if pks else ["_fivetran_id"],
                    "columns": {c["name"]: c["type"] for c in columns},
                }
                result.append(table_def)
                log.info(
                    f"Discovered {src_schema}.{table_name} → {dest_table} "
                    f"(PK: {table_def['primary_key']})"
                )
        return result
    finally:
        conn.close()


def update(configuration: dict, state: dict) -> None:
    """
    Main sync function. Called by Fivetran on every sync run.

    Iterates all tables visible in PI SQL DAS and applies the appropriate strategy:
      - Tables with a 'Modified' column        → incremental (cursor + windowed queries)
      - The 'Archive' table (with 'TimeStamp') → incremental with late-arrival rollback
      - All other tables                       → full reimport every sync

    Args:
        configuration: a dictionary containing connection settings.
        state: a dictionary containing cursor state from the previous run.
               Empty on the first sync or after a full re-sync.
    """
    validate_configuration(configuration)
    conn = get_connection(configuration)
    try:
        for src_schema, tables in discover_tables(conn).items():
            for table_name in tables:
                pks = get_primary_keys(conn, table_name, src_schema)
                columns = get_columns(conn, table_name, src_schema)

                if not columns:
                    log.warning(f"Skipping {src_schema}.{table_name}: no supported columns.")
                    continue

                col_name_set = {c["name"] for c in columns}
                col_name_list = [c["name"] for c in columns]
                use_hash_id = not pks
                dest_table = f"{src_schema}_{table_name}"

                # Determine the incremental column and which columns feed the hash ID
                if table_name == ARCHIVE_TABLE and ARCHIVE_INCREMENTAL_COLUMN in col_name_set:
                    incremental_col: str | None = ARCHIVE_INCREMENTAL_COLUMN
                    hash_columns = [
                        c["name"] for c in columns if c["name"] in ARCHIVE_HASH_COLUMNS
                    ]
                elif INCREMENTAL_COLUMN in col_name_set:
                    incremental_col = INCREMENTAL_COLUMN
                    hash_columns = col_name_list
                else:
                    incremental_col = None
                    hash_columns = col_name_list

                if incremental_col:
                    sync_incremental(
                        conn, table_name, src_schema, columns, dest_table,
                        incremental_col, use_hash_id, hash_columns, state,
                    )
                else:
                    sync_reimport(
                        conn, table_name, src_schema, columns, dest_table,
                        use_hash_id, state,
                    )
    finally:
        conn.close()


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the
# command line or IDE. The recommended way to test is: fivetran debug
if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
