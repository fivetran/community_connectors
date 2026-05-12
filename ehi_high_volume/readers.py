"""Readers for SQL Server table pagination."""

# For encoding binary cursor values as ASCII-safe strings in state
import base64

# For type-checking and serialising date/time column values
from datetime import datetime, date, time as _time

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# Tunable batch and concurrency settings
from constants import BATCH_SIZE

# Connection pool for managing pyodbc connections to SQL Server
from client import ConnectionPool

# Schema detection and data models
from models import TableSchema

_DATETIME_TYPES = (datetime, date, _time)

# Keep string PK tiebreaks to types with predictable SQL ordering.
_TIEBREAK_ELIGIBLE_STR_TYPES = frozenset({"varchar", "nvarchar", "char", "nchar", "text", "ntext"})

# SQL Server stores 7 decimals for these; Python datetime keeps only 6.
_DATETIME2_SQL_TYPES = frozenset({"datetime2", "datetimeoffset"})


def _build_select_columns(selectable_columns) -> str:
    """
    Build a comma-separated SELECT column list that preserves datetime2/datetimeoffset precision.

    SQL Server datetime2(7)/datetimeoffset(7) carry 7 fractional digits, but pyodbc materialises
    them as Python datetime which only holds 6 (microseconds). Wrapping these columns in
    CONVERT(NVARCHAR(50), col, 127) makes SQL Server emit the ISO 8601 string directly, so the
    7th digit is preserved end-to-end.

    Args:
        selectable_columns: iterable of ColumnInfo objects representing the columns to select.

    Returns:
        A comma-separated string of SQL column expressions suitable for use in a SELECT clause.
    """
    return ", ".join(
        (
            f"CONVERT(NVARCHAR(50), [{column.name}], 127) AS [{column.name}]"
            if column.sql_type.lower() in _DATETIME2_SQL_TYPES
            else f"[{column.name}]"
        )
        for column in selectable_columns
    )


def convert_value(value, python_type):
    """Convert a raw pyodbc value into an SDK-friendly scalar.

    Handles None pass-through, date/time serialisation to ISO 8601 strings,
    numeric coercions, and binary encoding to base64 ASCII.

    Args:
        value: The raw value returned by pyodbc for a column.
        python_type: The target Python type (str, int, float, bool, or bytes)
            as determined by the column's SQL type mapping.

    Returns:
        The converted scalar value, or None if the input is None or the
        python_type is unrecognised.
    """
    if value is None:
        return None

    if python_type is str:
        if isinstance(value, _DATETIME_TYPES):
            return value.isoformat()
        return str(value)

    if python_type is int:
        return int(value)
    if python_type is float:
        return float(value)
    if python_type is bool:
        return bool(value)

    if python_type is bytes:
        if isinstance(value, (bytes, bytearray, memoryview)):
            return base64.b64encode(bytes(value)).decode("ascii")
        return str(value)

    return None


def _get_tiebreak_primary_key_column(
    schema: TableSchema,
    primary_key_columns: list,
    use_primary_key_tiebreak: bool,
):
    """
    Return the single PK column that can safely break replication-key ties.

    Only integer and eligible string PKs are accepted; composite, computed, or
    unsupported-type PKs cause the tiebreak to be disabled with a warning.

    Args:
        schema: table schema used for column metadata and log messages.
        primary_key_columns: list of PK column names for the table.
        use_primary_key_tiebreak: whether the caller has requested tiebreak ordering.

    Returns:
        The name of the tiebreak PK column, or None if tiebreaking is not possible.
    """
    if not use_primary_key_tiebreak or len(primary_key_columns) != 1:
        if use_primary_key_tiebreak and len(primary_key_columns) > 1:
            log.warning(
                f"{schema.table_name}: tiebreak requested but table has a "
                f"{len(primary_key_columns)}-column composite PK — tiebreak disabled"
            )
        return None

    primary_key_name = primary_key_columns[0]
    for column in schema.columns:
        if column.name != primary_key_name:
            continue

        # Computed columns are excluded from selectable_columns so they cannot appear in the
        # SELECT list — using one as a tiebreak would reference an unselected column in ORDER BY.
        if column.is_computed:
            log.warning(
                f"{schema.table_name}: PK column '{primary_key_name}' is a computed column "
                "and cannot be used as a tiebreak — tiebreak disabled"
            )
            return None

        # Only numeric and simple string PKs are safe for `pk > ?`.
        if column.python_type is int:
            return primary_key_name
        if column.python_type is str and column.sql_type.lower() in _TIEBREAK_ELIGIBLE_STR_TYPES:
            return primary_key_name

        log.warning(
            f"{schema.table_name}: PK column '{primary_key_name}' has SQL type '{column.sql_type}' "
            "which is not eligible for tiebreaking — tiebreak disabled"
        )
        return None
    return None


class ReplicationKeysetReader:
    """
    Read a table using replication-key keyset pagination.

    Uses `(replication_key, primary_key)` when possible so duplicate timestamps
    do not skip rows. Yields `(batch, replication_marker, primary_key_marker)`.
    """

    def __init__(
        self,
        pool: ConnectionPool,
        schema: TableSchema,
        last_seen_value,
        batch_size: int = BATCH_SIZE,
        use_primary_key_tiebreak: bool = True,
        primary_key_columns: list = None,
        last_seen_primary_key=None,
    ) -> None:
        """
        Set up the reader with the connection pool, table schema, and resume cursors.

        Args:
            pool: shared connection pool for SQL Server.
            schema: table schema including column info and replication key.
            last_seen_value: last committed replication key value (None for a fresh sync).
            batch_size: number of rows to fetch per query page.
            use_primary_key_tiebreak: enable composite (replication_key, pk) ordering to
                handle duplicate replication key values correctly.
            primary_key_columns: list of PK column names for tiebreak ordering.
            last_seen_primary_key: last committed PK tiebreak value (for mid-page resume).
        """
        self._pool = pool
        self._schema = schema
        self._last_seen = last_seen_value
        self._batch_size = batch_size
        self._primary_key_columns = primary_key_columns or []
        self._last_seen_primary_key = last_seen_primary_key
        self._tiebreak_primary_key_column = _get_tiebreak_primary_key_column(
            schema,
            self._primary_key_columns,
            use_primary_key_tiebreak,
        )

    def read_batches(self):
        """
        Read SQL Server rows using replication-key keyset pagination.

        Uses self._last_seen for first sync vs incremental/resume behaviour.
        Uses self._last_seen_primary_key when a PK tiebreak cursor is available.

        Yields:
            A tuple of (batch, replication_marker, primary_key_marker), where batch
            is a list of records ready for upsert, replication_marker is the last
            replication key value in the batch, and primary_key_marker is the last
            PK tiebreak value in the batch.
        """
        replication_key_column = self._schema.replication_key.name
        selectable_columns = self._schema.selectable_columns
        # _build_select_columns wraps any datetime2/datetimeoffset columns (including the
        # replication key, if applicable) in CONVERT(NVARCHAR(50), ..., 127) so the full
        # 7-digit precision is preserved both in the row data and in the cursor value.
        select_with_cursor_sql = _build_select_columns(selectable_columns)
        table_sql = f"[{self._schema.schema_name}].[{self._schema.table_name}]"
        tiebreak_primary_key_column = self._tiebreak_primary_key_column
        pagination_clause = (
            f"OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY OPTION (FAST {self._batch_size})"
        )

        # next() returns the first matching replication-key index, or None if not found.
        replication_key_index = next(
            (
                index
                for index, column in enumerate(selectable_columns)
                if column.name == replication_key_column
            ),
            None,
        )
        if replication_key_index is None:
            raise ValueError(
                f"{self._schema.table_name}: replication key '{replication_key_column}' is not in "
                "selectable_columns (may be a computed column). Set incremental_column "
                "in configuration.json to a non-computed column."
            )

        # next() returns the first matching PK column index, or None if not found.
        primary_key_index = (
            next(
                (
                    index
                    for index, column in enumerate(selectable_columns)
                    if column.name == tiebreak_primary_key_column
                ),
                None,
            )
            if tiebreak_primary_key_column
            else None
        )

        # Build the SQL variants once; the loop only changes parameters.
        if tiebreak_primary_key_column:
            # First sync: start from the earliest non-null replication key.
            sql_start = (
                f"SELECT {select_with_cursor_sql} FROM {table_sql} WITH (NOLOCK) "
                f"WHERE [{replication_key_column}] IS NOT NULL "
                f"ORDER BY [{replication_key_column}], [{tiebreak_primary_key_column}] "
                f"{pagination_clause}"
            )
            # Incremental sync: read rows after the saved replication key.
            sql_from = (
                f"SELECT {select_with_cursor_sql} FROM {table_sql} WITH (NOLOCK) "
                f"WHERE [{replication_key_column}] > ? "
                f"ORDER BY [{replication_key_column}], [{tiebreak_primary_key_column}] "
                f"{pagination_clause}"
            )
            # Resume inside duplicate timestamps using the PK as the tiebreak.
            sql_composite = (
                f"SELECT {select_with_cursor_sql} FROM {table_sql} WITH (NOLOCK) "
                f"WHERE ([{replication_key_column}] > ?) "
                f"OR ([{replication_key_column}] = ? AND [{tiebreak_primary_key_column}] > ?) "
                f"ORDER BY [{replication_key_column}], [{tiebreak_primary_key_column}] "
                f"{pagination_clause}"
            )
        else:
            # First sync: no saved cursor yet.
            sql_start = (
                f"SELECT {select_with_cursor_sql} FROM {table_sql} WITH (NOLOCK) "
                f"WHERE [{replication_key_column}] IS NOT NULL "
                f"ORDER BY [{replication_key_column}] {pagination_clause}"
            )
            # Incremental sync: continue after the saved replication key.
            sql_from = (
                f"SELECT {select_with_cursor_sql} FROM {table_sql} WITH (NOLOCK) "
                f"WHERE [{replication_key_column}] > ? "
                f"ORDER BY [{replication_key_column}] {pagination_clause}"
            )
            sql_composite = sql_from

        current_last = self._last_seen
        current_last_primary_key = self._last_seen_primary_key
        page = 0

        if not tiebreak_primary_key_column:
            log.warning(
                f"{self._schema.table_name}: no eligible single-column primary key "
                "tiebreaker found. Rows with duplicate replication-key values may be skipped."
            )

        while True:
            if current_last is None:
                # First sync or fresh full resync.
                sql, parameters = sql_start, (self._batch_size,)
            elif not tiebreak_primary_key_column or current_last_primary_key is None:
                # Incremental sync or resumed full sync without a PK tiebreak.
                sql, parameters = sql_from, (current_last, self._batch_size)
            else:
                # Incremental sync or resumed full sync with timestamp + PK cursor.
                sql = sql_composite
                parameters = (
                    current_last,
                    current_last,
                    current_last_primary_key,
                    self._batch_size,
                )

            with self._pool.acquire() as connection:
                # execute_and_fetch_with_retry runs both execute and fetchmany inside the retry
                # loop so transient errors during fetch are recovered the same as during execute.
                rows = connection.execute_and_fetch_with_retry(sql, parameters, self._batch_size)

            if not rows:
                log.fine(f"{self._schema.table_name}: page {page} returned 0 rows — done")
                return

            # Warn because NULL replication keys are permanently skipped by this cursor.
            if page == 0 and current_last is None:
                null_count_sql = (
                    f"SELECT COUNT(*) FROM {table_sql} WITH (NOLOCK) "
                    f"WHERE [{replication_key_column}] IS NULL"
                )
                with self._pool.acquire() as count_connection:
                    # COUNT(*) returns a single row — fetch_size=1 is enough.
                    count_rows = count_connection.execute_and_fetch_with_retry(
                        null_count_sql, (), 1
                    )
                    null_count = (count_rows[0][0] if count_rows else 0) or 0
                if null_count:
                    log.warning(
                        f"{self._schema.table_name}: {null_count} row(s) have a NULL "
                        f"replication key ('{replication_key_column}') and will never be synced."
                    )

            # Without a PK tiebreak, duplicate replication keys can skip rows.
            if not tiebreak_primary_key_column and page > 0 and current_last is not None:
                first_replication_key_value = convert_value(
                    rows[0][replication_key_index],
                    selectable_columns[replication_key_index].python_type,
                )
                if first_replication_key_value == current_last:
                    log.warning(
                        f"{self._schema.table_name}: duplicate replication key values at "
                        f"page boundary (value={current_last}). Rows may be skipped."
                    )

            last_replication_key_value = current_last
            last_primary_key_value = current_last_primary_key
            batch = []
            for row in rows:
                # selectable_columns -> ColumnInfo(name, sql_type, python_type, is_primary_key, is_computed)
                record = {
                    column.name: convert_value(raw_value, column.python_type)
                    for column, raw_value in zip(selectable_columns, row)
                }
                batch.append(record)

                # Convert the raw replication key value through convert_value. For datetime2/
                # datetimeoffset columns this is already a full-precision string (see
                # _build_select_columns); for other types it's normalised here.
                replication_key_value = convert_value(
                    row[replication_key_index],
                    selectable_columns[replication_key_index].python_type,
                )
                # Fallback to string so the cursor never becomes None.
                last_replication_key_value = (
                    replication_key_value
                    if replication_key_value is not None
                    else str(row[replication_key_index])
                )

                if tiebreak_primary_key_column and primary_key_index is not None:
                    primary_key_value = convert_value(
                        row[primary_key_index],
                        selectable_columns[primary_key_index].python_type,
                    )
                    if primary_key_value is not None:
                        last_primary_key_value = primary_key_value

            # Update the page cursor before returning this batch to the caller.
            current_last = last_replication_key_value
            current_last_primary_key = last_primary_key_value
            page += 1
            log.fine(
                f"{self._schema.table_name}: page {page} — "
                f"{len(batch)} rows, last_value={current_last}"
            )
            # Yield streams one batch at a time instead of loading the whole table.
            yield batch, current_last, current_last_primary_key


class PrimaryKeyOnlyKeysetReader:
    """
    Read a table by primary-key keyset pagination when no replication key exists.

    Used for full loads and resume only. It cannot detect updates without a
    replication key. Yields `(batch, primary_key_marker)`.
    """

    def __init__(
        self,
        pool: ConnectionPool,
        schema: TableSchema,
        last_seen_primary_key=None,
        batch_size: int = BATCH_SIZE,
    ) -> None:
        """
        Set up the reader with the connection pool, table schema, and resume cursor.

        Args:
            pool: shared connection pool for SQL Server.
            schema: table schema; must have exactly one primary key column.
            last_seen_primary_key: last committed PK value to resume an interrupted full sync
                (None to start from the beginning of the table).
            batch_size: number of rows to fetch per query page.
        """
        self._pool = pool
        self._schema = schema
        self._last_seen_primary_key = last_seen_primary_key
        self._batch_size = batch_size
        self._primary_key_column = schema.primary_keys[0]

    def read_batches(self):
        """
        Read SQL Server rows using single-primary-key keyset pagination.

        Uses self._last_seen_primary_key to resume an interrupted full sync.

        Yields:
            A tuple of (batch, primary_key_marker), where batch is a list of
            records ready for upsert and primary_key_marker is the last primary
            key value in the batch.
        """
        primary_key_column = self._primary_key_column
        selectable_columns = self._schema.selectable_columns
        # Wrap datetime2/datetimeoffset columns in CONVERT to preserve full 7-digit precision.
        select_column_sql = _build_select_columns(selectable_columns)
        schema_table = f"[{self._schema.schema_name}].[{self._schema.table_name}]"
        pagination_clause = (
            f"OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY OPTION (FAST {self._batch_size})"
        )

        # First sync: scan from the beginning of the PK order.
        sql_first = (
            f"SELECT {select_column_sql} FROM {schema_table} WITH (NOLOCK) "
            f"ORDER BY [{primary_key_column}] {pagination_clause}"
        )
        # Resume sync: continue after the last checkpointed PK.
        sql_next = (
            f"SELECT {select_column_sql} FROM {schema_table} WITH (NOLOCK) "
            f"WHERE [{primary_key_column}] > ? "
            f"ORDER BY [{primary_key_column}] {pagination_clause}"
        )

        # next() returns the first matching PK column index, or None if not found.
        primary_key_index = next(
            (
                index
                for index, column in enumerate(selectable_columns)
                if column.name == primary_key_column
            ),
            None,
        )
        if primary_key_index is None:
            raise ValueError(
                f"{self._schema.table_name}: PK column '{primary_key_column}' "
                "is not in selectable_columns "
                "(may be a computed column). Cannot use PK-keyset pagination."
            )
        primary_key_python_type = selectable_columns[primary_key_index].python_type
        current_last = self._last_seen_primary_key

        while True:
            if current_last is None:
                # First full sync for a table with no replication key.
                sql, parameters = sql_first, (self._batch_size,)
            else:
                # Resume an interrupted full sync from the saved PK.
                sql, parameters = sql_next, (current_last, self._batch_size)

            with self._pool.acquire() as connection:
                # execute_and_fetch_with_retry runs both execute and fetchmany inside the retry
                # loop so transient errors during fetch are recovered the same as during execute.
                rows = connection.execute_and_fetch_with_retry(sql, parameters, self._batch_size)

            if not rows:
                log.fine(f"{self._schema.table_name}: PK-keyset page returned 0 rows — done")
                return

            last_primary_key_value = current_last
            batch = []
            for row in rows:
                record = {
                    column.name: convert_value(raw_value, column.python_type)
                    for column, raw_value in zip(selectable_columns, row)
                }
                batch.append(record)

                primary_key_converted = convert_value(
                    row[primary_key_index],
                    primary_key_python_type,
                )
                if primary_key_converted is not None:
                    last_primary_key_value = primary_key_converted

            current_last = last_primary_key_value
            log.fine(
                f"{self._schema.table_name}: PK-keyset page — "
                f"{len(batch)} rows, last_primary_key={current_last}"
            )
            yield batch, current_last


class OffsetReader:
    """
    Read a table with SQL OFFSET/FETCH pagination.

    This is the slow fallback for tables with no replication key or single-column
    PK. Yields `(batch, next_offset)`.
    """

    def __init__(
        self,
        pool: ConnectionPool,
        schema: TableSchema,
        last_offset: int,
        batch_size: int = BATCH_SIZE,
    ) -> None:
        """
        Set up the reader with the connection pool, table schema, and resume offset.

        Args:
            pool: shared connection pool for SQL Server.
            schema: table schema; used for column list and optional PK ordering.
            last_offset: row offset to resume from (0 for a fresh sync).
            batch_size: number of rows to fetch per query page.
        """
        self._pool = pool
        self._schema = schema
        self._last_offset = last_offset
        self._batch_size = batch_size
        primary_key_columns = schema.primary_keys
        if primary_key_columns:
            self._order_clause = "ORDER BY " + ", ".join(
                f"[{primary_key}]" for primary_key in primary_key_columns
            )
            self._has_primary_key_order = True
        else:
            self._order_clause = "ORDER BY (SELECT NULL)"
            self._has_primary_key_order = False

    def read_batches(self):
        """
        Read SQL Server rows using OFFSET/FETCH pagination.

        Uses self._last_offset to resume from the last checkpointed row offset.

        Yields:
            A tuple of (batch, next_offset), where batch is a list of records
            ready for upsert and next_offset is the next row offset to read.
        """
        selectable_columns = self._schema.selectable_columns
        # Wrap datetime2/datetimeoffset columns in CONVERT to preserve full 7-digit precision.
        select_column_sql = _build_select_columns(selectable_columns)
        schema_table = f"[{self._schema.schema_name}].[{self._schema.table_name}]"

        if not self._has_primary_key_order:
            log.warning(
                f"{self._schema.table_name}: no primary key — row order is non-deterministic. "
                "Rows may be skipped or duplicated if the table is modified during sync."
            )

        # Offset pagination: skip saved rows and fetch the next batch.
        sql = (
            f"SELECT {select_column_sql} FROM {schema_table} WITH (NOLOCK) "
            f"{self._order_clause} "
            f"OFFSET ? ROWS FETCH NEXT ? ROWS ONLY "
            f"OPTION (FAST {self._batch_size})"
        )

        offset = self._last_offset
        while True:
            # First sync uses offset 0; resume uses the saved offset.
            with self._pool.acquire() as connection:
                # execute_and_fetch_with_retry runs both execute and fetchmany inside the retry
                # loop so transient errors during fetch are recovered the same as during execute.
                rows = connection.execute_and_fetch_with_retry(
                    sql, (offset, self._batch_size), self._batch_size
                )

            if not rows:
                return

            batch = [
                {
                    column.name: convert_value(raw_value, column.python_type)
                    for column, raw_value in zip(selectable_columns, row)
                }
                for row in rows
            ]
            offset += len(batch)
            log.fine(f"{self._schema.table_name}: offset page, next_offset={offset}")
            yield batch, offset
