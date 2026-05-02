"""
Readers for full-table and incremental reads using keyset or offset pagination.

KeysetReader    — O(n) keyset pagination ordered by (repl_col[, pk_tiebreak]).
PKKeysetReader  — O(n) full load for tables with a single PK but no replication key.
OffsetReader    — O(n²) last-resort fallback for tables with no PK and no replication key.
"""

import base64
from datetime import datetime, date, time as _time

from fivetran_connector_sdk import Logging as log

from constants import BATCH_SIZE
from client import ConnectionPool
from models import TableSchema

_DATETIME_TYPES = (datetime, date, _time)

# uniqueidentifier excluded: SQL Server internal sort order doesn't match lexicographic order,
# so > comparisons produce an unreliable keyset cursor.
_TIEBREAK_ELIGIBLE_STR_TYPES = frozenset({"varchar", "nvarchar", "char", "nchar", "text", "ntext"})

# datetime2/datetimeoffset store 7 decimal places; Python datetime only holds 6.
# KeysetReader selects these as strings (style 127) to preserve full precision and
# prevent the truncated cursor from matching the same row on every page (infinite loop).
_DATETIME2_SQL_TYPES = frozenset({"datetime2", "datetimeoffset"})


def convert_value(value, python_type):
    """Convert a raw pyodbc value to a JSON-serialisable Python scalar."""
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


def _get_tiebreak_pk_col(schema: TableSchema, pk_cols: list, use_pk_tiebreak: bool):
    """Return the single PK column name eligible for tiebreaking, or None."""
    if not use_pk_tiebreak or len(pk_cols) != 1:
        if use_pk_tiebreak and len(pk_cols) > 1:
            log.warning(
                f"{schema.table_name}: tiebreak requested but table has a "
                f"{len(pk_cols)}-column composite PK — tiebreak disabled"
            )
        return None
    pk_name = pk_cols[0]
    for col in schema.columns:
        if col.name != pk_name:
            continue
        if col.python_type is int:
            return pk_name
        if col.python_type is str and col.sql_type.lower() in _TIEBREAK_ELIGIBLE_STR_TYPES:
            return pk_name
        log.warning(
            f"{schema.table_name}: PK column '{pk_name}' has SQL type '{col.sql_type}' "
            "which is not eligible for tiebreaking — tiebreak disabled"
        )
        return None
    return None


class KeysetReader:
    """
    Keyset (seek) pagination ordered by (repl_col[, pk_tiebreak]). O(n) cost.

    last_seen_value=None → fresh full load from beginning.
    last_seen_value=<cursor> → incremental or resumed full load from that cursor.

    Yields (batch, last_repl_value, last_pk_value) 3-tuples.
    """

    def __init__(
        self,
        pool: ConnectionPool,
        schema: TableSchema,
        last_seen_value,
        batch_size: int = BATCH_SIZE,
        use_pk_tiebreak: bool = True,
        pk_cols: list = None,
        last_seen_pk=None,
    ) -> None:
        self._pool = pool
        self._schema = schema
        self._last_seen = last_seen_value
        self._batch_size = batch_size
        self._pk_cols = pk_cols or []
        self._last_seen_pk = last_seen_pk
        self._tiebreak_pk = _get_tiebreak_pk_col(schema, self._pk_cols, use_pk_tiebreak)

    def read_batches(self):
        repl_col = self._schema.replication_key.name
        sel_cols = self._schema.selectable_columns
        col_sql = ", ".join(f"[{c.name}]" for c in sel_cols)
        tbl = f"[{self._schema.schema_name}].[{self._schema.table_name}]"
        tb_pk = self._tiebreak_pk
        fetch = f"OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY OPTION (FAST {self._batch_size})"

        repl_col_type = self._schema.replication_key.sql_type.lower()
        _needs_str_cursor = repl_col_type in _DATETIME2_SQL_TYPES
        if _needs_str_cursor:
            col_sql_select = col_sql + f", CONVERT(NVARCHAR(50), [{repl_col}], 127)"
            _repl_str_idx = len(sel_cols)
        else:
            col_sql_select = col_sql
            _repl_str_idx = None

        repl_idx = next((i for i, c in enumerate(sel_cols) if c.name == repl_col), None)
        if repl_idx is None:
            raise ValueError(
                f"{self._schema.table_name}: replication key '{repl_col}' is not in "
                "selectable_columns (may be a computed column). Set incremental_column "
                "in configuration.json to a non-computed column."
            )
        pk_idx = (
            next((i for i, c in enumerate(sel_cols) if c.name == tb_pk), None) if tb_pk else None
        )

        if tb_pk:
            sql_start = (
                f"SELECT {col_sql_select} FROM {tbl} WITH (NOLOCK) "
                f"WHERE [{repl_col}] IS NOT NULL "
                f"ORDER BY [{repl_col}], [{tb_pk}] {fetch}"
            )
            sql_from = (
                f"SELECT {col_sql_select} FROM {tbl} WITH (NOLOCK) "
                f"WHERE [{repl_col}] > ? "
                f"ORDER BY [{repl_col}], [{tb_pk}] {fetch}"
            )
            sql_composite = (
                f"SELECT {col_sql_select} FROM {tbl} WITH (NOLOCK) "
                f"WHERE ([{repl_col}] > ?) OR ([{repl_col}] = ? AND [{tb_pk}] > ?) "
                f"ORDER BY [{repl_col}], [{tb_pk}] {fetch}"
            )
        else:
            sql_start = (
                f"SELECT {col_sql_select} FROM {tbl} WITH (NOLOCK) "
                f"WHERE [{repl_col}] IS NOT NULL "
                f"ORDER BY [{repl_col}] {fetch}"
            )
            sql_from = (
                f"SELECT {col_sql_select} FROM {tbl} WITH (NOLOCK) "
                f"WHERE [{repl_col}] > ? "
                f"ORDER BY [{repl_col}] {fetch}"
            )
            sql_composite = sql_from

        current_last = self._last_seen
        current_last_pk = self._last_seen_pk
        page = 0

        while True:
            if current_last is None:
                sql, params = sql_start, (self._batch_size,)
            elif not tb_pk or current_last_pk is None:
                sql, params = sql_from, (current_last, self._batch_size)
            else:
                sql = sql_composite
                params = (current_last, current_last, current_last_pk, self._batch_size)

            with self._pool.acquire() as conn:
                cur = conn.execute_with_retry(sql, params)
                try:
                    rows = cur.fetchmany(self._batch_size)
                finally:
                    cur.close()

            if not rows:
                log.fine(f"{self._schema.table_name}: page {page} returned 0 rows — done")
                return

            if page == 0 and current_last is None:
                null_count_sql = (
                    f"SELECT COUNT(*) FROM {tbl} WITH (NOLOCK) WHERE [{repl_col}] IS NULL"
                )
                with self._pool.acquire() as _conn:
                    _cur = _conn.execute_with_retry(null_count_sql, ())
                    try:
                        null_count = _cur.fetchone()[0] or 0
                    finally:
                        _cur.close()
                if null_count:
                    log.warning(
                        f"{self._schema.table_name}: {null_count} row(s) have a NULL "
                        f"replication key ('{repl_col}') and will never be synced."
                    )

            if not tb_pk and page > 0 and current_last is not None:
                first_rk = convert_value(rows[0][repl_idx], sel_cols[repl_idx].python_type)
                if first_rk == current_last:
                    log.warning(
                        f"{self._schema.table_name}: duplicate replication key values at "
                        f"page boundary (value={current_last}). Rows may be skipped."
                    )

            last_rk = current_last
            last_pk = current_last_pk
            batch = []
            for row in rows:
                record = {
                    c.name: convert_value(raw, c.python_type) for c, raw in zip(sel_cols, row)
                }
                batch.append(record)
                if _needs_str_cursor:
                    last_rk = row[_repl_str_idx]
                else:
                    rk = convert_value(row[repl_idx], sel_cols[repl_idx].python_type)
                    last_rk = rk if rk is not None else str(row[repl_idx])
                if tb_pk and pk_idx is not None:
                    pk = convert_value(row[pk_idx], sel_cols[pk_idx].python_type)
                    if pk is not None:
                        last_pk = pk

            current_last = last_rk
            current_last_pk = last_pk
            page += 1
            log.fine(
                f"{self._schema.table_name}: page {page} — "
                f"{len(batch)} rows, last_value={current_last}"
            )
            yield batch, current_last, current_last_pk


class PKKeysetReader:
    """
    Keyset pagination by primary key for tables that have a single PK but no replication key.
    O(n) cost; resumable from last committed PK value.

    Yields (batch, last_pk_value) pairs.
    """

    def __init__(
        self,
        pool: ConnectionPool,
        schema: TableSchema,
        last_seen_pk=None,
        batch_size: int = BATCH_SIZE,
    ) -> None:
        self._pool = pool
        self._schema = schema
        self._last_seen_pk = last_seen_pk
        self._batch_size = batch_size
        self._pk_col = schema.primary_keys[0]

    def read_batches(self):
        pk_col = self._pk_col
        sel_cols = self._schema.selectable_columns
        col_sql = ", ".join(f"[{col.name}]" for col in sel_cols)
        schema_table = f"[{self._schema.schema_name}].[{self._schema.table_name}]"
        fetch = f"OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY OPTION (FAST {self._batch_size})"

        sql_first = (
            f"SELECT {col_sql} FROM {schema_table} WITH (NOLOCK) ORDER BY [{pk_col}] {fetch}"
        )
        sql_next = (
            f"SELECT {col_sql} FROM {schema_table} WITH (NOLOCK) "
            f"WHERE [{pk_col}] > ? ORDER BY [{pk_col}] {fetch}"
        )

        pk_col_idx = next((i for i, col in enumerate(sel_cols) if col.name == pk_col), None)
        if pk_col_idx is None:
            raise ValueError(
                f"{self._schema.table_name}: PK column '{pk_col}' is not in selectable_columns "
                "(may be a computed column). Cannot use PK-keyset pagination."
            )
        pk_python_type = sel_cols[pk_col_idx].python_type
        current_last = self._last_seen_pk

        while True:
            if current_last is None:
                sql, params = sql_first, (self._batch_size,)
            else:
                sql, params = sql_next, (current_last, self._batch_size)

            with self._pool.acquire() as conn:
                cur = conn.execute_with_retry(sql, params)
                try:
                    rows = cur.fetchmany(self._batch_size)
                finally:
                    cur.close()

            if not rows:
                log.fine(f"{self._schema.table_name}: PK-keyset page returned 0 rows — done")
                return

            last_pk_value = current_last
            batch = []
            for row in rows:
                record = {
                    col.name: convert_value(raw, col.python_type)
                    for col, raw in zip(sel_cols, row)
                }
                batch.append(record)
                pk_converted = convert_value(row[pk_col_idx], pk_python_type)
                if pk_converted is not None:
                    last_pk_value = pk_converted

            current_last = last_pk_value
            log.fine(
                f"{self._schema.table_name}: PK-keyset page — "
                f"{len(batch)} rows, last_pk={current_last}"
            )
            yield batch, current_last


class OffsetReader:
    """
    SQL OFFSET/FETCH pagination. O(n²) database cost.

    Used only as a last resort for tables with no replication key and no single-column PK.
    These tables are deferred and run after all keyset tables complete to avoid blocking
    high-volume tables.

    Yields (batch, next_offset) pairs.
    """

    def __init__(
        self,
        pool: ConnectionPool,
        schema: TableSchema,
        last_offset: int,
        batch_size: int = BATCH_SIZE,
    ) -> None:
        self._pool = pool
        self._schema = schema
        self._last_offset = last_offset
        self._batch_size = batch_size
        pk_cols = schema.primary_keys
        if pk_cols:
            self._order_clause = "ORDER BY " + ", ".join(f"[{pk}]" for pk in pk_cols)
            self._has_pk_order = True
        else:
            self._order_clause = "ORDER BY (SELECT NULL)"
            self._has_pk_order = False

    def read_batches(self):
        sel_cols = self._schema.selectable_columns
        col_sql = ", ".join(f"[{col.name}]" for col in sel_cols)
        schema_table = f"[{self._schema.schema_name}].[{self._schema.table_name}]"

        if not self._has_pk_order:
            log.warning(
                f"{self._schema.table_name}: no primary key — row order is non-deterministic. "
                "Rows may be skipped or duplicated if the table is modified during sync."
            )

        sql = (
            f"SELECT {col_sql} FROM {schema_table} WITH (NOLOCK) "
            f"{self._order_clause} "
            f"OFFSET ? ROWS FETCH NEXT ? ROWS ONLY "
            f"OPTION (FAST {self._batch_size})"
        )

        offset = self._last_offset
        while True:
            with self._pool.acquire() as conn:
                cur = conn.execute_with_retry(sql, (offset, self._batch_size))
                try:
                    rows = cur.fetchmany(self._batch_size)
                finally:
                    cur.close()

            if not rows:
                return

            batch = [
                {col.name: convert_value(raw, col.python_type) for col, raw in zip(sel_cols, row)}
                for row in rows
            ]
            offset += len(batch)
            log.fine(f"{self._schema.table_name}: offset page, next_offset={offset}")
            yield batch, offset
