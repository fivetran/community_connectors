from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed

from fivetran_connector_sdk import Logging as log

from constants import KNOWN_REPLICATION_KEY_PATTERNS
from client import ConnectionPool


@dataclass
class ColumnInfo:
    name: str
    sql_type: str
    python_type: type
    is_primary_key: bool
    is_computed: bool


@dataclass
class TableSchema:
    table_name: str
    schema_name: str
    columns: list = field(default_factory=list)
    replication_key: ColumnInfo = None

    @property
    def primary_keys(self) -> list:
        """Return a list of column names that make up the primary key."""
        return [column.name for column in self.columns if column.is_primary_key]

    @property
    def selectable_columns(self) -> list:
        """Return all columns that can appear in a SELECT list (excludes computed columns)."""
        return [column for column in self.columns if not column.is_computed]



class SchemaDetector:
    _METADATA_SQL = """
        SELECT
            c.COLUMN_NAME,
            c.DATA_TYPE,
            COLUMNPROPERTY(
                OBJECT_ID('[' + c.TABLE_SCHEMA + '].[' + c.TABLE_NAME + ']'),
                c.COLUMN_NAME,
                'IsComputed'
            ) AS is_computed,
            CASE WHEN kcu.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_primary_key
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            ON  tc.TABLE_SCHEMA = c.TABLE_SCHEMA
            AND tc.TABLE_NAME   = c.TABLE_NAME
            AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON  kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            AND kcu.TABLE_SCHEMA    = c.TABLE_SCHEMA
            AND kcu.TABLE_NAME      = c.TABLE_NAME
            AND kcu.COLUMN_NAME     = c.COLUMN_NAME
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
    """

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def detect_table(self, schema_name: str, table_name: str, config: dict = None) -> TableSchema:
        """Query INFORMATION_SCHEMA for one table and return its TableSchema."""
        with self._pool.acquire() as connection:
            cursor = connection.execute_with_retry(self._METADATA_SQL, (schema_name, table_name))
            try:
                rows = cursor.fetchall()
            finally:
                cursor.close()

        columns = []
        for column_name, data_type, is_computed, is_primary_key in rows:
            columns.append(
                # ColumnInfo -> (name, sql_type, python_type, is_primary_key, is_computed)
                ColumnInfo(
                    name=column_name,
                    sql_type=data_type,
                    python_type=SchemaDetector.map_sql_type_to_python(data_type),
                    is_primary_key=bool(is_primary_key),
                    is_computed=bool(is_computed),
                )
            )

        if not columns:
            log.warning(f"No columns found for {schema_name}.{table_name}")

        # TableSchema -> (table_name, schema_name, columns)
        table_schema = TableSchema(table_name=table_name, schema_name=schema_name, columns=columns)
        table_schema.replication_key = SchemaDetector.detect_replication_key(columns, config or {})
        return table_schema

    def detect_all_tables(
        self,
        schema_name: str,
        table_names: list = None,
        table_exclude: frozenset = None,
        config: dict = None,
        max_workers: int = 4,
    ) -> dict:
        """Detect schemas for all tables in scope in parallel and return {table_name: TableSchema}."""
        if table_names is None:
            table_names = self._list_tables(schema_name)
        if table_exclude:
            table_names = [
                table_name
                for table_name in table_names
                if table_name.lower() not in table_exclude
            ]

        results: dict = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_name = {
                executor.submit(self.detect_table, schema_name, table_name, config): table_name
                for table_name in table_names
            }
            for future in as_completed(future_to_name):
                table_name = future_to_name[future]
                try:
                    results[table_name] = future.result()
                except Exception as exc:
                    log.severe(f"Failed to detect schema for {schema_name}.{table_name}: {exc}")

        log.info(f"Schema detection complete: {len(results)} table(s) discovered")
        return results

    def _list_tables(self, schema_name: str) -> list:
        """Query INFORMATION_SCHEMA.TABLES and return all base table names in the given schema."""
        sql = (
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' "
            "ORDER BY TABLE_NAME"
        )
        with self._pool.acquire() as connection:
            cursor = connection.execute_with_retry(sql, (schema_name,))
            try:
                rows = cursor.fetchall()
            finally:
                cursor.close()
        return [row[0] for row in rows]

    @staticmethod
    def detect_replication_key(columns: list, config: dict):
        """Return the ColumnInfo to use as the replication key, or None if no suitable column is found.
        Checks incremental_column config first, then pattern-matches known column name suffixes."""
        # Priority 1: user-specified column name overrides auto-detection entirely
        configured_incremental_column = (config.get("incremental_column") or "").strip()
        if configured_incremental_column:
            for column in columns:
                if column.name.lower() == configured_incremental_column.lower():
                    # Computed columns are excluded from the SELECT list, so using one as the
                    # replication key would cause ReplicationKeysetReader to raise a ValueError
                    # instead of falling back to PK-keyset or offset pagination.
                    if column.is_computed:
                        log.warning(
                            f"Configured incremental_column '{column.name}' is a computed column "
                            "and cannot be used as a replication key — "
                            "table will fall back to PK-keyset or offset pagination"
                        )
                        return None
                    log.fine(f"Using configured incremental column: {column.name}")
                    return column
            log.warning(
                f"Configured incremental column '{configured_incremental_column}' not found in table columns — "
                "falling back to pattern-based auto-detection"
            )

        # Priority 2: match column name against known EHI replication key patterns.
        # Exclude computed columns — they are not in the SELECT list so the reader cannot
        # use them as a cursor, and the table would fail instead of falling back gracefully.
        column_by_lower_name = {
            column.name.lower(): column
            for column in columns
            if not column.is_computed
        }
        for pattern in KNOWN_REPLICATION_KEY_PATTERNS:
            if pattern.lower() in column_by_lower_name:
                matched_column = column_by_lower_name[pattern.lower()]
                log.fine(f"Replication key matched pattern '{pattern}': {matched_column.name}")
                return matched_column

        return None

    @staticmethod
    def map_sql_type_to_python(sql_type: str):
        """Map a SQL Server data type string to the closest Python type for use in convert_value()."""
        normalized_sql_type = sql_type.lower().strip()
        if normalized_sql_type in {"int", "bigint", "smallint", "tinyint"}:
            return int
        if normalized_sql_type in {"float", "real"}:
            return float
        # decimal, numeric, money, smallmoney mapped to str to preserve full precision —
        # Python float is IEEE 754 64-bit (~15 digits) while SQL Server supports up to 38.
        # pyodbc returns decimal.Decimal for these types; str(Decimal) gives the exact representation.
        if normalized_sql_type in {"decimal", "numeric", "money", "smallmoney"}:
            return str
        if normalized_sql_type in {
            "varchar", "nvarchar", "char", "nchar", "text", "ntext",
            "uniqueidentifier", "xml", "datetime", "datetime2", "date",
            "time", "smalldatetime", "datetimeoffset",
        }:
            return str
        if normalized_sql_type == "bit":
            return bool
        if normalized_sql_type in {"varbinary", "binary", "image", "geography", "geometry",
                                   "hierarchyid", "timestamp", "rowversion"}:
            return bytes
        log.fine(f"Unknown SQL type '{sql_type}' mapped to str")
        return str
