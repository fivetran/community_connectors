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
        return [col.name for col in self.columns if col.is_primary_key]

    @property
    def selectable_columns(self) -> list:
        return [col for col in self.columns if not col.is_computed]



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
            CASE WHEN kcu.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_pk
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
        with self._pool.acquire() as conn:
            cur = conn.execute_with_retry(self._METADATA_SQL, (schema_name, table_name))
            try:
                rows = cur.fetchall()
            finally:
                cur.close()

        columns = []
        for col_name, data_type, is_computed, is_pk in rows:
            columns.append(
                ColumnInfo(
                    name=col_name,
                    sql_type=data_type,
                    python_type=SchemaDetector.map_sql_type_to_python(data_type),
                    is_primary_key=bool(is_pk),
                    is_computed=bool(is_computed),
                )
            )

        if not columns:
            log.warning(f"No columns found for {schema_name}.{table_name}")

        ts = TableSchema(table_name=table_name, schema_name=schema_name, columns=columns)
        ts.replication_key = SchemaDetector.detect_replication_key(columns, config or {})
        return ts

    def detect_all_tables(
        self, schema_name: str, table_names: list = None, config: dict = None, max_workers: int = 4
    ) -> dict:
        if table_names is None:
            table_names = self._list_tables(schema_name)

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
        sql = (
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' "
            "ORDER BY TABLE_NAME"
        )
        with self._pool.acquire() as conn:
            cur = conn.execute_with_retry(sql, (schema_name,))
            try:
                rows = cur.fetchall()
            finally:
                cur.close()
        return [row[0] for row in rows]

    @staticmethod
    def detect_replication_key(columns: list, config: dict):
        # Priority 1: user-specified column name overrides auto-detection entirely
        overrided_incremental_column = (config.get("incremental_column") or "").strip()
        if overrided_incremental_column:
            for col in columns:
                if col.name.lower() == overrided_incremental_column.lower():
                    log.fine(f"Using overrided_incremental_column: {col.name}")
                    return col
            log.warning(
                f"overrided_incremental_column '{overrided_incremental_column}' not found in table columns — "
                "falling back to pattern-based auto-detection"
            )

        # Priority 2: match column name against known EHI replication key patterns
        col_map = {col.name.lower(): col for col in columns}
        for pattern in KNOWN_REPLICATION_KEY_PATTERNS:
            if pattern.lower() in col_map:
                matched_col = col_map[pattern.lower()]
                log.fine(f"Replication key matched pattern '{pattern}': {matched_col.name}")
                return matched_col

        return None

    @staticmethod
    def map_sql_type_to_python(sql_type: str):
        t = sql_type.lower().strip()
        if t in {"int", "bigint", "smallint", "tinyint"}:
            return int
        if t in {"float", "real", "decimal", "numeric", "money", "smallmoney"}:
            return float
        if t in {
            "varchar", "nvarchar", "char", "nchar", "text", "ntext",
            "uniqueidentifier", "xml", "datetime", "datetime2", "date",
            "time", "smalldatetime", "datetimeoffset",
        }:
            return str
        if t == "bit":
            return bool
        if t in {"varbinary", "binary", "image", "geography", "geometry",
                 "hierarchyid", "timestamp", "rowversion"}:
            return bytes
        log.fine(f"Unknown SQL type '{sql_type}' mapped to str")
        return str
