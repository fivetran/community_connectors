"""
EHI High-Volume Connector for Fivetran Connector SDK.
Syncs 100M+ row tables from Microsoft SQL Server using:
- Keyset pagination (O(n)) for tables with a replication key
- PK-keyset pagination (O(n)) for tables with a single primary key
- OFFSET pagination (O(n²), deferred) for tables with neither — runs after all keyset tables
- One thread per keyset/PK-keyset table; MAX_WORKERS threads (set in constants.py)
- READ UNCOMMITTED isolation on all connections to avoid lock contention
"""

import json
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from constants import BATCH_SIZE, CHECKPOINT_INTERVAL, MAX_WORKERS
from client import ConnectionPool
from models import SchemaDetector, TableSchema
from readers import KeysetReader, OffsetReader, PKKeysetReader


def validate_configuration(configuration: dict) -> None:
    required_fields = [
        "mssql_server",
        "mssql_port",
        "mssql_database",
        "mssql_user",
        "mssql_password",
        "mssql_schema",
    ]
    for field in required_fields:
        value = configuration.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Configuration field '{field}' must be a non-empty string.")

    port = configuration["mssql_port"].strip()
    try:
        if int(port) <= 0:
            raise ValueError()
    except (ValueError, TypeError):
        raise ValueError(f"Configuration field 'mssql_port' must be a positive integer, got '{port}'.")

    optional_str_fields = ["mssql_cert_server", "incremental_column", "table_list", "table_exclusion_list"]
    for field in optional_str_fields:
        value = configuration.get(field)
        if value is not None and not isinstance(value, str):
            raise ValueError(f"Configuration field '{field}' must be a string if provided.")


def _parse_table_filter(configuration: dict) -> tuple:
    """Read table_list and table_exclusion_list from configuration and return (include, exclude)."""
    excl_raw = configuration.get("table_exclusion_list", "").strip()
    table_exclude = frozenset(n.strip().lower() for n in excl_raw.split(",") if n.strip())
    list_raw = configuration.get("table_list", "").strip()
    table_include = (
        [n.strip() for n in list_raw.split(",") if n.strip() and n.strip().lower() not in table_exclude]
        if list_raw else None
    )
    return table_include, table_exclude


def _discover_table_schemas(
    pool: ConnectionPool,
    schema_name: str,
    table_include,
    table_exclude: frozenset,
    config: dict,
    max_workers: int,
) -> dict:
    detector = SchemaDetector(pool)
    table_schemas = detector.detect_all_tables(
        schema_name, table_include, config=config, max_workers=max_workers
    )
    if table_exclude:
        table_schemas = {
            name: ts for name, ts in table_schemas.items() if name.lower() not in table_exclude
        }
    return table_schemas


def _determine_mode(table_state: dict, table_schema: TableSchema) -> str:
    """
    Decide full vs incremental for a table.
    - No prior state → full
    - Prior state is full, incomplete → full (resume)
    - Prior is full, completed → incremental if replication key exists, else full
    - Prior is incremental → incremental (guard: fall back to full if cursor or key missing)
    """
    has_replication_key = table_schema.replication_key is not None

    if not table_state:
        return "full"

    prior_mode = table_state.get("mode", "full")

    if prior_mode == "full":
        if table_state.get("sync_completed_at") is None:
            return "full"
        return "incremental" if has_replication_key else "full"

    if prior_mode == "incremental":
        if has_replication_key and table_state.get("last_seen_replication_value") is not None:
            return "incremental"
        return "full"

    return "full"


def _save_checkpoint(
    state: dict,
    table_name: str,
    table_state: dict,
    mode: str,
    has_repl_key: bool,
    marker,
    rows_synced: int,
    completed: bool = False,
    pk_marker=None,
    use_pk_cursor: bool = False,
) -> None:
    if has_repl_key:
        if marker is not None:
            table_state["last_seen_replication_value"] = marker
        if pk_marker is not None:
            table_state["last_seen_pk_value"] = pk_marker
    elif use_pk_cursor:
        if marker is not None:
            table_state["last_seen_pk_cursor"] = marker
    else:
        table_state["last_offset"] = marker
    table_state["rows_synced"] = rows_synced
    table_state["mode"] = mode
    table_state["sync_completed_at"] = (
        datetime.now(timezone.utc).isoformat() if completed else None
    )
    state[table_name] = table_state
    op.checkpoint(state)


def _sync_table(
    table_schema: TableSchema,
    state: dict,
    table_state: dict,
    pool: ConnectionPool,
) -> None:
    """
    Unified sync function — picks the right reader based on table schema and state,
    then runs the checkpoint loop. All three reader types (keyset, PK-keyset, offset)
    go through this single code path.
    """
    table_name = table_schema.table_name
    has_repl_key = table_schema.replication_key is not None
    pk_cols = table_schema.primary_keys
    rows_synced = int(table_state.get("rows_synced", 0))

    if has_repl_key:
        mode = _determine_mode(table_state, table_schema)
        last_marker = table_state.get("last_seen_replication_value")
        last_pk_marker = table_state.get("last_seen_pk_value")

        # For an incremental sync, last_marker is the cursor to fetch rows strictly after.
        # For a full load (fresh or resumed), last_marker is either None (start of table)
        # or the last committed value from an interrupted full load (resume point).
        reader = KeysetReader(
            pool,
            table_schema,
            last_marker,
            BATCH_SIZE,
            use_pk_tiebreak=True,
            pk_cols=pk_cols,
            last_seen_pk=last_pk_marker,
        )
        log.info(f"{table_name}: starting {mode} keyset sync (cursor={last_marker})")

        for batch, progress_marker, progress_pk_marker in reader.read_batches():
            for row in batch:
                op.upsert(table_name, row)
            rows_synced += len(batch)
            last_marker = progress_marker
            last_pk_marker = progress_pk_marker
            if rows_synced % CHECKPOINT_INTERVAL == 0:
                _save_checkpoint(
                    state, table_name, table_state, mode, True,
                    last_marker, rows_synced, pk_marker=last_pk_marker,
                )
                log.info(f"{table_name}: checkpoint at {rows_synced:,} rows")

        _save_checkpoint(
            state, table_name, table_state, mode, True,
            last_marker, rows_synced, completed=True, pk_marker=last_pk_marker,
        )

    elif len(pk_cols) == 1:
        last_marker = table_state.get("last_seen_pk_cursor")
        reader = PKKeysetReader(pool, table_schema, last_marker, BATCH_SIZE)
        log.info(f"{table_name}: starting full PK-keyset sync (last_pk={last_marker})")

        for batch, progress_marker in reader.read_batches():
            for row in batch:
                op.upsert(table_name, row)
            rows_synced += len(batch)
            last_marker = progress_marker
            if rows_synced % CHECKPOINT_INTERVAL == 0:
                _save_checkpoint(
                    state, table_name, table_state, "full", False,
                    last_marker, rows_synced, use_pk_cursor=True,
                )
                log.info(f"{table_name}: checkpoint at {rows_synced:,} rows")

        _save_checkpoint(
            state, table_name, table_state, "full", False,
            last_marker, rows_synced, completed=True, use_pk_cursor=True,
        )

    else:
        last_marker = int(table_state.get("last_offset", 0))
        reader = OffsetReader(pool, table_schema, last_marker, BATCH_SIZE)
        log.info(f"{table_name}: starting full offset sync (last_offset={last_marker})")

        for batch, progress_marker in reader.read_batches():
            for row in batch:
                op.upsert(table_name, row)
            rows_synced += len(batch)
            last_marker = progress_marker
            if rows_synced % CHECKPOINT_INTERVAL == 0:
                _save_checkpoint(
                    state, table_name, table_state, "full", False, last_marker, rows_synced,
                )
                log.info(f"{table_name}: checkpoint at {rows_synced:,} rows")

        _save_checkpoint(
            state, table_name, table_state, "full", False, last_marker, rows_synced, completed=True,
        )

    log.info(f"{table_name}: sync complete — {rows_synced:,} row(s)")


def _sync_table_thread(table_schema: TableSchema, state: dict, pool: ConnectionPool) -> None:
    """
    Entry point for each worker thread. Each thread exclusively owns state[table_name]
    — no locking needed because the GIL serialises dict writes and no two threads
    touch the same key.
    """
    table_name = table_schema.table_name
    try:
        if not table_schema.selectable_columns:
            log.warning(
                f"{table_name}: no selectable columns found — "
                "table may not exist or may contain only computed/unsupported columns. Skipping."
            )
            return

        table_state = dict(state.get(table_name, {}))
        if "replication_key_col" not in table_state:
            table_state["replication_key_col"] = (
                table_schema.replication_key.name if table_schema.replication_key else None
            )

        _sync_table(table_schema, state, table_state, pool)

    except Exception as exc:
        log.severe(f"{table_name}: sync failed: {exc}")
        log.severe(traceback.format_exc())
        raise


def schema(configuration: dict):
    """
    Returns the list of tables and their primary keys for Fivetran to create destination tables.
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    """
    validate_configuration(configuration)
    schema_name = configuration.get("mssql_schema", "dbo")
    table_include, table_exclude = _parse_table_filter(configuration)

    pool = ConnectionPool(configuration=configuration, size=1)
    try:
        table_schemas = _discover_table_schemas(
            pool, schema_name, table_include, table_exclude, config=configuration, max_workers=1
        )
    finally:
        pool.close_all()

    schema_list = []
    for table_name, ts in sorted(table_schemas.items()):
        entry = {"table": table_name}
        if ts.primary_keys:
            entry["primary_key"] = ts.primary_keys
        schema_list.append(entry)

    log.info(f"schema(): returning {len(schema_list)} table(s)")
    return schema_list


def update(configuration: dict, state: dict):
    """
    Called by Fivetran on every sync. Syncs all discovered tables using the best available
    pagination strategy. Keyset and PK-keyset tables run in parallel; offset tables (no PK,
    no replication key) are deferred and run sequentially after all parallel work completes.
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    """
    log.warning("Example: Connectors - EHI High Volume")
    schema_name = configuration.get("mssql_schema", "dbo")
    table_include, table_exclude = _parse_table_filter(configuration)

    pool = ConnectionPool(configuration=configuration, size=MAX_WORKERS)

    try:
        table_schemas = _discover_table_schemas(
            pool, schema_name, table_include, table_exclude, config=configuration, max_workers=MAX_WORKERS,
        )

        if not table_schemas:
            log.warning("No tables discovered — nothing to sync")
            return

        log.info(f"Discovered {len(table_schemas)} table(s): {sorted(table_schemas)}")

        # Classify tables by best available pagination strategy
        keyset_tables = []
        pk_tables = []
        offset_tables = []

        for name, ts in table_schemas.items():
            if ts.replication_key is not None:
                keyset_tables.append(name)
            elif len(ts.primary_keys) == 1:
                pk_tables.append(name)
            else:
                offset_tables.append(name)
                log.warning(
                    f"{name}: no replication key and no single-column primary key — "
                    "using OFFSET pagination (O(n²), full sync only, no incremental). "
                    "This may be slow for large tables. Deferred until after keyset tables complete."
                )

        state["_sync_start"] = datetime.now(timezone.utc).isoformat()
        failed_tables = []

        # Run keyset + PK-keyset tables in parallel
        parallel_tables = keyset_tables + pk_tables
        if parallel_tables:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_table = {
                    executor.submit(_sync_table_thread, table_schemas[name], state, pool): name
                    for name in parallel_tables
                }
                for future in as_completed(future_to_table):
                    name = future_to_table[future]
                    try:
                        future.result()
                        log.info(f"{name}: thread finished successfully")
                    except Exception as exc:
                        log.severe(f"{name}: thread raised unhandled exception: {exc}")
                        failed_tables.append(name)

        # Run offset tables sequentially after all parallel work completes
        for name in offset_tables:
            try:
                _sync_table_thread(table_schemas[name], state, pool)
                log.info(f"{name}: offset sync finished successfully")
            except Exception as exc:
                log.severe(f"{name}: offset sync failed: {exc}")
                failed_tables.append(name)

        if failed_tables:
            log.warning(f"Tables that failed to sync: {failed_tables}")

        op.checkpoint(state)
        log.info("EHI High-Volume Connector sync complete")

    finally:
        pool.close_all()


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
