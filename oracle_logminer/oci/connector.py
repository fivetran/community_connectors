"""Oracle LogMiner connector (Autonomous Database / OCI) for Fivetran Connector SDK.

Sync model:
    * **Initial sync** (no ``last_scn`` in state, or ``initial_sync_done`` false):
      runs ``SELECT *`` on every configured table and emits ``op.upsert`` for each row.
      Progress is checkpointed after every table (and every ``checkpoint_rows`` rows), so an
      interrupted snapshot resumes at the first unfinished table; ``initial_sync_done`` becomes
      true when it finishes.
    * **Incremental syncs** (``last_scn`` present): mines committed DML from redo
      via ``DBMS_LOGMNR`` / ``V$LOGMNR_CONTENTS`` (classic LogMiner API on many RDS
      builds), parses ``SQL_REDO``, and emits upserts and deletes.

State tracking:
    last_scn (int): After the initial snapshot or an incremental run, the SCN
                    boundary for the next LogMiner window (incremental runs use
                    last_scn + 1 through current SCN).
    initial_sync_done (bool): Set true when the initial ``SELECT *`` snapshot
                    has finished; omitted in legacy state (``last_scn`` only).
    snapshot (dict): While the snapshot runs: its baseline ``scn``, ``mine_from`` and the
                    ``done_tables`` already copied.
    snapshot_tables (list): Tables already snapshotted; tables not listed are snapshotted
                    at the start of the next sync.

Configuration keys (see configuration.json for an example):
    host              Oracle RAC SCAN hostname or node VIP.
    port              TNS listener port (default: 1521).
    service_name      Oracle service name (or PDB name).
    username          Database user with LogMiner privileges.
    password          Database password.
    schema            Oracle schema (owner) to replicate.
    tables            Comma-separated table names, or '*' for all (default: *).
    batch_size        Rows per fetch for snapshot cursors and V$LOGMNR_CONTENTS (default: 1000).
    checkpoint_rows   Rows between checkpoints inside one snapshot table (default: 100000).
    declare_column_types  'true' (default) declares Oracle-derived column types in schema() so
                      snapshot and LogMiner rows land in the same type; 'false' lets Fivetran infer.
    logminer_max_archived_logs  Deprecated/ignored: all redo overlapping a window is always added.
    logminer_scn_window  Max SCNs mined per LogMiner session; each window is checkpointed and
                      halved automatically on ORA-04036 (default: 500000).
    logminer_buffer_mem_rows  Uncommitted rows held in RAM before spilling to disk (default: 50000).
    logminer_open_txn_check  'true' (default) samples GV$TRANSACTION so no in-flight transaction
                      can be missed; needs SELECT on V_$TRANSACTION and GV_$TRANSACTION.
    logminer_on_parse_error  'fail' (default) stops the sync on an unparseable committed change;
                      'skip' logs a warning and drops it.
    logminer_ordered_query  'true' adds ORDER BY SCN to the LogMiner query (uses PGA); only set it
                      if the sync reports out-of-order rows.
    oracle_thick_mode Set to 'true' to use Oracle Instant Client (default: false).
    oracle_client_lib_dir  Path to Oracle Instant Client libs (thick mode only).

Required Oracle setup (run as DBA before first sync):
    ALTER DATABASE ARCHIVELOG;                      -- enable archivelog mode
    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;       -- minimal supplemental logging
    GRANT CREATE SESSION           TO logminer_user;
    GRANT EXECUTE ON DBMS_LOGMNR   TO logminer_user;  -- classic package (RDS)
    GRANT EXECUTE ON DBMS_LOGMINER TO logminer_user;  -- optional on newer builds
    GRANT SELECT  ON V_$LOGMNR_CONTENTS TO logminer_user;
    GRANT SELECT  ON V_$TRANSACTION     TO logminer_user;  -- open-transaction safety check
    GRANT SELECT  ON GV_$TRANSACTION    TO logminer_user;
    GRANT SELECT  ON V_$ARCHIVED_LOG    TO logminer_user;
    GRANT SELECT  ON V_$LOG             TO logminer_user;
    GRANT SELECT  ON V_$LOGFILE         TO logminer_user;
    GRANT SELECT  ON V_$DATABASE        TO logminer_user;
    GRANT LOGMINING                     TO logminer_user; -- Oracle 12c+
    -- For CDB/PDB: connect to the CDB root and run LogMiner there
    -- or grant C##logminer_user CONTAINER=ALL privileges.

See the Technical Reference documentation:
https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
"""

from __future__ import annotations

import json
import os
import random
import re
import shutil
import sqlite3
import tempfile
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Iterator

import oracledb
from schema import TABLES
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# NUMBER -> decimal.Decimal (exact) and LOB -> str/bytes (no per-LOB round trip).
oracledb.defaults.fetch_decimals = True
oracledb.defaults.fetch_lobs = False


# =============================================================================
# Oracle connection (TLS / thin / thick) and error classification
# =============================================================================

# Errors that will never succeed on retry. Retrying a bad password can lock the account
# (Autonomous locks after 10 failures), so these fail immediately.
_PERMANENT_MARKERS = (
    "ORA-01017",  # invalid username/password
    "ORA-28000",  # account locked
    "ORA-28001",  # password expired
    "ORA-28009",
    "ORA-01045",  # lacks CREATE SESSION
    "ORA-12154",  # cannot resolve connect identifier
    "ORA-12514",  # listener does not know the service
    "ORA-65040",
    "CERTIFICATE_VERIFY_FAILED",
    "DPY-6001",  # service name not registered
    "DPY-3010",  # server version not supported by thin mode
)

# Network / instance blips that a reconnect normally cures.
_TRANSIENT_MARKERS = (
    "DPY-4011",  # database or network closed the connection
    "DPY-4010",
    "DPY-4024",  # call timeout
    "DPY-1001",  # not connected
    "DPY-6005",  # cannot connect (cause checked against _PERMANENT_MARKERS first)
    "DPY-6000",
    "ORA-03113",
    "ORA-03114",
    "ORA-03135",
    "ORA-03136",
    "ORA-12170",
    "ORA-12537",
    "ORA-12541",
    "ORA-12543",
    "ORA-12571",
    "ORA-01012",
    "ORA-02396",  # exceeded maximum idle time
    "ORA-17002",
    "ORA-17008",
    "ORA-17410",
    "ORA-00018",
    "ORA-00020",
    "ORA-01033",
    "ORA-01034",
    "ORA-01089",
    "ORA-44611",  # a cancelled sync's LogMiner session is still being released
)

_MAX_CONNECT_ATTEMPTS = 5
_SESSION_MODULE = "fivetran_oracle_logminer"
_MAX_SYNC_ATTEMPTS = 5


def _is_transient(err: BaseException) -> bool:
    """True when ``err`` is worth retrying on a fresh connection."""
    text = str(err)
    if any(m in text for m in _PERMANENT_MARKERS):
        return False
    if isinstance(err, (ConnectionError, TimeoutError)):
        return True
    return any(m in text for m in _TRANSIENT_MARKERS)


def _backoff_delay(attempt: int, base: float = 2.0, cap: float = 60.0) -> float:
    """Exponential backoff with jitter: ~2s, 4s, 8s ... capped."""
    return min(cap, base * (2 ** (attempt - 1))) * (0.75 + random.random() / 2)


def _ora_code(err: BaseException) -> int | None:
    """Return the ORA- error number carried by an oracledb exception, if any."""
    if err.args and hasattr(err.args[0], "code"):
        return err.args[0].code
    m = re.search(r"ORA-(\d{5})", str(err))
    return int(m.group(1)) if m else None


def build_dsn(host: str, port: str, service_name: str) -> str:
    """
    Build an Oracle connect descriptor for a TLS (TCPS) listener.

    ``SSL_SERVER_DN_MATCH=YES`` verifies the server certificate. Connect/transport timeouts stop a
    black-holed network from hanging a sync; the outer retry loop handles reconnects.
    """
    return (
        f"(DESCRIPTION="
        f"(RETRY_COUNT=3)(RETRY_DELAY=2)"
        f"(CONNECT_TIMEOUT=30)(TRANSPORT_CONNECT_TIMEOUT=30)"
        f"(ADDRESS=(PROTOCOL=TCPS)(HOST={host})(PORT={port}))"
        f"(CONNECT_DATA=(SERVICE_NAME={service_name}))"
        f"(SECURITY=(SSL_SERVER_DN_MATCH=YES))"
        f")"
    )


def init_oracle_client(thick_mode: bool, client_lib_dir: str | None = None) -> None:
    """Initialise the Oracle client library if thick mode is requested (once per process)."""
    if thick_mode and oracledb.is_thin_mode():
        try:
            oracledb.init_oracle_client(lib_dir=client_lib_dir or None)
            log.info("Oracle thick mode initialised")
        except oracledb.Error as err:
            raise RuntimeError(
                f"Failed to initialise Oracle thick client: {err}. "
                "Ensure Oracle Instant Client is installed and the path is correct, "
                "or set oracle_thick_mode to false."
            ) from err


def get_connection(configuration: dict) -> oracledb.Connection:
    """Open an Oracle connection, retrying transient failures with backoff."""
    thick_mode = _bool_config(configuration, "oracle_thick_mode", False)
    client_lib_dir = configuration.get("oracle_client_lib_dir") or None
    init_oracle_client(thick_mode=thick_mode, client_lib_dir=client_lib_dir)

    host = configuration["host"].strip()
    port = str(configuration.get("port", "1521")).strip()
    service_name = configuration["service_name"].strip()

    dsn = build_dsn(host=host, port=port, service_name=service_name)
    log.info(f"Connecting to Oracle at {host}:{port}/{service_name}")

    for attempt in range(1, _MAX_CONNECT_ATTEMPTS + 1):
        try:
            conn = oracledb.connect(
                user=configuration["username"],
                password=configuration["password"],
                dsn=dsn,
                tcp_connect_timeout=30,
                expire_time=2,  # TCP keepalive probe every 2 min: survives idle-killing firewalls
            )
            conn.module = _SESSION_MODULE  # lets a later sync recognise an orphan of this connector
            log.info("Oracle connection established")
            return conn
        except (oracledb.Error, OSError) as err:
            if not _is_transient(err):
                hint = ""
                if "CERTIFICATE_VERIFY_FAILED" in str(err):
                    hint = (
                        " The TLS certificate chain is not trusted: check for a TLS-inspecting "
                        "proxy/VPN, or whether the database requires mutual TLS (wallet)."
                    )
                elif "ORA-01017" in str(err):
                    hint = " Check username/password (not retried, to avoid locking the account)."
                raise RuntimeError(
                    f"Cannot connect to Oracle at {host}:{port}/{service_name}: {err}.{hint}"
                ) from err
            if attempt == _MAX_CONNECT_ATTEMPTS:
                raise RuntimeError(
                    f"Cannot connect to Oracle at {host}:{port}/{service_name} after "
                    f"{attempt} attempts: {err}"
                ) from err
            delay = _backoff_delay(attempt)
            log.warning(
                f"Oracle connect attempt {attempt}/{_MAX_CONNECT_ATTEMPTS} failed: {err}; "
                f"retrying in {delay:.0f}s"
            )
            time.sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover


# =============================================================================
# Schema discovery (ALL_TABLES, primary keys, column types)
# =============================================================================

# Oracle caps an IN list at 1000 expressions (ORA-01795); stay well below it.
_IN_LIST_CHUNK = 500


def _chunks(items: list[str], size: int = _IN_LIST_CHUNK) -> Iterator[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _in_clause(prefix: str, names: list[str]) -> tuple[str, dict[str, str]]:
    """Return ``(":p0, :p1, ...", binds)`` for an IN list."""
    binds = {f"{prefix}{i}": name.upper() for i, name in enumerate(names)}
    return ", ".join(f":{k}" for k in binds), binds


def _get_primary_keys(
    conn: oracledb.Connection,
    schema: str,
    table_names: list[str],
) -> dict[str, list[str]]:
    """Primary keys come from schema.py, not the data dictionary."""
    wanted = {t.upper() for t in table_names}
    return {
        t["table"].upper(): [c.upper() for c in t["primary_key"]]
        for t in TABLES
        if t["table"].upper() in wanted
    }


def _list_tables(
    conn: oracledb.Connection,
    schema: str,
    tables_filter: list[str],
) -> list[str]:
    """Tables come from schema.py; ``tables_filter`` (optional) narrows them."""
    names = [t["table"].upper() for t in TABLES]
    if tables_filter and tables_filter != ["*"]:
        names = [n for n in names if n in set(tables_filter)]
    return sorted(names)


# --- column metadata --------------------------------------------------------

_DEC_UNCONSTRAINED = {"type": "DECIMAL", "precision": 38, "scale": 10}
_LONG_MAX_PRECISION = 18


def _classify_column(
    data_type: str | None, precision: int | None, scale: int | None
) -> tuple[str, Any]:
    """
    Map an Oracle column to ``(kind, declared_type)``.

    ``kind`` drives value coercion so snapshot and LogMiner rows have identical Python types.
    ``declared_type`` is the Fivetran type declared in ``schema()``; ``None`` leaves the type to
    inference (used where Oracle semantics, e.g. time zones, cannot be mapped losslessly).
    """
    dt = (data_type or "").upper()
    if dt == "NUMBER":
        if scale is None and precision is None:
            return "decimal", dict(_DEC_UNCONSTRAINED)
        s = 0 if scale is None else int(scale)
        if s < 0:
            return "double", "DOUBLE"
        if s == 0 and precision is not None and int(precision) <= _LONG_MAX_PRECISION:
            return "long", "LONG"
        p = 38 if precision is None else int(precision)
        p = min(38, max(p, s))
        if s > 37:
            return "double", "DOUBLE"
        return "decimal", {"type": "DECIMAL", "precision": p, "scale": s}
    if dt in ("FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"):
        return "double", "DOUBLE"
    if dt == "DATE":
        return "datetime", "NAIVE_DATETIME"
    if dt.startswith("TIMESTAMP"):
        if "WITH" in dt:
            return "tz_datetime", None
        return "datetime", "NAIVE_DATETIME"
    if dt in ("CHAR", "NCHAR", "VARCHAR2", "NVARCHAR2", "CLOB", "NCLOB", "LONG"):
        return "string", "STRING"
    if dt in ("RAW", "BLOB", "LONG RAW"):
        return "binary", "BINARY"
    return "other", None


class _ColumnMeta:
    """Per-table column kinds, declared Fivetran types and the columns redo is expected to carry."""

    def __init__(self) -> None:
        self.kinds: dict[str, str] = {}
        self.declared: dict[str, Any] = {}
        self.required: set[str] = set()  # non-virtual columns: present in every full row image

    def add(self, name: str, data_type, precision, scale, virtual: bool, hidden: bool) -> None:
        if hidden:
            return
        kind, declared = _classify_column(data_type, precision, scale)
        self.kinds[name] = kind
        if declared is not None:
            self.declared[name] = declared
        if not virtual:
            self.required.add(name)


def _load_column_meta(
    conn: oracledb.Connection, schema: str, table_names: list[str]
) -> dict[str, _ColumnMeta]:
    """Read column types for ``table_names`` from ALL_TAB_COLS (which, unlike ALL_TAB_COLUMNS, exposes HIDDEN_COLUMN)."""
    owner = schema.upper()
    meta: dict[str, _ColumnMeta] = {}
    for chunk in _chunks(table_names):
        placeholders, binds = _in_clause("t", chunk)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE,
                       VIRTUAL_COLUMN, HIDDEN_COLUMN
                FROM   ALL_TAB_COLS
                WHERE  OWNER = :owner AND TABLE_NAME IN ({placeholders})
                ORDER BY TABLE_NAME, COLUMN_ID
                """,
                {"owner": owner, **binds},
            )
            for tbl, col, dtype, prec, scale, virt, hid in cur:
                meta.setdefault(tbl, _ColumnMeta()).add(
                    col,
                    dtype,
                    None if prec is None else int(prec),
                    None if scale is None else int(scale),
                    virt == "YES",
                    hid == "YES",
                )
    return meta


def discover_schemas(
    conn: oracledb.Connection,
    schema: str,
    tables_filter: list[str],
    declare_types: bool = True,
) -> list[dict]:
    """Return Fivetran schema dicts: ``table``, ``primary_key`` and (optionally) ``columns``."""
    schema_upper = schema.upper()
    table_names = _list_tables(conn=conn, schema=schema_upper, tables_filter=tables_filter)

    if not table_names:
        raise RuntimeError(
            f"No tables found in schema {schema_upper!r} "
            f"(filter: {tables_filter}). "
            "Check that the schema name and table list in the configuration are correct."
        )

    log.info(f"Discovered {len(table_names)} table(s) in schema {schema_upper}")

    pk_map = _get_primary_keys(conn=conn, schema=schema_upper, table_names=table_names)
    meta = _load_column_meta(conn, schema_upper, table_names) if declare_types else {}

    schemas = []
    no_pk = []
    for table_name in table_names:
        entry: dict[str, Any] = {
            "table": table_name,
            "primary_key": pk_map.get(table_name, []),
        }
        if not entry["primary_key"]:
            no_pk.append(table_name)
        cm = meta.get(table_name)
        if cm and cm.declared:
            entry["columns"] = dict(cm.declared)
        schemas.append(entry)

    if no_pk:
        log.warning(
            f"{len(no_pk)} table(s) have no primary key (e.g. {no_pk[:5]}). Fivetran keys them "
            "by a row hash, so UPDATE and DELETE cannot be matched to the original row. Add a "
            "primary key in Oracle for reliable change capture."
        )
    return schemas


# =============================================================================
# Value conversion (snapshot rows and LogMiner rows share these)
# =============================================================================


def _serialize_cell(value: Any) -> Any:
    """Generic fallback: convert Oracle driver values to JSON-friendly types."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, Decimal):
        return format(value, "f")
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if hasattr(value, "read"):
        data = value.read()
        return data if isinstance(data, (str, bytes)) else str(data)
    if isinstance(value, (int, float, str, dict, list)):
        return value
    return str(value)


def _to_long(v: Any) -> int:
    return int(Decimal(v)) if isinstance(v, str) else int(v)


def _to_decimal_str(v: Any) -> str:
    return format(v if isinstance(v, Decimal) else Decimal(str(v)), "f")


def _to_datetime_str(v: Any) -> str:
    if isinstance(v, datetime):
        return v.replace(tzinfo=None).isoformat()
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day).isoformat()
    return str(v)


def _to_binary(v: Any) -> bytes:
    if isinstance(v, str):
        return bytes.fromhex(v)
    return bytes(v)


def _to_string(v: Any) -> str:
    if isinstance(v, str):
        return v
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).decode("utf-8", errors="replace")
    return str(v)


_KIND_CONVERTERS: dict[str, Callable[[Any], Any]] = {
    "long": _to_long,
    "decimal": _to_decimal_str,
    "double": float,
    "datetime": _to_datetime_str,
    "binary": _to_binary,
    "string": _to_string,
}


def _converter_for(kind: str | None) -> Callable[[Any], Any]:
    return _KIND_CONVERTERS.get(kind or "", _serialize_cell)


def _coerce_value(kind: str | None, value: Any) -> Any:
    return None if value is None else _converter_for(kind)(value)


def _qualify_table_name(schema: str, table: str) -> str:
    """Return a safely quoted Oracle identifier pair: ``"OWNER"."TABLE"``."""
    s = schema.upper().replace('"', "")
    t = table.upper().replace('"', "")
    return f'"{s}"."{t}"'


def get_current_scn(conn: oracledb.Connection) -> int:
    """Return the current database SCN from ``V$DATABASE``."""
    with conn.cursor() as cur:
        cur.execute("SELECT CURRENT_SCN FROM V$DATABASE")
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("Could not retrieve CURRENT_SCN from V$DATABASE")
        return int(row[0])


# =============================================================================
# Initial snapshot (SELECT *) with table-level resume
# =============================================================================

_DEFAULT_CHECKPOINT_ROWS = 100_000


def _need_initial_sync(state: dict) -> bool:
    """
    Return True if we should run (or resume) the full-table snapshot.

    * Fresh state (no ``last_scn``): initial sync.
    * Legacy state (``last_scn`` only): incremental (initial already completed).
    * ``initial_sync_done`` False: snapshot still in progress (resume from ``snapshot``).
    * ``initial_sync_done`` True: switch to incremental LogMiner.
    """
    if state.get("initial_sync_done") is True:
        return False
    if state.get("initial_sync_done") is False:
        return True
    return "last_scn" not in state


def _column_names(description) -> list[str]:
    names: list[str] = []
    for i, d in enumerate(description or []):
        name = d[0]
        names.append(str(name).strip('"') if name is not None else f"COL{i}")
    return names


def _run_initial_sync(
    conn: oracledb.Connection,
    schema: str,
    tables_filter: list[str],
    batch_size: int,
    column_meta: dict[str, _ColumnMeta] | None = None,
    done_tables: list[str] | None = None,
    progress: Callable[[list[str]], None] | None = None,
    checkpoint_rows: int = _DEFAULT_CHECKPOINT_ROWS,
) -> None:
    """
    Upsert every row of every monitored table.

    Tables listed in ``done_tables`` are skipped, so an interrupted snapshot resumes at the first
    unfinished table (re-emitting a partly copied table is harmless: upserts are idempotent).
    ``progress(done_tables)`` is called to persist state after each table and every
    ``checkpoint_rows`` rows within one.
    """
    schema_upper = schema.upper()
    table_names = _list_tables(conn, schema_upper, tables_filter)
    if not table_names:
        raise RuntimeError(
            f"No tables found in schema {schema_upper!r} "
            f"(filter: {tables_filter}). "
            "Check that the schema name and table list in the configuration are correct."
        )

    done = [t for t in (done_tables or []) if t in table_names]
    pending = [t for t in table_names if t not in done]
    log.info(
        f"Initial sync: full table scan for {len(pending)} of {len(table_names)} table(s) "
        f"in schema {schema_upper} ({len(done)} already complete)"
    )

    pk_map = _get_primary_keys(conn=conn, schema=schema_upper, table_names=pending)
    meta = column_meta or {}

    total_rows = 0
    skipped_null_pk = 0
    for table_name in pending:
        fq = _qualify_table_name(schema_upper, table_name)
        log.info(f"Initial sync: querying {fq}")
        pks = pk_map.get(table_name, [])
        kinds = meta[table_name].kinds if table_name in meta else {}
        table_rows = 0
        rows_since_checkpoint = 0

        with conn.cursor() as cur:
            cur.arraysize = batch_size
            cur.prefetchrows = batch_size + 1
            try:
                cur.execute(f"SELECT * FROM {fq}")
            except oracledb.Error as err:
                if _ora_code(err) == 942:
                    log.warning(
                        f"Initial sync: skipping {fq} — no SELECT privilege "
                        f"or table does not exist (ORA-00942)"
                    )
                    done.append(table_name)
                    continue
                log.severe(f"Initial sync failed on {fq}: {err}")
                raise

            col_names = _column_names(cur.description)
            convs = [_converter_for(kinds.get(n)) for n in col_names]
            pk_idx = [i for i, n in enumerate(col_names) if n in pks]

            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    if pk_idx and any(row[i] is None for i in pk_idx):
                        skipped_null_pk += 1
                        continue
                    op.upsert(
                        table=table_name,
                        data={
                            n: (None if v is None else c(v))
                            for n, c, v in zip(col_names, convs, row)
                        },
                    )
                table_rows += len(rows)
                rows_since_checkpoint += len(rows)
                if progress and rows_since_checkpoint >= checkpoint_rows:
                    progress(done)
                    rows_since_checkpoint = 0
                    log.info(f"Initial sync: {fq} … {table_rows} row(s) so far")

        total_rows += table_rows
        done.append(table_name)
        log.info(f"Initial sync: {fq} complete — {table_rows} row(s)")
        if progress:
            progress(done)

    if skipped_null_pk:
        log.warning(f"Initial sync: skipped {skipped_null_pk} row(s) with null PK values")
    log.info(
        f"Initial sync: finished — {total_rows} row(s) across {len(pending)} table(s) this run"
    )


# =============================================================================
# SQL_REDO parsing (LogMiner)
# =============================================================================

_INSERT_RE = re.compile(
    r"""insert\s+into\s+"[^"]+"\."[^"]+"\s*\(([^)]+)\)\s*values\s*\((.+)\)\s*;?$""",
    re.IGNORECASE | re.DOTALL,
)

_UPDATE_PREFIX_RE = re.compile(r"""update\s+"[^"]+"\."[^"]+"\s+set\s+""", re.IGNORECASE)
_DELETE_PREFIX_RE = re.compile(r"""delete\s+from\s+"[^"]+"\."[^"]+"\s+where\s+""", re.IGNORECASE)

_TO_DATE_RE = re.compile(
    r"""TO_DATE\s*\(\s*'([^']*)'\s*(?:,\s*'([^']*)'\s*)?(?:,\s*'[^']*'\s*)?\)""",
    re.IGNORECASE,
)
_TO_TIMESTAMP_RE = re.compile(
    r"""TO_TIMESTAMP\s*\(\s*'([^']*)'\s*(?:,\s*'([^']*)'\s*)?\)""",
    re.IGNORECASE,
)
_TO_TIMESTAMP_TZ_RE = re.compile(
    r"""TO_TIMESTAMP_TZ\s*\(\s*'([^']*)'\s*(?:,\s*'([^']*)'\s*)?\)""",
    re.IGNORECASE,
)
_TIMESTAMP_LIT_RE = re.compile(r"""TIMESTAMP\s+'([^']*)'""", re.IGNORECASE)
_DATE_LIT_RE = re.compile(r"""DATE\s+'([^']*)'""", re.IGNORECASE)
_INTERVAL_RE = re.compile(r"""TO_(?:YM|DS)INTERVAL\s*\(\s*'([^']*)'\s*\)""", re.IGNORECASE)
_HEXTORAW_RE = re.compile(r"""HEXTORAW\s*\(\s*'([^']*)'\s*\)""", re.IGNORECASE)
_UNISTR_RE = re.compile(r"""UNISTR\s*\(\s*'(.*)'\s*\)""", re.IGNORECASE | re.DOTALL)
_EMPTY_LOB_RE = re.compile(r"""EMPTY_(?:CLOB|BLOB)\s*\(\s*\)""", re.IGNORECASE)
_NUMBER_RE = re.compile(r"""[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?|[+-]?(?:inf|nan)""", re.I)
_ISO_TS_RE = re.compile(
    r"""(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(\.\d+)?\s*(Z|[+-]\d{2}:?\d{2})?(?:\s+\S+)?""",
)
_PLACEHOLDER_COL_RE = re.compile(r"""COL\s*\d+""", re.IGNORECASE)


class _EmptyLob:
    """Marks an EMPTY_CLOB()/EMPTY_BLOB() literal: the real LOB bytes are not in SQL_REDO."""

    def __repr__(self) -> str:  # pragma: no cover
        return "EMPTY_LOB"


_EMPTY_LOB = _EmptyLob()


def _split_csv(text: str) -> list[str]:
    tokens: list[str] = []
    depth = 0
    in_str = False
    buf: list[str] = []

    i = 0
    while i < len(text):
        ch = text[i]

        if in_str:
            buf.append(ch)
            if ch == "'":
                if i + 1 < len(text) and text[i + 1] == "'":
                    buf.append("'")
                    i += 2
                    continue
                in_str = False
        elif ch == "'":
            in_str = True
            buf.append(ch)
        elif ch == "(":
            depth += 1
            buf.append(ch)
        elif ch == ")":
            depth -= 1
            buf.append(ch)
        elif ch == "," and depth == 0:
            tokens.append("".join(buf).strip())
            buf = []
            i += 1
            continue
        else:
            buf.append(ch)

        i += 1

    if buf:
        tokens.append("".join(buf).strip())

    return tokens


_FORMAT_TOKENS = (
    ("HH24", "%H"), ("HH12", "%I"), ("RRRR", "%Y"), ("YYYY", "%Y"), ("MON", "%b"),
    ("HH", "%I"), ("YY", "%y"), ("RR", "%y"), ("MM", "%m"), ("DD", "%d"), ("MI", "%M"),
    ("SS", "%S"), ("AM", "%p"), ("PM", "%p"),
)
_FORMAT_TOKEN_RE = re.compile(
    r"FF\d*|" + "|".join(t for t, _ in _FORMAT_TOKENS), re.IGNORECASE
)
_FORMAT_MAP = dict(_FORMAT_TOKENS)


def _oracle_format_to_strptime(fmt: str) -> str:
    def repl(m: re.Match) -> str:
        tok = m.group(0).upper()
        return "%f" if tok.startswith("FF") else _FORMAT_MAP[tok]

    out, last = [], 0
    for m in _FORMAT_TOKEN_RE.finditer(fmt):
        out.append(fmt[last : m.start()].replace("%", "%%"))
        out.append(repl(m))
        last = m.end()
    out.append(fmt[last:].replace("%", "%%"))
    return "".join(out)


_DEFAULT_NLS_FORMATS = (
    "%Y-%m-%d",
    "%d-%b-%y %I.%M.%S.%f %p",
    "%d-%b-%y %I.%M.%S %p",
    "%d-%b-%y %H.%M.%S.%f",
    "%d-%b-%y %H.%M.%S",
    "%d-%b-%y",
    "%d-%b-%Y %I.%M.%S.%f %p",
    "%d-%b-%Y %I.%M.%S %p",
    "%d-%b-%Y",
)


def _fix_rr_century(dt: datetime) -> datetime:
    """Oracle RR maps 50-99 to 19xx and 00-49 to 20xx; Python's %y maps 69-99 / 00-68."""
    return dt.replace(year=dt.year - 100) if 2050 <= dt.year <= 2068 else dt


def _normalise_datetime_literal(literal: str, fmt: str | None = None) -> str:
    """
    Convert an Oracle date/timestamp literal to ISO-8601 (``YYYY-MM-DDTHH:MM:SS[.ffffff][±HH:MM]``).

    Raises ``ValueError`` for anything it cannot interpret, so a committed change is never
    stored with a mangled value.
    """
    literal = literal.strip()
    iso_fmt = fmt is None or fmt.upper().startswith(("YYYY-MM-DD", "RRRR-MM-DD"))
    m = _ISO_TS_RE.fullmatch(literal)
    if m and iso_fmt:
        # ISO-shaped literal (LogMiner's default rendering): no format translation needed.
        day, clock, frac, tz = m.groups()
        datetime.strptime(f"{day} {clock}", "%Y-%m-%d %H:%M:%S")  # validate ranges
        if frac:
            frac = "." + frac[1:7]
        if tz:
            tz = "+00:00" if tz == "Z" else (tz if ":" in tz else f"{tz[:3]}:{tz[3:]}")
        return f"{day}T{clock}{frac or ''}{tz or ''}"
    lit = re.sub(r"(\.\d{6})\d+", r"\1", literal)  # %f accepts at most 6 digits
    if fmt is None:
        # No format given: Oracle used the session default (NLS_DATE/TIMESTAMP_FORMAT, RR years).
        for py_fmt in _DEFAULT_NLS_FORMATS:
            try:
                return _fix_rr_century(datetime.strptime(lit, py_fmt)).isoformat()
            except ValueError:
                continue
        raise ValueError(f"Cannot interpret date/time literal {literal!r}")

    parsed = datetime.strptime(lit, _oracle_format_to_strptime(fmt))
    if "RR" in fmt.upper().replace("RRRR", ""):
        parsed = _fix_rr_century(parsed)
    return parsed.isoformat()


def _unescape_unistr(text: str) -> str:
    text = text.replace("''", "'")
    return re.sub(
        r"\\(\\|[0-9A-Fa-f]{4})",
        lambda m: "\\" if m.group(1) == "\\" else chr(int(m.group(1), 16)),
        text,
    )


def _parse_value(raw: str) -> Any:
    """Parse one SQL literal from SQL_REDO. Raises ``ValueError`` for an unknown literal form."""
    raw = raw.strip()

    if raw.upper() == "NULL":
        return None

    if _EMPTY_LOB_RE.fullmatch(raw):
        return _EMPTY_LOB

    if raw.startswith("'") and raw.endswith("'") and len(raw) >= 2:
        return raw[1:-1].replace("''", "'")

    m = _TO_DATE_RE.fullmatch(raw)
    if m:
        return _normalise_datetime_literal(m.group(1), m.group(2))

    m = _TO_TIMESTAMP_TZ_RE.fullmatch(raw) or _TO_TIMESTAMP_RE.fullmatch(raw)
    if m:
        return _normalise_datetime_literal(m.group(1), m.group(2))

    m = _TIMESTAMP_LIT_RE.fullmatch(raw) or _DATE_LIT_RE.fullmatch(raw)
    if m:
        return _normalise_datetime_literal(m.group(1))

    m = _INTERVAL_RE.fullmatch(raw)
    if m:
        return m.group(1)

    m = _HEXTORAW_RE.fullmatch(raw)
    if m:
        return m.group(1)

    m = _UNISTR_RE.fullmatch(raw)
    if m:
        return _unescape_unistr(m.group(1))

    if _NUMBER_RE.fullmatch(raw):
        return raw

    raise ValueError(f"Unrecognised SQL_REDO literal: {raw[:120]!r}")


def _parse_column_name(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith('"') and raw.endswith('"'):
        return raw[1:-1]
    return raw


def _normalise_and_to_comma(text: str) -> str:
    result: list[str] = []
    depth = 0
    in_str = False
    i = 0

    while i < len(text):
        ch = text[i]

        if in_str:
            result.append(ch)
            if ch == "'":
                if i + 1 < len(text) and text[i + 1] == "'":
                    result.append("'")
                    i += 2
                    continue
                in_str = False
        elif ch == "'":
            in_str = True
            result.append(ch)
        elif ch == "(":
            depth += 1
            result.append(ch)
        elif ch == ")":
            depth -= 1
            result.append(ch)
        elif depth == 0 and text[i : i + 4].upper() == " AND":
            result.append(",")
            i += 4
            continue
        else:
            result.append(ch)

        i += 1

    return "".join(result)


def _parse_assignment_list(text: str) -> dict[str, Any]:
    normalised = _normalise_and_to_comma(text)
    pairs = _split_csv(normalised)

    result: dict[str, Any] = {}
    for pair in pairs:
        pair = pair.strip()
        m_null = re.fullmatch(r"(.+?)\s+IS\s+NULL", pair, re.IGNORECASE | re.DOTALL)
        if m_null:  # supplemental logging writes NULL columns as "COL" IS NULL
            col = _parse_column_name(m_null.group(1))
            if col.upper() != "ROWID":
                result[col] = None
            continue
        eq_idx = pair.find("=")
        if eq_idx == -1:
            continue
        col_raw = pair[:eq_idx].strip()
        val_raw = pair[eq_idx + 1 :].strip()

        col = _parse_column_name(col_raw)

        if col.upper() == "ROWID":
            continue

        result[col] = _parse_value(val_raw)

    return result


def parse_insert(sql_redo: str) -> dict[str, Any]:
    m = _INSERT_RE.match(sql_redo.strip())
    if not m:
        raise ValueError(f"Cannot parse INSERT SQL_REDO: {sql_redo[:200]!r}")

    columns_raw = m.group(1)
    values_raw = m.group(2)

    columns = [_parse_column_name(c) for c in _split_csv(columns_raw)]
    values = [_parse_value(v) for v in _split_csv(values_raw)]

    if len(columns) != len(values):
        raise ValueError(
            f"INSERT column count ({len(columns)}) != value count ({len(values)}): "
            f"{sql_redo[:200]!r}"
        )

    return dict(zip(columns, values))


def _find_top_level(text: str, keyword: str) -> int:
    """
    Index of ``keyword`` (surrounded by whitespace) outside string literals, quoted identifiers
    and parentheses, or -1. A regex cannot do this: a value like ``'went where it went'`` would
    otherwise split the statement in the wrong place.
    """
    kw = keyword.lower()
    n = len(kw)
    in_str = in_ident = False
    depth = 0
    i = 0
    while i < len(text):
        ch = text[i]
        if in_str:
            if ch == "'":
                if text[i + 1 : i + 2] == "'":
                    i += 2
                    continue
                in_str = False
        elif in_ident:
            if ch == '"':
                in_ident = False
        elif ch == "'":
            in_str = True
        elif ch == '"':
            in_ident = True
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif (
            depth == 0 and
            ch.isspace() and
            text[i + 1 : i + 1 + n].lower() == kw and
            text[i + 1 + n : i + 2 + n].isspace()
        ):
            return i + 1
        i += 1
    return -1


def parse_update_parts(sql_redo: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return ``(where_values, set_values)`` of an UPDATE (before-image keys, after-image changes)."""
    body = sql_redo.strip().rstrip(";").rstrip()
    m = _UPDATE_PREFIX_RE.match(body)
    if not m:
        raise ValueError(f"Cannot parse UPDATE SQL_REDO: {sql_redo[:200]!r}")
    rest = body[m.end() :]
    idx = _find_top_level(rest, "where")
    if idx < 0:
        raise ValueError(f"UPDATE SQL_REDO has no WHERE clause: {sql_redo[:200]!r}")
    return _parse_assignment_list(rest[idx + 5 :]), _parse_assignment_list(rest[:idx])


def parse_update(sql_redo: str) -> dict[str, Any]:
    # WHERE carries the PK value(s) when they are not in the SET clause; SET values win.
    where_data, set_data = parse_update_parts(sql_redo)
    return {**where_data, **set_data}


def parse_delete(sql_redo: str) -> dict[str, Any]:
    body = sql_redo.strip().rstrip(";").rstrip()
    m = _DELETE_PREFIX_RE.match(body)
    if not m:
        raise ValueError(f"Cannot parse DELETE SQL_REDO: {sql_redo[:200]!r}")
    return _parse_assignment_list(body[m.end() :])


def parse_sql_redo(operation: str, sql_redo: str) -> dict[str, Any]:
    op = operation.upper()
    if op == "INSERT":
        return parse_insert(sql_redo)
    elif op == "UPDATE":
        return parse_update(sql_redo)
    elif op == "DELETE":
        return parse_delete(sql_redo)
    else:
        raise ValueError(f"Unsupported LogMiner operation: {operation!r}")


# =============================================================================
# LogMiner session (DBMS_LOGMNR / V$LOGMNR_CONTENTS)
# =============================================================================
#
# Uses the classic DBMS_LOGMNR package (ADD_LOGFILE / START_LOGMNR / END_LOGMNR)
# with DICT_FROM_ONLINE_CATALOG. COMMITTED_DATA_ONLY is deliberately NOT used: it makes Oracle
# buffer whole in-flight transactions in PGA (ORA-04036) and cannot be windowed safely. Instead
# the connector buffers transactions itself (see _TxnBuffer) and applies them in commit order.
# SCN bounds are applied when reading V$LOGMNR_CONTENTS (START_LOGMNR has no SCN args).

_CONTENT_COLUMNS = (
    "SCN",
    "COMMIT_SCN",
    "TIMESTAMP",
    "OPERATION",
    "SEG_OWNER",
    "TABLE_NAME",
    "SQL_REDO",
    "SQL_UNDO",
    "ROW_ID",
    "XID",
    "STATUS",
    "USERNAME",
    "CSF",
)

# SQL_UNDO is only needed for DELETE (it is an INSERT carrying the full row, which recovers the
# primary key when SQL_REDO's WHERE clause lacks it); fetching it for every row would double the
# bytes read.
_CONTENT_SELECT = tuple(
    "CASE WHEN OPERATION = 'DELETE' THEN SQL_UNDO END AS SQL_UNDO" if c == "SQL_UNDO" else c
    for c in _CONTENT_COLUMNS
)

# Operations the connector cannot replicate. They are surfaced as warnings, never silently dropped.
_UNSUPPORTED_OPERATIONS = (
    "DDL",
    "UNSUPPORTED",
    "LOB_WRITE",
    "LOB_TRIM",
    "LOB_ERASE",
    "SEL_LOB_LOCATOR",
)

_UNSUPPORTED_SQL = ", ".join(f"'{o}'" for o in _UNSUPPORTED_OPERATIONS)

_LOG_SQL_MAX_LEN = 4000

# Autonomous Database keeps archived redo for 7 days.
_ARCHIVE_RETENTION_DAYS = 7

# COMMIT/ROLLBACK rows (any schema) are needed to resolve transactions; DML is limited to the
# replicated schema/tables. V$LOGMNR_CONTENTS already returns rows in SCN order, so ORDER BY is
# opt-in (it forces a sort in PGA); _consume_window verifies the order instead.
_CONTENT_QUERY_TEMPLATE = """
SELECT {cols}
FROM   V$LOGMNR_CONTENTS
WHERE  SCN BETWEEN :mine_start_scn AND :mine_end_scn
  AND  (   OPERATION IN ('COMMIT', 'ROLLBACK')
        OR (    OPERATION IN ('INSERT', 'UPDATE', 'DELETE', {unsupported})
            AND SEG_OWNER = :schema
            {table_filter}))
{order_by}
"""


class LogMinerManager:
    """Manages a single Oracle LogMiner session lifecycle (DBMS_LOGMNR)."""

    def __init__(
        self,
        connection: oracledb.Connection,
        ordered_query: bool = False,
        kill_orphans: bool = True,
    ) -> None:
        self._kill_orphans = kill_orphans
        self._conn = connection
        self._ordered_query = ordered_query
        self._started = False
        self._mine_start_scn: int | None = None
        self._mine_end_scn: int | None = None
        self._is_pdb: bool | None = None

    def __enter__(self) -> LogMinerManager:
        return self

    def __exit__(self, *_) -> None:
        self.stop_quietly()

    def get_current_scn(self) -> int:
        return get_current_scn(self._conn)

    def is_pdb(self) -> bool:
        """True when connected to a pluggable database (CON_ID > 1)."""
        if self._is_pdb is None:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT SYS_CONTEXT('USERENV','CON_ID'), "
                    "SYS_CONTEXT('USERENV','CON_NAME') FROM DUAL"
                )
                con_id, con_name = cur.fetchone()
            self._is_pdb = int(con_id or 0) > 1
            log.info(f"Connected to container {con_name!r} (CON_ID={con_id}); pdb={self._is_pdb}")
        return self._is_pdb

    def check_logmining_privilege(self) -> None:
        """Raise a readable error if the connected user lacks LOGMINING."""
        with self._conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM SESSION_PRIVS WHERE PRIVILEGE = 'LOGMINING'")
            (has_priv,) = cur.fetchone()
        if not has_priv:
            raise RuntimeError(
                "The connection user lacks the LOGMINING privilege required to run LogMiner. "
                "Ask a DBA to run: GRANT LOGMINING TO <user>; "
                "(also GRANT EXECUTE ON DBMS_LOGMNR and SELECT ON V_$LOGMNR_CONTENTS)."
            )

    def check_scn_within_archive_window(self, scn: int) -> None:
        """Raise if ``scn`` is older than the archived-log retention window."""
        too_old_msg = (
            f"The redo needed to resume (starting at SCN {scn}) is older than the "
            f"{_ARCHIVE_RETENTION_DAYS}-day archived redo retention window and is no longer "
            "available. Reset the connector state (clear last_scn) and run a fresh initial sync."
        )
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT CAST(SYSTIMESTAMP AS DATE) - CAST(SCN_TO_TIMESTAMP(:s) AS DATE) "
                    "FROM DUAL",
                    s=scn,
                )
                (age_days,) = cur.fetchone()
        except oracledb.DatabaseError as err:
            # ORA-08181: SCN not a valid SCN / too old to map to a timestamp.
            if "ORA-08181" in str(err):
                raise RuntimeError(too_old_msg) from err
            raise
        if age_days is not None and float(age_days) > _ARCHIVE_RETENTION_DAYS:
            raise RuntimeError(too_old_msg)

    def _add_log_files(
        self,
        start_scn: int,
        end_scn: int,
        max_archived_logs: int,
    ) -> int:
        """
        Add every redo file overlapping the SCN window and verify there is no sequence gap.

        ``max_archived_logs`` is accepted for backward compatibility but ignored: silently
        dropping older logs would lose changes, and SCN windowing already bounds the count.
        """
        log_files: list[str] = []
        thread_seqs: dict[int, set[int]] = {}
        # If we ADD an archivelog for (THREAD#, SEQUENCE#), adding the online member for
        # the same sequence raises ORA-01289 — Oracle treats it as one logical redo stream.
        archived_thread_seq: set[tuple[int, int]] = set()

        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT NAME,
                       COALESCE(THREAD#, 0) AS thread_num,
                       SEQUENCE#
                FROM   V$ARCHIVED_LOG
                WHERE  STANDBY_DEST = 'NO'
                  AND  STATUS        = 'A'
                  AND  DEST_ID       = 1
                  AND  FIRST_CHANGE# <= :end_scn
                  AND  NEXT_CHANGE#  >  :start_scn
                ORDER BY THREAD#, SEQUENCE#
                """,
                end_scn=end_scn,
                start_scn=start_scn,
            )
            for row in cur:
                name, thread_num, seq = row[0], row[1], row[2]
                if not name:
                    continue
                log_files.append(name)
                if seq is not None:
                    archived_thread_seq.add((int(thread_num), int(seq)))
                    thread_seqs.setdefault(int(thread_num), set()).add(int(seq))

            # One physical member per log group: multiplexed redo (two paths / group)
            # triggers ORA-01289 if both are added — LogMiner needs a single member per group.
            # PARTITION uses V$LOG.THREAD#/GROUP#; join on GROUP# only (V$LOGFILE has no THREAD# here).
            cur.execute(
                """
                SELECT member, thread_num, seq_num
                FROM (
                    SELECT LF.MEMBER AS member,
                           COALESCE(L.THREAD#, 0) AS thread_num,
                           L.SEQUENCE# AS seq_num,
                           ROW_NUMBER() OVER (
                               PARTITION BY L.THREAD#, L.GROUP#
                               ORDER BY LF.MEMBER
                           ) AS rn
                    FROM   V$LOG     L
                    JOIN   V$LOGFILE LF ON LF.GROUP# = L.GROUP#
                    WHERE  L.STATUS        IN ('CURRENT', 'ACTIVE')
                      AND  L.FIRST_CHANGE# <= :end_scn
                      AND  LF.STATUS IS NULL
                )
                WHERE rn = 1
                """,
                end_scn=end_scn,
            )
            for row in cur:
                member, thread_num, seq = row[0], row[1], row[2]
                if not member:
                    continue
                if seq is not None:
                    key = (int(thread_num), int(seq))
                    thread_seqs.setdefault(key[0], set()).add(key[1])
                    if key in archived_thread_seq:
                        log.fine(
                            f"Skipping online redo {member} (THREAD# {thread_num}, SEQ# {seq} "
                            "already covered by an added archivelog)"
                        )
                        continue
                log_files.append(member)

        # Same path can appear from V$ARCHIVED_LOG and V$LOGFILE (or duplicate rows);
        # ORA-01289 if ADD_LOGFILE sees it twice in one session.
        seen_paths: set[str] = set()
        unique_files: list[str] = []
        for path in log_files:
            if path and path not in seen_paths:
                seen_paths.add(path)
                unique_files.append(path)
        if len(unique_files) < len(log_files):
            log.fine(
                f"Deduplicated redo paths for LogMiner: {len(log_files)} -> {len(unique_files)}"
            )
        log_files = unique_files

        for thread_num, seqs in thread_seqs.items():
            missing = sorted(set(range(min(seqs), max(seqs) + 1)) - seqs)
            if missing:
                raise RuntimeError(
                    f"Redo gap for thread {thread_num} in SCN range {start_scn}-{end_scn}: "
                    f"missing log sequence(s) {missing[:10]}. Archived logs were removed "
                    "(e.g. by RMAN) before they were mined; the connector cannot guarantee "
                    "completeness. Restore the logs, or reset state and run a fresh initial sync."
                )

        if not log_files:
            raise RuntimeError(
                f"No redo log files found covering SCN range "
                f"{start_scn} – {end_scn}. "
                "Ensure ARCHIVELOG mode is enabled and archived logs are accessible."
            )

        with self._conn.cursor() as cur:
            for idx, logfile in enumerate(log_files):
                if idx == 0:
                    cur.execute(
                        """
                        BEGIN
                            DBMS_LOGMNR.ADD_LOGFILE(
                                LOGFILENAME => :logfile,
                                OPTIONS     => DBMS_LOGMNR.NEW
                            );
                        END;
                        """,
                        logfile=logfile,
                    )
                else:
                    cur.execute(
                        """
                        BEGIN
                            DBMS_LOGMNR.ADD_LOGFILE(
                                LOGFILENAME => :logfile,
                                OPTIONS     => DBMS_LOGMNR.ADDFILE
                            );
                        END;
                        """,
                        logfile=logfile,
                    )
                log.fine(f"Added log file to LogMiner: {logfile}")

        log.info(
            f"Added {len(log_files)} redo file(s) for SCN {start_scn}–{end_scn}"
        )
        return len(log_files)

    def start(
        self,
        start_scn: int,
        end_scn: int,
        max_archived_logs: int = 10,
    ) -> None:
        if self._started:
            raise RuntimeError("LogMiner session is already started; call stop() first.")

        self._mine_start_scn = start_scn
        self._mine_end_scn = end_scn

        log.info(f"Starting LogMiner session (DBMS_LOGMNR): SCN window {start_scn} – {end_scn}")
        try:
            if self.is_pdb():
                # Autonomous / PDB: ADD_LOGFILE raises ORA-65040; Oracle locates redo itself.
                self._start_pdb(start_scn, end_scn)
            else:
                self._add_log_files(
                    start_scn=start_scn,
                    end_scn=end_scn,
                    max_archived_logs=max_archived_logs,
                )
                with self._conn.cursor() as cur:
                    cur.execute(
                        """
                        BEGIN
                            DBMS_LOGMNR.START_LOGMNR(
                                OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
                            );
                        END;
                        """
                    )
        except Exception:
            # A failed start (or a half-built non-PDB log list) must not leak a session.
            self.stop_quietly()
            raise

        self._started = True
        log.info("LogMiner session started (START_LOGMNR)")

    def _start_pdb(self, start_scn: int, end_scn: int) -> None:
        """
        START_LOGMNR on a PDB. Autonomous allows one ad hoc LogMiner session (ORA-44611). A run
        that was cancelled or killed leaves its session behind until Oracle notices the dead
        client, so on ORA-44611 clear our own orphaned sessions and retry instead of failing.
        """
        attempts = 6
        for attempt in range(1, attempts + 1):
            try:
                with self._conn.cursor() as cur:
                    cur.execute(
                        """
                        BEGIN
                            DBMS_LOGMNR.START_LOGMNR(
                                STARTSCN => :start_scn,
                                ENDSCN   => :end_scn,
                                OPTIONS  => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
                            );
                        END;
                        """,
                        start_scn=start_scn,
                        end_scn=end_scn,
                    )
                return
            except oracledb.DatabaseError as err:
                if "ORA-44611" not in str(err) or attempt == attempts:
                    raise
                log.warning(
                    f"Another LogMiner session holds the single Autonomous slot (ORA-44611), "
                    f"attempt {attempt}/{attempts}"
                )
                if self._kill_orphans:
                    self._clear_orphan_sessions()
                time.sleep(5 * attempt)

    def _clear_orphan_sessions(self) -> int:
        """
        Kill earlier sessions of this connector (same user, tagged module, not this session).

        Only sessions tagged with ``_SESSION_MODULE`` are touched, so no other application's
        session can be hit. Needs SELECT on V_$SESSION and ALTER SYSTEM; without them it logs
        what to grant and the caller falls back to waiting.
        """
        killed = 0
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT SID, SERIAL#, STATUS, LAST_CALL_ET
                    FROM   V$SESSION
                    WHERE  USERNAME = SYS_CONTEXT('USERENV', 'SESSION_USER')
                      AND  MODULE   = :module
                      AND  SID     <> TO_NUMBER(SYS_CONTEXT('USERENV', 'SID'))
                    """,
                    module=_SESSION_MODULE,
                )
                orphans = cur.fetchall()
        except oracledb.DatabaseError as err:
            log.warning(
                f"Cannot look for orphaned sessions ({err}). Grant SELECT ON V_$SESSION to this "
                "user, or wait for Oracle to release the earlier session."
            )
            return 0
        for sid, serial, status, idle in orphans:
            for stmt in ("KILL", "DISCONNECT"):
                try:
                    with self._conn.cursor() as cur:
                        cur.execute(f"ALTER SYSTEM {stmt} SESSION '{int(sid)},{int(serial)}' IMMEDIATE")
                    log.warning(
                        f"Ended orphaned connector session sid={sid} serial#={serial} "
                        f"(status={status}, last call {idle}s ago) left by an earlier sync"
                    )
                    killed += 1
                    break
                except oracledb.DatabaseError as err:
                    last = err
            else:
                log.warning(
                    f"Could not end orphaned session {sid},{serial}: {last}. Grant ALTER SYSTEM "
                    "to this user, or end it manually."
                )
        return killed

    def stop(self) -> None:
        self._mine_start_scn = None
        self._mine_end_scn = None
        with self._conn.cursor() as cur:
            cur.execute(
                """
                BEGIN
                    DBMS_LOGMNR.END_LOGMNR();
                END;
                """
            )
        self._started = False
        log.info("LogMiner session stopped (END_LOGMNR)")

    def stop_quietly(self) -> None:
        """END_LOGMNR, swallowing errors (e.g. ORA-01307: no session active)."""
        try:
            self.stop()
        except Exception as err:
            self._started = False
            log.fine(f"LogMiner stop during cleanup: {err}")

    def iter_contents(
        self,
        schema: str,
        tables: list[str] | None = None,
        batch_size: int = 1000,
    ) -> Iterator[dict]:
        """
        Stream V$LOGMNR_CONTENTS rows for the mined window.

        Rows whose SQL_REDO exceeded the 4000-byte column limit arrive as a chain of rows with
        ``CSF = 1``; they are reassembled here so callers always see one complete statement.
        """
        if not self._started:
            raise RuntimeError("LogMiner session is not started; call start() first.")
        if self._mine_start_scn is None or self._mine_end_scn is None:
            raise RuntimeError("LogMiner SCN bounds are not set; call start() first.")

        bind_vars: dict = {
            "schema": schema.upper(),
            "mine_start_scn": self._mine_start_scn,
            "mine_end_scn": self._mine_end_scn,
        }

        # A long table list would exceed Oracle's IN-list limit; filter client side instead.
        client_filter: set[str] | None = None
        table_filter = ""
        if tables and tables != ["*"]:
            if len(tables) <= _IN_LIST_CHUNK:
                placeholders, binds = _in_clause("tbl", tables)
                table_filter = f"AND TABLE_NAME IN ({placeholders})"
                bind_vars.update(binds)
            else:
                client_filter = {t.upper() for t in tables}

        query = _CONTENT_QUERY_TEMPLATE.format(
            cols=", ".join(_CONTENT_SELECT),
            unsupported=_UNSUPPORTED_SQL,
            table_filter=table_filter,
            order_by="ORDER BY SCN, RS_ID, SSN" if self._ordered_query else "",
        )

        keys = [col.lower() for col in _CONTENT_COLUMNS]
        carry: dict | None = None

        with self._conn.cursor() as cur:
            cur.arraysize = batch_size
            cur.prefetchrows = batch_size + 1
            cur.execute(query, bind_vars)

            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    rec = dict(zip(keys, row))
                    if carry is not None:
                        rec["sql_redo"] = (carry["sql_redo"] or "") + (rec["sql_redo"] or "")
                        rec["sql_undo"] = (carry["sql_undo"] or "") + (rec["sql_undo"] or "")
                        for k in ("scn", "timestamp", "operation", "table_name", "row_id", "xid"):
                            rec[k] = carry[k]
                        carry = None
                    if rec["csf"]:
                        carry = rec
                        continue
                    if (
                        client_filter is not None and
                        rec["table_name"] and
                        rec["table_name"] not in client_filter and
                        (rec["operation"] or "").upper() not in ("COMMIT", "ROLLBACK")
                    ):
                        continue
                    yield rec

        if carry is not None:
            raise RuntimeError(
                f"V$LOGMNR_CONTENTS ended in the middle of a continued statement "
                f"(SCN {carry['scn']}); refusing to apply a truncated SQL_REDO."
            )


# =============================================================================
# Configuration validation
# =============================================================================

_REQUIRED_CONFIG_KEYS = [
    "host",
    "service_name",
    "username",
    "password",
    "schema",
]

_UNSAFE_DSN_CHARS = re.compile(r"""[()=\s'"\\]""")


def _bool_config(configuration: dict, key: str, default: bool) -> bool:
    raw = configuration.get(key)
    if raw is None or str(raw).strip() == "":
        return default
    value = str(raw).strip().lower()
    if value in ("true", "1", "yes"):
        return True
    if value in ("false", "0", "no"):
        return False
    raise ValueError(f"Configuration value '{key}' must be 'true' or 'false', got {raw!r}")


def _int_config(configuration: dict, key: str, default: int, minimum: int = 1) -> int:
    raw = configuration.get(key)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        value = int(str(raw).strip())
    except ValueError as err:
        raise ValueError(f"Configuration value '{key}' must be an integer, got {raw!r}") from err
    if value < minimum:
        raise ValueError(f"Configuration value '{key}' must be >= {minimum}, got {value}")
    return value


def validate_configuration(configuration: dict) -> None:
    for key in _REQUIRED_CONFIG_KEYS:
        if not str(configuration.get(key) or "").strip():
            raise ValueError(
                f"Missing required configuration value: '{key}'. "
                "Please supply this in the connector configuration."
            )
    for key in ("host", "service_name"):
        if _UNSAFE_DSN_CHARS.search(configuration[key].strip()):
            raise ValueError(
                f"Configuration value '{key}' contains whitespace or one of ( ) = ' \" \\ "
                "which is not valid in a hostname or service name."
            )
    port_raw = configuration.get("port", "1521")
    try:
        port = int(str(port_raw).strip())
    except (TypeError, ValueError) as err:
        raise ValueError(
            f"Invalid port value: {port_raw!r}. Use an integer from 1 to 65535."
        ) from err
    if port < 1 or port > 65535:
        raise ValueError(f"Port must be between 1 and 65535, got {port}")
    _bool_config(configuration, "oracle_thick_mode", False)


def _parse_tables(tables_config: str) -> list[str]:
    if not tables_config or tables_config.strip() == "*":
        return ["*"]
    names = [t.strip().upper() for t in tables_config.split(",") if t.strip()]
    return list(dict.fromkeys(names)) or ["*"]


def _read_settings(configuration: dict) -> dict[str, Any]:
    """Validate and normalise every tunable once, so a typo fails before any work starts."""
    on_parse_error = str(configuration.get("logminer_on_parse_error", "fail")).strip().lower()
    if on_parse_error not in ("fail", "skip"):
        raise ValueError("logminer_on_parse_error must be 'fail' or 'skip'")
    return {
        "schema": configuration["schema"].strip(),
        "tables_filter": _parse_tables(configuration.get("tables", "*")),
        "batch_size": _int_config(configuration, "batch_size", 1000),
        "checkpoint_rows": _int_config(configuration, "checkpoint_rows", _DEFAULT_CHECKPOINT_ROWS),
        "max_archived_logs": _int_config(configuration, "logminer_max_archived_logs", 10),
        "scn_window": _int_config(
            configuration, "logminer_scn_window", _DEFAULT_SCN_WINDOW, _MIN_SCN_WINDOW
        ),
        "buffer_mem_rows": _int_config(
            configuration, "logminer_buffer_mem_rows", _DEFAULT_BUFFER_MEM_ROWS
        ),
        "open_txn_check": _bool_config(configuration, "logminer_open_txn_check", True),
        "ordered_query": _bool_config(configuration, "logminer_ordered_query", False),
        "declare_types": _bool_config(configuration, "declare_column_types", True),
        "kill_orphans": _bool_config(configuration, "logminer_kill_orphan_session", True),
        "on_parse_error": on_parse_error,
    }


# =============================================================================
# Fivetran SDK: schema
# =============================================================================


def schema(configuration: dict) -> list[dict]:
    """
    Introspect Oracle and return tables, primary keys and (by default) column types.

    Declaring types keeps snapshot rows and LogMiner rows in the same destination column type.
    Set ``declare_column_types`` to ``false`` to let Fivetran infer every type from the data.
    """
    validate_configuration(configuration)
    settings = _read_settings(configuration)

    conn = get_connection(configuration)
    try:
        return discover_schemas(
            conn=conn,
            schema=settings["schema"],
            tables_filter=settings["tables_filter"],
            declare_types=settings["declare_types"],
        )
    finally:
        conn.close()


# =============================================================================
# Fivetran SDK: update
# =============================================================================


# =============================================================================
# Incremental engine: transaction buffering + windowed, crash-safe checkpoints
# =============================================================================
#
# Correctness model (why no change can be lost):
#
# * Changes are applied in COMMIT order. DML rows are buffered per XID (_TxnBuffer, spilling to
#   local disk) and flushed when that XID's COMMIT row is seen; ROLLBACK discards them.
# * State holds two SCNs. ``last_scn``: every transaction committed at or before it has been
#   emitted. ``mine_from_scn`` (<= last_scn + 1): where the next sync must start mining so that
#   every transaction still open at ``last_scn`` is re-read from its first change.
# * ``mine_from_scn`` is the minimum of: the first SCN of every buffered (uncommitted) XID, the
#   SCN read just before sampling GV$TRANSACTION, and the oldest START_SCN of any open
#   transaction in that sample. The last two close the gap for transactions whose redo is not
#   yet readable by LogMiner (still in the log buffer) when a window is mined.
# * Re-mined rows with SCN <= last_scn belong to transactions already emitted unless their COMMIT
#   is > last_scn, so commits are emitted only when ``commit SCN > last_scn``. Replays after a
#   crash are idempotent (upsert / delete by key).

_DEFAULT_SCN_WINDOW = 500_000
_MIN_SCN_WINDOW = 1_000
_DEFAULT_BUFFER_MEM_ROWS = 50_000
_MAX_STATE_OPEN_XIDS = 5_000
_DML_OPERATIONS = ("INSERT", "UPDATE", "DELETE")


def _xid_key(raw: Any) -> str:
    """Normalise a LogMiner / V$TRANSACTION XID (RAW) to an upper-case hex string."""
    if raw is None:
        return ""
    if isinstance(raw, (bytes, bytearray)):
        return bytes(raw).hex().upper()
    return str(raw).upper()


class _TxnBuffer:
    """
    Per-XID buffer of uncommitted DML.

    Small transactions stay in memory; once more than ``mem_row_limit`` rows are buffered the
    largest transactions spill to a temporary SQLite file, so a multi-million-row transaction
    costs disk, not RAM or Oracle PGA.
    """

    def __init__(self, mem_row_limit: int = _DEFAULT_BUFFER_MEM_ROWS) -> None:
        self._limit = max(1, int(mem_row_limit))
        self._mem: dict[str, list[dict]] = {}
        self._mem_rows = 0
        self._spilled: set[str] = set()
        self._first: dict[str, int] = {}
        self._batch: list[tuple[str, str]] = []
        self._dir: str | None = None
        self._db: sqlite3.Connection | None = None

    def _connect(self) -> sqlite3.Connection:
        if self._db is None:
            self._dir = tempfile.mkdtemp(prefix="oracle_txn_buffer_")
            self._db = sqlite3.connect(os.path.join(self._dir, "txn.db"))
            self._db.execute("PRAGMA journal_mode=OFF")
            self._db.execute("PRAGMA synchronous=OFF")
            self._db.execute(
                "CREATE TABLE changes ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, xid TEXT NOT NULL, payload TEXT NOT NULL)"
            )
            self._db.execute("CREATE INDEX changes_xid ON changes (xid)")
        return self._db

    def _flush_batch(self) -> None:
        if self._batch:
            db = self._connect()
            db.executemany("INSERT INTO changes (xid, payload) VALUES (?, ?)", self._batch)
            db.commit()
            self._batch = []

    def _spill_until_under_limit(self) -> None:
        target = int(self._limit * 0.8)
        for xid in sorted(self._mem, key=lambda k: len(self._mem[k]), reverse=True):
            if self._mem_rows <= target:
                break
            rows = self._mem.pop(xid)
            self._mem_rows -= len(rows)
            self._spilled.add(xid)
            self._batch.extend((xid, json.dumps(r)) for r in rows)
        self._flush_batch()

    def add(self, xid: str, rec: dict) -> None:
        self._first.setdefault(xid, int(rec["scn"]))
        if xid in self._spilled:
            self._batch.append((xid, json.dumps(rec)))
            if len(self._batch) >= 1000:
                self._flush_batch()
            return
        self._mem.setdefault(xid, []).append(rec)
        self._mem_rows += 1
        if self._mem_rows > self._limit:
            self._spill_until_under_limit()

    def _drain(self, xid: str) -> Iterator[dict]:
        self._flush_batch()
        db = self._connect()
        for (payload,) in db.execute(
            "SELECT payload FROM changes WHERE xid = ? ORDER BY id", (xid,)
        ):
            yield json.loads(payload)
        db.execute("DELETE FROM changes WHERE xid = ?", (xid,))
        db.commit()

    def pop(self, xid: str) -> Iterator[dict]:
        """Remove ``xid`` and return its buffered rows in redo order."""
        self._first.pop(xid, None)
        rows = self._mem.pop(xid, None)
        if rows is not None:
            self._mem_rows -= len(rows)
            return iter(rows)
        if xid in self._spilled:
            self._spilled.discard(xid)
            return self._drain(xid)
        return iter(())

    def discard(self, xid: str) -> None:
        for _ in self.pop(xid):
            pass

    def pending(self) -> dict[str, int]:
        """Buffered XID -> SCN of its first buffered change."""
        return dict(self._first)

    def reset(self) -> None:
        self._mem.clear()
        self._mem_rows = 0
        self._spilled.clear()
        self._first.clear()
        self._batch = []
        if self._db is not None:
            self._db.execute("DELETE FROM changes")
            self._db.commit()

    def close(self) -> None:
        if self._db is not None:
            self._db.close()
            self._db = None
        if self._dir:
            shutil.rmtree(self._dir, ignore_errors=True)
            self._dir = None


def _capture_floor(
    conn: oracledb.Connection, check_enabled: bool
) -> tuple[int, int, set[str]]:
    """
    Sample open transactions. Returns ``(boundary_scn, floor_scn, open_xids)``.

    ``boundary_scn`` is read just BEFORE the sample, so any transaction not in the sample starts
    after it. ``floor_scn`` = min(boundary, oldest open START_SCN): mining from there can never
    miss the first change of a transaction that was open when the sample was taken.
    """
    boundary = get_current_scn(conn)
    if not check_enabled:
        log.warning(
            "logminer_open_txn_check is disabled: transactions whose redo is not yet readable "
            "when a window is mined can be missed. Re-enable for guaranteed completeness."
        )
        return boundary, boundary, set()

    last_err: Exception | None = None
    for view in ("GV$TRANSACTION", "V$TRANSACTION"):
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT START_SCN, XID FROM {view}")
                rows = cur.fetchall()
        except oracledb.DatabaseError as err:
            if "ORA-00942" in str(err) or "ORA-01031" in str(err):
                last_err = err
                continue
            raise
        starts = [int(r[0]) for r in rows if r[0] is not None]
        xids = {_xid_key(r[1]) for r in rows if r[1] is not None}
        return boundary, min([boundary] + starts), xids
    raise RuntimeError(
        "Cannot read GV$TRANSACTION / V$TRANSACTION, which is required to guarantee no change is "
        "missed. Ask a DBA to run: GRANT SELECT ON V_$TRANSACTION TO <user>; "
        "GRANT SELECT ON GV_$TRANSACTION TO <user>; "
        "(or set logminer_open_txn_check to 'false' to accept the risk)."
    ) from last_err


_TRACK: dict[str, Any] = {"state": None}  # last state handed to op.checkpoint this update()
_WARNED: set[tuple[str, str]] = set()
_PROGRESS_SECONDS = 30


class _Progress:
    """Logs a line every ``_PROGRESS_SECONDS`` so a long window never looks hung."""

    def __init__(self) -> None:
        self.start = self.last = time.monotonic()
        self.read = self.applied = 0

    def tick(self, phase: str, buffered: int = 0) -> None:
        now = time.monotonic()
        if now - self.last < _PROGRESS_SECONDS:
            return
        self.last = now
        rate = (self.read + self.applied) / max(now - self.start, 1e-9)
        log.info(
            f"Window progress ({phase}): {self.read} redo row(s) read, {self.applied} applied, "
            f"{buffered} buffered, {rate:.0f} rows/s"
        )


def _checkpoint(state: dict) -> None:
    """Persist ``state`` and remember it, so a reconnect resumes from the last durable position."""
    op.checkpoint(state)
    _TRACK["state"] = json.loads(json.dumps(state))


def _warn_unsupported(kind: str, rec: dict) -> None:
    """Log (once per operation and table) a change LogMiner reports but we cannot replicate."""
    table = rec.get("table_name") or "?"
    if (kind, table) in _WARNED:
        return
    _WARNED.add((kind, table))
    if kind == "DDL":
        log.warning(
            f"DDL on {table} at SCN {rec.get('scn')}: {(rec.get('sql_redo') or '')[:160]!r}. "
            "Schema changes and TRUNCATE are not replicated by LogMiner mining; re-sync the "
            "table in Fivetran if the destination diverges."
        )
    elif kind == "UNSUPPORTED":
        log.warning(
            f"LogMiner reported an UNSUPPORTED change on {table} at SCN {rec.get('scn')} "
            "(a column type LogMiner cannot mine). That change is not replicated."
        )
    else:
        log.warning(
            f"{kind} on {table} at SCN {rec.get('scn')}: LOB data written without a row UPDATE "
            "is not in SQL_REDO and is not replicated; the LOB column may be stale until the "
            "row is next updated or the table is re-synced."
        )


def _policy_fail(msg: str, on_parse_error: str, cause: BaseException | None = None) -> bool:
    if on_parse_error == "skip":
        log.warning(msg + " (skipped: logminer_on_parse_error=skip)")
        return False
    raise RuntimeError(
        msg + ". Refusing to skip a committed change; fix the cause or set "
        "logminer_on_parse_error to 'skip' to accept the loss."
    ) from cause


def _prepare_values(
    table: str, parsed: dict[str, Any], cm: _ColumnMeta | None
) -> tuple[dict[str, Any], list[str]]:
    """Coerce SQL_REDO literals to the column's type. Returns ``(values, empty_lob_columns)``."""
    out: dict[str, Any] = {}
    lobs: list[str] = []
    kinds = cm.kinds if cm else {}
    for col, val in parsed.items():
        if _PLACEHOLDER_COL_RE.fullmatch(col):
            raise ValueError(
                f"LogMiner returned placeholder column {col!r} for {table}: the table structure "
                "changed after this redo was written (DDL). Re-sync the table"
            )
        if val is _EMPTY_LOB:
            lobs.append(col)
            continue
        out[col] = _coerce_value(kinds.get(col), val)
    return out, lobs


def _apply_record(
    conn: oracledb.Connection,
    schema: str,
    pk_map: dict[str, list[str]],
    record: dict,
    on_parse_error: str,
    meta: dict[str, _ColumnMeta] | None = None,
) -> bool:
    """Apply one committed change. Returns True if it produced an upsert/delete."""
    table_name = record["table_name"]
    operation = (record["operation"] or "").upper()
    sql_redo = record.get("sql_redo") or ""

    if not sql_redo.strip():
        log.fine(
            f"Skipping empty SQL_REDO for {schema}.{table_name} "
            f"(operation={operation}, SCN={record.get('scn')})"
        )
        return False

    cm = meta.get(table_name) if meta else None
    pk_cols = pk_map.get(table_name, [])
    where = f"{schema}.{table_name} SCN={record.get('scn')}"

    try:
        parsed = parse_sql_redo(operation=operation, sql_redo=sql_redo)
        values, lob_cols = _prepare_values(table_name, parsed, cm)
    except (ValueError, TypeError, ArithmeticError) as err:
        return _policy_fail(f"Failed to parse SQL_REDO for {where}: {err}", on_parse_error, err)

    if operation == "DELETE":
        keys = values
        if pk_cols:
            if any(values.get(c) is None for c in pk_cols):
                # SQL_UNDO of a DELETE is an INSERT of the whole row: recover the key from it.
                undo = record.get("sql_undo") or ""
                if undo.strip():
                    try:
                        values = {**values, **_prepare_values(table_name, parse_insert(undo), cm)[0]}
                    except (ValueError, TypeError, ArithmeticError):
                        pass
            missing = [c for c in pk_cols if values.get(c) is None]
            if missing:
                return _policy_fail(
                    f"Cannot determine primary key column(s) {missing} for DELETE at {where}",
                    on_parse_error,
                )
            keys = {c: values[c] for c in pk_cols}
        op.delete(table=table_name, keys=keys)
        return True

    # INSERT / UPDATE: upsert exactly what the redo carries. LOB placeholders are left out rather
    # than sent as NULL, so an unread LOB is never blanked.
    if pk_cols and any(values.get(c) is None for c in pk_cols):
        return _policy_fail(
            f"Primary key {pk_cols} is missing from the redo for {operation} at {where}. Enable "
            "supplemental logging on the table (see README: Supplemental logging). "
            f"SQL_REDO sample: {sql_redo[:300]!r}",
            on_parse_error,
        )
    missing = sorted(c for c in (cm.required if cm else ()) if c not in values)
    if (missing or lob_cols) and ("rowimage", table_name) not in _WARNED:
        _WARNED.add(("rowimage", table_name))
        log.warning(
            f"Row image for {table_name} is incomplete (not in redo: {sorted(set(missing) | set(lob_cols))}); "
            "upserting what the redo carries. If this is every update, the table's supplemental "
            f"logging is not taking effect (see README). SQL_REDO sample: {sql_redo[:300]!r}"
        )

    values["_logminer_scn"] = record.get("scn")
    values["_logminer_timestamp"] = (
        record["timestamp"].isoformat()
        if hasattr(record.get("timestamp"), "isoformat")
        else str(record.get("timestamp"))
    )
    values["_logminer_operation"] = operation
    op.upsert(table=table_name, data=values)
    return True


def _consume_window(
    lm: LogMinerManager,
    buffer: _TxnBuffer,
    apply_row,
    schema: str,
    tables_filter: list[str],
    batch_size: int,
    emitted_scn: int,
    open_xids: set[str] | None,
    filter_below: int | None,
) -> int:
    """Stream one started LogMiner window: buffer DML, flush on COMMIT, drop on ROLLBACK."""
    applied = 0
    prev_scn = -1
    prog = _Progress()
    for rec in lm.iter_contents(
        schema=schema,
        tables=tables_filter if tables_filter != ["*"] else None,
        batch_size=batch_size,
    ):
        scn = int(rec["scn"])
        prog.read += 1
        prog.tick("reading", len(buffer.pending()) if prog.read % 1000 == 0 else 0)
        if scn < prev_scn:
            raise RuntimeError(
                f"V$LOGMNR_CONTENTS returned SCN {scn} after {prev_scn}; rows are out of order. "
                "Set logminer_ordered_query to 'true'."
            )
        prev_scn = scn

        kind = (rec.get("operation") or "").upper()
        xid = _xid_key(rec.get("xid"))
        if kind == "COMMIT":
            if scn > emitted_scn:
                for row in buffer.pop(xid):
                    if apply_row(row):
                        applied += 1
                        prog.applied = applied
                    prog.tick("applying commit")
            else:
                buffer.discard(xid)  # already emitted by an earlier sync
        elif kind == "ROLLBACK":
            buffer.discard(xid)
        elif kind in _UNSUPPORTED_OPERATIONS:
            _warn_unsupported(kind, rec)
        elif kind in _DML_OPERATIONS:
            if (
                open_xids is not None and
                filter_below is not None and
                scn < filter_below and
                xid not in open_xids
            ):
                continue  # belongs to a transaction known to be finished before last_scn
            ts = rec.get("timestamp")
            buffer.add(
                xid,
                {
                    "scn": scn,
                    "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
                    "operation": kind,
                    "table_name": rec["table_name"],
                    "sql_redo": rec.get("sql_redo") or "",
                    "sql_undo": (rec.get("sql_undo") or "") if kind == "DELETE" else "",
                    "row_id": rec.get("row_id"),
                },
            )
    return applied


def _mine_incremental(
    *,
    lm: LogMinerManager,
    schema: str,
    tables_filter: list[str],
    apply_row,
    batch_size: int,
    max_archived_logs: int,
    last_scn: int,
    mine_from: int,
    open_xids: set[str] | None,
    filter_below: int | None,
    target: int,
    boundary_scn: int,
    floor_scn: int,
    sample_xids: set[str],
    window_size: int,
    buffer_mem_rows: int,
    state_extra: dict | None = None,
) -> int:
    """Mine (last_scn, target] in windows, checkpointing after each. Returns changes applied."""
    buffer = _TxnBuffer(buffer_mem_rows)
    ck = (last_scn, mine_from, open_xids, filter_below)  # last durable position
    pos = mine_from
    window = window_size
    total = 0
    try:
        while ck[0] < target:
            emitted, _, c_open, c_filter = ck
            end = min(emitted + window, target)
            try:
                lm.start(start_scn=pos, end_scn=end, max_archived_logs=max_archived_logs)
                applied = _consume_window(
                    lm, buffer, apply_row, schema, tables_filter, batch_size,
                    emitted, c_open, c_filter,
                )
            except oracledb.DatabaseError as err:
                lm.stop_quietly()
                if "ORA-04036" in str(err) and window > _MIN_SCN_WINDOW:
                    window = max(window // 2, _MIN_SCN_WINDOW)
                    log.warning(
                        f"ORA-04036 mining SCN {pos}-{end}; restarting from the last checkpoint "
                        f"with window size {window}"
                    )
                    buffer.reset()
                    pos = ck[1]
                    continue
                raise
            lm.stop_quietly()
            total += applied

            pending = buffer.pending()
            new_mine_from = min([end + 1, floor_scn, *pending.values()])
            xids = set(pending) | sample_xids
            state = {
                **(state_extra or {}),
                "last_scn": end,
                "mine_from_scn": new_mine_from,
                "initial_sync_done": True,
            }
            new_open: set[str] | None = None
            new_filter: int | None = None
            if len(xids) <= _MAX_STATE_OPEN_XIDS:
                new_open = xids
                new_filter = min(end + 1, boundary_scn)
                state["open_xids"] = sorted(xids)
                state["xid_filter_below"] = new_filter
            _checkpoint(state)
            log.info(
                f"Window {pos}-{end} done: {applied} change(s) applied, "
                f"{len(pending)} open txn(s); last_scn={end} mine_from={new_mine_from}"
            )
            ck = (end, new_mine_from, new_open, new_filter)
            pos = end + 1
    finally:
        buffer.close()
    return total


def _check_supplemental_logging(
    conn: oracledb.Connection, schema: str, table_names: list[str]
) -> None:
    """Warn (never fail) when redo will not carry enough to build a full row without a lookup."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT SUPPLEMENTAL_LOG_DATA_MIN, SUPPLEMENTAL_LOG_DATA_PK, "
                "SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE"
            )
            minimal, pk_db, all_db = cur.fetchone()
            cur.execute(
                "SELECT TABLE_NAME, LOG_GROUP_TYPE FROM ALL_LOG_GROUPS WHERE OWNER = :o",
                o=schema.upper(),
            )
            groups: dict[str, set[str]] = {}
            for tbl, kind in cur:
                groups.setdefault(tbl, set()).add(kind)
    except oracledb.Error as err:
        log.warning(f"Could not verify supplemental logging ({err}); skipping the check")
        return

    if minimal not in ("YES", "IMPLICIT"):
        # On Autonomous this view can read NO while table-level logging works, so it is only a
        # note. A real gap shows up as "Primary key ... missing from the redo" with a SQL_REDO sample.
        log.info(
            "V$DATABASE reports database-level minimal supplemental logging as off. If changes "
            "are mined with complete rows this can be ignored; otherwise run "
            "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;"
        )
    if pk_db == "YES" or all_db == "YES":
        return
    wanted = {"PRIMARY KEY LOGGING", "ALL COLUMN LOGGING"}
    lacking = [t for t in table_names if not groups.get(t, set()) & wanted]
    if lacking:
        log.warning(
            f"{len(lacking)} table(s) have no primary-key or all-column supplemental logging "
            f"(e.g. {lacking[:5]}). Without it an UPDATE's redo may lack the primary key (the row cannot be "
            "upserted) or unchanged columns. Run: ALTER TABLE <schema>.<table> "
            "ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;"
        )


def _snapshot(conn: oracledb.Connection, settings: dict, state: dict) -> None:
    """Run (or resume) the initial snapshot, then hand over to incremental mining."""
    schema_name = settings["schema"]
    tables_filter = settings["tables_filter"]

    snap = state.get("snapshot")
    if isinstance(snap, dict) and {"scn", "mine_from", "done_tables"} <= snap.keys():
        try:
            LogMinerManager(conn).check_scn_within_archive_window(int(snap["mine_from"]))
        except RuntimeError:
            log.warning(
                "Saved snapshot position is older than the redo retention window; "
                "restarting the snapshot from scratch."
            )
            snap = None
    else:
        snap = None

    if snap is None:
        # Transactions open at the snapshot may commit mid-scan or after it, so record where the
        # first incremental sync must start mining to catch their earlier changes.
        _, floor_scn, _ = _capture_floor(conn, settings["open_txn_check"])
        snapshot_scn = get_current_scn(conn)
        snap = {
            "scn": snapshot_scn,
            "mine_from": min(floor_scn, snapshot_scn),
            "done_tables": [],
        }
        log.info(f"Initial sync: snapshot SCN baseline={snapshot_scn}, mine_from={snap['mine_from']}")
    else:
        log.info(
            f"Initial sync: resuming snapshot at SCN {snap['scn']} "
            f"({len(snap['done_tables'])} table(s) already complete)"
        )

    table_names = _list_tables(conn, schema_name, tables_filter)
    meta = _load_column_meta(conn, schema_name, table_names)
    _check_supplemental_logging(conn, schema_name, table_names)

    def progress(done: list[str]) -> None:
        _checkpoint(
            {"initial_sync_done": False, "snapshot": {**snap, "done_tables": list(done)}}
        )

    _run_initial_sync(
        conn=conn,
        schema=schema_name,
        tables_filter=tables_filter,
        batch_size=settings["batch_size"],
        column_meta=meta,
        done_tables=snap["done_tables"],
        progress=progress,
        checkpoint_rows=settings["checkpoint_rows"],
    )
    _checkpoint(
        {
            "last_scn": snap["scn"],
            "mine_from_scn": snap["mine_from"],
            "initial_sync_done": True,
            "snapshot_tables": table_names,
        }
    )
    log.info("Initial sync: checkpoint acknowledged — update() returning")


def _incremental(conn: oracledb.Connection, settings: dict, state: dict) -> None:
    """Mine redo since the last checkpoint."""
    schema_name = settings["schema"]
    tables_filter = settings["tables_filter"]

    lm = LogMinerManager(
        conn, ordered_query=settings["ordered_query"], kill_orphans=settings["kill_orphans"]
    )
    try:
        if lm.is_pdb():
            lm.check_logmining_privilege()
        else:
            try:
                lm.check_logmining_privilege()
            except RuntimeError as err:
                log.warning(str(err))

        last_scn = int(state["last_scn"])
        mine_from = min(int(state.get("mine_from_scn", last_scn + 1)), last_scn + 1)
        saved_xids = state.get("open_xids")
        open_xids = {str(x).upper() for x in saved_xids} if isinstance(saved_xids, list) else None
        filter_below = state.get("xid_filter_below")
        filter_below = int(filter_below) if filter_below is not None else None
        if open_xids is None:
            filter_below = None

        table_names = _list_tables(conn, schema_name, tables_filter)
        pk_map = _get_primary_keys(conn=conn, schema=schema_name, table_names=table_names)
        meta = _load_column_meta(conn, schema_name, table_names)
        _check_supplemental_logging(conn, schema_name, table_names)

        # Tables that appeared since the snapshot (new table, or added to the `tables` filter)
        # never had a snapshot, so their existing rows would never arrive. Copy them first;
        # mining from last_scn then replays any changes made meanwhile (idempotent).
        state_extra: dict[str, Any] = {}
        known = state.get("snapshot_tables")
        if isinstance(known, list):
            new_tables = [t for t in table_names if t not in set(known)]
            if new_tables:
                log.info(f"Snapshotting {len(new_tables)} new table(s): {new_tables[:10]}")
                _run_initial_sync(
                    conn=conn,
                    schema=schema_name,
                    tables_filter=new_tables,
                    batch_size=settings["batch_size"],
                    column_meta=meta,
                    checkpoint_rows=settings["checkpoint_rows"],
                )
                known = sorted(set(known) | set(new_tables))
                _checkpoint({**state, "snapshot_tables": known})
            state_extra["snapshot_tables"] = known

        # Sample open transactions BEFORE reading the target SCN (see _capture_floor).
        boundary_scn, floor_scn, sample_xids = _capture_floor(conn, settings["open_txn_check"])
        target = lm.get_current_scn()
        log.info(
            f"Incremental sync: last_scn={last_scn} mine_from={mine_from} "
            f"target={target} open_txns_sampled={len(sample_xids)}"
        )
        if target <= last_scn:
            log.info("No new SCNs to mine; destination is up to date.")
            return

        lm.check_scn_within_archive_window(mine_from)

        def apply_row(row: dict) -> bool:
            return _apply_record(conn, schema_name, pk_map, row, settings["on_parse_error"], meta)

        total = _mine_incremental(
            lm=lm,
            schema=schema_name,
            tables_filter=tables_filter,
            apply_row=apply_row,
            batch_size=settings["batch_size"],
            max_archived_logs=settings["max_archived_logs"],
            last_scn=last_scn,
            mine_from=mine_from,
            open_xids=open_xids,
            filter_below=filter_below,
            target=target,
            boundary_scn=boundary_scn,
            floor_scn=floor_scn,
            sample_xids=sample_xids,
            window_size=settings["scn_window"],
            buffer_mem_rows=settings["buffer_mem_rows"],
            state_extra=state_extra,
        )
        log.info(f"Incremental sync complete – {total} changes applied; last_scn={target}")
    finally:
        lm.stop_quietly()


def _sync_once(configuration: dict, settings: dict, state: dict) -> None:
    conn = get_connection(configuration)
    try:
        if _need_initial_sync(state):
            _snapshot(conn, settings, state)
        else:
            _incremental(conn, settings, state)
    finally:
        try:
            conn.close()
        except Exception as err:  # a dead connection must not mask the real error
            log.fine(f"Ignoring error while closing connection: {err}")


def update(configuration: dict, state: dict):
    validate_configuration(configuration)
    settings = _read_settings(configuration)

    _WARNED.clear()
    _TRACK["state"] = None
    current = dict(state)

    # Every position is checkpointed and every write is idempotent (upsert / delete by key), so
    # after a dropped connection it is safe to reconnect and continue from the last checkpoint.
    for attempt in range(1, _MAX_SYNC_ATTEMPTS + 1):
        try:
            _sync_once(configuration, settings, current)
            return
        except (oracledb.Error, OSError) as err:
            if attempt == _MAX_SYNC_ATTEMPTS or not _is_transient(err):
                raise
            delay = _backoff_delay(attempt)
            log.warning(
                f"Sync attempt {attempt}/{_MAX_SYNC_ATTEMPTS} hit a transient error: {err}; "
                f"reconnecting in {delay:.0f}s and resuming from the last checkpoint"
            )
            if _TRACK["state"] is not None:
                current = dict(_TRACK["state"])
            time.sleep(delay)


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    connector.debug(configuration=configuration)
