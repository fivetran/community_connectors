---
name: Oracle Autonomous Database (LogMiner)
description: Connect and sync data from Oracle Autonomous Database on OCI using LogMiner change capture and the python-oracledb library
category: Databases
---

# Oracle LogMiner connector for Oracle Autonomous Database (OCI)

## Connector overview

This Fivetran Connector SDK connector replicates tables from an Oracle database into your Fivetran destination. It is tuned for **Oracle Autonomous Database on Oracle Cloud Infrastructure (OCI)**, which always runs as a pluggable database (PDB), and it also works against non-PDB Oracle databases.

It uses a two-phase sync model:

- **Initial sync:** runs a full `SELECT *` on every configured table and emits an upsert per row. Progress is checkpointed after every table (and every `checkpoint_rows` rows), so an interrupted snapshot resumes at the first unfinished table.
- **Incremental syncs:** opens an Oracle LogMiner session (`DBMS_LOGMNR`), reads committed `INSERT`, `UPDATE` and `DELETE` events from `V$LOGMNR_CONTENTS` for an SCN window, parses `SQL_REDO`, and emits upserts and deletes.

State is kept between syncs as a `last_scn` (System Change Number) watermark and an `initial_sync_done` flag. The tables to replicate and their primary keys are listed in `schema.py`. Column types are read from the data dictionary and declared in `schema()`.

### How this differs from the generic `oracle_logminer` connector

| Area | Generic connector | This connector |
|---|---|---|
| Transport | `TCP`, `LOAD_BALANCE`/`FAILOVER` (RAC, RDS) | `TCPS` (TLS) with `SSL_SERVER_DN_MATCH=YES`, connect timeouts, keepalive and classified retries |
| Redo selection on a PDB | `ADD_LOGFILE` (fails with ORA-65040 on Autonomous) | No `ADD_LOGFILE`. `START_LOGMNR` is called with `STARTSCN`/`ENDSCN` and Oracle locates the redo itself |
| Container detection | None | `SYS_CONTEXT('USERENV','CON_ID')`; a value above 1 selects the PDB path automatically |
| Privilege check | None | `LOGMINING` check before mining |
| Stale `last_scn` | Fails later with a LogMiner error | Fails fast with a clear message if older than the 7-day archive window |
| Session cleanup | Context manager only | `finally` block always runs `END_LOGMNR`, including after a failed start |
| Checkpointing | Every `batch_size` rows at the highest `COMMIT_SCN` seen | After every SCN window (`logminer_scn_window`); state also carries `mine_from_scn` and open transaction IDs so transactions spanning windows or syncs are never lost |
| Transaction handling | `COMMITTED_DATA_ONLY` (Oracle buffers whole transactions in PGA; ORA-04036 on large transactions) | Connector buffers per-transaction changes (spilling to local disk) and applies them in commit order; rolled-back transactions are dropped |


## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- Oracle Autonomous Database (19c or later), or another Oracle 12c+ database with archivelog mode enabled
- Network access from the machine running the connector to the database listener (Autonomous uses port `1522` with TLS)
- A database user with LogMiner privileges (see below)

### Autonomous Database limits that shape this connector

- `DBMS_LOGMNR.ADD_LOGFILE` and `REMOVE_LOGFILE` are not allowed. `START_LOGMNR` may only be called with an SCN range (or time range), and Oracle finds the redo itself.
- Only **one LogMiner session per database** is allowed. Do not run two syncs, or an interactive LogMiner session, at the same time against the same database.
- Archived redo is kept for **7 days**. If a connector is paused longer than that, it cannot resume incrementally and needs a fresh initial sync.
- The `LOGMINING` privilege is required.

### Database privileges

Run as `ADMIN` (or another privileged user). On Autonomous, archivelog mode is managed by Oracle. Supplemental logging needs to be checked: see [Supplemental logging](#supplemental-logging).

```sql
GRANT CREATE SESSION                TO logminer_user;
GRANT LOGMINING                     TO logminer_user;
GRANT EXECUTE ON DBMS_LOGMNR        TO logminer_user;
GRANT SELECT  ON V_$LOGMNR_CONTENTS TO logminer_user;
GRANT SELECT  ON V_$DATABASE        TO logminer_user;
GRANT SELECT  ON V_$TRANSACTION     TO logminer_user;
GRANT SELECT  ON GV_$TRANSACTION    TO logminer_user;
GRANT SELECT  ON V_$SESSION         TO logminer_user;  -- orphaned-session cleanup (optional)
GRANT ALTER SYSTEM                  TO logminer_user;  -- orphaned-session cleanup (optional)
GRANT SELECT_CATALOG_ROLE           TO logminer_user;  -- for dictionary views
-- Plus SELECT on each table you replicate (or SELECT ANY TABLE).
```

`SELECT` on `ALL_LOG_GROUPS` (visible to all users) and `V$DATABASE` is used for the supplemental logging check, which only warns.

Note: `V_$ARCHIVED_LOG`, `V_$LOG` and `V_$LOGFILE` are only used by the non-PDB path, which still calls `ADD_LOGFILE`. Grant them as well if you point this connector at a self-managed, non-PDB database. For non-PDB databases, also run `ALTER DATABASE ARCHIVELOG;` and `ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;` as a DBA.

### Supplemental logging

Two layers are involved. You need the first, plus at least one option from the second.

**1. Minimal supplemental logging (database level, required).** LogMiner needs this baseline. Check it as `ADMIN`:

```sql
SELECT SUPPLEMENTAL_LOG_DATA_MIN, SUPPLEMENTAL_LOG_DATA_PK, SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE;
```

If `SUPPLEMENTAL_LOG_DATA_MIN` is `NO`, run `ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;`. On Autonomous the PDB view of `V$DATABASE` can read `NO` here even when mining works, in which case the startup info message about minimal logging can be ignored. If an update fails with "Primary key … is missing from the redo", the logging is not taking effect.

**2. Key or full-row logging (table or database level, one of these).** This decides what an `UPDATE` records. Table level is preferred, because it limits extra redo to the tables you replicate:

```sql
-- Best: the full row is in every update
ALTER TABLE <schema>.<table> ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- Minimum: the primary key is in every update, but unchanged columns are not
ALTER TABLE <schema>.<table> ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;
```

Database-level equivalents (`ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;` or `(PRIMARY KEY) COLUMNS`) also satisfy the check but log every table in the database.

| Setup | What an `UPDATE` carries | Connector behavior |
|---|---|---|
| `(ALL) COLUMNS` | The whole row | Upserts straight from redo |
| `(PRIMARY KEY) COLUMNS` | Key and changed columns | Upserts the key and changed columns only |
| Neither | Changed columns and `ROWID` only | The update has no primary key, so the sync fails (or skips it, with `logminer_on_parse_error=skip`) |

Use `(ALL) COLUMNS` for complete rows. With `(PRIMARY KEY) COLUMNS` the connector never queries the database to fill in the rest, so an update upserts only the key and the changed columns. `(ALL) COLUMNS` increases redo volume on busy tables.

A warning at the start of each sync lists tables that have neither option.

The connector needs `SCN_TO_TIMESTAMP` (available to all users) and `SESSION_PRIVS` (visible to all users) for its startup checks. No further grants are needed.


## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/setup-guide) to get started.

1. Copy `connector.py`, `schema.py`, `requirements.txt` and `configuration.json` into a local project directory.

2. List your tables and primary keys in `schema.py` (see [Defining tables](#defining-tables)) and fill in `configuration.json`. See [Configuration file](#configuration-file).

3. Run the connector locally:

```bash
fivetran debug --configuration configuration.json
```

4. Deploy:

```bash
fivetran deploy --api-key <API_KEY> --destination <DESTINATION> --connection <CONNECTION_NAME> --configuration configuration.json
```

Note: `fivetran debug` writes `files/state.json` and `files/warehouse.db` in the project directory. A successful run advances `last_scn` in `files/state.json`. Back the file up first if you need to preserve the current position.


## Features

- Full initial snapshot that resumes at the first unfinished table after an interruption
- Change data capture through LogMiner for `INSERT`, `UPDATE` and `DELETE`
- Automatic PDB detection and an Autonomous-compatible LogMiner path (`STARTSCN`/`ENDSCN`, no `ADD_LOGFILE`)
- Startup checks for the `LOGMINING` privilege and the 7-day archive window, each with an actionable error message
- `END_LOGMNR` always runs, so a failed sync does not leave a LogMiner session open (important with the one-session limit)
- Checkpoints that never advance `last_scn` past data that has not been processed
- Tables and primary keys defined in `schema.py`, with an optional `tables` setting to narrow the list
- Tables without `SELECT` privilege and rows with null primary keys are skipped with warnings instead of failing the sync
- Oracle thin mode (no client library) and thick mode (Oracle Instant Client)
- TLS connections with server DN matching, keepalive, and connect timeouts
- Transient failures (dropped connection, listener blip) reconnect with backoff and resume from the last checkpoint; permanent ones (bad password, untrusted certificate) fail at once so an account is never locked by retries
- Column types are read from `ALL_TAB_COLS` and declared in `schema()`, and snapshot and LogMiner rows are coerced to the same types
- Long rows (`SQL_REDO` split across several `V$LOGMNR_CONTENTS` rows) are reassembled before parsing
- Deletes use `op.delete` keyed on the primary key only, recovered from `SQL_UNDO` when the redo omits it
- LOB placeholders are left out of upserts instead of being sent as `NULL`
- Tables added after the first sync are snapshotted automatically
- A cancelled or crashed sync no longer blocks the next one: its leftover LogMiner session is ended automatically (ORA-44611)
- Progress is logged every 30 seconds inside long windows (rows read, applied, rows/s)
- Warnings for unreplicable changes (DDL, `TRUNCATE`, LOB-only writes, unsupported column types) and for missing supplemental logging


## Configuration file

```json
{
  "host": "adb.<region>.oraclecloud.com",
  "port": "1522",
  "service_name": "<prefix>_<dbname>_high.adb.oraclecloud.com",
  "username": "LOGMINER_USER",
  "password": "<PASSWORD>",
  "schema": "MY_SCHEMA",
  "tables": "*",
  "batch_size": "1000",
  "oracle_thick_mode": "false",
  "oracle_client_lib_dir": ""
}
```

| Parameter | Required | Default | Description |
|---|---|---|---|
| `host` | Yes | | Autonomous Database hostname, from the connection string in the OCI console |
| `port` | No | `1521` | Listener port. Autonomous TLS uses `1522` |
| `service_name` | Yes | | Service name from the connection string (for example the `_high`, `_medium` or `_low` service) |
| `username` | Yes | | Database user with the privileges above |
| `password` | Yes | | Database password |
| `schema` | Yes | | Schema (owner) to replicate |
| `tables` | No | `*` | Comma-separated table names to replicate from those listed in `schema.py`, or `*` for all of them |
| `batch_size` | No | `1000` | Rows per fetch for snapshot cursors and `V$LOGMNR_CONTENTS` |
| `checkpoint_rows` | No | `100000` | Rows between checkpoints inside one snapshot table. A checkpoint is also written after every table |
| `declare_column_types` | No | `true` | Declare Oracle-derived column types in `schema()`. Set `false` to let Fivetran infer every type from the data |
| `logminer_scn_window` | No | `500000` | Maximum SCNs mined per LogMiner session. Each window is checkpointed, and halved automatically on ORA-04036 |
| `logminer_buffer_mem_rows` | No | `50000` | Uncommitted rows held in memory before spilling to local disk |
| `logminer_open_txn_check` | No | `true` | Sample `GV$TRANSACTION` so no in-flight transaction is missed. Needs `SELECT` on `V_$TRANSACTION` and `GV_$TRANSACTION` |
| `logminer_on_parse_error` | No | `fail` | `fail` stops the sync on an unparseable committed change. `skip` logs a warning and drops it |
| `logminer_kill_orphan_session` | No | `true` | On ORA-44611, end earlier sessions of this connector (same user, module `fivetran_oracle_logminer`) and retry. Needs the two optional grants above. Without them the connector waits and retries instead |
| `logminer_ordered_query` | No | `false` | Add `ORDER BY SCN` to the LogMiner query (uses PGA). Set only if the sync reports out-of-order rows |
| `logminer_max_archived_logs` | No | `10` | Non-PDB path only. Caps archived logs added per sync. Ignored on a PDB such as Autonomous |
| `oracle_thick_mode` | No | `false` | `true` to use Oracle Instant Client |
| `oracle_client_lib_dir` | No | | Path to Instant Client libraries (thick mode only) |

Note: Keep `configuration.json` out of version control. It contains credentials.

### Defining tables

`schema.py` holds the tables to replicate and their primary keys as a list of dicts. Names are Oracle names, case-insensitive here and upper-cased by the connector:

```python
TABLES = [
    {"table": "CUSTOMERS", "primary_key": ["ID"]},
]
```

The connector does not query `ALL_TABLES` or `ALL_CONSTRAINTS`. A table in the list that the user cannot read is skipped during the snapshot with a warning. Add a table by adding a dict. It is snapshotted at the start of the next sync. A wrong primary key breaks upserts and deletes.


## Requirements file

```
oracledb==3.4.2
```

`oracledb` is the Oracle thin/thick Python driver. `fivetran_connector_sdk` and `requests` are pre-installed in the Fivetran environment, so do not declare them in `requirements.txt`.


## Authentication

The connector signs in with a database username and password from the configuration. No OAuth or API keys are used.

The DSN (`def build_dsn`) uses `TCPS` with `SSL_SERVER_DN_MATCH=YES`, 30-second connect timeouts and a TCP keepalive probe every 2 minutes.

Connection failures are classified. Transient ones (`DPY-4011`, `ORA-03113`, timeouts, listener unavailable) are retried up to 5 times with exponential backoff and jitter. Permanent ones (`ORA-01017` wrong password, `ORA-28000` locked account, certificate verification failures, unknown service) fail immediately, because repeated bad logins can lock an Autonomous account.

If the connection drops mid-sync, `update()` reconnects and continues from the last checkpoint (up to 5 attempts). This is safe because every write is an idempotent upsert or delete by key.

### TLS certificate errors

`DPY-6005 ... CERTIFICATE_VERIFY_FAILED: self-signed certificate in certificate chain` means the client does not trust the certificate chain presented on the connection. Common causes:

- A VPN or corporate proxy that inspects TLS. Try from a network without inspection, or add the proxy's CA to your trust store.
- A database configured for **mutual TLS (mTLS)**, which needs the downloaded wallet. Either use the wallet (set `oracle_thick_mode`/wallet options for your setup) or switch the database to allow TLS-only connections in the OCI console.

Do not disable certificate verification to work around this.


## Sync behavior

### Initial sync

1. Open transactions are sampled and the current SCN is read as the baseline, before any rows are read.
2. Each configured table is read with `SELECT *`. Values are converted to the column's type (`NUMBER(p,0)` to integer, scaled `NUMBER` to a decimal string, `DATE`/`TIMESTAMP` to ISO-8601, `RAW`/`BLOB` to bytes, LOBs to text) and upserted.
3. State is checkpointed after every table, and every `checkpoint_rows` rows within one. It records the baseline SCN and the tables already copied, but not `last_scn`, so an interrupted snapshot can never look finished.
4. If the sync is interrupted, the next one resumes at the first unfinished table using the saved baseline. If that baseline is older than the 7-day redo window, the snapshot restarts from scratch.
5. When the snapshot finishes, state becomes `{"last_scn": <baseline>, "mine_from_scn": <floor>, "initial_sync_done": true, "snapshot_tables": [...]}`.

### Incremental sync

1. **Startup checks:** detect PDB vs non-PDB, verify `LOGMINING` (hard error on a PDB, warning otherwise), and read the current SCN.
2. If `last_scn + 1` is not below the current SCN, there is nothing to mine and the sync returns.
3. **Archive window check:** `last_scn` is converted to a timestamp. If it is older than 7 days, or Oracle cannot map it (ORA-08181), the sync stops with an error asking you to reset state and run a fresh initial sync.
4. **Start LogMiner:**
   - PDB: `START_LOGMNR(STARTSCN, ENDSCN, OPTIONS => DICT_FROM_ONLINE_CATALOG)`
   - Non-PDB: `ADD_LOGFILE` for the overlapping archived and online redo (a gap in log sequence numbers is a hard error), then `START_LOGMNR` with the same option.
   - `COMMITTED_DATA_ONLY` is not used: it makes Oracle hold whole in-flight transactions in PGA (ORA-04036). The connector buffers each transaction itself and applies it in commit order.
5. **Read** `V$LOGMNR_CONTENTS` for `INSERT`/`UPDATE`/`DELETE` and `COMMIT`/`ROLLBACK` in the configured schema (and tables). Statements split across rows (`CSF = 1`) are reassembled.
6. **Parse** `SQL_REDO`, convert values to the column types, then emit `op.upsert` (insert, update) or `op.delete` when the transaction commits. Rolled-back transactions are dropped.
7. **Checkpoint** after each SCN window: `last_scn`, `mine_from_scn` and the open transaction IDs, so transactions that span windows or syncs are never lost.
8. `END_LOGMNR` runs in a `finally` block.

### Why `last_scn` only advances at the end

Rows come back ordered by `SCN`, not `COMMIT_SCN`. Saving the highest `COMMIT_SCN` seen part-way through a window could move the watermark past rows that have not been read yet, and a crash would then skip them. Moving the watermark only after the full window avoids that. If a sync fails, the next run re-reads the same window, which is safe because upserts and deletes are idempotent. The trade-off is that a very large backlog is re-processed after a failure.

### State

```json
{"last_scn": 50000928632165, "mine_from_scn": 50000928632165, "initial_sync_done": true, "snapshot_tables": ["CUSTOMERS"]}
```

- `last_scn`: the upper SCN of the last completed window. The next incremental sync mines from `last_scn + 1`.
- `mine_from_scn`: where the next sync starts reading redo, so a transaction still open at `last_scn` is read from its first change. It can be lower than `last_scn + 1`. `open_xids` and `xid_filter_below` may also appear.
- `initial_sync_done`: `false` while a snapshot is in progress (with a `snapshot` entry holding its baseline SCN and finished tables), `true` after it completes. Legacy state with only `last_scn` is treated as already snapshotted.
- `snapshot_tables`: tables already snapshotted. A table in `schema.py` that is missing here is snapshotted at the start of the next sync.

### Resetting state

If the connector reports that `last_scn` is outside the 7-day window, clear its state and run a fresh initial sync.

- Local: delete `files/state.json` (and `files/warehouse.db` if you want a clean local warehouse).
- Fivetran dashboard: trigger a historical resync for the connection.


## Data handling

- **Schema:** tables and primary keys come from `schema.py` and are declared in `schema()`. Column types come from `ALL_TAB_COLS` (hidden columns excluded) and are declared too, unless `declare_column_types` is `false`.
- **Parsing:** `parse_insert`, `parse_update` and `parse_delete` handle `SQL_REDO`. UPDATE statements are split on the real top-level `WHERE` (not one inside a string value). Recognised value forms: `TO_DATE`, `TO_TIMESTAMP`, `TO_TIMESTAMP_TZ`, `TIMESTAMP '…'`, `DATE '…'`, `TO_YMINTERVAL`, `TO_DSINTERVAL`, `HEXTORAW`, `UNISTR`, `EMPTY_CLOB`, `EMPTY_BLOB`, quoted strings and numbers. An unrecognised form is an error (see `logminer_on_parse_error`), never stored as garbage.
- **Types:** `NUMBER(p,0)` with `p` up to 18 is `LONG`. Other `NUMBER(p,s)` is `DECIMAL(p,s)`. A `NUMBER` with no precision is `DECIMAL(38,10)`. `FLOAT`/`BINARY_*` is `DOUBLE`. `DATE` and `TIMESTAMP` are `NAIVE_DATETIME`. Character types and CLOBs are `STRING`. `RAW` and `BLOB` are `BINARY`. Time-zone timestamps and other types are left to inference.
- **Updates and inserts:** always `op.upsert` of exactly what the redo carries. Nothing is looked up in the database. If the redo lacks the primary key the change fails (see `logminer_on_parse_error`). If it lacks other columns or carries `EMPTY_CLOB()`/`EMPTY_BLOB()`, those columns are left out of the upsert (never sent as `NULL`) and a warning, once per table, shows a `SQL_REDO` sample.
- **Deletes:** emitted with `op.delete`, keyed on the primary key columns only. If the redo's `WHERE` clause lacks the key, it is recovered from `SQL_UNDO`.
- **Primary key changes:** upserted under the new key. The old row is not deleted.
- **Tables without a primary key:** give an empty `primary_key` in `schema.py` only if the table has none. Updates and deletes then cannot be matched to the original row, and a warning names the table.
- **Table names** match the source exactly (uppercase).

### Tables created

One destination table per source table. Tables updated through incremental sync also receive these metadata columns on upserted rows:

- `_logminer_scn`: SCN of the change in the redo log
- `_logminer_timestamp`: Oracle timestamp of the change
- `_logminer_operation`: `INSERT` or `UPDATE`


## Error handling

| Situation | Behavior |
|---|---|
| Cannot connect | Transient errors retry up to 5 times with backoff, then fail with the underlying Oracle error. Wrong password, locked account and untrusted certificate fail at once |
| Missing `LOGMINING` (PDB) | Fails with a message telling a DBA to `GRANT LOGMINING` |
| Missing `LOGMINING` (non-PDB) | Logs a warning and continues |
| `last_scn` older than 7 days | Fails and recommends a reset and fresh initial sync |
| `START_LOGMNR` fails | Session is ended, then the error is raised |
| Any failure during mining | `END_LOGMNR` still runs in `finally` |
| No `SELECT` privilege on a table (`ORA-00942`) | Table skipped during the snapshot, with a warning |
| Null primary key in the snapshot | Row skipped, with a summary count in the log |
| UPDATE redo missing primary key | Sync fails with a pointer to the supplemental logging setup. With `skip`, the update is dropped with a warning |
| Unparseable `SQL_REDO` | Sync fails (`logminer_on_parse_error=fail`, the default) so a committed change is never silently lost. With `skip` the change is dropped with a warning |
| Dropped connection mid-sync | Reconnects with backoff and resumes from the last checkpoint (up to 5 attempts) |
| Wrong password, locked account, untrusted certificate | Fails at once, with no retry |
| DDL, `TRUNCATE`, LOB-only write or unsupported column type in redo | Warning once per table and operation. Re-sync the table if the destination diverges |
| Gap in archived redo sequence (non-PDB) | Fails with a message to restore the logs or run a fresh initial sync |
| Empty `SQL_REDO` | Row skipped (fine-level log) |


## Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| `ORA-65040` from `DBMS_LOGMNR` | You are running a version that calls `ADD_LOGFILE` on a PDB. This connector avoids it by detecting the PDB. Confirm `CON_ID` is above 1. |
| `ORA-44611` | Autonomous allows one ad hoc LogMiner session, and a cancelled sync can leave its session behind. The connector ends its own orphaned session and retries. If it logs that it cannot, grant `SELECT ON V_$SESSION` and `ALTER SYSTEM`, or end the session by hand. Do not run two connections against one database |
| `Primary key [...] is missing from the redo` | The table's supplemental logging is not producing full rows. See [Supplemental logging](#supplemental-logging). The error includes a `SQL_REDO` sample. Redo written before logging was enabled cannot be repaired: resync the table |
| `LOGMINING` error | Ask a DBA to run `GRANT LOGMINING TO <user>;` |
| "older than the 7-day archived redo retention window" | The connector was idle too long. Reset state and run a fresh initial sync. |
| `DPY-6005` / `CERTIFICATE_VERIFY_FAILED` | See [TLS certificate errors](#tls-certificate-errors) |
| No incremental rows but data changed | Check `schema` and `tables`, confirm the user can read the tables, and confirm changes were committed (only committed data is returned). |

## Known limitations

- The one-LogMiner-session limit means only one connection to a given database can mine at a time.
- A transaction that is still open when a window ends is buffered and re-read from its first change by the next sync (`mine_from_scn`), so it is not lost. Redo that Oracle has not yet made readable is covered by sampling `GV$TRANSACTION` (`logminer_open_txn_check`).
- An update that changes a primary key is upserted under the new key. The old row stays in the destination.
- DDL is not replicated. New columns appear once rows containing them are replicated. `TRUNCATE` and dropped or renamed columns need a table re-sync. New tables are snapshotted automatically at the start of the next sync.
- Changes made only to a LOB (for example `DBMS_LOB.WRITE`) without touching another column do not appear in `SQL_REDO` and are not replicated until the row is next updated.
- Virtual columns are not in redo. The snapshot loads them, but incremental upserts do not carry them.
- Time-zone timestamps and other types without a lossless mapping are left to Fivetran's type inference.
- Existing connections that ran an earlier version have inferred column types. Declared types take effect for new tables; for existing ones, run a historical resync, or set `declare_column_types` to `false` to keep inference.


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
