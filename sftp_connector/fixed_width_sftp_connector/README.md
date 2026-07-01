# Fixed-Width SFTP Connector Example

Fivetran Connector SDK connector that reads 12 fixed-width files delivered daily to an SFTP server by three financial-data providers (ELAN, CUP, LPL/DFM) and syncs them to 12 destination tables.

---

## Connector overview

This connector handles fixed-width file formats from three providers:

- ELAN (Fiserv) – credit-card account data, 1 file per day, table: `fiserv_elan_cpf1582`
- CUP – mortgage account data, 1 file per day, table: `cup9078501`
- LPL/DFM – brokerage and investment data, 10 files per day, tables: `lpl_*`

Key behaviors:
- Missing file – abort entire sync with `RuntimeError`
- Malformed line – log error and skip that file, continue remaining files
- SFTP disconnect – retry up to 5 times with exponential backoff (5 s → 10 s → 20 s → 40 s)
- Full-refresh tables (10 of 12) include a `purge_indicator` boolean column for soft-delete
- Incremental tables (`lpl_commission_transaction`, `lpl_transaction_activity`) append only

---

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

---

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

To run the connector locally:

1. Install dependencies:

   ```bash
   pip install fivetran-connector-sdk paramiko==3.5.1
   ```

2. Fill in your credentials in `configuration.json`.

3. Run the connector locally using the Fivetran debug command:

   ```bash
   fivetran debug
   ```

   This executes the sync locally, prints all log output, and writes results to a local `warehouse.db` SQLite file for inspection.

---

## Features

- Reads 12 fixed-width files across 3 SFTP subdirectories (ELAN, CUP, DFM) in a single sync
- Two-pass streaming file reader — validates header/trailer counts without buffering the entire file in memory
- Soft-delete (purge) logic for 10 full-refresh tables: rows absent from the current file are emitted with `purge_indicator=True`
- Implied-decimal parsing for ELAN and CUP (COBOL `9(n)Vnn` format): integer value divided by `10^decimal_places`
- `MMDDYYYY` → ISO `YYYY-MM-DD` date conversion for CUP date fields
- Strict host-key verification via optional `sftp_host_key` configuration key
- Optional test mode: validates all 12 files end-to-end but upserts only the first 100 records per file; state is not modified

---

## Files synced

| # | Provider | Subdirectory | File pattern | Destination table | Type | Purge |
|---|----------|-------------|-------------|-------------------|------|-------|
| 1 | ELAN | `ELAN` | `ELAN` | `fiserv_elan_cpf1582` | Full | Yes |
| 2 | CUP | `CUP` | `CUP9078501` | `cup9078501` | Full | Yes |
| 3 | LPL | `DFM` | `DFM-7PBB-AccountEXT` | `lpl_account_ext` | Full | Yes |
| 4 | LPL | `DFM` | `DFM-7PBB-AccountParticipant` | `lpl_account_participant` | Full | Yes |
| 5 | LPL | `DFM` | `DFM-7PBB-Client` | `lpl_client` | Full | Yes |
| 6 | LPL | `DFM` | `DFM-7PBB-CommissionSecurity` | `lpl_commission_security` | Full | Yes |
| 7 | LPL | `DFM` | `DFM-7PBB-CommissionTransaction` | `lpl_commission_transaction` | Incremental | No |
| 8 | LPL | `DFM` | `DFM-7PBB-Position` | `lpl_position` | Full | Yes |
| 9 | LPL | `DFM` | `DFM-7PBB-PositionSecurity` | `lpl_position_security` | Full | Yes |
| 10 | LPL | `DFM` | `DFM-7PBB-Reference` | `lpl_reference` | Full | Yes |
| 11 | LPL | `DFM` | `DFM-7PBB-Rep` | `lpl_rep` | Full | Yes |
| 12 | LPL | `DFM` | `DFM-7PBB-TransactionActivity` | `lpl_transaction_activity` | Incremental | No |

---

## SFTP directory structure

The connector expects the remote server to be organised into subdirectories beneath `sftp_remote_path`:

```
sftp_remote_path/
├── ELAN/
│   └── ELAN.TXT
├── CUP/
│   └── CUP9078501_20260325.TXT
└── DFM/
    ├── DFM-7PBB-AccountEXT-20260325-v2.0.txt
    ├── DFM-7PBB-AccountParticipant-20260325-v2.0.txt
    ├── DFM-7PBB-Client-20260325-v2.0.txt
    ├── DFM-7PBB-CommissionSecurity-20260325-v2.0.txt
    ├── DFM-7PBB-CommissionTransaction-20260325-v2.0.txt
    ├── DFM-7PBB-Position-20260325-v2.0.txt
    ├── DFM-7PBB-PositionSecurity-20260325-v2.0.txt
    ├── DFM-7PBB-Reference-20260325-v2.0.txt
    ├── DFM-7PBB-Rep-20260325-v2.0.txt
    └── DFM-7PBB-TransactionActivity-20260325-v2.0.txt
```

Each spec in `table_specs.py` has a `subdirectory` key (`"ELAN"`, `"CUP"`, or `"DFM"`). The connector calls `sftp.listdir_attr()` once per unique subdirectory and caches the result — all 10 LPL/DFM files share a single directory listing call. The full remote path for each file is `sftp_remote_path/subdirectory/filename`.

---

## File pattern matching

File patterns are substrings matched against filenames in each subdirectory. The connector selects the most recently modified file whose name contains the pattern.

| Pattern | Example filename matched |
|---------|------------------------|
| `ELAN` | `ELAN.TXT` |
| `CUP9078501` | `CUP9078501_20260325.TXT` |
| `DFM-7PBB-AccountEXT` | `DFM-7PBB-AccountEXT-20260325-v2.0.txt` |
| `DFM-7PBB-AccountParticipant` | `DFM-7PBB-AccountParticipant-20260325-v2.0.txt` |
| *(and so on for all 10 DFM files)* | |

Patterns were derived from the provider file naming conventions. Confirm against real files on first connection — if the provider changes their naming convention the pattern in `table_specs.py` must be updated.

---

## Configuration file

| Key | Required | Description |
|-----|----------|-------------|
| `sftp_host` | Yes | SFTP server hostname or IP address |
| `sftp_port` | No | SFTP port (default `22`; must be 1–65535) |
| `sftp_username` | Yes | SFTP login username |
| `sftp_password` | Yes | SFTP login password |
| `sftp_remote_path` | Yes | Base remote directory. The connector appends `/ELAN`, `/CUP`, `/DFM` automatically. Example: `/incoming` |
| `sftp_host_key` | No | Base64-encoded RSA server host key for strict host verification. Strongly recommended for production. When omitted, host key verification is disabled and a warning is logged. |
| `test_mode` | No | `"true"` to run a non-destructive end-to-end validation sync. All 12 files are downloaded and validated; only the first 100 records per file are upserted; purge logic and checkpointing are skipped so state is never modified. Set back to `"false"` (or remove) for the first real production sync. |

All keys are validated at startup. An invalid `sftp_host_key` raises `ValueError` immediately with a clear message before any SFTP connection is attempted.

### How to populate each key with real credentials

- `sftp_host` — provided by the SFTP provider (e.g. `sftp.provider.com`)
- `sftp_port` — usually `22`; confirm with provider
- `sftp_username` — provided by the SFTP provider
- `sftp_password` — provided by the SFTP provider
- `sftp_remote_path` — the base directory on the server that contains the `ELAN`, `CUP`, and `DFM` subdirectories (e.g. `/data/feeds` or `/incoming`). Do not include a trailing slash — the connector strips it automatically.
- `sftp_host_key` — run the following on any machine that can reach the server, then paste the third column (the base64 key blob):

  ```bash
  ssh-keyscan -t rsa <sftp_host>
  # example output line:
  # sftp.provider.com ssh-rsa AAAAB3NzaC1yc2EAAAA...
  #                            ^^^^^^^^^^^^^^^^^^^ paste this part
  ```

Note: In production, store credentials in Fivetran's encrypted secrets store rather than in `configuration.json`.

---

## Requirements file

The `requirements.txt` file specifies the Python libraries required by this connector:

```
paramiko==3.5.1
```

Note: The `fivetran_connector_sdk` and `requests` packages are pre-installed in the Fivetran environment and must not be declared in `requirements.txt`.

---

## Authentication

This connector uses password authentication over SFTP. Provide `sftp_host`, `sftp_username`, and `sftp_password` in `configuration.json`. Optionally set `sftp_host_key` for strict RSA host-key verification, which is recommended for production environments.

---

## `table_specs.py` field guide

Each field definition in `table_specs.py` has three parsing-related keys that are internal parsing instructions, not Fivetran schema declarations:

| Key | Purpose |
|-----|---------|
| `parse_as` | Tells `convert_value()` in `connector.py` which Python conversion to apply to the raw text slice. The Fivetran SDK infers the destination column type automatically from the Python value passed to `op.upsert()` — this key is not passed to the SDK. |
| `decimal_places` | Present only on implied-decimal FLOAT fields (ELAN and CUP). The raw bytes contain no decimal point (COBOL `9(n)Vnn` format); the connector divides the integer value by `10^decimal_places`. Example: raw `"001234"` with `decimal_places=2` → `12.34`. Omit this key for all other fields. |
| `date_format` | Present only on CUP date fields where the file stores dates as `MMDDYYYY`. The connector reformats to ISO `YYYY-MM-DD` before upserting. Omit this key for all other fields — the default is `YYYYMMDD`. |

`parse_as` values used and what Python type they produce:

| `parse_as` | Python value produced | Notes |
|----------------|----------------------|-------|
| `STRING` | `str` or `None` | Whitespace-stripped; empty → `None` |
| `INT` | `int` or `None` | All-zeros → `None` |
| `LONG` | `int` or `None` | All-zeros → `None`; same as INT at runtime |
| `FLOAT` | `float` or `None` | Explicit: `float(raw)`; implied: `int(raw) / 10^n` |
| `NAIVE_DATE` | ISO date string `"YYYY-MM-DD"` or `None` | All-zeros or blank → `None` |

---

## Behavior

### Missing file

If any of the 12 expected file patterns is not found in its SFTP subdirectory, the entire sync is aborted with a `RuntimeError`. Fivetran will retry on the next scheduled run.

### No new file

If the most-recently modified file for a pattern is identical to the file processed in the previous sync, the sync is aborted. This prevents re-processing stale files.

### Malformed line

If any line in a file cannot be parsed, the connector logs an error message and skips that file entirely. The remaining files are still processed and checkpointed.

### SFTP retries

Connection is attempted up to 5 times with exponential backoff between attempts:

| Retry | Delay before this attempt |
|-------|--------------------------|
| 1→2 | 5 s |
| 2→3 | 10 s |
| 3→4 | 20 s |
| 4→5 | 40 s |
| Total max wait | 75 s |

Each retry logs a warning with the delay. A 300-second channel-level timeout is applied to all SFTP operations (file downloads, directory listings) to prevent indefinite hangs.

### Purge / soft-delete

Full-refresh tables include a `purge_indicator` boolean column.
- `False` — row is present in the current file (active).
- `True` — row was present in the previous sync but is absent today (soft-deleted).

The connector maintains a `pks` dictionary in Fivetran state to track the PK set across syncs. Incremental tables (`lpl_commission_transaction`, `lpl_transaction_activity`) never receive a `purge_indicator` column.

### Decimal handling

- ELAN – implied decimal (COBOL `9(n)Vnn`). Integer value divided by `10^decimal_places` at parse time. `BALANCE` and `RWD_POINTS_AVAILABLE` are negated when their companion sign field contains `"-"`.
- CUP / LPL – explicit decimal point present in the file; parsed directly with `float()`.

### Date handling

- ELAN – most date fields stored as raw `LONG` (e.g. `DATE_OPEN`, `STMT_CLOSING_DATE`) because some use non-standard formats (`0MMDDYY`). `BIRTH_DATE1` / `BIRTH_DATE_2` stored as `STRING` because spaces = NULL.
- CUP – date fields are `MMDDYYYY` — converted to `YYYY-MM-DD` ISO format.
- LPL – date fields are `YYYYMMDD` — converted to `YYYY-MM-DD` ISO format.

### PII masking

No PII masking is applied in the connector. Snowflake Dynamic Data Masking is recommended for sensitive columns (SSN, date of birth, phone numbers, etc.).

---

## Before going live

| Item | Impact |
|------|--------|
| CUP trailer record-count field position (`trailer_count_start`, `trailer_count_length`) | Required for trailer validation on `cup9078501`. Currently `None` — trailer count validation is skipped until confirmed from a sample file. |
| File patterns confirmed against real files | Field positions and file patterns were derived from spec documents. A `connector.debug()` run against real sample files is required to confirm before go-live. |

---

## Deployment

To deploy the connector to Fivetran after local testing:

```bash
fivetran connector deploy \
  --connector-id <your-connector-id> \
  --destination <your-destination-group-name>
```

Note: The API key must be base64-encoded as `key:secret`. Use the Fivetran group name (not the group ID) for `--destination`.

---

## Testing checklist

1. Local parse test — run `connector.debug()` with all 12 customer sample files; verify field counts, types, and row totals.
2. Trailer validation — manually corrupt a trailer count → confirm `RuntimeError`.
3. Missing file — remove one file pattern from its SFTP subdirectory → confirm sync aborts with a clear message.
4. Malformed line — corrupt one line → confirm error logged, file skipped, other 11 files still processed.
5. Purge logic — sync 10 records, then 8 (remove 2 PKs) → confirm 2 rows have `purge_indicator=True`.
6. SFTP retry — simulate disconnect → confirm 5 retries with delays 5 s, 10 s, 20 s, 40 s logged before failure.
7. Incremental tables — sync `lpl_commission_transaction` twice → confirm records accumulate and no `purge_indicator` column exists.
8. Invalid host key — set a garbage value in `sftp_host_key` → confirm `ValueError` at startup with a clear message.
9. Wildcard in remote path — set `sftp_remote_path` to `/incoming/*` → confirm `ValueError` at startup.
10. Deploy — deploy to destination and verify schema and data.

---

## Additional files

- `table_specs.py` – defines the 12 file specifications (field positions, data types, primary keys, purge flags, decimal and date format settings) consumed by `connector.py`. Edit this file to adjust field positions, add new fields, or register new file patterns.

---

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
