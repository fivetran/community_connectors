# BAI2 SFTP Connector Example

Fivetran custom connector that fetches BAI2-format cash management files from an SFTP server and loads all transactions into a single `bai2_transactions` table in your destination.

---

## Connector overview

BAI2 (Bank Administration Institute version 2) is the de facto US standard for bank-to-corporate cash management reporting. Banks deliver one file per day containing all account activity for the prior 24 hours.

This connector:
- Connects to an SFTP server and lists files in a configured directory
- Picks up any files not yet processed (incremental — no reprocessing)
- Parses the BAI2 hierarchical record structure into flat transaction rows
- Denormalizes account balance summaries (opening balance, closing balance, etc.) onto every transaction row
- Parses structured ACH fields from narrative continuation records inline
- Checkpoints after each file so a mid-run failure does not reprocess completed files
- Tracks any file-level failures in state for operational visibility

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

To initialize a new Connector SDK project using this connector as a starting point, run:

```
fivetran init --template sftp_connector/bai2_sftp_connector
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

---

## Features

- Incremental sync using processed filenames as cursors — files are never reprocessed
- BAI2 hierarchical record structure (01/02/03/16/88) parsed into flat transaction rows
- Account balance summaries (Record 03) denormalized onto every transaction row as `balance_{type_code}` columns — new type codes appear automatically on first sync
- Structured ACH fields parsed from 88-record narrative text into dedicated `ach_*` columns
- Checkpoint per file — a mid-run failure does not reprocess already-completed files
- File-level failure tracking in state for operational visibility
- Exponential backoff with jitter on SFTP connection retries (up to 5 attempts, delays 5 s → ~10 s → ~20 s → ~40 s)
- Optional test mode to limit each sync to 3 files for initial validation

---

## BAI2 file structure

A BAI2 file is comma-delimited with a hierarchical record structure. Each record is identified by a numeric code as its first field:

```
01 - File Header        (1 per file)
  02 - Group Header     (1 per file, represents the originating bank)
    03 - Account Summary  (1 per account — contains balance type codes)
    88 - Continuation     (extends the preceding record's field list)
      16 - Transaction    (1 per debit/credit)
      88 - Continuation   (extends the transaction narrative text)
    49 - Account Trailer
  98 - Group Trailer
99 - File Trailer
```

Records 49, 98, and 99 (trailers) contain control totals only and are not loaded into the destination.

---

## Configuration file

| Key | Required | Description |
|---|---|---|
| `sftp_host` | Yes | SFTP server hostname or IP address |
| `sftp_port` | No | SFTP port (default: `22`). Must be a valid integer between 1 and 65535. |
| `sftp_username` | Yes | SFTP username |
| `sftp_password` | Yes | SFTP password for authentication. Must not be empty. |
| `sftp_remote_path` | Yes | Directory path on the SFTP server where BAI2 files are deposited. Must not be empty. |
| `sftp_file_pattern` | No | Regex pattern to filter filenames (e.g. `web\..*\.prev\.out\..*`). When omitted, all files in the directory are processed. Validated at startup — an invalid regex raises an error before any connection is attempted. |
| `test_mode` | No | Set to `"true"` to limit each sync to 3 files. SFTP connection is made normally — all credentials still required. See [Test mode](#test-mode) below. |

Example `configuration.json`:

```json
{
    "sftp_host": "sftp.yourbank.com",
    "sftp_port": "22",
    "sftp_username": "your_sftp_username",
    "sftp_password": "your_sftp_password",
    "sftp_remote_path": "/outgoing/bai2/",
    "sftp_file_pattern": "web\\..*\\.prev\\.out\\..*"
}
```

> Note: When submitting connector code as a [Community Connector](https://github.com/fivetran/community_connectors/tree/main) in the open-source [Connector SDK repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

---

## Requirements file

The `requirements.txt` file specifies the Python libraries required by this connector:

```
paramiko==3.5.1
```

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

---

## Authentication

This connector uses password authentication over SFTP. Provide `sftp_host`, `sftp_username`, and `sftp_password` in `configuration.json`. For production deployments, store credentials in Fivetran's encrypted secrets store rather than in `configuration.json`.

---

## Connection reliability

The connector is built for production SFTP environments with several reliability features:

- TCP/banner/auth timeouts – the SSH connection uses `timeout=30s`, `banner_timeout=30s`, and `auth_timeout=30s` to prevent hanging at any phase of the SSH handshake.
- SFTP operation timeout – after the SFTP session is opened, all file operations (directory listing, file reads) are subject to a 60-second channel timeout.
- Exponential backoff with jitter – connection failures are retried up to 5 times. Delays increase exponentially (5 s → ~10 s → ~20 s → ~40 s) with proportional jitter (±30%) capped at 60 seconds, preventing thundering-herd behaviour when multiple instances retry simultaneously.

---

## Data handling

- Incremental – files are processed once and never reprocessed. The filename is used as the cursor.
- One file per day – the bank delivers one BAI2 file per day. If no new file is present, the sync completes with no rows written.
- Chronological order – new files are sorted alphabetically before processing. Because BAI2 filenames embed a timestamp (e.g. `web.galls.prev.out.20260102.144521`), alphabetical order is equivalent to chronological order.
- Checkpoint per file – state is saved after each file is fully processed. If a sync is interrupted, already-completed files are not reprocessed on the next run.
- Row-level error handling – if an individual record fails to parse, a warning is logged and that row is skipped. The rest of the file continues processing. A summary log line at the end of each file reports how many transactions were parsed and how many records were skipped.

---

## Error handling

- File-level errors – if an entire file fails (SFTP read error, unrecoverable parse failure), the failure is recorded in `state["failed_files"]` with the error type and message, a checkpoint is saved, and the connector moves on to the next file. The failed file is not added to `processed_files` and will be retried on the next sync.
- Connection errors – SFTP connection failures are retried up to 5 times with exponential backoff before raising a `RuntimeError`. See [Connection reliability](#connection-reliability) for details.

---

## Tables created

### `bai2_transactions`

One row per transaction (Record 16). Balance summary data from Record 03 is denormalized onto every row for that account.

#### Core fields

| Column | Type | Source | Description |
|---|---|---|---|
| `btid` | INT | Synthetic | Auto-incrementing primary key, persisted across syncs |
| `file_reference` | STRING | Filename | Source filename (e.g. `web.galls.prev.out.20260102.144521`) |
| `file_date` | DATE | Record 01 | Date the file was created (YYMMDD → ISO) |
| `file_creation_time` | STRING | Record 01 | Time the file was created (HHMM) |
| `sender_id` | STRING | Record 01 | Bank routing number or sender identifier |
| `receiver_id` | STRING | Record 01 | Receiving system identifier |
| `as_of_date` | DATE | Record 02 | Date the balances and transactions apply to |
| `as_of_time` | STRING | Record 02 | Time of reporting (HHMM) |
| `originator_id` | STRING | Record 02 | Bank SWIFT/routing number |
| `bank_account_number` | STRING | Record 03 | Bank account number |
| `currency` | STRING | Record 02/03 | ISO 4217 currency code (e.g. `USD`) |

#### Transaction fields (Record 16)

| Column | Type | Description |
|---|---|---|
| `type_code` | STRING | BAI2 transaction type code (e.g. `145` = ACH Credit) |
| `amount` | FLOAT | Transaction amount in currency units (e.g. `29.43`) |
| `debit_credit_mark` | STRING | `C` (credit) or `D` (debit), derived from type code range |
| `funds_type` | STRING | Funds availability indicator (`S`, `0`, `D`, `V`, `Z`) |
| `same_day_amount` | FLOAT | Same-day available portion (when `funds_type = S`) |
| `one_day_amount` | FLOAT | One-day available portion (when `funds_type = S`) |
| `two_plus_day_amount` | FLOAT | Two-or-more-day portion (when `funds_type = S`) |
| `bank_reference` | STRING | Unique transaction ID assigned by the bank |
| `customer_reference` | STRING | Corporate reconciliation reference |
| `description` | STRING | Full narrative text (inline 88 records merged) |

#### ACH structured fields (parsed from 88 narrative)

These columns are populated for ACH transactions where the bank embeds structured data in the narrative continuation records.

| Column | Description |
|---|---|
| `ach_cust_id` | ACH customer ID |
| `ach_description` | ACH description / payment type |
| `ach_comp_name` | Originating company name |
| `ach_comp_id` | Originating company ID |
| `ach_batch_discr` | Batch discriminator |
| `ach_sec_code` | Standard Entry Class code (e.g. `CCD`, `CTX`, `PPD`) |
| `ach_cust_name` | Customer name from ACH record |
| `ach_tran_date` | Transaction date from ACH record |
| `ach_tran_time` | Transaction time from ACH record |
| `ach_addenda` | Addenda indicator (`No Addenda` or `See EDI Report`) |

#### Balance summary columns (Record 03, denormalized)

Balance columns are named `balance_{type_code}` and appear dynamically based on what the bank reports. Common codes:

| Column | BAI2 code | Description |
|---|---|---|
| `balance_010` | 010 | Opening Ledger Balance |
| `balance_015` | 015 | Closing Ledger Balance |
| `balance_040` | 040 | Opening Available Balance |
| `balance_045` | 045 | Opening Available + Same-Day ACH |
| `balance_072` | 072 | Adjusted Closing Available Balance |
| `balance_074` | 074 | Adjusted Opening Available Balance |
| `balance_100` | 100 | Total Credits |
| `balance_110` | 110 | Total Lockbox Deposits |
| `balance_140` | 140 | Total ACH Credits Received |
| `balance_400` | 400 | Total Debits |
| `balance_550` | 550 | ZBA / Deposit Interest |
| `balance_570` | 570 | Available Balance |

Any additional type codes reported by the bank appear as new columns automatically on first sync. Amounts are in currency units (e.g. `209212.42`). A value of `0.0` means the bank reported a zero balance; `NULL` means the type code was not reported for that account.

---

## State management

The connector persists the following values in Fivetran state between syncs:

| Key | Description |
|---|---|
| `processed_files` | JSON array of filenames already processed — prevents reprocessing |
| `next_btid` | Next available `btid` value — ensures the primary key increments globally across all files |
| `failed_files` | JSON array of files that failed to process (capped at 100 entries). Each entry contains `filename`, `error_type`, `error` (truncated to 200 chars), and `timestamp`. Used for operational visibility — failed files require manual investigation. |

To reprocess a failed file after resolving the underlying issue, navigate to your connector in the Fivetran dashboard, then go to **Settings** → **Reset State**. Alternatively, remove the filename from `failed_files` in state — it was never added to `processed_files`, so it will be picked up automatically on the next sync.

---

## Test mode

Test mode connects to the real SFTP server but limits each sync to 3 files. Use it during initial setup to validate connectivity and parsing against live data without processing the full backlog.

When `test_mode` is `"true"`:
- SFTP connection is made normally — all credentials are still required.
- Only the first 3 unprocessed files (alphabetical order) are processed per sync.
- State and `processed_files` are updated normally after each file, so subsequent syncs continue where test mode left off.
- Disable `test_mode` (set to `"false"` or remove the key) once the integration is validated.

Example `configuration.json` for test mode:

```json
{
    "sftp_host": "sftp.yourbank.com",
    "sftp_port": "22",
    "sftp_username": "your_sftp_username",
    "sftp_password": "your_sftp_password",
    "sftp_remote_path": "/outgoing/bai2/",
    "sftp_file_pattern": "web\\..*\\.prev\\.out\\..*",
    "test_mode": "true"
}
```

---

## Common BAI2 type codes

| Code | Description | D/C |
|---|---|---|
| 115 | Lockbox Deposit | C |
| 145 | ACH Concentration Credit | C |
| 175 | Other Deposit | C |
| 191 | Miscellaneous Credit | C |
| 195 | Incoming Money Transfer | C |
| 275 | ZBA Credit | C |
| 399 | Miscellaneous Credit | C |
| 455 | ACH Debit | D |
| 475 | Other Debit | D |
| 491 | Miscellaneous Debit | D |
| 495 | Outgoing Money Transfer | D |
| 555 | ZBA Debit | D |
| 575 | ZBA Debit Transfer | D |
| 698 | Account Analysis / Service Charge | D |

---

## Troubleshooting

| Symptom | Likely cause | Resolution |
|---|---|---|
| `Missing required configuration key(s)` | One of the required keys is absent from `configuration.json` | Verify all four required keys (`sftp_host`, `sftp_username`, `sftp_password`, `sftp_remote_path`) are present and non-empty |
| `sftp_password cannot be empty` | Password value is an empty string | Provide the SFTP password in `configuration.json` |
| `sftp_port must be between 1 and 65535` | Port value is out of range or not a valid integer | Correct the `sftp_port` value in `configuration.json` |
| `sftp_remote_path cannot be empty` | Remote path is an empty string | Provide the directory path on the SFTP server |
| `Failed to connect to SFTP after 5 attempts` | Wrong host/port, firewall blocking the port, or incorrect password | Confirm SFTP credentials; test with an SFTP client |
| `paramiko.ssh_exception.AuthenticationException` | Password is incorrect or the SFTP user account is locked | Verify the password with the bank's IT team |
| `[Errno 2] No such file` on `sftp.listdir` | `sftp_remote_path` does not exist on the server | Confirm the directory path with the bank's IT team |
| `Invalid sftp_file_pattern regex` | `sftp_file_pattern` contains an invalid regex (e.g. unclosed bracket) | Fix the pattern syntax; test with `python -c "import re; re.compile(r'your_pattern')"` |
| 0 rows processed but files exist | `sftp_file_pattern` regex does not match the actual filenames | Test the pattern: `python -c "import re; print(re.search(r'your_pattern', 'your_filename'))"` |
| Balance columns are all `NULL` | BAI2 file has no Record 03 before the Record 16s | Contact the bank — a valid BAI2 file must include account summary records |
| `btid` gaps between syncs | Expected behaviour — btid increments globally and gaps occur when rows are skipped | Not an error; check logs for `Skipping record` warnings on the relevant sync |
| File appears in `failed_files` state | File-level failure (SFTP error, unrecoverable parse error) | Check `error_type` and `error` fields in state for the cause; resolve the underlying issue; the file will be retried automatically on the next sync |
| `test_mode must be 'true' or 'false'` | `test_mode` value is not a recognised string | Set `test_mode` to `"true"` or `"false"` |
| Only 3 files processed despite more being available | `test_mode` is `"true"` | Set `test_mode` to `"false"` or remove the key once validation is complete |

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

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
