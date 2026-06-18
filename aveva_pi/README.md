# AVEVA PI Connector

## Connector overview

This connector syncs data from AVEVA PI (formerly OSIsoft PI) to your Fivetran destination. It connects to the PI SQL Data Access Server (PI SQL DAS) via ODBC and discovers all table-type objects in the configured PI AF database at sync time.

Key capabilities:
- Dynamic schema discovery: tables and columns are discovered from PI SQL DAS ODBC metadata at each sync
- Incremental sync for tables that expose a `Modified` column, using time-windowed queries that automatically halve the window on query failures
- Special handling for the `Archive` table: epoch-seeded first sync, 2-hour late-arrival rollback on subsequent syncs, and hash-based row identification using `AttributeID` + `TimeStamp`
- Full reimport for all other tables (no `Modified` column)
- Synthetic `_fivetran_id` primary key for tables without a natural primary key, generated as an MD5 hash of the relevant column values
- Authentication-aware retry: transient connection failures are retried up to 3 times; auth failures surface immediately

The destination table name format is `<source_schema>_<table_name>` to preserve the PI schema hierarchy within a single Fivetran destination schema.


## Requirements

- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- AVEVA PI SQL Client (ODBC driver) installed on the host machine. Download from the [AVEVA Customer Portal](https://customers.osisoft.com/s/downloads). See the [PI SQL Client documentation](https://docs.aveva.com/bundle/pi-sql-client) for installation instructions.
- PI SQL Data Access Server (PI SQL DAS) running and reachable from the connector host on the configured port (default: 5461)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64) — requires the PI ODBC driver for Linux (available from AVEVA Support)


## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```
fivetran init --template aveva_pi
```

> Note: Ensure you have updated `configuration.json` with your PI SQL DAS connection details before running `fivetran debug`. See the [Configuration file](#configuration-file) section below.

1. Install the AVEVA PI SQL Client (ODBC driver) from the AVEVA Customer Portal.
2. Verify the PI SQL DAS service is running: `ping <PI_SQL_DAS_HOST>` and confirm port 5461 is open.
3. Update `configuration.json` with your connection details.
4. Test the connector locally:
   ```
   fivetran debug
   ```


## Features

- Dynamic table and column discovery via ODBC catalog metadata
- Cursor-based incremental sync using the `Modified` timestamp column
- Adaptive time-window backoff: starts at 365-day windows, halves automatically on query errors, minimum 1-hour window
- Archive table support: epoch-seeded initial sync + 2-hour late-arrival rollback
- MD5 hash-based synthetic primary key for tables without natural PKs
- Periodic checkpointing every 10,000 rows during full reimports
- Authentication-aware error handling: auth failures are surfaced immediately without unnecessary retries


## Configuration file

```json
{
  "host": "<PI_SQL_DAS_HOSTNAME_OR_IP>",
  "port": "5461",
  "database": "<PI_AF_DATABASE_NAME>",
  "username": "<PI_USERNAME>",
  "password": "<PI_PASSWORD>",
  "odbc_driver": "PI ODBC"
}
```

| Key | Required | Description |
|---|---|---|
| `host` | Yes | Hostname or IP address of the PI SQL DAS server |
| `port` | No | PI SQL DAS port (default: `5461`) |
| `database` | No | PI AF database name (defaults to the server default database if omitted) |
| `username` | Yes | PI user account with read access to the target database |
| `password` | Yes | Password for the PI user account |
| `odbc_driver` | No | ODBC driver name as registered on the host (default: `PI ODBC`). Common alternatives: `PI SQL Client` |

> Note: When submitting connector code as a Community Connector, ensure `configuration.json` has placeholder values. When deploying, do not check this file into version control to protect credentials.


## Requirements file

`requirements.txt` declares `pyodbc`, the Python ODBC bridge library. The Fivetran Connector SDK runtime installs this at deployment time.

> Note: [Some packages](https://fivetran.com/docs/connectors/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK environment. Do not re-declare them in `requirements.txt` to avoid dependency conflicts.


## Authentication

The connector uses PI username/password authentication passed directly in the ODBC connection string. The PI SQL DAS server validates credentials against the configured PI identity provider (PI Mapping or Windows authentication, depending on your PI server configuration).

If the connection attempt returns a 401, 403, or known PI DAS authentication error, the connector raises a `ValueError` immediately without retrying, so the sync fails fast and Fivetran prompts for updated credentials.


## Pagination

This connector does not use page-based pagination. Instead, it uses time-window-based streaming:

- For incremental tables: data is fetched in windows of up to 365 days. If a query fails with a transient error, the window is halved and the same slice is retried. This continues until either the query succeeds or the window drops below 1 hour (at which point the error is surfaced).
- For full reimport tables: all rows are fetched in a single query and streamed via the pyodbc cursor iterator, avoiding loading the entire result set into memory.

Checkpointing occurs after each successful time window (incremental) or every 10,000 rows (reimport), allowing the connector to resume from the last safe point after an interruption.


## Data handling

- **Type mapping**: AVEVA PI SQL types (AnsiString, Guid, Int8, DateTime, etc.) are mapped to Fivetran SDK types (STRING, SHORT, UTC_DATETIME, etc.). Columns with unrecognised types are skipped with a warning.
- **Timestamps**: pyodbc returns naive datetime objects. The connector attaches UTC timezone info before yielding rows to Fivetran, ensuring correct UTC_DATETIME handling.
- **Hash IDs**: Tables without a natural primary key receive a `_fivetran_id` column (MD5 hex string). For most tables this is computed from all column values; for the Archive table it uses only `AttributeID` and `TimeStamp` to match the original connector's identity semantics.
- **Destination table naming**: `<source_schema>_<table_name>` (e.g., the `Archive` table in the `PISystem` schema lands as `PISystem_Archive` in the destination).


## Error handling

- Connection failures are retried up to 3 times with a warning logged per attempt. Refer to `get_connection()`.
- Authentication errors (401/403, unknown DAS, bad port) are detected by matching the error message against known patterns and raise immediately without retrying. Refer to `_AUTH_ERROR_PATTERNS`.
- Incremental query failures trigger window halving rather than hard failure. If the window cannot be halved further (below 1 hour), a `RuntimeError` is raised. Refer to `sync_incremental()`.
- Tables with no supported columns are skipped with a warning rather than causing the entire sync to fail.


## Tables created

The connector creates one destination table per PI SQL DAS table visible in the configured AF database. The exact tables depend on the PI AF schema, but a typical PI deployment exposes:

| Source table | Incremental column | Notes |
|---|---|---|
| `Archive` | `TimeStamp` | Special epoch-seed + late-arrival rollback |
| `Element` | `Modified` | Incremental |
| `Event_Frame` | `Modified` | Incremental |
| `Attribute` | `Modified` | Incremental |
| `Category` | `Modified` | Incremental |
| `Class` | `Modified` | Incremental |
| `Element_Hierarchy` | — | Full reimport |
| `Unit_of_Measure` | — | Full reimport |
| Other tables | varies | Detected at sync time |

All destination tables follow the naming convention `<source_schema>_<table_name>`.


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
