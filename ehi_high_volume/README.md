# EHI High Volume Connector Example

## Connector overview

This connector syncs high-volume tables from Microsoft SQL Server using the Fivetran Connector SDK. It is designed to handle very large tables (100M+ rows) reliably and efficiently. The connector uses [pyodbc](https://pypi.org/project/pyodbc/) with Microsoft's ODBC Driver 18 for SQL Server.


## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_csdk_connectors/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- Microsoft ODBC Driver 18 for SQL Server installed on the host — see the [Microsoft installation guide](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
- A SQL Server login with `SELECT` permission on the target schema


## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```bash
fivetran init --template ehi_high_volume
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

> Note: Before running locally, install the Microsoft ODBC Driver 18 for SQL Server on your machine. On macOS, run `brew install msodbcsql18`. On Linux (Debian/Ubuntu), follow the [Microsoft apt installation guide](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server). Verify the installation with `odbcinst -q -d -n "ODBC Driver 18 for SQL Server"`.


## Features

- Automatic schema discovery — all tables in the target schema are discovered and synced without manual configuration
- Automatic replication key detection using known column name patterns (e.g. `UpdatedAt`, `_LastUpdatedInstant`, `ModifiedDate`), or an explicit `incremental_column` override in `configuration.json`
- Full load with keyset pagination when a replication key is available, primary keyset pagination for tables with a single primary key but no replication key, or offset pagination as a last resort
- Incremental sync after a completed full load — only rows where `repl_key > last_synced_value` are fetched
- Parallel table syncs via a configurable number of worker threads
- Configurable table include and exclude lists via `table_list` and `table_exclusion_list` in `configuration.json`
- Binary and spatial columns (`varbinary`, `geography`, `geometry`, etc.) included as base64-encoded strings
- Transient error retry with SQLSTATE-based detection and exponential backoff


## Configuration file

```json
{
    "mssql_server": "<YOUR_SQL_SERVER_HOST>",
    "mssql_cert_server": "<YOUR_CERT_HOSTNAME_OR_EMPTY>",
    "mssql_port": "<YOUR_SQL_SERVER_PORT>",
    "mssql_database": "<YOUR_SQL_SERVER_DATABASE>",
    "mssql_user": "<YOUR_SQL_SERVER_USERNAME>",
    "mssql_password": "<YOUR_SQL_SERVER_PASSWORD>",
    "mssql_schema": "<YOUR_SQL_SERVER_SCHEMA>",
    "incremental_column": "<OPTIONAL_REPLICATION_KEY_COLUMN_NAME>",
    "table_list": "<OPTIONAL_COMMA_SEPARATED_TABLE_NAMES>",
    "table_exclusion_list": "<OPTIONAL_COMMA_SEPARATED_TABLE_NAMES>"
}
```

The configuration keys are:

- `mssql_server` (required): Hostname or IP address of the SQL Server instance
- `mssql_cert_server` (optional): Hostname to validate in the server's TLS certificate; leave empty to trust the server certificate without hostname verification, which is suitable for AWS RDS and other cloud-hosted SQL Servers with self-signed certificates
- `mssql_port` (required): TCP port for the SQL Server instance; defaults to `1433`
- `mssql_database` (required): Name of the database to connect to
- `mssql_user` (required): SQL Server login username
- `mssql_password` (required): SQL Server login password
- `mssql_schema` (required): Schema to discover and sync tables from; defaults to `dbo`
- `incremental_column` (optional): Column name to use as the replication key for all tables
- `table_list` (optional): Comma-separated list of table names to sync; if omitted, all tables in the schema are synced
- `table_exclusion_list` (optional): Comma-separated list of table names to exclude from the sync

> Note: When submitting connector code as a [Community Connector](https://github.com/fivetran/fivetran_csdk_connectors/tree/main) in the open-source [Connector SDK repository](https://github.com/fivetran/fivetran_csdk_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## Requirements file

The `requirements.txt` file specifies the Python library required by the connector beyond those pre-installed in the Fivetran environment.

```
pyodbc==5.3.0
```

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

> Note: `pyodbc` requires Microsoft ODBC Driver 18 for SQL Server to be installed on the host. In the Fivetran connector runtime environment this driver is pre-installed.


## Authentication

This connector uses SQL Server username and password authentication with credentials supplied in `configuration.json`. The connection always uses TLS encryption (`Encrypt=yes`).

To set up authentication:

1. Create or identify a SQL Server login that the connector will use to connect to the database.
2. Grant that login `SELECT` permission on the target schema and all tables that need to be synced.
3. Collect the SQL Server hostname, port, database name, username, and password for that login.
4. If your SQL Server uses a TLS certificate issued to a specific hostname, determine that certificate hostname and set `mssql_cert_server` in `configuration.json` to that value.
5. If you are connecting to a cloud-hosted SQL Server instance (AWS RDS, Azure SQL) that uses a self-signed certificate, leave `mssql_cert_server` empty in `configuration.json`.
6. Allowlist [Fivetran's egress IP addresses](https://fivetran.com/docs/using-fivetran/fivetran-ip-addresses) for your account region in your SQL Server's firewall so the Fivetran cloud platform can reach it. If your SQL Server is on a private network, use [Fivetran Hybrid Deployment](https://fivetran.com/docs/using-fivetran/hybrid-deployment) instead.
7. Add all connection details and credentials to `configuration.json` before running `fivetran debug` or deploying the connector.

When `mssql_cert_server` is set, the connector validates the server's TLS certificate against that hostname (`TrustServerCertificate=no`, `HostNameInCertificate=<value>`). This is the recommended setting for production environments where the SQL Server has a valid certificate issued to a known hostname.

When `mssql_cert_server` is empty, the connector sets `TrustServerCertificate=yes`, which bypasses hostname verification. This is suitable for cloud-hosted SQL Server instances (AWS RDS, Azure SQL) that use self-signed certificates.


## Pagination

The connector uses three pagination strategies depending on the available key columns for each table. Each page executes a fresh bounded query that seeks directly to `WHERE repl_col > last_value` using the table index. This is O(log n) per page and O(n) total, with no server-side cursor state. The last seen replication key value is stored in state after every checkpoint interval and used as the resume point if the sync is interrupted.

When many rows share the same replication key value (for example, batch-inserted rows with the same timestamp), the connector switches to a composite keyset using the replication key and primary key together as tiebreakers. Offset pagination is O(n²) in database cost and is used only as a fallback for tables where no replication key column or single-column primary key can be detected. A warning is logged for each such table.


## Data handling

Schema detection queries `INFORMATION_SCHEMA.COLUMNS` and `COLUMNPROPERTY` for each table to determine column names, SQL Server data types, primary key membership, identity columns, and computed columns. Computed columns are excluded from `SELECT` lists because SQL Server rejects them in explicit column lists under certain schema configurations. Schema discovery runs in parallel using a thread pool.

The connector select the replication key for each table using the following priority order:

1. `incremental_column` key in `configuration.json` — Applies the specified column name to every table, overriding auto-detection.
2. Column name matches a known pattern (e.g. `_LastUpdatedInstant`, `UpdatedAt`, `ModifiedDate`) — Checked case-insensitively against `KNOWN_REPLICATION_KEY_PATTERNS` in `constants.py`.
3. None — No replication key detected; the table uses PK-keyset or offset pagination and has no incremental mode.

Refer to `class SchemaDetector` in `models.py` and `def convert_value` in `readers.py`.


## Error handling

Transient SQL Server errors are detected using SQLSTATE codes rather than substring-matching error messages. SQLSTATE codes are standardised and locale-independent. SQL Server native error 1222 (lock request time out period exceeded) arrives with SQLSTATE `HY000` and is handled by checking the native error number embedded in the message string.

On a retryable error the connection is closed, the thread sleeps with exponential backoff and jitter (starting at 5 seconds, capped at 300 seconds), the connection is reopened, and the query is retried. Keyset queries are idempotent — re-executing with the same `last_seen_value` parameter returns the same page. After `MAX_RETRIES` exhausted attempts the exception is re-raised.

Non-retryable errors are logged at `SEVERE` level and re-raised immediately, causing that table's thread to exit. Other tables continue syncing. Failed table names are collected and logged as a warning at the end of the sync run.

Refer to `def _is_retryable_error` and `def execute_with_retry` in `client.py`.


## Tables created

The connector discovers tables dynamically from the SQL Server schema specified in `mssql_schema`. No tables are hardcoded in the connector. The set of tables synced is the full contents of the schema, subject to the `table_list` and `table_exclusion_list` configuration keys.

The connector creates each table in the destination with:

- Column names and Fivetran-inferred types based on the mapped Python types
- Primary keys as detected from `INFORMATION_SCHEMA.KEY_COLUMN_USAGE`
- Binary and spatial columns stored as base64-encoded strings


## Additional files

- `client.py` – Defines `MSSQLConnection` (single pyodbc connection with retry logic and `READ UNCOMMITTED` isolation) and `ConnectionPool` (fixed-size queue-based pool for multi-threaded access)
- `models.py` – Defines `ColumnInfo` and `TableSchema` dataclasses and `SchemaDetector` (queries `INFORMATION_SCHEMA` to build per-table schemas and detect replication keys)
- `readers.py` – Defines `ReplicationKeysetReader` (keyset pagination ordered by replication key, with optional PK tiebreak), `PrimaryKeyOnlyKeysetReader` (keyset pagination ordered by primary key for tables without a replication key), and `OffsetReader` (offset pagination fallback) generators that stream table data in bounded batches, and `convert_value()` for type-safe row serialisation
- `constants.py` – Has tunable parameters, such as batch size, checkpoint interval, worker thread count, retry settings, and replication key detection patterns


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
