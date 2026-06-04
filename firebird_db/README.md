# Firebird Database Connector

## Connector overview

This connector fetches data from Firebird database tables using the firebirdsql library. The connector retrieves data from multiple tables with multi-threaded processing, pagination support, and incremental sync capabilities.

The connector maintains tables as defined in the `schema.py` file:
- Each table is configured with its own incremental column and primary key
- Tables are processed concurrently using thread pool execution

## Requirements

- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```
fivetran init --template firebird_db
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- Fetches data from multiple Firebird database tables
- Multi-threaded processing with configurable worker count
- Incremental sync based on configurable incremental columns
- Batch processing with configurable batch sizes
- Automatic datetime conversion to string format
- Thread-safe checkpointing for reliable data synchronization
- Concurrent table processing for improved performance

## Configuration file

The connector requires configuration with your Firebird database credentials:

```json
{
    "host": "<database_host_ip>",
    "database": "<path_to_.fdb_file>",
    "user": "<db_user>",
    "password": "<db_password>"
}
```

> Note: When submitting connector code as a [Community Connector](https://github.com/fivetran/community_connectors/tree/main) in the open-source [Connector SDK repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The connector requires the `firebirdsql` package for database connectivity:

```
firebirdsql==1.3.4
```

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

This connector uses standard Firebird database authentication:
- Host IP address or hostname
- Database file path (.fdb file)
- Username and password credentials
- Connection established on port 3050 (default Firebird port)

## Data handling

The connector processes data in the following way:
1. Connects to Firebird database using provided credentials
2. Reads table configuration from `schema.py` file
3. Processes multiple tables concurrently using thread pool
4. Implements incremental sync based on configured incremental columns
5. Converts datetime objects to string format for compatibility
6. Uses batch processing for efficient data retrieval and processing

The connector uses incremental sync based on table-specific incremental columns to avoid duplicate data and ensure efficient synchronization.

The connector includes several configurable performance parameters:
- `batch_process_size` – Number of records processed per upsert (default: `1000`)
- `batch_query_size` – Number of records retrieved per query (default: `10000`)
- `MAX_WORKERS` – Maximum number of concurrent threads (default: `10`)

## Error handling

The connector implements error handling for:
- Database connection failures
- Thread execution errors with proper logging
- Empty result set handling
- Batch processing boundary conditions
- Thread-safe state management

All errors are logged using the Fivetran logging system for debugging and monitoring.

## Tables created

The connector creates tables as defined in the `schema.py` file. Each table includes:
- Configurable table name
- Configurable primary key columns
- Configurable incremental sync column
- All columns from the source Firebird table

Example tables:
### table_1
Primary key: `ID`
Incremental column: `ID`

### table_2
Primary key: `ID`
Incremental column: `DATE_MODIFIED`

### table_3
Primary key: `ID`
Incremental column: `DATE_MODIFIED`

## Additional files

- [`schema.py`](schema.py) – Defines the list of Firebird tables to sync, with each entry specifying the table name, primary key columns, and incremental sync column.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
