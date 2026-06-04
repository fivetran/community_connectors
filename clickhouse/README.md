# ClickHouse Connector Example

## Connector overview

This connector shows how to pull data from ClickHouse databases. It connects to a specified ClickHouse instance, extracts data from tables, and syncs it to your destination.

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
fivetran init --template clickhouse
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- Connects to ClickHouse cloud or on-premise instances
- Streams query results to handle large datasets efficiently
- Tracks sync progress using state and checkpointing
- Supports incremental syncs

## Configuration file

The connector requires the following configuration parameters:

```json
{
  "hostname": "<YOUR_CLICKHOUSE_HOSTNAME>",
  "username": "<YOUR_CLICKHOUSE_USERNAME>",
  "password": "<YOUR_CLICKHOUSE_PASSWORD>",
  "database": "<YOUR_CLICKHOUSE_DATABASE>"
}
```

- hostname: Your ClickHouse server hostname
- username: Username for authentication
- password: Password for authentication
- database: The ClickHouse database to connect to

> Note: When submitting connector code as a [Community Connector](https://github.com/fivetran/community_connectors/tree/main) in the open-source [Connector SDK repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The connector requires the clickhouse_connect Python library for connecting to ClickHouse:

```
clickhouse_connect==0.8.17
```

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector uses basic username and password authentication to connect to ClickHouse. These credentials are specified in the configuration file. The connector uses TLS/SSL for secure connections (indicated by the secure=True parameter in the client configuration). To obtain credentials:

1. Create an account in the ClickHouse Cloud console.
2. Click on the "Connect" button for your cluster.
3. Choose the python Language client to get the connection details.
4. Use the connection details to fill in the configuration file.

## Pagination

The connector uses ClickHouse's streaming query capabilities to efficiently process large datasets without loading all data into memory. This is implemented using the `client.query_rows_stream()` method which upserts rows as they are retrieved from the database.

## Data handling

The connector:
- Establishes a connection to the ClickHouse database.
- Inserts data into dummy table for testing purposes.
- Retrieves column metadata for the specified table.
- Executes queries with incremental filtering based on a timestamp column (`created_at`).
- Upserts data into destination table.

## Error handling

The connector implements error handling for:
- Connection failures: Raises informative exceptions if the connector fails to connect to ClickHouse
- Configuration validation: Checks for required configuration parameters before attempting to connect

## Tables created

### `TEST_TABLE`

The `TEST_TABLE` table contains dummy data with the following schema:

```json
{
    "table": "test_table",
    "primary_key": ["id"],
    "columns": {
        "id": "INT",
        "created_at": "UTC_DATETIME"
    }
}
```

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
