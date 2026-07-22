# IBM Db2 for i Connector Example

## Connector overview

This connector allows you to sync data from an IBM Db2 for i (IBM i / AS400) database to a destination using the Fivetran Connector SDK. The connector uses the IBM i Access ODBC Driver via `pyodbc` to establish a connection to your IBM i system, reads data from the `CUSTOMER` table in batches, and upserts the rows to the destination. The first sync fetches all rows, while subsequent syncs are incremental and fetch only rows where `UPDATE_TIMESTAMP` is greater than the highest value seen in the previous sync.

This example connector demonstrates extracting customer data but can be modified to work with any IBM i table.

## Requirements

- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- IBM i Access ODBC Driver installed on the host running the connector — see [Additional files](#additional-files) for the installation script

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```
fivetran init --template ibm_db2i
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- Connects to a IBM Db2 for i database using the IBM i Access ODBC Driver
- Verifies TCP connectivity before opening the ODBC connection
- Fetches rows in configurable batches for memory-efficient syncs
- Checkpoints every 10,000 rows to support resumable syncs

## Configuration file

The connector uses a `configuration.json` file to define the connection parameters for the IBM i database. The connection parameters required are:

```json
{
    "hostname": "<YOUR_IBM_I_HOSTNAME>",
    "port": "<YOUR_IBM_I_PORT>",
    "database": "<YOUR_IBM_I_DATABASE>",
    "user_id": "<YOUR_IBM_I_USER_ID>",
    "password": "<YOUR_IBM_I_PASSWORD>"
}
```

The configuration parameters are:
- `hostname` (required): The hostname or IP address of your IBM i system.
- `port` (required): The port number for the IBM i Access ODBC connection (typically `8471`).
- `database` (required): The IBM i library/schema name used for both the ODBC connection and the SQL schema qualifier.
- `user_id` (required): The username to authenticate with the IBM i system.
- `password` (required): The password to authenticate with the IBM i system.
- `timeout_seconds` (optional): TCP connectivity check timeout in seconds. Defaults to `60`.

> Note: When submitting connector code as a [Community Connector](https://github.com/fivetran/community_connectors/tree/main) in the open-source [Connector SDK repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector. The `requirements.txt` file contains:

```
pyodbc==5.2.0
```

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector uses direct database authentication with a `user_id` and `password`. These credentials are specified in the `configuration.json` file and passed to `pyodbc` when building the ODBC connection string. The IBM i Access ODBC Driver handles authentication with the IBM i system.

To obtain the required credentials:

1. Log in to your IBM i system using an administrator profile.
2. Create a user profile with the necessary permissions to read from the target library and tables.
3. Note the system hostname or IP address and the port used for IBM i Access ODBC connections (typically `8471`).
4. Record the library (database) name you want to query as the `database` value in your configuration.
5. Use the user profile name and password as the `user_id` and `password` values in your configuration.

## Data handling

The connector performs the following data handling operations:
- Verifies TCP connectivity to the IBM i host before opening the ODBC connection
- Establishes an ODBC connection using the IBM i Access ODBC Driver
- Normalizes column names to lowercase before upserting to the destination
- Processes and upserts rows in batches of 1000

Incremental sync:
- On the first sync, all rows are fetched (full load)
- After each sync, the highest `UPDATE_TIMESTAMP` value seen is saved to state as `customer_last_update_timestamp`
- On subsequent syncs, only rows where `UPDATE_TIMESTAMP` is greater than `customer_last_update_timestamp` are fetched, ordered by `UPDATE_TIMESTAMP` ascending
- This requires the source CUSTOMER table to have an `UPDATE_TIMESTAMP` column that is updated whenever a row is modified

## Error handling

The connector implements the following error handling strategies:
- Configuration validation: Checks for all required configuration parameters before attempting connections
- TCP pre-check: Tests network reachability before opening the ODBC connection, with timing information logged on both success and failure
- Connection failures: Catches ODBC connection errors and raises a `RuntimeError` with elapsed time and the underlying exception message
- Logging: Uses the SDK logging framework to provide detailed timing information and progress updates

## Tables created

The connector creates and syncs the `CUSTOMER` table. Because the connector uses `SELECT *`, the full column set is determined at runtime by the `CUSTOMER` table on your IBM i system. Column data types are inferred by Fivetran from the values upserted.

The primary key columns for the `CUSTOMER` table are `c_d_id` and `c_id`.

Schema definition from connector:

```json
{
    "table": "customer",
    "primary_key": ["c_d_id", "c_id"]
}
```

## Additional files

The connector uses the `drivers/installation.sh` bash script that installs the IBM i Access ODBC Driver (`ibm-iaccess`) and unixODBC on Debian/Ubuntu systems. Run this script on any host where the connector will execute before running `fivetran debug` or deploying.
<<<<<<< HEAD
=======

The `ibm-iaccess_1.1.0.29-1.0_amd64.deb` package is bundled in the connector directory and is referenced by the installation script. It is included because the target environment cannot reach IBM's download servers directly.
>>>>>>> e6d0dfc (docs(ibm_db2i): address PR review comments on README and root README)

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
