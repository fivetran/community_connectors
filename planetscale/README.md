---
name: "PlanetScale"
description: "Sync data from PlanetScale (Vitess) databases, including deletes, using the VStream change stream without polling the source."
category: "Databases"
---

# PlanetScale Connector Example

## Connector overview

This connector replicates tables from a PlanetScale database to Fivetran destinations, including deletes. PlanetScale runs on Vitess, and the connector reads VStream, the Vitess change stream, through the same gRPC API that the partner-built [PlanetScale Go connector for Fivetran](https://github.com/planetscale/fivetran-source) uses. The first sync of a table copies it in primary key order. Every later sync resumes from a saved position and applies the inserts, updates, and deletes made since then. Because changes come from the stream, the connector does not scan tables to find them and does not need an `updated_at` column.

## Requirements

- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/getting-started) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```
fivetran init --template planetscale
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/getting-started/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- Replicates every base table that has a primary key, or only the tables listed in the `tables` configuration parameter
- Discovers shards and tables on every sync, so tables added to the database are copied automatically
- Copies each table in primary key order on its first sync and saves the last primary key copied, so an interrupted copy of a large table resumes where it stopped
- Applies inserts, updates, and deletes from the VStream change stream, without tracking row keys
- Keeps one saved position per table and shard
- Reads the stream one response at a time, so memory use stays flat however large a table is
- Checkpoints after each table that was copied or changed, and about once a minute while a large table is read
- Retries transient connection errors with exponential backoff
- Compiles the bundled Vitess proto files when the connector starts, so no generated code is checked in
- Fails with a clear message when the configuration is wrong or PlanetScale no longer has the saved position

## Configuration file

The connector requires the connection details of a PlanetScale database branch. All values must be strings.

```json
{
  "host": "<YOUR_PLANETSCALE_HOST>",
  "database": "<YOUR_PLANETSCALE_DATABASE>",
  "username": "<YOUR_PLANETSCALE_USERNAME>",
  "password": "<YOUR_PLANETSCALE_PASSWORD>",
  "tables": "<OPTIONAL_COMMA_SEPARATED_TABLE_NAMES>"
}
```

The configuration parameters are:

- `host` (required): Host name of the PlanetScale database, for example `aws.connect.psdb.cloud`. Do not include a scheme, port, or path.
- `database` (required): Name of the PlanetScale database (keyspace).
- `username` (required): Username of the PlanetScale password.
- `password` (required): Password for the PlanetScale branch.
- `tables` (optional): Comma-separated names of the tables to replicate, for example `orders, order_items`. Remove this parameter to replicate every table. The sync fails with a clear message if a listed table does not exist, is a view, or has no primary key.

> Note: When submitting connector code as a community connector in the open-source [Community Connector repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The connector requires the `pymysql` package for the MySQL protocol connection that discovers shards, tables, and primary keys.

```
pymysql==1.2.3
```

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

The `grpcio` and `grpcio-tools` packages that VStream needs, and the `protobuf` package that `grpcio-tools` installs with them, are pre-installed in the Connector SDK runtime, so they are not listed. The `grpcio-tools` package also compiles the bundled Vitess protobuf definitions when the connector starts (refer to the [Additional files](#additional-files) section).

> Note: `fivetran debug` runs in your local Python environment, and Connector SDK 3.0 does not install `grpcio` or `grpcio-tools` there. To debug locally, install them together with the packages in `requirements.txt`, for example with `pip install grpcio grpcio-tools`.

## Authentication

The connector authenticates with a PlanetScale username and password. The MySQL protocol connection on port 3306 discovers the shards, tables, and primary keys. It uses them with a verified TLS connection (refer to the `connect_mysql()` function). The gRPC connection on port 443 reads VStream. It sends them as HTTP Basic credentials in the `authorization` metadata, which is how PlanetScale's own Go client authenticates (refer to the `build_auth_metadata()` function).

To get the credentials:

1. Sign in to the [PlanetScale dashboard](https://app.planetscale.com/) and open the database you want to replicate.
2. Select **Connect**, choose the branch you want to replicate, and create a new password for it. A password with read-only access is enough because the connector never writes to PlanetScale.
3. Copy the host, username, and password into the `configuration.json` file, and enter the database name. PlanetScale shows the password only once.

> Note: A password belongs to one branch. Saved positions from one branch are not valid on another, so after you point the connection at a different branch or database, run **Resync All Data**.

## Pagination

The connector does not page through a table with queries and never runs a table scan. Vitess copies each table through VStream in primary key order and reports the last primary key it copied, which the `Cursor` class saves. A pass lasts up to 15 minutes. If a pass ends before the table is finished, the `sync_shard()` function starts another pass from the saved key within the same sync, and the `stream_pass()` function reads each pass one response at a time. After three passes in a row in which the saved cursor does not advance, the connector gives up on that table (refer to the Error handling section).

## Data handling

The connector reads each table through VStream and sends the changes to Fivetran as they arrive.

- Discovery – The `discover_source()` function connects over the MySQL protocol, lists the shards with `discover_shards()`, and lists the base tables with their primary keys with `discover_tables()`. Views and Vitess internal tables are excluded. Tables without a primary key are skipped with a warning (refer to `describe_skipped_tables()`), because deletes cannot be applied without a key. The `select_tables()` function applies the `tables` parameter.
- First sync of a table – The `sync_table()` function truncates the table in the destination, which marks the existing rows as deleted, and then copies it. Vitess reads the table in primary key order and reports the last primary key it copied. The `Cursor` class saves that key with the GTID position, so an interrupted copy resumes where it stopped. It also saves a flag that stays set until VStream reports the copy as complete, so a table counts as up to date only after its copy has finished. A copy that was interrupted before the first row starts over.
- Later syncs – The `peek_position()` function reads the shard's current position once, when the sync of a table and shard starts. The `stream_pass()` function applies the changes from the saved position until the transaction at that position has ended, however many passes that takes. Anything newer waits for the next sync, so a sync can finish slightly behind the source.
- Row changes – The `emit_row_changes()` function turns inserts and updates into upserts and deletes into delete operations. A change to a primary key deletes the old key with `delete_row()` and upserts the new row. The destination marks deleted rows with `_fivetran_deleted`.
- Data types – The `to_python()` function converts integers to `int`, floats to `float`, and binary values to `bytes`. Everything else arrives as text, decoded as UTF-8 with a Latin-1 fallback. For example, decimals, dates, times, and JSON values arrive as text, and ENUM values arrive as their text label.
- Schema – The `schema()` function declares only the type of each primary key column, using `primary_key_sdk_type()`. A sync whose first operation is a delete has no upsert to infer the key type from. All other columns are inferred.
- State – The `Checkpointer` class stores one cursor per table and shard and writes it with `op.checkpoint()` about once a minute while a table is read, after each table that was copied or changed, and at the end of the sync. The `prepare_state()` function drops the saved positions of tables that are no longer replicated and replaces state that belongs to a different database.

The state has this structure:

```json
{
  "source": {"database": "<database>"},
  "tables": {
    "<table>": {
      "shards": {
        "<shard>": {
          "position": "<GTID set>",
          "last_primary_key": "<base64 or null>",
          "is_copy_in_progress": false
        }
      }
    }
  }
}
```

## Error handling

- Transient errors – The `retry()` function retries MySQL connection failures (`is_transient_mysql()`) and the gRPC errors `UNAVAILABLE`, `RESOURCE_EXHAUSTED`, and `DEADLINE_EXCEEDED` (`is_transient_grpc()`) up to 4 attempts, waiting 2, 4, and 8 seconds between them. Each retry is logged. Permanent errors, such as wrong credentials or a TLS certificate failure on the MySQL connection, are raised immediately.
- Configuration – The `validate_configuration()` function raises a `ValueError` before any network call if a value is missing, is still a placeholder, or has the wrong format.
- Proto compilation – The `load_vitess_modules()` function raises a `RuntimeError` when the connector starts if `protoc` cannot compile the proto files.
- Table selection – The `select_tables()` function raises a `RuntimeError` if a listed table is missing, is a view, or has no primary key, or if no table can be replicated.
- Expired saved position – The `classify_stream_error()` function raises a `RuntimeError` that asks you to run **Resync All Data** when the PlanetScale binary logs that hold the saved position have expired. This happens when a connection is paused for longer than PlanetScale keeps its logs.
- Partial JSON updates – The `emit_row_changes()` function raises a `RuntimeError` when a row change carries a partial JSON value, because VStream then sends a JSON patch expression instead of the whole document. The sync stops instead of writing an incomplete JSON document.
- Interrupted and stalled streams – A pass that times out, drops, or ends before the table is up to date continues from the saved cursor. A pass counts as progress only if the saved cursor advanced, so rows that a dropped stream replays do not count. If three passes in a row make no progress, the `handle_stalled_passes()` function raises an error when the stream keeps dropping. If the passes keep timing out, it logs a warning, keeps the saved position, and the next sync continues.

## Limitations

- The connector has been tested on an unsharded keyspace, on tables of up to 65,536 rows, and on a simulated large table. Multi-million-row tables, sharded keyspaces, and `ALTER TABLE` statements during a sync have not been tested.
- Tables are synced one after another, each with its own stream.
- `SET` and `GEOMETRY` values are passed through unconverted, and the connector logs a warning.
- The connector does not provide the replica, shard selection, or starting GTID options of the Go connector.
- A table that is dropped from the database or removed from `tables` stops syncing. Its rows stay in the destination.
- Partial JSON updates, where a JSON column is changed in place and VStream reports a patch expression instead of the whole document, are not supported. The sync fails with an error.
- `TRUNCATE TABLE` and other DDL statements on the source are not propagated, because the connector ignores DDL events. After a `TRUNCATE TABLE`, the destination keeps the old rows until you run **Resync All Data**.
- Primary key values of a `bigint unsigned` column that are 2^63 or larger are not supported, because the SDK `LONG` type is a signed 64-bit integer.
- The connector does not declare the type of primary key columns of other types, such as `datetime`, `decimal`, and `enum`. If a sync deletes a row of such a table before it upserts any row of that table, the SDK fails the sync because the primary key data type is unknown.

## Tables created

The connector creates one destination table for each replicated source table. Each table keeps its source name and column names. The connector declares only the primary key columns in the `schema()` function, with a data type for each key column as follows. All other columns and their types are inferred from the values that the connector upserts.

- `bigint` and unsigned `int` columns are `LONG`
- `int`, `mediumint`, `smallint`, and `tinyint` columns are `INT`
- `char`, `varchar`, and `text` columns are `STRING`
- `binary` and `varbinary` columns are `BINARY`
- Primary key columns of other types are not declared, and their type is inferred

For example, if the source database has an `ORDERS` table with a `bigint` primary key `id`, the connector creates the `ORDERS` table, and the `schema()` function returns this definition for it:

```json
{
  "table": "ORDERS",
  "primary_key": ["id"],
  "columns": {
    "id": "LONG"
  }
}
```

## Additional files

The connector needs the Vitess protobuf definitions to call VStream and to read its rows and positions. Python has no ready-made VStream client, and generated Python code is not checked into this repository, so the connector ships the original `.proto` files in the `proto` folder and compiles them when it starts. The `load_vitess_modules()` function runs `protoc` from the pre-installed `grpcio-tools` package, writes the generated modules to a temporary directory, imports them, and removes the directory. Compiling takes about half a second, and the generated code always matches the `grpcio` and `protobuf` versions of the runtime.

The files are unmodified copies from the [Vitess v0.24.1 proto directory](https://github.com/vitessio/vitess/tree/v0.24.1/proto), the version the Go connector builds against:

- `proto/vtgateservice.proto` – The `Vitess` service that contains the `VStream` call.
- `proto/vtgate.proto` – The `VStreamRequest` message and its flags.
- `proto/binlogdata.proto` – The VStream events, GTID positions, filters, and copy progress messages.
- `proto/query.proto` – The rows, fields, and column types, which the connector also uses to save the last primary key copied.
- `proto/topodata.proto` – The tablet types.
- `proto/vtrpc.proto`, `proto/vtorcdata.proto`, `proto/vttime.proto`, and `proto/querythrottler.proto` – Messages that the other files import.

The Vitess project is licensed under the Apache License 2.0. The files are copied without changes, so they keep the license headers of the original files.

> Note: To use another Vitess version, replace the files in the `proto` folder with the files of that version. Keep all nine files, because they import each other.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
