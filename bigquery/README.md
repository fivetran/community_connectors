---
name: BigQuery
description: Sync large BigQuery tables from one or more datasets and projects with the Storage Read API, using partitions for incremental syncs that pick up modified partitions, with schema change detection
category: Cloud Data Warehouses
---

# BigQuery Connector Example

## Connector overview

This connector replicates the Google BigQuery tables that are listed in `table_specs.py`, from one or more datasets and projects, using the Connector SDK. It is built for large tables. It reads data with the [BigQuery Storage Read API](https://cloud.google.com/bigquery/docs/reference/storage), pushes column selection and row filters to BigQuery, and uses table partitions to limit how much data is scanned in both the initial historical sync and incremental syncs. Each BigQuery dataset becomes a schema in the destination, using the SDK's multiple schemas support.

The tables to sync are listed in `TABLE_SPECS` in `table_specs.py`: one entry per table, written as `dataset.table` or `project.dataset.table`. The connector reads each table's columns, partitioning, size and declared primary key from BigQuery. Every table needs a primary key, declared in BigQuery or set in its entry. Only the listed tables are read.

The main goal of the incremental logic is that no data is missed. The exceptions are deletes, which are not replicated, and late streamed rows for an older partition, which need a lookback. See [Streamed tables](#streamed-tables) and [Limitations and future improvements](#limitations-and-future-improvements).

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
fivetran init --template bigquery
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/getting-started/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

> Note: List the tables to sync in `table_specs.py` before running `fivetran debug`. See [Table specifications](#table-specifications).

## Features

- Multiple datasets in one connection, each written to its own destination schema, including tables from other projects such as a shared data project or `bigquery-public-data`.
- A primary key, declared in BigQuery or set in `table_specs.py`, for every synced table, so changed rows update in place and are never duplicated.
- An initial historical sync that reads one batch of partitions at a time and saves its progress after each batch, so an interrupted sync resumes where it stopped.
- An incremental sync that reads new partitions plus every partition that BigQuery reports as modified since the last sync, with an automatic extra lookback for tables that receive streamed rows and an optional lookback for any table.
- Partition pruning: row filters on the partition column are applied by BigQuery, so partitions outside the range are not scanned.
- Support for time-unit column partitioning (hourly, daily, monthly, yearly), ingestion-time partitioning and integer-range partitioning.
- A cursor-based incremental sync for tables that are not column-partitioned, using an update timestamp column.
- Detection of schema changes in the source tables, with an automatic re-read when a column is added or changes type.
- Per-table filters written like a WHERE clause, including filters relative to the sync time such as `event_date >= today-90d`.
- Per-table column include and exclude lists, and a `skip` option.
- Parallel reads of several tables and several Storage Read API streams per table, with periodic checkpoints inside large batches to keep memory use within the connector limit.
- A setup form with a connection test, and short, readable logs. See [Reading the logs](#reading-the-logs).

## Configuration file

The connector requires the following configuration parameters:

```json
{
  "service_account_key": "<YOUR_GOOGLE_SERVICE_ACCOUNT_KEY_JSON>",
  "project_id": "<YOUR_GOOGLE_CLOUD_PROJECT_ID>"
}
```

- `service_account_key` - The full service account key JSON, as a single string with its inner quotes escaped. Required.
- `project_id` - The Google Cloud project that the service account belongs to. It must be the same as the `project_id` inside the key, otherwise the connection test and the sync fail with a message that names both. This is the project that is billed for reads and queries. Required.

The tables to sync are not part of the configuration. They are listed in `table_specs.py`, which is part of the deployed code.

> Note: When submitting connector code as a community connector in the open-source [Community Connector repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

### Setup form

The connector defines a setup form with two fields: the service account key (a masked password field) and the project. The form includes a Test connection check, which is quick on purpose: it does not depend on the number of tables, and it never reads data. It has two parts:

- Without calling BigQuery - the project and key are present, the key is a service account key, and `project_id` matches the project inside the key. `TABLE_SPECS` is not empty, every key names one table, no table is listed twice, and every `primary_keys` and `filter` can be read.
- One call to BigQuery - the metadata of the first listed table is read, with a 15 second timeout (`SETUP_TEST_TIMEOUT_SECONDS`) and no retries. This shows that Google accepts the key and that the service account can reach the table. A table that is not found, a rejected key, a permission error and no answer within the time each give a short message that says what failed and never includes the key.

The test does not check every table or the permissions that reading data needs. Whether every table exists, can be read and has a primary key is checked in full when the schema is read, which fails before any data is read, and again when each sync starts. Run `fivetran configuration` to fill in the form locally and create `configuration.json`, and `fivetran configuration --test` to run the connection test.

## Requirements file

The connector requires the Google Cloud client libraries, which are not pre-installed in the Connector SDK runtime:

```
google-cloud-bigquery==3.47.0
google-cloud-bigquery-storage==2.43.0
google-auth==2.62.1
pyarrow==25.0.1
protobuf==7.36.2
```

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

When you run `fivetran debug`, the SDK may suggest removing some of these packages. They are needed. The connector imports them from `bigquery_client.py`, and `pyarrow` is used to read the Arrow data of the Storage Read API.

## Authentication

The connector authenticates with a Google Cloud service account key supplied in `service_account_key`. The key is only used in memory and is never logged. To set it up, do the following:

1. In the Google Cloud console, make sure the BigQuery API and the BigQuery Storage API are enabled for the project that will be billed for reads and queries. This is the project that you enter as `project_id`.

2. Create a service account in that project, under IAM and Admin, Service Accounts.

3. Grant the service account the roles in the table below.

4. Create a JSON key for the service account, under Keys, Add key, Create new key, JSON.

5. Copy the full contents of the key file into `service_account_key` as a single string with its inner quotes escaped, and enter the project in `project_id`.

| Role | Granted on | Why it is needed |
|---|---|---|
| `BigQuery Data Viewer` | Each project, dataset or table that holds a listed table | Read table metadata and data |
| `BigQuery Read Session User` | The billing project (`project_id`) | Create Storage Read API sessions |
| `BigQuery Job User` | The billing project (`project_id`) | Run the small `INFORMATION_SCHEMA.PARTITIONS` query that lists partitions |

Without `BigQuery Job User`, the connector cannot list partitions. It then reads each partitioned table in full, in one pass with no batches, on every sync, so nothing is incremental until the permission is granted. It logs a warning and shows a dashboard warning that names the table. The same fallback is used when listing the partitions fails with a not-found or rejected-key error.

Application Default Credentials and workload identity federation are not supported yet. See [Limitations and future improvements](#limitations-and-future-improvements).

## Pagination

The connector does not page through results with tokens. It reads each table with the Storage Read API, in up to `READ_STREAMS_PER_TABLE` parallel streams of Arrow pages, and every page is converted and upserted before the next one is read. Partitioned tables are read in batches of contiguous partitions, and a batch is limited to about `BATCH_TARGET_ROWS` rows. See [Partition-based sync](#partition-based-sync).

## Data handling

### Table metadata

The connector reads the metadata of each table listed in `TABLE_SPECS`, in parallel: columns, partitioning, size, row count and declared primary key. It does not list datasets or tables. Only plain tables can be synced. A view, materialized view or external table cannot be read with the Storage Read API, so listing one stops the sync with an error that names it. Refer to `def get_table_plans` and `def _build_plan` in `bigquery_client.py`.

### Choosing a sync strategy

For each table the connector picks the first option that applies:

1. Partitioned tables (time-unit column, ingestion-time or integer-range) use partition-based sync. For ingestion-time tables the connector filters on the `_PARTITIONTIME` pseudo-column, and a table filter can use `_PARTITIONTIME` or `_PARTITIONDATE`.

2. Tables with an update timestamp column use cursor-based sync. The cursor is the first column found, case-insensitively, from the list in `PREFERRED_CURSOR_COLUMN_NAMES` in `table_specs.py`, which includes `_fivetran_synced`, `system_modstamp`, `last_modified_date`, `updated_at` and `last_updated`, in that order of preference. The column must be a `TIMESTAMP`, `DATETIME` or `DATE`. Columns such as a creation date are never inferred, because they would miss updates to existing rows.

3. All other tables are fully refreshed on every sync.

You can override the strategy and cursor column for any table in `table_specs.py`.

### Partition-based sync

The connector lists the table's partitions, with their row counts and last modified times, using `INFORMATION_SCHEMA.PARTITIONS`. It then reads batches of contiguous partitions. Each read filters on the partition column, so BigQuery scans only those partitions.

- Initial sync - all partitions are read in order. After each batch the connector saves a watermark, the highest partition fully synced. If the sync is interrupted, the next run continues after the watermark.
- Incremental sync - the connector reads every partition after the watermark, and every older partition that BigQuery reports as modified since the last completed sync. This is automatic, so late-arriving and corrected rows are picked up without any lookback. A modified partition is read in full.
- Optional lookback - `LOOKBACK_DAYS` and `LOOKBACK_PARTITIONS` default to 0. Set `lookback_days` or `lookback_partitions` for a table to also re-read its most recent partitions on every sync, as an extra margin.
- Automatic lookback for streamed tables - see [Streamed tables](#streamed-tables).
- Special partitions - rows with a `NULL` partition column are read until the first load completes and whenever they change. Rows in the `__UNPARTITIONED__` partition (the streaming buffer, or values outside the partition range) are not synced and are reported in the log.

How modified partitions are detected:

- The connector saves a high-water mark, the latest modification time that BigQuery reported for the table, when a sync completes. The next sync reads every partition modified after it. The mark comes from BigQuery's own clock, so a difference between the clock of the machine running the connector and Google's cannot hide a change.
- Changes can become visible slightly out of order, so partitions modified within `MODIFIED_MARGIN_SECONDS` (default 10 minutes) before the mark are also checked. The connector saves the exact modification time it saw for each of them, and reads one again only if that time has changed or it was not seen before. A partition that has not changed is therefore not read on every sync.
- An initial load that is interrupted keeps the point it started at. When it resumes, partitions that were already read but have changed since are read again.
- If the partitioning, primary key, strategy, cursor column, filter, `include`, `exclude`, `column_types` or `resync_token` of a table change, the saved progress no longer applies, and the table is read again from the start with a dashboard warning. See [Schema changes](#schema-changes) for changes in the source table itself.
- The connector relies on `last_modified_time` in `INFORMATION_SCHEMA.PARTITIONS`. BigQuery documents that it updates when data is inserted, loaded, streamed or modified in a partition, and that modifications which only delete records might not be reflected. Deletes are not replicated, so that does not change what is synced.

Refer to `def _sync_partitioned`, `def _select_partitions` and `def _changed` in `bigquery_client.py`.

### Streamed tables

Rows that are streamed into a table, with legacy streaming inserts or the Storage Write API, can be read at once. BigQuery's partition metadata does not show them until they leave the streaming buffer, and in a test with a small table that took about an hour. Google documents it as typically a few minutes and in rare cases up to 90 minutes.

- While a table has a streaming buffer, while rows wait in the `__UNPARTITIONED__` partition, and for two hours after either was last seen (`STREAMING_GRACE_SECONDS`), every sync re-reads at least the newest partition and also reads the rows that BigQuery has not put in a listed partition yet. It shows a dashboard warning. Tables that are only loaded in batches are not affected.
- For a column-partitioned table, the rows that are not in a listed partition are the rows whose partition value is after the newest listed partition, which is where the rows of a new day arrive. For an ingestion-time partitioned table they are the rows whose `_PARTITIONTIME` is still `NULL`. These reads do not move the watermark. When BigQuery later lists the partition, it is read as a new partition, and rows that were read twice are upserted over themselves.
- What BigQuery does when the buffer clears depends on the table. In a test, an ingestion-time partitioned table got its partition at that moment, with its modification time set to the time the rows left the buffer. The rows of a column-partitioned table moved to the `__UNPARTITIONED__` partition and were still there 48 minutes later, with their date partition not listed at all. The test watched for 110 minutes in total, and the rows could be read with the Storage Read API the whole time, which is why the connector reads them by value.
- Rows streamed into an older, already listed partition, for example late events, are only covered if the partition is among the newest `STREAMING_MIN_LOOKBACK`, because nothing shows which partition they belong to. Otherwise they are found when BigQuery shows the partition as modified, which for a column-partitioned table had not happened after 110 minutes in the test. Set `lookback_partitions` for a table that receives late events, to cover the partitions that they can reach.

### Cursor-based sync

The connector reads rows where the cursor column is greater than or equal to the last cursor seen, minus a margin, and also the rows whose cursor column is `NULL`. Re-reading rows is safe because upserts are idempotent. Storage Read API streams are not ordered, so the cursor moves forward only after the whole read finishes. If a large table fails part way through, the next sync reads it again from the previous cursor.

- Margin - `CURSOR_MARGIN_SECONDS` (default one hour, whole days with a minimum of one for a `DATE` column) is subtracted from the saved cursor, so a row that was committed late with an earlier cursor value is still found. Change it per table with `cursor_margin_seconds`. A row that commits later than the margin after its own cursor value is not found, so use a larger margin if your source can be that late.
- `NULL` cursor values - a comparison never matches `NULL`, so rows with no value in the cursor column are read on every sync, and a dashboard warning shows how many. If many rows have no cursor value, the table is re-read often, and a different cursor column or a partitioned source table is a better choice.
- Only changes that move the cursor column are found. A row that is updated without changing the column is not read again, and there is no partition metadata check as there is for partitioned tables. For a table that is updated in place without a reliable update timestamp, set `strategy` to `FULL`.

Refer to `def _sync_cursor_or_full` in `bigquery_client.py`.

### Schema changes

The connector saves the names and types of the columns it reads for each table, and compares them with the source table at the start of every sync.

- Column added or type changed - existing rows have no value, or a value of the old type, for that column, and incremental sync does not read old partitions again. By default (`on_schema_change` is `resync`) the saved progress for the table is dropped and the table is read again from the start, with a dashboard warning that names the columns. This can be a lot of data for a very large table. Set `"on_schema_change": "keep"` for a table to leave existing rows alone and only show the warning. The existing rows then have no value for an added column until their partition is read again.
- Column removed - the table is not read again. The destination keeps the column and its history, and new rows have no value for it.
- Field added inside a `STRUCT` column - this counts as a type change of that column.
- Columns that you leave out with `include` or `exclude` are not compared.
- Saved progress from before schemas were recorded is adopted without a re-read.

To force a table to be read again, for example after fixing bad data in the source, change its `resync_token` in `table_specs.py` to any new text or number. To reload every table, use Re-sync on the connection in the Fivetran dashboard.

A table that is read again is upserted over the existing rows, so rows that were deleted in the source stay in the destination. Set `"truncate_on_resync": True` for a table to mark the rows that are not sent again as deleted. This is only applied if the sync succeeds, and it also marks rows that fall outside a filter that you narrowed.

Whether the destination accepts a changed column type depends on the destination. Reading the table again sends the data, but it does not change how the destination handles the new type.

### Datasets and schemas

Each BigQuery dataset that has a table in `TABLE_SPECS` becomes a schema in the destination, and its tables are created in that schema. This uses the Connector SDK's multiple schemas support, so every table is declared and written with its dataset as the schema.

- A key is `dataset.table` for a table in the project of the service account key (`project_id`), or `project.dataset.table` for a table in another project. A project id may itself contain dots or a colon, for example `example.com:my-project.sales.orders`. Wildcards and table names without a dataset are not supported, and each key names exactly one table.
- Reads and queries are billed to `project_id`, whichever project the table is in. The service account needs read access to every project that is listed. Public datasets such as `bigquery-public-data` can be read by any service account, and are billed to your project.
- State and logs use the name `dataset.table`, or `project.dataset.table` for a table in another project.
- Every listed table is checked in full when the schema is read and when a sync starts: it must exist, be a plain table, have a primary key and have valid options. A mistake fails before any data is read. The connection test checks the form of every entry and reads only the first listed table.
- Fivetran normalizes schema and table names (lower case, with underscores). BigQuery names are case-sensitive, so tables such as `NETSUITE.orders` and `netsuite.orders` would become the same destination table. The connector stops with an error that names them instead of merging their data. Columns are checked the same way: `userId` and `user_id` would become one column, so the sync fails and asks you to use `exclude` for all but one. Two tables with the same dataset and table name in different projects are rejected in the same way.

> Note: Adding or removing the multiple schemas setup on a table that is already deployed changes where its data lands, and Fivetran does not move existing data. Use the final schema layout from the first sync.

### Choosing which tables to sync

`TABLE_SPECS` is the only list of tables. The connector cannot see which tables are selected in the Fivetran Schema tab, so it reads every table in the list. Row filters set in the Schema tab are applied after the data is extracted, and deselected tables and columns are very likely still read. Use `filter`, `skip`, `include` and `exclude` in `TABLE_SPECS` to reduce what is read from BigQuery. This is inferred from the `update()` signature and is not stated in the documentation, so confirm it in the BigQuery job history on your first sync.

- To sync a table, add an entry with its primary key. To stop syncing it, set `"skip": True` or delete the entry. The data that was already synced stays in the destination.
- Changing the list means changing `table_specs.py` and deploying the connector again.
- Only the listed tables are read. Nothing is discovered, so a dataset with thousands of other tables costs nothing.
- A table whose primary key is declared in BigQuery still needs an entry, because the entry is what puts it in the list. Use an empty entry: `"sales.orders": {}`.

### Filters

A table filter limits which rows are read. Write it like a simple WHERE clause in `TABLE_SPECS`:

```python
"filter": "event_date >= today-90d"
"filter": "event_date >= today-90d AND country = 'IE' AND amount > 10.50"
```

- Each condition is `COLUMN OPERATOR VALUE`, joined by `AND`. The operators are `>`, `>=`, `<`, `<=`, `=` and `!=` (`<>` also works). `OR`, `IN`, `LIKE`, `BETWEEN`, functions and parentheses are not supported, and give an error that shows the syntax.
- A value is a bare word, such as a number, a date (`2024-01-31`), `true` or a relative value, or text in single or double quotes. Quote a value that contains spaces, for example `ts >= '2024-01-01 10:30:00'`. Double the quote character to put one inside a value, as in `name = 'O''Brien'`.
- For `DATE`, `DATETIME` and `TIMESTAMP` columns a value can be relative to the start of the sync, in UTC: `today`, `today-90d`, `now`, `now-12h` or `now-7d`. A value that starts with `today` can only use days.
- In an incremental sync a relative value can only be used with `>` or `>=`, which shrinks the window as time passes. With `<`, `<=`, `=` or `!=` the window would move over rows that the connector had already passed, and those rows would never be read, so the connector stops with an error. Use a fixed value, or set the table's `strategy` to `FULL`.
- A `BOOLEAN` value must be `true` or `false`, and a `FLOAT` value must be a finite number. Anything else is an error, so a typo cannot read the wrong rows.
- Values are parsed according to the column type and are never inserted into SQL as written. The filter is checked when the table metadata is read, so a mistake fails the sync before any data is read.
- When the column is the table's partition column, BigQuery prunes partitions, so the partitions outside the range are not scanned. For ingestion-time tables use `_PARTITIONTIME` or `_PARTITIONDATE`. A filter on another column reduces the rows delivered, but BigQuery still scans the selected columns of the whole table.
- The filter applies to every read of the table. It does not remove rows that were already synced, and rows that fall out of a relative window stay in the destination.

### Primary keys

Every synced table needs a primary key. BigQuery does not enforce primary keys, and many tables do not declare one. The connector never guesses a key. It uses, in order:

1. `primary_keys` set for the table in `table_specs.py`: a list of column names, with several columns for a composite key, for example `["order_id", "line_number"]`.

2. The primary key declared on the table in BigQuery.

A table with neither cannot be synced. Without a key the destination cannot update a row, so every row that changed in BigQuery would be written as a new row next to its old version, and the old version would stay.

- A listed table without a key stops the sync, when the schema is read or the sync starts, with an error that names it.
- Key columns must exist and cannot be arrays, records, JSON, geography, interval or range columns. A column cannot be listed twice. Key columns are always read, even if they are listed in `exclude`.
- The key values must be unique and not NULL. The connector does not check this. A duplicate key overwrites an earlier row with the same key, and a NULL key fails the sync.

### Table specifications

Every entry of `TABLE_SPECS` in `table_specs.py` is one table to sync. The key is `dataset.table`, or `project.dataset.table` for a table in another project. Besides the primary key, all options are optional:

```python
TABLE_SPECS = {
    "analytics.events": {
        "primary_keys": ["event_id"],
        "filter": "event_date >= today-90d AND country = 'IE'",
        "exclude": ["raw_payload"],
    },
    "sales.order_lines": {
        "primary_keys": ["order_id", "line_number"],
        "lookback_days": 7,
    },
    "sales.scratch_table": {"skip": True},
    "my-data-project.sales.orders": {"primary_keys": ["order_id"]},
}
```

| Key | Description |
|---|---|
| `primary_keys` | Primary key columns, required unless the table declares a key in BigQuery. Overrides a declared key. |
| `strategy` | `FULL` or `INCREMENTAL`. |
| `cursor_column` | Cursor column for tables that are not column-partitioned. |
| `cursor_margin_seconds` | How far before the last cursor value a cursor-based read starts. Overrides `CURSOR_MARGIN_SECONDS` (default 3600) for this table. |
| `include`, `exclude` | Column lists. Primary key and cursor columns are always included. |
| `column_types` | Sets the Fivetran type of a column, for example `{"amount": {"type": "DECIMAL", "precision": 18, "scale": 2}}` or `{"notes": "STRING"}`. Used to change the default `DECIMAL(38, 9)` of `BIGNUMERIC` columns. |
| `skip` | `True` leaves the table out of the sync without deleting its entry. |
| `lookback_days` | Re-reads this many days of the most recent partitions on every sync, in addition to the partitions that BigQuery reports as modified. Overrides `LOOKBACK_DAYS` (default 0) for this table. |
| `lookback_partitions` | Sets the number of re-read partitions directly. Wins over `lookback_days`. Overrides `LOOKBACK_PARTITIONS` (default 0). This is the setting for integer-range partitioned tables, which have no time unit. |
| `on_schema_change` | `resync` (default) or `keep`. What to do when a column is added or changes type in the source. See [Schema changes](#schema-changes). Overrides `SCHEMA_CHANGE_DEFAULT`. |
| `resync_token` | Any text or number. Changing it makes the connector read the table again from the start. |
| `truncate_on_resync` | `True` marks the rows that are not sent again as deleted whenever the table is read again from the start. Defaults to `False`. |
| `filter` | A condition written like a WHERE clause. See [Filters](#filters). |

Constants at the top of `table_specs.py` control parallelism (`MAX_PARALLEL_TABLES`, `READ_STREAMS_PER_TABLE`), batch size (`BATCH_TARGET_ROWS`), memory use (`FLUSH_MAX_VALUES`, `FLUSH_MAX_BYTES`, `FLUSH_MIN_VALUES`, `FLUSH_INTERVAL_SECONDS`), the cursor margin (`CURSOR_MARGIN_SECONDS`), the optional lookback (`LOOKBACK_DAYS`, `LOOKBACK_PARTITIONS`), the automatic lookback for streamed tables (`STREAMING_GRACE_SECONDS`, `STREAMING_MIN_LOOKBACK`), the margin used to detect modified partitions (`MODIFIED_MARGIN_SECONDS`), and the size at which a fully re-read table triggers a warning (`FULL_REFRESH_WARNING_BYTES`).

### Data types

Only columns whose type cannot be inferred from the value are declared in `schema()`: `FLOAT64`, `NUMERIC`, `BIGNUMERIC`, `BYTES`, `DATE`, `DATETIME`, `TIMESTAMP`, `JSON`, `STRUCT`, `INTERVAL`, `RANGE` and arrays. Their values arrive as text, bytes or nested objects, and a float that is not declared would be read as a 32-bit `FLOAT` and lose digits. All other columns are inferred from their values, which leaves the schema free to evolve. A column that only holds `NULL` is not created until it has a value, unless its type is declared. The schema is rebuilt from the live BigQuery metadata on every sync. Declared columns do not appear in the Fivetran Schema tab before the first sync.

| BigQuery type | Fivetran type | Declared | Notes |
|---|---|---|---|
| `STRING`, `TIME`, `GEOGRAPHY` | `STRING` | inferred | `TIME` is text such as `10:11:12.123456`, and `GEOGRAPHY` is WKT text |
| `INT64` | `LONG` | inferred | Exact for the full 64-bit range |
| `FLOAT64` | `DOUBLE` | declared | Keeps all digits. `NaN` and infinity become `NULL`. |
| `BOOL` | `BOOLEAN` | inferred | |
| `BYTES` | `BINARY` | declared | Sent as bytes. Bytes inside a `STRUCT` or `ARRAY` are Base64 text. |
| `DATE`, `DATETIME`, `TIMESTAMP` | `NAIVE_DATE`, `NAIVE_DATETIME`, `UTC_DATETIME` | declared | Microseconds are kept. `TIMESTAMP` is in UTC. |
| `NUMERIC`, `BIGNUMERIC` | `DECIMAL` | declared | `NUMERIC` keeps its precision and scale. `BIGNUMERIC` is `DECIMAL(38, 9)` by default. |
| `JSON`, `STRUCT`, `INTERVAL`, `RANGE` and any `ARRAY` | `JSON` | declared | `INTERVAL` is `{"months", "days", "nanoseconds"}` and `RANGE` is `{"start", "end"}` |

A BigQuery type that is not in this table is left for Fivetran to infer, with a warning in the log. Set the type of any column with `column_types`, which also declares it.

### Memory use

Rows are read as Arrow pages and upserted immediately. The connector never holds a whole table in memory. The SDK holds upserted rows until a checkpoint, and one checkpoint flushes every table being read, so the connector counts the buffered cells (the cells that are sent, plus a weight of `ROW_WEIGHT_CELLS` for every row) and the size of the Arrow pages across all tables. It checkpoints when `FLUSH_MAX_VALUES` values are buffered, when the pages reach `FLUSH_MAX_BYTES` (which catches tables with very long text or JSON cells), or when `FLUSH_MIN_VALUES` are buffered and `FLUSH_INTERVAL_SECONDS` have passed. These checkpoints save the unchanged state, so a failure still re-reads the batch. The checkpoints that save the progress of a finished table or batch are limited to at most one per `FLUSH_INTERVAL_SECONDS`, and a last checkpoint at the end of the sync saves the rest. If a sync fails, at most about a minute of finished tables is read again, which is safe because upserts are idempotent. Lower `FLUSH_MAX_BYTES` or `FLUSH_MAX_VALUES` if a sync runs close to the 4 GB memory limit.

## Error handling

- Transient BigQuery errors (service unavailable, rate limits and quota errors, including those that BigQuery reports as a 403, and timeouts) are attempted four times in total, which is three retries. The wait before a retry starts at 5 seconds, doubles each time and is capped at 60 seconds, plus up to 50% random jitter, so a wait can reach about 90 seconds. A permission error is not retried. Refer to `def _retry` in `bigquery_client.py`.
- Metadata calls, partition queries and read-session creation have a 120 second timeout.
- Permission, authentication and not-found errors stop the sync with a message that names the failed step.
- Invalid configuration, such as a malformed key, a `cursor_column` that is not a timestamp or date column, an unsupported filter operator, a filter or primary key column that does not exist, or an unsupported filter column type, stops the sync before any data is read.
- A mistake in `TABLE_SPECS` (a table that does not exist or cannot be read, a missing primary key, an invalid option or filter) stops the sync before any data is read. The connection test catches a malformed entry and an unreadable first table. The other mistakes are found when the schema is read.
- A failure in one table fails the sync instead of skipping the table. Tables that are still queued are not started, the state of the tables that finished is saved, and the dashboard warnings are still shown. Refer to `def update` in `connector.py`.
- Data that is skipped or read in a degraded way produces a dashboard warning, grouped by reason, at the end of the sync. Refer to `def report_skipped` in `bigquery_client.py`.

## Tables created

One destination table is created for each table listed in `TABLE_SPECS`, in a schema named after its dataset. Names are normalized by Fivetran, and a `project.` prefix of a table in another project is not used. A table with `skip` set is not created. Each table has the columns of the BigQuery table, except those that you exclude, and the Fivetran system columns.

The schema is read from BigQuery, so the columns depend on your tables. As an example, the entry for `analytics.events` above is declared like this (refer to the `schema()` function). Only columns whose type cannot be inferred are listed:

```json
{
  "schema": "analytics",
  "table": "events",
  "primary_key": ["event_id"],
  "columns": {
    "event_date": "NAIVE_DATE",
    "created_at": "UTC_DATETIME",
    "properties": "JSON"
  }
}
```

## Additional files

The connector uses the following additional files:

- `bigquery_client.py` - Reads table metadata, plans each sync, and contains the partition-aware, cursor-based and full sync logic, the filter parser, the retry logic and the schema change detection.
- `progress.py` - Writes the short progress logs of tables and of the whole sync.
- `table_specs.py` - Holds `TABLE_SPECS`, the list of tables to sync, and the constants that control how they are read.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

### Performance

The speed of a sync is limited mostly by the SDK, which sends rows one at a time with `op.upsert`, and not by BigQuery. As a guide, a local `fivetran debug` run read about 5,000 rows per second from a table with 21 columns, including one long text column. The speed depends on the number of columns and the size of the values.

- Reading in parallel overlaps the waiting for BigQuery with the sending of rows. More threads do not make the sending itself faster, because the SDK serializes the calls.
- Cells that are `NULL` are not sent. The Fivetran documentation says that, for an upsert, "columns present in target table but not present in data passed in operation become NULL", so leaving them out gives the same result. Many tables are mostly `NULL`, so this is a large saving.
- Excluding columns that you never need is the best way to speed up a sync. The time is proportional to the cells that are sent. A column that you list in `exclude` is not read, not sent and not created in the destination, and it stays out of the destination even if values appear in BigQuery later.
- Only the first sync is long. Later syncs read only the new and changed rows.

The sync time shown on the Fivetran dashboard also includes the time to load the rows into the destination, which this connector does not control.

### Reading the logs

The logs are written so that progress can be followed without reading every line. The example below uses illustrative names and numbers. Tables run two at a time, so lines of different tables are mixed, and every line starts with the table name. A table that finishes quickly produces only a start line and a completion line.

```
Connected to BigQuery project my-project
Table metadata read in 2s: 2 table(s) to sync in 1 dataset(s), 41,200,000 rows, 18.3 GB
Strategies: 1 incremental by partition, 1 incremental by cursor column, 0 full reload
Largest tables: sales.events (17.9 GB), sales.orders (412.0 MB)
Sync started: 2 table(s), 2 at a time
sales.events: started (1/2) - initial load | partition column event_date (DAY), lookback 0 | primary key: event_id | 41,000,000 rows, 17.9 GB, 24 columns
sales.orders: started (2/2) - initial load | cursor column updated_at | primary key: order_id | 200,000 rows, 412.0 MB, 18 columns
sales.events: 730 partition(s), 730 to read (~41,000,000 rows) in 9 batch(es)
sales.orders: completed in 12s - 200,000 rows read, ~412.0 MB scanned (1/2 tables done; running: sales.events)
sales.events: batch 1/9 done [partitions 20240101..20240318 (78)] - 4,600,000 rows; 4,600,000 of ~41,000,000 rows read (11%), 4m10s, 18,400 rows/s
sales.events: progress - 5,300,000 of ~41,000,000 rows read (12%), 4m50s, 18,300 rows/s
sales.events: completed in 38m20s - 41,000,000 rows read, ~17.9 GB scanned (2/2 tables done)
Sync finished in 38m21s - 2/2 tables, 41,200,000 rows read, ~18.3 GB scanned (estimated)
```

- Start line - the mode (`initial load`, `resuming initial load`, `incremental` or `full reload`), how the table is read (partition column and granularity with the lookback, cursor column, or no partition or cursor column), the primary key, the size from BigQuery metadata, the number of columns and any filter.
- Partition line - for partitioned tables, how many partitions exist, how many the table filter rules out (only when the filter has a condition on the partition column), how many will be read and the rows in them, and the number of batches.
- Progress lines - a line about once a minute (`PROGRESS_LOG_INTERVAL_SECONDS`) for tables that take long, and one after every batch of partitions. They show the rows read, the speed, and the percentage when the expected size is reliable. When a filter or a cursor can make the real number smaller, the line shows `up to ~N expected` instead of a percentage.
- Completion line - the duration, the rows read, the estimated bytes that BigQuery will scan (the sum of the read sessions' estimates), how many tables are done and which others are still running.
- Failures - a failed table logs one error line with its name, the time and rows so far, and the error. The sync then fails.
- Warnings - retries of transient errors, and data that could not be read.
- Row counts and sizes in the start line come from BigQuery metadata and do not include rows in the streaming buffer. The rows in a completion line are the rows that were actually read after any filter.
- The details of each table and of each read session are only logged at the `DEBUG` level, which `fivetran debug` shows and production syncs do not.

### Local debug runs

In some local `fivetran debug` runs the sync pauses for about a minute right after it starts. The SDK's local memory monitor starts a subprocess while gRPC, which the BigQuery Storage client uses, is active. Run `GRPC_ENABLE_FORK_SUPPORT=0 fivetran debug` to avoid the pause. It comes from the local debug tooling, and whether it can happen in a Fivetran deployment has not been checked.

### Limitations and future improvements

- Deletes are not replicated. Rows deleted in BigQuery stay in the destination, including after a table is read again. Two things reload a table and mark the rows that are not sent again as deleted: `truncate_on_resync` together with a new `resync_token`, or a schema change with `truncate_on_resync` set. Rows that are removed from the source by partition expiration are not replicated either.
- Authentication - only service account keys are supported. Application Default Credentials and workload identity federation are planned.
- Cost controls such as a dry-run estimate before each read and a maximum billed bytes limit are not implemented. The connector logs the estimated bytes that each table's read sessions will scan when the table completes.
- Views and external tables are not supported. A listed view or external table stops the sync. List the table it reads from, or read the view through a table that you refresh yourself.
- Fully re-read tables - a table with no partition and no update-style cursor column is read in full on every sync. When it is at least `FULL_REFRESH_WARNING_BYTES` (100 GB), the sync shows a dashboard warning. Set `cursor_column` or `strategy` for the table, or partition the source table.
- Primary keys are never inferred and are required for every listed table. Each table needs its own entry, which has to be written by hand.
- Decimal precision - `NUMERIC` columns keep their declared precision and scale, which fits `DECIMAL(38, 9)`. `BIGNUMERIC` columns are `DECIMAL(38, 9)` by default, so values that need more than 9 decimal places are rounded and values with more than 29 integer digits become `NULL`. A dashboard warning names such columns. Override the type per column with `column_types`.
- Cursor-based tables only find rows whose cursor value moves, and not rows that commit later than the cursor margin. See [Cursor-based sync](#cursor-based-sync).
- Streamed rows - rows streamed into an older partition, such as late events, are only found when BigQuery's partition metadata shows the partition as modified, which can take an hour or more, unless you set `lookback_partitions` for the table. Rows for a new partition are read as soon as they can be read. See [Streamed tables](#streamed-tables).

#### Planned improvement: delete capture

An optional per-table mode, for example `capture_deletes`, would use BigQuery change history (the `CHANGES` function) to send deletes with `op.delete`. What has been checked so far:

- It needs `enable_change_history` to be set on the source table before the deletes happen, with `ALTER TABLE ... SET OPTIONS (enable_change_history = TRUE)`. That is only possible for tables that you own, and history from before it was enabled is incomplete.
- A DML `UPDATE` appears in the change history as a `DELETE` event with `_CHANGE_IS_FOR_UPDATE = TRUE`, followed by an `UPDATE` event with the new values. Only `DELETE` events with that flag set to `FALSE` are real deletes.
- A real delete event carries the deleted row, including its primary key columns.
- The table metadata does not show whether change history is enabled, so the connector would probe with a small `CHANGES` query. A table without it fails with the error `Change history is not enabled for table`.
- History is limited to the table's time travel window (7 days by default) and one call can cover at most one day, so a longer gap would need the table to be read again, with `truncate_on_resync`.
- Not yet checked: partition-level deletes, `TRUNCATE`, `WRITE_TRUNCATE` loads, `MERGE`, streamed rows and tables with row-level access policies.
