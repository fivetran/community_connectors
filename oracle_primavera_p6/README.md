# Oracle Primavera P6 Connector Example

## Connector overview
This connector syncs data from the Oracle Primavera P6 EPPM "Data Service" REST API. It discovers all non-blacklisted tables via the metadata endpoints and syncs each one, one table at a time, using the `runquery` endpoint in `SYNC` mode.

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
fivetran init --template oracle_primavera_p6
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features
- Discovers tables and columns automatically via the `metadata/tables` and `metadata/columns/{tableName}` endpoints, or restricts the sync to an explicit `tables` list.
- Incremental sync per table using a `sinceDate` cursor, either from an explicit `incremental_tables` list or auto-detected from an `UPDATE_DATE`, `LASTUPDATEDATE`, `CHANGEDATE`, or `UPDATEDATE` column.
- Checkpoints after every table (and periodically mid-sync for very large tables) so upserts are flushed to the destination in bounded batches, without advancing a table's own incremental cursor until it fully finishes syncing.
- LOB columns (`BLOB`/`CLOB`/`NCLOB`/`LONG RAW`) are excluded, since `SYNC` mode does not support them.
- Table and column names are normalized to `lowercase_snake_case` for the destination schema; the original physical names are used for API calls.

## Configuration file
```json
{
  "username": "<YOUR_P6_DATA_SERVICE_USERNAME>",
  "password": "<YOUR_P6_DATA_SERVICE_PASSWORD>",
  "config_code": "<ds_p6adminuser_OR_ds_p6reportuser_OR_ds_unifier>",
  "base_url": "<YOUR_P6_DATASERVICE_BASE_URL_EG_https://p6.oraclecloud.com/YOUR_TENANT/pds/rest-service/dataservice/>",
  "tables": "<OPTIONAL_COMMA_SEPARATED_TABLE_NAMES_OR_OMIT_FOR_ALL>",
  "incremental_tables": "<OPTIONAL_COMMA_SEPARATED_INCREMENTAL_TABLE_NAMES_OMIT_FOR_AUTO_DETECT>"
}
```

- `username` - P6 Data Service username.
- `password` - P6 Data Service password.
- `config_code` - one of `ds_p6adminuser`, `ds_p6reportuser`, or `ds_unifier`. Defaults to `ds_p6adminuser` if omitted.
- `base_url` - your P6 Data Service base URL, including your tenant path. Must start with `http://` or `https://`.
- `tables` - comma-separated list of table names to sync (matching `physicalTableName`, falling back to `displayTableName`, case-insensitive). Omit this key, or leave it empty, to sync all non-blacklisted tables.
- `incremental_tables` - comma-separated list of table names (matching either `physicalTableName` or `displayTableName`) that should sync incrementally via `sinceDate`. When this key is present at all (even as an empty string), it fully overrides auto-detection: any in-scope table not listed here is fully resynced every run, regardless of whether it has an update-date column. Omit the key entirely to fall back to column-based auto-detection.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Authentication
This connector authenticates to the P6 Data Service using HTTP Basic Auth. Refer to `def build_headers` in `connector.py`.

To set up authentication:

1. Ask your Primavera P6 administrator for a Data Service account with the appropriate role (`ds_p6adminuser`, `ds_p6reportuser`, or `ds_unifier`) and your tenant's Data Service base URL.
2. Provide the account's username and password as `username` and `password` in `configuration.json`.
3. Provide the base URL as `base_url`, and the account's role as `config_code`, in `configuration.json`.

## Pagination
Each `runquery` call returns a page of rows for one table. Refer to `def sync_table` in `connector.py`. Pagination follows the `nextKey`/`nextTableName` values returned by the API, parsed by `def parse_pagination`, until no more pages remain for that table.

## Data handling
Refer to `def schema` and `def update` in `connector.py`. Table and column metadata is discovered dynamically, LOB columns are excluded, and names are sanitized to `lowercase_snake_case`. Each table's sync type (full or incremental) is resolved from `incremental_tables` when present, or auto-detected from update-timestamp columns otherwise, by `def resolve_sync_type`.

## Error handling
Refer to `def request_with_retries` in `connector.py`. Connection errors, timeouts, chunked-encoding errors, and HTTP 429/500/502/503/504 are retried with backoff (429 honors `Retry-After`). HTTP 400/404/405/406/415 fail fast with a `RuntimeError`. HTTP 401/403, or an invalid `config_code`, abort the entire sync immediately. Any other per-table error raised by this connector's own request/response handling is logged and that table is skipped, with the sync continuing for the remaining tables; an unexpected error (for example, from an SDK operation) is not swallowed and fails the sync.

## Tables created
Tables are created dynamically based on the P6 tables discovered (or listed in `tables`). Refer to `def schema` in `connector.py`: each table declares only its `table` name and, when the source reports primary-key columns, its `primary_key`; the Fivetran SDK infers column types from the upserted rows.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
