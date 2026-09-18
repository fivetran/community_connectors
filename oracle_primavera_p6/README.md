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

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features
- Discovers tables and columns automatically via the `metadata/tables` and `metadata/columns/{tableName}` endpoints, or restricts the sync to an explicit `tables` list.
- Incremental sync per table using a `sinceDate` cursor, either from an explicit `incremental_tables` list or auto-detected from an `UPDATE_DATE`, `LASTUPDATEDATE`, `CHANGEDATE`, or `UPDATEDATE` column.
- Checkpoints after every table (and periodically mid-sync for very large tables) so upserts are flushed to the destination in bounded batches.
- LOB columns (`BLOB`/`CLOB`/`NCLOB`/`LONG RAW`) are excluded, since `SYNC` mode does not support them.
- Table and column names are normalized to `lowercase_snake_case` for the destination schema; the original physical names are used for API calls.

## Configuration file
```
{
  "username": "YOUR_P6_DATA_SERVICE_USERNAME",
  "password": "YOUR_P6_DATA_SERVICE_PASSWORD",
  "config_code": "ds_p6adminuser",
  "base_url": "https://p6.oraclecloud.com/YOUR_TENANT/pds/rest-service/dataservice/",
  "tables": "",
  "incremental_tables": ""
}
```

- `username` - P6 Data Service username.
- `password` - P6 Data Service password.
- `config_code` - one of `ds_p6adminuser`, `ds_p6reportuser`, or `ds_unifier`. Defaults to `ds_p6adminuser` if omitted.
- `base_url` - your P6 Data Service base URL, including your tenant path.
- `tables` - comma-separated list of table names to sync (matching `physicalTableName`, falling back to `displayTableName`, case-insensitive). Leave empty to sync all non-blacklisted tables.
- `incremental_tables` - comma-separated list of table names (from `tables`) that should sync incrementally via `sinceDate`. When this key is present at all (even as an empty string), it fully overrides auto-detection: any in-scope table not listed here is fully resynced every run, regardless of whether it has an update-date column. Omit the key entirely to fall back to column-based auto-detection.

> Note: When submitting connector code as a community connector in the open-source [Community Connector repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Authentication
Authentication uses HTTP Basic Auth: an `Authorization: Basic base64(username:password)` header is sent on every request, along with the `configCode` query parameter. Obtain P6 Data Service credentials and your tenant's base URL from your Primavera P6 administrator.

## Pagination
Each `runquery` call returns a page of rows for one table. Pagination follows the `nextKey`/`nextTableName` values returned by the API until no more pages remain for that table.

## Data handling
Refer to `schema()` and `update()` in `connector.py`. Table and column metadata is discovered dynamically, LOB columns are excluded, and names are sanitized to `lowercase_snake_case`. Each table's sync type (full or incremental) is resolved from `incremental_tables` when present, or auto-detected from update-timestamp columns otherwise.

## Error handling
Connection errors, timeouts, chunked-encoding errors, and HTTP 429/500/503 are retried with backoff (429 honors `Retry-After`). HTTP 400/404/405/406/415 fail fast with a `RuntimeError`. HTTP 401/403, or an invalid `config_code`, abort the entire sync immediately. Any other per-table error is logged and that table is skipped, with the sync continuing for the remaining tables.

## Tables created
Tables are created dynamically based on the P6 tables discovered (or listed in `tables`), with primary keys and column types taken from the P6 metadata endpoints.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
