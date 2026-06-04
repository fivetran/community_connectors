# Amazon Video Central Connector Example

## Connector overview

This Fivetran connector extracts report data from Amazon Video Central API and syncs it to your destination warehouse.

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
fivetran init --template amazon_video_central
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- OAuth authentication – Uses refresh token to automatically obtain access tokens
- Encrypted token storage – Refresh tokens are encrypted using Fernet encryption before storing in state
- Initial sync mode – Robust initial sync with progress tracking and checkpointing
- Incremental sync – Only fetches reports modified since the last sync
- Automatic pagination – Handles paginated API responses automatically
- CSV processing – Downloads and parses ZIP files containing CSV data
- Dynamic table creation – Creates tables based on report group, channel/studio, territory, and report type
- Automatic retry logic – Uses built-in retry attempts and exponential backoff for transient failures
- Comprehensive error handling – Graceful handling of API failures and network issues

## Configuration file

The connector requires the following configuration parameters in `configuration.json`:

```json
{
  "refresh_token": "<YOUR_REFRESH_TOKEN>",
  "client_id": "<YOUR_CLIENT_ID>",
  "client_secret": "<YOUR_CLIENT_SECRET>",
  "account_id": "<YOUR_ACCOUNT_ID>",
  "report_group_ids": "<YOUR_REPORT_GROUP_IDS>",
  "initial_sync_start": "<YOUR_INITIAL_SYNC_START>",
  "fernet_key": "<YOUR_FERNET_KEY>"
}
```

Configuration parameters:
- `refresh_token` (required) – OAuth refresh token for the Amazon API.
- `client_id` (required) – Amazon OAuth client application ID.
- `client_secret` (required) – Amazon OAuth client secret.
- `account_id` (required) – Amazon Video Central account ID.
- `report_group_ids` (required) – Comma-separated list of report group IDs to sync (e.g., `channels`, `channelsPremium`).
- `initial_sync_start` (required) – Start date for the initial sync in ISO 8601 format (e.g., `2020-01-01T00:00:00.000Z`).
- `fernet_key` (required) – Base64-encoded Fernet key used to encrypt the refresh token in state.

> Note: When submitting connector code as a [Community Connector](https://github.com/fivetran/community_connectors/tree/main) in the open-source [Connector SDK repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector requires the `cryptography` library for Fernet token encryption:

```
cryptography==44.0.2
```

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

This connector uses OAuth 2.0 refresh token flow to authenticate with the Amazon Video Central API. The `refresh_token`, `client_id`, and `client_secret` from `configuration.json` are exchanged for a short-lived access token on each sync via `get_access_token(configuration, state)`.

Amazon returns a new refresh token with every access token response. The connector encrypts the latest refresh token using Fernet symmetric encryption and stores it in state so that each subsequent sync uses the rotated token rather than the original one from configuration. Encryption and decryption are handled by `encrypt_token(token, fernet_key)` and `decrypt_token(encrypted_token, fernet_key)`. The `fernet_key` in `configuration.json` must be a valid base64-encoded Fernet key.

## Pagination


The connector uses limit/offset pagination with a page size of 50. On each iteration it appends `limit` and `offset` to the request parameters and advances the offset by the page size. Pagination stops when the API response contains no `next` field or returns an empty `data` array. All pages are accumulated and returned as a single list to the caller.

Refer to `paginated_request(base_url, access_token, params)`.

## Data handling


The connector processes each configured report group in sequence. For each group it fetches all channels and studios, then retrieves the available report types for each channel. If reports exist for a given type, it downloads the ZIP archive from the pre-signed S3 URL, extracts and processes the CSV in memory to avoid disk I/O, and upserts each row to a dynamically named destination table. Table names are generated by `clean_table_name` by lowercasing and sanitising the concatenation of report group, channel/studio name, territory, report type, and cadence. All timestamps are handled in UTC format.

Refer to `update(configuration, state)`, `download_and_parse_csv(download_url)`, and `clean_table_name(name)`.


### Initial sync

On the first sync (empty state) or when `initial_sync_in_progress` is set in state, the connector syncs all reports from `initial_sync_start`. It checkpoints after each channel/studio is fully processed, storing `processed_channels` in state so that a failed sync can resume from where it left off rather than starting over. Once all channels are processed, `initial_sync_in_progress` and `processed_channels` are cleared from state.

### Incremental sync

On subsequent syncs the connector uses `last_sync_time` from state to fetch only reports with a `modifiedDate` at or after that value. A single checkpoint is written at the end of the sync.

## Error handling


All HTTP calls — token refresh, API requests, and file downloads — are wrapped in `retry_with_backoff`, which retries on status codes 429, 500, 502, 503, and 504 using exponential backoff with configurable attempts and base delay controlled by `__DEFAULT_MAX_RETRIES` and `__DEFAULT_BASE_DELAY_SECONDS` connector constants. A 401 response raises `RuntimeError` immediately without retrying. If retries are exhausted for a download, ZIP extraction, or CSV parsing operation, the connector raises `RuntimeError` and the sync stops rather than skipping the affected report.

Refer to `retry_with_backoff(func, max_retries, base_delay, operation_name)`.


## Tables created

The connector creates two types of tables:

### Report metadata table: `report_metadata`

Contains one row per report with metadata:

| Column | Description |
|--------|-------------|
| `report_id` | Unique identifier for linking to data tables |
| `report_group_id` | The report group (e.g., `channels`, `channelsPremium`) |
| `channel_studio_id` | Unique channel/studio identifier |
| `channel_studio_name` | Channel/studio name |
| `channel_studio_territory` | Territory code (e.g., `US`, `CA`, `DE`) |
| `report_type_id` | Unique report type identifier |
| `report_type_name` | Human-readable report type name |
| `report_cadence` | Report frequency (`Daily`, `Weekly`, `Monthly`) |
| `report_date_begin` | Report period start date |
| `report_date_end` | Report period end date |
| `report_modified_date` | When the report was last modified |
| `report_num_rows` | Number of rows in the original report |
| `download_url` | Pre-signed S3 URL for the report file |

### Data tables: `{report_group_id}_{channel_studio_name}_{territory}_{report_type_name}_{cadence}`

Contains the actual CSV data with all original CSV columns from the report, plus `report_id` which links back to the `report_metadata` table. Primary keys are not defined in the schema since the data structure varies by report type.

Example data table name: `channelspremium_bbc_select_us_first_and_last_vod_title_daily`

### Linking tables

Join data tables with metadata using the `report_id` field:
```sql
SELECT dm.*, rm.report_date_begin, rm.channel_studio_name, rm.channel_studio_territory
FROM channelspremium_bbc_select_us_first_and_last_vod_title_daily dm
JOIN report_metadata rm ON dm.report_id = rm.report_id
```

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
