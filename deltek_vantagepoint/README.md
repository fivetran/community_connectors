---
name: Deltek Vantagepoint
description: Sync Hubs master data, detail/child collections, billing invoice headers, and General/Configuration, Workflow, and Third-Party Integration reference data from the Deltek Vantagepoint REST API, including a monthly full truncate-and-resync so source deletions are reflected in the destination.
category: SaaS & APIs
---

# Deltek Vantagepoint Connector Example

## Connector overview
This connector syncs data from the Deltek Vantagepoint REST API to a Fivetran destination. It replicates Hubs master data (employees, firms, contacts, projects, activities, marketing campaigns, and boilerplates), their detail/child collections, billing invoice headers, and General/Configuration, Workflow, and Third-Party Integration reference data. See Features for a full list of what this connector supports.

## Requirements
- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```bash
fivetran init --template deltek_vantagepoint
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features
- Syncs Hubs master data: employees, firms, contacts, projects, activities, marketing campaigns, and boilerplates.
- Syncs each Hub's detail/child collections (for example employee skills, firm addresses, project team members) and billing invoice headers for top-level projects.
- Syncs General/Configuration, Workflow, and Third-Party Integration reference data.
- Incremental sync on Hubs tables using the `ModDate` audit column, with page-based pagination.
- Monthly full truncate-and-resync of Hubs tables and their detail/billing tables, so source deletions are reflected in the destination.
- Configurable start date for a table's first sync, to bound data volume on an initial run.
- Retry with exponential backoff on transient errors, and automatic access token refresh.
- A table-level failure (for example a permission error) is logged and skipped rather than failing the entire sync, except during the table's monthly full resync - see Error handling.

## Configuration file
The connector requires the following configuration parameters in the `configuration.json` file. This configuration is uploaded to Fivetran and defines how the connector authenticates with Vantagepoint and selects a sync start date.

```json
{
  "base_url": "<YOUR_BASE_URL>",
  "username": "<YOUR_USERNAME>",
  "password": "<YOUR_PASSWORD>",
  "database": "<YOUR_DATABASE>",
  "client_id": "<YOUR_CLIENT_ID>",
  "client_secret": "<YOUR_CLIENT_SECRET>",
  "culture": "<YOUR_CULTURE>",
  "initial_sync_start_date": "<YOUR_INITIAL_SYNC_START_DATE>",
  "full_resync_day_of_month": "<YOUR_FULL_RESYNC_DAY_OF_MONTH>"
}
```

Required parameters:

- `base_url`: Your Vantagepoint tenant root, for example `https://YOUR_TENANT.deltekfirst.com/YOUR_FIRM`. Do not include `/api` - it is appended automatically.
- `username`: Vantagepoint username for the service account used by the connector.
- `password`: Password for the service account.
- `database`: Vantagepoint database name.
- `client_id`: OAuth2 client ID.
- `client_secret`: OAuth2 client secret.

Optional parameters:

- `culture`: Locale passed to the token request. The default is `en-US`.
- `initial_sync_start_date`: Formatted as `YYYY-MM-DDTHH:MM:SS`, for example `2026-08-01T00:00:00`. A persistent lower bound on how much history this connector ever syncs, not a one-time bootstrap setting: it's used as the starting cursor on a table's very first sync, and is reapplied on every later monthly full resync too (see Data handling), so moving it forward after data has already been synced will remove the now out-of-range history at the next monthly resync rather than being ignored.
- `full_resync_day_of_month`: Day of the month that triggers the monthly full resync described under Data handling. The default is `1`, and the value is clamped to the current month's actual length.

> Note: When submitting connector code as a community connector in the open-source [Community Connector repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file
This connector does not require a `requirements.txt` file. It uses the Fivetran Connector SDK and the `requests` library, which are available in the Connector SDK runtime environment.

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
The connector authenticates using OAuth2 password grant against Vantagepoint's token endpoint (`{base_url}/api/token`), using the `username`, `password`, `database`, `client_id`, and `client_secret` configuration values. The resulting access token is valid for 12 hours, comfortably longer than a single sync, and is refreshed automatically if a request returns a 401 response. Refer to `get_access_token` in `authentication.py`.

Contact your Vantagepoint system administrator to obtain the `client_id` and `client_secret`, and to set up a dedicated service account for this connector.

## Pagination
Hubs tables are paginated using `page` and `pageSize` query parameters. Refer to `sync_table` in `sync_operations.py`. The connector requests pages of records and continues until a page returns fewer records than the configured page size. Detail/child collections and reference tables are not paginated - each request returns the full result set for that parent record or table.

## Data handling
Hubs tables sync incrementally: each request filters on the `ModDate` audit column using the table's last saved cursor, and only parent records returned by that sync trigger a fetch of their detail/child collections and, for top-level projects, billing invoice headers. Refer to `sync_table` and `sync_children_for_record` in `sync_operations.py`.

Because this API never signals that a record was deleted, Hubs tables (and their detail/child and billing tables) are also fully truncated and resynced once per calendar month, on or after the configured `full_resync_day_of_month`. This lets records removed at the source be reflected as removed in the destination, since the full resync's upserts replace the truncated data. The monthly resync restarts from `initial_sync_start_date` (or full history if that value isn't set), the same as the first sync ever, so it stays consistent with that configured bound instead of pulling in additional history every month. If a monthly resync is interrupted partway through, only the tables that did not finish are retried on the next sync - tables that already completed are not redone. Refer to `should_run_full_resync` and `truncate_table_and_details` in `sync_operations.py`.

General/Configuration, Workflow, and Third-Party Integration reference tables have no audit column to filter on, so they are truncated and fully replaced on every sync. Refer to `sync_config_table` in `sync_operations.py`.

## Error handling
Requests that fail with a timeout, connection error, or a 429/5xx response are retried with exponential backoff. Refer to `get_api_response` in `rest_client.py`.

If a table fails with an unrecoverable error (for example a 403 permission error), that table is logged and skipped for the rest of the sync, and the sync continues with the remaining tables. A summary of skipped tables is logged at the end of the sync. Refer to `update` in `connector.py`.

> Note: This leniency does not apply to a table during its monthly full resync. Because that table has already been truncated, a failure partway through the resync is raised and fails the sync, rather than leaving the destination with incomplete data reported as a successful sync.

## Tables created
- Hubs master data: `employee`, `firm`, `contact`, `project`, `activity`, `marketing_campaign`, `boilerplate`.
- Detail/child collections for each Hub, for example `employee_skill`, `employee_credential`, `firm_address`, `contact_project`, `project_team_member`, `activity_link`, and similar tables scoped to their parent Hub.
- `invoice_header` - billing invoice headers for top-level projects.
- General/Configuration, Workflow, and Third-Party Integration reference tables, for example `organization`, `company`, `role`, `user`, `workflow_event`, `workflow_action`, and `thirdparty_integration_account`.

Refer to `table_registry.py` for the complete list of tables, their source endpoints, and primary keys.

## Additional files
The connector's logic is split across the following files by concern:

- `sync_limits.py` - constants used throughout the connector, such as retry limits, page size, and the default day of month for the monthly full resync.
- `table_registry.py` - the list of tables synced by the connector and the Vantagepoint endpoint, primary key, and (where applicable) parent/child relationship for each one.
- `authentication.py` - configuration validation and sign-in to obtain an access token.
- `rest_client.py` - the HTTP request logic used by every table sync, including retries, backoff, and automatic token refresh.
- `schema_builder.py` - defines the tables and primary keys delivered to the destination.
- `sync_operations.py` - the sync logic for each table type, including the monthly full resync.
- `connector.py` - the entry point that brings the above together.

A handful of endpoints are intentionally not synced by this connector - see the comments in `table_registry.py` for which ones and why. One returns plaintext credentials, one requires a custom hub name specific to your instance, and one has no documented example available to verify its format against.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
