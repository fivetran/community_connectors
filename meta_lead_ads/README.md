# Meta Lead Ads Connector Example

## Connector overview
This connector syncs Facebook (Meta) Lead Ads data into your destination using the Meta Graph API. It discovers the pages and lead-generation forms configured (or all pages/forms accessible to the provided token) and syncs each lead into a single `leads` table, with the raw form field data preserved as JSON for downstream modeling.

Incremental sync uses a per-form cursor on `created_time`. Rows are checkpointed once at least `check_point_limit` new rows have been written for a form, with a final flush at the end of each form's pagination, so a failure only risks re-processing a bounded number of rows.

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
- Discovers pages and lead-generation forms automatically, or restricts the sync to explicit `page_ids` / `form_ids`.
- Single `leads` table with an explicit schema; the raw `field_data` array is preserved as a JSON string column for flexible downstream parsing.
- Threshold-based checkpointing per form to bound replay volume on failure without checkpointing on every row.
- Retries transient HTTP errors (429, 5xx) with exponential backoff.

## Configuration file
```
{
  "system_user_access_token": "YOUR_META_SYSTEM_USER_ACCESS_TOKEN",
  "page_ids": "ALL",
  "form_ids": "ALL",
  "initial_start_time": "2023-01-01T00:00:00+0000",
  "graph_version": "v24.0",
  "include_archived_forms": "false",
  "request_timeout_seconds": "30",
  "fetch_limit": "500",
  "check_point_limit": "3000"
}
```

- `system_user_access_token` - a Meta system user access token with permissions to list pages and read lead-gen forms/leads.
- `page_ids` - comma-separated page IDs to sync, or `ALL` to auto-discover every page the token can access.
- `form_ids` - comma-separated form IDs to sync, or `ALL` to sync every (non-archived, unless `include_archived_forms` is `true`) form on each page.
- `initial_start_time` - optional `YYYY-MM-DD` or full ISO8601 timestamp; used as the starting cursor only when no prior state exists. If omitted, the first sync starts from the earliest available leads.
- `graph_version` - the Graph API version to call.
- `include_archived_forms` - `"true"` or `"false"`; whether archived forms are included when `form_ids` is `ALL`.
- `request_timeout_seconds` - HTTP request timeout.
- `fetch_limit` - page size when requesting leads; lower this if you observe timeouts.
- `check_point_limit` - number of rows accumulated per form before a checkpoint is written.

> Note: When submitting connector code as a community connector in the open-source [Community Connector repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Authentication
Authentication uses a Meta system user access token, passed as `system_user_access_token`. The token must have permissions to list the target pages (`pages_show_list`) and to read lead-gen forms and leads for those pages. Generate the token through a Meta system user in Business Manager and grant it access to the relevant pages.

## Pagination
Page and form listings are paginated using the Graph API's `paging.next` cursor. Lead listings are paginated using the `after` cursor returned in `paging.cursors`; pagination for a form stops once no `after` cursor is returned.

## Data handling
Each lead is transformed into a single row with page and form metadata, `ad_id`, `created_time`, and a `field_data` column containing the raw list of field objects from the source, serialized as JSON. Refer to `_process_leads` in `connector.py`. Parsing individual form fields into columns is left to downstream transformation (for example, in a warehouse view or dbt model), since new fields may be added to forms without requiring a connector change.

## Error handling
Refer to `request_with_retries` in `http_helpers.py`. Transient HTTP errors (429 and 5xx) are retried with exponential backoff. Non-2xx responses that exhaust retries, and invalid JSON responses, are logged and treated as a failed request for that page of data. Pagination for a form stops if no further page cursor is returned.

## Tables created
The connector creates a single `leads` table (primary key: `lead_id`) with the following columns: `lead_id`, `page_id`, `page_name`, `form_id`, `form_name`, `created_time`, `ad_id`, and `field_data` (JSON array of `{"name": <str>, "values": [<str>, ...]}` objects).

## Additional files
- `http_helpers.py` - HTTP request helper with retry/backoff for Graph API calls.
- `meta_helpers.py` - page/form/lead discovery and pagination helpers for the Graph API.
- `validator.py` - configuration validation and parsing.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
