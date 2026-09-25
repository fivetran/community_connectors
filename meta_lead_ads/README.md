# Meta Lead Ads Connector Example

## Connector overview
This connector syncs Facebook (Meta) Lead Ads data into your destination using the Meta Graph API. It discovers the pages and lead-generation forms configured (or all pages/forms accessible to the provided token) and syncs each lead into a single `leads` table, with the raw form field data preserved as JSON for downstream modeling.

Incremental sync uses a per-form cursor on `created_time`. The cursor only advances once a form's pagination has fully completed; rows are still flushed to the destination periodically once at least `check_point_limit` new rows have been written for a form, so a failure only risks re-processing a bounded number of rows, never skipping any.

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
fivetran init --template meta_lead_ads
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features
- Discovers pages and lead-generation forms automatically, or restricts the sync to explicit `page_ids` / `form_ids`.
- Single `leads` table with an explicit schema; the raw `field_data` array is preserved as a JSON string column for flexible downstream parsing.
- Threshold-based checkpointing per form to bound replay volume on failure, without advancing the resumable cursor until a form's pagination fully completes.
- Retries transient HTTP errors (429, 5xx) with exponential backoff, and fails the sync on unrecoverable request errors instead of silently truncating data.

## Configuration file
```json
{
  "system_user_access_token": "<YOUR_META_SYSTEM_USER_ACCESS_TOKEN>",
  "page_ids": "<ALL_OR_COMMA_SEPARATED_PAGE_IDS>",
  "form_ids": "<ALL_OR_COMMA_SEPARATED_FORM_IDS>",
  "initial_start_time": "<OPTIONAL_ISO8601_START_TIME_EG_2023-01-01T00:00:00Z>",
  "graph_version": "<GRAPH_API_VERSION_EG_v24.0>",
  "include_archived_forms": "<TRUE_OR_FALSE_DEFAULT_FALSE>",
  "request_timeout_seconds": "<REQUEST_TIMEOUT_SECONDS_DEFAULT_30>",
  "fetch_limit": "<LEADS_PAGE_SIZE_DEFAULT_500>",
  "check_point_limit": "<ROWS_PER_CHECKPOINT_DEFAULT_3000>"
}
```

- `system_user_access_token` - a Meta system user access token with permissions to list pages and read lead-gen forms/leads.
- `page_ids` - comma-separated page IDs to sync, or `ALL` to auto-discover every page the token can access.
- `form_ids` - comma-separated form IDs to sync, or `ALL` to sync every (non-archived, unless `include_archived_forms` is `true`) form on each page.
- `initial_start_time` - optional `YYYY-MM-DD` or full ISO8601 timestamp with a timezone offset; used as the starting cursor only when no prior state exists. If omitted, the first sync starts from the earliest available leads.
- `graph_version` - the Graph API version to call.
- `include_archived_forms` - `true` or `false`; whether archived forms are included when `form_ids` is `ALL`.
- `request_timeout_seconds` - HTTP request timeout, in seconds.
- `fetch_limit` - page size when requesting leads; lower this if you observe timeouts.
- `check_point_limit` - number of rows accumulated per form before a checkpoint is written.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Authentication
This connector authenticates to the Meta Graph API using a system user access token, passed as `system_user_access_token`. Refer to `discover_pages` in `meta_helpers.py`.

To set up authentication:

1. In Meta Business Manager, create or open a system user under Business settings > Users > System users.
2. Assign the system user access to every page whose lead ads you want to sync.
3. Generate a system user access token with the `pages_show_list` and `leads_retrieval` permissions.
4. Provide the generated token as `system_user_access_token` in `configuration.json`.

## Pagination
Page and form listings are paginated using the Graph API's `paging.next` cursor. Refer to `def _paginate_graph_collection` in `meta_helpers.py`. Lead listings are paginated using the `after` cursor returned in `paging.cursors`; pagination for a form stops once no `after` cursor is returned. Refer to `def iterate_leads_for_form` in `meta_helpers.py`.

## Data handling
Each lead is transformed into a single row with page and form metadata, `ad_id`, `created_time`, and a `field_data` column containing the raw list of field objects from the source, serialized as JSON. Refer to `def _process_leads` in `connector.py`. Parsing individual form fields into columns is left to downstream transformation (for example, in a warehouse view or dbt model), since new fields may be added to forms without requiring a connector change.

## Error handling
Refer to `def request_with_retries` in `http_helpers.py`. Transient HTTP errors (429 and 5xx) are retried with exponential backoff. A request that exhausts its retries, receives a non-retryable non-2xx status, or returns an invalid JSON body raises a `RuntimeError`, which fails the sync instead of silently treating the failure as the end of pagination. Sensitive query-string values such as `access_token` are redacted before any URL is logged.

## Tables created
The connector creates a single `leads` table (primary key: `lead_id`) with the following columns: `lead_id`, `page_id`, `page_name`, `form_id`, `form_name`, `created_time`, `ad_id`, and `field_data` (JSON array of `{"name": <str>, "values": [<str>, ...]}` objects).

## Additional files
- **`http_helpers.py`** - HTTP request helper with retry/backoff and URL redaction for Graph API calls.
- **`meta_helpers.py`** - page/form/lead discovery and pagination helpers for the Graph API.
- **`validator.py`** - configuration validation and parsing.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
