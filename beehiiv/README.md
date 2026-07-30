# Beehiiv Connector Example

## Connector overview
This connector integrates with the [beehiiv API](https://developers.beehiiv.com/) to synchronize newsletter data into your destination. It fetches publications, subscriptions, posts, email blasts, automations, engagement metrics, and other newsletter management data from a single beehiiv publication.

The connector supports incremental sync for high-volume tables (subscriptions, posts, email blasts, engagements) using timestamp-based cursors, and full sync for low-volume reference tables. It handles both page-number and cursor-based pagination patterns used across the beehiiv API.

## Accreditation

This example was contributed by [AngelList](https://www.angellist.com/).

## Requirements
- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```bash
fivetran init --template beehiiv
```
`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features
- Syncs 17 tables covering the full beehiiv API surface (publications, subscriptions, posts, email blasts, automations, journeys, authors, segments, custom fields, newsletter lists, tiers, referral program, polls, condition sets, post templates, engagements, advertisement opportunities)
- Incremental sync for high-volume tables using `created` timestamp cursors
- Date-range based incremental sync for daily engagement metrics
- Both page-number and cursor-based pagination support
- Nested object handling via automation journey sub-resource iteration
- VARIANT columns for nested JSON objects (stats, custom fields, tags, etc.)
- Retry logic with exponential backoff for rate-limited and server error responses
- Periodic checkpointing for high-volume tables to enable resumption on interruption

## Configuration file

```json
{
  "api_key": "<YOUR_BEEHIIV_API_KEY>",
  "publication_id": "<YOUR_BEEHIIV_PUBLICATION_ID>"
}
```

Configuration parameters:
- `api_key` (required) - your beehiiv API key (Bearer token). Required scopes: `publications:read`, `posts:read`, `automations:read`, `condition_sets:read`.
- `publication_id` (required) - the prefixed publication ID (e.g., `pub_xxx`). All endpoints except publications are scoped to this publication. Create a separate Fivetran connection for each publication.

> Note: When submitting connector code as a community connector in the open-source [Community Connector repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file
This connector uses only the `requests` library, which is pre-installed in the Fivetran environment, so no `requirements.txt` file is needed.

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
This connector authenticates with the beehiiv API using a bearer token. Every request includes an `Authorization` header whose value is the word `Bearer`, a space, and your beehiiv API key (for example, `Authorization: Bearer YOUR_API_KEY`).

To obtain your beehiiv API key:
1. Log in to your [beehiiv Dashboard](https://app.beehiiv.com/).
2. Navigate to **Settings**, then **Integrations**, then **API**.
3. Generate a new API key with the required scopes: `publications:read`, `posts:read`, `automations:read`, `condition_sets:read`.
4. Copy the key and add it to your `configuration.json` file.

## Pagination

The connector implements two pagination patterns used across the beehiiv API:

Page-number pagination:
- Used by most endpoints (posts, email blasts, automations, authors, segments, etc.)
- Fetches pages sequentially with `page` and `limit` parameters (100 records per page)
- Continues until `page >= total_pages` from the response
- Refer to `paginate_page_number()` in `connector.py`

Cursor-based pagination:
- Used by subscriptions, polls, and condition sets endpoints
- Passes `cursor` from the `next_cursor` field in each response
- Continues until `has_more` is false
- Refer to `paginate_cursor()` in `connector.py`

## Data handling

Incremental sync:
- Subscriptions, posts, and email blasts track a `created` timestamp cursor in the connector state, fetching only records created after the last sync
- Engagements use a date-range approach, fetching from the last synced date to today in chunks of up to 31 days (the API maximum)
- Refer to `sync_subscriptions()`, `sync_posts()`, `sync_email_blasts()`, and `sync_engagements()` in `connector.py`

Full sync:
- Low-volume reference tables (authors, segments, custom fields, tiers, etc.) are fully replaced each sync
- Refer to `sync_simple_page_table()` and `sync_simple_cursor_table()` in `connector.py`

Nested data:
- Nested JSON objects and arrays (stats, custom fields, tags, poll choices, milestones, prices) are passed through as VARIANT columns for downstream extraction
- HTML content fields are explicitly excluded from post records to avoid syncing large blobs
- Automation journeys are a nested sub-resource requiring iteration over each parent automation
- Refer to `sync_automations_and_journeys()` and `sync_posts()` in `connector.py`

## Error handling
- All API requests include retry logic with exponential backoff (1s initial, 60s max, 5 retries) for rate-limited (429) and server error (5xx) responses
- Request timeouts are set to 30 seconds
- Optional endpoints (referral program, advertisement opportunities) log warnings and continue if unavailable
- Configuration is validated at the start of each sync to fail fast on missing parameters
- Refer to `make_api_request()` and `validate_configuration()` in `connector.py`

## Tables created

| Table | Primary key | Sync strategy | Description |
|---|---|---|---|
| `publications` | `id` | Full | Publication metadata and stats |
| `subscriptions` | `id` | Incremental (created) | Subscriber records with stats, custom fields, tags |
| `posts` | `id` | Incremental (created) | Newsletter posts with stats (content excluded) |
| `email_blasts` | `id` | Incremental (created) | Email blast campaigns with stats |
| `automations` | `id` | Full | Automation definitions with stats |
| `automation_journeys` | `id` | Full | Individual automation journey records |
| `authors` | `id` | Full | Publication authors |
| `segments` | `id` | Full | Subscriber segments with stats |
| `custom_fields` | `id` | Full | Custom field definitions |
| `newsletter_lists` | `id` | Full | Newsletter list definitions |
| `tiers` | `id` | Full | Subscription tiers with prices |
| `referral_program` | `id` | Full | Referral program config and milestones |
| `polls` | `id` | Full | Polls with choices and stats |
| `condition_sets` | `id` | Full | Dynamic content condition sets |
| `post_templates` | `id` | Full | Post template definitions |
| `engagements` | `date` | Incremental (date) | Daily engagement metrics (aggregated across email types) |
| `advertisement_opportunities` | `id` | Full | Accepted ad opportunities |

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
