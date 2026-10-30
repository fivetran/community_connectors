---
name: Emplifi
description: "Connect and sync profiles, metrics, posts, and aggregated post metrics from the Emplifi social media management API"
category: "SaaS & APIs"
---

# Emplifi Connector Example

## Connector overview
This connector syncs social media profile, metrics, posts, and aggregated post-metrics data from the Emplifi social media management API, across nine networks: Facebook, Instagram, Twitter, YouTube, LinkedIn, Pinterest, TikTok, Snapchat, and WhatsApp.

The sync has four steps: it first fetches every connected profile on every network, then daily profile-level metrics per profile, then posts per profile with cursor-based pagination, and finally cross-platform aggregated post metrics batched across profiles. On the first run it backfills a configurable lookback window; later runs sync incrementally from the last checkpoint with a 2-day overlap to absorb late-arriving data.

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
fivetran init --template emplifi
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features
- Syncs profiles, daily profile metrics, posts, and cross-platform aggregated post metrics across nine social networks.
- Chunks metrics requests into 30-day windows to stay within Emplifi's 12-month date-range limit per call, and batches aggregated-metrics requests up to 100 profiles per call.
- Retries transient network errors and retryable HTTP status codes (429, 5xx) with backoff, honoring a `Retry-After` header on 429 responses (the sync fails for a later retry if the requested wait exceeds 5 minutes).
- Handles Emplifi's application-level error codes (validation errors, before-history-limit, insights-not-enabled) by skipping the affected request instead of failing the whole sync.
- Checkpoints after each profile (profile metrics), after each page of posts, and after each metric batch (aggregated metrics), recording resumable progress in state. `last_synced_date` only advances once the whole window has succeeded, and request failures abort the sync rather than being skipped.

## Configuration file
```json
{
  "token": "<YOUR_EMPLIFI_API_TOKEN>",
  "secret": "<YOUR_EMPLIFI_API_SECRET>",
  "base_url": "<OPTIONAL_EMPLIFI_API_BASE_URL>",
  "lookback_days": "<OPTIONAL_LOOKBACK_DAYS_DEFAULT_365>"
}
```

- `token` - your Emplifi API token.
- `secret` - your Emplifi API secret, paired with `token` for Basic authentication.
- `base_url` - optional. Overrides the default Emplifi API base URL (`https://api.emplifi.io`).
- `lookback_days` - optional. Number of days to backfill on the first sync. Defaults to 365. Values left as `<...>` placeholders are treated as unset.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file
This connector only uses the `requests` library, which is pre-installed in the Connector SDK runtime environment, so no `requirements.txt` is needed.

## Authentication
This connector authenticates to Emplifi using HTTP Basic authentication. Refer to `def _auth_header` in `connector.py`.

To set up authentication:

1. Obtain an API token and secret from your Emplifi account.
2. Provide them as `token` and `secret` in `configuration.json`.

Every request sends an `Authorization: Basic <base64(token:secret)>` header built from these values.

## Pagination
Refer to `def _sync_posts` in `connector.py`. Posts are paginated with Emplifi's cursor-based `after` field: the first request per profile omits `after` entirely, and each subsequent request sends only the `after` cursor returned by the previous response, until no cursor is returned.

Profile metrics and aggregated post metrics are not paginated per se, but their date ranges are chunked into 30-day windows (`def _date_chunks`) to stay within Emplifi's 12-month date-range limit per call.

## Data handling
Refer to `def update` and `def schema` in `connector.py`. The sync runs in four steps: profiles, then profile metrics, then posts, then aggregated post metrics, checkpointing after each profile or batch within a step.

- Profile metrics are requested one profile at a time with a single `date.day` dimension, and stored narrow: one row per `(profile_id, network, date, metric_name)`.
- Posts are requested one profile at a time with the per-network field list in `POST_FIELDS_BY_NETWORK`, since each network's API accepts a different confirmed-valid field set.
- Aggregated post metrics batch all profiles across all networks into groups of up to 100, issuing one call per metric per batch per date chunk, since the aggregated-metrics endpoint accepts only a single metric per request.

Incremental syncs resume from `state["last_synced_date"]` minus a 2-day overlap, to re-absorb any data that arrived late in Emplifi after the previous sync's window closed. The first sync instead backfills `lookback_days` (default 365) from today.

A full sync for 169 profiles over a 1-day window issues roughly 365 API calls (9 for profiles, ~169 for profile metrics, ~169 for posts plus pagination, ~18 for aggregated metrics), well within Emplifi's 5,000 requests/hour limit per credential.

Resuming an interrupted sync: the sync window and finished work are saved in `state["progress"]` (`window`, `done`, and per-profile post `cursors`). If a run is interrupted, the next run resumes that exact window, even on a later calendar day, skips finished profiles and batches, and continues posts from the saved `after` cursor. `last_synced_date` advances to the window end only when the whole window completes; the following run then computes a new window. Aggregated metrics batches are identified by a hash of their member profiles rather than their position, so a changed profile list never reuses another batch's completion.

## Error handling
Refer to `def _request_with_backoff` and `def _parse_body` in `connector.py`. Connection errors, timeouts, and HTTP 429/5xx responses are retried with backoff, honoring a `Retry-After` header in delay-seconds form on 429 responses. Other non-2xx responses raise an exception and fail the sync, except HTTP 400/403/404 on a network's profiles endpoint (network not available to the account) and 404/405 on the aggregated-metrics endpoint.

Emplifi returns some errors as a 200 response with an `errors` array rather than a non-2xx status. `_parse_body` handles these application-level codes:
- Codes 10 and 11 (rate limit) raise an exception, failing the sync.
- Code 6 (requested date is before the account's history limit) logs a warning and returns an empty result for that request.
- Code 3 (validation error) raises an exception, failing the sync. It is not skipped, because treating it as an empty result would mark the work complete and advance `last_synced_date`, so the history would never be backfilled after the configuration is fixed.
- Code 7 (insights not enabled for a profile) logs a warning and returns an empty result for that request. To avoid losing accessible data, `insights_` metrics and post fields are not requested for profiles with `insights_enabled` set to false.

If zero profiles are returned in step 1 (typically indicating the account is rate-limited), the sync logs a warning and returns without advancing `last_synced_date`, so the same window is retried on the next run.

## Tables created
The connector creates the following tables (refer to `def schema` in `connector.py`). Only primary keys are declared; Fivetran infers column types.

| Table | Primary key | Columns |
| --- | --- | --- |
| `profiles` | `id`, `network` | `id`, `network`, `name`, `profile_name`, `picture`, `timezone`, `insights_enabled`, `community_enabled` |
| `profile_metrics` | `profile_id`, `network`, `date`, `metric_name` | `profile_id`, `network`, `date`, `metric_name`, `value` |
| `posts` | `id`, `network` | Dynamic columns, one per post field returned for the network (nested objects are stored as JSON strings), plus `network` and `profile_id` |
| `aggregated_post_metrics` | `profile_id`, `network`, `date`, `metric_name` | `profile_id`, `network`, `date`, `metric_name`, `value` |

## Additional files
The connector uses the following additional file:
- **constants.py** - Stores the per-network profile metrics, aggregated metrics, posts endpoints, and post field lists used by `connector.py`.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
