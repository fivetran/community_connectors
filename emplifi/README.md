# Emplifi Connector Example

## Connector overview

Syncs social profiles, daily profile metrics, posts, and cross-platform aggregated post metrics across nine networks from the Emplifi social media management API.

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

## Features

- Syncs connected profiles into the `profiles` table.
- Syncs daily profile metrics into the `profile_metrics` table.
- Syncs social posts into the `posts` table using cursor pagination.
- Syncs cross-platform aggregated post metrics into the `aggregated_post_metrics` table.
- Uses incremental sync with `last_synced_date` state and a lookback window.
- Uses retry with exponential backoff for transient API failures.

## Configuration file

The connector requires the following configuration parameters:

```json
{
  "token": "<YOUR_EMPLIFI_API_TOKEN>",
  "secret": "<YOUR_EMPLIFI_API_SECRET>"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Authentication

This connector uses Basic authentication. The credentials are provided using the `token` and `secret` fields in `configuration.json`, and are encoded as `Authorization: Basic <base64(token:secret)>` by the `_auth_header()` function in `connector.py`.

To set up authentication:

1. Obtain an API token and secret from your Emplifi account.
2. Provide them as `token` and `secret` in `configuration.json`.

## Pagination

The connector paginates post data using Emplifi cursor pagination in `_sync_posts()`. It also paginates large sync windows into date chunks using `_date_chunks()` and processes aggregated metric requests in batches in `_sync_aggregated_post_metrics()`.

## Data handling

The connector defines table schemas in `schema()`, then syncs data in `update()` by running four phases:

- `_fetch_all_profiles()` and `_sync_profiles()` for profiles
- `_sync_profile_metrics()` for profile metrics
- `_sync_posts()` for posts
- `_sync_aggregated_post_metrics()` for aggregated metrics

The connector checkpoints state with `op.checkpoint(state)` between phases and stores the incremental cursor in `state["last_synced_date"]`.

## Error handling

Network requests are handled in `_request_with_backoff()` with retry and exponential backoff for transient failures. HTTP failures are surfaced with `raise_for_status()` in `_get()` and `_post()`. Application-level Emplifi response codes are handled in `_parse_body()` with explicit handling for recoverable and rate-limit scenarios.

## Tables created

The connector creates the following tables (refer to `schema()` and the sync helper functions):

### `profiles`

- Primary key: `id`, `network`
- Columns: `id`, `network`, `name`, `profile_name`, `picture`, `timezone`, `insights_enabled`, `community_enabled`

### `profile_metrics`

- Primary key: `profile_id`, `network`, `date`, `metric_name`
- Columns: `profile_id`, `network`, `date`, `metric_name`, `value`

### `posts`

- Primary key: `id`, `network`
- Columns: `id`, `network`, `profile_id`, plus post fields returned by Emplifi for each supported network (for example `created_time`, `content`, `url`, `media_type`, `interactions`, `reactions`, `comments`, `shares`, `profileId`, `origin`, `video_views`, `duration`, and other network-specific fields).

### `aggregated_post_metrics`

- Primary key: `profile_id`, `network`, `date`, `metric_name`
- Columns: `profile_id`, `network`, `date`, `metric_name`, `value`

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
