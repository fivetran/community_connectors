# People.ai Connector Example

## Connector overview

This connector syncs activity data from the People.ai API into a destination using the Fivetran Connector SDK. It retrieves base activity records from `/v0/public/activities` and participant records from `/v0/public/activities/participants`.

The connector uses OAuth2 client credentials authentication, refreshes the access token after an unauthorized response, retries transient API failures with exponential backoff, and writes records page by page to avoid loading the full API result set into memory.

## Requirements

- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```shell
fivetran init --template people_ai
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- Syncs base activity records from `/v0/public/activities`.
- Syncs participant records from `/v0/public/activities/participants`.
- Authenticates with OAuth2 client credentials.
- Refreshes the access token once after a `401 Unauthorized` response.
- Retries transient server and network failures with exponential backoff.
- Uses offset-based pagination and writes each page with `op.upsert(...)`.
- Checkpoints progress after each successfully written page.

## Configuration file

The connector requires the following configuration parameters:

```json
{
  "api_key": "<YOUR_PEOPLE_AI_API_KEY>",
  "api_secret": "<YOUR_PEOPLE_AI_API_SECRET>"
}
```

- `api_key` - Your People.ai OAuth client ID or API key.
- `api_secret` - Your People.ai OAuth client secret.

> Note: When submitting connector code as a [Community Connector](https://github.com/fivetran/community_connectors/tree/main) in the open-source [Connector SDK repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Authentication

The connector uses OAuth2 client credentials authentication in `get_access_token()`.

- Token URL: `https://api.people.ai/auth/v1/tokens`
- Grant type: `client_credentials`
- Request content type: `application/x-www-form-urlencoded`
- Access token usage: `Authorization: Bearer <access_token>`

The `update()` function performs initial authentication before syncing data. The connector passes a `reauthenticate()` closure into the page-fetching functions so `get_page()` can request a new token and retry the request after a `401 Unauthorized` response.

## Pagination

People.ai activity endpoints use offset-based pagination. The connector handles pagination in `sync_base_activities()` and `sync_activity_type()`.

- The base activities endpoint uses the `limit` and `offset` query parameters with a page size of 1000.
- The participant activity endpoint uses the `limit` and `offset` query parameters with a page size of 100000.
- Pagination stops when the API returns an empty page or a page with fewer records than the requested limit.
- Each page is upserted before the connector advances and checkpoints the offset.

## Data handling

The connector processes records in pages and writes them to destination tables using `op.upsert(...)`.

- `schema()` defines the `activity` table with primary key `uid`.
- `schema()` defines the `participants` table with primary key `uid`, `email`.
- `sync_base_activities()` copies each base activity record and renames a `subject` field to `api_subject` when present.
- `sync_activity_type()` writes participant records to the `participants` table.
- The connector stores the most recently processed page offsets in `state["activity_offset"]` and `state["participants_offset"]` after each successful page write.

This example resumes page-based syncs from the checkpointed offsets stored in state on subsequent runs, so it can continue from the last successfully written page.

## Error handling

The connector implements retry and authentication handling in `get_page()`.

- `401 Unauthorized` responses trigger one access token refresh and request retry.
- `5xx` server responses are retried up to five times with exponential backoff.
- Network and timeout errors are retried up to five times with exponential backoff.
- Other HTTP errors are treated as unrecoverable and raised.
- Missing `api_key` or `api_secret` values cause early validation failure in `validate_configuration()`.
- The connector uses `fivetran_connector_sdk.Logging` for status, retry, and error messages.

## Tables created

The connector creates the following tables:

Columns are inferred from the API response. The primary keys configured in `schema()` are:

| Table name | Primary key | Description |
|------------|-------------|-------------|
| `activity` | `uid` | Base People.ai activity records from `/v0/public/activities`. |
| `participants` | `uid`, `email` | Participant activity records from `/v0/public/activities/participants`. |

## Additional files

- `connector.py` - Contains the connector implementation, including schema definition, authentication, pagination, retry handling, and data writes.
- `configuration.json` - Contains placeholder People.ai credential fields used by the connector.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
