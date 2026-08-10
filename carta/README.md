# Carta Issuer API Connector Example

## Connector overview

This connector syncs issuer-level equity data from the [Carta](https://carta.com) Issuer API into your destination. Carta is a cap table and equity management platform, and its Issuer API exposes the securities a company has issued: option grants, restricted stock units, restricted stock awards, certificates, the vesting events behind each security, stakeholders, share classes, 409A fair market values, vesting schedule templates, convertible notes and the stakeholder capitalization table.

The connector is read only. It makes no write calls to Carta.

Typical uses are total compensation reporting (equity joined to payroll or HRIS data through the `employee_id` and `email` fields on `stakeholders`), equity dilution and burn analysis, and vesting forecasts built from the flattened `vesting_events` table.

One connection can sync several issuers. Each destination row carries its `issuer_id`, and every incremental cursor is tracked per issuer, so issuers never interfere with each other.

## Requirements

- [Supported Python versions](https://github.com/fivetran/connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```bash
fivetran init <project-path> --template connectors/carta
```

> Note : Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- Syncs 16 tables covering securities, stakeholders, share classes, valuations, vesting and cap table holdings.
- Supports multiple issuers in a single connection through a comma separated `issuer_ids` value.
- Incremental sync for the four securities resources that accept Carta's `lastModifiedDatetimeAfter` cursor, tracked per issuer and per resource.
- Flattens nested vesting events, option grant exercises, share class valuations and per share class holdings into their own tables.
- Preserves Carta's high-precision decimal quantities and prices as strings, so no precision is lost on equity data.
- Refreshes the access token automatically when it expires mid-sync.
- Skips any resource the OAuth app is not scoped for and continues the sync, so an app with a narrow scope grant still replicates what it can reach.
- Retries transient failures with exponential backoff and honors Carta's `Retry-After` header on rate limits.
- Checkpoints inside large resources, so an interrupted sync keeps the rows already delivered.

## Configuration file

```
{
  "client_id": "<YOUR_CARTA_OAUTH_CLIENT_ID>",
  "client_secret": "<YOUR_CARTA_OAUTH_CLIENT_SECRET>",
  "issuer_ids": "<COMMA_SEPARATED_CARTA_ISSUER_IDS>",
  "token_url": "<YOUR_CARTA_TOKEN_URL_DEFAULT_https://login.app.carta.com/o/access_token/>",
  "api_base_url": "<YOUR_CARTA_API_BASE_URL_DEFAULT_https://api.carta.com>",
  "api_version": "<YOUR_CARTA_API_VERSION_DEFAULT_v1alpha1>",
  "scopes": "<SPACE_SEPARATED_OAUTH_SCOPES_OPTIONAL>"
}
```

Required keys:

- `client_id` and `client_secret` are the credentials of your Carta OAuth application.
- `issuer_ids` is one Carta issuer id, or several separated by commas, for example `144361,144362`.

Optional keys, each with a default:

- `token_url` defaults to `https://login.app.carta.com/o/access_token/`. Point it at the Carta playground token URL to test against playground data.
- `api_base_url` defaults to `https://api.carta.com`. The playground base URL is `https://api.playground.carta.team`.
- `api_version` defaults to `v1alpha1`, the version Carta currently publishes the Issuer API under.
- `scopes` is a space separated scope list that overrides the default request. Set it when your OAuth app is registered for a different scope set. See the Authentication section for why this matters.

Note: Carta issues separate credentials per environment. Playground credentials return HTTP 401 against the production token URL, and production credentials return HTTP 401 against the playground token URL.

> Note: When submitting connector code as a [Community Connector](https://github.com/fivetran/community_connectors) or enhancing an [example](https://github.com/fivetran/connector_sdk/tree/main/examples) in the open-source [Connector SDK repository](https://github.com/fivetran/connector_sdk), ensure the `configuration.json` file has placeholder values.
When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector needs no `requirements.txt` file. It uses only the Python standard library and `requests`, and `requests` is pre-installed in the Fivetran environment.

> Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

Carta uses OAuth2 with the `client_credentials` grant. Register an application in the [Carta Developer Portal](https://developers.app.carta.com) to obtain a client id and secret, and have Carta promote the application before using it against production data.

Three requirements are easy to miss:

1. The client id and secret must be sent as HTTP Basic credentials in the `Authorization` header. Sending them in the request body returns HTTP 401.
2. The token request must name the scopes explicitly. A token minted with no `scope` parameter is issued successfully but is rejected with HTTP 403 by every data endpoint, so this connector treats an empty granted scope as a failure and raises immediately.
3. Carta rejects some default user agents, so the connector sends an explicit `User-Agent` header.

Carta grants scopes per application, and the grant is all or nothing: if the application is not registered for one of the requested scopes, the token comes back with no scope at all. Set the `scopes` configuration value to match what your application is registered for. Resources whose scope is missing return HTTP 403 and are skipped with a warning, so a narrower grant reduces coverage instead of failing the sync.

Access tokens are short lived, one hour in production. The connector refreshes the token and retries when a request returns HTTP 401, so a first sync longer than the token lifetime completes normally.

## Pagination

Every list endpoint is cursor paginated with a `pageToken` request parameter and a `nextPageToken` response field. The connector requests `pageSize=50`, which is the server-side maximum. Larger values are accepted but ignored, so do not expect fewer round trips from raising it.

The `paginate` helper is a generator: it yields each record as its page arrives rather than accumulating pages in memory, so memory stays flat regardless of resource size. It logs progress every 10 pages.

The stakeholder capitalization table endpoint returns a nested object rather than a flat list, so it has its own pagination loop with the same page token contract.

## Data handling

Records are upserted one at a time as they stream out of the paginator. Nested structures are flattened into child tables rather than stored as JSON blobs: `vesting_events` from all three vesting security types, `option_grant_exercises` from option grants, `fair_market_value_share_class_valuations` from fair market values, and `stakeholder_share_class_holdings` from the capitalization table. Each child row carries its parent's identifiers so it can be joined back.

Columns are declared explicitly in the `schema` function, and equity quantities and prices are declared as `STRING`. Carta returns high-precision decimals such as `99.00000000000000000000`, and a floating point type would silently lose precision on equity data. Cast these columns downstream if you need arithmetic.

Carta wraps many scalar fields in a single key envelope, for example `{"value": "100"}`, and returns money as an object with an amount and a currency code. The `unwrap_value` and `split_money` helpers normalize both shapes so destination columns hold plain scalars.

The `stakeholders` field Carta calls `group` lands in a column named `stakeholder_group`, because `group` is a reserved word in most warehouses.

Four resources accept the `lastModifiedDatetimeAfter` cursor and sync incrementally: option grants, restricted stock units, restricted stock awards and certificates. These are also the largest, because one security expands into many vesting events. The cursor is the highest `lastModifiedDatetime` seen for that issuer and resource and is written only when the resource completes, because Carta does not return records in modified order and a cursor advanced mid-resource could skip older records that have not been fetched yet. The filter is inclusive of the boundary record, which is harmless because upserts are idempotent.

The remaining resources have no cursor and no last modified field, so they are replicated in full on every sync. All of them are small except stakeholders and the capitalization table.

Inside a large resource the connector checkpoints every 10,000 upserts. This preserves progress across an interruption, and it also keeps each commit small, because a single very large commit at the end of a resource can fail and roll back everything already sent.

Hard deletes are not visible: the Carta cursor reports modifications only, and Carta publishes no deletions feed. Most equity "deletions" arrive as status changes such as canceled or forfeited, which are ordinary updates. If you need to detect hard deletes, schedule a periodic re-sync.

## Error handling

`validate_configuration` runs first and raises `ValueError` naming the offending key when a required value is missing, when `issuer_ids` lists no issuer, or when a supplied URL is not `https`.

Request failures are handled by kind rather than uniformly:

- HTTP 429 waits for the interval in the `Retry-After` header before retrying.
- HTTP 5xx, connection errors and timeouts retry up to three attempts with exponential backoff of 15 seconds doubling to a 60 second cap. The final failure is logged and raised.
- HTTP 401 triggers one token refresh and one retry. A second consecutive 401 is raised, so a genuinely bad credential fails fast instead of looping.
- HTTP 403 is raised as `InsufficientScopeError`, caught per resource, and logged as a warning naming the skipped resource. A missing scope is a permanent fact about the OAuth application, not a transient error, so retrying it would only waste the sync.
- Other 4xx responses are logged and raised, since they will not resolve on retry.

Any exception other than a missing scope propagates and fails the sync, so real problems stay visible rather than being swallowed.

## Tables created

Securities and their children:

- `option_grants` (primary key `issuer_id`, `id`), incremental. Stock option grants.
- `restricted_stock_units` (primary key `issuer_id`, `id`), incremental.
- `restricted_stock_awards` (primary key `issuer_id`, `id`), incremental.
- `certificates` (primary key `issuer_id`, `id`), incremental. Note that issued common shares can be double counted here across reclassifications, where one certificate is superseded by another with an identical quantity. Use `stakeholder_holdings.outstanding_shares` for an authoritative issued share count.
- `vesting_events` (primary key `issuer_id`, `security_type`, `security_id`, `id`). Vesting tranches flattened from option grants, restricted stock units and restricted stock awards. `security_type` names the parent kind.
- `option_grant_exercises` (primary key `issuer_id`, `option_grant_id`, `exercise_index`). Exercises flattened from option grants, keyed by position because Carta does not always supply an exercise id.

Stakeholders and holdings:

- `stakeholders` (primary key `issuer_id`, `id`). `email` and `employee_id` are the join keys to an HRIS or payroll source.
- `stakeholder_holdings` (primary key `issuer_id`, `stakeholder_id`). Capitalization table summary per stakeholder, including the authoritative `outstanding_shares`.
- `stakeholder_share_class_holdings` (primary key `issuer_id`, `stakeholder_id`, `share_class_id`). Per share class breakdown of the above.

Equity structure and valuations:

- `share_classes` (primary key `issuer_id`, `id`).
- `fair_market_values` (primary key `issuer_id`, `id`). 409A valuations.
- `fair_market_value_share_class_valuations` (primary key `issuer_id`, `fair_market_value_id`, `share_class_id`). The authoritative price per share class for each valuation.
- `vesting_schedule_templates` (primary key `issuer_id`, `id`).
- `convertible_notes` (primary key `issuer_id`, `id`). Convertible notes and SAFEs.

Entity metadata:

- `issuers` (primary key `id`). Populated only when the OAuth application can read the issuer detail endpoint; skipped with a warning otherwise.
- `corporations` (primary key `id`). Requires the `read_corporation_info` scope. Synced once per sync rather than per issuer.

## Additional considerations

Warrants and interests are deliberately not included. The warrants endpoint and the interests endpoint were not reachable during development (interests requires a UUID issuer identifier that numeric issuer ids do not satisfy), so their column shapes could not be confirmed. Add them only against an issuer where the endpoints return data.

The compensation benchmarks and capitalization table summary endpoints are also excluded. The scopes exist, but every documented path returned HTTP 404 during development.

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
