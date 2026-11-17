---
name: Toast
description: Sync POS data from Toast
category: SaaS & APIs
---

# Toast Connector Example

## Connector overview

This is a custom [Fivetran connector](https://fivetran.com/docs/connectors/connector-sdk) implementation to extract and sync data from the [Toast POS API](https://doc.toasttab.com/) into a destination warehouse. Toast is a restaurant management platform providing point-of-sale, labor, menu, and kitchen operations data.

The connector's code is split across four files (`connector.py`, `auth.py`, `transform.py`, `endpoints.py`) rather than a single `connector.py`, to keep sync orchestration, authentication, generic data-shaping helpers, and per-endpoint processing logic separately readable.

For full implementation details, see the [Toast connector example code](https://github.com/fivetran/community_connectors/tree/main/toast/).

## Requirements

- Toast API credentials: `clientId`, `clientSecret`, `userAccessType`
- Domain to connect to (e.g., `ws-api.toasttab.com`)
- A Fivetran system key with `CONNECTOR: MANAGE` for this connection, used to refresh the connector's own Toast session token (see [Authentication](#authentication))
- `initialSyncStart` ISO timestamp to define the start of sync window
- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```
fivetran init --template toast
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- Syncs data from multiple Toast endpoints, including orders, employees, shifts, menus, kitchen prep stations, and item fulfillments
- Each endpoint tracks and advances its own independent cursor, so endpoints catch up independently instead of waiting on the slowest one
- Automatically handles nested JSON structures and normalizes them into relational tables, including Toast modifiers that are themselves modified by other modifiers (recursed to arbitrary depth)
- Includes incremental sync via time-based windowing and state checkpointing
- Graceful handling of rate limits, authentication, and transient API errors (5xx responses retry with exponential backoff)
- Supports voids, deletions, and nested child entities
- Refreshes the Toast session token on demand and pushes it to this connection's own Fivetran configuration via the REST API, deferred until after each sync's data is pulled and checkpointed so a push failure never costs a sync its data

## Configuration file

Example `configuration.json`:

```json
{
  "clientId": "<YOUR_TOAST_CLIENT_ID>",
  "clientSecret": "<YOUR_TOAST_CLIENT_SECRET>",
  "userAccessType": "TOAST_MACHINE_CLIENT",
  "domain": "ws-api.toasttab.com",
  "initialSyncStart": "<YOUR_INITIAL_SYNC_START_TIMESTAMP>",
  "fivetran_system_key": "<YOUR_FIVETRAN_SYSTEM_KEY>",
  "itemFulfillmentsInitialDaysBack": "<YOUR_ITEM_FULFILLMENT_BACKFILL_DAYS>"
}
```

Required parameters:
- `clientId` / `clientSecret`: credentials to authenticate with Toast.
- `userAccessType` / `domain`: static values specified by Toast's API spec, for example `TOAST_MACHINE_CLIENT` and `ws-api.toasttab.com`.
- `initialSyncStart`: timestamp to start historical sync from for every endpoint by default, in `YYYY-MM-DDTHH:MM:SS.sssZ` format.
- `fivetran_system_key`: a Fivetran system key with `CONNECTOR: MANAGE` for this connection. See [Authentication](#authentication) for how to create one.

Optional parameters:
- `itemFulfillmentsInitialDaysBack`: on item fulfillment data's very first sync, backfills this many days back from yesterday instead of the default of going all the way back to `initialSyncStart`. Every sync after the first ignores this value and resumes from its own state cursor.

> Note: When submitting connector code as a community connector in the open-source [Community Connector repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector uses only the `requests` package and the Python standard library, so no `requirements.txt` file is needed.

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector authenticates with Toast using a machine client ID and secret, and separately authenticates with the Fivetran REST API using a system key, to keep its Toast session token current.

### Steps to obtain Toast credentials

1. Obtain a Toast API client ID, client secret, and access type from Toast for a machine client with access to the restaurants you want to sync.
2. Add the `clientId`, `clientSecret`, `userAccessType`, and `domain` values to your `configuration.json` file.

### Steps to obtain a Fivetran system key

1. In the Fivetran dashboard, go to **Account Settings > System Keys**.
2. Create a new system key and grant it `CONNECTOR: MANAGE` for this connection, or its group or account scope.
3. Copy the Base64-encoded system key value shown when the key is created, and add it to your `configuration.json` file as `fivetran_system_key`.

Refer to `get_session_token`, `refresh_session_token`, and `push_token_to_fivetran` in `auth.py`. Toast session tokens are reused directly from `configuration["token"]` while still fresh. Once a cached token goes stale, `refresh_session_token` logs in to Toast again and stores the new token in memory for immediate use. `push_token_to_fivetran` is called once at the end of each sync to push that token into this connection's own Fivetran configuration, so a failure to push there never blocks that sync's Toast data from being pulled and checkpointed first. On a 401 response mid-sync, `get_api_response` in `auth.py` calls `refresh_session_token` again and retries the request with the new token.

## Pagination

The connector handles two different pagination styles depending on the endpoint, both implemented in `get_api_response` in `auth.py` and its callers in `endpoints.py`.

Config, labor, and prep station endpoints use Toast's `Toast-Next-Page-Token` response header: `get_api_response` reads that header and the caller passes it back as a `pageToken` parameter on the next request, continuing until no further token is returned.

The orders endpoint uses page-number-based pagination instead: `process_orders` in `endpoints.py` requests a fixed page size and increments the page number until a page comes back with fewer records than the requested size.

## Data handling

The connector performs the following actions for each key aspect:
- Authentication: generates a fresh Toast token on demand and defers pushing it to Fivetran's own configuration until after this sync's data is pulled, so authentication issues never cost a sync its data
- Sync loop: each endpoint runs in its own independently-advancing time window, chunked to 30 days at a time, paginating through endpoints
- Data normalization: flattens nested objects and lists using `flatten_dict`, `extract_fields`, and `stringify_lists` in `transform.py`, and recursively extracts nested lists into child tables using `process_child`
- Upserts and deletes: emits operations using `op.upsert()` and `op.delete()` directly, called inline rather than yielded
- Checkpointing: updates state after each pass to resume seamlessly

## Error handling

- Retries on 401 Unauthorized (max 3), refreshing the Toast session token before each retry
- Skips 403 Forbidden
- Backs off on 429 Too Many Requests based on Toast's `Retry-After` or `X-Toast-RateLimit-Reset` headers
- Retries once on 409 Conflict without the current pagination token
- Treats 400 Bad Request and 204 No Content as an empty page rather than an error
- Retries 5xx server errors up to four times with exponential backoff (10, 20, 40, then 80 seconds) before raising

Uses Fivetran SDK logging levels (`info`, `debug`, `warning`, `error`) for detailed sync visibility.

## Tables created

The entity-relationship diagram (ERD) below shows how tables are linked in the Toast schema. It predates the kitchen tables (`prep_station`, `prep_station_connected_prep_station`, `item_fulfillment`) added in this version.

<img src="https://raw.githubusercontent.com/fivetran/connector_sdk/main/connectors/toast/Toast_ERD.png" alt="Fivetran Toast Connector ERD" width="100%">

### Core tables

- `restaurant`
- `job`, `employee`, `shift`, `break`, `time_entry`
- `orders`, `orders_check`, `payment`

### Configuration tables

- `menu`, `menu_item`, `menu_group`, `discounts`, `tables`, etc.

### Nested children

- `orders_check_payment`, `orders_check_selection`, `orders_check_selection_modifier`, etc. `orders_check_selection_modifier` is self-referential: a modifier that is itself modified by another modifier is stored as another row in the same table, tied back to its parent modifier.

### Cash management

- `cash_entry`, `cash_deposit`

### Kitchen

- `prep_station`, `prep_station_connected_prep_station`, `item_fulfillment`

## Additional files

- `auth.py` - Toast session token lifecycle (`get_session_token`, `refresh_session_token`, `push_token_to_fivetran`) and the shared HTTP request handling used by every endpoint (`get_api_response`).
- `transform.py` - generic data-shaping helpers (field flattening, child table extraction, guid-to-id renaming) and per-endpoint cursor bookkeeping.
- `endpoints.py` - one processing function per Toast API endpoint (config, labor, cash management, orders, prep stations, item fulfillments).

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

## Resources

- [Fivetran Connector SDK Docs](https://fivetran.com/docs/connectors/connector-sdk)
- [Toast API Reference](https://doc.toasttab.com/)
