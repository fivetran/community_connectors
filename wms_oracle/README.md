# Oracle WMS Connector Example

## Connector overview

This connector syncs warehouse management data from the Oracle WMS Cloud REST API to a Fivetran destination. It supports 26 warehouse entities including orders, inventory, containers, and purchasing documents, plus two monitoring tables that record daily volume probes and hourly drift counts.

The connector uses a two-phase incremental strategy per entity: Phase 1 advances a `mod_ts` cursor forward in time, and Phase 2 catches up records that were created after the cursor with a backdated `mod_ts`. Historical backfill runs in descending order across rolling 30-day windows so recent data reaches the destination first. A pre-cursor hourly drift check runs before each sync to detect and re-pull any records modified in already-advanced windows. Entities with active backfills run in parallel via `ThreadPoolExecutor`; incremental-only entities run sequentially to avoid checkpoint contention.


## Requirements

- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- An Oracle WMS Cloud instance with REST API access and a service account with read permissions on the entities you want to sync


## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```
fivetran init --template wms_oracle
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.


## Features

- Two-phase incremental sync per entity: `mod_ts` cursor-advancement (Phase 1) plus `create_ts` catch-up for backdated records (Phase 2)
- Descending historical backfill across rolling 30-day windows, newest data first
- Pre-cursor hourly drift check: probes the 24 clock-aligned hours before each entity's cursor each sync and re-pulls any hour whose count increased
- Parallel entity processing via `ThreadPoolExecutor` for backfill and mod_ts capability discovery
- Adaptive page sizing: automatically halves `page_size` on timeout and recalculates the offset to resume without data loss
- Automatic full-scan fallback for entities that do not support DESC ordering
- `mod_ts` support discovered once per entity and cached in state, avoiding repeated describe-endpoint calls
- Entities sorted largest-first before processing using a lightweight count probe
- Two monitoring tables (`counts_by_day`, `pre_cursor_hourly_counts`) written each sync for observability


## Configuration file

```json
{
    "base_url": "https://<YOUR_REGION>.wms.ocs.oraclecloud.com/<YOUR_ORG>",
    "username": "<YOUR_USERNAME>",
    "password": "<YOUR_PASSWORD>",
    "page_size": "500",
    "max_pages": "100",
    "lookback_check_hours": "24",
    "lag_minutes": "0",
    "test_entities": ""
}
```

| Key | Required | Description |
|-----|----------|-------------|
| `base_url` | Yes | Base URL of your Oracle WMS instance, e.g. `https://region.wms.ocs.oraclecloud.com/org` |
| `username` | Yes | Oracle WMS service account username |
| `password` | Yes | Oracle WMS service account password |
| `page_size` | No | Records per page (default `1000`). Reduce if timeouts occur; the connector also adapts automatically |
| `max_pages` | No | Soft page limit per entity per sync for backfill (default `100`). The connector continues past this limit until the current timestamp group is fully consumed |
| `lookback_check_hours` | No | Number of hours before each entity's cursor to probe for drift (default `24`) |
| `lag_minutes` | No | Minutes to subtract from the current time when computing the incremental upper bound (default `0`) |
| `test_entities` | No | Comma-separated list of entity names to sync; leave empty to sync all entities |

> Note: When submitting connector code as a [Community Connector](https://github.com/fivetran/community_connectors/tree/main) in the open-source [Connector SDK repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector beyond those pre-installed in the Connector SDK runtime.

```
requests>=2.28.0
```

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.


## Authentication

The connector uses HTTP Basic Authentication. Provide your Oracle WMS service account `username` and `password` in `configuration.json`. All requests are made over HTTPS.


## Pagination

The Oracle WMS REST API uses offset-based pagination. The connector requests pages sequentially using `page` and `page_size` parameters. `page_count` is read from the first response and used to bound the loop; if Oracle reduces `page_count` mid-stream (stale cache behaviour), the lower value is accepted immediately to avoid phantom last-page 500 errors.

The `max_pages` configuration key sets a soft limit for backfill fetches per entity per sync. When this limit is reached mid-timestamp-group, the connector continues fetching until the timestamp changes before checkpointing, ensuring no records at a boundary timestamp are skipped.

On timeout, the connector halves `page_size` (down to a minimum of 25) and recalculates the current page number to preserve the same record offset. This reduction persists for the remainder of that entity's fetch.


## Data handling

Incremental entities (those whose Oracle WMS describe endpoint lists a `mod_ts` field) use cursor-based sync. Entities without `mod_ts` support receive a full scan each sync, preceded by `op.truncate()` to soft-delete removed records.

Each record is delivered via `op.upsert()` using `id` as the primary key. The two monitoring tables use composite primary keys:

- `counts_by_day`: `(table_name, mod_ts_day, batch_id)`
- `pre_cursor_hourly_counts`: `(table_name, hour_start, batch_id)`

Timestamps are normalized to second precision before being used as Oracle WMS query parameters, as the API rejects sub-second values.


## Error handling

- Transient request failures are retried up to 5 times with exponential backoff starting at 1 second — refer to `make_api_request()` in `api.py`.
- Entities that return HTTP 400 for a given ordering parameter raise `OrderingNotSupportedError`, which bypasses retry and falls back to an unordered full scan.
- Timeouts trigger adaptive page-size reduction rather than a hard failure, allowing large entities to complete at a smaller page size.
- Per-entity failures are caught and logged without aborting the sync; partial progress is checkpointed so the next sync retries only the failed entity.


## Tables created

26 warehouse entity tables (all with primary key `id`):

| Table | Description |
|-------|-------------|
| `allocation` | Inventory allocations to orders |
| `batch_number` | Lot/batch tracking numbers |
| `company` | Company master records |
| `container` | Physical storage containers |
| `container_lock_xref` | Container lock cross-references |
| `facility` | Warehouse facility records |
| `history_activity` | Warehouse activity history |
| `ib_container` | Inbound containers |
| `ib_shipment` | Inbound shipment headers |
| `ib_shipment_dtl` | Inbound shipment detail lines |
| `inventory` | Current inventory positions |
| `inventory_attribute` | Inventory attribute values |
| `inventory_lock` | Inventory lock records |
| `inventory_status` | Inventory status codes |
| `item` | Item master records |
| `item_metric` | Item measurement metrics |
| `location` | Warehouse location master |
| `order_dtl` | Outbound order detail lines |
| `order_hdr` | Outbound order headers |
| `order_status` | Order status codes |
| `order_type` | Order type codes |
| `purchase_order_dtl` | Purchase order detail lines |
| `purchase_order_hdr` | Purchase order headers |
| `purchase_order_status` | Purchase order status codes |
| `putaway_type` | Putaway type codes |
| `vendor` | Vendor master records |

2 monitoring tables:

| Table | Primary key | Description |
|-------|-------------|-------------|
| `counts_by_day` | `(table_name, mod_ts_day, batch_id)` | Daily mod_ts record counts for the last 30 calendar days, written each sync |
| `pre_cursor_hourly_counts` | `(table_name, hour_start, batch_id)` | Hourly mod_ts counts for the lookback window before each entity's cursor, written each sync |


## Additional files

- `api.py` – Oracle WMS REST API client: single-page requests with retry and exponential backoff, multi-page pagination with adaptive page sizing, entity count probes, and mod_ts capability discovery.
- `utils.py` – Constants, the entity list, `OrderingNotSupportedError`, configuration validation, and timestamp utility functions.
- `incremental.py` – Two-phase incremental sync logic: Phase 1 `mod_ts` cursor-advancement and Phase 2 `create_ts` catch-up for backdated records.
- `backfill.py` – Historical backfill logic: descending offset pagination in rolling 30-day windows with timeout rollback and consecutive-empty-window termination.
- `pre_sync_drift_check.py` – Pre-cursor hourly drift detection: probes counts for clock-aligned hourly windows before each entity's cursor, compares against prior-sync counts, and re-pulls any hour whose count increased.


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
