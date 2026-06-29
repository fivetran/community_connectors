# AVEVA PI Connector SDK Connector Example

## Connector overview

This connector syncs data from AVEVA PI (formerly OSIsoft PI) to your Fivetran destination. It communicates with the PI system via the PI Web API REST interface — no proprietary ODBC drivers are required, so the connector runs in Fivetran's managed cloud environment without any additional installation.

Key capabilities:
- REST-based connectivity via PI Web API (HTTPS + Basic auth) — no ODBC driver installation required
- Four fixed tables: `elements`, `attributes`, `event_frames`, and `recorded_values`
- Full reimport for `elements` and `attributes` (PI AF asset hierarchy)
- Cursor-based incremental sync for `event_frames` and `recorded_values` with adaptive time-window backoff
- 2-hour late-arrival rollback on `recorded_values` to capture values written after their timestamps
- MD5-based synthetic `_fivetran_id` primary key for `recorded_values`
- Authentication-aware retry: 4xx responses surface immediately; 5xx / network errors retry up to 3 times
- `recorded_values` sync is opt-in (set `sync_recorded_values = "true"`) because it can generate very large data volumes


## Requirements

- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- PI Web API 2019 SP1 or later, reachable over HTTPS from the connector host
- Basic authentication enabled on the PI Web API server
- A PI user account with read access to the target AF database


## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```bash
fivetran init --template aveva_pi
```

> Note: Ensure you have updated `configuration.json` with your PI Web API connection details before running `fivetran debug`. See the [Configuration file](#configuration-file) section below.

1. Verify PI Web API is reachable: open `https://<PI_WEB_API_HOSTNAME>/piwebapi` in a browser and confirm you can authenticate.
2. Update `configuration.json` with your connection details.
3. Test the connector locally:
   ```bash
   fivetran debug
   ```


## Features

- No-driver REST connectivity via PI Web API — works in Fivetran's managed cloud environment
- Fixed four-table schema: `elements`, `attributes`, `event_frames`, `recorded_values`
- Cursor-based incremental sync for `event_frames` and `recorded_values`
- Adaptive time-window backoff: starts at 30-day windows, halves automatically on request errors, minimum 1-hour window
- 2-hour late-arrival rollback for `recorded_values` to capture late-written archive data
- MD5 hash-based synthetic primary key (`_fivetran_id`) for `recorded_values`
- Periodic checkpointing every 10,000 rows during full reimports
- Paginated fetching via `Links.Next` — no large in-memory result sets


## Configuration file

```json
{
  "base_url": "<PI_WEB_API_BASE_URL>",
  "username": "<PI_USERNAME>",
  "password": "<PI_PASSWORD>",
  "database_name": "<PI_AF_DATABASE_NAME>",
  "verify_ssl": "<true_or_false>",
  "start_date": "<START_DATE_ISO8601>",
  "sync_recorded_values": "<true_or_false>"
}
```

| Key | Required | Description |
|---|---|---|
| `base_url` | Yes | Base URL of the PI Web API instance (e.g. `https://piserver/piwebapi`) |
| `username` | Yes | PI user account with read access to the target AF database |
| `password` | Yes | Password for the PI user account |
| `database_name` | No | PI AF database name to sync. Defaults to the first database found if omitted |
| `verify_ssl` | No | Set to `"false"` to skip TLS certificate verification for self-signed certificates (default: `"true"`) |
| `start_date` | No | ISO 8601 start date for the first incremental sync (default: Unix epoch). Example: `"2020-01-01T00:00:00Z"` |
| `sync_recorded_values` | No | Set to `"true"` to also sync the `recorded_values` table. Disabled by default because it can generate very large data volumes on large PI deployments |

> Note: When submitting connector code as a Community Connector, ensure `configuration.json` has placeholder values. When deploying, do not check this file into version control to protect credentials.


## Authentication

The connector uses HTTP Basic authentication. Credentials are passed in each request via the `Authorization` header. The PI Web API server validates them against the configured PI identity provider.

To set up authentication:

1. Log in to your PI Web API server admin interface and confirm that Basic authentication is enabled under Security settings.
2. Create or identify a PI user account with read access to the target AF database.
3. Add the `username` and `password` for that account to `configuration.json`.
4. If your PI Web API server uses a self-signed TLS certificate, set `verify_ssl` to `"false"` in `configuration.json`.

If a request returns a 401 or 403 response, the connector raises a `ValueError` immediately without retrying, so the sync fails fast and Fivetran prompts for updated credentials. Other 4xx errors (e.g. 404 for a missing PI Point stream) are treated as skippable warnings for individual resources.


## Pagination

PI Web API responses include a `Links.Next` URL when there are more items. The connector follows this link chain automatically until all items are retrieved. Each page is processed and yielded immediately — no full result set is held in memory.

For incremental tables (`event_frames` and `recorded_values`), data is fetched in time windows of up to 30 days. If a request fails with a transient error, the window is halved and the same slice is retried. This continues until either the request succeeds or the window drops below 1 hour, at which point the error is surfaced.

Checkpointing occurs after each successful time window (incremental) or every 10,000 rows (full reimport), allowing the connector to resume from the last safe point after an interruption.


## Data handling

- Schema: Fixed four-table schema. Column types are auto-detected by the Fivetran Connector SDK from the data values upserted during sync.
- Timestamps: PI Web API returns ISO 8601 timestamps. The connector parses them to UTC-aware `datetime` objects before yielding rows to Fivetran.
- PI digital states: When a PI recorded value is a system digital state (a JSON object like `{"Name": "Shutdown", "Value": 248}`), only the `Name` string is stored in the `value` column.
- Hash IDs: The `recorded_values` table has no natural primary key. A `_fivetran_id` column is generated as the MD5 hex digest of `attribute_web_id|timestamp`.
- Category names: The `category_names` column stores a JSON-serialized array of category name strings (e.g. `["Production", "Critical"]`).


## Error handling

- HTTP 4xx responses raise a `ValueError` immediately (no retry). Refer to `api_get()` in `client.py`.
- HTTP 5xx and network errors retry up to 3 times with a warning logged per attempt. Refer to `api_get()` in `client.py`.
- Incremental query failures trigger adaptive window halving rather than a hard failure. If the window cannot be halved further (below 1 hour), a `RuntimeError` is raised. Refer to `sync_event_frames()` and `sync_recorded_values()` in `sync.py`.
- Individual `recorded_values` attribute streams that return 4xx errors are skipped with a warning (e.g. deleted PI Points). Refer to `sync_recorded_values()` in `sync.py`.


## Tables created

### elements

Full reimport. Represents PI AF elements (the asset hierarchy).

| Column | Type | Primary key |
|---|---|---|
| `web_id` | STRING | Yes |
| `name` | STRING | |
| `description` | STRING | |
| `path` | STRING | |
| `template_name` | STRING | |
| `category_names` | STRING | JSON array, e.g. `["Production"]` |

### attributes

Full reimport. Represents PI AF element attributes, including PI Point data reference metadata.

| Column | Type | Primary key |
|---|---|---|
| `web_id` | STRING | Yes |
| `element_web_id` | STRING | |
| `name` | STRING | |
| `description` | STRING | |
| `path` | STRING | |
| `type` | STRING | |
| `type_qualifier` | STRING | |
| `data_reference` | STRING | e.g. `"PI Point"` |
| `data_reference_path` | STRING | PI Point tag path |
| `category_names` | STRING | JSON array |

### event_frames

Incremental by `start_time`. Represents PI AF event frames (time-bounded events).

| Column | Type | Primary key |
|---|---|---|
| `web_id` | STRING | Yes |
| `name` | STRING | |
| `description` | STRING | |
| `start_time` | UTC_DATETIME | |
| `end_time` | UTC_DATETIME | |
| `template_name` | STRING | |
| `category_names` | STRING | JSON array |
| `database_web_id` | STRING | |

### recorded_values

Incremental by `timestamp`. Opt-in via `sync_recorded_values = "true"`. Represents PI archive (time-series) data for PI Point attributes.

| Column | Type | Primary key |
|---|---|---|
| `_fivetran_id` | STRING | Yes — MD5 of `attribute_web_id\|timestamp` |
| `attribute_web_id` | STRING | |
| `timestamp` | UTC_DATETIME | |
| `value` | STRING | |
| `quality` | STRING | `"good"` or `"questionable"` |
| `good` | BOOLEAN | |


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
