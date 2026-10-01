# USGS Earthquake Connector Example

## Connector overview

This connector syncs earthquake and other seismic events from the [USGS FDSN Event Web Service](https://earthquake.usgs.gov/fdsnws/event/1/) (FDSN is the International Federation of Digital Seismograph Networks, whose query standard the service follows) into a Fivetran destination. The service publishes the USGS Comprehensive Catalog (ComCat): every event's time, location, depth, magnitude, review status, felt reports, PAGER (Prompt Assessment of Global Earthquakes for Response) alert level and the networks that contributed to it, worldwide.

Seismic catalogs change after the fact. A network revises a magnitude, a human reviews an automatic solution, two networks' solutions for the same event are merged under one preferred id (the id USGS treats as authoritative among those the networks assigned), and duplicates or false detections are deleted. The connector keeps the destination in step with those changes: after a historical sync from a configured start date, each incremental sync reads every event updated since the last sync, whatever year on or after `start_date` the event itself happened in, and applies deletes as deletes. Typical uses are regional seismic activity reporting, exposure monitoring for sites or assets, and research that needs the catalog as it stands today rather than as it was first published.

USGS data is in the public domain. Credit: U.S. Geological Survey.

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
fivetran init --template usgs_earthquake/connector
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- Historical sync of every event from a configured start date, in 30-day windows of event time.
- Incremental syncs that read every event updated since the previous sync, including revisions to events from any year on or after `start_date`.
- Deleted events are applied as deletes, in the historical sync and in every incremental sync, so duplicates and false detections that USGS removes do not stay in the destination as live rows.
- When USGS changes an event's preferred id, the row under the old id is deleted, leaving one live row per event.
- Keyset pagination on event time, so rows entering or leaving the result between requests cannot shift other rows out of view.
- A cursor taken from the server's clock rather than the local clock, and resumable state: a failed sync continues where it stopped.
- Every magnitude and every event type is synced, including quarry blasts and explosions, so a filter can be applied downstream without losing events.
- Retries with exponential backoff and jitter on rate limits, server errors and network failures.

## Configuration file

The connector has one configuration parameter.

```json
{
  "start_date": "<START_DATE_YYYY_MM_DD>"
}
```

- `start_date` (required) – The earliest event date to sync, in `YYYY-MM-DD` format, interpreted as midnight UTC. It must be between 1900-01-01 and today. The historical sync makes at least one request per 30-day window from this date, so `1900-01-01` means more than 1,500 requests before any recent event is read. Changing `start_date` after the first sync restarts the historical sync from the new date. Moving it later does not remove rows already synced from before the new date. A full re-sync does not remove them either, because it clears the connector's state but not the destination table. If USGS updates one of them, the next incremental sync deletes its row. Refer to `def validate_configuration(configuration: dict)` and `def parse_start_date(value)`.

> Note: When submitting connector code as a community connector in the open-source [Community Connector repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Authentication

The USGS event service is public and requires no API key or account. The connector sends no credentials, only a `User-Agent` header that identifies this example.

## Pagination

The FDSN event service can order results only by event time or by magnitude, not by update time. Every request asks for GeoJSON with `orderby=time-asc` and `limit=5000`, and the connector pages by keyset on event time (refer to `def fetch_page(session: requests.Session, params: dict)` and `def next_keyset_position(features: list, position: int)`):

- Each request sends `starttime` with millisecond precision. When a page is full, the next request starts at the time of that page's last event. `starttime` is inclusive, so the events at exactly that millisecond are read again, and the upsert makes the second read harmless.
- A page shorter than `limit` ends the window or the pass.
- Keyset paging filters on a value rather than an offset. Responses are cached at the CDN for 60 seconds per URL, so two pages of an offset query can come from different snapshots and skip rows. A keyset query cannot shift rows out of view that way. An event whose time is revised to before the current position mid-pass is missed by that pass, and the next sync reads it, because its update time is after the cursor.
- If a full page ends at the same millisecond it started at, more than 5000 events share one millisecond and paging on time cannot advance. The connector stops the sync with an error rather than looping or skipping events.

Both passes of the historical sync also bound each query with `endtime`, walking event time in 30-day windows from `start_date` (refer to `def walk_backfill_windows(session: requests.Session, state: dict, start_ms: int, pass_name: str)`). The windows bound the server-side sort, and keyset paging runs inside each window.

## Data handling

The first sync is a historical sync, and every later sync is incremental (refer to `def update(configuration: dict, state: dict)`).

The historical sync upserts every event from `start_date` up to the server time of its first response (refer to `def run_backfill(session: requests.Session, state: dict, start_ms: int)`). That server time, `metadata.generated`, is stored as the end of the historical sync, and the same time less a 10-minute overlap is stored as the starting cursor for the first incremental sync. Both are kept until the historical sync completes, so a historical sync that spans several syncs stops at the same point. It then makes a delete pass over the same 30-day windows with `includedeleted=only` and deletes each event USGS has deleted. The destination table is not always empty when a historical sync runs: a full re-sync clears the connector's state but keeps the table, and so does a change to `start_date`. Without the delete pass, a row from an earlier sync would stay live whenever USGS deleted its event after the last incremental sync. Deleting an event that never landed does nothing. An event deleted while the historical sync runs is updated after the stored cursor, so the first incremental sync removes it. A failed historical sync resumes the pass it was in. On 2026-10-01, USGS listed 742 deleted events from 2026-08-03 on, and a 30-day window of them came back in under two seconds.

Each incremental sync makes two passes in the same run, over events updated after the stored cursor (refer to `def run_incremental(session: requests.Session, state: dict, start_ms: int)` and `def run_pass(session: requests.Session, state: dict, start_ms: int, pass_name: str)`):

- The upsert pass queries `updatedafter=<cursor>&starttime=1000-01-01` and upserts each event on or after `start_date` (refer to `def upsert_events(features: list, start_ms: int)`).
- The delete pass sends the same query with `includedeleted=only`, which returns only the events USGS has deleted, and deletes each one (refer to `def delete_events(features: list)`).

The passes are separate because the service cannot return live and deleted events together on this query. `includedeleted=true` would return both, but over multi-day spans the query exceeds the CDN's 60-second limit and returns HTTP 504. Without the delete pass, events USGS removes, such as duplicates and false detections, would stay in the destination as live rows.

Both passes send an explicit `starttime`. Without it, the service silently limits `updatedafter` to events from the last 30 days, while an event from any year can be updated today.

That `starttime` is 1000-01-01, not `start_date`, because USGS can revise an event's origin time. No event in the catalog is older, so a revision cannot move an event out of view. An already-synced event revised to before `start_date` would otherwise drop out of both passes, and its row would stay live with the old time. With the earlier `starttime`, the upsert pass reads it and deletes the rows of all its ids instead of upserting it, and the delete pass still reads it if USGS later deletes it (refer to `def is_before_start(feature: dict, start_ms: int)`). The passes also read updated events from before `start_date` that were never synced, and deleting an id that has no row does nothing. In a 7-day sample taken on 2026-10-01, `starttime=1000-01-01` returned 5,010 updated events and `starttime=2026-08-03` returned 3,559. That is about 180 events per 6-hour sync, far below one 5,000-event page.

After both passes finish, the cursor moves to the server time of the sync's first response, less the 10-minute overlap. The overlap absorbs the whole-second precision of the server time and responses served from the 60-second cache. An event updated while a sync runs is read again by the next sync rather than missed.

A deleted event is sent to the destination as a delete for its own id, so the destination marks the row with `_fivetran_deleted` and keeps its last values. The other ids in a deleted event's `ids` field are not deleted, so a merge cannot remove the row of the event that survives it. Events can carry several ids when more than one network reported them, and USGS can change which one is preferred. After each upsert, the connector deletes the event's other ids, which removes the row left under a previous preferred id. A delete of an id that has no row does nothing.

Each event becomes one row (refer to `def build_event_row(feature: dict)`):

- API fields whose names are reserved or ambiguous in SQL are renamed: `time` to `event_time`, `updated` to `updated_at`, `magType` to `mag_type`, `status` to `review_status`, `net` to `network`, `code` to `network_event_code` and `type` to `event_type`.
- `event_time` and `updated_at` are converted from epoch milliseconds to UTC datetimes.
- Numbers are typed by column. DOUBLE columns take any number as a float, so a magnitude of 5 loads as 5.0. INT columns take only whole numbers, so 3.0 loads as 3. A boolean, a string such as `"5.2"` or a fraction in an INT column becomes null (refer to `def as_float(value)` and `def as_int(value)`).
- `geometry.coordinates` is split into `longitude`, `latitude` and `depth_km`. A missing or short coordinate array gives null values.
- The wrapping commas are removed from `ids`, `sources` and `types`, so `,us7000abcd,at00xyz,` becomes `us7000abcd,at00xyz`.
- `tz` is not synced. It is empty in the service's responses.

The connector checkpoints after every full page and at the end of each pass, so a sync that fails resumes from the last page it finished, in the same pass and with the same cursor (refer to `def save_state(state: dict)`). The incremental cursor is `updated_cursor`. The other state keys, and the reset when `start_date` changes, are in `def resolve_state(state: dict, fingerprint: str)`.

## Error handling

- Rate limits, server errors and network failures – HTTP 429, 500, 502, 503 and 504 responses, connection errors, timeouts, interrupted downloads and responses that are not valid JSON are tried up to 5 times. The wait doubles from 2 seconds and adds up to 1 second of jitter. A `Retry-After` header in seconds is honored when it asks for a longer wait, up to 60 seconds. After the last attempt, the sync fails with the last error. Refer to `def fetch_page(session: requests.Session, params: dict)` and `def retry_delay(attempt: int, retry_after)`.
- Other HTTP errors – Any other error status fails the sync immediately with the status code and the start of the response body, because a retry cannot fix the request.
- Request timeout – Every request has a 90-second timeout, longer than the CDN's 60-second limit, so the CDN answers a slow query first with an HTTP 504, which is retried.
- Missing server time – A response without `metadata.generated` fails the sync, because the cursor cannot be set without it.
- Events without an id – Skipped, and reported with a warning that shows in the Fivetran dashboard. Refer to `def upsert_events(features: list, start_ms: int)`.
- Invalid configuration – A missing, placeholder, malformed, pre-1900 or future `start_date` fails the sync with a message naming the parameter. Refer to `def validate_configuration(configuration: dict)`.

## Tables created

The connector creates one table, `earthquake`, with the primary key `id` (refer to `def schema(configuration: dict)`).

| Column | Type | Description |
| --- | --- | --- |
| `id` | STRING | The event's preferred id. Primary key |
| `event_time` | UTC_DATETIME | When the event occurred. From the API field `time` |
| `updated_at` | UTC_DATETIME | When the event was last updated. From the API field `updated` |
| `mag` | DOUBLE | The preferred magnitude |
| `mag_type` | STRING | The method used to calculate the preferred magnitude, for example `ml` or `mww` |
| `place` | STRING | A text description of the region near the event |
| `event_type` | STRING | The type of seismic event, for example `earthquake`, `quarry blast` or `explosion`. From the API field `type` |
| `review_status` | STRING | `automatic` or `reviewed`. From the API field `status` |
| `longitude` | DOUBLE | Longitude of the epicenter in decimal degrees |
| `latitude` | DOUBLE | Latitude of the epicenter in decimal degrees |
| `depth_km` | DOUBLE | Depth of the event in kilometers |
| `felt` | INT | The number of felt reports submitted to the Did You Feel It? system |
| `cdi` | DOUBLE | The maximum reported intensity from Did You Feel It? reports |
| `mmi` | DOUBLE | The maximum estimated instrumental intensity from ShakeMap |
| `alert` | STRING | The PAGER alert level: `green`, `yellow`, `orange` or `red` |
| `tsunami` | INT | 1 for large events in oceanic regions, otherwise 0. It does not mean a tsunami occurred |
| `sig` | INT | A significance score based on magnitude, intensity, felt reports and estimated impact. USGS documents it as 0 to 1000, but larger values occur: the data this example was tested on reached 2910 |
| `network` | STRING | The id of the preferred contributing network. From the API field `net` |
| `network_event_code` | STRING | The event's code within the preferred network. From the API field `code` |
| `ids` | STRING | Comma-separated list of every id associated with the event |
| `sources` | STRING | Comma-separated list of the contributing networks |
| `types` | STRING | Comma-separated list of the product types available for the event |
| `nst` | INT | The number of seismic stations used to locate the event |
| `dmin` | DOUBLE | Horizontal distance from the epicenter to the nearest station, in degrees |
| `rms` | DOUBLE | Root-mean-square travel time residual, in seconds |
| `gap` | DOUBLE | The largest azimuthal gap between adjacent stations, in degrees |
| `title` | STRING | The event title, combining magnitude and place |
| `url` | STRING | Link to the USGS event page |
| `detail` | STRING | Link to the event's detailed GeoJSON |

## Additional considerations

The optional context pack in [`../context_pack/`](../context_pack/) gives AI agents a dbt and MetricFlow semantic layer and DuckDB agent instructions for this connector's `earthquake` table. The connector does not use it.

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
