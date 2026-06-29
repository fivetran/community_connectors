# ZoomInfo Connector Example

Contributed by ZoomInfo Technologies LLC. See [Accreditation](#accreditation) below for the maintenance posture.

## Connector overview

This connector syncs ZoomInfo Go-To-Market data — contacts, companies, scoops, intent signals, news, and optional enrichments (contacts, companies, scoops, technologies, corporate hierarchy) — from the ZoomInfo Search and Enrich APIs into a Fivetran destination.

Free Search endpoints consume only your ZoomInfo records/requests quota. Enrich endpoints debit one ZoomInfo credit per record (or per company, depending on the endpoint) and are opt-in via configuration flags.

The connector uses OAuth Client Credentials Flow to authenticate, paginates via JSON:API `page[size]` / `page[number]`, and syncs incrementally where the API exposes a date filter. The `companies` table has no incremental cursor on the ZoomInfo side, so it is full-replaced each run: pagination buffers the universe in memory first, and only after pagination succeeds does the connector `op.truncate` + `op.upsert` as a two-phase swap. If pagination fails partway through, the destination table is left untouched.

## Accreditation

This example was contributed by ZoomInfo Technologies LLC.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_csdk_connectors/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```
fivetran init --template zoominfo
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with your ZoomInfo `client_id` and `client_secret` before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- 11 tables across Search (free) and Enrich (credit-bearing) endpoints, gated by configuration flags
- Incremental sync on `contacts`, `scoops`, `intent`, and `news` via per-endpoint server-side date filters confirmed against `/lookup/search`
- Full-replace sync on `companies` via buffer-then-`op.truncate` + `op.upsert` (the ZoomInfo Search API does not expose a `lastUpdated`-style filter for the company entity). Pagination buffers the full universe before any destructive op — if the Search call fails partway through, the destination table is preserved.
- OAuth Client Credentials token caching with mid-sync 401 refresh
- Per-company enrichments (`scoops_enriched`, `technologies`) parallelized across a 3-worker thread pool, with rows streamed back through a bounded queue (max 2,000 in flight) to keep memory bounded under Fivetran's 1 GB runtime ceiling. `op.upsert` calls stay on the main thread for SDK safety.
- Mid-sync checkpoints fire after every completed table AND every 1,000 rows inside `contacts`, `scoops`, `intent`, `news`, and the streaming per-company enrich helpers (`scoops_enriched`, `technologies`), so the destination commits incrementally instead of buffering an entire large table in flight.
- 403 responses on enrich endpoints (missing license) are logged as warnings and skipped — the rest of the sync continues
- Retries with exponential backoff on 429 and transient 5xx; per-request connect/read timeouts

## Configuration file

Configuration values are passed to Fivetran via `configuration.json` at deploy time. All values are stored as strings; the connector parses booleans (`"true"` / `"false"`), comma-separated lists, and dropdown values internally. The Connector SDK does not support a custom UI form schema — the setup form in the Fivetran dashboard is auto-generated from the keys in `configuration.json`, with every value masked for security.

For local development, fill in the two required secrets in `configuration.json`, and optionally add any non-default toggles you want to exercise.

The configuration is a flat JSON object of string values. A minimal `configuration.json` contains only the two required secrets:

```json
{
  "client_id": "<YOUR_ZOOMINFO_CLIENT_ID>",
  "client_secret": "<YOUR_ZOOMINFO_CLIENT_SECRET>"
}
```

To exercise optional behavior, add any of the toggles documented below (all values are strings). For example:

```json
{
  "client_id": "<YOUR_ZOOMINFO_CLIENT_ID>",
  "client_secret": "<YOUR_ZOOMINFO_CLIENT_SECRET>",
  "countries": "United States",
  "full_refresh": "false",
  "intent_topics": "",
  "sync_news": "false",
  "enrich_contacts": "false",
  "enrich_filter": "has_email_or_phone",
  "enrich_management_levels": "",
  "enrich_companies": "false",
  "enrich_scoops": "false",
  "enrich_technologies": "false",
  "enrich_corporate_hierarchy": "false"
}
```

### Required

| Field | Description |
|-------|-------------|
| `client_id` | ZoomInfo API Client ID from your Developer Portal application. |
| `client_secret` | ZoomInfo API Client Secret. Stored encrypted by Fivetran; never displayed after save. |

### Optional — filters and sync mode

| Field | Default | Description |
|-------|---------|-------------|
| `countries` | `"United States"` | Country to sync from ZoomInfo Search endpoints. The ZoomInfo Search API accepts one country per request — deploy one connector per country to sync multiple. Example: `"United Kingdom"`. |
| `full_refresh` | `"false"` | When `"true"`, every sync re-fetches the entire record universe instead of only records updated since the last successful sync. Use after a config change (switching countries, adding output fields) or to recover from suspected data drift. |
| `intent_topics` | `""` (skip Intent sync) | Comma-separated list of ZoomInfo Intent topics. Must be exact names from your account's licensed intent-topic list — call `GET /gtm/data/v1/lookup/intent-topics` to see valid values. Up to 50 topics per sync. Unrecognized topics are skipped with a warning. No credits consumed (records and requests are counted). |
| `sync_news` | `"false"` | When `"true"`, syncs ZoomInfo News articles to the `news` table. No credits consumed. |

### Optional — Contact enrichment

Each enriched contact costs one ZoomInfo credit. Combine `enrich_contacts` with the filters below to control credit spend.

| Field | Default | Description |
|-------|---------|-------------|
| `enrich_contacts` | `"false"` | When `"true"`, enriches contacts with full profile data after the Search sync. |
| `enrich_filter` | `"has_email_or_phone"` | Which contacts are eligible for enrichment. One of: `has_email` (ZoomInfo has an email), `has_phone` (ZoomInfo has a direct or mobile phone), `has_email_or_phone` (either), `all` (enrich every contact — highest credit usage). |
| `enrich_management_levels` | `""` (all levels) | Comma-separated list of management levels to enrich. Valid values: `Board Member`, `C Level Exec`, `VP Level Exec`, `Director`, `Manager`, `Non Manager`. Example: `"C Level Exec,VP Level Exec"`. |
| `enrich_output_fields` | (curated default set) | Comma-separated list of fields to return from Contact Enrich. Leave blank to use the default set. See `DEFAULT_CONTACT_ENRICH_FIELDS` in `constants.py` for the default list, and ZoomInfo's `/lookup/enrich?filter[entity]=contact` for the full set of valid field names. |

### Optional — Company enrichment

Each enriched company costs one ZoomInfo credit per enrichment type. `enrich_scoops` and `enrich_technologies` are also one credit per company, but each can produce many rows.

| Field | Default | Description |
|-------|---------|-------------|
| `enrich_companies` | `"false"` | When `"true"`, enriches companies with full firmographic data (revenue, employee count, industry, description) after the Search sync. |
| `enrich_companies_output_fields` | (curated default set) | Comma-separated override for Company Enrich output fields. See `DEFAULT_COMPANY_ENRICH_FIELDS` in `constants.py` and ZoomInfo's `/lookup/enrich?filter[entity]=company`. |
| `enrich_scoops` | `"false"` | When `"true"`, enriches each company with its latest Scoops (buying signals, hiring news, etc.). Each scoop returned counts as a record. |
| `enrich_technologies` | `"false"` | When `"true"`, enriches each company with its technology stack. Produces ~2,600 rows per large company — a 10K-company sync can generate ~26M `technologies` rows. See "Known limitations" below. |
| `enrich_corporate_hierarchy` | `"false"` | When `"true"`, enriches each company with its full corporate hierarchy (parent, subsidiaries, acquisitions, former names, locations). |
| `enrich_corp_hier_output_fields` | (curated default set) | Comma-separated override for Corporate Hierarchy Enrich output fields. See `DEFAULT_CORP_HIER_FIELDS` in `constants.py` and ZoomInfo's `/lookup/enrich?filter[entity]=corporate-hierarchy`. |

> Note: When submitting connector code as a [Community Connector](https://github.com/fivetran/fivetran_csdk_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector requires only `requests` at runtime, which is pre-installed in the Fivetran runtime along with `fivetran_connector_sdk`. Because it has no additional runtime dependencies, this connector does not include a `requirements.txt` file.

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Project structure

The connector is split across several modules for readability. `connector.py` remains the entry point Fivetran loads; the rest are sibling modules imported by it.

- `connector.py` — Entry point: defines `schema()` and `update()` and instantiates the `Connector` object.
- `constants.py` — Endpoint paths, tunables (page size, worker count, retry policy, checkpoint cadence), default output-field lists, and state keys.
- `client.py` — OAuth 2.0 token cache, retry-aware HTTP transport, and the Search / per-company Enrich pagination generators.
- `config.py` — Configuration parsing and validation, Search-filter construction, the incremental-cursor predicate, and enrichment eligibility checks.
- `transforms.py` — Pure value coercion helpers (safe int/float/datetime, incremental-cursor comparison). No network or SDK dependency.
- `sync.py` — One `sync_*` function per destination table plus the bounded-memory streaming helper for per-company enrichments.

## Authentication

ZoomInfo uses the OAuth 2.0 Client Credentials Flow.

1. Sign in to the [ZoomInfo Developer Portal](https://api.zoominfo.com/).
2. Go to **Apps** and select **Create app**.
3. Add the scopes you need: `api:data:contacts`, `api:data:companies`, `api:data:scoops`, `api:data:intent`, `api:data:news`, plus the relevant enrich scopes.
4. Copy the resulting Client ID and Client Secret into your local `configuration.json` (or paste them into the Client ID / Client Secret fields in the Fivetran UI when configuring a production connector).

The connector exchanges these credentials for a Bearer token at `https://api.zoominfo.com/gtm/oauth/v1/token` and caches the token in process until 60 seconds before expiry. If a mid-sync 401 occurs, the cache is invalidated and a fresh token is fetched once before the request is retried. Refer to `def get_access_token(configuration)`.

## Pagination

All Search endpoints and the Scoops Enrich endpoint use JSON:API URL-query pagination: `page[size]=100` and `page[number]=N`, incremented until the response's `meta.page.total` is reached (or `meta.totalResults` is exhausted for endpoints that omit `meta.page.total`). The Search API caps `meta.page.total` at 100, so a single query yields at most 10,000 records — see "Search result ceiling" under Known limitations. Refer to `def paginate(configuration, endpoint, attributes)` and `def paginate_enrich_scoops(configuration, company_id)`.

The Technologies Enrich endpoint (`/gtm/data/v1/companies/technologies/enrich`) is an exception: it ignores `page[number]` and returns the full technology stack for the company in a single response. `def paginate_enrich_technologies(configuration, company_id)` is therefore a single-shot fetch.

## Data handling

Tables with a server-side date filter (`contacts`, `scoops`, `intent`, `news`) sync incrementally. The connector tracks the maximum `lastUpdatedDate` (or endpoint-specific equivalent) per run in `state` and, on the next run, applies it as the per-endpoint filter field confirmed against `/lookup/search`. Refer to `def apply_incremental_filter(...)`.

The `companies` table does not have an incremental cursor available on the ZoomInfo Search API. To keep destination data in sync with reality (defunct companies removed, name/website changes reflected), `def sync_companies(...)` buffers the entire live universe in memory first, then — only if pagination succeeds — calls `op.truncate(table="companies")` and `op.upsert` for each company. This two-phase swap means a pagination failure (network, 5xx, auth) leaves the destination table untouched rather than empty. Rows soft-deleted by truncate that re-appear in the upsert are restored; rows that don't re-appear remain marked `_fivetran_deleted = TRUE`.

All list and nested-object fields (e.g. `topics`, `types`, `familyTree`, `parentage`) are serialized to JSON strings before upsert. Numeric fields are coerced via `_safe_int` / `_safe_float` because ZoomInfo occasionally returns numbers as strings.

Mid-sync checkpoints are emitted after each completed table via `op.checkpoint(state=cumulative_state)`, and additionally every 1,000 rows inside `contacts`, `scoops`, `intent`, `news`, `scoops_enriched`, and `technologies`. A crash resumes from the most recent checkpoint — row-level for those six tables, table-level for everything else.

## Error handling

HTTP-layer retries are handled by `def post_with_retry(...)` and `def get_with_retry(...)`: up to 5 attempts on 429 and on transient 5xx (500, 502, 503, 504), with exponential backoff starting at 2 seconds and capped at 60 seconds per wait. Connection-level failures (timeouts, dropped sockets) follow the same retry policy.

A 403 from an enrich endpoint indicates the account does not hold a license for that specific enrichment. Each `sync_*_enriched` function and the per-company `paginate_enrich_*` generators detect 403 separately and log a single warning naming the missing license, then return cleanly so the parent `update()` continues with later tables. The corresponding table remains declared in the schema but is empty for that sync.

A 403 from `intent/search` or `news/search` is treated the same way (intent and news require additional licensing on the ZoomInfo side).

All other non-200 responses bubble up as `RuntimeError` so real outages and credential failures are surfaced to the Fivetran dashboard rather than silently swallowed.

## Tables created

| Table | Source endpoint | Primary key | Sync mode | Cost |
|-------|-----------------|-------------|-----------|------|
| `CONTACTS` | `/gtm/data/v1/contacts/search` | `id` | incremental on `lastUpdatedDateAfter` | free |
| `COMPANIES` | `/gtm/data/v1/companies/search` | `id` | full-replace (truncate + upsert) | free |
| `SCOOPS` | `/gtm/data/v1/scoops/search` | `id` | incremental on `publishedStartDate` | free |
| `USAGE` | `/gtm/data/v1/users/usage` | `id` (= `limitType`) | full-replace via upsert | free |
| `INTENT` | `/gtm/data/v1/intent/search` | `id` | incremental on `signalStartDate` | free |
| `NEWS` | `/gtm/data/v1/news/search` | `id` | incremental on `pageDateMin` | free |
| `CONTACTS_ENRICHED` | `/gtm/data/v1/contacts/enrich` | `id` | upsert | 1 credit / contact |
| `COMPANIES_ENRICHED` | `/gtm/data/v1/companies/enrich` | `id` | upsert | 1 credit / company |
| `SCOOPS_ENRICHED` | `/gtm/data/v1/scoops/enrich` | `id` | upsert | 1 credit / company |
| `TECHNOLOGIES` | `/gtm/data/v1/companies/technologies/enrich` | synthetic `{company_id}_{technology_id}` | upsert | 1 credit / company |
| `CORPORATE_HIERARCHY` | `/gtm/data/v1/companies/corporate-hierarchy/enrich` | `company_id` | upsert | 1 credit / company |

The full schema (column names and types) is declared in `connector.py` under `def schema(configuration)`.

## Additional considerations

This connector is published as-is and is not actively maintained. Customers needing new tables, additional fields, or fixes for breaking ZoomInfo API changes should fork the example. Every table includes a `raw_attributes` JSON column containing the full API response — new ZoomInfo fields added after publication are automatically captured there and can be extracted via warehouse SQL without a code change.

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

For ZoomInfo API issues, contact your ZoomInfo account team.

### Known limitations

- Search result ceiling (most important). The ZoomInfo Search API returns at most 10,000 records per query (`meta.page.total` caps at 100 pages of `page[size]=100`), regardless of how large `meta.totalResults` is. If a query's universe exceeds 10,000, only the first 10,000 records are retrievable — the remainder cannot be fetched by paging further. Because incremental syncs advance the cursor past whatever was fetched, the dropped records are not backfilled on later runs. The connector logs a warning (see `_warn_if_truncated()`) when `totalResults` exceeds the ceiling. To capture a larger universe, narrow each query so it stays under 10,000 records, for example by deploying one connector instance per country (or per finer-grained segment). The default `countries` value of `United States` will exceed this ceiling; treat the default run as a sample, not a complete extract.
- Single-country filter per connector instance. The ZoomInfo Search API accepts one country per request; configure additional countries by deploying multiple connector instances.
- Client-side page cap. `MAX_PAGES` is a client-side backstop set to 10,000 pages per endpoint. In practice the 10,000-record server-side ceiling above is reached first, so this backstop is rarely the binding limit.
- High volume on the `technologies` table. The Technologies Enrich endpoint returns the full technology stack per company in a single response — observed at ~2,600 rows for large companies. For a sync covering 10,000 companies this produces ~26M `technologies` rows. The connector handles this safely (streaming queue + intra-table checkpoints every 1,000 rows), but sync time and destination storage scale linearly with company count. If you do not need per-technology granularity, leave `enrich_technologies` disabled — the firmographic technology fields are already available on `companies_enriched`.
- Intra-table crash recovery is row-granular (checkpoints every 1,000 rows) for `contacts`, `scoops`, `intent`, `news`, `scoops_enriched`, and `technologies`. It is table-granular for `companies`, `companies_enriched`, `contacts_enriched`, `corporate_hierarchy`, and `usage` — a crash partway through those re-pulls the table from its previous state cursor on the next run.
- Schema drift is silent. New fields added by ZoomInfo to a Search or Enrich response are not auto-emitted; they require a schema update in `connector.py`. The `raw_attributes` JSON column on every table preserves the full API response so new fields can be extracted via warehouse SQL without a code change.
