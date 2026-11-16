# SAP SuccessFactors Connector Example

## Connector overview

This connector replicates SAP SuccessFactors HR data to a Fivetran-supported destination using the SAP OData v2 API. It is designed for tenants that require coverage beyond the ~130 tables available in Fivetran's native SAP SuccessFactors connector.

The connector syncs every configured entity with its full field set. Fields declared in OData `$metadata` that are absent from a given record's JSON payload are emitted as `NULL` so every destination column is always present. All date and datetime values are normalized from SAP's `/Date(milliseconds)/` wire format to UTC ISO 8601 strings. Primary keys, pagination order, and effective-dating behaviour are derived at runtime from the authenticated OData `$metadata` endpoint — never hard-coded — which protects against silent schema drift as the SuccessFactors instance evolves.

Effective-dated entities (those whose key includes a date field such as `startDate`, `effectiveStartDate`, or `mdfSystemEffectiveStartDate`) are queried across the full history window (`fromDate=1900-01-01`, `toDate=9999-12-31`) instead of SAP's implicit `asOfDate=today` default. This ensures every past, current, and future-dated record is extracted rather than only the slice effective at query time.

The form hierarchy (Review and 360 form types) is replicated through the mandatory `FormHeader` navigation context using breadth-first expansion. Incremental form syncs scan only changed form IDs and re-fetch only the forms that changed, with a full pass running on a configurable interval (default: 7 days). Ratings, comments, and supplementary navigations are preserved inline as JSON on the parent row.

## Requirements

- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- A SAP SuccessFactors tenant with OData v2 API access
- An API user with read permissions across all entities to be synced
- Either SAP OAuth 2.0 credentials (client ID, company ID, user ID, private key) or a pre-issued bearer token

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/getting-started) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```
fivetran init --template sap_successfactors
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/getting-started/connector-sdk-commands#fivetraninit).

## Features

- Syncs all SAP SuccessFactors OData v2 entities discovered from `$metadata` to the destination
- Incremental sync using `lastModifiedDateTime` watermarks per entity; full reload for entities SAP marks as non-filterable
- Effective-dated entity support — full historical and future-dated record extraction
- Parallel page prefetch (4 workers, 8 pages in-flight) for large entity loads
- 30-day windowed time-slice fetch for 8 large time management entities, each window individually checkpointed and resumable
- 40 req/sec token-bucket rate limiter matching SAP's native connector cap, preventing silent record loss under parallel load
- `$paging=snapshot` on all requests to guarantee consistent skip-based pagination
- Review and 360 form hierarchy replication through `FormHeader` navigation expansion
- Incremental form sync with per-form retry, 200-ID classification batches, and per-phase timing instrumentation
- Full field completeness — `NULL`-filled for fields absent in the SAP payload
- Runtime primary key and schema detection from OData `$metadata`
- Per-entity failure isolation — a single failing entity is logged to `connector_sync_issues` and retried next sync without aborting the run
- Dashboard warnings for partial form extraction failures and permission-blocked entities
- Automatic OAuth token refresh mid-sync using SAML2 bearer assertion flow

## Configuration file

Use one of the two authentication modes below — do not combine them.

OAuth mode (recommended):
```json
{
  "sf_api_url": "<YOUR_SAP_API_BASE_URL_E_G_HTTPS_API4_SAPSF_COM_ODATA_V2>",
  "sf_client_id": "<YOUR_SAP_OAUTH_CLIENT_ID>",
  "sf_company_id": "<YOUR_SAP_COMPANY_ID>",
  "sf_user_id": "<YOUR_SAP_API_USER_ID>",
  "sf_private_key": "<YOUR_RSA_PRIVATE_KEY_PEM_OR_BASE64>",
  "force_full_form_pass": "<TRUE_OR_FALSE_DEFAULT_FALSE>",
  "form_full_pass_interval_days": "<NUMBER_OF_DAYS_DEFAULT_7>"
}
```

Bearer token mode (if managing token issuance externally):
```json
{
  "sf_api_url": "<YOUR_SAP_API_BASE_URL_E_G_HTTPS_API4_SAPSF_COM_ODATA_V2>",
  "sf_access_token": "<YOUR_BEARER_TOKEN>",
  "force_full_form_pass": "<TRUE_OR_FALSE_DEFAULT_FALSE>",
  "form_full_pass_interval_days": "<NUMBER_OF_DAYS_DEFAULT_7>"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

- `sf_api_url` — SAP SuccessFactors OData v2 API base URL for your data center (e.g. `https://api4.sapsf.com/odata/v2`). Required. Find your data center URL in SAP SuccessFactors Admin Center under Company System and Logo Settings.
- `sf_client_id` — OAuth 2.0 client ID registered in SAP SuccessFactors. Required when using OAuth authentication.
- `sf_company_id` — SAP SuccessFactors company ID. Required when using OAuth authentication.
- `sf_user_id` — API user ID used to sign the SAML2 assertion. Required when using OAuth authentication.
- `sf_private_key` — RSA private key in PEM format or its base64-encoded body, corresponding to the X.509 certificate registered for the OAuth client. Required when using OAuth authentication.
- `sf_access_token` — Pre-issued bearer token. Use this as an alternative to OAuth credentials when you manage token issuance externally. Mutually exclusive with the four OAuth fields.
- `force_full_form_pass` — Set to `"true"` to force a full pass over all forms on the next sync, regardless of the last full pass timestamp. Resets to incremental after one run. Optional; defaults to `"false"`.
- `form_full_pass_interval_days` — Number of days between scheduled full form passes. Optional; defaults to `"7"`.

> Note: All configuration values must be strings. Do not use numeric or boolean JSON types.

## Requirements file

The connector pins `requests==2.32.5` to ensure consistent behaviour. The `requests` library is pre-installed by the Connector SDK runtime; the pinned version locks it to the release validated with this connector.

```
requests==2.32.5
```

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector supports two authentication modes:

### OAuth 2.0 (SAML2 bearer assertion flow)

SAP SuccessFactors uses the `urn:ietf:params:oauth:grant-type:saml2-bearer` grant type. To set up OAuth:

1. In SAP SuccessFactors Admin Center, register an OAuth client application.
2. Upload an X.509 certificate for the client application.
3. Note the client ID and company ID.
4. Generate an RSA private key pair; provide the private key in `sf_private_key`.
5. Set `sf_client_id`, `sf_company_id`, `sf_user_id`, and `sf_private_key` in `configuration.json`.

The connector automatically refreshes the token before expiry (minimum 300 seconds remaining) and handles concurrent token renewal safely across parallel worker threads.

### Bearer token

If you manage token issuance externally, provide a pre-issued bearer token in `sf_access_token` and omit the four OAuth fields. The connector will use the token for the lifetime of the sync without attempting renewal.

## Pagination

Refer to `def build_entity_url()` and `def iter_records()` in `connector.py`.

All requests include `$paging=snapshot`, which instructs SAP to create a server-side consistent pagination snapshot before the first page is fetched. Without this parameter, concurrent inserts and deletes on the SAP side can cause skip-based pagination to silently skip or duplicate rows with no HTTP error.

Top-level entities use explicit `$skip`-based pagination ordered by primary key fields derived from `$metadata`. Entities whose key fields SAP marks as non-sortable fall back to server-driven `__next` link pagination.

For the 8 large time management entities, pagination is performed inside 30-day `lastModifiedDateTime` windows rather than across the full dataset in one pass. Each window is fetched, upserted, and checkpointed independently before the next window begins.

The form hierarchy uses `formDataId`-keyed parent pages with 10-form batches, fetched across 4 concurrent workers.

## Data handling

Refer to `def build_row()`, `def sync_entity()`, `def sync_entity_windowed()`, and `def sync_form_hierarchies()` in `connector.py`.

**Date and time normalization** — All `Edm.DateTime` and `Edm.DateTimeOffset` values in the SAP payload are converted from `/Date(milliseconds)/` or `/Date(milliseconds+offset)/` format to UTC ISO 8601 strings. Navigation link URIs (`__deferred`) and OData metadata objects (`__metadata`) are dropped from every row.

**Field completeness** — The connector requests all fields declared in `$metadata` for each entity. Fields absent from a record's payload are emitted as `NULL` so the destination schema is consistent across all rows.

**Effective-dated entities** — Entities whose OData key contains a date field matching `startDate`, `effectiveStartDate`, or `mdfSystemEffectiveStartDate` are queried with `fromDate=1900-01-01` and `toDate=9999-12-31` to extract the full history rather than only the current-effective slice.

**Incremental state** — Each entity's cursor is the maximum `lastModifiedDateTime` seen in the last completed sync, stored in connector state under a versioned key. An overlap of 60 seconds is applied to the cursor on each incremental run to guard against SAP save lag. The state version is bumped when a schema or query change requires a one-time full re-read.

**Windowed fetch** — On first load, the following 8 entities are fetched in 30-day `lastModifiedDateTime` windows from `2001-01-01` to the current time, with a checkpoint after each window: `EmployeeTime`, `EmployeeTimeSheet`, `EmployeeTimeSheetEntry`, `EmployeeTimeValuationResult`, `TimeAccount`, `TimeAccountDetail`, `TimeAccountSnapshot`, `TimeAccountSummary`. Subsequent incremental syncs use the normal cursor path.

**Form hierarchy** — Review and 360 forms are replicated through `FormHeader` using breadth-first navigation expansion. Incremental syncs scan `FormContent` for changed form IDs, classify them by form type in 200-ID lookup batches, and re-fetch only the changed forms. A configurable full pass re-fetches all forms on a time interval. Ratings and supplementary navigations (`overallFormRating`, `calculatedFormRating`, `overallAdjustedRating`, `officialRating`, `selfRatingComment`) are expanded inline and stored as JSON on the parent row. Rows that a form no longer contains are identified by their `form_fetched_at` timestamp rather than by truncation.

**Excluded fields** — Certain fields that cause SAP to silently suppress records from the response are excluded per entity. For `Photo`, `photoHash` and `personUUID` are excluded because requesting them causes SAP to omit rows that have null values for those fields.

**Entity filters** — The `User` entity is queried with `status ne 'fivetran_all_statuses_sentinel'` to force SAP to return all users regardless of status. Without this filter, SAP silently returns only active users by default.

## Error handling

Refer to `def raise_for_error()`, `def sync_entity()`, and `def update()` in `connector.py`.

**Per-entity failure isolation** — If an entity raises an exception during sync (HTTP error, unexpected payload shape, transient network failure), it is skipped for that run and retried from its last saved cursor on the next sync. The failure is logged to the `connector_sync_issues` table in the destination and surfaced as a dashboard warning if the number of failed entities exceeds 25% of the total.

**Rate limiting** — All outbound requests pass through a token-bucket rate limiter capped at 40 requests per second, matching the native SuccessFactors connector's limit. SAP silently drops records from pages when this threshold is exceeded without returning an HTTP error; the limiter prevents this.

**OAuth token refresh** — If the access token is within 300 seconds of expiry when a request is made, the connector refreshes the token before proceeding. Token renewal is serialised across parallel worker threads using a lock to prevent duplicate refresh attempts.

**Transient errors in form fetching** — Each 10-form hierarchy page is retried up to 4 times with exponential backoff on transient failures. If a page cannot be retrieved after all attempts, the affected forms are recorded as `FormNotRetrieved` in `connector_sync_issues` and retried on the next sync.

**Partial expansion failures** — If SAP cannot expand a navigation path for a subset of forms (for example, `metricLookupTables` on some 360 forms), the affected forms are recorded as `ExpansionOmitted` in `connector_sync_issues` and a dashboard warning is raised. The sync completes successfully for all other forms.

## Tables created

The connector creates one destination table per configured SAP SuccessFactors entity, with the table name derived from the entity name in snake_case. In addition, two system tables are always created:

### connector_sync_issues

Records per-entity and per-form failures that did not abort the sync, queryable in the destination.

| Column | Type | Description |
|---|---|---|
| `sync_started_at` | `UTC_DATETIME` | Timestamp when the sync that produced this issue began |
| `source_entity` | `STRING` | SAP OData entity name |
| `destination_table` | `STRING` | Destination table name |
| `error_type` | `STRING` | One of: `RuntimeError`, `FormNotRetrieved`, `ExpansionOmitted`, `PermissionBlocked` |
| `error_message` | `STRING` | Error detail, truncated to 2,000 characters |

### form_fetch_log

Records the most recent fetch timestamp for each form, used to identify child rows that a form no longer contains without truncating whole tables.

| Column | Type | Description |
|---|---|---|
| `form_data_id` | `STRING` | SAP form ID |
| `form_type` | `STRING` | `Review` or `360` |
| `form_subject_id` | `STRING` | Subject user ID |
| `last_fetched_at` | `STRING` | ISO 8601 timestamp of the most recent successful fetch |
| `fetch_mode` | `STRING` | `full` or `incremental` |

> Note: All other tables are created dynamically from the SAP tenant's `$metadata`. Column names, types, and primary keys are derived at runtime and will reflect the schema of your specific SuccessFactors instance.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
