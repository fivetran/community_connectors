# CMS Hospital Compare Connector Example

## Connector overview

This connector syncs five hospital datasets from the public CMS Provider Data Catalog (Care Compare) datastore API (`https://data.cms.gov/provider-data/api/1`): Hospital General Information, Timely and Effective Care, Unplanned Hospital Visits for hospitals and for the nation, and the Hospital Readmissions Reduction Program. Together they give facility-level quality, emergency department throughput and readmission measures for every Medicare-certified hospital, with national rates beside them, so a health system can compare its own hospitals against real peers.

CMS publishes each dataset as a full-replacement snapshot on a release cycle (roughly quarterly). The connector loads each release once and keeps it: rows are keyed on the dataset's natural key plus the release version, and the `dataset_release` table records every release that loaded completely.

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
fivetran init --template cms_hospital_compare/connector
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- Five CMS hospital datasets, each written to its own table, plus a `dataset_release` table recording each loaded release.
- Release detection from the catalog metadata: a dataset whose current release is already loaded costs one metadata call and is skipped.
- Offset paging at up to 1,500 rows per call, sorted on the natural key so pages are stable, with a configurable pause between calls.
- A checkpoint after every page, so an interrupted load of a large dataset resumes at the next page.
- Release history: every release lands beside the previous ones, so measures can be compared release over release.
- A setup form with one field per configuration key, and a setup test that calls the API (refer to `def configuration_form()` and `def verify_api_access(configuration)`).

## Configuration file

The connector reads the following parameters from `configuration.json`. Every value is a string, and both are optional.

```json
{
  "page_size": "<PAGE_SIZE>",
  "request_delay_seconds": "<REQUEST_DELAY_SECONDS>"
}
```

- `page_size` is the number of rows requested per API call, from 1 to 1,500; defaults to 1,500, the API maximum.
- `request_delay_seconds` is the pause between page requests, a finite number from 0 to 10 (`nan` and `inf` are rejected); defaults to 0.2. The API publishes no rate limit, so the pause is a courtesy, not a contract.

A placeholder or empty value uses the default, so the `configuration.json` in this directory runs as shipped. It holds no credentials.

> Note: When submitting connector code as a community connector in the open-source [Community Connector repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector has no `requirements.txt`: it needs only `requests` and the Connector SDK.

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

None. The CMS Provider Data Catalog API is public and needs no key.

## Pagination

The datastore query endpoint (`/datastore/query/<dataset_id>/0`) pages with `limit` and `offset`; `limit` above 1,500 is rejected with HTTP 400. Each request asks for the total row count and sorts on the dataset's natural key, so a page boundary does not move between requests. The offset advances by the number of records the page returned, and paging stops on a short page or when the offset reaches the count. Refer to `def fetch_page(session, dataset, offset, page_size)` and `def page_release(session, dataset, release, paging, datasets_state, page_size, delay_seconds, totals)`.

## Data handling

- Release detection: the catalog metadata (`/metastore/schemas/dataset/items/<dataset_id>?show-reference-ids`) carries the data file's version, a Unix epoch that changes whenever CMS replaces the file. It is the release identifier, written to every row as `cms_release_version`. When no version is published, the catalog's `%modified` timestamp is used instead. Refer to `def get_release(session, dataset_id)`.
- State holds, per dataset, the release version, the offset reached and whether the release finished loading. A finished release is skipped; a partly loaded one resumes at its offset; a new release starts again at offset 0. A partly loaded release also keeps its paging evidence, so the guards under Error handling hold across a resume: a fingerprint of the release's first page and of the last page read, the last natural key, and the offset of any short page. A partly loaded release saved without that evidence, by an earlier version of this connector, is reloaded from offset 0. Refer to `def sync_dataset(session, dataset, datasets_state, page_size, delay_seconds, totals)`, `def resolve_start(table, release, saved)` and `def save_page_progress(dataset_id, release, paging, page_size, datasets_state)`.
- History, not replacement: the primary key includes `cms_release_version`, so a new release adds rows and never deletes the previous release's. To read current values, join to the latest `dataset_release` row per dataset. A release that CMS replaced while it was being paged is not recorded in `dataset_release`, and the next sync loads the new one. The rows already written from that partial release stay in the raw tables, orphaned: no `dataset_release` row points to them, so a join to `dataset_release` leaves them out.
- Values are written as text, exactly as the API returns them, including sentinel strings such as `Not Available`, `Not Applicable`, `Too Few to Report` and banded text (`very high`). Dates such as `start_date` stay in the API's `MM/DD/YYYY` form. Typing belongs in the warehouse.
- Column names are the API's, lowercase, with two renames: `sample` becomes `sample_size` (a reserved word in several SQL engines) and `_condition` becomes `measure_condition`. Refer to `def get_column_name(source_name)`.
- A record with a blank natural key value is skipped and counted; a natural key repeated within one release is counted and the last record kept. Rows are sorted on the natural key, so a repeat is found by comparing each key with the one before it, and memory stays flat however large the release is. Both counts are logged and reported to the dashboard with `op.warning`. Refer to `def upsert_page_rows(table, rows, page_keys, paging, totals)`.

## Error handling

- HTTP 429, 500, 502, 503 and 504, connection errors, timeouts and a body cut off mid-transfer are retried with exponential backoff, four attempts in all, with each retry logged, then the sync fails naming the dataset and offset. Any other request error, such as too many redirects, fails the sync at once, since a retry cannot fix it. Refer to `def get_with_retries(session, url, params, label)` and `def wait_before_retry(label, attempt, reason)`.
- Any other non-success status, a body that is not a JSON object, or a page without a `results` list or a row count fails the sync immediately naming the dataset and offset. Refer to `def parse_json_object(response, label)` and `def fetch_page(session, dataset, offset, page_size)`.
- Catalog metadata with neither a file version nor a modified timestamp fails the sync naming the dataset; a catalog entry of an unexpected shape is skipped. Refer to `def get_release(session, dataset_id)` and `def get_file_version(distribution)`.
- A page that repeats the release's first page or the page before it fails the sync: the API is not applying the offset, and the release would otherwise be recorded as complete with repeated rows. Pages are compared by a fingerprint of their first two distinct natural keys, saved in state, so the first page after a resume is checked too. Only pages with at least two distinct natural keys are compared, so a page size of 1 or a run of rows with a blank key does not trip it. For the same reason the guard is off for a release whose first page has fewer than two distinct keys, which the default page size of 1,500 makes unlikely. Refer to `def check_page_signature(table, page_keys, paging)` and `def compute_page_signature(page_keys)`.
- A dataset that keeps returning full pages more than 10 pages past the row count its first page reported fails the sync, as a guard against a row count that keeps growing. The limit scales with `page_size`, so a small page size does not trip it. Refer to `def page_release(session, dataset, release, paging, datasets_state, page_size, delay_seconds, totals)`.
- A short page before the reported row count leaves the release incomplete, with a warning on the dashboard, saves that offset in state as evidence of the short page, and the next sync resumes there. If that resume finds no rows at the saved offset, and the release has at least one row, the row count was overstated: the release is recorded in `dataset_release` with the rows CMS returned, so `row_count` is below `api_row_count`, and the dashboard shows a warning. Without that, a release whose count CMS overstates would never complete. A sync killed after a full page leaves no such evidence, so one empty page on the resume only records the short page, and the release completes on the next sync only if that page is still empty. Refer to `def finish_release(session, dataset, release, paging, is_resuming, saved, datasets_state, totals)` and `def accept_short_release(table, paging, is_resuming, saved, totals)`.
- Configuration errors are raised by `def validate_configuration(configuration)` before any API call, and reported at setup time by the setup test.

## Tables created

Every dataset table has the API's columns, written as STRING, plus the two release columns. The tables below list the declared columns; the other columns are created as the API names them.

### hospital

The `hospital` table holds Hospital General Information (`xubh-q36u`): one row per hospital per release, with type, ownership, emergency services, the overall star rating and the measure-group counts. Primary key: `facility_id`, `cms_release_version`.

| Column | Type | Description | Primary key |
|---|---|---|---|
| `facility_id` | STRING | CMS Certification Number (CCN) | Yes |
| `cms_release_version` | STRING | Release the row belongs to | Yes |
| `cms_released` | STRING | Catalog release date of that release, `YYYY-MM-DD` | No |

### timely_effective_care

The `timely_effective_care` table holds Timely and Effective Care for hospitals (`yv7e-xc69`): one row per hospital per measure per release, including the emergency department measures `EDV` (volume band), `OP_18a` to `OP_18d` (median time in the department), `OP_22` and `OP_23`. Primary key: `facility_id`, `measure_id`, `cms_release_version`.

| Column | Type | Description | Primary key |
|---|---|---|---|
| `facility_id` | STRING | CMS Certification Number (CCN) | Yes |
| `measure_id` | STRING | CMS measure identifier | Yes |
| `cms_release_version` | STRING | Release the row belongs to | Yes |
| `cms_released` | STRING | Catalog release date of that release, `YYYY-MM-DD` | No |

### unplanned_hospital_visit

The `unplanned_hospital_visit` table holds Unplanned Hospital Visits for hospitals (`632h-zaca`): 30-day readmission and excess-days measures with the comparison to the national rate. Primary key: `facility_id`, `measure_id`, `cms_release_version`.

| Column | Type | Description | Primary key |
|---|---|---|---|
| `facility_id` | STRING | CMS Certification Number (CCN) | Yes |
| `measure_id` | STRING | CMS measure identifier, for example `READM_30_HF` | Yes |
| `cms_release_version` | STRING | Release the row belongs to | Yes |
| `cms_released` | STRING | Catalog release date of that release, `YYYY-MM-DD` | No |

### unplanned_hospital_visit_national

The `unplanned_hospital_visit_national` table holds the national rates for the same measures (`cvcs-xecj`), with the number of hospitals better than, no different from and worse than the nation. Primary key: `measure_id`, `cms_release_version`.

| Column | Type | Description | Primary key |
|---|---|---|---|
| `measure_id` | STRING | CMS measure identifier | Yes |
| `cms_release_version` | STRING | Release the row belongs to | Yes |
| `cms_released` | STRING | Catalog release date of that release, `YYYY-MM-DD` | No |

### hrrp

The `hrrp` table holds the Hospital Readmissions Reduction Program (`9n3s-kdb3`): one row per hospital per condition with discharges, predicted and expected readmission rates and the excess readmission ratio. Primary key: `facility_id`, `measure_name`, `cms_release_version`.

| Column | Type | Description | Primary key |
|---|---|---|---|
| `facility_id` | STRING | CMS Certification Number (CCN) | Yes |
| `measure_name` | STRING | HRRP condition, for example `READM-30-HF-HRRP` | Yes |
| `cms_release_version` | STRING | Release the row belongs to | Yes |
| `cms_released` | STRING | Catalog release date of that release, `YYYY-MM-DD` | No |

### dataset_release

The `dataset_release` table has one row per dataset per release that loaded completely. Primary key: `dataset_id`, `release_version`.

| Column | Type | Description | Primary key |
|---|---|---|---|
| `dataset_id` | STRING | CMS dataset identifier | Yes |
| `release_version` | STRING | Release identifier, equal to `cms_release_version` on that release's rows | Yes |
| `table_name` | STRING | Destination table the dataset lands in | No |
| `cms_released` | STRING | Catalog release date, `YYYY-MM-DD` | No |
| `cms_modified` | STRING | Catalog modified timestamp | No |
| `row_count` | INT | Rows read from the API for this release | No |
| `api_row_count` | INT | Row count the API reported for the release | No |
| `loaded_at` | UTC_DATETIME | When the release finished loading | No |

## Additional considerations

The optional context pack in [`../context_pack/`](../context_pack/) gives AI agents DuckDB views and agent instructions for this connector's tables: the latest complete CMS release only, CMS text values cast to numbers and dates, and four checked questions with their SQL and answers. The connector does not use it.

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
