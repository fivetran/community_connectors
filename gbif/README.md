# GBIF Occurrence Connector Example

## Connector overview

This connector syncs species occurrence records from the [GBIF (Global Biodiversity Information Facility) occurrence search API](https://techdocs.gbif.org/en/openapi/v1/occurrence) into a Fivetran destination. GBIF aggregates biodiversity data from thousands of institutions worldwide: each occurrence record is one observation or specimen of a species at a place and time, carrying its full taxonomic classification, coordinates, event date, and the dataset and institution that published it.

The API is public and requires no credentials. Because the full corpus holds several billion occurrences, the connector is designed for a bounded query: optionally filter by taxon and country, page through the result set, and resume where a previous run stopped. Typical uses are building a regional species checklist, tracking observations of a taxon over time, and assembling a biodiversity corpus for analysis.

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
fivetran init --template gbif
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

## Features

- Incremental, resumable backfill of a bounded occurrence query. The offset is the resume cursor, so each run continues where the last one stopped rather than restarting.
- Offset pagination that respects the GBIF deep-paging cap: the connector reads the total match count, warns when it exceeds the reachable limit, and never requests past it.
- Reserved SQL keywords in the API payload are renamed at the source, so the delivered schema creates cleanly on any warehouse.
- Configurable record limit per sync. Because the offset advances per record, the limit is a true ceiling: a run stops on an exact record and the next run resumes on the following one.
- Retry with exponential backoff on transient transport and server errors, and immediate failure on client errors that a retry cannot fix.
- No credentials required. The GBIF API is public.

## Configuration file

The connector requires no authentication. Every configuration value is optional: supply the filters you want to narrow the query, and the paging controls you want to override.

```json
{
  "page_size": "<YOUR_PAGE_SIZE>",
  "max_records_per_sync": "<YOUR_MAX_RECORDS_PER_SYNC>",
  "taxon_key": "<YOUR_TAXON_KEY>",
  "country": "<YOUR_COUNTRY_CODE>"
}
```

> Note: When submitting connector code as a community connector in the open-source [Community Connector repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

### Configuration parameters

- `page_size` – Occurrences requested per API call, between 1 and 300. Defaults to 300. The API caps any larger value at 300. Refer to `def validate_configuration(configuration: dict)`.
- `max_records_per_sync` – Upper bound on occurrences upserted in a single sync, where 0 means no limit. Defaults to 0. Because the offset advances per record, the run stops on the exact record that meets the limit and the next sync resumes on the following one.
- `taxon_key` – Optional GBIF taxon key to restrict occurrences to one taxon, for example `212` for birds (Aves). Leave empty for no taxon filter. Narrowing the query is how you sync a set larger than the deep-paging cap.
- `country` – Optional ISO 3166-1 alpha-2 country code to restrict occurrences to one country, for example `US`. Leave empty for no country filter.

> Note: Do not add a `requirements.txt` file. This connector depends only on `requests`, which the Fivetran runtime provides.

## Authentication

The GBIF API is public and requires no API key, token, or account. No authentication configuration is needed and the connector sends no credentials.

## Pagination

The connector uses offset pagination. Each request sets an `offset` and a `limit` (the page size), and the offset advances by the number of records actually consumed, so a run stopped mid-page by `max_records_per_sync` resumes on the exact next record rather than skipping the remainder of the page. Refer to `def build_url(offset, page_size, taxon_key, country)`.

> Note: GBIF caps deep pagination at `offset + limit <= 100000`. A request past the cap returns HTTP 400 rather than data. The connector reads the total match `count` from the first page, warns when it exceeds 100,000, stops at the cap, and never issues a request the API would reject. To sync a result set larger than the cap, narrow the query with `taxon_key` or `country` so it fits under it. The walk otherwise terminates when the API reports `endOfRecords`.

## Data handling

Each occurrence is flattened into a single row in the `occurrence` table, keyed on `gbif_id`. Refer to `def flatten_occurrence(record: dict)`.

The API returns the taxonomic rank fields `class` and `order` as top-level keys. Both are reserved SQL keywords that fail an unquoted `CREATE TABLE` on most warehouses, so they are delivered as the columns `taxon_class` and `taxon_order`. The numeric `key` field duplicates `gbifID` and is itself a reserved word, so it is dropped in favour of the string `gbif_id` primary key.

The `recordedBy` field is usually a string but is a list in some datasets. It is coerced to a single delimited string by `def as_text(value)` so a list never lands stringified in a scalar column. Occurrences that arrive without a `gbifID` are skipped and logged, because that value is the primary key, and the offset still advances past them so the next sync does not re-read them.

Incremental state is the offset into the bounded query. GBIF occurrence search exposes no stable modified-time ordering — `lastInterpreted` is rewritten whenever a record is reprocessed — so a time cursor would skip or duplicate records. The offset is a reliable resume point as long as the result ordering is stable for the duration of the backfill, which GBIF guarantees within the deep-paging cap. A sync that returns no records re-checkpoints the existing offset rather than resetting it. Delivery is at-least-once and the primary key makes the repeated upsert idempotent.

## Error handling

Refer to `def get_api_response(url: str)`.

- Transient failures – Connection errors, timeouts, and 5xx responses are retried up to three times with exponential backoff starting at two seconds.
- Client errors – A 4xx response other than 429 indicates a malformed request, such as an offset past the deep-paging cap. These fail immediately with the response body included, because a retry cannot change the outcome and only delays the error the operator needs to see.
- Rate limiting – A 429 response is treated as transient and retried with backoff.
- Configuration errors – Invalid values raise `ValueError` before any request is sent, so a malformed page size, taxon key, or country code fails at the start of the sync rather than midway through it.

## Tables created

The connector creates one table.

`OCCURRENCE`

Primary key: `gbif_id`

| Column | Type | Description |
| --- | --- | --- |
| gbif_id | STRING | The stable GBIF occurrence identifier. Primary key. Derived from the API field `gbifID` |
| dataset_key | STRING | Key of the GBIF dataset that published the record |
| publishing_country | STRING | Country of the publishing organization |
| basis_of_record | STRING | How the record was observed, for example PRESERVED_SPECIMEN or HUMAN_OBSERVATION |
| occurrence_status | STRING | Whether the taxon was present or absent |
| scientific_name | STRING | Scientific name as interpreted by GBIF |
| accepted_scientific_name | STRING | Currently accepted scientific name for the taxon |
| taxon_key | LONG | GBIF taxon key for the interpreted name |
| kingdom | STRING | Taxonomic kingdom |
| phylum | STRING | Taxonomic phylum |
| taxon_class | STRING | Taxonomic class. Renamed from the reserved API field `class` |
| taxon_order | STRING | Taxonomic order. Renamed from the reserved API field `order` |
| family | STRING | Taxonomic family |
| genus | STRING | Taxonomic genus |
| species | STRING | Species name |
| taxon_rank | STRING | Rank of the interpreted taxon, for example SPECIES or GENUS |
| taxonomic_status | STRING | Taxonomic status, for example ACCEPTED or SYNONYM |
| country_code | STRING | ISO country code where the occurrence was recorded |
| country | STRING | Country name where the occurrence was recorded |
| locality | STRING | Free-text locality description |
| decimal_latitude | DOUBLE | Latitude in decimal degrees, where provided |
| decimal_longitude | DOUBLE | Longitude in decimal degrees, where provided |
| event_date | STRING | Date or date range the occurrence was recorded, as reported |
| event_year | INT | Year of the occurrence event |
| event_month | INT | Month of the occurrence event |
| event_day | INT | Day of the occurrence event |
| recorded_by | STRING | Who recorded the occurrence. A list is delimited into a single string |
| institution_code | STRING | Code of the holding institution |
| catalog_number | STRING | Catalog number within the institution |
| last_interpreted | STRING | Timestamp GBIF last interpreted the record |
| license | STRING | License under which the record is published |

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

GBIF holds several billion occurrences, and offset pagination can reach only the first 100,000 records of any single query. For anything larger, narrow the query with `taxon_key` or `country`, or run several connections each scoped to a different filter. For a complete bulk export of a very large query, GBIF's asynchronous [Occurrence Download API](https://techdocs.gbif.org/en/openapi/v1/occurrence#/Searching%20occurrences/searchOccurrence) is the better fit; this connector targets bounded, incrementally-synced queries.
