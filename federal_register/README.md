# Federal Register Connector Example

## Connector overview

This connector syncs documents from the [Federal Register API](https://www.federalregister.gov/developers/documentation/api/v1) into a Fivetran destination. The Federal Register is the official daily journal of the United States government, publishing rules, proposed rules, notices, and presidential documents from federal agencies. Each document carries a stable document number, a publication date, a document type, the issuing agencies, and links to the HTML and PDF renderings.

The API is public and requires no credentials. The connector syncs incrementally on `publication_date`, oldest first, and requires no authentication. Typical uses are regulatory change monitoring, agency rulemaking analysis, compliance tracking, and building a searchable corpus of federal notices.

## Requirements

- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```
fivetran init --template federal_register
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

## Features

- Incremental sync keyed on a compound `(publication_date, document_number)` cursor, so each run transfers only documents published after the previous checkpoint.
- Cursor-based pagination that follows the API's own `next_page_url`, which carries a stable `search_after` cursor for deep pagination.
- Configurable record limit per sync. Because the cursor is compound, the limit is a true ceiling: a run stops on an exact document and the next run resumes on the following one.
- Retry with exponential backoff on transient transport and server errors, and immediate failure on client errors that a retry cannot fix.
- No credentials required. The Federal Register API is public.

## Configuration file

The connector requires no authentication, so every configuration value is optional and has a documented default. Supply only the values you want to override.

```json
{
  "initial_sync_start_date": "<YOUR_INITIAL_SYNC_START_DATE>",
  "page_size": "<YOUR_PAGE_SIZE>",
  "max_records_per_sync": "<YOUR_MAX_RECORDS_PER_SYNC>"
}
```

> Note: When submitting connector code as a community connector in the open-source [Community Connector repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

### Configuration parameters

- `initial_sync_start_date` – The inclusive lower bound of the first sync, in `YYYY-MM-DD` format. Used only when no previous state exists. Defaults to `2024-01-01`. Refer to `def validate_configuration(configuration: dict)`.
- `page_size` – Documents requested per API call, between 1 and 1000. Defaults to 100. The API rejects any larger value.
- `max_records_per_sync` – Upper bound on documents upserted in a single sync, where 0 means no limit. Defaults to 0. Because the cursor is compound, the run stops on the exact record that meets the limit and the next sync resumes on the following one. Refer to `def update(configuration: dict, state: dict)`.

> Note: Do not add a `requirements.txt` file. This connector depends only on `requests`, which the Fivetran runtime provides.

## Authentication

The Federal Register API is public and requires no API key, token, or account. No authentication configuration is needed and the connector sends no credentials.

## Pagination

The connector uses cursor-based pagination. The first request of each sync sets the `publication_date` lower bound and `order=oldest`, and every subsequent request follows the `next_page_url` returned in the previous response. That URL carries a `search_after` cursor encoding the publication date and document number of the last row on the page, which is what makes deep pagination stable. Refer to `def build_initial_url(start_date, page_size)`.

> Note: The walk terminates when `next_page_url` is absent rather than when a page is empty.

## Data handling

Each document is flattened into a single row in the `document` table, keyed on `document_number`. Refer to `def flatten_document(record: dict)`.

The `agencies` field is a list of objects. It is flattened into two delimited strings: `agency_ids` by `def join_agency_ids(agencies)` and `agency_names` by `def join_agency_names(agencies)`. An empty agency list is stored as null rather than an empty string, and the cleaned agency name is preferred over its raw form.

The API field `type` is delivered as the column `document_type`. The name `type` reads ambiguously as a column and is refused by some warehouses as an identifier, so it is renamed at the source.

Incremental state is a compound cursor: the `publication_date` plus the `document_number` within that date. The API's `publication_date` filter is inclusive and day-granular, and a single day can carry dozens of documents, so a date-only cursor would either re-fetch a whole day on every sync or skip records. The document number is the same value the API sorts on within a day, so pairing it with the date lets a bounded run stop between two documents on the same day and resume exactly there. On resume, the inclusive lower bound re-serves the last synced day and the connector skips every document at or before the stored cursor, which is exactly the set already delivered. Delivery is therefore at-least-once, and the primary key makes the repeated upsert idempotent. A sync that returns no records re-checkpoints the existing cursor rather than resetting it.

Documents that arrive without a document number are skipped and logged, because the document number is the primary key.

## Error handling

Refer to `def get_api_response(url: str)`.

- Transient failures – Connection errors, timeouts, and 5xx responses are retried up to three times with exponential backoff starting at two seconds.
- Client errors – A 4xx response other than 429 indicates a malformed request, such as an out-of-range page size. These fail immediately with the response body included, because a retry cannot change the outcome and only delays the error the operator needs to see.
- Rate limiting – A 429 response is treated as transient and retried with backoff.
- Configuration errors – Invalid values raise `ValueError` before any request is sent, so a malformed page size fails at the start of the sync rather than midway through it.

## Tables created

The connector creates one table.

`document`

Primary key: `document_number`

| Column | Type | Description |
| --- | --- | --- |
| document_number | STRING | The Federal Register document number. Primary key |
| publication_date | NAIVE_DATE | Date the document was published. Drives the incremental sync |
| document_type | STRING | Document type, for example Rule, Proposed Rule, Notice, or Presidential Document. Renamed from the API field `type` |
| title | STRING | Document title |
| abstract | STRING | Document abstract, where one is provided |
| excerpts | STRING | Short text excerpts from the document, where provided |
| html_url | STRING | URL of the document on federalregister.gov |
| pdf_url | STRING | URL of the document PDF on govinfo.gov |
| public_inspection_pdf_url | STRING | URL of the public inspection PDF, where available |
| agency_ids | STRING | Comma-separated Federal Register agency ids for the document |
| agency_names | STRING | Semicolon-separated agency names for the document |

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

The Federal Register holds documents back to 1994. A first sync with an early `initial_sync_start_date` and no record limit therefore transfers a substantial volume, so set a recent `initial_sync_start_date`, or set `max_records_per_sync`, when testing.

The API exposes richer document detail through per-document endpoints, including full text, regulatory dockets, and CFR references. This connector syncs the document index fields only.
