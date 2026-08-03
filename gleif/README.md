# GLEIF Connector Example

## Connector overview

This connector syncs Legal Entity Identifier (LEI) reference data from the [GLEIF API](https://www.gleif.org/en/lei-data/gleif-api) into a Fivetran destination. The Global Legal Entity Identifier Foundation publishes the authoritative public register of LEI codes: a 20-character identifier assigned to every legally distinct entity that participates in a financial transaction, together with that entity's registered name, legal and headquarters addresses, jurisdiction, registration status, and any associated BIC and MIC codes.

The register holds over 3.3 million entities and is refreshed continuously, so the connector syncs incrementally on `registration.lastUpdateDate` and requires no credentials. Typical uses are counterparty and KYC reference data, entity resolution across internal systems that key on inconsistent company names, sanctions and regulatory reporting, and supplier or customer master enrichment.

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
fivetran init --template gleif
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

## Features

- Incremental sync keyed on `registration.lastUpdateDate`, so each run transfers only entities changed since the previous checkpoint.
- Cursor-based pagination, which allows a sync to traverse result sets larger than the API's page-number ceiling.
- Configurable record limit per sync, useful for bounded test runs against a register of this size.
- Retry with exponential backoff on transient transport and server errors, and immediate failure on client errors that a retry cannot fix.
- No credentials required. The GLEIF API is public.

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

- `initial_sync_start_date` – The lower bound of the first sync window, in `YYYY-MM-DDTHH:MM:SSZ` format. Used only when no previous state exists. Defaults to `2026-01-01T00:00:00Z`. Refer to `def validate_configuration(configuration: dict)`.
- `page_size` – Records requested per API call, between 1 and 200. Defaults to 200. The API rejects any larger value.
- `max_records_per_sync` – Approximate lower bound on records upserted in a single sync, where 0 means no limit. Defaults to 0. Once the limit is met the connector keeps reading until the current `lastUpdateDate` value changes, then stops on that boundary, so a run can overshoot the limit by up to the size of one timestamp group. Refer to `def update(configuration: dict, state: dict)`.

> Note: Do not add a `requirements.txt` file. This connector depends only on `requests`, which the Fivetran runtime provides.

## Authentication

The GLEIF API is public and requires no API key, token, or account. No authentication configuration is needed and the connector sends no credentials.

## Pagination

The connector uses cursor-based pagination. The first request of each sync window sets `page[cursor]=*`, and every subsequent request follows the `links.next` URL returned in the previous response. Refer to `def build_initial_url(start_timestamp, end_timestamp, page_size)`.

Cursor pagination is used rather than page numbers because the API rejects any request where `page[number]` multiplied by `page[size]` exceeds 10000. A page-number walk therefore caps a sync at 10000 records, which is fewer than a single busy day of updates to the register, and the truncation is silent. Cursor pagination has no such ceiling.

> Note: The final page of a window is returned with the `links.next` key still present and an empty `data` array. The connector terminates when the `next` link is absent rather than when a page is empty.

## Data handling

Each LEI record is flattened from its nested JSON:API structure into a single row in the `lei_record` table, keyed on the LEI itself. Refer to `def flatten_record(record: dict)`.

The `bic` and `mic` fields are arrays when populated and null otherwise. Both are flattened to comma-separated strings by `def join_codes(value)`, and an empty array is stored as null rather than an empty string. Address lines are joined into a single string by `def join_address_lines(address: dict)`.

Incremental state is a single `last_update_date` value, advanced to the newest `registration.lastUpdateDate` observed during the sync and checkpointed every 1000 records. The API's date filter requires both bounds and treats the lower bound as inclusive, so entities whose timestamp equals the stored cursor are returned again on the following sync. Delivery is therefore at-least-once, and the primary key makes the repeated upsert idempotent. A sync that returns no records re-checkpoints the existing cursor rather than resetting it.

Records that arrive without an LEI value are skipped and logged, because the LEI is the primary key.

> Note: When `max_records_per_sync` is set, the connector stops on a timestamp boundary rather than at an exact record count. Many entities share a single `lastUpdateDate` value, and stopping partway through such a group would checkpoint that group's own timestamp. Because the lower bound is inclusive, the next sync would reopen the window on the group it just left and stop in the same place, so a limit smaller than the group size would prevent the connector from ever advancing. Stopping on the boundary and checkpointing the first timestamp of the next group avoids this and delivers every record.

## Error handling

Refer to `def get_api_response(url: str)`.

- Transient failures – Connection errors, timeouts, and 5xx responses are retried up to three times with exponential backoff starting at two seconds.
- Client errors – A 4xx response other than 429 indicates a malformed request, such as an out-of-range page size or an incomplete date range. These fail immediately with the response body included, because a retry cannot change the outcome and only delays the error the operator needs to see.
- Rate limiting – A 429 response is treated as transient and retried with backoff.
- Configuration errors – Invalid values raise `ValueError` before any request is sent, so a malformed page size fails at the start of the sync rather than midway through it.

## Tables created

The connector creates one table.

`lei_record`

Primary key: `lei`

| Column | Type | Description |
| --- | --- | --- |
| lei | STRING | The 20-character Legal Entity Identifier. Primary key |
| legal_name | STRING | Registered legal name of the entity |
| legal_name_language | STRING | ISO language code of the legal name |
| entity_status | STRING | Entity status, for example ACTIVE or INACTIVE |
| entity_category | STRING | Entity category, for example GENERAL, FUND, or BRANCH |
| entity_sub_category | STRING | Entity sub-category where one applies |
| legal_jurisdiction | STRING | ISO code of the jurisdiction of formation |
| legal_form_id | STRING | Entity Legal Form code |
| legal_form_other | STRING | Free-text legal form when no ELF code applies |
| registered_as | STRING | Registration number in the local business register |
| registered_at_id | STRING | Identifier of the local business register |
| entity_creation_date | UTC_DATETIME | Date the entity was created |
| entity_expiration_date | UTC_DATETIME | Date the entity expired, where applicable |
| entity_expiration_reason | STRING | Reason the entity expired |
| successor_lei | STRING | LEI of the successor entity after a merger |
| successor_name | STRING | Name of the successor entity |
| associated_entity_lei | STRING | LEI of an associated entity, such as a fund manager |
| associated_entity_name | STRING | Name of the associated entity |
| legal_address_lines | STRING | Street lines of the legal address |
| legal_address_city | STRING | City of the legal address |
| legal_address_region | STRING | Region or state of the legal address |
| legal_address_country | STRING | ISO country code of the legal address |
| legal_address_postal_code | STRING | Postal code of the legal address |
| headquarters_address_lines | STRING | Street lines of the headquarters address |
| headquarters_address_city | STRING | City of the headquarters address |
| headquarters_address_region | STRING | Region or state of the headquarters address |
| headquarters_address_country | STRING | ISO country code of the headquarters address |
| headquarters_address_postal_code | STRING | Postal code of the headquarters address |
| registration_status | STRING | LEI registration status, for example ISSUED or LAPSED |
| initial_registration_date | UTC_DATETIME | Date the LEI was first issued |
| last_update_date | UTC_DATETIME | Date the LEI record was last updated. Drives the incremental sync |
| next_renewal_date | UTC_DATETIME | Date the LEI is next due for renewal |
| managing_lou | STRING | LEI of the Local Operating Unit managing this record |
| corroboration_level | STRING | Level of corroboration against the business register |
| validated_at_id | STRING | Identifier of the register used for validation |
| validated_as | STRING | Registration number used for validation |
| bic_codes | STRING | Comma-separated BIC codes mapped to this LEI |
| mic_codes | STRING | Comma-separated MIC codes mapped to this LEI |
| ocid | STRING | Open Corporates identifier mapped to this LEI |

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

The register holds over 3.3 million entities. A first sync with no `initial_sync_start_date` and no record limit therefore transfers a substantial volume, so set `initial_sync_start_date` to a recent date, or set `max_records_per_sync`, when testing.

Relationship data, such as direct and ultimate parent hierarchies, is exposed by the API under a separate `relationships` object and separate endpoints. This connector syncs entity attributes only.

The `bic` and `mic` fields are populated for a small minority of entities, mostly banks and trading venues. A sample of arbitrary records is likely to contain none of them, so both are stored as nullable columns.
