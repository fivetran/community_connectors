# SpecStream Connector Example

## Connector overview

This connector syncs your [SpecStream](https://specstream.ai) data into your Fivetran destination. SpecStream is a live web data engine: you ask in plain words for companies, local businesses, people at companies, job posts, SEC filings, tenders or grants, and it returns every match as structured data, each value with the page it was read from. Each request becomes a list in your SpecStream workspace.

The connector reads every list your API key can open and writes three tables: the lists, their records, and one row per value with its source page and the time it was read. Use it to join live web data with your CRM or product data in the warehouse, track new matches from scheduled requests over time, or audit where each value came from.

The connector only reads. It never starts a request or spends from your SpecStream balance.

## Accreditation

This example was contributed by [SpecStream](https://specstream.ai).

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
fivetran init --template specstream
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- Syncs every SpecStream list your API key can open: lists from requests, scheduled runs, lists you imported and lists you combined.
- Writes each value with the page it was read from and when it was read.
- Incremental syncs read only the lists created since the last sync. A SpecStream schedule delivers new and changed records as a new list, so they arrive on the next sync.
- Retries rate limits and server errors with exponential backoff and honors `Retry-After`.
- Checkpoints after each list, so an interrupted sync resumes where it stopped.

## Configuration file

```json
{
  "api_key": "<YOUR_SPECSTREAM_API_KEY>"
}
```

Configuration parameters:

- `api_key` (required): a SpecStream API key.

> Note: When submitting connector code as a community connector in the open-source [Community Connector repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The connector uses only `requests`, which is pre-installed in the Connector SDK runtime, so it has no `requirements.txt` file.

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector sends the API key as a Bearer token (`Authorization: Bearer <key>`). To get a key:

1. Sign in at [specstream.ai](https://specstream.ai). New accounts get $10 of work free, no card.
2. Open **Agents & API** and create an API key. The connector only reads, so you can give the key a spending limit of $0.
3. Copy the key into `configuration.json`.

## Pagination

The lists endpoint (`GET /v1/workspace/collections`) returns up to 100 lists per page, newest first, with a `next_after` value while more pages remain. The connector follows `next_after` and stops at the first list it has already synced. Each list's records come from `GET /v1/workspace/collections/{id}`. Refer to `def fetch_lists_created_after(session, synced_through)` and `def sync_list(session, summary)`.

## Data handling

- Records SpecStream found carry one cell per column. Each cell becomes a row in `RECORD_VALUE` with its label, its displayed value, its raw value, the page it came from and when it was read. Refer to `def upsert_record(list_id, row)`.
- A value SpecStream couldn't read is written as null.
- Rows of a list you imported carry the columns you provided, without a source page. Refer to `def upsert_imported_record(list_id, row)`.
- A list you made by combining lists keeps each column's values together; the connector writes the value the list shows. Refer to `def read_found_record(row)`.
- The state holds `lists_created_through`, the creation time of the newest synced list. Clear it to re-sync everything.

## Error handling

- Refused API key: the sync fails at once with a message to create a new key under **Agents & API**.
- Rate limits and server errors: up to five attempts with exponential backoff, using `Retry-After` when SpecStream sends it.
- Connection errors and timeouts: retried the same way.

Refer to `def get_json(session, path, params=None)`.

## Tables created

- `LIST`: one row per list. Primary key `id`. Columns `name`, `project_id`, `imported`, `created_at` and `row_count`.
- `RECORD`: one row per record. Primary key `list_id`, `record_id`. Columns `entity_id` (stable across lists for the same company, place, person or posting), `values` (JSON object of label to value) and `sources` (JSON array of the pages the record's values came from).
- `RECORD_VALUE`: one row per value. Primary key `list_id`, `record_id`, `column_id`. Columns `label`, `value`, `raw_value` (JSON), `source_url` and `observed_at`.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

For SpecStream questions, email support@specstream.ai or see the [API reference](https://specstream.ai/docs/api).
