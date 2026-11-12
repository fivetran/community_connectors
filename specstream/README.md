---
name: SpecStream
description: Sync live web data from SpecStream (companies, local businesses, job posts, filings, tenders and grants), each value with the page it was read from
category: SaaS & APIs
---

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
- Retries a list read that fails part way through the response.
- Checkpoints after each list, so an interrupted sync resumes where it stopped.
- Holds one page of lists and one record at a time, so memory does not grow with the workspace or with the size of a list.

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

The `requirements.txt` file lists the one library the connector needs besides `requests`: `ijson`, which reads a list's records from the response one at a time.

```
ijson==3.6.0
```

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector sends the API key as a Bearer token (`Authorization: Bearer <key>`). To get a key:

1. Sign in at [specstream.ai](https://specstream.ai). New accounts get $10 of work free, no card.
2. Open **Agents & API** and create an API key. The connector only reads, so you can give the key a spending limit of $0.
3. Copy the key into `configuration.json`.

## Pagination

The lists endpoint (`GET /v1/workspace/collections`) returns up to 100 lists per page, newest first, with a `next_after` value while more pages remain. The connector syncs the lists of one page before it asks for the next, and stops at the first list created before the last completed sync. `GET /v1/workspace/collections/{id}` returns all the records of a list in one response, so the connector parses the response as a stream and upserts each record as it arrives, with a checkpoint every 1,000 records. Refer to `def fetch_lists_page(session, after)` and `def stream_list_records(session, list_id)`.

## Data handling

- Records SpecStream found carry one cell per column. Each cell becomes a row in `RECORD_VALUE` with its label, its displayed value, its raw value, the page it came from and when it was read. Refer to `def upsert_record(list_id, row)`.
- The `matches` and `open` columns are buttons in the SpecStream workspace, not facts, so they are not synced to `RECORD` or `RECORD_VALUE`.
- In `RECORD.values`, each value is keyed by its label. When two columns share a label, the later one is keyed by its column ID, so no value replaces another.
- A value SpecStream couldn't read is written as null.
- Rows of a list you imported carry the columns you provided, without a source page. Refer to `def upsert_imported_record(list_id, row)`.
- A list you made by combining lists keeps each column's values together; the connector writes the value the list shows. Refer to `def read_found_record(row)`.
- The connector writes a list's `LIST` row before its records, with `row_count` empty, and sets `row_count` when all the records are synced. An empty `row_count` marks a list whose sync was interrupted; the next sync completes it.
- The state holds `lists_created_through`, the creation time in UTC of the newest list of the last completed sync, and `list_ids_at_boundary`, the IDs of the lists synced at that exact time, so a list that shares the time is not skipped. While a sync runs, `pass_in_progress` holds the last list it finished, so an interrupted sync resumes there. Clear the state to re-sync everything.

## Error handling

- Refused API key: the sync fails at once with a message to create a new key under **Agents & API**.
- Rate limits and server errors: up to five attempts with exponential backoff, using `Retry-After` when SpecStream sends it, in seconds or as an HTTP date.
- Connection errors and timeouts: retried the same way. A list read that fails part way through the response is read again from its first record, up to five times.
- Any other error status: the sync fails with the status and the path.

Refer to `def send_request(session, path, params=None, stream=False)` and `def sync_list(session, summary, state)`.

## Tables created

- `LIST`: one row per list. Primary key `id`. Columns `name`, `project_id`, `imported`, `created_at` and `row_count` (empty while the list's records are being synced).
- `RECORD`: one row per record. Primary key `list_id`, `record_id`. `record_id` depends on the kind of list: for a record SpecStream found, it is the record's `entity_id`, which is unique within the list and does not change if the list is re-ordered; for a row of a list you imported, it is the row's `id`; for a record of a list you combined, it is the combined record's `id`. Columns `entity_id` (stable across lists for the same company, place, person or posting), `values` (JSON object of label to value) and `sources` (JSON array of the pages the record's values came from).
- `RECORD_VALUE`: one row per value. Primary key `list_id`, `record_id`, `column_id`. Columns `label`, `value`, `raw_value` (JSON), `source_url` and `observed_at`.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

For SpecStream questions, email support@specstream.ai or see the [API reference](https://specstream.ai/docs/api).
