# Semantic Scholar Academic Graph Connector Example

## Connector overview

This connector syncs academic paper records from the Semantic Scholar Academic Graph API and optionally enriches each paper with Snowflake Cortex AI analysis during ingestion. For every paper, Cortex assesses:

- Research impact (high, medium, or low)
- Technical domain (NLP, CV, ML, Systems, Theory, Biology, Chemistry, Physics, Medicine, Social, or Other)
- Accessibility level (beginner, intermediate, or advanced)

Papers are fetched via the `/paper/search/bulk` endpoint using a continuation-token cursor, which has no offset cap and is the correct endpoint for exhaustively walking a large result set (the alternative `/paper/search` endpoint enforces an `offset + limit` ceiling and is not suitable for bulk retrieval). Authors are delivered to a separate `paper_authors` join table, and nested `externalIds` / `openAccessPdf` objects are flattened to scalar columns on the `papers` table.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Fetches papers from the Semantic Scholar Graph API bulk search endpoint using token-based cursor pagination (no offset cap, no page ever revisited or skipped)
- Flattens nested `externalIds` (DOI, ArXiv, MAG, PubMed, DBLP, ACL, CorpusId) and `openAccessPdf` (url, status) objects to scalar columns
- Extracts authors to a separate `paper_authors` join table, skipping authors the API returns with a null `authorId` (a null primary key column fails at the destination)
- Renames the API's `year` and `abstract` fields to `publication_year` and `paper_abstract` defensively — `year` collides with temporal keywords/functions in several SQL engines even where not strictly reserved
- Retries transient HTTP errors (429, 500, 502, 503, 504) with exponential backoff; supports an optional `x-api-key` header for the authenticated (higher rate limit) pool
- Optionally enriches each paper with Snowflake Cortex AI analysis, bounded both per sync (`max_enrichments`) and per UTC day (`max_enrichments_per_day`) so that a frequent schedule cannot multiply the spend
- Tracks a within-page `page_offset` in state so a per-sync record cap never discards unconsumed records from an already-fetched page (see Pagination below)

## Configuration file

Create a `configuration.json` file with the following parameters:

```json
{
    "search_query": "<YOUR_SEARCH_QUERY>",
    "api_key": "<OPTIONAL_SEMANTIC_SCHOLAR_API_KEY>",
    "max_records_per_sync": "<MAX_RECORDS_PER_SYNC>",
    "batch_size": "<BATCH_SIZE>",
    "enable_cortex": "<TRUE_OR_FALSE>",
    "snowflake_account": "<YOUR_SNOWFLAKE_ACCOUNT_HOSTNAME>",
    "snowflake_pat_token": "<YOUR_SNOWFLAKE_PAT>",
    "cortex_model": "<CORTEX_MODEL_NAME>",
    "cortex_timeout": "<CORTEX_TIMEOUT_SECONDS>",
    "max_enrichments": "<MAX_CORTEX_ENRICHMENTS>",
    "max_enrichments_per_day": "<MAX_CORTEX_ENRICHMENTS_PER_DAY>"
}
```

- `search_query` is the free-text query passed to the Semantic Scholar bulk search endpoint, for example `"data engineering pipelines"`. Required, must be non-empty.
- `api_key` is an optional Semantic Scholar API key sent as the `x-api-key` header. Semantic Scholar's unauthenticated pool is shared across all users and returns HTTP 429 aggressively (live-confirmed); a key raises the effective rate limit. Leave empty for unauthenticated access.
- `max_records_per_sync` caps how many papers this connector processes in a single sync run. Defaults to 200.
- `batch_size` controls how many records are consumed (and checkpointed) at a time from a fetched page. It does **not** control the API's page size — see Pagination. Defaults to 50.
- `enable_cortex` toggles Snowflake Cortex enrichment on or off. Must be exactly `"true"` or `"false"`.
- `snowflake_account` is your Snowflake account hostname, ending in `snowflakecomputing.com`, with no `http://`/`https://` scheme prefix. Required only when `enable_cortex` is `"true"`.
- `snowflake_pat_token` is a Snowflake Programmatic Access Token used as a Bearer token for the Cortex Inference API. Required only when `enable_cortex` is `"true"`.
- `cortex_model` selects the Cortex LLM model. Must be one of `claude-sonnet-5`, `claude-sonnet-4-6`, `mistral-large2`, `llama3.1-70b`, `llama3.1-8b`. Defaults to `claude-sonnet-5`.
- `cortex_timeout` is the timeout in seconds for each Cortex API call. Defaults to 30.
- `max_enrichments` caps how many papers receive Cortex enrichment in a single sync, independent of `max_records_per_sync`. Defaults to 3.
- `max_enrichments_per_day` caps how many papers receive Cortex enrichment across **all** syncs in a UTC day, tracked in connector state. Defaults to 15.

The two caps do different jobs, which is why both exist. `max_enrichments` bounds one sync; on a 15-minute schedule that still permits 96 syncs a day, so a per-sync cap alone bounds nothing that matters to a bill. `max_enrichments_per_day` is the ceiling that actually holds. Raise them deliberately: every enriched paper is one Cortex inference call, and the defaults are set to demonstrate the feature rather than to enrich a corpus. Records continue to sync normally once either cap is reached — enrichment stops, ingestion does not.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Authentication

The Semantic Scholar Graph API does not require authentication for basic use. An optional API key, sent as the `x-api-key` header, raises the caller's rate limit above the shared unauthenticated pool. Keys can be requested at the [Semantic Scholar API portal](https://www.semanticscholar.org/product/api#api-key-form).

Snowflake Cortex authentication uses a Personal Access Token (PAT) sent as a Bearer token in the `Authorization` header against `POST /api/v2/cortex/inference:complete`. This is required only when `enable_cortex` is `"true"`.

## Pagination

The connector uses the `/paper/search/bulk` endpoint with a continuation-token cursor rather than the offset-based `/paper/search` endpoint, because `/paper/search` enforces an `offset + limit` ceiling that makes it unsuitable for exhaustively walking a large, unbounded result set.

Live API profiling (2026-08-21) found that `/paper/search/bulk` **ignores any `limit` parameter it is sent** and always returns up to 1,000 records per page — confirmed by requesting `limit=5` and `limit=20` and receiving 1,000 records both times. Because `max_records_per_sync` and `batch_size` are typically far smaller than 1,000, the connector cannot assume a fetched page is fully consumed in one pass.

To avoid silently discarding the unconsumed remainder of a page (the same class of bug as an offset-pagination off-by-N, expressed with a token cursor and a server-fixed page size instead), the connector tracks two state keys:

- `bulk_token`: the continuation token to fetch the *next* page. It only advances once every record on the current page has been consumed — never on a partial page.
- `page_offset`: how many records of the *current* page (identified by `bulk_token`) have already been consumed.

When a sync run stops mid-page (`max_records_per_sync` reached before the page is drained), `bulk_token` is left unchanged and `page_offset` is checkpointed. The next sync re-fetches the same page via the same token and resumes from `page_offset`, so no record is ever skipped or duplicated. Once a page is fully drained, `bulk_token` advances to the API's next token and `page_offset` resets to 0. Traversal is complete when the API returns no further token after a page is fully drained.

## Data handling

The `def update(configuration, state)` function calls `def fetch_bulk_page(session, query, token)` to retrieve one page (up to 1,000 records) from the bulk search endpoint, with exponential-backoff retry on transient errors. Each raw paper record is flattened by `def flatten_paper(record, enrichment)`, which renames `year` → `publication_year` and `abstract` → `paper_abstract`, flattens the nested `externalIds` object to seven scalar `external_id_*` columns, flattens the nested `openAccessPdf` object to `open_access_pdf_url` / `open_access_pdf_status`, and serializes `publicationTypes` to a JSON string. Authors are extracted separately by `def flatten_authors(record)` into `paper_authors` rows, skipping any author the API returns with a null `authorId` — a null value in a primary key column fails at the destination, confirmed on an early debug run.

When `enable_cortex` is `"true"`, `def enrich_paper(session, configuration, record)` calls `def call_cortex_enrich(...)` for each paper up to the `max_enrichments` cap, asking for research impact, technical domain, and accessibility level in a single Cortex call per paper to minimize API calls. The Cortex response is parsed as Server-Sent Events by `def parse_cortex_streaming_response(response)`, and the JSON payload is extracted from the assembled content by `def extract_json_from_content(content)`. A dedicated `requests.Session` is reused across all Cortex calls in a sync for connection pooling.

## Error handling

`def fetch_bulk_page(...)` retries HTTP 429, 500, 502, 503, and 504 with exponential backoff (2s, 4s, 8s) across 3 attempts; HTTP 400 fails immediately without retry since it indicates a structurally invalid request rather than a transient condition; HTTP 401/403 are raised immediately as authentication errors. `ConnectionError` and `Timeout` are caught as specific exception types (never a bare `except Exception`) and retried with the same backoff schedule. `def call_cortex_enrich(...)` catches `requests.exceptions.Timeout` and `requests.exceptions.RequestException` separately, logs a warning, and returns `None` so a Cortex failure degrades a single paper's enrichment fields to null rather than failing the sync. All object lookups that chain a second `.get()` use `(record.get(k) or {})` rather than `record.get(k, {})`, so an API field present with an explicit `null` (not merely absent) does not raise `AttributeError`. The `requests.Session` is closed in a `finally` block regardless of sync outcome.

## Tables created

### PAPERS

The `PAPERS` table consists of the following columns:
- `paper_id` (STRING, primary key): Semantic Scholar paper identifier
- `title` (STRING): Paper title
- `publication_year` (INT): Publication year (renamed from the API's `year`)
- `paper_abstract` (STRING): Paper abstract (renamed from the API's `abstract`)
- `reference_count` (INT): Number of references cited by this paper
- `citation_count` (INT): Number of times this paper has been cited
- `publication_date` (STRING): ISO-8601 publication date
- `publication_types` (STRING): JSON-serialized array of publication types, e.g. `["JournalArticle"]`
- `external_id_doi` (STRING): DOI identifier
- `external_id_arxiv` (STRING): ArXiv identifier
- `external_id_mag` (STRING): Microsoft Academic Graph identifier
- `external_id_pubmed` (STRING): PubMed identifier
- `external_id_dblp` (STRING): DBLP identifier
- `external_id_acl` (STRING): ACL Anthology identifier
- `external_id_corpus_id` (STRING): Semantic Scholar corpus ID
- `open_access_pdf_url` (STRING): Open-access PDF URL, when available
- `open_access_pdf_status` (STRING): Open-access status, e.g. GREEN, GOLD, CLOSED
- `cortex_research_impact` (STRING): Cortex-assessed research impact (high, medium, low); null when `enable_cortex` is false or enrichment failed
- `cortex_technical_domain` (STRING): Cortex-assessed technical domain
- `cortex_accessibility_level` (STRING): Cortex-assessed accessibility level (beginner, intermediate, advanced)
- `cortex_model_used` (STRING): Cortex model name used for enrichment; null when not enriched

### PAPER_AUTHORS

The `PAPER_AUTHORS` table consists of the following columns:
- `paper_id` (STRING, primary key): Semantic Scholar paper identifier, links to PAPERS
- `author_id` (STRING, primary key): Semantic Scholar author identifier
- `author_name` (STRING): Author display name

## Cost considerations

Each enriched paper costs exactly one Cortex Inference API call. All three assessments (research impact, technical domain, accessibility level) are requested in that single call rather than three, because per-paper call count is the only cost lever the connector controls.

Enrichment is bounded twice, and the second bound is the one that matters:

| Cap | Default | Scope |
|-----|---------|-------|
| `max_enrichments` | 3 | one sync |
| `max_enrichments_per_day` | 15 | all syncs in a UTC day, tracked in state |

A per-sync cap on its own is not a spend control. A connector on a 15-minute schedule runs 96 times a day, so `max_enrichments` of 3 permits 288 calls a day, not 3. The daily ceiling is what makes the bill predictable, and it is enforced in connector state so it survives across sync runs.

The defaults are deliberately small — enough to prove the enrichment path works end to end, not enough to enrich a corpus. Raise them against your Snowflake Cortex rate card, not by feel. Reaching either cap stops enrichment only; papers continue to sync and their `cortex_*` columns are null, so a run that hits the ceiling is a complete data sync with partial enrichment rather than a failure.

## Additional considerations

This example was contributed by [Kelly Kohlleffel](https://github.com/kellykohlleffel).

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
