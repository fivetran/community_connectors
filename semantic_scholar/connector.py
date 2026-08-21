"""Semantic Scholar Academic Graph connector — syncs research papers and enriches
them with Snowflake Cortex AI analysis during ingestion.

Fetches papers matching a configurable search query via the Semantic Scholar
Graph API bulk search endpoint (token-based cursor pagination, no offset cap).
Each paper is optionally enriched with Cortex LLM analysis: research impact
assessment, technical domain classification, and accessibility level rating.

Live-confirmed 2026-08-21: the bulk endpoint ignores any 'limit' parameter and
always returns up to 1000 records per page. batch_size/max_records_per_sync
are therefore local consumption caps, not API page sizes — the connector
tracks a within-page 'page_offset' in state so a per-sync cap never discards
unconsumed records from an already-fetched page.

API fields 'year' and 'abstract' are renamed to 'publication_year' and
'paper_abstract' to avoid ambiguity with SQL reserved and common keywords.
Authors are delivered to a separate 'paper_authors' JOIN table; externalIds
and openAccessPdf are flattened to scalar columns on the papers table.

See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

import json

# For the UTC-day key on the Cortex spend ceiling
from datetime import datetime, timezone
import time
import urllib.parse

import requests

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

__BASE_URL = "https://api.semanticscholar.org/graph/v1/paper/search/bulk"

__FIELDS = ",".join(
    [
        "paperId",
        "title",
        "year",
        "abstract",
        "authors",
        "externalIds",
        "openAccessPdf",
        "referenceCount",
        "citationCount",
        "publicationDate",
        "publicationTypes",
    ]
)

__DEFAULT_BATCH_SIZE = 50
__DEFAULT_MAX_RECORDS_PER_SYNC = 200
# The bulk search endpoint always returns up to this many records per page and
# ignores any requested 'limit' — live-confirmed 2026-08-21. batch_size is
# therefore a LOCAL checkpoint-and-cap granularity within a fetched page, not
# an API-level page size.
__BULK_API_FIXED_PAGE_SIZE = 1000
__DEFAULT_CORTEX_MODEL = "claude-sonnet-5"
__DEFAULT_CORTEX_TIMEOUT = 30
# Cortex enrichment is billed inference. These caps exist to PROVE THE PATH WORKS, not
# to enrich a corpus. Kelly, 2026-08-21: "we just need to prove that the connector works
# as advertised with cortex."
#
# A per-sync cap alone is the wrong shape: it multiplies by sync frequency. At the old
# default of 100/sync a 15-minute schedule is 9,600 calls a day, and the connector had
# no way to know that was happening. So there are two caps, and the daily one is the
# real ceiling because it survives across syncs in state.
__DEFAULT_MAX_ENRICHMENTS = 3  # per sync
__DEFAULT_MAX_ENRICHMENTS_PER_DAY = 15  # hard ceiling across all syncs in a UTC day

__MAX_RETRIES = 3
__BASE_DELAY_SECONDS = 2
__RETRYABLE_STATUS_CODES = [429, 500, 502, 503, 504]
__REQUEST_TIMEOUT_SECONDS = 30

__CORTEX_INFERENCE_ENDPOINT = "/api/v2/cortex/inference:complete"
__CORTEX_RATE_LIMIT_DELAY = 0.2

__ALLOWED_CORTEX_MODELS = frozenset(
    {
        "claude-sonnet-5",
        "claude-sonnet-4-6",
        "mistral-large2",
        "llama3.1-70b",
        "llama3.1-8b",
    }
)


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure all required parameters are present and valid.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: if any required configuration parameter is missing or invalid.
    """
    # search_query is required and must be non-empty
    search_query = configuration.get("search_query", "")
    if not str(search_query).strip():
        raise ValueError("search_query is required and must not be empty")

    # api_key is optional — empty string is valid (unauthenticated, rate-limited)
    # Validated by presence in configuration only; no format constraint.
    _ = configuration.get("api_key", "")

    # enable_cortex must be exactly "true" or "false" — a bare .lower() == "true"
    # check silently treats "yes-please" as False and hides misconfiguration.
    enable_cortex_raw = str(configuration.get("enable_cortex", "true")).lower()
    if enable_cortex_raw not in ("true", "false"):
        raise ValueError(
            f"enable_cortex must be 'true' or 'false', got: "
            f"{configuration.get('enable_cortex')!r}"
        )
    enable_cortex = enable_cortex_raw == "true"

    # Numeric fields: use <= 0, not < 0 — zero is not a valid positive integer.
    numeric_fields = {
        "batch_size": __DEFAULT_BATCH_SIZE,
        "max_records_per_sync": __DEFAULT_MAX_RECORDS_PER_SYNC,
        "cortex_timeout": __DEFAULT_CORTEX_TIMEOUT,
        "max_enrichments": __DEFAULT_MAX_ENRICHMENTS,
        "max_enrichments_per_day": __DEFAULT_MAX_ENRICHMENTS_PER_DAY,
    }
    for field, default in numeric_fields.items():
        raw = configuration.get(field, str(default))
        try:
            value = int(raw)
        except (TypeError, ValueError):
            raise ValueError(f"{field} must be a positive integer, got: {raw!r}")
        if value <= 0:
            raise ValueError(f"{field} must be a positive integer (> 0), got: {value}")

    # Cortex credentials required only when Cortex is enabled.
    if enable_cortex:
        snowflake_account = configuration.get("snowflake_account", "")
        if not snowflake_account:
            raise ValueError("snowflake_account is required when enable_cortex is true")
        # Reject scheme prefixes — f"https://{snowflake_account}..." doubles the scheme.
        if snowflake_account.startswith(("http://", "https://")):
            raise ValueError(
                "snowflake_account must be a hostname (no scheme prefix), "
                f"got: {snowflake_account!r}"
            )
        if not snowflake_account.endswith("snowflakecomputing.com"):
            raise ValueError(
                "snowflake_account must end with 'snowflakecomputing.com', "
                f"got: {snowflake_account!r}"
            )

        snowflake_pat_token = configuration.get("snowflake_pat_token", "")
        if not snowflake_pat_token:
            raise ValueError("snowflake_pat_token is required when enable_cortex is true")

        cortex_model = configuration.get("cortex_model", __DEFAULT_CORTEX_MODEL)
        if cortex_model not in __ALLOWED_CORTEX_MODELS:
            raise ValueError(
                f"cortex_model must be one of {sorted(__ALLOWED_CORTEX_MODELS)}, "
                f"got: {cortex_model!r}"
            )


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "papers",
            "primary_key": ["paper_id"],
            "columns": {
                "paper_id": "STRING",
                "title": "STRING",
                # API field 'year' renamed: year is a SQL temporal keyword and
                # collides with DuckDB/Snowflake functions even when not reserved.
                "publication_year": "INT",
                # API field 'abstract' renamed: defensive — not reserved in Snowflake
                # but a common collision in other engines and tool SQL generation.
                "paper_abstract": "STRING",
                "reference_count": "INT",
                "citation_count": "INT",
                "publication_date": "STRING",
                "publication_types": "STRING",
                "external_id_doi": "STRING",
                "external_id_arxiv": "STRING",
                "external_id_mag": "STRING",
                "external_id_pubmed": "STRING",
                "external_id_dblp": "STRING",
                "external_id_acl": "STRING",
                "external_id_corpus_id": "STRING",
                "open_access_pdf_url": "STRING",
                "open_access_pdf_status": "STRING",
                "cortex_research_impact": "STRING",
                "cortex_technical_domain": "STRING",
                "cortex_accessibility_level": "STRING",
                "cortex_model_used": "STRING",
            },
        },
        {
            "table": "paper_authors",
            "primary_key": ["paper_id", "author_id"],
            "columns": {
                "paper_id": "STRING",
                "author_id": "STRING",
                "author_name": "STRING",
            },
        },
    ]


def flatten_paper(record: dict, enrichment: dict | None = None) -> dict:
    """
    Flatten a raw Semantic Scholar paper record to the papers table shape.

    Renames 'year' -> 'publication_year' and 'abstract' -> 'paper_abstract'.
    Flattens externalIds (object) and openAccessPdf (object|null) to scalar columns.
    Serialises publicationTypes (array|null) to a JSON string.
    Authors are NOT included here — they go to the paper_authors table.

    Args:
        record: raw paper object from the API
        enrichment: optional dict of cortex enrichment fields

    Returns:
        dict with exactly the columns declared in schema()['papers']
    """
    external_ids = record.get("externalIds") or {}
    pdf = record.get("openAccessPdf") or {}

    row = {
        "paper_id": record.get("paperId"),
        "title": record.get("title"),
        "publication_year": record.get("year"),
        "paper_abstract": record.get("abstract"),
        "reference_count": record.get("referenceCount"),
        "citation_count": record.get("citationCount"),
        "publication_date": record.get("publicationDate"),
        "publication_types": (
            json.dumps(record["publicationTypes"]) if record.get("publicationTypes") else None
        ),
        "external_id_doi": external_ids.get("DOI"),
        "external_id_arxiv": external_ids.get("ArXiv"),
        "external_id_mag": external_ids.get("MAG"),
        "external_id_pubmed": external_ids.get("PubMed"),
        "external_id_dblp": external_ids.get("DBLP"),
        "external_id_acl": external_ids.get("ACL"),
        "external_id_corpus_id": (
            str(external_ids["CorpusId"]) if external_ids.get("CorpusId") is not None else None
        ),
        "open_access_pdf_url": pdf.get("url"),
        "open_access_pdf_status": pdf.get("status"),
        "cortex_research_impact": None,
        "cortex_technical_domain": None,
        "cortex_accessibility_level": None,
        "cortex_model_used": None,
    }

    if enrichment:
        row.update(enrichment)

    # Check 25: stable identifiers are stamped AFTER the enrichment merge, never before.
    # The previous order relied on every enrichment key carrying a `cortex_` prefix --
    # true today, and one rename away from an LLM response silently clobbering the
    # primary key. Ordering is a guarantee; a naming convention is a hope.
    row["paper_id"] = record.get("paperId")

    return row


def flatten_authors(record: dict) -> list[dict]:
    """
    Extract author rows from a raw paper record for the paper_authors JOIN table.

    Authors whose authorId is null are skipped — a null primary key column would
    cause a SYNC FAILED at the destination. The Semantic Scholar API returns null
    authorId for authors who do not have a Semantic Scholar profile.

    Args:
        record: raw paper object from the API

    Returns:
        list of dicts, one per author with a non-null authorId
    """
    paper_id = record.get("paperId")
    authors = record.get("authors") or []
    rows = []
    for author in authors:
        author_id = author.get("authorId")
        if author_id is None:
            log.info(
                f"Skipping author with null authorId for paper {paper_id}: "
                f"name={author.get('name')!r}"
            )
            continue
        rows.append(
            {
                "paper_id": paper_id,
                "author_id": author_id,
                "author_name": author.get("name"),
            }
        )
    return rows


def create_cortex_session(configuration: dict) -> requests.Session:
    """A session used ONLY for Cortex inference.

    Check 28, and the ai-enrichment-patterns reference: "Don't share with the
    data-source session." Two reasons that both matter. Connection pooling is per-host
    and Cortex is a different host from the Semantic Scholar API. And a bearer token for
    Snowflake has no business living on a session that talks to a third-party API.

    Built only inside the enrichment guard, so a run with enable_cortex=false never
    constructs it and never reads the PAT.
    """
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {configuration['snowflake_pat_token']}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    )
    return session


def create_session(api_key: str) -> requests.Session:
    """
    Create a requests session with appropriate headers.

    Args:
        api_key: optional Semantic Scholar API key; empty string = unauthenticated

    Returns:
        requests.Session configured for Semantic Scholar requests
    """
    session = requests.Session()
    headers = {"User-Agent": "Fivetran-SemanticScholar-Connector/1.0"}
    if api_key:
        headers["x-api-key"] = api_key
    session.headers.update(headers)
    return session


def fetch_bulk_page(session: requests.Session, query: str, token: str | None) -> dict:
    """
    Fetch one page of bulk paper search results.

    Uses urllib.parse.quote to safely encode the search query in the URL.
    Retries on transient errors with exponential backoff.

    Live-confirmed 2026-08-21: the /paper/search/bulk endpoint IGNORES any
    'limit' parameter and always returns up to __BULK_API_FIXED_PAGE_SIZE
    (1000) records per page regardless of what is requested (verified with
    limit=5 and limit=20, both returned 1000 records). No 'limit' param is
    sent here — sending one would silently misrepresent the actual page size
    to a future reader. Callers MUST NOT assume a page has fewer than 1000
    records; see the page_offset handling in update() for how a per-sync cap
    is applied without discarding unconsumed records from a fetched page.

    Args:
        session: requests.Session with headers set
        query: search query string (URL-encoded internally)
        token: continuation token from previous response, or None for first page

    Returns:
        API response dict with 'data' list (up to 1000 records) and optional
        'token' key (present unless this is the final page of the traversal)

    Raises:
        RuntimeError: if all retry attempts fail, or on non-retryable errors
    """
    params = {
        "query": query,
        "fields": __FIELDS,
        "sort": "publicationDate:desc",
    }
    if token:
        params["token"] = token

    url = __BASE_URL + "?" + urllib.parse.urlencode(params)

    for attempt in range(__MAX_RETRIES):
        try:
            response = session.get(url, timeout=__REQUEST_TIMEOUT_SECONDS)

            # 400 means the request is structurally bad — not a transient error.
            # Retrying it wastes attempts and delays the failure message.
            if response.status_code == 400:
                raise RuntimeError(f"API rejected the request (HTTP 400): {response.text[:200]}")

            response.raise_for_status()
            return response.json()

        except requests.exceptions.ConnectionError as e:
            if attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(f"Connection error, retrying in {delay}s: {e}")
                time.sleep(delay)
            else:
                raise RuntimeError(f"Connection failed after {__MAX_RETRIES} attempts: {e}") from e

        except requests.exceptions.Timeout as e:
            if attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(f"Timeout, retrying in {delay}s: {e}")
                time.sleep(delay)
            else:
                raise RuntimeError(f"Request timed out after {__MAX_RETRIES} attempts: {e}") from e

        except requests.exceptions.RequestException as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status in (401, 403):
                raise RuntimeError(f"HTTP {status}: check your api_key. URL: {url}") from e
            if status in __RETRYABLE_STATUS_CODES and attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(f"HTTP {status}, retrying in {delay}s (attempt {attempt + 1})")
                time.sleep(delay)
            else:
                raise RuntimeError(
                    f"API request failed after {attempt + 1} attempt(s): {e}"
                ) from e


def parse_cortex_streaming_response(response: requests.Response) -> str:
    """
    Parse the SSE streaming response from the Snowflake Cortex inference API.

    Args:
        response: requests.Response with SSE content

    Returns:
        Concatenated content string from all SSE data events
    """
    content = ""
    for line in response.text.split("\n"):
        if line.startswith("data: "):
            try:
                data = json.loads(line[6:])
                if data.get("choices"):
                    # Use `or {}` rather than `.get(k, {})`: dict.get's default applies
                    # only when the key is absent. A key present with value None returns
                    # None and the chained .get() raises AttributeError.
                    content += (data["choices"][0].get("delta") or {}).get("content", "")
            except (json.JSONDecodeError, KeyError, IndexError):
                continue
    return content


def extract_json_from_content(content: str) -> dict | None:
    """
    Extract a JSON object from a string that may contain surrounding text.

    Args:
        content: string potentially containing a JSON object

    Returns:
        parsed dictionary if JSON found, or None
    """
    if "{" in content and "}" in content:
        start = content.find("{")
        end = content.rfind("}") + 1
        try:
            return json.loads(content[start:end])
        except json.JSONDecodeError:
            return None
    return None


def call_cortex_enrich(
    cortex_session: requests.Session,
    account: str,
    title: str,
    abstract_text: str | None,
    model: str,
    timeout: int,
) -> dict | None:
    """
    Call Snowflake Cortex API to enrich a paper with research intelligence.

    Asks for research impact (high/medium/low), technical domain (NLP, CV, etc.),
    and accessibility level (beginner/intermediate/advanced) in a single call
    to minimise Cortex API requests per paper.

    Args:
        cortex_session: dedicated Cortex session (check 28 — never the data-source one)
        account: Snowflake account hostname
        title: paper title
        abstract_text: paper abstract or None
        model: Cortex LLM model name
        timeout: API request timeout in seconds

    Returns:
        dict with cortex enrichment keys, or None on error
    """
    url = f"https://{account}{__CORTEX_INFERENCE_ENDPOINT}"

    context = f"Title: {title}"
    if abstract_text:
        context += f"\nAbstract: {abstract_text[:500]}"

    prompt = (
        "Analyze this academic paper and respond ONLY with a JSON object in this exact format:\n"
        '{"research_impact": "high|medium|low", '
        '"technical_domain": '
        '"NLP|CV|ML|Systems|Theory|Biology|Chemistry|Physics|Medicine|Social|Other", '
        '"accessibility_level": "beginner|intermediate|advanced"}\n\n'
        f"{context}\n\nJSON:"
    )

    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0,
        "max_tokens": 150,
    }

    try:
        # Headers live on the dedicated Cortex session (check 28), not per call.
        response = cortex_session.post(url, json=payload, timeout=timeout)
        response.raise_for_status()
        content = parse_cortex_streaming_response(response)
        result = extract_json_from_content(content)
        return result
    except requests.exceptions.Timeout:
        log.warning(f"Cortex enrichment timeout after {timeout}s for paper: {title[:60]}")
        return None
    except requests.exceptions.RequestException as e:
        log.warning(f"Cortex enrichment API error: {e}")
        return None


def enrich_paper(cortex_session: requests.Session, configuration: dict, record: dict) -> dict:
    """
    Orchestrate Cortex enrichment for a single paper record.

    Args:
        cortex_session: dedicated Cortex session (check 28 — never the data-source one)
        configuration: connector configuration dict
        record: raw paper record from the API

    Returns:
        dict with cortex_* fields for the papers table
    """
    account = configuration.get("snowflake_account")
    model = configuration.get("cortex_model", __DEFAULT_CORTEX_MODEL)
    timeout = int(configuration.get("cortex_timeout", str(__DEFAULT_CORTEX_TIMEOUT)))

    enrichment = {
        "cortex_research_impact": None,
        "cortex_technical_domain": None,
        "cortex_accessibility_level": None,
        "cortex_model_used": model,
    }

    title = record.get("title") or ""
    if not title:
        return enrichment

    result = call_cortex_enrich(
        cortex_session, account, title, record.get("abstract"), model, timeout
    )

    if result:
        enrichment["cortex_research_impact"] = result.get("research_impact")
        enrichment["cortex_technical_domain"] = result.get("technical_domain")
        enrichment["cortex_accessibility_level"] = result.get("accessibility_level")

    time.sleep(__CORTEX_RATE_LIMIT_DELAY)
    return enrichment


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: connector_builds/semantic_scholar : snowflake-cortex-semantic-scholar")

    validate_configuration(configuration)

    search_query = configuration.get("search_query", "")
    api_key = configuration.get("api_key", "")
    enable_cortex = str(configuration.get("enable_cortex", "true")).lower() == "true"
    max_records = int(
        configuration.get("max_records_per_sync", str(__DEFAULT_MAX_RECORDS_PER_SYNC))
    )
    max_enrichments = int(configuration.get("max_enrichments", str(__DEFAULT_MAX_ENRICHMENTS)))
    batch_size = int(configuration.get("batch_size", str(__DEFAULT_BATCH_SIZE)))

    bulk_token = state.get("bulk_token")
    # page_offset: how many records of the CURRENT page (identified by
    # bulk_token) have already been consumed. Required because the bulk API
    # always returns __BULK_API_FIXED_PAGE_SIZE (1000) records per page and
    # ignores batch_size/max_records — without tracking a within-page
    # position, stopping mid-page and advancing bulk_token to next_token
    # would permanently skip every unconsumed record on that page (this is
    # the consumed-not-fetched pagination invariant, applied to a token
    # cursor instead of an offset cursor).
    page_offset = state.get("page_offset", 0)
    total_synced = state.get("total_synced", 0)

    if enable_cortex:
        log.info(
            f"Cortex enrichment ENABLED: model={configuration.get('cortex_model', __DEFAULT_CORTEX_MODEL)}"
        )
    else:
        log.info("Cortex enrichment DISABLED")

    log.info(
        f"Resuming from token={'<none — fresh start>' if bulk_token is None else bulk_token[:20] + '...'}, "
        f"total_synced={total_synced}"
    )

    session = create_session(api_key)
    synced_this_run = 0
    enriched_count = 0

    # Cortex spend ceiling. enriched_count resets every sync, so on its own it is a cap
    # per sync and NOT per day -- at 5/sync on a 15-minute schedule that is still 480 a
    # day. The real ceiling is carried in state and keyed to the UTC date, so it holds
    # no matter how often Fivetran syncs.
    max_per_day = int(
        configuration.get("max_enrichments_per_day", str(__DEFAULT_MAX_ENRICHMENTS_PER_DAY))
    )
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if state.get("enrichment_day") != today:
        state["enrichment_day"] = today
        state["enriched_today"] = 0
    enriched_today = int(state.get("enriched_today", 0))

    # Build the Cortex session ONCE, and only when enrichment will actually run
    # (check 28). enable_cortex=false never constructs it and never reads the PAT.
    cortex_session = None
    if enable_cortex and enriched_today < max_per_day:
        cortex_session = create_cortex_session(configuration)
    elif enable_cortex:
        log.warning(
            f"Cortex enrichment SKIPPED: daily ceiling reached "
            f"({enriched_today}/{max_per_day} for {today}). Records still sync."
        )

    try:
        papers: list | None = None  # cache of the currently-fetched page
        next_token: str | None = None  # token that will follow the current page

        while synced_this_run < max_records:
            if papers is None:
                log.info(
                    f"Fetching page: token={'none' if bulk_token is None else bulk_token[:20]}"
                )
                page = fetch_bulk_page(session, search_query, bulk_token)
                papers = page.get("data") or []
                next_token = page.get("token")  # None when traversal is complete

                if not papers:
                    log.info("No papers returned — traversal complete")
                    state["bulk_token"] = None
                    state["page_offset"] = 0
                    state["total_synced"] = total_synced + synced_this_run
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
                    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewithlargedatasets).
                    op.checkpoint(state=state)
                    break

            # Consume up to batch_size records from the CURRENT page, starting
            # at page_offset, bounded by whatever is left of max_records this
            # run. This never skips unconsumed records: if the run stops
            # mid-page, bulk_token still points at THIS page (unchanged below)
            # and page_offset marks where to resume — the page is re-fetched
            # and the already-consumed prefix is skipped, not lost.
            remaining_in_run = max_records - synced_this_run
            remaining_in_page = len(papers) - page_offset
            chunk_size = min(batch_size, remaining_in_run, remaining_in_page)

            for paper in papers[page_offset : page_offset + chunk_size]:
                enrichment = None
                # Split into named booleans rather than a multi-line `and` chain.
                # Black moves `and` to line starts and the target repo selects W, so
                # the two tools fight over that shape and CI loses -- exactly what
                # turned PR #55 red. No operator split, nothing to disagree about.
                under_sync_cap = enriched_count < max_enrichments
                under_day_cap = enriched_today < max_per_day
                if cortex_session is not None and under_sync_cap and under_day_cap:
                    enrichment = enrich_paper(cortex_session, configuration, paper)
                    enriched_count += 1
                    enriched_today += 1
                    state["enriched_today"] = enriched_today

                row = flatten_paper(paper, enrichment)
                author_rows = flatten_authors(paper)

                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(table="papers", data=row)

                for author_row in author_rows:
                    # The 'upsert' operation is used to insert or update data in the destination table.
                    # The first argument is the name of the destination table.
                    # The second argument is a dictionary containing the record to be upserted.
                    op.upsert(table="paper_authors", data=author_row)

                synced_this_run += 1

            page_offset += chunk_size
            page_exhausted = page_offset >= len(papers)

            if page_exhausted:
                # Only advance the token once every record on this page has
                # actually been consumed — never on partial consumption.
                bulk_token = next_token
                page_offset = 0
                papers = None  # force a re-fetch of the next page next iteration

            state["bulk_token"] = bulk_token
            state["page_offset"] = page_offset
            state["total_synced"] = total_synced + synced_this_run

            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewithlargedatasets).
            op.checkpoint(state=state)

            log.info(
                f"Checkpointed: synced_this_run={synced_this_run}, "
                f"enriched={enriched_count}, page_offset={page_offset}, "
                f"next_token={'none' if bulk_token is None else bulk_token[:20]}"
            )

            if page_exhausted and bulk_token is None:
                log.info("Token exhausted — full traversal complete for this query")
                break

        log.info(
            f"Sync complete: {synced_this_run} papers synced this run, "
            f"{enriched_count} enriched with Cortex, "
            f"total lifetime: {total_synced + synced_this_run}"
        )

    except Exception as e:
        log.error(f"Unexpected error during sync: {e}")
        raise

    finally:
        session.close()


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
#
# IMPORTANT: The recommended way to test your connector is using the Fivetran debug command:
#   fivetran debug
#
# This local testing block is provided as a convenience for quick debugging during development,
# such as using IDE debug tools (breakpoints, step-through debugging, etc.).
# Note: This method is not called by Fivetran when executing your connector in production.
# Always test using 'fivetran debug' prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
