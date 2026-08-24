"""This connector syncs academic paper records from the Semantic Scholar Academic Graph
API, optionally enriching each paper with Snowflake Cortex AI analysis during ingestion.

Papers matching a configurable search query are fetched from the bulk search endpoint
using token-based cursor pagination, which has no offset cap. API fields 'year' and
'abstract' are renamed to 'publication_year' and 'paper_abstract' to avoid ambiguity
with SQL reserved and common keywords. Authors are delivered to a separate
'paper_authors' JOIN table; externalIds and openAccessPdf are flattened to scalar
columns on the papers table. Cortex enrichment is optional and defaults to off.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file and serialising list columns
import json

# For the UTC-day key used by the Cortex daily spend ceiling
from datetime import datetime, timezone

# For the exponential backoff delay between retries
import time

# For safely encoding the search query into the request URL
import urllib.parse

# For issuing HTTP requests to the Semantic Scholar API
import requests

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Optional Snowflake Cortex enrichment, kept in its own module so the data path
# and the inference path can be read and maintained independently
import cortex

# Pure record-shaping helpers, re-exported here so the connector's public surface
# is unchanged by the split
from transform import flatten_authors, flatten_paper

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
# ignores any requested 'limit'
__BULK_API_FIXED_PAGE_SIZE = 1000

__MAX_RETRIES = 3
__BASE_DELAY_SECONDS = 2
__RETRYABLE_STATUS_CODES = [429, 500, 502, 503, 504]
__REQUEST_TIMEOUT_SECONDS = 30

# Values still carrying the placeholder shape from the shipped configuration.json
# template, for example "<YOUR_SEARCH_QUERY>". Treated as "not configured" rather
# than as a real value, so a forgotten field fails with a clear message instead of
# being sent to the API.
__PLACEHOLDER_PATTERN = ("<", ">")


def is_placeholder(value) -> bool:
    """
    Report whether a configuration value is still an unedited template placeholder.

    Args:
        value: raw configuration value

    Returns:
        True if the value looks like "<SOMETHING>", otherwise False
    """
    text = str(value).strip()
    return text.startswith(__PLACEHOLDER_PATTERN[0]) and text.endswith(__PLACEHOLDER_PATTERN[1])


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure all required parameters are present and valid.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: if any required configuration parameter is missing or invalid.
    """
    # Reject unedited placeholders before any per-field rule runs. Without this a
    # value like "<YOUR_SEARCH_QUERY>" is a non-empty string and passes every
    # emptiness check, then reaches the API as a literal search term.
    for key, value in configuration.items():
        if is_placeholder(value):
            raise ValueError(
                f"{key} still holds the template placeholder {str(value).strip()!r} -- "
                "replace it with a real value in configuration.json"
            )

    # search_query is required and must be non-empty
    search_query = configuration.get("search_query", "")
    if not str(search_query).strip():
        raise ValueError("search_query is required and must not be empty")

    # api_key is optional -- empty string is valid (unauthenticated, rate-limited)
    # Validated by presence in configuration only; no format constraint.
    _ = configuration.get("api_key", "")

    # enable_cortex must be exactly "true" or "false" -- a bare .lower() == "true"
    # check silently treats "yes-please" as False and hides misconfiguration.
    enable_cortex_raw = str(configuration.get("enable_cortex", "true")).lower()
    if enable_cortex_raw not in ("true", "false"):
        raise ValueError(
            f"enable_cortex must be 'true' or 'false', got: "
            f"{configuration.get('enable_cortex')!r}"
        )
    enable_cortex = enable_cortex_raw == "true"

    # Numeric fields: use <= 0, not < 0 -- zero is not a valid positive integer.
    numeric_fields = {
        "batch_size": __DEFAULT_BATCH_SIZE,
        "max_records_per_sync": __DEFAULT_MAX_RECORDS_PER_SYNC,
        "cortex_timeout": cortex.DEFAULT_TIMEOUT,
        "max_enrichments": cortex.DEFAULT_MAX_ENRICHMENTS,
        "max_enrichments_per_day": cortex.DEFAULT_MAX_ENRICHMENTS_PER_DAY,
    }
    for field, default in numeric_fields.items():
        raw = configuration.get(field, str(default))
        try:
            value = int(raw)
        except (TypeError, ValueError):
            raise ValueError(f"{field} must be a positive integer, got: {raw!r}")
        if value <= 0:
            raise ValueError(f"{field} must be a positive integer (> 0), got: {value}")

    # Cortex credentials are required only when Cortex is enabled, so a data-only
    # sync needs no Snowflake account at all.
    if enable_cortex:
        cortex.validate_configuration(configuration)


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
                # API field 'abstract' renamed: defensive -- not reserved in Snowflake
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

    The endpoint ignores any 'limit' parameter and always returns up to
    __BULK_API_FIXED_PAGE_SIZE records per page, so no 'limit' is sent -- sending
    one would misrepresent the actual page size to a future reader. Callers must
    not assume a page holds fewer than that; see the page_offset handling in
    update() for how a per-sync cap is applied without discarding unconsumed
    records from a fetched page.

    Args:
        session: requests.Session with headers set
        query: search query string (URL-encoded internally)
        token: continuation token from previous response, or None for first page

    Returns:
        API response dict with 'data' list and an optional 'token' key, absent on
        the final page of the traversal

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

            # 400 means the request is structurally bad -- not a transient error.
            # Retrying it wastes attempts and delays the failure message.
            if response.status_code == 400:
                raise RuntimeError(f"API rejected the request (HTTP 400): {response.text[:200]}")

            response.raise_for_status()
            return response.json()

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            error_type = (
                "Timeout" if isinstance(e, requests.exceptions.Timeout) else "Connection error"
            )
            if attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(f"{error_type}, retrying in {delay}s: {e}")
                time.sleep(delay)
            else:
                raise RuntimeError(
                    f"{error_type} failed after {__MAX_RETRIES} attempts: {e}"
                ) from e

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
    log.warning("Example: Source Examples : Semantic Scholar Academic Graph")

    validate_configuration(configuration)

    search_query = configuration.get("search_query", "")
    api_key = configuration.get("api_key", "")
    enable_cortex = str(configuration.get("enable_cortex", "true")).lower() == "true"
    max_records = int(
        configuration.get("max_records_per_sync", str(__DEFAULT_MAX_RECORDS_PER_SYNC))
    )
    max_enrichments = int(
        configuration.get("max_enrichments", str(cortex.DEFAULT_MAX_ENRICHMENTS))
    )
    batch_size = int(configuration.get("batch_size", str(__DEFAULT_BATCH_SIZE)))

    bulk_token = state.get("bulk_token")
    # page_offset: how many records of the CURRENT page (identified by bulk_token)
    # have already been consumed. Required because the bulk API always returns
    # __BULK_API_FIXED_PAGE_SIZE records per page and ignores batch_size and
    # max_records -- without tracking a within-page position, stopping mid-page and
    # advancing bulk_token to next_token would permanently skip every unconsumed
    # record on that page.
    page_offset = state.get("page_offset", 0)
    total_synced = state.get("total_synced", 0)

    if enable_cortex:
        model = configuration.get("cortex_model", cortex.DEFAULT_MODEL)
        log.info(f"Cortex enrichment ENABLED: model={model}")
    else:
        log.info("Cortex enrichment DISABLED")

    log.info(
        f"Resuming from token={'<none -- fresh start>' if bulk_token is None else bulk_token[:20] + '...'}, "
        f"total_synced={total_synced}"
    )

    session = create_session(api_key)
    synced_this_run = 0
    enriched_count = 0

    # Cortex spend ceiling. enriched_count resets every sync, so on its own it caps
    # per sync and not per day. The real ceiling is carried in state and keyed to the
    # UTC date, so it holds no matter how often Fivetran syncs.
    max_per_day = int(
        configuration.get("max_enrichments_per_day", str(cortex.DEFAULT_MAX_ENRICHMENTS_PER_DAY))
    )
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if state.get("enrichment_day") != today:
        state["enrichment_day"] = today
        state["enriched_today"] = 0
    enriched_today = int(state.get("enriched_today", 0))

    # Build the Cortex session once, and only when enrichment will actually run, so
    # a run with enable_cortex=false never constructs it and never reads the token.
    cortex_session = None
    if enable_cortex and enriched_today < max_per_day:
        cortex_session = cortex.create_session(configuration)
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
                    log.info("No papers returned -- traversal complete")
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

            # Consume up to batch_size records from the CURRENT page, starting at
            # page_offset, bounded by whatever is left of max_records this run. This
            # never skips unconsumed records: if the run stops mid-page, bulk_token
            # still points at THIS page and page_offset marks where to resume -- the
            # page is re-fetched and the already-consumed prefix is skipped, not lost.
            remaining_in_run = max_records - synced_this_run
            remaining_in_page = len(papers) - page_offset
            chunk_size = min(batch_size, remaining_in_run, remaining_in_page)

            for paper in papers[page_offset : page_offset + chunk_size]:
                enrichment = None
                # Named booleans rather than a multi-line `and` chain: black moves a
                # trailing `and` to the start of the next line, which the repo's flake8
                # configuration rejects. No operator split, nothing for the two tools
                # to disagree about.
                under_sync_cap = enriched_count < max_enrichments
                under_day_cap = enriched_today < max_per_day
                if cortex_session is not None and under_sync_cap and under_day_cap:
                    enrichment = cortex.enrich_paper(cortex_session, configuration, paper)
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
                # Only advance the token once every record on this page has actually
                # been consumed -- never on partial consumption.
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
                log.info("Token exhausted -- full traversal complete for this query")
                break

        log.info(
            f"Sync complete: {synced_this_run} papers synced this run, "
            f"{enriched_count} enriched with Cortex, "
            f"total lifetime: {total_synced + synced_this_run}"
        )

    # The types this loop can actually raise: RuntimeError from fetch_bulk_page,
    # ValueError from validation, KeyError/TypeError from a record shaped
    # differently than the API documents, and any transport error that escaped the
    # retry budget. Named rather than caught broadly, so an error this code did not
    # anticipate propagates untouched instead of being logged as if it were expected.
    except (
        RuntimeError,
        ValueError,
        KeyError,
        TypeError,
        requests.exceptions.RequestException,
    ) as e:
        log.error(f"Error during sync: {type(e).__name__}: {e}")
        raise

    finally:
        session.close()
        if cortex_session is not None:
            cortex_session.close()


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
