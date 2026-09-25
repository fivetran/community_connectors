"""This connector syncs academic paper records from the Semantic Scholar Academic Graph
API, optionally enriching each paper with Snowflake Cortex AI analysis during ingestion.

Papers matching a configurable search query are fetched from the bulk search endpoint
using token-based cursor pagination, which has no offset cap. API fields 'year' and
'abstract' are renamed to 'publication_year' and 'paper_abstract' to avoid ambiguity
with SQL reserved and common keywords. Authors are delivered to a separate
'paper_authors' JOIN table; externalIds and openAccessPdf are flattened to scalar
columns on the papers table. Cortex enrichment is optional and stays off unless
enable_cortex is "true". Only search_query is required: every other configuration key
is optional, and a key left at its shipped "<PLACEHOLDER>" value is treated as unset
and falls back to its documented default.
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
# than as a real value, so a forgotten required field fails with a clear message
# and a forgotten optional field falls back to its default.
__PLACEHOLDER_PATTERN = ("<", ">")

# Cortex defaults live here with every other default, so the enrichment module never
# reads configuration and the connector is the single owner of what "default" means.
__DEFAULT_CORTEX_MODEL = "claude-sonnet-5"
__DEFAULT_CORTEX_TIMEOUT_SECONDS = 30

# Enrichment is billed inference, so it is bounded twice. A per-sync cap alone is
# not a spend control: it multiplies by sync frequency, and a connector on a
# 15-minute schedule runs 96 times a day. The daily cap is the ceiling that
# actually holds, because it is carried in connector state across syncs.
__DEFAULT_MAX_ENRICHMENTS = 3
__DEFAULT_MAX_ENRICHMENTS_PER_DAY = 15


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


def get_config_value(configuration: dict, key: str, default: str = "") -> str:
    """
    Read one configuration value, treating missing, blank, and placeholder values as unset.

    The shipped configuration.json carries a "<PLACEHOLDER>" for every key. A user who
    only needs the data path edits search_query and leaves the rest, so an unedited
    optional key must behave exactly like an omitted one.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        key: configuration key to read
        default: value to return when the key is unset

    Returns:
        the stripped configured value, or the default
    """
    raw = configuration.get(key)
    if raw is None:
        return default
    text = str(raw).strip()
    if not text or is_placeholder(text):
        return default
    return text


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure all required parameters are present and valid.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: if any required configuration parameter is missing or invalid.
    """
    # search_query is the only key that is always required. A placeholder counts as
    # missing: it is a non-empty string, so without this it would pass every
    # emptiness check and reach the API as a literal search term.
    search_query = get_config_value(configuration, "search_query")
    if not search_query:
        raise ValueError(
            "search_query is required: replace the <YOUR_SEARCH_QUERY> placeholder in "
            "configuration.json with a real query"
        )

    # api_key is optional -- empty or placeholder means unauthenticated (rate-limited)
    _ = get_config_value(configuration, "api_key")

    # enable_cortex must be exactly "true" or "false" when set, and defaults to
    # "false" so a base sync never needs a Snowflake account. A loose truthiness
    # check would silently treat "yes-please" as False and hide the misconfiguration.
    enable_cortex_raw = get_config_value(configuration, "enable_cortex", "false")
    if enable_cortex_raw not in ("true", "false"):
        raise ValueError(
            f"enable_cortex must be 'true' or 'false', got: {configuration.get('enable_cortex')!r}"
        )
    enable_cortex = enable_cortex_raw == "true"

    # Numeric fields: use <= 0, not < 0 -- zero is not a valid positive integer.
    numeric_fields = {
        "batch_size": __DEFAULT_BATCH_SIZE,
        "max_records_per_sync": __DEFAULT_MAX_RECORDS_PER_SYNC,
        "cortex_timeout": __DEFAULT_CORTEX_TIMEOUT_SECONDS,
        "max_enrichments": __DEFAULT_MAX_ENRICHMENTS,
        "max_enrichments_per_day": __DEFAULT_MAX_ENRICHMENTS_PER_DAY,
    }
    for field, default in numeric_fields.items():
        raw = get_config_value(configuration, field, str(default))
        try:
            value = int(raw)
        except (TypeError, ValueError):
            raise ValueError(f"{field} must be a positive integer, got: {raw!r}")
        if value <= 0:
            raise ValueError(f"{field} must be a positive integer (> 0), got: {value}")

    # Cortex settings are required only when Cortex is enabled, so a data-only
    # sync needs no Snowflake account at all.
    if enable_cortex:
        cortex.validate_settings(
            account=get_config_value(configuration, "snowflake_account"),
            pat_token=get_config_value(configuration, "snowflake_pat_token"),
            model=get_config_value(configuration, "cortex_model", __DEFAULT_CORTEX_MODEL),
        )


def resolve_settings(configuration: dict) -> dict:
    """
    Resolve every configuration value once, applying the documented defaults.

    Must be called after validate_configuration(), which guarantees the numeric
    fields parse and the Cortex settings are present when enable_cortex is true.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Returns:
        dict of typed settings used by update()
    """
    enable_cortex = get_config_value(configuration, "enable_cortex", "false") == "true"
    return {
        "search_query": get_config_value(configuration, "search_query"),
        "api_key": get_config_value(configuration, "api_key"),
        "enable_cortex": enable_cortex,
        "max_records_per_sync": int(
            get_config_value(
                configuration, "max_records_per_sync", str(__DEFAULT_MAX_RECORDS_PER_SYNC)
            )
        ),
        "batch_size": int(
            get_config_value(configuration, "batch_size", str(__DEFAULT_BATCH_SIZE))
        ),
        "max_enrichments": int(
            get_config_value(configuration, "max_enrichments", str(__DEFAULT_MAX_ENRICHMENTS))
        ),
        "max_enrichments_per_day": int(
            get_config_value(
                configuration, "max_enrichments_per_day", str(__DEFAULT_MAX_ENRICHMENTS_PER_DAY)
            )
        ),
        "cortex_account": get_config_value(configuration, "snowflake_account"),
        "cortex_pat_token": get_config_value(configuration, "snowflake_pat_token"),
        "cortex_model": get_config_value(configuration, "cortex_model", __DEFAULT_CORTEX_MODEL),
        "cortex_timeout": int(
            get_config_value(
                configuration, "cortex_timeout", str(__DEFAULT_CORTEX_TIMEOUT_SECONDS)
            )
        ),
    }


def reset_pagination_state(state: dict) -> None:
    """
    Clear every pagination key so the next fetch starts from the first page.

    Args:
        state: the connector state dictionary, modified in place
    """
    state["bulk_token"] = None
    state["page_offset"] = 0
    state["last_paper_id"] = None
    state["total_synced"] = 0


def find_resume_offset(papers: list, page_offset: int, last_paper_id: str | None) -> int:
    """
    Work out where to resume inside a re-fetched page.

    A checkpointed page_offset is only correct if the page still holds the same
    records in the same order. Pages are sorted by publication date, so papers
    indexed between two syncs can shift positions. The last consumed paperId is
    checkpointed as an anchor and used here to relocate the resume point:

    - the anchor is still at page_offset - 1: resume at page_offset (fast path)
    - the anchor moved: resume immediately after its new position
    - the anchor is gone: reprocess the page from its first record. Upserts are
      idempotent, so reprocessing costs time but never produces duplicates,
      whereas trusting a stale offset could skip records permanently.

    Args:
        papers: the records of the page just fetched
        page_offset: checkpointed number of records already consumed from this page
        last_paper_id: checkpointed paperId of the last consumed record, or None

    Returns:
        the offset to resume from, never greater than len(papers)
    """
    if page_offset <= 0:
        return 0
    if last_paper_id is None:
        return min(page_offset, len(papers))
    if page_offset <= len(papers):
        if papers[page_offset - 1].get("paperId") == last_paper_id:
            return page_offset
    for index in range(len(papers) - 1, -1, -1):
        if papers[index].get("paperId") == last_paper_id:
            log.info(
                f"Page shifted since the last checkpoint: resuming after the last consumed "
                f"record at position {index + 1} instead of {page_offset}"
            )
            return index + 1
    log.warning(
        "The current page no longer contains the last consumed record; reprocessing the "
        "page from its first record. Upserts are idempotent, so this cannot duplicate rows."
    )
    return 0


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
    settings = resolve_settings(configuration)
    search_query = settings["search_query"]
    max_records = settings["max_records_per_sync"]
    batch_size = settings["batch_size"]
    enable_cortex = settings["enable_cortex"]
    max_enrichments = settings["max_enrichments"]
    max_per_day = settings["max_enrichments_per_day"]

    # Pagination state is only meaningful for the query that produced it. A
    # continuation token or a within-page offset from a different query would skip
    # or repeat records, so a changed query restarts the traversal from page one.
    if state.get("search_query") not in (None, search_query):
        log.warning(
            "search_query changed since the last checkpoint; restarting the traversal "
            "from the first page"
        )
        reset_pagination_state(state)
    state["search_query"] = search_query

    bulk_token = state.get("bulk_token")
    # page_offset: how many records of the CURRENT page (identified by bulk_token)
    # have already been consumed. Required because the bulk API always returns
    # __BULK_API_FIXED_PAGE_SIZE records per page and ignores batch_size and
    # max_records -- without tracking a within-page position, stopping mid-page and
    # advancing bulk_token to next_token would permanently skip every unconsumed
    # record on that page. last_paper_id anchors that offset to a record, so a page
    # that shifted between syncs is relocated rather than trusted blindly.
    page_offset = state.get("page_offset", 0)
    last_paper_id = state.get("last_paper_id")
    total_synced = state.get("total_synced", 0)

    if enable_cortex:
        log.info(f"Cortex enrichment ENABLED: model={settings['cortex_model']}")
    else:
        log.info("Cortex enrichment DISABLED")

    log.info(
        f"Resuming from token={'<none -- fresh start>' if bulk_token is None else bulk_token[:20] + '...'}, "
        f"page_offset={page_offset}, total_synced={total_synced}"
    )

    session = create_session(settings["api_key"])
    synced_this_run = 0
    enriched_count = 0

    # Cortex spend ceiling. enriched_count resets every sync, so on its own it caps
    # per sync and not per day. The real ceiling is carried in state and keyed to the
    # UTC date, so it holds no matter how often Fivetran syncs.
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if state.get("enrichment_day") != today:
        state["enrichment_day"] = today
        state["enriched_today"] = 0
    enriched_today = int(state.get("enriched_today", 0))

    # Build the Cortex session once, and only when enrichment will actually run, so
    # a run with enable_cortex=false never constructs it and never reads the token.
    cortex_session = None
    if enable_cortex and enriched_today < max_per_day:
        cortex_session = cortex.create_session(settings["cortex_pat_token"])
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
                    state["last_paper_id"] = None
                    state["total_synced"] = total_synced + synced_this_run
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
                    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
                    op.checkpoint(state=state)
                    break

                # A re-fetched page may have shifted; relocate the resume point.
                page_offset = find_resume_offset(papers, page_offset, last_paper_id)

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
                # A paper without a title has nothing to assess, so it is not sent for
                # inference and does not consume either enrichment cap.
                has_title = bool((paper.get("title") or "").strip())
                under_sync_cap = enriched_count < max_enrichments
                under_day_cap = enriched_today < max_per_day
                caps_open = under_sync_cap and under_day_cap
                should_enrich = cortex_session is not None and caps_open and has_title
                if should_enrich:
                    enrichment = cortex.enrich_paper(
                        cortex_session,
                        settings["cortex_account"],
                        settings["cortex_model"],
                        settings["cortex_timeout"],
                        paper,
                    )
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

            if chunk_size > 0:
                page_offset += chunk_size
                last_paper_id = papers[page_offset - 1].get("paperId")
            page_exhausted = page_offset >= len(papers)

            if page_exhausted:
                # Only advance the token once every record on this page has actually
                # been consumed -- never on partial consumption.
                bulk_token = next_token
                page_offset = 0
                last_paper_id = None
                papers = None  # force a re-fetch of the next page next iteration

            state["bulk_token"] = bulk_token
            state["page_offset"] = page_offset
            state["last_paper_id"] = last_paper_id
            state["total_synced"] = total_synced + synced_this_run

            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
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

    # The types this loop can actually raise: RuntimeError from fetch_bulk_page or a
    # permanent Cortex error, ValueError from validation, KeyError/TypeError from a
    # record shaped differently than the API documents, and any transport error that
    # escaped the retry budget. Named rather than caught broadly, so an error this
    # code did not anticipate propagates untouched instead of being logged as if it
    # were expected.
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
