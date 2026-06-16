"""ZoomInfo Connector SDK example — syncs ZoomInfo Go-To-Market data into Fivetran.

This is a comprehensive reference connector for the ZoomInfo Search and Enrich
APIs. It is intentionally broad rather than single-purpose: a real ZoomInfo
deployment needs contacts, companies, scoops, intent, news, and several
credit-bearing enrichments to be useful, and they share one auth flow, one
pagination scheme, and one incremental-state model. Keeping them in one
connector lets the example demonstrate how those concerns compose. Each
enrichment is opt-in via a configuration flag (see README), so a default run
only touches the free Search endpoints.

Patterns demonstrated:
- OAuth 2.0 Client Credentials auth with in-process token caching and mid-sync
  401 refresh (see get_access_token).
- JSON:API page[size]/page[number] pagination with end-of-pages detection
  (see paginate).
- Incremental sync via per-endpoint server-side date filters, with a full-replace
  truncate+upsert fallback for the companies entity, which has no cursor
  (see apply_incremental_filter, sync_companies).
- Bounded-memory streaming of high-volume per-company enrichments through a
  worker pool + queue (see _stream_per_company_enrich).
- Exponential-backoff retries on 429/5xx and intra-table checkpointing
  (see post_with_retry, CHECKPOINT_EVERY_N_ROWS).

Pure helper logic and the HTTP retry path are unit-tested in connector_test.py
(mocked transport, no credentials required).

See the Connector SDK Technical Reference
(https://fivetran.com/docs/connectors/connector-sdk/technical-reference) and
Best Practices (https://fivetran.com/docs/connectors/connector-sdk/best-practices).
"""

# Import required classes from fivetran_connector_sdk for connector initialization,
# logging, and data operations (upsert, update, delete, checkpoint).
from fivetran_connector_sdk import Connector, Logging as log, Operations as op

# For parallelizing per-company enrichment fetches across a bounded worker pool.
from concurrent.futures import ThreadPoolExecutor

# For parsing and comparing ISO 8601 timestamps when advancing the incremental cursor.
from datetime import datetime, timezone

# For the bounded queue used to stream per-company enrich rows back to the main thread.
import queue

# For the lock guarding the shared token cache and the worker-pool abort event.
import threading

# For making HTTP calls to the ZoomInfo Search and Enrich APIs.
import requests

# For Base64-encoding the client_id:client_secret pair in the OAuth token request.
import base64

# For exponential-backoff sleeps between retries.
import time

# For reading configuration, building JSON:API request bodies, and serializing
# nested/list fields to JSON strings before upsert.
import json

# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────
TOKEN_URL = "https://api.zoominfo.com/gtm/oauth/v1/token"
SEARCH_BASE = "https://api.zoominfo.com"

# Search endpoints (free — no credits, records/requests counted)
ENDPOINT_CONTACTS = "/gtm/data/v1/contacts/search"
ENDPOINT_COMPANIES = "/gtm/data/v1/companies/search"
ENDPOINT_SCOOPS = "/gtm/data/v1/scoops/search"
ENDPOINT_INTENT = "/gtm/data/v1/intent/search"
ENDPOINT_NEWS = "/gtm/data/v1/news/search"

# Enrich endpoints (cost credits per company/contact)
ENDPOINT_CONTACTS_ENRICH = "/gtm/data/v1/contacts/enrich"
ENDPOINT_COMPANIES_ENRICH = "/gtm/data/v1/companies/enrich"
ENDPOINT_SCOOPS_ENRICH = "/gtm/data/v1/scoops/enrich"
ENDPOINT_TECHNOLOGIES_ENRICH = "/gtm/data/v1/companies/technologies/enrich"
ENDPOINT_CORP_HIER_ENRICH = "/gtm/data/v1/companies/corporate-hierarchy/enrich"

# Usage endpoint (free)
ENDPOINT_USAGE = "/gtm/data/v1/users/usage"

# JSON:API type name per endpoint
JSONAPI_TYPE = {
    ENDPOINT_CONTACTS: "ContactSearch",
    ENDPOINT_COMPANIES: "CompanySearch",
    ENDPOINT_SCOOPS: "ScoopSearch",
    ENDPOINT_INTENT: "IntentSearch",
    ENDPOINT_NEWS: "NewsSearch",
    ENDPOINT_CONTACTS_ENRICH: "ContactEnrich",
    ENDPOINT_COMPANIES_ENRICH: "CompanyEnrich",
    ENDPOINT_SCOOPS_ENRICH: "ScoopEnrich",
    ENDPOINT_TECHNOLOGIES_ENRICH: "TechnologyEnrich",
    ENDPOINT_CORP_HIER_ENRICH: "CorporateHierarchyEnrich",
}

# Pagination via URL query params: page[size] and page[number]
PAGE_SIZE = 100

# Safety cap on pages per endpoint per sync (client-side backstop). At
# PAGE_SIZE=100 this is 1M records. See SEARCH_RESULT_CEILING below for the
# server-side limit that is reached first in practice.
MAX_PAGES = 10000

# Server-side ceiling on a single ZoomInfo Search query. The Search API returns
# at most 100 pages (meta.page.total caps at 100), i.e. PAGE_SIZE * 100 = 10,000
# records, regardless of how large meta.totalResults is. A query whose universe
# exceeds this returns only the first 10,000 records — the remainder are NOT
# retrievable by paging further. The connector cannot page past this; the only
# way to capture more is to narrow the query (e.g. a tighter `countries` filter)
# so each query's universe stays under the ceiling. paginate() logs a WARNING
# when totalResults exceeds this so silent truncation is visible. See the
# "Known limitations" section of the README.
SEARCH_RESULT_CEILING = PAGE_SIZE * 100  # = 10,000

# Enrich API accepts up to 25 records per request (contacts and companies)
ENRICH_BATCH_SIZE = 25

# Worker pool size for per-company enrich loops (scoops, technologies).
# Lowered from 5 to 3 in 2026-05-28 — Fivetran cloud OOM'd with 5 workers
# accumulating per-company row lists in memory. Streaming via queue also added;
# see _stream_per_company_enrich. Don't raise above 3 without re-validating
# memory under the 1 GB cloud limit.
ENRICH_WORKERS = 3

# Bounded queue size for the per-company streaming pattern. Caps peak memory
# at roughly QUEUE_MAX × avg_row_size bytes. At ~5 KB/row → ~10 MB ceiling.
# Workers block on put() when queue is full, providing backpressure.
ENRICH_QUEUE_MAX = 2000

# Intra-table checkpoint cadence for high-volume streams. Fivetran best
# practice is ~1K rows or ~10 min per checkpoint — without intermediate
# checkpoints the entire table buffers in flight and OOMs the runtime.
# See https://fivetran.com/docs/connector-sdk/best-practices
CHECKPOINT_EVERY_N_ROWS = 1000

# Default country filter when the customer leaves `countries` blank.
DEFAULT_COUNTRY = "United States"

# Default output fields for Contact Enrichment
# Note: linkedInUrl is NOT a valid field — LinkedIn data is not available via this API.
# Note: managementLevel returns a list (e.g. ["C-Level"]) — stored as JSON string.
# Note: companyName is nested under company.name in the response, not a top-level field.
DEFAULT_CONTACT_ENRICH_FIELDS = [
    "firstName",
    "lastName",
    "email",
    "phone",
    "mobilePhone",
    "jobTitle",
    "jobFunction",
    "managementLevel",
    "companyId",
    "companyName",
    "companyWebsite",
    "companyRevenue",
    "companyEmployeeCount",
    "companyPrimaryIndustry",
    "city",
    "region",
    "state",
    "country",
    "directPhoneDoNotCall",
    "mobilePhoneDoNotCall",
    "lastUpdatedDate",
    "validDate",
    "contactAccuracyScore",
]

# Default output fields for Company Enrichment
# Confirmed valid via GET /gtm/data/v1/lookup/enrich?filter[entity]=company&filter[fieldType]=output
# Invalid names we fixed: "industry" -> "primaryIndustry", "companyType" -> "type"
DEFAULT_COMPANY_ENRICH_FIELDS = [
    "name",
    "website",
    "phone",
    "revenue",
    "revenueRange",
    "employeeCount",
    "employeeRange",
    "employeeGrowth",
    "primaryIndustry",
    "industries",
    "city",
    "state",
    "country",
    "street",
    "zipCode",
    "metroArea",
    "description",
    "ticker",
    "type",
    "naicsCodes",
    "sicCodes",
    "foundedYear",
    "companyStatus",
    "parentId",
    "parentName",
    "ultimateParentId",
    "ultimateParentName",
    "totalFundingAmount",
    "recentFundingAmount",
    "recentFundingDate",
    "socialMediaUrls",
    "logo",
    "lastUpdatedDate",
    "isDefunct",
]

# Default output fields for Corporate Hierarchy Enrichment
# Confirmed valid via GET /gtm/data/v1/lookup/enrich?filter[entity]=corporate-hierarchy&filter[fieldType]=output
# Only 4 valid fields exist: id, companyId, parentage, familyTree
# Note: id is the primary key (auto-returned), companyId is the ZoomInfo company ID,
#       parentage lists companies higher up in the hierarchy,
#       familyTree lists all companies and locations in the family tree.
# No standard company fields (name, website, city, etc.) are valid here —
# use companies_enriched table for those details.
DEFAULT_CORP_HIER_FIELDS = [
    "companyId",
    "parentage",
    "familyTree",
]

# Retry settings for transient HTTP failures
MAX_RETRIES = 5
RETRY_BASE_WAIT = 2  # seconds — doubles each retry (exponential backoff)
RETRY_MAX_WAIT = 60  # seconds — cap on any single wait

# HTTP status codes that should trigger a retry (429 + transient 5xx).
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

# (connect, read) timeout for every outbound request. Without this, a hung
# ZoomInfo connection would stall the sync forever.
REQUEST_TIMEOUT = (10, 60)

# State keys persisted via op.checkpoint between syncs. Centralized so a typo
# at one call-site can't silently break incremental sync (a wrong key reads as
# "no prior state" and re-pulls the full universe).
STATE_CONTACTS_LAST_UPDATED = "contacts_last_updated"
STATE_COMPANIES_LAST_UPDATED = "companies_last_updated"
STATE_SCOOPS_LAST_UPDATED = "scoops_last_updated"
STATE_INTENT_LAST_UPDATED = "intent_last_updated"
STATE_NEWS_LAST_UPDATED = "news_last_updated"

# In-memory token cache. Guarded by _token_cache_lock — enrich worker threads
# can race on token refresh otherwise, wasting credits on duplicate /oauth/v1/token
# calls and potentially tripping per-IP rate limits.
_token_cache = {"access_token": None, "expires_at": 0}
_token_cache_lock = threading.Lock()


# ─────────────────────────────────────────────
# CONFIGURATION VALIDATION
# ─────────────────────────────────────────────
def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or any optional
            parameter holds an invalid value.
    """
    # Required credentials for the OAuth Client Credentials Flow.
    required_configs = ["client_id", "client_secret"]
    for key in required_configs:
        if not configuration.get(key):
            raise ValueError(f"Missing required configuration value: {key}")

    # enrich_filter, when contact enrichment is enabled, must be one of the
    # supported eligibility filters — an invalid value would silently spend
    # credits on the wrong contacts.
    if _bool_config(configuration, "enrich_contacts"):
        enrich_filter = configuration.get("enrich_filter", "has_email_or_phone").strip()
        valid_filters = {"has_email", "has_phone", "has_email_or_phone", "all"}
        if enrich_filter and enrich_filter not in valid_filters:
            raise ValueError(
                f"Invalid enrich_filter '{enrich_filter}'. "
                f"Valid values: {', '.join(sorted(valid_filters))}"
            )

    # intent_topics is capped at 50 by the ZoomInfo API.
    topics = _list_config(configuration, "intent_topics")
    if len(topics) > 50:
        raise ValueError(
            f"Too many intent_topics ({len(topics)}). The ZoomInfo Intent API "
            f"accepts at most 50 topics per request."
        )


# ─────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────
def get_access_token(configuration: dict) -> str:
    """
    Returns a valid ZoomInfo Bearer token using Client Credentials Flow.
    Caches the token and only re-authenticates when within 60s of expiry.

    Thread-safe: holds _token_cache_lock across the cache-check / refresh /
    cache-write window so concurrent enrich workers don't double-fetch.
    """
    now = time.time()

    with _token_cache_lock:
        if _token_cache["access_token"] and now < _token_cache["expires_at"] - 60:
            log.debug("Using cached access token")
            return _token_cache["access_token"]

        log.info("Requesting new ZoomInfo access token...")

        client_id = configuration["client_id"]
        client_secret = configuration["client_secret"]
        encoded = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()

        response = requests.post(
            TOKEN_URL,
            headers={
                "Authorization": f"Basic {encoded}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={"grant_type": "client_credentials"},
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"ZoomInfo authentication failed ({response.status_code}). "
                f"Please check your client_id and client_secret. "
                f"Details: {response.text[:200]}"
            )

        token_data = response.json()
        _token_cache["access_token"] = token_data["access_token"]
        _token_cache["expires_at"] = now + token_data["expires_in"]

        log.info("Successfully obtained ZoomInfo access token")
        return _token_cache["access_token"]


def _invalidate_token_cache():
    """Clear the cached token so the next get_access_token() refreshes. Thread-safe."""
    with _token_cache_lock:
        _token_cache["access_token"] = None
        _token_cache["expires_at"] = 0


# ─────────────────────────────────────────────
# API HELPERS
# ─────────────────────────────────────────────
def get_headers(token: str) -> dict:
    """Headers for JSON:API POST requests (search/enrich)."""
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/vnd.api+json",
        "Accept": "application/vnd.api+json",
    }


def get_headers_get(token: str) -> dict:
    """Headers for GET requests (no Content-Type needed)."""
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.api+json",
    }


def build_body(endpoint: str, attributes: dict) -> dict:
    """Wraps filter/input attributes in the JSON:API request envelope."""
    return {"data": {"type": JSONAPI_TYPE[endpoint], "attributes": attributes}}


def _sleep_for_retry(response: requests.Response, current_wait: int):
    """
    Sleeps for the appropriate wait time on a retryable response and returns
    (wait_secs_used, next_current_wait). Honors Retry-After when present,
    otherwise uses exponential backoff capped at RETRY_MAX_WAIT.
    """
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            wait_secs = int(retry_after)
            time.sleep(wait_secs)
            return wait_secs, current_wait
        except ValueError:
            pass  # fall through to backoff

    wait_secs = min(current_wait, RETRY_MAX_WAIT)
    time.sleep(wait_secs)
    return wait_secs, current_wait * 2


def post_with_retry(
    url: str, headers: dict, json_body: dict, params: dict = None
) -> requests.Response:
    """
    POST with exponential backoff on retryable failures: 429 + transient 5xx
    + connection errors / timeouts. Honors Retry-After on 429. Raises
    RuntimeError after MAX_RETRIES.
    """
    wait = RETRY_BASE_WAIT

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(
                url,
                headers=headers,
                json=json_body,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES:
                raise RuntimeError(f"Network error on {url} after {MAX_RETRIES} attempts: {e}")
            wait_secs = min(wait, RETRY_MAX_WAIT)
            log.warning(
                f"Network error on {url} ({type(e).__name__}) — backing off {wait_secs}s (attempt {attempt}/{MAX_RETRIES})"
            )
            time.sleep(wait_secs)
            wait *= 2
            continue

        if response.status_code in RETRY_STATUS_CODES:
            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"ZoomInfo API failed after {MAX_RETRIES} retries on {url} "
                    f"[HTTP {response.status_code}]: {response.text[:200]}"
                )
            wait_secs, wait = _sleep_for_retry(response, wait)
            log.warning(
                f"Retryable status {response.status_code} on {url} — waited {wait_secs}s "
                f"(attempt {attempt}/{MAX_RETRIES})"
            )
            continue

        return response

    raise RuntimeError(f"Exhausted retries on {url}")


def get_with_retry(url: str, headers: dict, params: dict = None) -> requests.Response:
    """
    GET with exponential backoff on retryable failures: 429 + transient 5xx
    + connection errors / timeouts.
    """
    wait = RETRY_BASE_WAIT

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES:
                raise RuntimeError(f"Network error on {url} after {MAX_RETRIES} attempts: {e}")
            wait_secs = min(wait, RETRY_MAX_WAIT)
            log.warning(
                f"Network error on {url} ({type(e).__name__}) — backing off {wait_secs}s (attempt {attempt}/{MAX_RETRIES})"
            )
            time.sleep(wait_secs)
            wait *= 2
            continue

        if response.status_code in RETRY_STATUS_CODES:
            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"ZoomInfo API failed after {MAX_RETRIES} retries on {url} "
                    f"[HTTP {response.status_code}]: {response.text[:200]}"
                )
            wait_secs, wait = _sleep_for_retry(response, wait)
            log.warning(
                f"Retryable status {response.status_code} on {url} — waited {wait_secs}s "
                f"(attempt {attempt}/{MAX_RETRIES})"
            )
            continue

        return response

    raise RuntimeError(f"Exhausted retries on {url}")


def _warn_if_truncated(endpoint: str, total_results) -> bool:
    """
    Logs a WARNING when a Search query's universe exceeds the API result ceiling.

    The ZoomInfo Search API returns at most SEARCH_RESULT_CEILING records per
    query; records beyond that are not retrievable by paging further. When the
    reported totalResults is larger, this run will only capture the first
    SEARCH_RESULT_CEILING records, and incremental syncs will not backfill the
    remainder. Warn so the operator can narrow the query.
    Args:
        endpoint: the Search endpoint path being paginated (for the log message).
        total_results: meta.totalResults from the first page (may be None/non-int).
    Returns:
        True if a truncation warning was emitted, otherwise False.
    """
    count = _safe_int(total_results)
    if count is not None and count > SEARCH_RESULT_CEILING:
        log.warning(
            f"{endpoint}: totalResults={count} exceeds the ZoomInfo Search API "
            f"ceiling of {SEARCH_RESULT_CEILING} records per query. Only the first "
            f"{SEARCH_RESULT_CEILING} will be synced, and incremental runs will NOT "
            f"backfill the remainder. Narrow the query (e.g. a tighter `countries` "
            f"filter) so each query's universe stays under {SEARCH_RESULT_CEILING}."
        )
        return True
    return False


def paginate(configuration: dict, endpoint: str, attributes: dict):
    """
    Generator that paginates through ZoomInfo Search results.

    Confirmed API behaviour:
    - Pagination: URL query params page[size] and page[number]
    - Filters: go in JSON:API body data.attributes
    - Response: data[] of {id, type, attributes} resource objects
    - Metadata: meta.page.total (total pages), meta.totalResults
    - Auto-retries once on 401, retries with backoff on 429
    """
    page = 1
    token = get_access_token(configuration)
    body = build_body(endpoint, attributes)

    while True:
        params = {
            "page[size]": PAGE_SIZE,
            "page[number]": page,
        }

        response = post_with_retry(
            f"{SEARCH_BASE}{endpoint}", headers=get_headers(token), json_body=body, params=params
        )

        if response.status_code == 401:
            log.warning(f"401 on {endpoint} page {page} — refreshing token and retrying...")
            _invalidate_token_cache()
            token = get_access_token(configuration)
            response = post_with_retry(
                f"{SEARCH_BASE}{endpoint}",
                headers=get_headers(token),
                json_body=body,
                params=params,
            )

        if response.status_code != 200:
            # ZoomInfo returns 400 PFAPI0004 ("Page number requested is greater
            # than the available results") when meta.page.total is stale or
            # over-reported. Treat it as a clean end-of-pagination signal rather
            # than a fatal error so syncs against small datasets don't crash.
            if response.status_code == 400 and "PFAPI0004" in response.text and page > 1:
                log.debug(
                    f"{endpoint} page {page}: PFAPI0004 — server reports no more pages, stopping"
                )
                break
            raise RuntimeError(
                f"ZoomInfo API error on {endpoint} page {page} "
                f"[HTTP {response.status_code}]: {response.text}"
            )

        payload = response.json()

        if page == 1:
            meta = payload.get("meta", {})
            total_results = meta.get("totalResults")
            log.info(
                f"{endpoint}: totalResults={total_results} "
                f"totalPages={meta.get('page', {}).get('total')}"
            )
            # Surface silent truncation: the Search API caps a single query at
            # SEARCH_RESULT_CEILING records. If the universe is larger, only the
            # first ceiling records are retrievable — and because incremental
            # syncs advance the cursor past whatever was fetched, the dropped
            # records are never backfilled on later runs. Warn loudly so the
            # operator knows to narrow the query.
            _warn_if_truncated(endpoint, total_results)

        results = payload.get("data", [])

        if not results:
            if page == 1:
                log.info(
                    f"{endpoint}: zero results on page 1 — check filter "
                    f"(countries, intent topics, incremental cursor)"
                )
            else:
                log.debug(f"No results on {endpoint} page {page} — done")
            break

        log.debug(f"{endpoint} page {page}: {len(results)} records")
        yield results

        total_pages = payload.get("meta", {}).get("page", {}).get("total", 1)
        if page >= total_pages:
            break
        if MAX_PAGES is not None and page >= MAX_PAGES:
            log.debug(f"{endpoint}: reached MAX_PAGES={MAX_PAGES}, stopping early")
            break

        page += 1


def paginate_enrich_scoops(configuration: dict, company_id: str):
    """
    Paginates through Enrich Scoops results for a single company.
    Scoops Enrich is per-company (not batch) — 1 credit per company.
    """
    page = 1
    token = get_access_token(configuration)
    body = build_body(ENDPOINT_SCOOPS_ENRICH, {"companyId": company_id})

    while True:
        params = {
            "page[size]": PAGE_SIZE,
            "page[number]": page,
            "sort": "-originalPublishedDate",
        }

        response = post_with_retry(
            f"{SEARCH_BASE}{ENDPOINT_SCOOPS_ENRICH}",
            headers=get_headers(token),
            json_body=body,
            params=params,
        )

        if response.status_code == 401:
            _invalidate_token_cache()
            token = get_access_token(configuration)
            response = post_with_retry(
                f"{SEARCH_BASE}{ENDPOINT_SCOOPS_ENRICH}",
                headers=get_headers(token),
                json_body=body,
                params=params,
            )

        if response.status_code == 403:
            # Raise so the streaming helper (_stream_per_company_enrich) can
            # emit a single warning and abort once, rather than this generator
            # logging the same warning per company.
            raise RuntimeError(
                "Scoops enrich access denied "
                f"[HTTP 403] for companyId={company_id}: {response.text[:200]}"
            )

        if response.status_code != 200:
            # PFAPI0004 = pagination overshoot; treat as clean end-of-pages.
            if response.status_code == 400 and "PFAPI0004" in response.text and page > 1:
                log.debug(
                    f"Scoops enrich for companyId={company_id} page {page}: "
                    f"PFAPI0004 — stopping"
                )
                return
            raise RuntimeError(
                f"Scoops enrich failed for companyId={company_id} "
                f"[HTTP {response.status_code}]: {response.text[:200]}"
            )

        payload = response.json()
        results = payload.get("data", [])

        if not results:
            break

        yield results

        total_pages = payload.get("meta", {}).get("page", {}).get("total", 1)
        if page >= total_pages:
            break
        if MAX_PAGES is not None and page >= MAX_PAGES:
            break

        page += 1


def paginate_enrich_technologies(configuration: dict, company_id: str):
    """
    Fetches Technology Enrich results for a single company.

    The Technologies Enrich endpoint IGNORES page[number] and returns the full
    technology stack for the company in one response (~2,600 records observed
    for large companies). Probing the endpoint with page[number]=1..5 returned
    identical record sets every time. The endpoint also reports only
    `meta.totalResults` (no `meta.page.total`), reinforcing that it is not a
    real paginating endpoint. So we fetch once and return.

    Yielding a single page keeps the call shape compatible with the streaming
    helper, which expects an iterator of page-result lists.

    Input format: flat {"companyId": id} — NOT matchCompanyInput.
    No outputFields param — API returns all fields automatically.
    """
    token = get_access_token(configuration)
    body = build_body(ENDPOINT_TECHNOLOGIES_ENRICH, {"companyId": company_id})
    params = {"page[size]": PAGE_SIZE, "page[number]": 1}

    response = post_with_retry(
        f"{SEARCH_BASE}{ENDPOINT_TECHNOLOGIES_ENRICH}",
        headers=get_headers(token),
        json_body=body,
        params=params,
    )

    if response.status_code == 401:
        _invalidate_token_cache()
        token = get_access_token(configuration)
        response = post_with_retry(
            f"{SEARCH_BASE}{ENDPOINT_TECHNOLOGIES_ENRICH}",
            headers=get_headers(token),
            json_body=body,
            params=params,
        )

    if response.status_code == 403:
        # Raise so the streaming helper (_stream_per_company_enrich) can emit a
        # single warning and abort once, rather than this generator logging the
        # same warning per company.
        raise RuntimeError(
            "Technologies enrich access denied "
            f"[HTTP 403] for companyId={company_id}: {response.text[:200]}"
        )

    if response.status_code != 200:
        raise RuntimeError(
            f"Technologies enrich failed for companyId={company_id} "
            f"[HTTP {response.status_code}]: {response.text[:200]}"
        )

    results = response.json().get("data", [])
    if results:
        yield results


# ─────────────────────────────────────────────
# CONFIGURATION HELPERS
# ─────────────────────────────────────────────
def _bool_config(configuration: dict, key: str) -> bool:
    """Parses a boolean config value that may be a string or bool."""
    val = configuration.get(key, False)
    if isinstance(val, str):
        return val.lower() in ("true", "1", "yes")
    return bool(val)


def _list_config(configuration: dict, key: str) -> list:
    """Parses a comma-separated config value into a list of stripped strings."""
    raw = configuration.get(key, "").strip()
    return [v.strip() for v in raw.split(",") if v.strip()] if raw else []


def _fields_config(configuration: dict, key: str, defaults: list) -> list:
    """Parses output fields config, falling back to defaults if blank."""
    raw = configuration.get(key, "").strip()
    return [f.strip() for f in raw.split(",") if f.strip()] if raw else defaults


def _safe_int(value):
    """
    Coerces ZoomInfo API responses to int. The API has been observed to
    return numeric fields as either int, float, or stringified numbers
    depending on the endpoint. Returns None for None/empty/uncoerceable
    values rather than raising — Fivetran handles None as NULL.
    """
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def _safe_float(value):
    """Float counterpart to _safe_int. See that function for rationale."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_utc_datetime(value):
    """
    Coerces a ZoomInfo date response into a Fivetran UTC_DATETIME-acceptable string.
    The SDK's UTC_DATETIME parser only accepts full ISO 8601 timestamps
    (YYYY-MM-DDTHH:MM:SS[.f]±HHMM), but ZoomInfo sometimes returns a bare date
    (YYYY-MM-DD) for fields like technologies.createdDate. Bare dates are promoted
    to midnight UTC; full timestamps pass through unchanged; anything else
    (empty, None, unrecognized shape) becomes None so it lands as NULL rather
    than crashing the whole sync.
    """
    if not value or not isinstance(value, str):
        return None
    if "T" in value:
        return value
    if len(value) == 10 and value[4] == "-" and value[7] == "-":
        return f"{value}T00:00:00Z"
    return None


def _parse_iso_for_compare(value):
    """
    Parse a ZoomInfo-shaped ISO 8601 timestamp into a timezone-aware datetime
    for safe max-cursor comparison.

    String comparison on ISO strings *almost* works for the Z-suffixed
    timestamps ZoomInfo returns today, but breaks the moment any record arrives
    with a non-UTC offset (e.g. ``+02:00``). Parse to a real datetime instead.

    Returns None for falsy / unparseable input — callers should treat that as
    "skip this record for cursor purposes" rather than crashing the sync.
    """
    if not value or not isinstance(value, str):
        return None
    # datetime.fromisoformat accepts "Z" suffix from Python 3.11+, but we still
    # support 3.10 (per the CI matrix) so normalise it here.
    s = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        # Bare-date inputs like "2026-05-19" — promote to midnight UTC.
        if len(value) == 10 and value[4] == "-" and value[7] == "-":
            try:
                return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
            except ValueError:
                return None
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _max_cursor(current, candidate):
    """
    Returns the later of two ISO timestamp strings, treating None as "no value".
    Comparison is performed on parsed datetimes (timezone-aware) so mixed-offset
    responses are handled correctly. The returned value is whichever input
    string compared larger — we preserve the original string so it round-trips
    back into state unchanged.
    """
    if not candidate:
        return current
    if not current:
        return candidate
    c_dt = _parse_iso_for_compare(current)
    n_dt = _parse_iso_for_compare(candidate)
    if c_dt is None:
        return candidate
    if n_dt is None:
        return current
    return candidate if n_dt > c_dt else current


def build_search_filter(configuration: dict) -> dict:
    """
    Builds the JSON:API `attributes` filter used by every Search endpoint.

    Reads `countries` from configuration (comma-separated). Defaults to
    DEFAULT_COUNTRY when blank. Currently supports a single country only —
    if multiple are provided, the first is used and a warning is logged.
    """
    raw = configuration.get("countries", "").strip()
    countries = [c.strip() for c in raw.split(",") if c.strip()] if raw else [DEFAULT_COUNTRY]

    if len(countries) > 1:
        log.warning(
            f"Multiple countries configured ({countries}) but the ZoomInfo Search API "
            f"only accepts a single country per request. Using first value: '{countries[0]}'. "
            f"To sync additional countries, run a separate connector instance per country."
        )

    return {"country": countries[0]}


def _iso_to_yyyymmdd(iso_string: str) -> str:
    """
    Coerces an ISO 8601 timestamp like '2026-05-19T23:31:00Z' down to the
    'YYYY-MM-DD' format the ZoomInfo Search filter API expects. If the input
    already looks like a date (no 'T'), it's passed through unchanged.
    Returns None for falsy input.
    """
    if not iso_string:
        return None
    return iso_string.split("T", 1)[0] if "T" in iso_string else iso_string


def apply_incremental_filter(
    base_filter: dict,
    configuration: dict,
    state: dict,
    state_key: str,
    api_field: str,
) -> dict:
    """
    Returns a new filter dict that includes an incremental "since" predicate
    when prior state exists and `full_refresh` is not enabled.

    ZoomInfo Search filter API uses flat per-entity field names with a
    YYYY-MM-DD date format (no time, no operators). Field names confirmed
    via /gtm/data/v1/lookup/search?filter[entity]=<entity>&filter[fieldType]=input:

      contacts: lastUpdatedDateAfter
      companies: (no incremental filter available — always full-replace)
      scoops:    publishedStartDate
      intent:    signalStartDate
      news:      pageDateMin

    Caller passes the correct `api_field` for the endpoint it's syncing.
    If `api_field` is None or empty, no incremental predicate is added
    (used for companies, which has no server-side incremental option).
    """
    if _bool_config(configuration, "full_refresh"):
        log.info(f"full_refresh=true — running full sync for {state_key}")
        return dict(base_filter)

    since = state.get(state_key)
    if not since:
        log.info(f"No prior state for {state_key} — running full sync (first run)")
        return dict(base_filter)

    if not api_field:
        log.info(
            f"No server-side incremental filter available for {state_key} — running full sync. "
            f"State is still tracked for future use."
        )
        return dict(base_filter)

    since_date = _iso_to_yyyymmdd(since)
    log.info(f"Incremental sync for {state_key}: {api_field} >= {since_date}")
    return {**base_filter, api_field: since_date}


def get_enrich_config(configuration: dict) -> dict:
    """
    Parses and validates the contact-enrichment configuration. Returns a dict
    of enrichment options, or None if enrichment is disabled.
    """
    enrich_enabled = _bool_config(configuration, "enrich_contacts")
    if not enrich_enabled:
        return None

    enrich_filter = configuration.get("enrich_filter", "has_email_or_phone").strip()
    valid_filters = {"has_email", "has_phone", "has_email_or_phone", "all"}
    if enrich_filter not in valid_filters:
        log.warning(
            f"Invalid enrich_filter '{enrich_filter}' — defaulting to 'has_email_or_phone'. "
            f"Valid values: {', '.join(sorted(valid_filters))}"
        )
        enrich_filter = "has_email_or_phone"

    mgmt_levels = _list_config(configuration, "enrich_management_levels")
    output_fields = _fields_config(
        configuration, "enrich_output_fields", DEFAULT_CONTACT_ENRICH_FIELDS
    )

    log.info(
        f"Contact enrichment enabled — filter={enrich_filter}, "
        f"managementLevels={mgmt_levels or 'all'}, "
        f"outputFields={len(output_fields)} fields"
    )

    return {
        "filter": enrich_filter,
        "mgmt_levels": mgmt_levels,
        "output_fields": output_fields,
    }


def should_enrich(record_attributes: dict, enrich_filter: str) -> bool:
    """
    Determines whether a contact from Search results should be enriched,
    based on the customer's chosen filter and ZoomInfo's availability hints.

    Using hints (hasEmail, hasDirectPhone, hasMobilePhone) avoids spending
    credits on contacts where ZoomInfo doesn't have the requested data.
    """
    if enrich_filter == "all":
        return True

    has_email = record_attributes.get("hasEmail", False)
    has_phone = record_attributes.get("hasDirectPhone", False) or record_attributes.get(
        "hasMobilePhone", False
    )

    if enrich_filter == "has_email":
        return bool(has_email)
    if enrich_filter == "has_phone":
        return bool(has_phone)
    if enrich_filter == "has_email_or_phone":
        return bool(has_email or has_phone)

    return False


def matches_mgmt_level(record_attributes: dict, mgmt_levels: list) -> bool:
    """
    Returns True if the contact's management level is in the customer's
    configured list, or if no filter is set (enrich all levels).

    managementLevel from the Enrich API returns a LIST (e.g. ["C-Level"]),
    not a string. We check if any configured level matches any value in the list.
    """
    if not mgmt_levels:
        return True
    raw = record_attributes.get("managementLevel", [])
    if isinstance(raw, str):
        raw = [raw]
    contact_levels = [v.lower() for v in raw]
    # Exact (case-insensitive) match — NOT substring. Substring containment
    # would treat a configured "Manager" as matching the API value
    # "Non Manager", enriching extra contacts and spending credits.
    return any(
        configured.lower() == contact_level
        for configured in mgmt_levels
        for contact_level in contact_levels
    )


# ─────────────────────────────────────────────
# ENRICH HELPERS
# ─────────────────────────────────────────────
def _enrich_post(configuration: dict, endpoint: str, attributes: dict) -> list:
    """
    Shared helper for all POST-based enrich calls.
    Handles 401 token refresh and returns data[] from the response.
    """
    token = get_access_token(configuration)
    body = build_body(endpoint, attributes)

    response = post_with_retry(
        f"{SEARCH_BASE}{endpoint}", headers=get_headers(token), json_body=body
    )

    if response.status_code == 401:
        log.warning(f"401 on {endpoint} — refreshing token and retrying...")
        _invalidate_token_cache()
        token = get_access_token(configuration)
        response = post_with_retry(
            f"{SEARCH_BASE}{endpoint}", headers=get_headers(token), json_body=body
        )

    if response.status_code != 200:
        raise RuntimeError(
            f"ZoomInfo Enrich API error on {endpoint} [HTTP {response.status_code}]: {response.text}"
        )

    return response.json().get("data", [])


def enrich_contacts_batch(configuration: dict, person_ids: list, output_fields: list) -> list:
    """Enriches a batch of up to 25 contacts by personId."""
    return _enrich_post(
        configuration,
        ENDPOINT_CONTACTS_ENRICH,
        {
            "matchPersonInput": [{"personId": pid} for pid in person_ids],
            "outputFields": output_fields,
        },
    )


def enrich_companies_batch(configuration: dict, company_ids: list, output_fields: list) -> list:
    """Enriches a batch of up to 25 companies by companyId."""
    return _enrich_post(
        configuration,
        ENDPOINT_COMPANIES_ENRICH,
        {
            "matchCompanyInput": [{"companyId": cid} for cid in company_ids],
            "outputFields": output_fields,
        },
    )


def enrich_corp_hierarchy_batch(
    configuration: dict, company_ids: list, output_fields: list
) -> list:
    """Enriches corporate hierarchy for a batch of up to 25 companies."""
    return _enrich_post(
        configuration,
        ENDPOINT_CORP_HIER_ENRICH,
        {
            "matchCompanyInput": [{"companyId": cid} for cid in company_ids],
            "outputFields": output_fields,
        },
    )


# ─────────────────────────────────────────────
# SYNC FUNCTIONS — SEARCH (free)
# ─────────────────────────────────────────────
def sync_contacts(
    configuration: dict,
    state: dict,
    enrich_config: dict,
    search_filter: dict,
    cumulative_state: dict = None,
):
    """
    Syncs contacts from ZoomInfo Search, then optionally enriches a subset.

    Search pass:
    - Upserts basic contact records to the 'contacts' table
    - Collects contact IDs eligible for enrichment based on customer config

    Enrich pass (if enabled):
    - Batches eligible IDs into groups of 25
    - Calls Enrich API for full profile data
    - Upserts enriched records to the 'contacts_enriched' table
    - Logs match status and skips NO_MATCH records (no credit charged)
    - Applies managementLevel filter post-enrich if configured

    State semantics:
    - When prior state exists, only contacts with `lastUpdatedDate >= state`
      are fetched (incremental). Override with `full_refresh=true`.
    - `latest_date` is the max `lastUpdatedDate` seen this run; checkpointed
      back as the new state so the next run picks up from here.
    """
    log.info("Starting contacts sync...")

    latest_date = state.get("contacts_last_updated")
    count = 0
    enrich_queue = []

    effective_filter = apply_incremental_filter(
        search_filter, configuration, state, "contacts_last_updated", "lastUpdatedDateAfter"
    )

    for page_results in paginate(configuration, ENDPOINT_CONTACTS, effective_filter):
        for record in page_results:
            record_id = record.get("id")
            a = record.get("attributes", {})

            if count == 0:
                log.debug(f"Sample contact attribute keys: {list(a.keys())}")

            company = a.get("company") or {}
            company_id = company.get("id")
            company_nm = company.get("name")

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(
                table="contacts",
                data={
                    "id": record_id,
                    "first_name": a.get("firstName"),
                    "last_name": a.get("lastName"),
                    "middle_name": a.get("middleName"),
                    "job_title": a.get("jobTitle"),
                    "company_id": company_id,
                    "company_name": company_nm,
                    "contact_accuracy_score": _safe_float(a.get("contactAccuracyScore")),
                    "has_email": a.get("hasEmail"),
                    "has_direct_phone": a.get("hasDirectPhone"),
                    "has_mobile_phone": a.get("hasMobilePhone"),
                    "has_supplemental_email": a.get("hasSupplementalEmail"),
                    "direct_phone_do_not_call": a.get("directPhoneDoNotCall"),
                    "mobile_phone_do_not_call": a.get("mobilePhoneDoNotCall"),
                    "last_updated_date": _safe_utc_datetime(a.get("lastUpdatedDate")),
                    "valid_date": _safe_utc_datetime(a.get("validDate")),
                    "raw_attributes": json.dumps(a) if a else None,
                },
            )
            count += 1

            record_date = a.get("lastUpdatedDate")
            latest_date = _max_cursor(latest_date, record_date)

            if enrich_config and record_id and should_enrich(a, enrich_config["filter"]):
                enrich_queue.append(record_id)

            # Intra-table checkpoint so a crash mid-pull doesn't re-fetch from
            # the previous run's cursor — resume picks up from the latest_date
            # we've seen so far.
            if cumulative_state is not None and count % CHECKPOINT_EVERY_N_ROWS == 0:
                if latest_date:
                    cumulative_state[STATE_CONTACTS_LAST_UPDATED] = latest_date
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
                # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
                op.checkpoint(state=cumulative_state)

    log.info(f"Contacts sync complete — {count} records upserted")

    if enrich_config and enrich_queue:
        log.info(
            f"Starting contact enrichment — {len(enrich_queue)} contacts queued "
            f"(filter={enrich_config['filter']})"
        )
        _run_contact_enrichment(configuration, enrich_queue, enrich_config)

    return latest_date


def sync_companies(configuration: dict, state: dict, search_filter: dict):
    """
    Syncs companies from ZoomInfo Search Companies.
    Returns (None, list_of_company_ids) — companies has no incremental cursor,
    so the return tuple's first element is always None for forward-compat with
    the update() caller's signature.

    The ZoomInfo Search filter API does not expose a server-side lastUpdated
    field for the company entity (verified via /lookup/search), so every sync
    re-pulls the full universe. To make deletions propagate to the destination
    instead of leaving stale rows forever, we truncate then upsert.
    op.truncate() marks every previously-synced row with _fivetran_deleted=TRUE;
    rows we then upsert in this run land with _fivetran_deleted=FALSE.

    Crash-safety: we paginate the full universe into an in-memory buffer first,
    then truncate + upsert in one block. If pagination throws (network, 5xx,
    auth), the destination table is left untouched — vastly better than the
    naive "truncate then stream upserts" pattern, which leaves the destination
    with every row soft-deleted if pagination dies partway through.

    Confirmed response fields (from data[].attributes):
      name, website, city, state, country, employeeCount, revenue

    Note: record.id holds the company ID (not in attributes).
    """
    log.info("Starting companies sync (full-replace via truncate + upsert)...")

    # Phase 1: buffer the full universe before any destructive op. Tradeoff —
    # peak memory scales with universe × ~1 KB/row. At the documented 10K-company
    # scale this is ~10 MB, well under the 1 GB Fivetran cloud limit. For very
    # large universes, narrow the `countries` filter or split across multiple
    # connectors instead of raising PAGE_SIZE.
    buffered_rows = []
    company_ids = []

    for page_results in paginate(configuration, ENDPOINT_COMPANIES, search_filter):
        for record in page_results:
            record_id = record.get("id")
            a = record.get("attributes", {})

            if not buffered_rows:
                log.debug(f"Sample company attribute keys: {list(a.keys())}")

            buffered_rows.append(
                {
                    "id": record_id,
                    "name": a.get("name"),
                    "website": a.get("website"),
                    "city": a.get("city"),
                    "state": a.get("state"),
                    "country": a.get("country"),
                    "employee_count": _safe_int(a.get("employeeCount")),
                    "revenue": _safe_int(a.get("revenue")),
                    "raw_attributes": json.dumps(a) if a else None,
                }
            )
            if record_id:
                company_ids.append(record_id)

    log.info(f"Companies sync — buffered {len(buffered_rows)} rows; truncating and upserting...")

    # Phase 2: pagination succeeded — now do the destructive swap.
    # The 'truncate' operation soft-deletes every previously-synced row
    # (marks _fivetran_deleted=TRUE); rows re-upserted below land with
    # _fivetran_deleted=FALSE, so deletions on the ZoomInfo side propagate.
    op.truncate(table="companies")
    for row in buffered_rows:
        # The 'upsert' operation is used to insert or update data in the destination table.
        op.upsert(table="companies", data=row)

    log.info(f"Companies sync complete — {len(buffered_rows)} records upserted")
    return None, company_ids


def sync_scoops(
    configuration: dict, state: dict, search_filter: dict, cumulative_state: dict = None
):
    """
    Syncs scoops from ZoomInfo Search Scoops.

    Confirmed response fields (from data[].attributes):
      company: {id, name}, description, topics, types,
      publishedDate, originalPublishedDate

    Note: 'country' is the only confirmed working filter for scoops.
    """
    log.info("Starting scoops sync...")

    latest_date = state.get("scoops_last_updated")
    count = 0

    effective_filter = apply_incremental_filter(
        search_filter, configuration, state, "scoops_last_updated", "publishedStartDate"
    )

    for page_results in paginate(configuration, ENDPOINT_SCOOPS, effective_filter):
        for record in page_results:
            record_id = record.get("id")
            a = record.get("attributes", {})

            if count == 0:
                log.debug(f"Sample scoop attribute keys: {list(a.keys())}")

            company = a.get("company") or {}
            company_id = company.get("id")
            company_nm = company.get("name")

            topics = a.get("topics")
            types = a.get("types")

            # Upsert one scoop row into the destination. 'upsert' inserts or
            # updates the record keyed by its primary key ('id').
            op.upsert(
                table="scoops",
                data={
                    "id": record_id,
                    "company_id": company_id,
                    "company_name": company_nm,
                    "description": a.get("description"),
                    "topics": json.dumps(topics) if topics else None,
                    "types": json.dumps(types) if types else None,
                    "published_date": _safe_utc_datetime(a.get("publishedDate")),
                    "original_published_date": _safe_utc_datetime(a.get("originalPublishedDate")),
                    "raw_attributes": json.dumps(a) if a else None,
                },
            )
            count += 1

            record_date = a.get("publishedDate")
            latest_date = _max_cursor(latest_date, record_date)

            if cumulative_state is not None and count % CHECKPOINT_EVERY_N_ROWS == 0:
                if latest_date:
                    cumulative_state[STATE_SCOOPS_LAST_UPDATED] = latest_date
                # Checkpoint mid-table so progress is flushed and the next sync
                # can resume from the latest scoops cursor.
                op.checkpoint(state=cumulative_state)

    log.info(f"Scoops sync complete — {count} records upserted")
    return latest_date


def get_valid_intent_topics(configuration: dict) -> set:
    """
    Fetches the list of licensed intent topic names from the Lookup endpoint.
    Returns a set of valid topic name strings (case-sensitive).
    Returns empty set on any error — caller should skip validation if empty.
    """
    try:
        token = get_access_token(configuration)
        resp = get_with_retry(
            f"{SEARCH_BASE}/gtm/data/v1/lookup/intent-topics", headers=get_headers_get(token)
        )
        if resp.status_code != 200:
            log.warning(
                f"Could not fetch intent topics lookup [{resp.status_code}] — skipping validation"
            )
            return set()
        return {
            item.get("attributes", {}).get("name")
            for item in resp.json().get("data", [])
            if item.get("attributes", {}).get("name")
        }
    except (requests.exceptions.RequestException, ValueError, KeyError, TypeError) as e:
        # Network failures (RequestException), JSON decode errors (ValueError),
        # and malformed-payload access errors (KeyError/TypeError). Unexpected
        # exceptions are allowed to surface rather than be silently swallowed.
        log.warning(f"Intent topics lookup failed: {e} — skipping validation")
        return set()


def sync_intent(configuration: dict, state: dict, cumulative_state: dict = None):
    """
    Syncs Intent Signals from ZoomInfo Search Intent.
    Free endpoint — no credits consumed (records and requests counted).

    Requires at least 1 intent topic in the request (up to 50).
    Topics are customer-configurable via 'intent_topics' in configuration.
    Topics must be exact names from the customer's licensed intent topic list.
    Use GET /gtm/data/v1/lookup/intent-topics to see valid values.

    Response fields: company.{id,name}, topic, signalScore, audienceStrength,
                     signalDate, category

    Gracefully skips on 403 — customer may not have Intent product licensed
    or the app scope may not include api:data:intent.
    """
    topics_raw = configuration.get("intent_topics", "").strip()
    topics = [t.strip() for t in topics_raw.split(",") if t.strip()]

    if not topics:
        log.info("Intent sync skipped — no intent_topics configured")
        return state.get("intent_last_updated")

    # Validate topics against the licensed list — warn and drop invalid ones
    valid_topics = get_valid_intent_topics(configuration)
    if valid_topics:
        invalid = [t for t in topics if t not in valid_topics]
        if invalid:
            log.warning(
                f"Removing {len(invalid)} invalid intent topic(s): {invalid}. "
                f"Valid topics for this account: {sorted(valid_topics)}"
            )
            topics = [t for t in topics if t in valid_topics]
        if not topics:
            log.warning("Intent sync skipped — no valid topics remain after validation")
            return state.get("intent_last_updated")

    log.info(f"Starting intent sync — {len(topics)} topic(s)...")

    latest_date = state.get("intent_last_updated")
    count = 0

    intent_filter = apply_incremental_filter(
        {"topics": topics}, configuration, state, "intent_last_updated", "signalStartDate"
    )

    try:
        for page_results in paginate(configuration, ENDPOINT_INTENT, intent_filter):
            for record in page_results:
                record_id = record.get("id")
                a = record.get("attributes", {})

                if count == 0:
                    log.debug(f"Sample intent attribute keys: {list(a.keys())}")

                company = a.get("company") or {}
                company_id = company.get("id")
                company_nm = company.get("name")

                # Upsert one intent-signal row into the destination. 'upsert'
                # inserts or updates the record keyed by its primary key ('id').
                op.upsert(
                    table="intent",
                    data={
                        "id": record_id,
                        "company_id": company_id,
                        "company_name": company_nm,
                        "topic": a.get("topic"),
                        "category": a.get("category"),
                        "signal_score": _safe_float(a.get("signalScore")),
                        "audience_strength": _safe_float(a.get("audienceStrength")),
                        "signal_date": _safe_utc_datetime(a.get("signalDate")),
                        "raw_attributes": json.dumps(a) if a else None,
                    },
                )
                count += 1

                record_date = a.get("signalDate")
                latest_date = _max_cursor(latest_date, record_date)

                if cumulative_state is not None and count % CHECKPOINT_EVERY_N_ROWS == 0:
                    if latest_date:
                        cumulative_state[STATE_INTENT_LAST_UPDATED] = latest_date
                    # Checkpoint mid-table so progress is flushed and the next
                    # sync can resume from the latest intent cursor.
                    op.checkpoint(state=cumulative_state)

    except RuntimeError as e:
        if "[HTTP 403]" in str(e):
            log.warning(
                "Intent sync skipped — 403 Access Denied. "
                "Ensure your DevPortal app has the 'api:data:intent' scope and "
                "your ZoomInfo account includes the Intent product."
            )
            return state.get("intent_last_updated")
        raise

    log.info(f"Intent sync complete — {count} records upserted")
    return latest_date


def sync_news(configuration: dict, state: dict, cumulative_state: dict = None):
    """
    Syncs News articles from ZoomInfo Search News.
    Free endpoint — no credits consumed (records and requests counted).
    Opt-in via 'sync_news' configuration flag.

    Response fields: company.{id,name}, title, url, publishedDate, category, summary

    Gracefully skips on 403 — customer may not have News product licensed
    or the app scope may not include api:data:news.
    """
    if not _bool_config(configuration, "sync_news"):
        log.info("News sync skipped — sync_news not enabled")
        return state.get("news_last_updated")

    log.info("Starting news sync...")

    latest_date = state.get("news_last_updated")
    count = 0

    news_filter = apply_incremental_filter(
        {}, configuration, state, "news_last_updated", "pageDateMin"
    )

    try:
        for page_results in paginate(configuration, ENDPOINT_NEWS, news_filter):
            for record in page_results:
                record_id = record.get("id")
                a = record.get("attributes", {})

                if count == 0:
                    log.debug(f"Sample news attribute keys: {list(a.keys())}")

                # company is a LIST in News (an article can mention multiple companies)
                # Take the first company for the primary foreign key columns
                company_list = a.get("company") or []
                if isinstance(company_list, dict):
                    company_list = [company_list]
                primary_company = company_list[0] if company_list else {}
                company_id = primary_company.get("id")
                company_nm = primary_company.get("name")

                # Upsert one news row into the destination. 'upsert' inserts or
                # updates the record keyed by its primary key ('id').
                op.upsert(
                    table="news",
                    data={
                        "id": record_id,
                        "company_id": company_id,
                        "company_name": company_nm,
                        # all_companies stores the full list as JSON (articles can mention multiple)
                        "all_companies": json.dumps(company_list) if company_list else None,
                        "title": a.get("title"),
                        "url": a.get("url"),
                        "domain": a.get("domain"),
                        "image_url": a.get("imageUrl"),
                        # categories is a list (e.g. ["FINANCIAL_RESULTS"]) — store as JSON
                        "categories": (
                            json.dumps(a.get("categories")) if a.get("categories") else None
                        ),
                        # description is the article text; field is "description" not "summary"
                        "description": a.get("description"),
                        # date field is "pageDate" not "publishedDate"
                        "page_date": _safe_utc_datetime(a.get("pageDate")),
                        "raw_attributes": json.dumps(a) if a else None,
                    },
                )
                count += 1

                record_date = a.get("pageDate")
                latest_date = _max_cursor(latest_date, record_date)

                if cumulative_state is not None and count % CHECKPOINT_EVERY_N_ROWS == 0:
                    if latest_date:
                        cumulative_state[STATE_NEWS_LAST_UPDATED] = latest_date
                    # Checkpoint mid-table so progress is flushed and the next
                    # sync can resume from the latest news cursor.
                    op.checkpoint(state=cumulative_state)

    except RuntimeError as e:
        if "[HTTP 403]" in str(e):
            log.warning(
                "News sync skipped — 403 Access Denied. "
                "Ensure your DevPortal app has the 'api:data:news' scope and "
                "your ZoomInfo account includes the News product."
            )
            return state.get("news_last_updated")
        raise

    log.info(f"News sync complete — {count} records upserted")
    return latest_date


# ─────────────────────────────────────────────
# SYNC FUNCTIONS — ENRICH (cost credits)
# ─────────────────────────────────────────────
def _run_contact_enrichment(configuration: dict, person_ids: list, enrich_config: dict):
    """
    Enriches contacts in batches of 25 and upserts results to
    the 'contacts_enriched' table.

    Skips NO_MATCH records (no credit charged for those).
    Applies managementLevel filter post-enrich if configured.
    On 403 (license missing), logs a warning and returns — the table is
    declared in schema() but no rows are produced for this sync.
    """
    output_fields = enrich_config["output_fields"]
    mgmt_levels = enrich_config["mgmt_levels"]

    total = len(person_ids)
    enriched = 0
    skipped_match = 0
    skipped_mgmt = 0

    try:
        for i in range(0, total, ENRICH_BATCH_SIZE):
            batch = person_ids[i : i + ENRICH_BATCH_SIZE]
            batch_num = (i // ENRICH_BATCH_SIZE) + 1
            log.debug(f"Enriching contact batch {batch_num} ({len(batch)} contacts)...")

            results = enrich_contacts_batch(configuration, batch, output_fields)

            for record in results:
                record_id = record.get("id")
                a = record.get("attributes", {})
                match_status = record.get("meta", {}).get("matchStatus", "")

                if match_status == "NO_MATCH":
                    skipped_match += 1
                    log.debug(f"NO_MATCH for personId={record_id} — skipping")
                    continue

                if mgmt_levels and not matches_mgmt_level(a, mgmt_levels):
                    skipped_mgmt += 1
                    continue

                # companyName is nested under company.name, not a top-level field
                company = a.get("company") or {}
                # Upsert one enriched-contact row into the destination. 'upsert'
                # inserts or updates the record keyed by its primary key ('id').
                op.upsert(
                    table="contacts_enriched",
                    data={
                        "id": record_id,
                        "match_status": match_status,
                        "first_name": a.get("firstName"),
                        "last_name": a.get("lastName"),
                        "email": a.get("email"),
                        "phone": a.get("phone"),
                        "mobile_phone": a.get("mobilePhone"),
                        "job_title": a.get("jobTitle"),
                        "job_function": a.get("jobFunction"),
                        # managementLevel is a list (e.g. ["C-Level"]) — store as JSON string
                        "management_level": (
                            json.dumps(a.get("managementLevel"))
                            if a.get("managementLevel")
                            else None
                        ),
                        "company_id": company.get("id") or a.get("companyId"),
                        "company_name": company.get("name") or a.get("companyName"),
                        "company_website": a.get("companyWebsite"),
                        "company_revenue": _safe_int(a.get("companyRevenue")),
                        "company_employee_count": _safe_int(a.get("companyEmployeeCount")),
                        "company_industry": a.get("companyPrimaryIndustry"),
                        "city": a.get("city"),
                        "region": a.get("region"),
                        "state": a.get("state"),
                        "country": a.get("country"),
                        "direct_phone_do_not_call": a.get("directPhoneDoNotCall"),
                        "mobile_phone_do_not_call": a.get("mobilePhoneDoNotCall"),
                        "contact_accuracy_score": _safe_float(a.get("contactAccuracyScore")),
                        "last_updated_date": _safe_utc_datetime(a.get("lastUpdatedDate")),
                        "valid_date": _safe_utc_datetime(a.get("validDate")),
                        "raw_attributes": json.dumps(a) if a else None,
                    },
                )
                enriched += 1

    except RuntimeError as e:
        if "[HTTP 403]" in str(e):
            log.warning(
                "Contact enrichment skipped — 403 Access Denied. "
                "Ensure your ZoomInfo account includes Contact Enrich."
            )
            return
        raise

    log.info(
        f"Contact enrichment complete — {enriched} upserted, "
        f"{skipped_match} no-match, {skipped_mgmt} filtered by management level"
    )


def sync_companies_enriched(configuration: dict, company_ids: list):
    """
    Enriches companies in batches of 25 and upserts to 'companies_enriched'.
    Opt-in via 'enrich_companies' configuration flag.
    Costs 1 credit per new company record.

    Uses company IDs collected during sync_companies() Search pass.
    Gracefully skips on 403 — customer may not have Company Enrich licensed.
    """
    if not _bool_config(configuration, "enrich_companies"):
        log.info("Company enrichment skipped — enrich_companies not enabled")
        return

    if not company_ids:
        log.info("Company enrichment skipped — no company IDs available")
        return

    output_fields = _fields_config(
        configuration, "enrich_companies_output_fields", DEFAULT_COMPANY_ENRICH_FIELDS
    )

    log.info(f"Starting company enrichment — {len(company_ids)} companies queued...")

    total = len(company_ids)
    enriched = 0
    skipped_match = 0

    try:
        for i in range(0, total, ENRICH_BATCH_SIZE):
            batch = company_ids[i : i + ENRICH_BATCH_SIZE]
            batch_num = (i // ENRICH_BATCH_SIZE) + 1
            log.debug(f"Enriching company batch {batch_num} ({len(batch)} companies)...")

            results = enrich_companies_batch(configuration, batch, output_fields)

            for record in results:
                record_id = record.get("id")
                a = record.get("attributes", {})
                match_status = record.get("meta", {}).get("matchStatus", "")

                if match_status == "NO_MATCH":
                    skipped_match += 1
                    log.debug(f"NO_MATCH for companyId={record_id} — skipping")
                    continue

                # Upsert one enriched-company row into the destination. 'upsert'
                # inserts or updates the record keyed by its primary key ('id').
                op.upsert(
                    table="companies_enriched",
                    data={
                        "id": record_id,
                        "match_status": match_status,
                        "name": a.get("name"),
                        "website": a.get("website"),
                        "phone": a.get("phone"),
                        "revenue": _safe_int(a.get("revenue")),
                        "revenue_range": a.get("revenueRange"),
                        "employee_count": _safe_int(a.get("employeeCount")),
                        "employee_range": a.get("employeeRange"),
                        # employeeGrowth is a nested object — store as JSON
                        "employee_growth": (
                            json.dumps(a.get("employeeGrowth"))
                            if a.get("employeeGrowth")
                            else None
                        ),
                        "primary_industry": a.get("primaryIndustry"),
                        # industries is a list — store as JSON
                        "industries": (
                            json.dumps(a.get("industries")) if a.get("industries") else None
                        ),
                        "city": a.get("city"),
                        "state": a.get("state"),
                        "country": a.get("country"),
                        "street": a.get("street"),
                        "zip_code": a.get("zipCode"),
                        "metro_area": a.get("metroArea"),
                        "description": a.get("description"),
                        "ticker": a.get("ticker"),
                        "type": a.get("type"),
                        # naicsCodes / sicCodes are lists — store as JSON
                        "naics_codes": (
                            json.dumps(a.get("naicsCodes")) if a.get("naicsCodes") else None
                        ),
                        "sic_codes": json.dumps(a.get("sicCodes")) if a.get("sicCodes") else None,
                        "founded_year": _safe_int(a.get("foundedYear")),
                        "company_status": a.get("companyStatus"),
                        "parent_id": a.get("parentId"),
                        "parent_name": a.get("parentName"),
                        "ultimate_parent_id": a.get("ultimateParentId"),
                        "ultimate_parent_name": a.get("ultimateParentName"),
                        "total_funding_amount": _safe_int(a.get("totalFundingAmount")),
                        "recent_funding_amount": _safe_int(a.get("recentFundingAmount")),
                        "recent_funding_date": _safe_utc_datetime(a.get("recentFundingDate")),
                        # socialMediaUrls is a list of objects — store as JSON
                        "social_media_urls": (
                            json.dumps(a.get("socialMediaUrls"))
                            if a.get("socialMediaUrls")
                            else None
                        ),
                        "logo": a.get("logo"),
                        "last_updated_date": _safe_utc_datetime(a.get("lastUpdatedDate")),
                        "is_defunct": a.get("isDefunct"),
                        "raw_attributes": json.dumps(a) if a else None,
                    },
                )
                enriched += 1

    except RuntimeError as e:
        if "[HTTP 403]" in str(e):
            log.warning(
                "Company enrichment skipped — 403 Access Denied. "
                "Ensure your ZoomInfo account includes Company Enrich."
            )
            return
        raise

    log.info(f"Company enrichment complete — {enriched} upserted, {skipped_match} no-match")


def _stream_per_company_enrich(
    configuration: dict,
    company_ids: list,
    table_name: str,
    row_builder,
    license_label: str,
    checkpoint_state: dict,
):
    """
    Streaming per-company enrichment with bounded memory.

    Workers fetch a single page from the per-company enrich endpoint at a time,
    build row dicts via ``row_builder(company_id, record)``, and push them onto
    a bounded queue. The main thread drains the queue and calls ``op.upsert``
    on each row, so memory holds at most ENRICH_QUEUE_MAX rows in flight
    instead of accumulating an entire company's record list per worker.

    Calls ``op.checkpoint(state=checkpoint_state)`` every CHECKPOINT_EVERY_N_ROWS
    successful upserts so Fivetran flushes buffered rows to the destination
    incrementally rather than buffering an entire million-row table in memory.
    Tech/scoops enrich tables don't have their own incremental cursor, so
    ``checkpoint_state`` is just the cumulative state of preceding tables —
    the resume semantics belong to those tables, not this one.

    SDK constraint: ``op.upsert`` and ``op.checkpoint`` only run on the main
    thread. Workers never call them directly; they only push to the queue.

    Sentinels on the queue (instead of row dicts):
        ("done", company_id)           — worker finished one company cleanly
        ("403", company_id)            — license missing; main thread sets the
                                         abort event and reports the warning
        ("error", company_id, exc)     — non-403 fetch failure for one company;
                                         main thread logs and continues
    """
    abort = threading.Event()
    work_queue: queue.Queue = queue.Queue(maxsize=ENRICH_QUEUE_MAX)

    def worker(cid: str):
        if abort.is_set():
            work_queue.put(("done", cid))
            return
        try:
            for page_results in row_builder.paginate(configuration, cid):
                if abort.is_set():
                    break
                for record in page_results:
                    row = row_builder.build_row(cid, record)
                    if row is not None:
                        work_queue.put(row)
        except Exception as e:
            if "[HTTP 403]" in str(e):
                work_queue.put(("403", cid))
                return
            work_queue.put(("error", cid, e))
            return
        work_queue.put(("done", cid))

    total_rows = 0
    companies_processed = 0
    license_missing = False
    expected_signals = len(company_ids)

    with ThreadPoolExecutor(max_workers=ENRICH_WORKERS) as pool:
        for cid in company_ids:
            pool.submit(worker, cid)

        while companies_processed < expected_signals:
            item = work_queue.get()
            if isinstance(item, dict):
                # A missing license means this table must be empty for the sync
                # (per README). Workers already running in parallel may have
                # queued rows before abort.set() landed — discard them rather
                # than partially populating the table, but keep draining the
                # queue so workers finish cleanly.
                if license_missing:
                    continue
                # Upsert one enriched row into the destination. op.upsert runs
                # only on the main thread (SDK constraint); workers enqueue rows.
                op.upsert(table=table_name, data=item)
                total_rows += 1
                if total_rows % CHECKPOINT_EVERY_N_ROWS == 0:
                    # Checkpoint periodically so Fivetran flushes buffered rows
                    # to the destination incrementally instead of holding the
                    # whole table in memory.
                    op.checkpoint(state=checkpoint_state)
                continue
            tag = item[0]
            cid = item[1]
            if tag == "done":
                companies_processed += 1
            elif tag == "403":
                if not license_missing:
                    log.warning(
                        f"{license_label} enrichment skipped — 403 Access Denied. "
                        f"Ensure your ZoomInfo account includes {license_label} Enrich."
                    )
                    license_missing = True
                    abort.set()
                companies_processed += 1
            elif tag == "error":
                exc = item[2]
                log.warning(f"{license_label} enrich failed for companyId={cid}: {exc}")
                companies_processed += 1

            if companies_processed % 100 == 0 and companies_processed > 0:
                log.info(
                    f"{license_label} enrichment progress: "
                    f"{companies_processed}/{expected_signals} companies"
                )

    if license_missing:
        return 0
    return total_rows


class _ScoopsEnrichBuilder:
    """Row-builder bundle for the per-company Scoops Enrich endpoint."""

    paginate = staticmethod(paginate_enrich_scoops)

    @staticmethod
    def build_row(company_id: str, record: dict) -> dict:
        record_id = record.get("id")
        a = record.get("attributes", {})
        company = a.get("company") or {}
        topics = a.get("topics")
        types = a.get("types")
        return {
            "id": record_id,
            "company_id": company_id,
            "company_name": company.get("name"),
            "description": a.get("description"),
            "topics": json.dumps(topics) if topics else None,
            "types": json.dumps(types) if types else None,
            "published_date": _safe_utc_datetime(a.get("publishedDate")),
            "original_published_date": _safe_utc_datetime(a.get("originalPublishedDate")),
            "link": a.get("link"),
            "link_text": a.get("linkText"),
            "raw_attributes": json.dumps(a) if a else None,
        }


def sync_scoops_enriched(configuration: dict, company_ids: list, cumulative_state: dict):
    """
    Enriches scoops per company and upserts to 'scoops_enriched'.
    Opt-in via 'enrich_scoops' configuration flag.
    Costs 1 credit per company enriched (each scoop counts as a record).

    Streams rows through a bounded queue (see _stream_per_company_enrich) so
    memory stays bounded regardless of company count or per-company scoop
    volume. ``op.upsert`` calls stay on the main thread for SDK safety.
    """
    if not _bool_config(configuration, "enrich_scoops"):
        log.info("Scoops enrichment skipped — enrich_scoops not enabled")
        return

    if not company_ids:
        log.info("Scoops enrichment skipped — no company IDs available")
        return

    log.info(
        f"Starting scoops enrichment — {len(company_ids)} companies, "
        f"{ENRICH_WORKERS} parallel workers (streaming, queue_max={ENRICH_QUEUE_MAX})..."
    )

    total = _stream_per_company_enrich(
        configuration,
        company_ids,
        table_name="scoops_enriched",
        row_builder=_ScoopsEnrichBuilder,
        license_label="Scoops",
        checkpoint_state=cumulative_state,
    )
    log.info(f"Scoops enrichment complete — {total} scoops across {len(company_ids)} companies")


class _TechnologiesEnrichBuilder:
    """Row-builder bundle for the per-company Technologies Enrich endpoint."""

    paginate = staticmethod(paginate_enrich_technologies)

    @staticmethod
    def build_row(company_id: str, record: dict) -> dict:
        tech_id = record.get("id")
        a = record.get("attributes", {})
        return {
            "id": f"{company_id}_{tech_id}",
            "company_id": company_id,
            "technology_id": tech_id,
            "attribute": a.get("attribute"),
            "product": a.get("product"),
            "vendor": a.get("vendor"),
            "category": a.get("category"),
            "category_parent": a.get("categoryParent"),
            "description": a.get("description"),
            "domain": a.get("domain"),
            "logo": a.get("logo"),
            "website": a.get("website"),
            "created_date": _safe_utc_datetime(a.get("createdDate")),
            "modified_date": _safe_utc_datetime(a.get("modifiedDate")),
            "raw_attributes": json.dumps(a) if a else None,
        }


def sync_technologies(configuration: dict, company_ids: list, cumulative_state: dict):
    """
    Enriches company technology stacks and upserts to 'technologies'.
    Opt-in via 'enrich_technologies' configuration flag.
    Costs 1 credit per company enriched.

    Each technology record is a separate row keyed by {company_id}_{tech_id}.
    A single company can have thousands of technology records, which is why
    this uses the streaming queue helper — accumulating per-company row lists
    OOM'd the connector in Fivetran cloud (1 GB limit, ~99K rows from 100
    companies). Streams rows through a bounded queue so memory stays flat.

    Confirmed response fields (from live API probe):
      id (technology id), attribute, product, vendor, category, categoryParent,
      description, domain, logo, website, createdDate, modifiedDate

    Input format: flat {"companyId": id} — NOT matchCompanyInput.
    No outputFields param — API returns all fields automatically.
    Pagination uses meta.totalResults only (no meta.page.total).
    """
    if not _bool_config(configuration, "enrich_technologies"):
        log.info("Technologies sync skipped — enrich_technologies not enabled")
        return

    if not company_ids:
        log.info("Technologies sync skipped — no company IDs available")
        return

    log.info(
        f"Starting technologies enrichment — {len(company_ids)} companies, "
        f"{ENRICH_WORKERS} parallel workers (streaming, queue_max={ENRICH_QUEUE_MAX})..."
    )

    total = _stream_per_company_enrich(
        configuration,
        company_ids,
        table_name="technologies",
        row_builder=_TechnologiesEnrichBuilder,
        license_label="Technologies",
        checkpoint_state=cumulative_state,
    )
    log.info(f"Technologies enrichment complete — {total} technology records upserted")


def sync_corporate_hierarchy(configuration: dict, company_ids: list):
    """
    Enriches corporate hierarchy for companies and upserts to 'corporate_hierarchy'.
    Opt-in via 'enrich_corporate_hierarchy' configuration flag.
    Costs 1 credit per company enriched.

    Returns the full family tree structure: parent company, subsidiaries,
    acquisitions, former names, and locations.
    """
    if not _bool_config(configuration, "enrich_corporate_hierarchy"):
        log.info("Corporate hierarchy sync skipped — enrich_corporate_hierarchy not enabled")
        return

    if not company_ids:
        log.info("Corporate hierarchy sync skipped — no company IDs available")
        return

    output_fields = _fields_config(
        configuration, "enrich_corp_hier_output_fields", DEFAULT_CORP_HIER_FIELDS
    )

    log.info(f"Starting corporate hierarchy enrichment — {len(company_ids)} companies queued...")

    total = 0
    skipped_match = 0

    try:
        for i in range(0, len(company_ids), ENRICH_BATCH_SIZE):
            batch = company_ids[i : i + ENRICH_BATCH_SIZE]
            batch_num = (i // ENRICH_BATCH_SIZE) + 1
            log.debug(
                f"Enriching corporate hierarchy batch {batch_num} ({len(batch)} companies)..."
            )

            results = enrich_corp_hierarchy_batch(configuration, batch, output_fields)

            for record in results:
                record_id = record.get("id")
                a = record.get("attributes", {})
                match_status = record.get("meta", {}).get("matchStatus", "")

                if match_status == "NO_MATCH":
                    skipped_match += 1
                    continue

                # PK is company_id (not record_id): the response 'id' is the
                # input matchId we sent, which is not stable across batches/syncs.
                # company_id (from attributes) is the stable ZoomInfo company ID.
                company_id = a.get("companyId") or record_id

                # Upsert one corporate-hierarchy row into the destination.
                # 'upsert' inserts or updates the record keyed by 'company_id'.
                op.upsert(
                    table="corporate_hierarchy",
                    data={
                        "company_id": company_id,
                        "match_status": match_status,
                        # parentage and familyTree are nested structures — store as JSON blobs
                        "family_tree": (
                            json.dumps(a.get("familyTree")) if a.get("familyTree") else None
                        ),
                        "parentage": (
                            json.dumps(a.get("parentage")) if a.get("parentage") else None
                        ),
                        "raw_attributes": json.dumps(a) if a else None,
                    },
                )
                total += 1

    except RuntimeError as e:
        if "[HTTP 403]" in str(e):
            log.warning(
                "Corporate hierarchy enrichment skipped — 403 Access Denied. "
                "Ensure your ZoomInfo account includes Corporate Hierarchy Enrich."
            )
            return
        raise

    log.info(
        f"Corporate hierarchy enrichment complete — {total} upserted, {skipped_match} no-match"
    )


def sync_usage(configuration: dict):
    """
    Fetches current API usage and limits from ZoomInfo and upserts to 'usage'.
    Always runs — no opt-in required. Free endpoint.

    Useful for customers to monitor credit consumption and rate limits
    directly in their data warehouse.

    Response shape (confirmed against live tenant):
      data: [{attributes: {usage: [
        {limitType: "requestLimit",      currentUsage, totalLimit, usageRemaining, description},
        {limitType: "recordLimit",       ...},
        {limitType: "uniqueRedeemLimit", ...},
        ...
      ]}}]

    Each limitType becomes its own row keyed by `limit_type`.
    """
    log.info("Fetching API usage data...")

    token = get_access_token(configuration)
    response = get_with_retry(f"{SEARCH_BASE}{ENDPOINT_USAGE}", headers=get_headers_get(token))

    if response.status_code == 401:
        _invalidate_token_cache()
        token = get_access_token(configuration)
        response = get_with_retry(f"{SEARCH_BASE}{ENDPOINT_USAGE}", headers=get_headers_get(token))

    # /users/usage acts as our auth preflight. A 401/403 here is unambiguous —
    # the credentials are bad or have been revoked, not a missing-product-license
    # situation (every authenticated tenant has access to /usage). Raise so the
    # Fivetran dashboard surfaces "auth failed" instead of silently completing
    # with empty enrich tables (every downstream enrich would 403 too, and our
    # 403-soft-skip logic would mask the real problem).
    if response.status_code in (401, 403):
        raise RuntimeError(
            f"ZoomInfo auth preflight failed on {ENDPOINT_USAGE} "
            f"[HTTP {response.status_code}]. Check that your client_id and "
            f"client_secret are valid and not revoked. Details: {response.text[:200]}"
        )

    if response.status_code != 200:
        log.warning(
            f"Usage data fetch failed [HTTP {response.status_code}]: {response.text[:200]}"
        )
        return

    payload = response.json()
    log.debug(f"Usage response: {json.dumps(payload)[:500]}")

    data = payload.get("data", [])
    if isinstance(data, dict):
        data = [data]

    upserted = 0
    for item in data:
        attrs = (item or {}).get("attributes", {}) or {}
        usage_rows = attrs.get("usage", [])
        for row in usage_rows:
            limit_type = row.get("limitType")
            if not limit_type:
                continue
            # Upsert one usage row into the destination. 'upsert' inserts or
            # updates the record keyed by its primary key ('id' = limitType).
            op.upsert(
                table="usage",
                data={
                    "id": limit_type,
                    "limit_type": limit_type,
                    "description": row.get("description"),
                    "current_usage": _safe_int(row.get("currentUsage")),
                    "total_limit": _safe_int(row.get("totalLimit")),
                    "usage_remaining": _safe_int(row.get("usageRemaining")),
                    "raw_attributes": json.dumps(row) if row else None,
                },
            )
            upserted += 1

    log.info(f"Usage data synced — {upserted} limit type(s) recorded")


# ─────────────────────────────────────────────
# SCHEMA
# ─────────────────────────────────────────────
def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    # Validate configuration here too: schema() runs before update() during a
    # sync, so failing fast on bad credentials surfaces the error immediately.
    validate_configuration(configuration)

    # Core tables — always present
    tables = [
        {
            "table": "contacts",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "first_name": "STRING",
                "last_name": "STRING",
                "middle_name": "STRING",
                "job_title": "STRING",
                "company_id": "STRING",
                "company_name": "STRING",
                "contact_accuracy_score": "FLOAT",
                "has_email": "BOOLEAN",
                "has_direct_phone": "BOOLEAN",
                "has_mobile_phone": "BOOLEAN",
                "has_supplemental_email": "BOOLEAN",
                "direct_phone_do_not_call": "BOOLEAN",
                "mobile_phone_do_not_call": "BOOLEAN",
                "last_updated_date": "UTC_DATETIME",
                "valid_date": "UTC_DATETIME",
                "raw_attributes": "JSON",
            },
        },
        {
            "table": "companies",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "website": "STRING",
                "city": "STRING",
                "state": "STRING",
                "country": "STRING",
                "employee_count": "INT",
                "revenue": "LONG",
                "raw_attributes": "JSON",
            },
        },
        {
            "table": "scoops",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "company_id": "STRING",
                "company_name": "STRING",
                "description": "STRING",
                "topics": "JSON",
                "types": "JSON",
                "published_date": "UTC_DATETIME",
                "original_published_date": "UTC_DATETIME",
                "raw_attributes": "JSON",
            },
        },
        {
            "table": "usage",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "limit_type": "STRING",
                "description": "STRING",
                "current_usage": "LONG",
                "total_limit": "LONG",
                "usage_remaining": "LONG",
                "raw_attributes": "JSON",
            },
        },
    ]

    # Intent — only if topics are configured
    if configuration.get("intent_topics", "").strip():
        tables.append(
            {
                "table": "intent",
                "primary_key": ["id"],
                "columns": {
                    "id": "STRING",
                    "company_id": "STRING",
                    "company_name": "STRING",
                    "topic": "STRING",
                    "category": "STRING",
                    "signal_score": "FLOAT",
                    "audience_strength": "FLOAT",
                    "signal_date": "UTC_DATETIME",
                    "raw_attributes": "JSON",
                },
            }
        )

    # News — opt-in
    if _bool_config(configuration, "sync_news"):
        tables.append(
            {
                "table": "news",
                "primary_key": ["id"],
                "columns": {
                    "id": "STRING",
                    "company_id": "STRING",
                    "company_name": "STRING",
                    "all_companies": "JSON",
                    "title": "STRING",
                    "url": "STRING",
                    "domain": "STRING",
                    "image_url": "STRING",
                    "categories": "JSON",
                    "description": "STRING",
                    "page_date": "UTC_DATETIME",
                    "raw_attributes": "JSON",
                },
            }
        )

    # Contact Enrichment — opt-in
    if _bool_config(configuration, "enrich_contacts"):
        tables.append(
            {
                "table": "contacts_enriched",
                "primary_key": ["id"],
                "columns": {
                    "id": "STRING",
                    "match_status": "STRING",
                    "first_name": "STRING",
                    "last_name": "STRING",
                    "email": "STRING",
                    "phone": "STRING",
                    "mobile_phone": "STRING",
                    "job_title": "STRING",
                    "job_function": "STRING",
                    "management_level": "JSON",
                    "company_id": "STRING",
                    "company_name": "STRING",
                    "company_website": "STRING",
                    "company_revenue": "LONG",
                    "company_employee_count": "INT",
                    "company_industry": "STRING",
                    "city": "STRING",
                    "region": "STRING",
                    "state": "STRING",
                    "country": "STRING",
                    "direct_phone_do_not_call": "BOOLEAN",
                    "mobile_phone_do_not_call": "BOOLEAN",
                    "contact_accuracy_score": "FLOAT",
                    "last_updated_date": "UTC_DATETIME",
                    "valid_date": "UTC_DATETIME",
                    "raw_attributes": "JSON",
                },
            }
        )

    # Company Enrichment — opt-in
    if _bool_config(configuration, "enrich_companies"):
        tables.append(
            {
                "table": "companies_enriched",
                "primary_key": ["id"],
                "columns": {
                    "id": "STRING",
                    "match_status": "STRING",
                    "name": "STRING",
                    "website": "STRING",
                    "phone": "STRING",
                    "revenue": "LONG",
                    "revenue_range": "STRING",
                    "employee_count": "INT",
                    "employee_range": "STRING",
                    "employee_growth": "JSON",
                    "primary_industry": "STRING",
                    "industries": "JSON",
                    "city": "STRING",
                    "state": "STRING",
                    "country": "STRING",
                    "street": "STRING",
                    "zip_code": "STRING",
                    "metro_area": "STRING",
                    "description": "STRING",
                    "ticker": "STRING",
                    "type": "STRING",
                    "naics_codes": "JSON",
                    "sic_codes": "JSON",
                    "founded_year": "INT",
                    "company_status": "STRING",
                    "parent_id": "STRING",
                    "parent_name": "STRING",
                    "ultimate_parent_id": "STRING",
                    "ultimate_parent_name": "STRING",
                    "total_funding_amount": "LONG",
                    "recent_funding_amount": "LONG",
                    "recent_funding_date": "UTC_DATETIME",
                    "social_media_urls": "JSON",
                    "logo": "STRING",
                    "last_updated_date": "UTC_DATETIME",
                    "is_defunct": "BOOLEAN",
                    "raw_attributes": "JSON",
                },
            }
        )

    # Scoops Enrichment — opt-in
    if _bool_config(configuration, "enrich_scoops"):
        tables.append(
            {
                "table": "scoops_enriched",
                "primary_key": ["id"],
                "columns": {
                    "id": "STRING",
                    "company_id": "STRING",
                    "company_name": "STRING",
                    "description": "STRING",
                    "topics": "JSON",
                    "types": "JSON",
                    "published_date": "UTC_DATETIME",
                    "original_published_date": "UTC_DATETIME",
                    "link": "STRING",
                    "link_text": "STRING",
                    "raw_attributes": "JSON",
                },
            }
        )

    # Technologies — opt-in
    if _bool_config(configuration, "enrich_technologies"):
        tables.append(
            {
                "table": "technologies",
                "primary_key": ["id"],
                "columns": {
                    "id": "STRING",
                    "company_id": "STRING",
                    "technology_id": "STRING",
                    "attribute": "STRING",
                    "product": "STRING",
                    "vendor": "STRING",
                    "category": "STRING",
                    "category_parent": "STRING",
                    "description": "STRING",
                    "domain": "STRING",
                    "logo": "STRING",
                    "website": "STRING",
                    "created_date": "UTC_DATETIME",
                    "modified_date": "UTC_DATETIME",
                    "raw_attributes": "JSON",
                },
            }
        )

    # Corporate Hierarchy — opt-in
    if _bool_config(configuration, "enrich_corporate_hierarchy"):
        tables.append(
            {
                "table": "corporate_hierarchy",
                "primary_key": ["company_id"],
                "columns": {
                    "company_id": "STRING",
                    "match_status": "STRING",
                    "family_tree": "JSON",
                    "parentage": "JSON",
                    "raw_attributes": "JSON",
                },
            }
        )

    return tables


# ─────────────────────────────────────────────
# UPDATE (main sync entrypoint)
# ─────────────────────────────────────────────
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
    log.warning("Example: Source Examples : ZoomInfo")

    # Validate the configuration before doing any work so a misconfiguration
    # fails fast with a clear error instead of part-way through a sync.
    validate_configuration(configuration)

    get_access_token(configuration)

    # Cumulative state we'll checkpoint after each completed table. Starting
    # from the prior state means a resume picks up where the last run left off.
    cumulative_state = dict(state)

    # ── Usage (always — free, no opt-in) ──
    sync_usage(configuration)
    # Save the progress by checkpointing the state. This tells Fivetran it is
    # safe to write the data synced so far to the destination, and lets the next
    # sync resume from here. We checkpoint after each completed table below.
    op.checkpoint(state=cumulative_state)

    # ── Parse enrichment + filter config once ──
    enrich_config = get_enrich_config(configuration)
    search_filter = build_search_filter(configuration)
    log.info(f"Search filter for this sync: {search_filter}")

    # ── Contacts (Search + optional Enrich) ──
    contacts_latest = sync_contacts(
        configuration, state, enrich_config, search_filter, cumulative_state
    )
    if contacts_latest:
        cumulative_state[STATE_CONTACTS_LAST_UPDATED] = contacts_latest
    op.checkpoint(state=cumulative_state)

    # ── Companies (Search + optional downstream enrichments) ──
    companies_latest, company_ids = sync_companies(configuration, state, search_filter)
    if companies_latest:
        cumulative_state[STATE_COMPANIES_LAST_UPDATED] = companies_latest
    op.checkpoint(state=cumulative_state)

    # ── Scoops (Search) ──
    scoops_latest = sync_scoops(configuration, state, search_filter, cumulative_state)
    if scoops_latest:
        cumulative_state[STATE_SCOOPS_LAST_UPDATED] = scoops_latest
    op.checkpoint(state=cumulative_state)

    # ── Intent (Search — requires topics config) ──
    intent_latest = sync_intent(configuration, state, cumulative_state)
    if intent_latest:
        cumulative_state[STATE_INTENT_LAST_UPDATED] = intent_latest
    op.checkpoint(state=cumulative_state)

    # ── News (Search — opt-in) ──
    news_latest = sync_news(configuration, state, cumulative_state)
    if news_latest:
        cumulative_state[STATE_NEWS_LAST_UPDATED] = news_latest
    op.checkpoint(state=cumulative_state)

    # ── Company-based Enrichments (all use company_ids from Search) ──
    sync_companies_enriched(configuration, company_ids)
    op.checkpoint(state=cumulative_state)

    sync_scoops_enriched(configuration, company_ids, cumulative_state)
    op.checkpoint(state=cumulative_state)

    sync_technologies(configuration, company_ids, cumulative_state)
    op.checkpoint(state=cumulative_state)

    sync_corporate_hierarchy(configuration, company_ids)
    op.checkpoint(state=cumulative_state)

    log.info("ZoomInfo Fivetran Connector — sync complete")


# ─────────────────────────────────────────────
# CONNECTOR INIT
# ─────────────────────────────────────────────
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
