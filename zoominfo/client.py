"""HTTP client for the ZoomInfo connector: auth, retry transport, pagination.

Holds the OAuth 2.0 Client Credentials token cache and the low-level request
helpers (post/get with exponential-backoff retry) plus the Search and
per-company Enrich pagination generators. Everything that talks to the
ZoomInfo API over the wire lives here; higher-level sync orchestration in
sync.py calls into these helpers.
"""

# For making HTTP calls to the ZoomInfo Search and Enrich APIs.
import requests

# For Base64-encoding the client_id:client_secret pair in the OAuth token request.
import base64

# For exponential-backoff sleeps between retries.
import time

# For the lock guarding the shared token cache.
import threading

from fivetran_connector_sdk import Logging as log

from constants import (
    TOKEN_URL,
    SEARCH_BASE,
    JSONAPI_TYPE,
    PAGE_SIZE,
    MAX_PAGES,
    SEARCH_RESULT_CEILING,
    MAX_RETRIES,
    RETRY_BASE_WAIT,
    RETRY_MAX_WAIT,
    RETRY_STATUS_CODES,
    REQUEST_TIMEOUT,
    ENDPOINT_SCOOPS_ENRICH,
    ENDPOINT_TECHNOLOGIES_ENRICH,
)
from transforms import _safe_int

# In-memory token cache. Guarded by _token_cache_lock — enrich worker threads
# can race on token refresh otherwise, wasting credits on duplicate /oauth/v1/token
# calls and potentially tripping per-IP rate limits.
_token_cache = {"access_token": None, "expires_at": 0}
_token_cache_lock = threading.Lock()


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

        response = None
        wait = RETRY_BASE_WAIT
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.post(
                    TOKEN_URL,
                    headers={
                        "Authorization": f"Basic {encoded}",
                        "Content-Type": "application/x-www-form-urlencoded",
                    },
                    data={"grant_type": "client_credentials"},
                    timeout=REQUEST_TIMEOUT,
                )
            except requests.exceptions.RequestException as e:
                if attempt == MAX_RETRIES:
                    raise RuntimeError(
                        f"Network error requesting ZoomInfo access token after {MAX_RETRIES} attempts: {e}"
                    )
                wait_secs = min(wait, RETRY_MAX_WAIT)
                log.warning(
                    f"Token request network error ({type(e).__name__}) — backing off {wait_secs}s "
                    f"(attempt {attempt}/{MAX_RETRIES})"
                )
                time.sleep(wait_secs)
                wait *= 2
                continue

            if response.status_code in RETRY_STATUS_CODES:
                if attempt == MAX_RETRIES:
                    break
                wait_secs, wait = _sleep_for_retry(response, wait)
                log.warning(
                    f"Retryable status {response.status_code} on token request — waited {wait_secs}s "
                    f"(attempt {attempt}/{MAX_RETRIES})"
                )
                continue

            break

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
                f"Network error on {url} ({type(e).__name__}) — backing off "
                f"{wait_secs}s (attempt {attempt}/{MAX_RETRIES})"
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
                f"Network error on {url} ({type(e).__name__}) — backing off "
                f"{wait_secs}s (attempt {attempt}/{MAX_RETRIES})"
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
