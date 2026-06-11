"""Oracle WMS REST API client: single-page requests, multi-page fetching, and entity probing."""

import time
import requests
from typing import Optional, Tuple

from fivetran_connector_sdk import Logging as log

from utils import (
    API_VERSION,
    DEFAULT_PAGE_SIZE,
    MIN_PAGE_SIZE,
    CHECKPOINT_INTERVAL_PAGES,
    MAX_RETRIES,
    INITIAL_BACKOFF_SECONDS,
    OrderingNotSupportedError,
    normalize_timestamp_to_oracle_format,
)

# ── Entity capability probe ───────────────────────────────────────────────────


def check_entity_has_mod_ts(base_url: str, username: str, password: str, entity: str) -> bool:
    """Return True if the entity's describe endpoint lists a mod_ts field."""
    endpoint = f"{base_url}/wms/lgfapi/{API_VERSION}/entity/{entity}/describe"
    try:
        response = requests.get(
            endpoint, params={"format": "json"}, auth=(username, password), timeout=60
        )
        response.raise_for_status()
        return "mod_ts" in response.json().get("fields", {})
    except Exception as e:
        log.warning(f"Could not check mod_ts for {entity}: {e}. Assuming no mod_ts support.")
        return False


# ── Single-page request ───────────────────────────────────────────────────────


def make_api_request(
    base_url: str,
    username: str,
    password: str,
    entity: str,
    page: int = 1,
    mod_ts_filter: Optional[str] = None,
    mod_ts_lt_filter: Optional[str] = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    ordering: Optional[str] = None,
    session: Optional[requests.Session] = None,
    create_ts_gte_filter: Optional[str] = None,
    create_ts_lt_filter: Optional[str] = None,
    fields: Optional[str] = None,
) -> dict:
    """
    Make a single paged request to the Oracle WMS entity endpoint with retry logic.

    Args:
        mod_ts_filter:        mod_ts__gte — lower bound for incremental (ASC) queries
        mod_ts_lt_filter:     mod_ts__lt  — upper bound for backfill (DESC) queries
        ordering:             e.g. "mod_ts,id" (ASC) or "-mod_ts,id" (DESC)
        create_ts_gte_filter: create_ts__gte — used in Phase 1b to catch backdated records
        create_ts_lt_filter:  create_ts__lt  — upper bound for Phase 1b (sync_start_time)
        session:              Optional Session for connection reuse across pages

    Raises:
        OrderingNotSupportedError: if the entity returns 400 for the given ordering (never retried).
        requests.exceptions.Timeout: propagated immediately for adaptive page-size handling upstream.
        requests.exceptions.RequestException: after MAX_RETRIES exhausted.
    """
    params = {
        "format": "json",
        "page": str(page),
        "page_size": str(page_size),
    }
    if mod_ts_filter:
        params["mod_ts__gte"] = mod_ts_filter
    if mod_ts_lt_filter:
        params["mod_ts__lt"] = mod_ts_lt_filter
    if create_ts_gte_filter:
        params["create_ts__gte"] = create_ts_gte_filter
    if create_ts_lt_filter:
        params["create_ts__lt"] = create_ts_lt_filter
    if ordering:
        params["ordering"] = ordering
    if fields:
        params["fields"] = fields

    endpoint = f"{base_url}/wms/lgfapi/{API_VERSION}/entity/{entity}"
    requester = session or requests
    last_exception = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requester.get(
                endpoint, params=params, auth=(username, password), timeout=60
            )

            if response.status_code == 400 and ordering:
                raise OrderingNotSupportedError(
                    f"{entity} does not support ordering='{ordering}' (400 Bad Request)"
                )
            if response.status_code == 404:
                return {
                    "result_count": 0,
                    "page_count": 1,
                    "page_nbr": page,
                    "next_page": None,
                    "previous_page": None,
                    "results": [],
                }

            response.raise_for_status()
            return response.json()

        except (OrderingNotSupportedError, requests.exceptions.Timeout):
            raise  # Caller handles these — no retry
        except requests.exceptions.RequestException as e:
            last_exception = e
            if attempt < MAX_RETRIES:
                backoff = INITIAL_BACKOFF_SECONDS * (2 ** (attempt - 1))
                log.warning(
                    f"API request failed for {entity} page {page} (attempt {attempt}/{MAX_RETRIES}). Retrying in {backoff}s…"
                )
                time.sleep(backoff)
            else:
                log.error(
                    f"API request failed for {entity} page {page} after {MAX_RETRIES} attempts: {e}"
                )

    raise last_exception


# ── Result count probe ────────────────────────────────────────────────────────


def probe_entity_count(
    base_url: str,
    username: str,
    password: str,
    entity: str,
    mod_ts_filter: Optional[str] = None,
    mod_ts_lt_filter: Optional[str] = None,
    ordering: Optional[str] = None,
) -> int:
    """
    Fetch page 1 at page_size=1 to read result_count without loading records.
    Used to sort entities largest-first before submitting to the thread pool.
    Returns 0 on any error so the entity sorts to the back.
    """
    try:
        response = make_api_request(
            base_url,
            username,
            password,
            entity,
            page=1,
            page_size=1,
            ordering=ordering,
            mod_ts_filter=(
                normalize_timestamp_to_oracle_format(mod_ts_filter) if mod_ts_filter else None
            ),
            mod_ts_lt_filter=(
                normalize_timestamp_to_oracle_format(mod_ts_lt_filter)
                if mod_ts_lt_filter
                else None
            ),
        )
        return response.get("result_count", 0) or 0
    except Exception:
        return 0


# ── Multi-page fetch ──────────────────────────────────────────────────────────


def fetch_entity_data(
    base_url: str,
    username: str,
    password: str,
    entity: str,
    mod_ts_filter: Optional[str] = None,
    mod_ts_lt_filter: Optional[str] = None,
    ordering: Optional[str] = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: Optional[int] = None,
    checkpoint_callback=None,
    records_callback=None,
    session: Optional[requests.Session] = None,
    create_ts_gte_filter: Optional[str] = None,
    create_ts_lt_filter: Optional[str] = None,
    phase_label: Optional[str] = None,
) -> Tuple[int, Optional[str], bool]:
    """
    Paginate through all pages for an entity, calling records_callback on each page.

    Pagination ends on a truly empty page (len == 0). A partial page is NOT treated as
    the end: Oracle pagination bugs can produce partial intermediate pages, and live
    modifications can shift records in or out mid-stream. Only an empty page
    unambiguously means no more records exist.

    Returns:
        (total_records, extreme_mod_ts, finished_all_pages)
        extreme_mod_ts:     max mod_ts for ASC ordering, min for DESC — used as the next cursor.
        finished_all_pages: True if all available pages were consumed.
    """
    normalized_mod_ts_gte = (
        normalize_timestamp_to_oracle_format(mod_ts_filter) if mod_ts_filter else None
    )
    normalized_mod_ts_lt = (
        normalize_timestamp_to_oracle_format(mod_ts_lt_filter) if mod_ts_lt_filter else None
    )
    normalized_create_ts_gte = (
        normalize_timestamp_to_oracle_format(create_ts_gte_filter)
        if create_ts_gte_filter
        else None
    )
    normalized_create_ts_lt = (
        normalize_timestamp_to_oracle_format(create_ts_lt_filter) if create_ts_lt_filter else None
    )

    is_desc = bool(ordering and ordering.startswith("-"))
    total_records = 0
    pages_fetched = 0
    pages_since_checkpoint = 0
    extreme_mod_ts = None
    page = 1
    total_pages = None  # unknown until Oracle tells us; only ever increases once set
    is_exhausted = False  # True when an empty page confirms no more records
    fetch_start = time.time()
    total_api_ms = 0
    max_page_ms = 0
    # DESC only: extreme_mod_ts when max_pages was first hit. Keeps the loop running
    # until the timestamp changes (handles same-ts bulk imports safely).
    ts_when_max_reached = None

    while (total_pages is None or page <= total_pages) and (
        max_pages is None
        or pages_fetched < max_pages
        or (is_desc and ts_when_max_reached is not None and extreme_mod_ts == ts_when_max_reached)
    ):

        # Adaptive page size: on timeout, halve page_size and recalculate the page number
        # to preserve the same record offset. Reduction persists for all subsequent pages.
        while True:
            page_start = time.time()
            try:
                response_data = make_api_request(
                    base_url,
                    username,
                    password,
                    entity,
                    page,
                    normalized_mod_ts_gte,
                    normalized_mod_ts_lt,
                    page_size,
                    ordering,
                    session,
                    normalized_create_ts_gte,
                    normalized_create_ts_lt,
                )
                break
            except requests.exceptions.Timeout:
                if page_size <= MIN_PAGE_SIZE:
                    log.error(
                        f"{entity}: page {page} timed out at minimum page_size={page_size}, giving up"
                    )
                    raise
                old_offset = (page - 1) * page_size
                page_size = max(page_size // 2, MIN_PAGE_SIZE)
                page = (old_offset // page_size) + 1
                total_pages = None  # reset; Oracle will report new page_count at the new page_size
                log.warning(
                    f"{entity}: page timed out, retrying at page {page} with page_size={page_size}"
                )

        page_ms = round((time.time() - page_start) * 1000)
        total_api_ms += page_ms
        max_page_ms = max(max_page_ms, page_ms)

        records = response_data.get("results", [])

        # Oracle's page_count can decrease mid-pagination (stale cache). Accept lower values
        # immediately — a lower bound stops the loop before a phantom last page produces a 500.
        new_page_count = response_data.get("page_count")
        if new_page_count is not None:
            if total_pages is not None and new_page_count < total_pages:
                log.warning(
                    f"{entity}: page_count dropped {total_pages} → {new_page_count} on page {page}"
                )
                total_pages = new_page_count
            else:
                total_pages = (
                    new_page_count if total_pages is None else max(total_pages, new_page_count)
                )

        if len(records) == 0:
            is_exhausted = True
            break

        current_page = response_data.get("page_nbr", page)
        if current_page % 50 == 0:
            pct_str = f"{round(current_page / total_pages * 100, 1)}%" if total_pages else "?"
            log.info(
                f"{entity}: page {current_page}/{total_pages or '?'} ({pct_str}) "
                f"— {len(records)} records this page, {total_records + len(records)} synced so far"
            )

        for record in records:
            ts = record.get("mod_ts")
            if ts:
                if extreme_mod_ts is None:
                    extreme_mod_ts = ts
                elif is_desc:
                    extreme_mod_ts = min(extreme_mod_ts, ts)
                else:
                    extreme_mod_ts = max(extreme_mod_ts, ts)

        if records_callback:
            records_callback(records)

        total_records += len(records)
        page += 1
        pages_fetched += 1
        pages_since_checkpoint += 1

        if (
            is_desc
            and max_pages is not None
            and pages_fetched == max_pages
            and ts_when_max_reached is None
        ):
            ts_when_max_reached = extreme_mod_ts
            if ts_when_max_reached:
                log.warning(
                    f"{entity}: reached max_pages={max_pages} but cursor is still "
                    f"{ts_when_max_reached} — possible bulk import; continuing until timestamp changes"
                )

        if (
            checkpoint_callback
            and pages_since_checkpoint >= CHECKPOINT_INTERVAL_PAGES
            and extreme_mod_ts
        ):
            checkpoint_callback(extreme_mod_ts)
            pages_since_checkpoint = 0

    if ts_when_max_reached is not None and extreme_mod_ts != ts_when_max_reached:
        log.info(
            f"{entity}: timestamp changed {ts_when_max_reached} → {extreme_mod_ts} after {pages_fetched} pages — bulk import cleared"
        )

    finished = is_exhausted or (total_pages is not None and page > total_pages)
    elapsed = time.time() - fetch_start
    if phase_label:
        log.info(
            f"{entity}: {phase_label} complete — {total_records} records, {pages_fetched} pages"
        )
    else:
        avg_ms = round(total_api_ms / pages_fetched) if pages_fetched else 0
        rps = round(total_records / elapsed, 1) if elapsed else 0
        log.info(
            f"Fetch complete for {entity}: {total_records} records in {round(elapsed, 1)}s "
            f"({rps} rec/s) — {pages_fetched} pages, page_size={page_size}, "
            f"avg {avg_ms}ms/page, max {max_page_ms}ms/page, finished={finished}"
        )
    return total_records, extreme_mod_ts, finished
