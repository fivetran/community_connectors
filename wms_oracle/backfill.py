"""Historical backfill for Oracle WMS entities.

Fetches records in DESC order (newest first) within rolling 30-day windows so the
destination is populated with recent data as quickly as possible.

Pagination strategy:
  - ordering="-mod_ts,id" for stable pagination within same-timestamp groups
  - Full pages: advance the page offset within the current window
  - Past max_pages: keep fetching until page_max_ts < prev_page_min_ts, which guarantees
    the entire same-timestamp group at the boundary has been consumed before checkpointing
  - Partial page: window exhausted — slide cursor back BACKFILL_WINDOW_DAYS and reset
  - Empty window: slide back and checkpoint; stop after BACKFILL_MAX_EMPTY_WINDOWS consecutive
"""

import requests
from datetime import datetime, timedelta
from typing import Optional, Tuple

from fivetran_connector_sdk import Logging as log

from api import make_api_request
from utils import (
    MIN_PAGE_SIZE,
    BACKFILL_WINDOW_DAYS,
    BACKFILL_MAX_EMPTY_WINDOWS,
    normalize_timestamp_to_oracle_format,
)


def run_backfill_phase(
    base_url: str,
    username: str,
    password: str,
    entity: str,
    backfill_cursor: Optional[str],
    max_pages: int,
    page_size: int,
    handle_records,
    checkpoint_fn,
    session: requests.Session,
) -> Tuple[int, Optional[str], bool]:
    """
    Run the historical backfill for one entity.

    Args:
        backfill_cursor: Upper bound (mod_ts__lt) for the current window. None on first sync.
        max_pages:       Soft page limit per sync. Fetching continues past this until the
                         timestamp group at the boundary is fully consumed.
        handle_records:  Callable(records: list) — called for each page of results.
        checkpoint_fn:   Callable(cursor_dt: datetime) — saves backfill progress to state.

    Returns:
        (total_records, new_backfill_cursor, finished)
        new_backfill_cursor: None if backfill completed, otherwise the new window anchor.
        finished: True if all historical data has been consumed.

    Raises:
        OrderingNotSupportedError: if Oracle returns 400 for -mod_ts ordering.
                                   Caller should fall back to an unordered full scan.
    """
    if backfill_cursor is None:
        log.info(f"Starting historical backfill for {entity} (DESC, offset pagination)")
    else:
        log.info(
            f"Resuming backfill for {entity}: window cursor={backfill_cursor} "
            f"(DESC, offset pagination)"
        )

    cursor_dt: Optional[datetime] = (
        datetime.fromisoformat(backfill_cursor) if backfill_cursor else None
    )
    bf_page = 1
    bf_pages_fetched = 0
    bf_records = 0
    bf_pages_since_log = 0
    bf_min_mod_ts_seen = None
    bf_prev_page_min_ts = None
    bf_page_size = page_size
    bf_finished = False
    bf_consecutive_empty = 0
    bf_cursor_rollback = None  # set on timeout when prev page data exists

    while True:
        # ── Cursor rollback: a mid-window timeout occurred on a previous pass ──
        # Pages 1..N-1 were already fetched. Roll the window cursor back to the
        # oldest mod_ts seen so far so we restart from a stable Oracle offset.
        if bf_cursor_rollback is not None:
            cursor_dt = datetime.fromisoformat(bf_cursor_rollback)
            log.warning(
                f"{entity}: backfill rolling back cursor to {bf_cursor_rollback} "
                f"after timeout, restarting from page 1"
            )
            bf_cursor_rollback = None
            bf_page = 1
            bf_prev_page_min_ts = None

        cursor_str = (
            normalize_timestamp_to_oracle_format(cursor_dt.isoformat()) if cursor_dt else None
        )
        lower_dt = cursor_dt - timedelta(days=BACKFILL_WINDOW_DAYS) if cursor_dt else None
        lower_str = (
            normalize_timestamp_to_oracle_format(lower_dt.isoformat()) if lower_dt else None
        )

        # Adaptive page-size retry on timeout
        while True:
            try:
                response_data = make_api_request(
                    base_url,
                    username,
                    password,
                    entity,
                    page=bf_page,
                    mod_ts_filter=lower_str,
                    mod_ts_lt_filter=cursor_str,
                    ordering="-mod_ts,id",
                    page_size=bf_page_size,
                    session=session,
                )
                break
            except requests.exceptions.Timeout:
                if bf_page_size <= MIN_PAGE_SIZE:
                    log.error(
                        f"{entity}: backfill page {bf_page} timed out at minimum "
                        f"page_size={bf_page_size}, giving up"
                    )
                    raise
                if bf_prev_page_min_ts is not None:
                    # Mid-window timeout: pages 1..N-1 already fetched and safe.
                    # Roll back the cursor to bf_prev_page_min_ts and restart
                    # from page 1 to avoid Oracle pagination instability at the
                    # current offset boundary.
                    # Round UP to the next whole second when sub-second precision
                    # is present: Oracle truncates mod_ts__lt to seconds, so
                    # mod_ts__lt=02:25:50 would exclude records at 02:25:50.440928.
                    _prev_dt = datetime.fromisoformat(bf_prev_page_min_ts)
                    if _prev_dt.microsecond:
                        _prev_dt = _prev_dt.replace(microsecond=0) + timedelta(seconds=1)
                    bf_cursor_rollback = _prev_dt.isoformat()
                    bf_page_size = max(bf_page_size // 2, MIN_PAGE_SIZE)
                    log.warning(
                        f"{entity}: backfill page timed out at page {bf_page} "
                        f"(page_size={bf_page_size * 2}→{bf_page_size}); "
                        f"will roll back cursor to {bf_cursor_rollback}"
                    )
                    break  # exit inner retry loop; outer loop handles the rollback
                else:
                    # First-page timeout: no data fetched yet, just shrink page size
                    old_offset = (bf_page - 1) * bf_page_size
                    bf_page_size = max(bf_page_size // 2, MIN_PAGE_SIZE)
                    bf_page = (old_offset // bf_page_size) + 1
                    log.warning(
                        f"{entity}: backfill page timed out, retrying at page {bf_page} "
                        f"with page_size={bf_page_size}"
                    )

        # If a rollback was triggered in the inner loop, restart the outer loop
        if bf_cursor_rollback is not None:
            continue

        records = response_data.get("results", [])
        bf_pages_fetched += 1
        bf_pages_since_log += 1

        # ── Empty window: slide back one period ──────────────────────────────
        if not records:
            bf_consecutive_empty += 1
            if bf_consecutive_empty >= BACKFILL_MAX_EMPTY_WINDOWS:
                log.info(
                    f"{entity}: {BACKFILL_MAX_EMPTY_WINDOWS} consecutive empty windows "
                    f"— backfill complete"
                )
                bf_finished = True
                break
            log.info(
                f"{entity}: empty window — sliding back {BACKFILL_WINDOW_DAYS} days "
                f"(empty #{bf_consecutive_empty})"
            )
            cursor_dt = lower_dt
            bf_page = 1
            bf_prev_page_min_ts = None
            bf_pages_since_log = 0
            if cursor_dt:
                checkpoint_fn(cursor_dt)
            if bf_pages_fetched >= max_pages:
                break
            continue

        bf_consecutive_empty = 0
        handle_records(records)
        bf_records += len(records)

        page_min_ts = min((r["mod_ts"] for r in records if r.get("mod_ts")), default=None)
        page_max_ts = max((r["mod_ts"] for r in records if r.get("mod_ts")), default=None)

        if page_min_ts and (bf_min_mod_ts_seen is None or page_min_ts < bf_min_mod_ts_seen):
            bf_min_mod_ts_seen = page_min_ts

        # ── Partial page: window exhausted, slide cursor back ─────────────────
        if len(records) < bf_page_size:
            cursor_dt = lower_dt
            bf_page = 1
            bf_prev_page_min_ts = None
            bf_pages_since_log = 0
            if cursor_dt:
                checkpoint_fn(cursor_dt)
            if bf_pages_fetched >= max_pages:
                break
            continue

        # ── Full page: advance offset within the current window ───────────────
        bf_page += 1

        # Past max_pages: keep fetching until the entire current timestamp group is consumed.
        # Break only when this page's max_ts is strictly below the previous page's min_ts —
        # that guarantees no same-ts records remain on unfetched pages.
        if bf_pages_fetched == max_pages:
            log.info(
                f"{entity}: reached max_pages={max_pages} mid-timestamp group "
                f"(current ts={page_max_ts}) — continuing until timestamp changes"
            )
        if (
            bf_pages_fetched >= max_pages
            and bf_prev_page_min_ts is not None  # noqa: W503
            and page_max_ts is not None  # noqa: W503
            and page_max_ts < bf_prev_page_min_ts  # noqa: W503
        ):
            cursor_dt = datetime.fromisoformat(bf_prev_page_min_ts)
            checkpoint_fn(cursor_dt)
            break

        bf_prev_page_min_ts = page_min_ts

        if bf_pages_since_log >= 10:
            log.info(
                f"{entity}: backfill progress — {bf_records:,} records, {bf_pages_fetched} pages, "
                f"oldest mod_ts={bf_min_mod_ts_seen}"
            )
            bf_pages_since_log = 0

    log.info(
        f"{entity}: backfill {'complete' if bf_finished else 'paused'} "
        f"— {bf_records} records, {bf_pages_fetched} pages"
    )

    new_cursor = None if bf_finished else (cursor_dt.isoformat() if cursor_dt else None)
    return bf_records, new_cursor, bf_finished
