"""Incremental sync for Oracle WMS entities.

Two-phase approach:

  Phase 1 (mod_ts) — cursor-advancement pagination (ordering="mod_ts,id"):
    Fetches records where mod_ts__gte=cursor and mod_ts__lt=sync_start_time.
    After each full page, if the page's max mod_ts is greater than the cursor,
    advance the cursor and restart from page 1. For same-timestamp batches
    (page max == cursor), increment the page offset instead — safe because
    ordering="mod_ts,id" gives stable offsets within a same-ts group.
    Stops on empty page or partial page.

  Phase 2 (create_ts) — backdated-record catch-up (create_ts__gte=cursor):
    Fetches records where create_ts__gte=cursor and create_ts__lt=sync_start_time.
    A separate request is needed because Oracle ANDs all query params; catching records
    that have an old mod_ts but were created after the cursor requires OR semantics.
    Upserts are idempotent so overlap with Phase 1 is harmless.
    Phase 2's max(mod_ts) is discarded — the cursor is driven by Phase 1 only,
    since backdated records must not push the cursor forward.
"""

import time
import requests
from datetime import datetime, timezone
from typing import Optional, Tuple

from fivetran_connector_sdk import Logging as log

from api import make_api_request, fetch_entity_data
from utils import (
    MIN_PAGE_SIZE,
    INCREMENTAL_CHECKPOINT_INTERVAL_SECONDS,
    normalize_timestamp_to_oracle_format,
    OrderingNotSupportedError,
)


def run_incremental_phase(
    base_url: str,
    username: str,
    password: str,
    entity: str,
    cursor: str,
    sync_start_time: str,
    page_size: int,
    handle_records,
    session: requests.Session,
    checkpoint_fn=None,
) -> Tuple[int, Optional[str]]:
    """
    Run Phase 1 (mod_ts cursor-advancement) and Phase 2 (create_ts catch-up).

    Returns:
        (total_records, incremental_max_mod_ts)
        incremental_max_mod_ts is None if no records were returned in Phase 1.
    """
    cursor_dt = datetime.fromisoformat(cursor)
    limit_str = normalize_timestamp_to_oracle_format(sync_start_time)
    cursor_utc_str = cursor_dt.astimezone(timezone.utc).isoformat(timespec="seconds")
    sync_start_utc_str = (
        datetime.fromisoformat(sync_start_time.replace("Z", "+00:00"))
        .astimezone(timezone.utc)
        .isoformat(timespec="seconds")
    )

    total_records = 0
    incremental_max_mod_ts = None
    page_size_1a = page_size
    phase1a_page = 1
    phase1a_pages = 0
    phase1a_records = 0
    last_checkpoint_wall = time.monotonic()

    log.info(
        f"Starting incremental Phase 1 (mod_ts) for {entity}: "
        f"{cursor_utc_str} → {sync_start_utc_str} (cursor={cursor})"
    )

    # ── Phase 1 (mod_ts) ──────────────────────────────────────────────────────
    while True:
        cursor_str = normalize_timestamp_to_oracle_format(cursor_dt.isoformat())

        # Adaptive page-size retry on timeout
        while True:
            try:
                response_data = make_api_request(
                    base_url,
                    username,
                    password,
                    entity,
                    page=phase1a_page,
                    mod_ts_filter=cursor_str,
                    mod_ts_lt_filter=limit_str,
                    ordering="mod_ts,id",
                    page_size=page_size_1a,
                    session=session,
                )
                break
            except requests.exceptions.Timeout:
                if page_size_1a <= MIN_PAGE_SIZE:
                    log.error(
                        f"{entity}: Phase 1 (mod_ts) page {phase1a_page} timed out at minimum "
                        f"page_size={page_size_1a}, giving up"
                    )
                    raise
                page_size_1a = max(page_size_1a // 2, MIN_PAGE_SIZE)
                phase1a_page = 1
                log.warning(
                    f"{entity}: Phase 1 (mod_ts) page timed out, restarting from page 1 "
                    f"with page_size={page_size_1a}"
                )

        records = response_data.get("results", [])
        phase1a_pages += 1

        if not records:
            break

        handle_records(records)
        total_records += len(records)
        phase1a_records += len(records)

        page_max_ts = max(
            (r["mod_ts"] for r in records if r.get("mod_ts")),
            default=None,
        )
        if page_max_ts:
            if incremental_max_mod_ts is None or page_max_ts > incremental_max_mod_ts:
                incremental_max_mod_ts = page_max_ts
            page_max_dt = datetime.fromisoformat(page_max_ts)
            if page_max_dt > cursor_dt:
                # New timestamps seen — restart from page 1 at the advanced cursor
                cursor_dt = page_max_dt
                phase1a_page = 1
                if checkpoint_fn and (
                    time.monotonic() - last_checkpoint_wall
                    >= INCREMENTAL_CHECKPOINT_INTERVAL_SECONDS
                ):
                    checkpoint_fn(cursor_dt)
                    last_checkpoint_wall = time.monotonic()
            else:
                # Same-timestamp batch — advance the page offset (stable with id tiebreaker)
                phase1a_page += 1
        else:
            phase1a_page += 1

        if len(records) < page_size_1a:
            break  # Partial page: no more records in this window

    log.info(
        f"{entity}: Phase 1 (mod_ts) complete — {phase1a_records} records, {phase1a_pages} pages"
    )

    # ── Phase 2 (create_ts) ───────────────────────────────────────────────────
    log.info(
        f"Starting incremental Phase 2 (create_ts) for {entity}: "
        f"{cursor_utc_str} → {sync_start_utc_str} (cursor={cursor})"
    )
    try:
        count_1b, _, _ = fetch_entity_data(
            base_url,
            username,
            password,
            entity,
            create_ts_gte_filter=cursor,
            create_ts_lt_filter=sync_start_time,
            ordering="create_ts,id",
            page_size=page_size,
            records_callback=handle_records,
            session=session,
            phase_label="Phase 2 (create_ts)",
        )
        total_records += count_1b
    except OrderingNotSupportedError:
        log.warning(
            f"{entity}: Phase 2 (create_ts) ordering=create_ts,id not supported "
            f"— retrying without ordering"
        )
        try:
            count_1b, _, _ = fetch_entity_data(
                base_url,
                username,
                password,
                entity,
                create_ts_gte_filter=cursor,
                page_size=page_size,
                records_callback=handle_records,
                session=session,
                phase_label="Phase 2 (create_ts)",
            )
            total_records += count_1b
        except Exception as e:
            log.warning(f"{entity}: Phase 2 (create_ts) skipped — {e}")
    except Exception as e:
        log.warning(f"{entity}: Phase 2 (create_ts) skipped — {e}")

    return total_records, incremental_max_mod_ts
