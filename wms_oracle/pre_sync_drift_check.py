"""Pre-sync hourly drift check for Oracle WMS connector.

Before each sync, probes the count for every clock-aligned hourly window in the
24 hours preceding each entity's cursor. Compares against counts saved from the
prior sync, and re-pulls any hour whose count increased (long-running transactions
that committed after the cursor advanced past their mod_ts window).

Window design
─────────────
24 full clock-aligned windows:  [cursor_aligned - i*h, cursor_aligned - (i-1)*h)
  Keyed by absolute UTC hour_str. Comparable across syncs. Saved to state.

1 partial window (if cursor is not exactly on the hour):
  [cursor_aligned, cursor_dt)
  Upserted to Snowflake for visibility but NEVER compared against previous state
  (this window legitimately grows each sync as the cursor advances within the
  current hour and would produce false positives if compared).
  NOT saved to state.

Blind spot: transactions delayed by less than ~60 minutes (sub-hour gap) are not
detected here — that range falls within the current-hour partial window.
"""

import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api import probe_entity_count, fetch_entity_data
from utils import MAX_CONCURRENT_ENTITIES


def run_pre_cursor_hourly_check(
    base_url: str,
    username: str,
    password: str,
    entities: List[str],
    entity_cursors: Dict[str, str],
    sync_start_time: str,
    prev_hourly_counts: Dict[str, Dict[str, int]],
    page_size: int,
    hours: int = 24,
) -> Tuple[Dict[str, Dict[str, int]], List[Tuple[str, str]]]:
    """
    Probe hourly counts for the 24 hours before each entity's cursor, detect
    increases, re-pull and verify any triggered windows.

    Args:
        entities:           Entities that have an incremental cursor in state.
        entity_cursors:     {entity: cursor_str} — current state values.
        sync_start_time:    Used as batch_id for Snowflake rows.
        prev_hourly_counts: {entity: {hour_str: count}} — saved from prior sync.
        page_size:          Page size used for re-pull pagination.

    Returns:
        (new_counts, summary_lines) where:
          new_counts:    {entity: {hour_str: count}} — full windows only, saved to state.
          summary_lines: [("info"|"warning", message)] — repull log lines to replay
                         at the end of the sync summary. Empty if no repulls triggered.
    """
    hourly_check_start = time.time()
    batch_id = sync_start_time

    # ── Build task list ───────────────────────────────────────────────────────
    # Each task: (entity, hour_str, gte, lt, is_partial)
    tasks: List[Tuple[str, str, str, str, bool]] = []

    for entity in entities:
        cursor_str = entity_cursors[entity]
        cursor_dt = datetime.fromisoformat(cursor_str.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
        cursor_aligned = cursor_dt.replace(minute=0, second=0, microsecond=0)

        # Full clock-aligned hourly windows (oldest → newest)
        for hour_offset in range(hours, 0, -1):
            window_start = cursor_aligned - timedelta(hours=hour_offset)
            window_end = cursor_aligned - timedelta(hours=hour_offset - 1)
            hour_str = window_start.strftime("%Y-%m-%dT%H:%M:%SZ")
            gte = window_start.strftime("%Y-%m-%dT%H:%M:%SZ")
            lt = window_end.strftime("%Y-%m-%dT%H:%M:%SZ")
            tasks.append((entity, hour_str, gte, lt, False))

        # 1 partial window: [cursor_aligned, cursor_dt) — only if sub-hour gap exists
        if cursor_dt > cursor_aligned:
            hour_str = cursor_aligned.strftime("%Y-%m-%dT%H:%M:%SZ")
            gte = cursor_aligned.strftime("%Y-%m-%dT%H:%M:%SZ")
            lt = cursor_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            tasks.append((entity, hour_str, gte, lt, True))

    if not tasks:
        log.info("Pre-cursor hourly check: no entities with cursors, skipping")
        return {}, []

    # ── Probe counts in parallel ──────────────────────────────────────────────
    new_counts: Dict[str, Dict[str, int]] = {}
    hours_to_repull: List[Tuple[str, str, str, str, int, int]] = []
    summary_lines: List[Tuple[str, str]] = []

    def fetch_count(task: Tuple[str, str, str, str, bool]):
        entity, hour_str, gte, lt, is_partial = task
        count = probe_entity_count(
            base_url,
            username,
            password,
            entity,
            mod_ts_filter=gte,
            mod_ts_lt_filter=lt,
        )
        return entity, hour_str, gte, lt, is_partial, count

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_ENTITIES) as executor:
        futures = {executor.submit(fetch_count, task): task for task in tasks}
        for future in as_completed(futures):
            entity, hour_str, gte, lt, is_partial, count = future.result()

            # Full windows: compare and save to state
            if not is_partial:
                prev_count = prev_hourly_counts.get(entity, {}).get(hour_str)
                if prev_count is not None and count > prev_count:
                    log.warning(
                        f"Pre-cursor drift: {entity} {hour_str} count increased {prev_count} → {count}"
                    )
                    hours_to_repull.append((entity, hour_str, gte, lt, prev_count, count))

                if entity not in new_counts:
                    new_counts[entity] = {}
                new_counts[entity][hour_str] = count

            # Upsert hourly count into the monitoring table for observability.
            op.upsert(
                "pre_cursor_hourly_counts",
                {
                    "table_name": entity,
                    "hour_start": hour_str,
                    "record_count": count,
                    "batch_id": batch_id,
                    "is_partial": is_partial,
                },
            )

    # ── Re-pull triggered hours (sequential) ─────────────────────────────────
    for entity, hour_str, gte, lt, prev_count, new_count in hours_to_repull:
        drift_msg = (
            f"Pre-cursor drift: {entity} {hour_str} count increased {prev_count} → {new_count}"
        )
        log.warning(drift_msg)
        summary_lines.append(("warning", drift_msg))

        repull_msg = (
            f"Re-pulling {entity} {hour_str} (count increased {prev_count} → {new_count})…"
        )
        log.info(repull_msg)
        summary_lines.append(("info", repull_msg))

        with requests.Session() as session:
            total_pulled, _, _ = fetch_entity_data(
                base_url,
                username,
                password,
                entity,
                mod_ts_filter=gte,
                mod_ts_lt_filter=lt,
                ordering="mod_ts,id",
                page_size=page_size,
                records_callback=lambda records: [op.upsert(entity, r) for r in records],
                session=session,
                phase_label=f"pre-cursor re-pull {hour_str}",
            )

        complete_msg = f"{entity}: pre-cursor re-pull {hour_str} complete — {total_pulled} records"
        log.info(complete_msg)
        summary_lines.append(("info", complete_msg))

        # Verify post-repull count matches what triggered the re-pull
        verified_count = probe_entity_count(
            base_url,
            username,
            password,
            entity,
            mod_ts_filter=gte,
            mod_ts_lt_filter=lt,
        )
        if verified_count == new_count:
            verify_msg = f"Verified: {entity} {hour_str} confirmed at {new_count} ({total_pulled} records re-pulled)"
            log.info(verify_msg)
            summary_lines.append(("info", verify_msg))
        else:
            verify_msg = (
                f"Unverified: {entity} {hour_str} expected {new_count}, "
                f"got {verified_count} — data may still be changing"
            )
            log.warning(verify_msg)
            summary_lines.append(("warning", verify_msg))

    elapsed = round(time.time() - hourly_check_start, 1)
    check_summary = (
        f"Pre-cursor hourly check: {len(tasks)} windows probed across {len(entities)} entities "
        f"({hours}h lookback) in {elapsed}s | re-pulls triggered: {len(hours_to_repull)}"
    )
    log.info(check_summary)
    summary_lines.append(("info", check_summary))

    return new_counts, summary_lines


def run_daily_counts(
    base_url: str,
    username: str,
    password: str,
    entities: List[str],
    sync_start_time: str,
    days: int = 30,
) -> None:
    """
    Probe daily mod_ts counts for the last N calendar days for each incremental entity
    and upsert to counts_by_day. Runs all probes in parallel.

    Args:
        entities:        Incremental entities (those with mod_ts support and a cursor).
        sync_start_time: Used as batch_id and as the reference point for day alignment.
        days:            Number of calendar days to cover (default 30).
    """
    if not entities:
        return

    batch_id = sync_start_time
    sync_dt = datetime.fromisoformat(sync_start_time.replace("Z", "+00:00")).astimezone(
        timezone.utc
    )
    today = sync_dt.replace(hour=0, minute=0, second=0, microsecond=0)

    # Build one task per (entity, calendar day)
    tasks: List[tuple] = []
    for entity in entities:
        for day_offset in range(days):
            day_start = today - timedelta(days=day_offset)
            day_end = day_start + timedelta(days=1)
            day_str = day_start.strftime("%Y-%m-%d")
            gte = day_start.strftime("%Y-%m-%dT%H:%M:%SZ")
            lt = day_end.strftime("%Y-%m-%dT%H:%M:%SZ")
            tasks.append((entity, day_str, gte, lt))

    def fetch_count(task: tuple):
        entity, day_str, gte, lt = task
        count = probe_entity_count(
            base_url,
            username,
            password,
            entity,
            mod_ts_filter=gte,
            mod_ts_lt_filter=lt,
        )
        return entity, day_str, count

    daily_counts_start = time.time()
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_ENTITIES) as executor:
        futures = {executor.submit(fetch_count, task): task for task in tasks}
        for future in as_completed(futures):
            entity, day_str, count = future.result()
            # Upsert daily count into the monitoring table for observability.
            op.upsert(
                "counts_by_day",
                {
                    "table_name": entity,
                    "mod_ts_day": day_str,
                    "record_count": count,
                    "batch_id": batch_id,
                },
            )

    elapsed = round(time.time() - daily_counts_start, 1)
    log.info(
        f"Daily counts: {len(tasks)} windows probed across {len(entities)} entities "
        f"({days}d lookback) in {elapsed}s"
    )
