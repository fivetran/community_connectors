"""Oracle WMS → Fivetran connector.

Syncs warehouse management entities from the Oracle WMS REST API to a Fivetran destination.

Design overview:
  - Incremental sync: two-phase per entity
      Phase 1 (mod_ts): cursor-advancement with ordering="mod_ts,id" (stable same-ts pagination)
      Phase 2 (create_ts): catch-up for records created after the cursor with a backdated mod_ts
  - Backfill: DESC offset pagination in rolling 30-day windows, newest data first,
      ordering="-mod_ts,id" for stable same-ts pagination
  - Entities with active backfill run in parallel (ThreadPoolExecutor)
  - Incremental-only entities run sequentially (no parallelism benefit, avoids lock contention)
  - Mid-sync backfill checkpoints preserve per-entity progress; final checkpoint clears the flag

"""

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting data operations like upsert(), update(), delete(), and checkpoint()
from fivetran_connector_sdk import Operations as op

# For reading configuration from a JSON file
import json

# For making HTTP requests to the Oracle WMS REST API
import requests

# For parsing and manipulating timestamps
from datetime import datetime

# For type hints
from typing import Optional

# For tracking elapsed time and sync duration
import time

# For parallel entity processing during backfill and mod_ts discovery
from concurrent.futures import ThreadPoolExecutor, as_completed

# For thread-safe checkpointing across concurrent entity workers
from threading import Lock

# For funnelling op.* calls from backfill worker threads to the main thread
import queue

# Constants, entity list, exceptions, configuration validation, and timestamp utilities
from utils import (
    ORACLE_WMS_ENTITIES,
    DEFAULT_PAGE_SIZE,
    DEFAULT_MAX_PAGES,
    MAX_CONCURRENT_ENTITIES,
    OrderingNotSupportedError,
    validate_configuration,
    get_current_timestamp,
    to_utc,
)

# Oracle WMS REST API client: single-page requests, pagination, count probes, and mod_ts discovery
from api import check_entity_has_mod_ts, probe_entity_count, fetch_entity_data

# Two-phase incremental sync logic (mod_ts cursor-advancement + create_ts catch-up)
from incremental import run_incremental_phase

# Historical backfill logic (descending rolling-window offset pagination)
from backfill import run_backfill_phase

# Pre-sync hourly drift detection and monitoring table writes
from pre_sync_drift_check import run_pre_cursor_hourly_check, run_daily_counts

# ── Schema ────────────────────────────────────────────────────────────────────


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": "allocation", "primary_key": ["id"]},
        {"table": "batch_number", "primary_key": ["id"]},
        {"table": "company", "primary_key": ["id"]},
        {"table": "container", "primary_key": ["id"]},
        {"table": "container_lock_xref", "primary_key": ["id"]},
        {"table": "facility", "primary_key": ["id"]},
        {"table": "history_activity", "primary_key": ["id"]},
        {"table": "ib_container", "primary_key": ["id"]},
        {"table": "ib_shipment", "primary_key": ["id"]},
        {"table": "ib_shipment_dtl", "primary_key": ["id"]},
        {"table": "inventory", "primary_key": ["id"]},
        {"table": "inventory_attribute", "primary_key": ["id"]},
        {"table": "inventory_lock", "primary_key": ["id"]},
        {"table": "inventory_status", "primary_key": ["id"]},
        {"table": "item", "primary_key": ["id"]},
        {"table": "item_metric", "primary_key": ["id"]},
        {"table": "location", "primary_key": ["id"]},
        {"table": "order_dtl", "primary_key": ["id"]},
        {"table": "order_hdr", "primary_key": ["id"]},
        {"table": "order_status", "primary_key": ["id"]},
        {"table": "order_type", "primary_key": ["id"]},
        {"table": "purchase_order_dtl", "primary_key": ["id"]},
        {"table": "purchase_order_hdr", "primary_key": ["id"]},
        {"table": "purchase_order_status", "primary_key": ["id"]},
        {"table": "putaway_type", "primary_key": ["id"]},
        {"table": "vendor", "primary_key": ["id"]},
        {"table": "counts_by_day", "primary_key": ["table_name", "mod_ts_day", "batch_id"]},
        {
            "table": "pre_cursor_hourly_counts",
            "primary_key": ["table_name", "hour_start", "batch_id"],
        },
    ]


# ── Entity processing ─────────────────────────────────────────────────────────


def process_entity(
    base_url: str,
    username: str,
    password: str,
    entity: str,
    incremental_cursor: Optional[str],
    backfill_cursor: Optional[str],
    sync_start_time: str,
    entity_cursors_live: dict,
    entity_backfill_cursors_snapshot: dict,
    entity_mod_ts_support_snapshot: dict,
    in_progress_backfill_cursors: dict,
    lock: Lock,
    has_mod_ts: bool = True,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: Optional[int] = None,
    run_incremental: bool = True,
    run_backfill: bool = True,
    output_queue=None,
) -> dict:
    """
    Sync a single entity. run_incremental / run_backfill flags allow the caller
    to run only one phase (e.g. incremental sequentially, then backfill in parallel).
    Returns a result dict consumed by record_result().
    """
    entity_start = time.time()
    try:
        log.info(f"Processing entity: {entity}")

        total_records = 0
        new_backfill_cursor = None
        backfill_ran = False
        incremental_max_mod_ts = None

        def handle_records(records: list):
            for record in records:
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                if output_queue is not None:
                    output_queue.put(("upsert", entity, record))
                else:
                    op.upsert(table=entity, data=record)

        def checkpoint_incremental(cursor_dt: datetime):
            """Checkpoint incremental progress when cursor advances to a new timestamp."""
            cursor_str = to_utc(cursor_dt.isoformat())
            with lock:
                entity_cursors_live[entity] = cursor_str
                # Save progress so the sync can resume from this cursor if interrupted.
                op.checkpoint(
                    {
                        "entity_cursors": dict(entity_cursors_live),
                        "entity_backfill_cursors": {
                            **entity_backfill_cursors_snapshot,
                            **dict(in_progress_backfill_cursors),
                        },
                        "entity_mod_ts_support": dict(entity_mod_ts_support_snapshot),
                        "sync_in_progress": True,
                    }
                )
                log.info(f"Incremental checkpoint for {entity}: cursor={cursor_str}")

        def checkpoint_backfill(cursor_dt: datetime):
            """Checkpoint backfill progress to state. Thread-safe via shared lock."""
            cursor_str = to_utc(cursor_dt.isoformat())
            with lock:
                in_progress_backfill_cursors[entity] = cursor_str
                state_snapshot = {
                    "entity_cursors": dict(entity_cursors_live),
                    "entity_backfill_cursors": {
                        **entity_backfill_cursors_snapshot,
                        **dict(in_progress_backfill_cursors),
                    },
                    "entity_mod_ts_support": dict(entity_mod_ts_support_snapshot),
                    "sync_in_progress": True,
                }
            log.info(f"Backfill checkpoint for {entity}: cursor={cursor_str}")
            if output_queue is not None:
                output_queue.put(("checkpoint", entity, state_snapshot))
            else:
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
                # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
                op.checkpoint(state_snapshot)

        with requests.Session() as session:
            if not has_mod_ts:
                # ── Full sync (no mod_ts support) ────────────────────────────
                log.info(
                    f"Full sync for {entity} "
                    f"(no mod_ts support, existing records _fivetran_deleted = true)"
                )
                # Truncate soft-deletes all existing rows before the full re-scan
                # so removed records are marked deleted.
                if output_queue is not None:
                    output_queue.put(("truncate", entity, None))
                else:
                    op.truncate(table=entity)
                count, _, _ = fetch_entity_data(
                    base_url,
                    username,
                    password,
                    entity,
                    page_size=page_size,
                    records_callback=handle_records,
                    session=session,
                )
                total_records += count

            else:
                # ── Phase 1: Incremental ─────────────────────────────────────
                if run_incremental and incremental_cursor:
                    inc_count, incremental_max_mod_ts = run_incremental_phase(
                        base_url,
                        username,
                        password,
                        entity,
                        cursor=incremental_cursor,
                        sync_start_time=sync_start_time,
                        page_size=page_size,
                        handle_records=handle_records,
                        session=session,
                        checkpoint_fn=checkpoint_incremental,
                    )
                    total_records += inc_count

                # ── Phase 2: Backfill ────────────────────────────────────────
                is_first_sync = not incremental_cursor and backfill_cursor is None
                ongoing_backfill = backfill_cursor is not None

                if run_backfill and (is_first_sync or ongoing_backfill):
                    backfill_ran = True
                    try:
                        # On first sync backfill_cursor is None (no prior state). Seed it at
                        # sync_start_time so the first window starts at the current time and
                        # walks cleanly backwards — prevents a gap between the backfill cursor
                        # and the incremental cursor.
                        initial_backfill_cursor = (
                            backfill_cursor if backfill_cursor is not None else sync_start_time
                        )
                        bf_count, new_backfill_cursor, _ = run_backfill_phase(
                            base_url,
                            username,
                            password,
                            entity,
                            backfill_cursor=initial_backfill_cursor,
                            max_pages=max_pages,
                            page_size=page_size,
                            handle_records=handle_records,
                            checkpoint_fn=checkpoint_backfill,
                            session=session,
                        )
                        total_records += bf_count
                    except OrderingNotSupportedError:
                        log.warning(
                            f"{entity} does not support DESC ordering — falling back to full scan"
                        )
                        count, _, _ = fetch_entity_data(
                            base_url,
                            username,
                            password,
                            entity,
                            page_size=page_size,
                            records_callback=handle_records,
                            session=session,
                        )
                        total_records += count
                        new_backfill_cursor = None

        elapsed = round(time.time() - entity_start, 1)
        return {
            "entity": entity,
            "success": True,
            "record_count": total_records,
            "elapsed_seconds": elapsed,
            "error_msg": None,
            "new_backfill_cursor": new_backfill_cursor,
            "backfill_ran": backfill_ran,
            "new_incremental_cursor": incremental_max_mod_ts,
            "incremental_ran": run_incremental,
        }

    except Exception as e:
        elapsed = round(time.time() - entity_start, 1)
        log.error(f"Failed to process entity {entity}: {e}")
        return {
            "entity": entity,
            "success": False,
            "record_count": 0,
            "elapsed_seconds": elapsed,
            "error_msg": str(e),
            "new_backfill_cursor": backfill_cursor,  # Preserve existing cursor on failure
            "backfill_ran": False,
        }


# ── Sync orchestration ────────────────────────────────────────────────────────


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details, e.g. base_url, username, password.
        state: A dictionary containing state information from previous runs.
               The state dictionary is empty for the first sync or for any full re-sync.

    State structure:
        entity_cursors:          {entity: timestamp} — incremental cursor per entity
        entity_backfill_cursors: {entity: timestamp} — backfill window anchor per entity;
                                 absent once backfill is complete
        entity_mod_ts_support:   {entity: bool} — cached describe-endpoint results
        sync_in_progress:        bool — True in mid-sync checkpoints, False in final checkpoint
    """
    sync_wall_start = time.time()
    log.warning("Example: Oracle WMS : wms_oracle")

    validate_configuration(configuration)

    base_url = configuration.get("base_url")
    username = configuration.get("username")
    password = configuration.get("password")
    page_size = int(configuration.get("page_size", DEFAULT_PAGE_SIZE))
    max_pages = int(configuration.get("max_pages", DEFAULT_MAX_PAGES))
    lookback_check_hours = int(configuration.get("lookback_check_hours", 24))
    test_entities_raw = configuration.get("test_entities")
    test_entities = (
        [e.strip() for e in test_entities_raw.split(",")] if test_entities_raw else None
    )

    entity_cursors = state.get("entity_cursors", {})
    entity_backfill_cursors = state.get("entity_backfill_cursors", {})

    sync_start_time = get_current_timestamp()

    entities_to_sync = [
        e for e in ORACLE_WMS_ENTITIES if test_entities is None or e in test_entities
    ]
    log.info(
        f"Sync started at: {sync_start_time} | "
        f"page_size={page_size}, max_pages={max_pages}, concurrency={MAX_CONCURRENT_ENTITIES}"
    )
    log.info(f"Processing {len(entities_to_sync)} entities")

    # ── Discover mod_ts support (once per entity, cached in state) ────────────
    entity_mod_ts_support = state.get("entity_mod_ts_support", {})
    entities_to_check = [e for e in entities_to_sync if e not in entity_mod_ts_support]
    if entities_to_check:
        log.info(f"Checking mod_ts support for {len(entities_to_check)} entities in parallel")
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_ENTITIES) as executor:
            futures = {
                executor.submit(check_entity_has_mod_ts, base_url, username, password, e): e
                for e in entities_to_check
            }
            for future in as_completed(futures):
                entity = futures[future]
                entity_mod_ts_support[entity] = future.result()
                support_str = "supports" if entity_mod_ts_support[entity] else "does not support"
                sync_type = "incremental" if entity_mod_ts_support[entity] else "full"
                log.info(f"Entity {entity} {support_str} mod_ts - will use {sync_type} sync")

    # ── Probe result counts to sort entities largest-first ────────────────────
    log.info(f"Probing {len(entities_to_sync)} entities to determine processing order…")
    entity_counts = {}
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_ENTITIES) as executor:
        probe_futures = {}
        for entity in entities_to_sync:
            has_mod_ts = entity_mod_ts_support.get(entity, False)
            incremental_cursor = entity_cursors.get(entity) if has_mod_ts else None
            backfill_cursor = entity_backfill_cursors.get(entity) if has_mod_ts else None

            if has_mod_ts and (not incremental_cursor or backfill_cursor is not None):
                probe_ordering, probe_lt, probe_gte = "-mod_ts", backfill_cursor, None
            elif has_mod_ts and incremental_cursor:
                probe_ordering, probe_lt, probe_gte = "mod_ts", None, incremental_cursor
            else:
                probe_ordering, probe_lt, probe_gte = None, None, None

            future = executor.submit(
                probe_entity_count,
                base_url,
                username,
                password,
                entity,
                probe_gte,
                probe_lt,
                probe_ordering,
            )
            probe_futures[future] = entity

        for future in as_completed(probe_futures):
            entity_counts[probe_futures[future]] = future.result()

    entities_to_sync.sort(key=lambda e: entity_counts.get(e, 0), reverse=True)
    order_str = ", ".join(f"{e}({entity_counts.get(e, 0):,})" for e in entities_to_sync)
    log.info(f"Processing order (by record count): {order_str}")

    # ── Classify: incremental-only (sequential) vs backfill/full-scan (parallel) ──
    incremental_only = []
    needs_backfill = []
    for entity in entities_to_sync:
        has_mod_ts = entity_mod_ts_support.get(entity, False)
        incremental_cursor = entity_cursors.get(entity) if has_mod_ts else None
        backfill_cursor = entity_backfill_cursors.get(entity) if has_mod_ts else None
        is_first_sync = has_mod_ts and not incremental_cursor and backfill_cursor is None
        if has_mod_ts and incremental_cursor and backfill_cursor is None and not is_first_sync:
            incremental_only.append(entity)
        else:
            needs_backfill.append(entity)

    log.info(
        f"Execution plan: {len(incremental_only)} incremental-only (sequential), "
        f"{len(needs_backfill)} need backfill/full-scan (parallel)"
    )

    # Shared state updated by worker threads during mid-sync backfill checkpoints
    in_progress_backfill_cursors: dict = {}
    lock = Lock()
    new_entity_backfill_cursors: dict = {}
    completed_entities: set = set()
    entity_results: dict = {}
    all_success = True

    def build_backfill_state() -> dict:
        """Merge completed + in-progress + original cursors into a consistent snapshot."""
        with lock:
            in_progress = dict(in_progress_backfill_cursors)
        result = {
            e: in_progress.get(e, cursor)
            for e, cursor in entity_backfill_cursors.items()
            if e not in completed_entities
        }
        result.update(new_entity_backfill_cursors)
        return result

    def record_result(result: dict):
        """Update cursors and checkpoint after an entity completes."""
        nonlocal all_success
        entity = result["entity"]
        has_mod_ts = entity_mod_ts_support.get(entity, False)
        entity_results[entity] = result
        completed_entities.add(entity)

        if result["success"]:
            if has_mod_ts:
                if result.get("incremental_ran", True):
                    # Only advance cursor when we actually observed records at a new timestamp.
                    # Never jump to sync_start_time on an empty window — an empty response could
                    # indicate a partial or silent API outage rather than a genuinely empty range.
                    new_inc_cursor = result.get("new_incremental_cursor")
                    if new_inc_cursor:
                        entity_cursors[entity] = to_utc(new_inc_cursor)
                if result["backfill_ran"]:
                    if result["new_backfill_cursor"] is not None:
                        new_entity_backfill_cursors[entity] = result["new_backfill_cursor"]
                    else:
                        # Backfill complete — remove from new cursors.
                        # The incremental pass (Pass 1) may have already added this entity
                        # to new_entity_backfill_cursors to preserve the in-progress cursor;
                        # now that backfill is done we must explicitly drop it.
                        new_entity_backfill_cursors.pop(entity, None)
                    # Seed the incremental cursor if not already set — whether backfill
                    # completed or just made progress. Without this, records modified after
                    # the backfill window has passed them are never picked up: backfill won't
                    # revisit them and Phase 1 won't run without a cursor.
                    if not entity_cursors.get(entity):
                        entity_cursors[entity] = sync_start_time
                        log.info(f"{entity}: seeding incremental cursor at {sync_start_time}")
                elif entity in entity_backfill_cursors:
                    # Incremental-only this sync, but backfill still ongoing — preserve cursor
                    new_entity_backfill_cursors[entity] = entity_backfill_cursors[entity]
            else:
                entity_cursors.pop(entity, None)

            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(
                {
                    "entity_cursors": dict(entity_cursors),
                    "entity_backfill_cursors": build_backfill_state(),
                    "entity_mod_ts_support": entity_mod_ts_support,
                    "sync_in_progress": True,
                }
            )
            inc_cursor = entity_cursors.get(entity)
            bf_cursor = new_entity_backfill_cursors.get(entity)
            backfill_just_completed = (
                result.get("backfill_ran") and result.get("new_backfill_cursor") is None
            )
            cursor_parts = []
            if inc_cursor:
                cursor_parts.append(f"cursor={inc_cursor}")
            if backfill_just_completed:
                cursor_parts.append("backfill=complete")
            elif bf_cursor:
                cursor_parts.append(f"backfill={bf_cursor}")
            if cursor_parts:
                log.info(f"Checkpointed {entity}: {', '.join(cursor_parts)}")
        else:
            log.error(f"Failed to process {entity}: {result['error_msg']}")
            all_success = False
            preserved = in_progress_backfill_cursors.get(
                entity, entity_backfill_cursors.get(entity)
            )
            if preserved:
                new_entity_backfill_cursors[entity] = preserved

    def submit_entity(
        entity: str, run_incremental: bool = True, run_backfill: bool = True, output_queue=None
    ) -> dict:
        """Build process_entity kwargs and dispatch."""
        has_mod_ts = entity_mod_ts_support.get(entity, False)
        incremental_cursor = entity_cursors.get(entity) if has_mod_ts else None
        backfill_cursor = entity_backfill_cursors.get(entity) if has_mod_ts else None

        return process_entity(
            base_url,
            username,
            password,
            entity,
            incremental_cursor,
            backfill_cursor,
            sync_start_time,
            entity_cursors,
            dict(entity_backfill_cursors),
            dict(entity_mod_ts_support),
            in_progress_backfill_cursors,
            lock,
            has_mod_ts,
            page_size,
            max_pages,
            run_incremental=run_incremental,
            run_backfill=run_backfill,
            output_queue=output_queue,
        )

    try:
        # ── Pre-pass: hourly drift check for entities with cursors ────────────
        # Probes counts for the 24 clock-aligned hours before each cursor.
        # Re-pulls any hour whose count increased since the last sync.
        entities_with_cursor = [
            e for e in entities_to_sync if entity_mod_ts_support.get(e) and entity_cursors.get(e)
        ]
        prev_hourly_counts = state.get("pre_cursor_hourly_counts", {})
        new_hourly_counts, repull_summary = run_pre_cursor_hourly_check(
            base_url,
            username,
            password,
            entities_with_cursor,
            entity_cursors,
            sync_start_time,
            prev_hourly_counts,
            page_size,
            hours=lookback_check_hours,
        )

        run_daily_counts(
            base_url,
            username,
            password,
            entities_with_cursor,
            sync_start_time,
        )

        # ── Pass 1: Sequential incremental for ALL entities with a cursor ─────
        # Runs before any backfill so incremental phases are never concurrent,
        # regardless of whether an entity also has an active backfill.
        for entity in entities_with_cursor:
            try:
                record_result(submit_entity(entity, run_incremental=True, run_backfill=False))
            except Exception as e:
                log.error(f"Error processing {entity} (incremental): {e}")
                all_success = False
                completed_entities.add(entity)

        # ── Pass 2: Parallel backfill / full-scan ─────────────────────────────
        # Worker threads do API fetching only. All op.* calls are funnelled
        # through result_queue and executed on the main thread to comply with
        # the SDK's single-threaded output stream requirement.
        if needs_backfill:
            result_queue = queue.Queue()

            def run_backfill_worker(ent):
                try:
                    result = submit_entity(
                        ent, run_incremental=False, run_backfill=True, output_queue=result_queue
                    )
                    result_queue.put(("done", ent, result))
                except Exception as exc:
                    result_queue.put(("error", ent, exc))

            with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_ENTITIES) as executor:
                for ent in needs_backfill:
                    executor.submit(run_backfill_worker, ent)

                remaining = len(needs_backfill)
                while remaining > 0:
                    msg_type, ent, data = result_queue.get()
                    if msg_type == "upsert":
                        op.upsert(table=ent, data=data)
                    elif msg_type == "truncate":
                        op.truncate(table=ent)
                    elif msg_type == "checkpoint":
                        op.checkpoint(data)
                    elif msg_type == "done":
                        remaining -= 1
                        record_result(data)
                    elif msg_type == "error":
                        remaining -= 1
                        log.error(f"Error processing {ent} (backfill): {data}")
                        all_success = False
                        completed_entities.add(ent)

        # ── Sync summary ──────────────────────────────────────────────────────
        results_sorted = sorted(entity_results.values(), key=lambda r: r["entity"])
        total_records = sum(r.get("record_count", 0) for r in results_sorted)
        sync_elapsed = round(time.time() - sync_wall_start, 1)
        incremental_results = [
            r for r in results_sorted if entity_mod_ts_support.get(r["entity"], False)
        ]
        full_sync_results = [
            r for r in results_sorted if not entity_mod_ts_support.get(r["entity"], False)
        ]

        log.info(f"--- Sync summary ({sync_elapsed}s wall, {total_records:,} total records) ---")
        for r in incremental_results:
            status = "✓" if r["success"] else "✗"
            entity = r["entity"]
            rec_str = f"{r.get('record_count', 0):,} records in {r.get('elapsed_seconds', '?')}s"
            cursor = entity_cursors.get(entity)
            backfill = new_entity_backfill_cursors.get(entity, "complete")
            cursor_str = f"cursor={cursor}" if cursor else ""
            bf_str = f"backfill={backfill}" if backfill else ""
            detail = " | ".join(p for p in [cursor_str, bf_str] if p)
            log.info(f"  {status} {entity}: {rec_str} | {detail}")
        log.info("--- End: Sync summary ---")

        if full_sync_results:
            log.info("--- Full sync tables ---")
            for r in full_sync_results:
                status = "✓" if r["success"] else "✗"
                rec_str = (
                    f"{r.get('record_count', 0):,} records in {r.get('elapsed_seconds', '?')}s"
                )
                log.info(f"  {status} {r['entity']}: {rec_str}")
            log.info("--- End: Full sync tables ---")

        completed_bf = [
            r["entity"]
            for r in results_sorted
            if r.get("success") and r.get("backfill_ran") and r.get("new_backfill_cursor") is None
        ]
        if completed_bf or new_entity_backfill_cursors:
            log.info("--- Backfill summary ---")
            if completed_bf:
                log.info(f"  Completed this sync: {', '.join(sorted(completed_bf))}")
            if new_entity_backfill_cursors:
                log.info(f"  Still in progress ({len(new_entity_backfill_cursors)} entities):")
                for e, cursor in sorted(new_entity_backfill_cursors.items(), key=lambda x: x[1]):
                    probe_total = entity_counts.get(e, 0)
                    records_this_sync = entity_results.get(e, {}).get("record_count", 0)
                    if probe_total > 0:
                        pct = round(records_this_sync / probe_total * 100, 1)
                        log.info(
                            f"    {e}: reached {cursor} — fetched {pct}% "
                            f"of {probe_total:,} pending records this sync"
                        )
                    else:
                        log.info(f"    {e}: reached {cursor}")
            log.info("--- End: Backfill summary ---")

        if repull_summary:
            log.info("--- Repull Summary ---")
            for level, msg in repull_summary:
                if level == "warning":
                    log.warning(msg)
                else:
                    log.info(msg)
            log.info("--- End: Repull Summary ---")

        # Final checkpoint marks the sync as complete and persists hourly drift counts
        # for the next run.
        op.checkpoint(
            {
                "entity_cursors": entity_cursors,
                "entity_backfill_cursors": new_entity_backfill_cursors,
                "entity_mod_ts_support": entity_mod_ts_support,
                "sync_in_progress": False,
                "pre_cursor_hourly_counts": new_hourly_counts,
            }
        )

        if not all_success:
            log.error(
                f"Sync completed with failures | page_size={page_size}, max_pages={max_pages}"
            )
            raise RuntimeError(
                "One or more entities failed; checkpointed partial progress for retry"
            )
        else:
            log.info(
                f"Sync completed successfully | page_size={page_size}, "
                f"max_pages={max_pages}, concurrency={MAX_CONCURRENT_ENTITIES}"
            )

    except RuntimeError:
        raise
    except Exception as e:
        log.error(f"Oracle WMS Connector failed: {e}")
        raise RuntimeError(f"Failed to sync Oracle WMS data: {e}")


# ── Entry point ───────────────────────────────────────────────────────────────

# Create the connector object using the schema and update functions.
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
