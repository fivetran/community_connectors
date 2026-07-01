# For time-window calculations in incremental syncs
from datetime import datetime, timezone, timedelta

# For making paginated PI Web API requests
import requests

# For enabling logs in the connector
from fivetran_connector_sdk import Logging as log

# For supporting data operations like upsert() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Local helpers for API pagination and record extraction
from client import paginate
from models import (
    fmt_ts,
    parse_pi_timestamp,
    extract_element,
    extract_attribute,
    extract_event_frame,
    extract_recorded_value,
)

# Maximum items per PI Web API response page
__MAX_COUNT = 1000

# Starting size of the incremental time window (days)
__INITIAL_WINDOW_DAYS = 30

# If the adaptive window shrinks below this threshold, surface the error
__MIN_WINDOW_HOURS = 1

# Roll back the cursor by this many hours on subsequent syncs to capture late-arriving data
__LATE_ARRIVAL_ROLLBACK_HOURS = 2

# Emit a checkpoint every N rows during full reimport to signal liveness
__CHECKPOINT_INTERVAL = 10_000


def _handle_window_failure(
    table_name: str, window: timedelta, exc: Exception
) -> timedelta:
    """
    Halve the time window on a transient failure and raise if it drops below the minimum.

    Used by both sync_event_frames and sync_recorded_values to apply the same
    adaptive backoff policy.

    Args:
        table_name: name of the table being synced, used in log/error messages.
        window: the current time window size.
        exc: the exception that triggered the failure.
    Returns:
        The halved window timedelta.
    Raises:
        RuntimeError: if the halved window would drop below __MIN_WINDOW_HOURS.
    """
    new_window = window / 2
    if new_window < timedelta(hours=__MIN_WINDOW_HOURS):
        raise RuntimeError(
            f"{table_name} sync failed; window cannot be halved below "
            f"{__MIN_WINDOW_HOURS}h. Last error: {exc}"
        ) from exc
    log.warning(
        f"  {table_name} window failed; halving to "
        f"{int(new_window.total_seconds() // 3600)}h. Error: {exc}"
    )
    return new_window


def sync_elements(
    session: requests.Session, base: str, db_web_id: str, state: dict
) -> None:
    """
    Full reimport of all PI AF elements in the database (the asset hierarchy).

    Checkpoints every __CHECKPOINT_INTERVAL rows during large syncs.

    Args:
        session: an authenticated requests.Session.
        base: the PI Web API base URL.
        db_web_id: the WebId of the target AF database.
        state: the current connector state dict (mutated in place on checkpoint).
    """
    log.info("Syncing elements (full reimport)")
    count = 0

    for item in paginate(
        session,
        f"{base}/assetdatabases/{db_web_id}/elements",
        params={"searchFullHierarchy": "true", "maxCount": __MAX_COUNT},
    ):
        # The 'upsert' operation inserts or updates a row in the destination table.
        # The first argument is the destination table name.
        # The second argument is the record dict to upsert.
        op.upsert(table="elements", data=extract_element(item))
        count += 1
        if count % __CHECKPOINT_INTERVAL == 0:
            # Save progress so the sync can resume if interrupted mid-reimport.
            op.checkpoint(state)
            log.info(f"  elements: {count} rows synced")

    # Final checkpoint after all rows are processed.
    op.checkpoint(state)
    log.info(f"elements sync complete ({count} rows)")


def sync_attributes(
    session: requests.Session, base: str, db_web_id: str, state: dict
) -> list:
    """
    Full reimport of all PI AF element attributes in the database.

    Attempts the database-wide /elementattributes endpoint first (PI Web API 2019+).
    Falls back to iterating each element individually if that endpoint returns a 4xx error.

    Returns the list of WebIds for PI Point attributes, which are used by
    sync_recorded_values to fetch time-series data.

    Args:
        session: an authenticated requests.Session.
        base: the PI Web API base URL.
        db_web_id: the WebId of the target AF database.
        state: the current connector state dict.
    Returns:
        List of WebId strings for PI Point attributes found in the database.
    """
    log.info("Syncing attributes (full reimport)")
    count = 0
    pi_point_web_ids = []

    def _process(item, element_web_id):
        nonlocal count
        # The 'upsert' operation inserts or updates a row in the destination table.
        op.upsert(table="attributes", data=extract_attribute(item, element_web_id))
        count += 1
        if item.get("DataReferencePlugIn") == "PI Point":
            pi_point_web_ids.append(item["WebId"])
        if count % __CHECKPOINT_INTERVAL == 0:
            # Save progress so the sync can resume if interrupted mid-reimport.
            op.checkpoint(state)
            log.info(f"  attributes: {count} rows synced")

    try:
        for item in paginate(
            session,
            f"{base}/assetdatabases/{db_web_id}/elementattributes",
            params={"searchFullHierarchy": "true", "maxCount": __MAX_COUNT},
        ):
            element_web_id = (
                item["Element"]["WebId"]
                if isinstance(item.get("Element"), dict)
                else ""
            )
            _process(item, element_web_id)
    except ValueError as exc:
        # Only fall back if the endpoint returned a non-auth 4xx (e.g. 404/405 — endpoint
        # not available on this PI Web API version). Auth failures should surface immediately.
        if "Authentication error" in str(exc):
            raise
        log.info(
            "  /elementattributes not available; falling back to per-element fetch"
        )
        for elem_item in paginate(
            session,
            f"{base}/assetdatabases/{db_web_id}/elements",
            params={"searchFullHierarchy": "true", "maxCount": __MAX_COUNT},
        ):
            elem_web_id = elem_item.get("WebId", "")
            try:
                for attr_item in paginate(
                    session,
                    f"{base}/elements/{elem_web_id}/attributes",
                    params={"maxCount": __MAX_COUNT},
                ):
                    _process(attr_item, elem_web_id)
            except ValueError as exc:
                log.warning(f"  Skipping attributes for element {elem_web_id}: {exc}")

    # Final checkpoint after all attribute rows are processed.
    op.checkpoint(state)
    log.info(
        f"attributes sync complete ({count} rows, "
        f"{len(pi_point_web_ids)} PI Point attributes)"
    )
    return pi_point_web_ids


def sync_event_frames(
    session: requests.Session, base: str, db_web_id: str, state: dict, start_date: str
) -> None:
    """
    Cursor-based incremental sync of PI AF event frames, keyed on start_time.

    Uses an adaptive time-window strategy: starts with __INITIAL_WINDOW_DAYS-day
    windows and halves the window on request failures. Raises if the window shrinks
    below __MIN_WINDOW_HOURS and the query still fails.

    Checkpoints state after each successful time window so the sync can resume
    from the last safe point after an interruption.

    Args:
        session: an authenticated requests.Session.
        base: the PI Web API base URL.
        db_web_id: the WebId of the target AF database.
        state: the current connector state dict (cursor stored under state["cursors"]).
        start_date: ISO 8601 fallback start date used on the first sync.
    """
    cursors = state.setdefault("cursors", {})
    start = parse_pi_timestamp(
        cursors.get("event_frames", start_date)
    ) or datetime.fromtimestamp(0, tz=timezone.utc)
    now = datetime.now(timezone.utc)
    window = timedelta(days=__INITIAL_WINDOW_DAYS)
    total = 0

    log.info(f"Syncing event_frames (incremental from {fmt_ts(start)})")

    while start < now:
        end = min(start + window, now)
        window_count = 0

        try:
            for item in paginate(
                session,
                f"{base}/assetdatabases/{db_web_id}/eventframes",
                params={
                    "searchFullHierarchy": "true",
                    "startTime": fmt_ts(start),
                    "endTime": fmt_ts(end),
                    "maxCount": __MAX_COUNT,
                },
            ):
                # The 'upsert' operation inserts or updates a row in the destination table.
                op.upsert(
                    table="event_frames", data=extract_event_frame(item, db_web_id)
                )
                window_count += 1

            cursors["event_frames"] = end.isoformat()
            # Save progress after each successful time window so the sync can
            # resume from this point if interrupted on the next window.
            op.checkpoint(state)
            log.info(
                f"  event_frames {fmt_ts(start)} → {fmt_ts(end)}: {window_count} rows"
            )
            total += window_count
            start = end

        except ValueError:
            # Auth / client errors are not transient — surface immediately
            raise
        except requests.exceptions.RequestException as exc:
            # Transient network or server error — halve the window and retry
            window = _handle_window_failure("event_frames", window, exc)

    log.info(f"event_frames sync complete ({total} rows)")


def sync_recorded_values(
    session: requests.Session,
    base: str,
    pi_point_web_ids: list,
    state: dict,
    start_date: str,
) -> None:
    """
    Cursor-based incremental sync of PI archive (recorded) values for PI Point attributes.

    A single time cursor is shared across all PI Point attributes. On subsequent syncs
    the cursor is rolled back by __LATE_ARRIVAL_ROLLBACK_HOURS to capture values that
    were written slightly after their timestamps.

    Individual attribute streams that return 4xx errors (e.g. deleted PI Points) are
    skipped with a warning so one bad attribute does not fail the entire sync.

    Args:
        session: an authenticated requests.Session.
        base: the PI Web API base URL.
        pi_point_web_ids: list of PI Point attribute WebIds collected by sync_attributes.
        state: the current connector state dict (cursor stored under state["cursors"]).
        start_date: ISO 8601 fallback start date used on the first sync.
    """
    if not pi_point_web_ids:
        log.info("No PI Point attributes found; skipping recorded_values sync")
        return

    cursors = state.setdefault("cursors", {})
    start_str = cursors.get("recorded_values")
    is_first_sync = start_str is None
    start = parse_pi_timestamp(start_str or start_date) or datetime.fromtimestamp(
        0, tz=timezone.utc
    )

    if not is_first_sync:
        start = start - timedelta(hours=__LATE_ARRIVAL_ROLLBACK_HOURS)
        log.info(f"  recorded_values: late-arrival rollback applied → {fmt_ts(start)}")

    now = datetime.now(timezone.utc)
    window = timedelta(days=__INITIAL_WINDOW_DAYS)
    total = 0

    log.info(
        f"Syncing recorded_values (incremental from {fmt_ts(start)}, "
        f"{len(pi_point_web_ids)} PI Point attributes)"
    )

    while start < now:
        end = min(start + window, now)
        window_count = 0

        try:
            for attr_web_id in pi_point_web_ids:
                try:
                    for item in paginate(
                        session,
                        f"{base}/streams/{attr_web_id}/recorded",
                        params={
                            "startTime": fmt_ts(start),
                            "endTime": fmt_ts(end),
                            "maxCount": __MAX_COUNT,
                        },
                    ):
                        # The 'upsert' operation inserts or updates a row in the destination table.
                        op.upsert(
                            table="recorded_values",
                            data=extract_recorded_value(item, attr_web_id),
                        )
                        window_count += 1
                except ValueError as exc:
                    # 404 = PI Point deleted; 403 = no permission — skip this attribute
                    log.warning(f"  Skipping stream {attr_web_id}: {exc}")

            cursors["recorded_values"] = end.isoformat()
            # Save progress after each complete time window across all attributes.
            op.checkpoint(state)
            log.info(
                f"  recorded_values {fmt_ts(start)} → {fmt_ts(end)}: {window_count} rows"
            )
            total += window_count
            start = end

        except ValueError:
            raise
        except requests.exceptions.RequestException as exc:
            # Transient network or server error — halve the window and retry
            window = _handle_window_failure("recorded_values", window, exc)

    log.info(f"recorded_values sync complete ({total} rows)")
