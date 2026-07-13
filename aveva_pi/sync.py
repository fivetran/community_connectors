# For time-window calculations in incremental syncs
from datetime import datetime, timezone, timedelta

# For making paginated PI Web API requests
import requests

# For enabling logs in the connector
from fivetran_connector_sdk import Logging as log

# For supporting data operations like upsert() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Local helpers for API pagination and record extraction
from client import paginate, PiApiError
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


def _handle_window_failure(table_name: str, window: timedelta, exc: Exception) -> timedelta:
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


def _sync_incremental_windows(
    table_name: str,
    cursor_key: str,
    start: datetime,
    state: dict,
    fetch_window,
) -> int:
    """
    Drive the adaptive time-window loop shared by sync_event_frames and sync_recorded_values.

    Iterates 30-day windows from *start* to now, calling fetch_window(start, end) for each
    slice. Halves the window on transient errors; raises RuntimeError if it drops below
    __MIN_WINDOW_HOURS. Checkpoints state after each successful window.

    Args:
        table_name: table name used in log messages and _handle_window_failure.
        cursor_key: key in state["cursors"] to advance after each window.
        start: beginning of the first time window.
        state: connector state dict (cursors updated in place).
        fetch_window: callable(start, end) -> int — performs the upserts for one
            time window and returns the number of rows written.
    Returns:
        Total rows written across all windows.
    """
    cursors = state.setdefault("cursors", {})
    now = datetime.now(timezone.utc)
    window = timedelta(days=__INITIAL_WINDOW_DAYS)
    total = 0

    while start < now:
        end = min(start + window, now)
        try:
            window_count = fetch_window(start, end)
            cursors[cursor_key] = end.isoformat()
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(state)
            log.info(f"  {table_name} {fmt_ts(start)} → {fmt_ts(end)}: {window_count} rows")
            total += window_count
            start = end
        except PiApiError:
            # Auth / client errors are not transient — surface immediately
            raise
        except requests.exceptions.RequestException as exc:
            # Transient network or server error — halve the window and retry
            window = _handle_window_failure(table_name, window, exc)

    return total


def sync_elements(session: requests.Session, base: str, db_web_id: str, state: dict) -> None:
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
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="elements", data=extract_element(item))
        count += 1
        if count % __CHECKPOINT_INTERVAL == 0:
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(state)
            log.info(f"  elements: {count} rows synced")

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state)
    log.info(f"elements sync complete ({count} rows)")


def sync_attributes(
    session: requests.Session,
    base: str,
    db_web_id: str,
    state: dict,
    collect_pi_points: bool = True,
) -> list:
    """
    Full reimport of all PI AF element attributes in the database.

    Attempts the database-wide /elementattributes endpoint first (PI Web API 2019+).
    Falls back to iterating each element individually if that endpoint is unavailable (404/405).

    Returns the list of WebIds for PI Point attributes, which are used by
    sync_recorded_values to fetch time-series data.

    Args:
        session: an authenticated requests.Session.
        base: the PI Web API base URL.
        db_web_id: the WebId of the target AF database.
        state: the current connector state dict.
        collect_pi_points: when True, collect and return PI Point attribute WebIds
            for use by sync_recorded_values. Pass False when recorded_values sync is
            disabled to avoid building a large list on large PI deployments.
    Returns:
        List of WebId strings for PI Point attributes found in the database.
    """
    log.info("Syncing attributes (full reimport)")
    count = 0
    pi_point_web_ids = []

    def _process(item, element_web_id):
        """
        Upsert one attribute record and optionally collect PI Point WebIds.

        Args:
            item: raw attribute dict from PI Web API.
            element_web_id: WebId of the parent element, used as a foreign key.
        """
        nonlocal count
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="attributes", data=extract_attribute(item, element_web_id))
        count += 1
        if collect_pi_points and item.get("DataReferencePlugIn") == "PI Point":
            web_id = item.get("WebId", "")
            if web_id:
                pi_point_web_ids.append(web_id)
        if count % __CHECKPOINT_INTERVAL == 0:
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(state)
            log.info(f"  attributes: {count} rows synced")

    try:
        for item in paginate(
            session,
            f"{base}/assetdatabases/{db_web_id}/elementattributes",
            params={"searchFullHierarchy": "true", "maxCount": __MAX_COUNT},
        ):
            element_web_id = (
                item.get("Element", {}).get("WebId", "")
                if isinstance(item.get("Element"), dict)
                else ""
            )
            _process(item, element_web_id)
    except PiApiError as exc:
        # Surface auth failures immediately.
        if exc.status_code in (401, 403):
            raise
        # Only fall back on 404 (endpoint missing) or 405 (method not allowed) —
        # other client errors (400/422/etc.) are unrelated to endpoint availability.
        if exc.status_code not in (404, 405):
            raise
        log.info("  /elementattributes not available; falling back to per-element fetch")
        for elem_item in paginate(
            session,
            f"{base}/assetdatabases/{db_web_id}/elements",
            params={"searchFullHierarchy": "true", "maxCount": __MAX_COUNT},
        ):
            elem_web_id = elem_item.get("WebId", "")
            if not elem_web_id:
                log.warning("  Skipping element with missing WebId in fallback fetch")
                continue
            try:
                for attr_item in paginate(
                    session,
                    f"{base}/elements/{elem_web_id}/attributes",
                    params={"maxCount": __MAX_COUNT},
                ):
                    _process(attr_item, elem_web_id)
            except PiApiError as exc:
                # Surface 401 immediately — session-level auth failure, not per-element
                if exc.status_code == 401:
                    raise
                log.warning(f"  Skipping attributes for element {elem_web_id}: {exc}")

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state)
    log.info(
        f"attributes sync complete ({count} rows, " f"{len(pi_point_web_ids)} PI Point attributes)"
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
    start_str = cursors.get("event_frames")
    parsed_cursor = parse_pi_timestamp(start_str) if start_str else None
    if start_str and parsed_cursor is None:
        log.warning(f"  Malformed event_frames cursor '{start_str}'; falling back to start_date.")
    _fallback = parse_pi_timestamp(start_date) or datetime.fromtimestamp(0, tz=timezone.utc)
    start = parsed_cursor if parsed_cursor is not None else _fallback
    log.info(f"Syncing event_frames (incremental from {fmt_ts(start)})")

    def fetch_window(start, end):
        """
        Fetch and upsert all event frames whose start_time falls in [start, end).

        Args:
            start: beginning of the time window (UTC-aware datetime).
            end: end of the time window (UTC-aware datetime).
        Returns:
            Number of event frame rows upserted.
        """
        count = 0
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
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table="event_frames", data=extract_event_frame(item, db_web_id))
            count += 1
        return count

    total = _sync_incremental_windows("event_frames", "event_frames", start, state, fetch_window)
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

    Individual attribute streams that return a 403 (no access) or 404 (deleted PI Point)
    are skipped with a warning so one bad attribute does not fail the entire sync.

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
    # Derive is_first_sync from whether the stored cursor parses successfully, not just
    # whether it exists. An unparseable cursor value is treated as absent so the
    # late-arrival rollback is not applied on what is effectively a first sync.
    parsed_cursor = parse_pi_timestamp(start_str) if start_str else None
    is_first_sync = parsed_cursor is None
    _fallback = parse_pi_timestamp(start_date) or datetime.fromtimestamp(0, tz=timezone.utc)
    start = parsed_cursor if parsed_cursor is not None else _fallback

    if not is_first_sync:
        start = start - timedelta(hours=__LATE_ARRIVAL_ROLLBACK_HOURS)
        log.info(f"  recorded_values: late-arrival rollback applied → {fmt_ts(start)}")

    log.info(
        f"Syncing recorded_values (incremental from {fmt_ts(start)}, "
        f"{len(pi_point_web_ids)} PI Point attributes)"
    )

    def fetch_window(start, end):
        """
        Fetch and upsert recorded values for all PI Point attributes in [start, end).

        Args:
            start: beginning of the time window (UTC-aware datetime).
            end: end of the time window (UTC-aware datetime).
        Returns:
            Total number of recorded value rows upserted across all PI Point attributes.
        """
        count = 0
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
                    # Skip items with a missing or empty Timestamp to avoid generating
                    # a degenerate _fivetran_id (attr_web_id|"") that causes primary-key
                    # collisions across different attributes missing timestamps.
                    if not item.get("Timestamp"):
                        log.warning(
                            f"  Skipping recorded value with missing Timestamp for {attr_web_id}"
                        )
                        continue
                    # The 'upsert' operation is used to insert or update data in the destination table.
                    # The first argument is the name of the destination table.
                    # The second argument is a dictionary containing the record to be upserted.
                    op.upsert(
                        table="recorded_values",
                        data=extract_recorded_value(item, attr_web_id),
                    )
                    count += 1
                    if count % __CHECKPOINT_INTERVAL == 0:
                        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                        # from the correct position in case of next sync or interruptions.
                        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
                        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
                        # Learn more about how and where to checkpoint by reading our best practices documentation
                        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
                        op.checkpoint(state)
                        log.info(f"  recorded_values: {count} rows synced")
            except PiApiError as exc:
                # Surface 401 immediately — session-level auth failure affects all streams
                if exc.status_code == 401:
                    raise
                # Only skip expected per-stream conditions:
                # 403 = no access to this stream; 404 = PI Point deleted
                if exc.status_code not in (403, 404):
                    raise
                log.warning(f"  Skipping stream {attr_web_id}: {exc}")
        return count

    total = _sync_incremental_windows(
        "recorded_values", "recorded_values", start, state, fetch_window
    )
    log.info(f"recorded_values sync complete ({total} rows)")
