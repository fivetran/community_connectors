"""This connector syncs earthquake events from the USGS FDSN Event Web Service, including updates and deletes.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For adding jitter to retry delays
import random

# For waiting between retries
import time

# For converting between epoch milliseconds, API query times and UTC datetimes
from datetime import datetime, timedelta, timezone

# For making HTTP requests to the USGS event service
import requests

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# The FDSN event query endpoint. It needs no authentication.
__BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

# Identifies this example in the USGS server logs.
__USER_AGENT = "fivetran-connector-sdk-usgs-earthquake-example"

__TABLE_NAME = "earthquake"

# Events per request. The API accepts up to 20000; 5000 keeps a response to a few megabytes.
__PAGE_SIZE = 5000

# The historical sync walks event time in windows of this length, which bounds the server-side sort.
__BACKFILL_WINDOW_MS = int(timedelta(days=30).total_seconds() * 1000)

# The next sync reads events updated after this sync's first server time, minus this overlap. It absorbs
# the whole-second precision of the server time and responses served from the 60-second CDN cache.
__CURSOR_OVERLAP_MS = int(timedelta(minutes=10).total_seconds() * 1000)

# Longer than the CDN's 60-second edge timeout, so a slow query gets the CDN's HTTP 504, which is retried.
__REQUEST_TIMEOUT_SECONDS = 90

# Attempts for transient failures, with exponential backoff and up to a second of jitter. A Retry-After
# header can lengthen a wait, up to __MAX_DELAY_SECONDS.
__MAX_RETRIES = 5
__BASE_DELAY_SECONDS = 2
__MAX_JITTER_SECONDS = 1
__MAX_DELAY_SECONDS = 60
__RETRYABLE_STATUS_CODES = (429, 500, 502, 503, 504)

# How much of an error response body goes into the error message.
__ERROR_BODY_PREVIEW_CHARS = 500

# The two passes of an incremental sync.
__UPSERT_PASS = "upsert"
__DELETE_PASS = "delete"

# Every state key. resolve_state gives each one a value, None when unset, so the sync reads any key directly.
# config_fingerprint holds the normalized start_date the rest of the state belongs to.
__STATE_KEYS = (
    "config_fingerprint",
    "updated_cursor",
    "pending_cursor",
    "backfill_end",
    "backfill_next_start",
    "pass",
    "pass_next_start",
)

# The UTC time format the API accepts in starttime, endtime and updatedafter.
__QUERY_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

# strftime's %f gives microseconds; the API takes milliseconds, so the last three digits are dropped.
__MICROSECOND_DIGITS_TO_DROP = 3

# The start_date format.
__START_DATE_FORMAT = "%Y-%m-%d"

# A validation floor for start_date, not the catalog's first event. strftime's %Y does not zero-pad
# years below 1000 on every platform, so stored query times from such years would not parse back.
__MIN_START_DATE = datetime(1900, 1, 1, tzinfo=timezone.utc)

# The epoch as an aware datetime, for exact millisecond conversions, including before 1970.
__EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

# Both incremental passes read event times from here, not from start_date. USGS can revise an event's
# origin time, and an event revised to before start_date must still be read so its row can be deleted.
# The catalog holds no event before this date, and it is the earliest one format_query_time can write
# and parse back (see __MIN_START_DATE).
__INCREMENTAL_FLOOR_MS = (datetime(1000, 1, 1, tzinfo=timezone.utc) - __EPOCH) // timedelta(
    milliseconds=1
)

# GeoJSON coordinates are [longitude, latitude, depth].
__COORDINATE_COUNT = 3


def validate_configuration(configuration: dict) -> int:
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        start_date as midnight UTC, in epoch milliseconds.
    Raises:
        ValueError: if start_date is missing, a placeholder, not a YYYY-MM-DD date, before 1900 or in the future.
    """
    return parse_start_date(configuration.get("start_date"))


def parse_start_date(value) -> int:
    """
    Parse the start_date configuration value.
    Args:
        value: the raw start_date value.
    Returns:
        Midnight UTC on that date, in epoch milliseconds.
    Raises:
        ValueError: if the value is missing, a placeholder, not a YYYY-MM-DD date, before 1900 or in the future.
    """
    text = value.strip() if isinstance(value, str) else ""
    if not text or (text.startswith("<") and text.endswith(">")):
        raise ValueError("Missing required configuration value: start_date (YYYY-MM-DD).")
    try:
        start = datetime.strptime(text, __START_DATE_FORMAT).replace(tzinfo=timezone.utc)
    except ValueError:
        raise ValueError(f"Invalid start_date {text!r}: expected YYYY-MM-DD.") from None
    if start < __MIN_START_DATE:
        raise ValueError(
            f"Invalid start_date {text!r}: the earliest supported date is 1900-01-01."
        )
    if start > datetime.now(timezone.utc):
        raise ValueError(f"Invalid start_date {text!r}: the date is in the future.")
    return (start - __EPOCH) // timedelta(milliseconds=1)


def format_start_date(ms: int) -> str:
    """
    Format start_date in its normalized YYYY-MM-DD form, so 2026-9-1 and 2026-09-01 match.
    Args:
        ms: start_date in epoch milliseconds.
    Returns:
        The date as YYYY-MM-DD.
    """
    return (__EPOCH + timedelta(milliseconds=ms)).strftime(__START_DATE_FORMAT)


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
            "table": __TABLE_NAME,
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "event_time": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "mag": "DOUBLE",
                "mag_type": "STRING",
                "place": "STRING",
                "event_type": "STRING",
                "review_status": "STRING",
                "longitude": "DOUBLE",
                "latitude": "DOUBLE",
                "depth_km": "DOUBLE",
                "felt": "INT",
                "cdi": "DOUBLE",
                "mmi": "DOUBLE",
                "alert": "STRING",
                "tsunami": "INT",
                "sig": "INT",
                "network": "STRING",
                "network_event_code": "STRING",
                "ids": "STRING",
                "sources": "STRING",
                "types": "STRING",
                "nst": "INT",
                "dmin": "DOUBLE",
                "rms": "DOUBLE",
                "gap": "DOUBLE",
                "title": "STRING",
                "url": "STRING",
                "detail": "STRING",
            },
        }
    ]


def format_query_time(ms: int) -> str:
    """
    Format epoch milliseconds as a UTC query time with millisecond precision.
    Args:
        ms: epoch milliseconds.
    Returns:
        A string such as 2026-09-30T12:34:56.789.
    """
    formatted = (__EPOCH + timedelta(milliseconds=ms)).strftime(__QUERY_TIME_FORMAT)
    return formatted[:-__MICROSECOND_DIGITS_TO_DROP]


def parse_query_time(value):
    """
    Parse a stored query time back to epoch milliseconds.
    Args:
        value: a string written by format_query_time, or None.
    Returns:
        Epoch milliseconds, or None when the value is None.
    """
    if value is None:
        return None
    parsed = datetime.strptime(value, __QUERY_TIME_FORMAT).replace(tzinfo=timezone.utc)
    return (parsed - __EPOCH) // timedelta(milliseconds=1)


def to_utc_datetime(value):
    """
    Convert an epoch-millisecond API time to an aware UTC datetime.
    Args:
        value: the raw value.
    Returns:
        The datetime, or None when the value is missing or not an integer.
    """
    return __EPOCH + timedelta(milliseconds=value) if isinstance(value, int) else None


def as_float(value):
    """
    Return a numeric value as a float, so a whole number such as a magnitude of 5 loads as DOUBLE.
    Args:
        value: the raw value.
    Returns:
        The float, or None when the value is missing, a boolean or not a number.
    """
    if isinstance(value, bool):
        return None
    return float(value) if isinstance(value, (int, float)) else None


def as_int(value):
    """
    Return a whole-number value as an int, so an INT column never receives a float or a string.
    Args:
        value: the raw value.
    Returns:
        The int, or None when the value is missing, a boolean or not a whole number.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def strip_list(value):
    """
    Remove the wrapping commas from a list field such as ",us7000abcd,at00xyz,".
    Args:
        value: the raw string, or None.
    Returns:
        The inner comma-separated list, or None when the field is missing or empty.
    """
    return (value.strip(",") or None) if isinstance(value, str) else None


def build_event_row(feature: dict) -> dict:
    """
    Map one GeoJSON event to an earthquake row, renaming time, updated, status, net, code and type,
    which are reserved or ambiguous in SQL.
    Args:
        feature: one element of the response's features array.
    Returns:
        A dictionary whose keys match the earthquake table columns.
    """
    properties = feature.get("properties") or {}
    coordinates = (feature.get("geometry") or {}).get("coordinates")
    # Pad, so a missing or short coordinate array gives nulls instead of an IndexError.
    coordinates = (coordinates if isinstance(coordinates, list) else []) + [
        None
    ] * __COORDINATE_COUNT
    return {
        "id": feature.get("id"),
        "event_time": to_utc_datetime(properties.get("time")),
        "updated_at": to_utc_datetime(properties.get("updated")),
        "mag": as_float(properties.get("mag")),
        "mag_type": properties.get("magType"),
        "place": properties.get("place"),
        "event_type": properties.get("type"),
        "review_status": properties.get("status"),
        "longitude": as_float(coordinates[0]),
        "latitude": as_float(coordinates[1]),
        "depth_km": as_float(coordinates[2]),
        "felt": as_int(properties.get("felt")),
        "cdi": as_float(properties.get("cdi")),
        "mmi": as_float(properties.get("mmi")),
        "alert": properties.get("alert"),
        "tsunami": as_int(properties.get("tsunami")),
        "sig": as_int(properties.get("sig")),
        "network": properties.get("net"),
        "network_event_code": properties.get("code"),
        "ids": strip_list(properties.get("ids")),
        "sources": strip_list(properties.get("sources")),
        "types": strip_list(properties.get("types")),
        "nst": as_int(properties.get("nst")),
        "dmin": as_float(properties.get("dmin")),
        "rms": as_float(properties.get("rms")),
        "gap": as_float(properties.get("gap")),
        "title": properties.get("title"),
        "url": properties.get("url"),
        "detail": properties.get("detail"),
    }


def event_ids(feature: dict) -> list:
    """
    Return every id of an event: the preferred id first, then the other ids in its ids field.
    Args:
        feature: one element of the response's features array.
    Returns:
        A list of distinct, non-empty ids.
    """
    listed = strip_list((feature.get("properties") or {}).get("ids")) or ""
    candidates = [feature.get("id")] + listed.split(",")
    return list(dict.fromkeys(event_id for event_id in candidates if event_id))


def retry_delay(attempt: int, retry_after) -> float:
    """
    Compute the wait before the next attempt: exponential backoff with jitter, or Retry-After if longer.
    Both are capped at __MAX_DELAY_SECONDS. A Retry-After given as an HTTP date is ignored.
    Args:
        attempt: the attempt that just failed, starting at 1.
        retry_after: the response's Retry-After header in seconds, or None.
    Returns:
        The number of seconds to wait.
    """
    delay = min(__BASE_DELAY_SECONDS * 2 ** (attempt - 1), __MAX_DELAY_SECONDS)
    delay += random.uniform(0, __MAX_JITTER_SECONDS)
    if retry_after and retry_after.strip().isdigit():
        delay = max(delay, min(float(retry_after), __MAX_DELAY_SECONDS))
    return delay


def fetch_page(session: requests.Session, params: dict):
    """
    Request one page of events, oldest first. HTTP 429, 500, 502, 503 and 504, connection errors,
    timeouts, truncated bodies and invalid JSON are retried with exponential backoff; any other HTTP
    error fails at once. An empty result is HTTP 200 with an empty features array.
    Args:
        session: the shared HTTP session.
        params: this page's filters (starttime, endtime or updatedafter, and includedeleted).
    Returns:
        A tuple (features, generated): the page's GeoJSON events and the server time in epoch ms.
    Raises:
        RuntimeError: on a non-retryable error, a missing server time, or when every attempt failed.
    """
    # Only documented FDSN parameters are sent: the service rejects unknown ones with HTTP 400.
    query = {"format": "geojson", "orderby": "time-asc", "limit": __PAGE_SIZE, **params}
    for attempt in range(1, __MAX_RETRIES + 1):
        retry_after = None
        try:
            response = session.get(__BASE_URL, params=query, timeout=__REQUEST_TIMEOUT_SECONDS)
            if response.status_code == 200:
                body = response.json()
                generated = (body.get("metadata") or {}).get("generated")
                if not isinstance(generated, int):
                    raise RuntimeError("The USGS response has no metadata.generated server time")
                return body.get("features") or [], generated
            if response.status_code not in __RETRYABLE_STATUS_CODES:
                raise RuntimeError(
                    f"USGS returned HTTP {response.status_code}: "
                    f"{response.text[:__ERROR_BODY_PREVIEW_CHARS]}"
                )
            failure = f"HTTP {response.status_code}"
            retry_after = response.headers.get("Retry-After")
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ) as error:
            failure = f"{type(error).__name__}: {error}"
        except requests.exceptions.JSONDecodeError as error:
            failure = f"invalid JSON: {error}"
        if attempt < __MAX_RETRIES:
            delay = retry_delay(attempt, retry_after)
            log.warning(f"USGS request failed ({failure}); retrying in {delay:.1f}s")
            time.sleep(delay)
    raise RuntimeError(f"USGS request failed after {__MAX_RETRIES} attempts: {failure}")


def next_keyset_position(features: list, position: int) -> int:
    """
    Return the next page's starttime after a full page: the time of that page's last event.
    Keyset paging filters on a value rather than an offset, so rows entering or leaving the result
    between requests cannot shift later rows out of view. Events at exactly that millisecond are read
    again on the next page, and the upsert makes that harmless.
    Args:
        features: the full page just processed.
        position: the starttime this page was requested with, in epoch ms.
    Returns:
        The next starttime in epoch ms.
    Raises:
        RuntimeError: if more than a page of events share one millisecond, so paging cannot advance.
    """
    last_time = (features[-1].get("properties") or {}).get("time")
    if not isinstance(last_time, int) or last_time <= position:
        raise RuntimeError(
            f"More than {__PAGE_SIZE} events start at {format_query_time(position)}, "
            "so paging on event time cannot advance without skipping events"
        )
    return last_time


def is_before_start(feature: dict, start_ms: int) -> bool:
    """
    Report whether an event's origin time is before start_date, which only a revision can cause
    for an event the connector has already synced.
    Args:
        feature: one element of the response's features array.
        start_ms: start_date in epoch ms.
    Returns:
        True when the event time is an integer before start_ms.
    """
    event_time = (feature.get("properties") or {}).get("time")
    return isinstance(event_time, int) and event_time < start_ms


def upsert_events(features: list, start_ms: int) -> int:
    """
    Upsert each event, then delete the rows of its non-preferred ids. USGS can change which id is
    preferred, and this removes the row left under the old one. Deleting an absent key does nothing.
    An event whose origin time USGS has revised to before start_date has left the synced range, so
    the rows of all its ids are deleted instead of upserted.
    Args:
        features: GeoJSON events from the historical sync or the upsert pass.
        start_ms: start_date in epoch ms.
    Returns:
        The number of events processed: upserted, or deleted for being before start_date.
    """
    upserted, skipped, out_of_range = 0, 0, 0
    for feature in features:
        if not feature.get("id"):
            skipped += 1
            continue
        if is_before_start(feature, start_ms):
            for event_id in event_ids(feature):
                # The 'delete' operation marks a row as deleted in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the primary key of the row.
                op.delete(table=__TABLE_NAME, keys={"id": event_id})
            out_of_range += 1
            continue
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table=__TABLE_NAME, data=build_event_row(feature))
        # event_ids lists the preferred id first, so the rest are the non-preferred ids.
        for other_id in event_ids(feature)[1:]:
            # The 'delete' operation marks a row as deleted in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the primary key of the row.
            op.delete(table=__TABLE_NAME, keys={"id": other_id})
        upserted += 1
    if out_of_range:
        log.info(
            f"Deleted the ids of {out_of_range} updated events from before start_date, which removes "
            "any row synced before a revision moved the event out of range"
        )
    if skipped:
        # The 'warning' operation logs the message and shows it in the Fivetran dashboard without failing the sync.
        op.warning(f"Skipped {skipped} events that have no id")
    return upserted + out_of_range


def delete_events(features: list) -> int:
    """
    Delete the row of each event USGS has deleted, such as a duplicate or a false detection.
    Only the deleted event's own id is deleted. Its ids field is not used, so a merge can never
    remove the row of the event that survives it; the upsert pass already removes non-preferred ids.
    The destination sets _fivetran_deleted and keeps the last good values, instead of overwriting
    them with the mostly null fields of a deleted event.
    Args:
        features: GeoJSON events from the delete pass, each with status "deleted".
    Returns:
        The number of events deleted.
    """
    deleted = 0
    for feature in features:
        if not feature.get("id"):
            continue
        # The 'delete' operation marks a row as deleted in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the primary key of the row.
        op.delete(table=__TABLE_NAME, keys={"id": feature["id"]})
        deleted += 1
    return deleted


def save_state(state: dict):
    """
    Checkpoint the state dictionary, which always holds every state key.
    Args:
        state: the sync state.
    """
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state=state)


def resolve_state(state: dict, fingerprint: str):
    """
    Prepare the state in place: reset it when start_date has changed, then make sure every key exists.
    Args:
        state: the state from the previous sync, empty on the first sync.
        fingerprint: the start_date the stored state must belong to.
    """
    if state.get("config_fingerprint") != fingerprint:
        if state:
            log.warning("start_date changed since the last sync, so the historical sync restarts")
        state.clear()
    for key in __STATE_KEYS:
        state.setdefault(key, None)
    state["config_fingerprint"] = fingerprint


def run_backfill(session: requests.Session, state: dict, start_ms: int):
    """
    Historical sync: upsert every event from start_date to the server time of its first response.
    It has no delete pass. An event deleted before the backfill never lands, and one deleted during
    it is updated after pending_cursor, so the first incremental delete pass removes it.
    Args:
        session: the shared HTTP session.
        state: the sync state, checkpointed after every page.
        start_ms: start_date in epoch ms.
    """
    stored_position = parse_query_time(state["backfill_next_start"])
    position = start_ms if stored_position is None else stored_position
    backfill_end = parse_query_time(state["backfill_end"])
    window_start, window_events, total = position, 0, 0
    while backfill_end is None or position < backfill_end:
        window_end = window_start + __BACKFILL_WINDOW_MS
        if backfill_end is not None:
            window_end = min(window_end, backfill_end)
        params = {
            "starttime": format_query_time(position),
            "endtime": format_query_time(window_end),
        }
        features, generated = fetch_page(session, params)
        if backfill_end is None:
            # The first server time fixes where the backfill stops and, less the overlap, where the
            # first incremental sync starts. Both are kept until the backfill completes.
            backfill_end = generated
            state["backfill_end"] = format_query_time(generated)
            state["pending_cursor"] = format_query_time(generated - __CURSOR_OVERLAP_MS)
            window_end = min(window_end, backfill_end)
        window_events += upsert_events(features, start_ms)
        # A short page ends the window; a full page continues it from its last event's time.
        if len(features) < __PAGE_SIZE:
            if window_events:
                log.info(f"Backfill to {format_query_time(window_end)}: {window_events} events")
            total += window_events
            position = window_start = window_end
            window_events = 0
        else:
            position = next_keyset_position(features, position)
        state["backfill_next_start"] = format_query_time(position)
        save_state(state)

    state["updated_cursor"] = state["pending_cursor"]
    state["pending_cursor"] = state["backfill_end"] = state["backfill_next_start"] = None
    save_state(state)
    log.info(
        f"Historical sync complete: {total} events in this run, up to {state['updated_cursor']}"
    )


def run_pass(session: requests.Session, state: dict, start_ms: int, pass_name: str):
    """
    Page through the events updated after updated_cursor, oldest first, upserting or deleting each.
    Every query sends starttime: without it, the API silently limits updatedafter to events from the
    last 30 days, while an event from any year can be updated today. The starttime is
    __INCREMENTAL_FLOOR_MS, not start_date, so an event whose origin time USGS revised to before
    start_date is still read, and upsert_events deletes its row instead of leaving it live.
    Args:
        session: the shared HTTP session.
        state: the sync state, checkpointed after every full page.
        start_ms: start_date in epoch ms.
        pass_name: __UPSERT_PASS or __DELETE_PASS.
    """
    stored_position = parse_query_time(state["pass_next_start"])
    position = __INCREMENTAL_FLOOR_MS if stored_position is None else stored_position
    processed = 0
    while True:
        params = {
            "updatedafter": state["updated_cursor"],
            "starttime": format_query_time(position),
        }
        if pass_name == __DELETE_PASS:
            # includedeleted=only returns just the deleted events. includedeleted=true would return
            # both kinds in one query, but it times out at the CDN on anything but a short span.
            params["includedeleted"] = "only"
        features, generated = fetch_page(session, params)
        if state["pending_cursor"] is None:
            # The next sync reads events updated after this server time, less the overlap, so an
            # event updated while this sync runs is read again next time rather than missed.
            state["pending_cursor"] = format_query_time(generated - __CURSOR_OVERLAP_MS)
        if pass_name == __DELETE_PASS:
            processed += delete_events(features)
        else:
            processed += upsert_events(features, start_ms)
        if len(features) < __PAGE_SIZE:
            flags = ", includedeleted=only" if pass_name == __DELETE_PASS else ""
            log.info(
                f"The {pass_name} pass read {processed} events (updatedafter={state['updated_cursor']}, "
                f"starttime={format_query_time(__INCREMENTAL_FLOOR_MS)}{flags})"
            )
            return
        position = next_keyset_position(features, position)
        state["pass_next_start"] = format_query_time(position)
        save_state(state)


def run_incremental(session: requests.Session, state: dict, start_ms: int):
    """
    Incremental sync: an upsert pass, then a delete pass, over events updated after updated_cursor.
    A failed sync resumes the pass it was in, from the stored position, with the same pending_cursor.
    Args:
        session: the shared HTTP session.
        state: the sync state.
        start_ms: start_date in epoch ms.
    """
    if state["pass"] != __DELETE_PASS:
        state["pass"] = __UPSERT_PASS
        run_pass(session, state, start_ms, __UPSERT_PASS)
        state["pass"], state["pass_next_start"] = __DELETE_PASS, None
        save_state(state)
    run_pass(session, state, start_ms, __DELETE_PASS)

    state["updated_cursor"] = state["pending_cursor"]
    state["pending_cursor"] = state["pass"] = state["pass_next_start"] = None
    save_state(state)


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
    log.warning("Example: Source Examples : USGS Earthquake")

    start_ms = validate_configuration(configuration=configuration)
    resolve_state(state, format_start_date(start_ms))

    # One session for the whole sync, so every page reuses pooled connections.
    session = requests.Session()
    session.headers.update({"User-Agent": __USER_AGENT, "Accept": "application/json"})
    try:
        if state["updated_cursor"] is None:
            run_backfill(session, state, start_ms)
        else:
            run_incremental(session, state, start_ms)
    finally:
        session.close()


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
