"""AVEVA PI Connector for Fivetran Connector SDK.

Syncs data from AVEVA PI (formerly OSIsoft PI) via the PI Web API REST interface.
No proprietary drivers required — the connector communicates over HTTPS using
standard Basic authentication.

Tables synced:
  - elements        : PI AF elements (full reimport each sync)
  - attributes      : PI AF element attributes (full reimport each sync)
  - event_frames    : PI AF event frames (cursor-based incremental by start_time)
  - recorded_values : PI archive / recorded data (cursor-based incremental, opt-in via
                      sync_recorded_values = "true" in configuration)

Prerequisites:
  - PI Web API 2019 SP1 or later, reachable over HTTPS from the connector host.
  - Basic authentication enabled on the PI Web API server.
  - The PI user must have read access to the configured AF database.

See the Technical Reference: https://fivetran.com/docs/connectors/connector-sdk/technical-reference
See Best Practices:          https://fivetran.com/docs/connectors/connector-sdk/best-practices
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone, timedelta

import requests
from requests.auth import HTTPBasicAuth

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_NAME = "AVEVA PI Web API"

MAX_RETRIES = 3
MAX_COUNT = 1000              # PI Web API maximum items per response page
INITIAL_WINDOW_DAYS = 30      # Starting size of the incremental time window (days)
MIN_WINDOW_HOURS = 1          # If the adaptive window shrinks below this, surface the error
LATE_ARRIVAL_ROLLBACK_HOURS = 2   # Roll back cursor by this many hours on subsequent syncs
CHECKPOINT_INTERVAL = 10_000  # Emit a checkpoint every N rows during full reimport

EPOCH_ISO = "1970-01-01T00:00:00Z"


# ---------------------------------------------------------------------------
# Configuration validation
# ---------------------------------------------------------------------------

def validate_configuration(configuration: dict) -> None:
    """Raise ValueError if any required configuration key is missing or blank."""
    for key in ("base_url", "username", "password"):
        if not configuration.get(key):
            raise ValueError(f"Missing or empty required configuration key: '{key}'")


# ---------------------------------------------------------------------------
# HTTP session and request helpers
# ---------------------------------------------------------------------------

def _build_session(configuration: dict) -> requests.Session:
    """Create an authenticated requests.Session for PI Web API calls."""
    session = requests.Session()
    session.auth = HTTPBasicAuth(configuration["username"], configuration["password"])
    session.headers.update({
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    })
    # Allow users to disable TLS verification for self-signed PI Web API certificates
    session.verify = configuration.get("verify_ssl", "true").lower() != "false"
    return session


def _base_url(configuration: dict) -> str:
    return configuration["base_url"].rstrip("/")


def _api_get(session: requests.Session, url: str, params: dict | None = None) -> dict:
    """
    GET a PI Web API endpoint and return the parsed JSON body.

    Raises ValueError immediately on 4xx responses (auth failures, not-found, etc.) —
    these are not worth retrying. Retries up to MAX_RETRIES times on 5xx or network errors.
    """
    last_exc: Exception = RuntimeError("No request attempted")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code in (401, 403):
                raise ValueError(
                    f"Authentication error ({resp.status_code}) for {url}: {resp.text[:200]}"
                )
            if 400 <= resp.status_code < 500:
                raise ValueError(
                    f"Client error ({resp.status_code}) for {url}: {resp.text[:200]}"
                )
            resp.raise_for_status()
            return resp.json()
        except ValueError:
            raise
        except Exception as exc:
            last_exc = exc
            log.warning(f"Request attempt {attempt}/{MAX_RETRIES} failed for {url}: {exc}")

    raise ConnectionError(
        f"Could not reach {SOURCE_NAME} after {MAX_RETRIES} attempts. URL: {url}"
    ) from last_exc


def _paginate(
    session: requests.Session, url: str, params: dict | None = None
):
    """
    Yield every item from a paginated PI Web API response.

    PI Web API paginates via a 'Links.Next' URL embedded in the response body.
    """
    next_url: str | None = url
    next_params: dict | None = params
    while next_url:
        body = _api_get(session, next_url, next_params)
        for item in body.get("Items", []):
            yield item
        next_url = body.get("Links", {}).get("Next")
        next_params = None  # Parameters are already encoded in the Next URL


# ---------------------------------------------------------------------------
# Database discovery
# ---------------------------------------------------------------------------

def get_database_web_id(
    session: requests.Session, base_url: str, database_name: str | None
) -> str:
    """
    Find the WebId of the target AF database.

    Searches all asset servers visible to this PI Web API instance. If database_name
    is provided, returns the first database with that exact name. If omitted, returns
    the first database found across any server.

    Raises ValueError if the target database cannot be found.
    """
    servers = _api_get(session, f"{base_url}/assetservers").get("Items", [])
    if not servers:
        raise ValueError("No PI Asset Servers found via PI Web API. Check the base_url.")

    for server in servers:
        server_web_id = server["WebId"]
        databases = _api_get(
            session, f"{base_url}/assetservers/{server_web_id}/assetdatabases"
        ).get("Items", [])
        for db in databases:
            if database_name is None or db.get("Name") == database_name:
                log.info(
                    f"Connected to database '{db['Name']}' on server '{server.get('Name')}'"
                )
                return db["WebId"]

    target = f"'{database_name}'" if database_name else "any database"
    raise ValueError(f"Could not find {target} on any PI Asset Server. Check database_name.")


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def _parse_pi_timestamp(ts: str | None) -> datetime | None:
    """Parse a PI Web API ISO 8601 timestamp string to a UTC-aware datetime."""
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


def _fmt_ts(dt: datetime) -> str:
    """Format a datetime as a PI Web API-compatible UTC timestamp string."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Record extraction
# ---------------------------------------------------------------------------

def _category_names(item: dict) -> str:
    """Serialize the CategoryNames list (if present) to a JSON array string."""
    return json.dumps(item.get("CategoryNames") or [])


def _extract_element(item: dict) -> dict:
    return {
        "web_id":          item.get("WebId", ""),
        "name":            item.get("Name", ""),
        "description":     item.get("Description", ""),
        "path":            item.get("Path", ""),
        "template_name":   item.get("TemplateName", ""),
        "category_names":  _category_names(item),
    }


def _extract_attribute(item: dict, element_web_id: str) -> dict:
    return {
        "web_id":              item.get("WebId", ""),
        "element_web_id":      element_web_id,
        "name":                item.get("Name", ""),
        "description":         item.get("Description", ""),
        "path":                item.get("Path", ""),
        "type":                item.get("Type", ""),
        "type_qualifier":      item.get("TypeQualifier", ""),
        "data_reference":      item.get("DataReferencePlugIn", ""),
        "data_reference_path": item.get("ConfigString", ""),
        "category_names":      _category_names(item),
    }


def _extract_event_frame(item: dict, db_web_id: str) -> dict:
    return {
        "web_id":          item.get("WebId", ""),
        "name":            item.get("Name", ""),
        "description":     item.get("Description", ""),
        "start_time":      _parse_pi_timestamp(item.get("StartTime")),
        "end_time":        _parse_pi_timestamp(item.get("EndTime")),
        "template_name":   item.get("TemplateName", ""),
        "category_names":  _category_names(item),
        "database_web_id": db_web_id,
    }


def _generate_recorded_value_id(attr_web_id: str, timestamp: str) -> str:
    """Deterministic MD5 primary key for a recorded value (attribute WebId + raw timestamp)."""
    return hashlib.md5(f"{attr_web_id}|{timestamp}".encode("utf-8")).hexdigest()


def _extract_recorded_value(item: dict, attr_web_id: str) -> dict:
    ts_str = item.get("Timestamp", "")
    value = item.get("Value")
    # PI value may be a system digital state dict, e.g. {"Name": "Shutdown", "Value": 248}
    if isinstance(value, dict):
        value = value.get("Name") or str(value)
    else:
        value = str(value) if value is not None else None

    return {
        "_fivetran_id":     _generate_recorded_value_id(attr_web_id, ts_str),
        "attribute_web_id": attr_web_id,
        "timestamp":        _parse_pi_timestamp(ts_str),
        "value":            value,
        "quality":          "questionable" if item.get("Questionable") else "good",
        "good":             not item.get("Questionable", False),
    }


# ---------------------------------------------------------------------------
# Sync strategies
# ---------------------------------------------------------------------------

def sync_elements(
    session: requests.Session, base_url: str, db_web_id: str, state: dict
) -> None:
    """Full reimport of all PI AF elements in the database (the asset hierarchy)."""
    log.info("Syncing elements (full reimport)")
    count = 0
    for item in _paginate(
        session,
        f"{base_url}/assetdatabases/{db_web_id}/elements",
        params={"searchFullHierarchy": "true", "maxCount": MAX_COUNT},
    ):
        op.upsert("elements", _extract_element(item))
        count += 1
        if count % CHECKPOINT_INTERVAL == 0:
            op.checkpoint(state)
            log.info(f"  elements: {count} rows synced")

    op.checkpoint(state)
    log.info(f"elements sync complete ({count} rows)")


def sync_attributes(
    session: requests.Session, base_url: str, db_web_id: str, state: dict
) -> list[str]:
    """
    Full reimport of all PI AF element attributes in the database.

    Attempts the database-wide /elementattributes endpoint first (PI Web API 2019+).
    Falls back to iterating each element individually if that endpoint is unavailable.

    Returns the WebIds of all PI Point attributes found (used by sync_recorded_values).
    """
    log.info("Syncing attributes (full reimport)")
    count = 0
    pi_point_web_ids: list[str] = []

    def _process_attr(item: dict, element_web_id: str) -> None:
        nonlocal count
        op.upsert("attributes", _extract_attribute(item, element_web_id))
        count += 1
        if item.get("DataReferencePlugIn") == "PI Point":
            pi_point_web_ids.append(item["WebId"])
        if count % CHECKPOINT_INTERVAL == 0:
            op.checkpoint(state)
            log.info(f"  attributes: {count} rows synced")

    try:
        for item in _paginate(
            session,
            f"{base_url}/assetdatabases/{db_web_id}/elementattributes",
            params={"searchFullHierarchy": "true", "maxCount": MAX_COUNT},
        ):
            # The Element link is an embedded object on newer PI Web API versions
            element_web_id = (
                item["Element"]["WebId"]
                if isinstance(item.get("Element"), dict)
                else ""
            )
            _process_attr(item, element_web_id)
    except ValueError:
        # /elementattributes not available on this PI Web API version — iterate per element
        log.info("  /elementattributes not available; falling back to per-element fetch")
        for elem_item in _paginate(
            session,
            f"{base_url}/assetdatabases/{db_web_id}/elements",
            params={"searchFullHierarchy": "true", "maxCount": MAX_COUNT},
        ):
            elem_web_id = elem_item.get("WebId", "")
            try:
                for attr_item in _paginate(
                    session,
                    f"{base_url}/elements/{elem_web_id}/attributes",
                    params={"maxCount": MAX_COUNT},
                ):
                    _process_attr(attr_item, elem_web_id)
            except ValueError as exc:
                log.warning(f"  Skipping attributes for element {elem_web_id}: {exc}")

    op.checkpoint(state)
    log.info(
        f"attributes sync complete ({count} rows, "
        f"{len(pi_point_web_ids)} PI Point attributes)"
    )
    return pi_point_web_ids


def sync_event_frames(
    session: requests.Session, base_url: str, db_web_id: str, state: dict, start_date: str
) -> None:
    """
    Cursor-based incremental sync of PI AF event frames, keyed on start_time.

    Uses an adaptive time-window strategy: starts with INITIAL_WINDOW_DAYS-day windows
    and halves the window size on transient query failures. Raises if the window shrinks
    below MIN_WINDOW_HOURS and the query still fails.
    """
    cursors = state.setdefault("cursors", {})
    start_str = cursors.get("event_frames", start_date)
    start = _parse_pi_timestamp(start_str) or datetime.fromtimestamp(0, tz=timezone.utc)
    now = datetime.now(timezone.utc)
    window = timedelta(days=INITIAL_WINDOW_DAYS)
    total = 0

    log.info(f"Syncing event_frames (incremental from {_fmt_ts(start)})")

    while start < now:
        end = min(start + window, now)
        window_count = 0

        try:
            for item in _paginate(
                session,
                f"{base_url}/assetdatabases/{db_web_id}/eventframes",
                params={
                    "searchFullHierarchy": "true",
                    "startTime":           _fmt_ts(start),
                    "endTime":             _fmt_ts(end),
                    "maxCount":            MAX_COUNT,
                },
            ):
                op.upsert("event_frames", _extract_event_frame(item, db_web_id))
                window_count += 1

            cursors["event_frames"] = end.isoformat()
            op.checkpoint(state)
            log.info(
                f"  event_frames window {_fmt_ts(start)} → {_fmt_ts(end)}: {window_count} rows"
            )
            total += window_count
            start = end

        except ValueError:
            raise
        except Exception as exc:
            window = window / 2
            if window < timedelta(hours=MIN_WINDOW_HOURS):
                raise RuntimeError(
                    f"event_frames sync failed; window cannot be halved below "
                    f"{MIN_WINDOW_HOURS}h. Last error: {exc}"
                ) from exc
            log.warning(
                f"  event_frames window failed; halving to "
                f"{int(window.total_seconds() // 3600)}h. Error: {exc}"
            )

    log.info(f"event_frames sync complete ({total} rows)")


def sync_recorded_values(
    session: requests.Session,
    base_url: str,
    pi_point_web_ids: list[str],
    state: dict,
    start_date: str,
) -> None:
    """
    Cursor-based incremental sync of PI archive (recorded) values for all PI Point attributes.

    A single time cursor is shared across all attributes. On subsequent syncs the cursor
    is rolled back by LATE_ARRIVAL_ROLLBACK_HOURS to capture values that were written
    slightly after their timestamps.

    Individual attribute streams that return 4xx errors (e.g. deleted PI Points) are
    skipped with a warning rather than failing the whole sync.
    """
    if not pi_point_web_ids:
        log.info("No PI Point attributes found; skipping recorded_values sync")
        return

    cursors = state.setdefault("cursors", {})
    start_str = cursors.get("recorded_values")
    is_first_sync = start_str is None
    start = (
        _parse_pi_timestamp(start_str) or _parse_pi_timestamp(start_date)
        or datetime.fromtimestamp(0, tz=timezone.utc)
    )

    if not is_first_sync:
        start = start - timedelta(hours=LATE_ARRIVAL_ROLLBACK_HOURS)
        log.info(f"  recorded_values: late-arrival rollback applied → {_fmt_ts(start)}")

    now = datetime.now(timezone.utc)
    window = timedelta(days=INITIAL_WINDOW_DAYS)
    total = 0

    log.info(
        f"Syncing recorded_values (incremental from {_fmt_ts(start)}, "
        f"{len(pi_point_web_ids)} PI Point attributes)"
    )

    while start < now:
        end = min(start + window, now)
        window_count = 0

        try:
            for attr_web_id in pi_point_web_ids:
                try:
                    for item in _paginate(
                        session,
                        f"{base_url}/streams/{attr_web_id}/recorded",
                        params={
                            "startTime": _fmt_ts(start),
                            "endTime":   _fmt_ts(end),
                            "maxCount":  MAX_COUNT,
                        },
                    ):
                        op.upsert("recorded_values", _extract_recorded_value(item, attr_web_id))
                        window_count += 1
                except ValueError as exc:
                    # 404 = PI Point deleted; 403 = no read permission — skip this attribute
                    log.warning(f"  Skipping stream {attr_web_id}: {exc}")

            cursors["recorded_values"] = end.isoformat()
            op.checkpoint(state)
            log.info(
                f"  recorded_values window {_fmt_ts(start)} → {_fmt_ts(end)}: {window_count} rows"
            )
            total += window_count
            start = end

        except ValueError:
            raise
        except Exception as exc:
            window = window / 2
            if window < timedelta(hours=MIN_WINDOW_HOURS):
                raise RuntimeError(
                    f"recorded_values sync failed; window cannot be halved below "
                    f"{MIN_WINDOW_HOURS}h. Last error: {exc}"
                ) from exc
            log.warning(
                f"  recorded_values window failed; halving to "
                f"{int(window.total_seconds() // 3600)}h. Error: {exc}"
            )

    log.info(f"recorded_values sync complete ({total} rows)")


# ---------------------------------------------------------------------------
# Fivetran Connector SDK entry points
# ---------------------------------------------------------------------------

def schema(configuration: dict) -> list[dict]:
    """
    Return the static schema for the AVEVA PI Web API connector.

    The schema is fixed — no connection is required. The four tables map directly
    to the PI AF object types exposed by the PI Web API REST endpoints.
    The recorded_values table is always declared in the schema even when
    sync_recorded_values is "false"; Fivetran will simply receive no rows for it.
    """
    validate_configuration(configuration)
    return [
        {"table": "elements",       "primary_key": ["web_id"]},
        {"table": "attributes",     "primary_key": ["web_id"]},
        {"table": "event_frames",   "primary_key": ["web_id"]},
        {"table": "recorded_values","primary_key": ["_fivetran_id"]},
    ]


def update(configuration: dict, state: dict) -> None:
    """
    Main sync function. Called by Fivetran on every sync run.

    Sync order:
      1. elements        — full reimport (PI AF asset hierarchy)
      2. attributes      — full reimport; also collects PI Point attribute WebIds
      3. event_frames    — cursor-based incremental, keyed by start_time
      4. recorded_values — cursor-based incremental, opt-in via sync_recorded_values = "true"

    Configuration keys:
      base_url             : PI Web API base URL (e.g. https://piserver/piwebapi)
      username             : PI user account name
      password             : PI user password
      database_name        : (optional) target AF database name; defaults to first found
      verify_ssl           : (optional) "true" / "false" — verify TLS certificate
      start_date           : (optional) ISO 8601 start date for first incremental sync
      sync_recorded_values : (optional) "true" to also sync the recorded_values table
                             (can generate very large data volumes on large deployments)
    """
    validate_configuration(configuration)
    session = _build_session(configuration)
    base = _base_url(configuration)
    database_name = configuration.get("database_name")
    start_date = configuration.get("start_date", EPOCH_ISO)
    do_recorded = configuration.get("sync_recorded_values", "false").lower() == "true"

    db_web_id = get_database_web_id(session, base, database_name)

    sync_elements(session, base, db_web_id, state)
    pi_point_web_ids = sync_attributes(session, base, db_web_id, state)
    sync_event_frames(session, base, db_web_id, state, start_date)

    if do_recorded:
        sync_recorded_values(session, base, pi_point_web_ids, state, start_date)
    else:
        log.info(
            "Skipping recorded_values sync (set sync_recorded_values = \"true\" to enable). "
            "Note: enabling this can generate very large data volumes on large PI deployments."
        )


# Create the connector object with the schema and update functions.
connector = Connector(update=update, schema=schema)

# Standard Python entry point for local debugging.
# The recommended way to test is: fivetran debug
if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
