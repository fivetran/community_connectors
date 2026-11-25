"""Pure value-coercion helpers for the ZoomInfo connector.

Stateless functions that normalize raw ZoomInfo API values into the shapes
Fivetran expects: safe int/float coercion, ISO-8601 datetime handling, and
incremental-cursor comparison. No network access and no SDK dependency — kept
free of imports from client.py/sync.py so it can be imported anywhere without
a cycle.
"""

# For parsing and comparing ISO 8601 timestamps when advancing the incremental cursor.
from datetime import datetime, timezone


def _safe_int(value):
    """
    Coerces ZoomInfo API responses to int. The API has been observed to
    return numeric fields as either int, float, or stringified numbers
    depending on the endpoint. Returns None for None/empty/uncoerceable
    values rather than raising — Fivetran handles None as NULL.
    """
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def _safe_float(value):
    """Float counterpart to _safe_int. See that function for rationale."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_utc_datetime(value):
    """
    Coerces a ZoomInfo date response into a Fivetran UTC_DATETIME-acceptable string.
    The SDK's UTC_DATETIME parser only accepts full ISO 8601 timestamps
    (YYYY-MM-DDTHH:MM:SS[.f]±HHMM), but ZoomInfo sometimes returns a bare date
    (YYYY-MM-DD) for fields like technologies.createdDate. Bare dates are promoted
    to midnight UTC; full timestamps pass through unchanged; anything else
    (empty, None, unrecognized shape) becomes None so it lands as NULL rather
    than crashing the whole sync.
    """
    if not value or not isinstance(value, str):
        return None
    if "T" in value:
        return value
    if len(value) == 10 and value[4] == "-" and value[7] == "-":
        return f"{value}T00:00:00Z"
    return None


def _parse_iso_for_compare(value):
    """
    Parse a ZoomInfo-shaped ISO 8601 timestamp into a timezone-aware datetime
    for safe max-cursor comparison.

    String comparison on ISO strings *almost* works for the Z-suffixed
    timestamps ZoomInfo returns today, but breaks the moment any record arrives
    with a non-UTC offset (e.g. ``+02:00``). Parse to a real datetime instead.

    Returns None for falsy / unparseable input — callers should treat that as
    "skip this record for cursor purposes" rather than crashing the sync.
    """
    if not value or not isinstance(value, str):
        return None
    # datetime.fromisoformat accepts "Z" suffix from Python 3.11+, but we still
    # support 3.10 (per the CI matrix) so normalise it here.
    s = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        # Bare-date inputs like "2026-05-19" — promote to midnight UTC.
        if len(value) == 10 and value[4] == "-" and value[7] == "-":
            try:
                return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
            except ValueError:
                return None
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _max_cursor(current, candidate):
    """
    Returns the later of two ISO timestamp strings, treating None as "no value".
    Comparison is performed on parsed datetimes (timezone-aware) so mixed-offset
    responses are handled correctly. The returned value is whichever input
    string compared larger — we preserve the original string so it round-trips
    back into state unchanged.
    """
    if not candidate:
        return current
    if not current:
        return candidate
    c_dt = _parse_iso_for_compare(current)
    n_dt = _parse_iso_for_compare(candidate)
    if c_dt is None:
        return candidate
    if n_dt is None:
        return current
    return candidate if n_dt > c_dt else current


def _iso_to_yyyymmdd(iso_string: str | None) -> str | None:
    """
    Coerces an ISO 8601 timestamp like '2026-05-19T23:31:00Z' down to the
    'YYYY-MM-DD' format the ZoomInfo Search filter API expects. If the input
    already looks like a date (no 'T'), it's passed through unchanged.
    Returns None for falsy input.
    """
    if not iso_string:
        return None
    return iso_string.split("T", 1)[0] if "T" in iso_string else iso_string
