# For MD5 hash generation used in synthetic primary keys
import hashlib

# For JSON serialization of list fields (e.g. category_names)
import json

# For UTC-aware datetime parsing and formatting
from datetime import datetime, timezone

# For nullable return type annotation
from typing import Optional


def parse_pi_timestamp(ts: str) -> Optional[datetime]:
    """
    Parse a PI Web API ISO 8601 timestamp string to a UTC-aware datetime.

    Args:
        ts: ISO 8601 timestamp string (with Z or numeric offset).
    Returns:
        UTC-aware datetime, or None if ts is empty/invalid.
    """
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


def fmt_ts(dt: datetime) -> str:
    """
    Format a datetime as a PI Web API-compatible UTC timestamp string.

    Args:
        dt: a timezone-aware datetime object.
    Returns:
        ISO 8601 string in 'YYYY-MM-DDTHH:MM:SSZ' format.
    """
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _category_names(item: dict) -> str:
    """
    Serialize the CategoryNames list from a PI Web API item to a JSON string.

    Args:
        item: a PI Web API response item dict.
    Returns:
        JSON-encoded list string, e.g. '["Production", "Critical"]'.
    """
    return json.dumps(item.get("CategoryNames") or [])


def extract_element(item: dict) -> dict:
    """
    Extract a flat record dict from a PI Web API element item.

    Args:
        item: a PI Web API element response dict.
    Returns:
        Record dict for upserting into the 'elements' table.
    """
    return {
        "web_id": item.get("WebId", ""),
        "name": item.get("Name", ""),
        "description": item.get("Description", ""),
        "path": item.get("Path", ""),
        "template_name": item.get("TemplateName", ""),
        "category_names": _category_names(item),
    }


def extract_attribute(item: dict, element_web_id: str) -> dict:
    """
    Extract a flat record dict from a PI Web API attribute item.

    Args:
        item: a PI Web API attribute response dict.
        element_web_id: the WebId of the parent element.
    Returns:
        Record dict for upserting into the 'attributes' table.
    """
    return {
        "web_id": item.get("WebId", ""),
        "element_web_id": element_web_id,
        "name": item.get("Name", ""),
        "description": item.get("Description", ""),
        "path": item.get("Path", ""),
        "type": item.get("Type", ""),
        "type_qualifier": item.get("TypeQualifier", ""),
        "data_reference": item.get("DataReferencePlugIn", ""),
        "data_reference_path": item.get("ConfigString", ""),
        "category_names": _category_names(item),
    }


def extract_event_frame(item: dict, db_web_id: str) -> dict:
    """
    Extract a flat record dict from a PI Web API event frame item.

    Args:
        item: a PI Web API event frame response dict.
        db_web_id: the WebId of the parent AF database.
    Returns:
        Record dict for upserting into the 'event_frames' table.
    """
    return {
        "web_id": item.get("WebId", ""),
        "name": item.get("Name", ""),
        "description": item.get("Description", ""),
        "start_time": parse_pi_timestamp(item.get("StartTime")),
        "end_time": parse_pi_timestamp(item.get("EndTime")),
        "template_name": item.get("TemplateName", ""),
        "category_names": _category_names(item),
        "database_web_id": db_web_id,
    }


def generate_recorded_value_id(attr_web_id: str, timestamp: str) -> str:
    """
    Generate a deterministic MD5 primary key for a recorded value row.

    Uses the attribute WebId and the raw timestamp string so that the same
    data point always produces the same key across syncs.

    Args:
        attr_web_id: the WebId of the PI Point attribute.
        timestamp: the raw ISO timestamp string from the PI Web API response.
    Returns:
        A 32-character lowercase hex digest string.
    """
    return hashlib.md5(
        f"{attr_web_id}|{timestamp}".encode("utf-8"), usedforsecurity=False
    ).hexdigest()


def extract_recorded_value(item: dict, attr_web_id: str) -> dict:
    """
    Extract a flat record dict from a PI Web API recorded value item.

    PI digital state values are returned as dicts (e.g. {"Name": "Shutdown", "Value": 248});
    only the Name string is stored in the 'value' column.

    Args:
        item: a PI Web API recorded value response dict.
        attr_web_id: the WebId of the PI Point attribute this value belongs to.
    Returns:
        Record dict for upserting into the 'recorded_values' table.
    """
    ts_str = item.get("Timestamp", "")
    value = item.get("Value")
    if isinstance(value, dict):
        value = value.get("Name") or str(value)
    else:
        value = str(value) if value is not None else None

    return {
        "_fivetran_id": generate_recorded_value_id(attr_web_id, ts_str),
        "attribute_web_id": attr_web_id,
        "timestamp": parse_pi_timestamp(ts_str),
        "value": value,
        "quality": "questionable" if item.get("Questionable") else "good",
        "good": not item.get("Questionable", False),
    }
