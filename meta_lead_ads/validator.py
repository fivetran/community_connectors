"""Validation utilities for the Meta Leads connector."""

from __future__ import annotations

import re  # For normalizing timestamp offsets before parsing them
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

__GRAPH_VERSION_FALLBACK = "v19.0"


def _parse_bool(value: Optional[str], default: bool = False) -> bool:
    """Parse a configuration string value as a boolean, defaulting only when unset.

    Args:
        value: the raw configuration string, or None if the key was not set.
        default: the value to return when `value` is None.

    Returns:
        bool: the parsed boolean value.

    Raises:
        ValueError: if `value` is set but is not a recognized true/false literal.
    """
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    raise ValueError(f"Invalid boolean configuration value: '{value}'. Use 'true' or 'false'.")


def _normalize_to_meta_utc(value: str) -> str:
    """Normalize an ISO8601 timestamp to the UTC "+0000" format Meta returns for leads.

    Args:
        value: a timezone-aware ISO8601 timestamp, such as "2023-01-01T00:00:00Z" or
            "2023-01-01T00:00:00+05:30".

    Returns:
        str: the equivalent timestamp in "%Y-%m-%dT%H:%M:%S+0000" form, so it can be
        compared lexicographically with Meta's own `created_time` values.

    Raises:
        ValueError: if the value cannot be parsed, or has no timezone offset.
    """
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    else:
        # Insert the colon fromisoformat expects in a bare "+HHMM"/"-HHMM" offset.
        normalized = re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", normalized)
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"'{value}' is not a valid ISO8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"'{value}' must include timezone information")
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000")


def validate_configuration(configuration: Dict[str, str]) -> Dict[str, Any]:
    """Validate and normalize the raw configuration into typed values used by the connector.

    Args:
        configuration: raw string-valued configuration dictionary provided by the user.

    Returns:
        dict: configuration with defaults applied and values parsed to their target types
        (int settings, booleans, parsed ID lists, and a normalized initial_start_time).

    Raises:
        ValueError: if a required key is missing or blank, or a value fails validation.
    """
    required = ["system_user_access_token", "page_ids", "form_ids"]
    missing = [k for k in required if not str(configuration.get(k, "")).strip()]
    if missing:
        raise ValueError(f"Missing required configuration keys: {missing}")

    cfg: Dict[str, Any] = dict(configuration)
    cfg["system_user_access_token"] = configuration["system_user_access_token"].strip()
    cfg["graph_version"] = configuration.get("graph_version") or __GRAPH_VERSION_FALLBACK
    cfg["include_archived_forms"] = _parse_bool(
        configuration.get("include_archived_forms", "false")
    )
    cfg["request_timeout_seconds"] = _int_setting(
        configuration, "request_timeout_seconds", default=30, minimum=1
    )
    cfg["fetch_limit"] = _int_setting(
        configuration, "fetch_limit", default=1000, minimum=1, maximum=1000
    )
    cfg["check_point_limit"] = _int_setting(
        configuration, "check_point_limit", default=1000, minimum=1
    )

    cfg["page_ids_list"] = _parse_id_list(configuration, "page_ids")
    cfg["form_ids_list"] = _parse_id_list(configuration, "form_ids")

    raw_start = configuration.get("initial_start_time")
    if raw_start:
        start_time = raw_start.strip()
        if len(start_time) == 10:
            start_time = f"{start_time}T00:00:00+00:00"
        cfg["initial_start_time"] = _normalize_to_meta_utc(start_time)
    else:
        cfg["initial_start_time"] = None  # start from earliest available

    return cfg


def _parse_id_list(configuration: Dict[str, str], key: str) -> Optional[List[str]]:
    """Parse a comma-separated configuration value into an ID list, or None for "ALL".

    Args:
        configuration: raw string-valued configuration dictionary provided by the user.
        key: the configuration key to parse ("page_ids" or "form_ids").

    Returns:
        list: the trimmed, non-empty IDs, or None when the value is "ALL".

    Raises:
        ValueError: if the value is blank, or contains no valid IDs after splitting.
    """
    raw_value = configuration.get(key, "")
    raw = raw_value.strip() if isinstance(raw_value, str) else str(raw_value).strip()
    if not raw:
        raise ValueError(f"{key} is required; use 'ALL' or provide IDs.")
    if raw.upper() == "ALL":
        return None
    ids = [item.strip() for item in raw.split(",") if item.strip()]
    if not ids:
        raise ValueError(f"{key} contains no valid IDs; use 'ALL' or supply values.")
    return ids


def validate_page_access_tokens(pages: List[Dict[str, Any]]) -> None:
    """Raise if any discovered page is missing an access token needed to sync its forms.

    Args:
        pages: page dictionaries as returned by discover_pages().

    Raises:
        RuntimeError: if any page has no access_token.
    """
    missing = [p.get("id") for p in pages if not p.get("access_token")]
    if missing:
        raise RuntimeError(
            f"Discovered pages missing access tokens: {missing}. Verify system user token "
            "permissions (pages_show_list, ads_management, leads retrieval)."
        )


def _int_setting(
    configuration: Dict[str, Any],
    key: str,
    *,
    default: int,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
) -> int:
    """Parse a configuration value as an integer, applying a default and range checks.

    Args:
        configuration: raw string-valued configuration dictionary provided by the user.
        key: the configuration key to parse.
        default: the value to use when the key is absent or blank.
        minimum: if set, the smallest value allowed.
        maximum: if set, the largest value allowed.

    Returns:
        int: the parsed, validated value.

    Raises:
        ValueError: if the value is not an integer, or is out of the allowed range.
    """
    raw = configuration.get(key, default)
    if isinstance(raw, str):
        raw = raw.strip() or default
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be an integer. Error: {exc}") from exc
    if minimum is not None and value < minimum:
        raise ValueError(f"{key} must be >= {minimum}. Got {value}.")
    if maximum is not None and value > maximum:
        raise ValueError(f"{key} must be <= {maximum}. Got {value}.")
    return value
