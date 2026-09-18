"""Validation utilities for Meta Leads connector."""

from __future__ import annotations
from datetime import datetime
from typing import Dict, Any, List, Optional

__GRAPH_VERSION_FALLBACK = "v19.0"


def _parse_bool(val: Optional[str], default: bool = False) -> bool:
    """Parse a configuration string value as a boolean, defaulting when unset."""
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y"}


def validate_configuration(configuration: Dict[str, str]) -> Dict[str, Any]:
    """Validate and normalize the raw configuration into typed values used by the connector.

    Args:
        configuration: raw string-valued configuration dictionary provided by the user.

    Returns:
        dict: configuration with defaults applied and values parsed to their target types
        (int settings, booleans, parsed ID lists, and a normalized initial_start_time).

    Raises:
        ValueError: if a required key is missing or a value fails validation.
    """
    required = [
        "system_user_access_token",
        "page_ids",
        "form_ids",
    ]
    missing = [k for k in required if not configuration.get(k)]
    if missing:
        raise ValueError(f"Missing required configuration keys: {missing}")

    cfg: Dict[str, Any] = dict(configuration)
    cfg["graph_version"] = (
        configuration.get("graph_version") or __GRAPH_VERSION_FALLBACK
    )
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

    def _list(key: str) -> Optional[List[str]]:
        """Parse a comma-separated configuration value into an ID list, or None for 'ALL'."""
        raw_value = configuration.get(key, "")
        raw = (
            raw_value.strip() if isinstance(raw_value, str) else str(raw_value).strip()
        )
        if not raw:
            raise ValueError(f"{key} is required; use 'ALL' or provide IDs.")
        if raw.upper() == "ALL":
            return None
        ids = [x.strip() for x in raw.split(",") if x.strip()]
        if not ids:
            raise ValueError(
                f"{key} contains no valid IDs; use 'ALL' or supply values."
            )
        return ids

    cfg["page_ids_list"] = _list("page_ids")
    cfg["form_ids_list"] = _list("form_ids")

    raw_start = configuration.get("initial_start_time")
    if raw_start:
        start_time = raw_start.strip()
        if len(start_time) == 10:
            start_dt = datetime.strptime(start_time, "%Y-%m-%d")
            cfg["initial_start_time"] = start_dt.strftime("%Y-%m-%dT00:00:00+0000")
        else:
            if "T" not in start_time:
                raise ValueError(
                    "initial_start_time must be YYYY-MM-DD or full ISO8601 timestamp with 'T'"
                )
            cfg["initial_start_time"] = start_time
    else:
        cfg["initial_start_time"] = None  # start from earliest available

    return cfg


def validate_page_access_tokens(pages: List[Dict[str, Any]]) -> None:
    """Raise if any discovered page is missing an access token needed to sync its forms."""
    missing = [p.get("id") for p in pages if not p.get("access_token")]
    if missing:
        raise RuntimeError(
            f"Discovered pages missing access tokens: {missing}. Verify system user token permissions (pages_show_list, ads_management, leads retrieval)."
        )


def _int_setting(
    configuration: Dict[str, Any],
    key: str,
    *,
    default: int,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
) -> int:
    """Parse a configuration value as an integer, applying a default and range checks."""
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
