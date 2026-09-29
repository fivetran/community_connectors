"""Meta (Facebook) Graph helpers for page/form/lead iteration."""

from __future__ import annotations

import re  # For normalizing timestamp offsets before parsing them
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Tuple
import json  # For encoding the leads "filtering" query parameter

from http_helpers import _graph_get
from validator import validate_page_access_tokens


def discover_pages(cfg: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    """Yield the pages to sync, honoring explicit IDs when provided.

    Pages are yielded one at a time (one explicit page ID, or one paginated response, at a
    time) instead of being accumulated into a list first, so the connector can start
    processing a page's forms before later pages (or IDs) are even fetched. Each yielded
    page's access token is validated immediately, so a missing token fails fast.

    Args:
        cfg: the validated connector configuration.

    Yields:
        dict: a page dictionary with "id", "name", and "access_token" keys.
    """
    if cfg["page_ids_list"] is not None:
        for page_id in cfg["page_ids_list"]:
            data = _graph_get(
                f"{page_id}?fields=id,name,access_token",
                {"access_token": cfg["system_user_access_token"]},
                cfg,
            )
            page = {
                "id": page_id,
                "name": data.get("name"),
                "access_token": data.get("access_token"),
            }
            validate_page_access_tokens([page])
            yield page
        return
    for resp in _paginate_graph_collection(
        "me/accounts?fields=id,name,access_token",
        {"access_token": cfg["system_user_access_token"]},
        cfg,
    ):
        for page in resp.get("data", []):
            validate_page_access_tokens([page])
            yield page


def list_forms(page: Dict[str, Any], cfg: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    """Yield lead-gen forms for a page, filtered per configuration.

    When explicit `form_ids` are configured, every listed form is returned by ID
    regardless of status, and `include_archived_forms` is ignored. Otherwise, all
    forms are returned unless `include_archived_forms` is false, in which case only
    ACTIVE forms are kept. Forms are yielded one page of results at a time instead of
    being accumulated into a list first.

    Args:
        page: the page dictionary returned by discover_pages(), used for its ID and
            page-scoped access token.
        cfg: the validated connector configuration.

    Yields:
        dict: a form dictionary with "id", "name", and "status" keys.
    """
    explicit_form_ids = cfg["form_ids_list"]
    keep_active_only = explicit_form_ids is None and not cfg["include_archived_forms"]
    params = {"access_token": page.get("access_token")}
    for resp in _paginate_graph_collection(
        f"{page['id']}/leadgen_forms?fields=id,name,status",
        params,
        cfg,
    ):
        for form in resp.get("data", []):
            if explicit_form_ids is not None:
                if form.get("id") in explicit_form_ids:
                    yield form
            elif not keep_active_only or form.get("status") == "ACTIVE":
                yield form


def _convert_time_to_unix(timestr: str) -> int:
    """Convert an ISO8601 UTC timestamp string to a Unix epoch timestamp.

    Args:
        timestr: a timezone-aware ISO8601 timestamp, such as Meta's
            "2025-08-31T07:45:31+0000" or a "Z"-suffixed UTC timestamp.

    Returns:
        int: the equivalent Unix epoch timestamp.

    Raises:
        ValueError: if the value cannot be parsed, or has no timezone offset.
    """
    normalized = timestr.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    else:
        # Insert the colon fromisoformat expects in a bare "+HHMM"/"-HHMM" offset.
        normalized = re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", normalized)
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise ValueError(f"Timestamp '{timestr}' is missing timezone information")
    return int(parsed.timestamp())


def iterate_leads_for_form(
    page_token: str, form_id: str, cfg: Dict[str, Any], since_time: str | None
) -> Iterator[Tuple[List[Dict[str, Any]], Optional[str]]]:
    """Yield (batch, max_created_time) tuples when iterating leads for a form.

    Args:
        page_token: the page-scoped access token used to authorize the request.
        form_id: the lead-gen form to fetch leads for.
        cfg: the validated connector configuration.
        since_time: an ISO8601 cursor; only leads created after this time are fetched.

    Yields:
        tuple: a page of raw lead dictionaries, and the maximum `created_time` seen in
        that page (or None if the page had no leads with a `created_time`).
    """
    params: Dict[str, Any] = {
        "access_token": page_token,
        "limit": cfg["fetch_limit"],
        "fields": "created_time,id,ad_id,form_id,field_data",
    }
    if since_time:
        # Query with a 1-second overlap on the lower bound: since the cursor may have been
        # checkpointed mid-pagination (see connector.py's _process_form), a strict lower
        # bound could permanently skip a lead with the same second-level created_time as the
        # saved cursor. Re-fetching that second is safe because leads are upserted by lead_id.
        overlap_unix_time = _convert_time_to_unix(since_time) - 1
        params["filtering"] = json.dumps(
            [
                {
                    "field": "time_created",
                    "operator": "GREATER_THAN",
                    "value": overlap_unix_time,
                }
            ]
        )
    path = f"{form_id}/leads"
    after: Optional[str] = None
    while True:
        if after:
            params["after"] = after
        data = _graph_get(path, params, cfg)
        batch = data.get("data", [])
        if not batch:
            break
        batch_max_created: Optional[str] = None
        for lead in batch:
            created_time = lead.get("created_time")
            if created_time and (batch_max_created is None or created_time > batch_max_created):
                batch_max_created = created_time
        yield batch, batch_max_created
        cursors = data.get("paging", {}).get("cursors", {})
        after = cursors.get("after")
        if not after:
            break


def _paginate_graph_collection(
    path: str, params: Dict[str, Any] | None, cfg: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """Walk a Graph API collection endpoint's paging via the `next` cursor.

    Args:
        path: the initial relative path (or absolute URL) to request.
        params: query-string parameters for the initial request; subsequent pages
            are requested using the absolute `paging.next` URL, which already
            embeds its own parameters.
        cfg: the validated connector configuration.

    Yields:
        dict: each page's decoded JSON response body, in request order.
    """
    next_path: Optional[str] = path
    next_params: Optional[Dict[str, Any]] = params
    while next_path:
        resp = _graph_get(next_path, next_params, cfg)
        yield resp
        next_path = resp.get("paging", {}).get("next")
        next_params = None
