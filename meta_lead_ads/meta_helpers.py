"""Meta (Facebook) Graph helpers for page/form/lead iteration."""

from __future__ import annotations
import json
from typing import Dict, Any, List, Optional, Iterator, Tuple
from http_helpers import _graph_get
from validator import validate_page_access_tokens
from datetime import datetime


def discover_pages(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return the list of pages to sync, honoring explicit IDs when provided."""
    if cfg["page_ids_list"] is not None:
        pages: List[Dict[str, Any]] = []
        for page_id in cfg["page_ids_list"]:
            data = _graph_get(
                f"{page_id}?fields=name,access_token",
                {"access_token": cfg["system_user_access_token"]},
                cfg,
            )
            if data:
                pages.append(
                    {
                        "id": page_id,
                        "name": data.get("name"),
                        "access_token": data.get("access_token"),
                    }
                )
        validate_page_access_tokens(pages)
        return pages
    pages: List[Dict[str, Any]] = []
    for resp in _paginate_graph_collection(
        "me/accounts",
        {"access_token": cfg["system_user_access_token"]},
        cfg,
    ):
        pages.extend(resp.get("data", []))
    validate_page_access_tokens(pages)
    return pages


def list_forms(page: Dict[str, Any], cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """List lead-gen forms for a page, filtered per configuration."""
    params = {"access_token": page.get("access_token")}
    forms: List[Dict[str, Any]] = []
    for resp in _paginate_graph_collection(
        f"{page['id']}/leadgen_forms",
        params,
        cfg,
    ):
        forms.extend(resp.get("data", []))
    if not cfg["include_archived_forms"]:
        forms = [f for f in forms if f.get("status") == "ACTIVE"]
    if cfg["form_ids_list"] is not None:
        forms = [f for f in forms if f.get("id") in cfg["form_ids_list"]]
    return forms


def _convert_time_to_unix(timestr: str) -> int:
    """Convert a Graph API-formatted UTC timestamp string to a Unix epoch timestamp."""
    dt = datetime.strptime(timestr, "%Y-%m-%dT%H:%M:%S+0000")
    return int(dt.timestamp())


def iterate_leads_for_form(
    page_token: str, form_id: str, cfg: Dict[str, Any], since_time: str | None
) -> Iterator[Tuple[List[Dict[str, Any]], Optional[str]]]:
    """Yield (batch, max_created_time) tuples when iterating leads for a form."""
    params: Dict[str, Any] = {
        "access_token": page_token,
        "limit": cfg["fetch_limit"],
        "fields": "created_time,id,ad_id,form_id,field_data",
    }
    if since_time:
        params["filtering"] = json.dumps(
            [
                {
                    "field": "time_created",
                    "operator": "GREATER_THAN",
                    "value": _convert_time_to_unix(since_time),
                }
            ]
        )
    path = f"{form_id}/leads"
    after: Optional[str] = None
    while True:
        if after:
            params["after"] = after
        data = _graph_get(path, params, cfg)
        if not data:
            break
        batch = data.get("data", [])
        if not batch:
            break
        batch_max_created: Optional[str] = None
        for lead in batch:
            ct = lead.get("created_time")
            if ct and (batch_max_created is None or ct > batch_max_created):
                batch_max_created = ct
        yield batch, batch_max_created
        cursors = data.get("paging", {}).get("cursors", {})
        after = cursors.get("after")
        if not after:
            break


def _paginate_graph_collection(
    path: str, params: Dict[str, Any] | None, cfg: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """Generator that walks Graph API paging via the `next` cursor."""
    next_path: Optional[str] = path
    next_params: Optional[Dict[str, Any]] = params
    while next_path:
        resp = _graph_get(next_path, next_params, cfg)
        if not resp:
            break
        yield resp
        next_path = resp.get("paging", {}).get("next")
        next_params = None
        if not next_path:
            break
