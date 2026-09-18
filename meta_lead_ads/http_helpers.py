"""HTTP helper utilities for Meta Leads connector."""

from __future__ import annotations
import time
from typing import Dict, Any
import requests
from requests import Response
from fivetran_connector_sdk import Logging as log

__RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
__MAX_ATTEMPTS = 5
__BACKOFF_BASE = 1.5
BASE_URL_TEMPLATE = "https://graph.facebook.com/{version}"


def request_with_retries(
    method: str, url: str, *, params: Dict[str, Any] | None = None, cfg: Dict[str, Any]
) -> Dict[str, Any] | None:
    """Execute an HTTP request with simple exponential backoff.

    Returns the decoded JSON body on success; returns None after exhausting retries.
    Callers must handle the None case (critical failures).
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            resp: Response = requests.request(
                method, url, params=params, timeout=cfg["request_timeout_seconds"]
            )
        except requests.RequestException as e:
            log.warning(
                f"Network error (attempt {attempt}/{__MAX_ATTEMPTS}) url={url} error={e}"
            )
            if attempt >= __MAX_ATTEMPTS:
                return None
            time.sleep(__BACKOFF_BASE**attempt)
            continue
        if resp.status_code == 200:
            try:
                return resp.json()
            except ValueError:
                log.severe(f"Invalid JSON response from {url}")
                return None
        if resp.status_code in __RETRY_STATUS_CODES and attempt < __MAX_ATTEMPTS:
            wait = __BACKOFF_BASE**attempt
            log.warning(
                f"Retryable status {resp.status_code} attempt {attempt}/{__MAX_ATTEMPTS} waiting {wait:.1f}s url={url}"
            )
            time.sleep(wait)
            continue
        if resp.status_code != 200:
            log.severe(
                f"Non-success status {resp.status_code} url={url} body={resp.text[:300]}"
            )
            return None


def _graph_get(
    path: str, params: Dict[str, Any] | None, cfg: Dict[str, Any]
) -> Dict[str, Any] | None:
    """Issue a Graph API GET request for a relative path or absolute URL."""
    if path.startswith("http://") or path.startswith("https://"):
        url = path
    else:
        base = BASE_URL_TEMPLATE.format(version=cfg["graph_version"])
        url = f"{base}/{path}"
    return request_with_retries("GET", url, params=params, cfg=cfg)
