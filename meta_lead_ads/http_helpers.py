"""HTTP helper utilities for the Meta Leads connector."""

from __future__ import annotations

import re  # For redacting sensitive query-string values before they reach the logs
import time  # For sleeping between retry attempts
from typing import Any, Dict
from urllib.parse import urlsplit, urlunsplit

import requests  # For issuing HTTP requests to the Meta Graph API
from requests import Response

from fivetran_connector_sdk import Logging as log

__RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
__MAX_ATTEMPTS = 5
__BACKOFF_BASE = 1.5
BASE_URL_TEMPLATE = "https://graph.facebook.com/{version}"

# Query-string parameters whose values must never reach the logs.
__SENSITIVE_PARAMS = ("access_token",)


def _redact_url(url: str) -> str:
    """Redact sensitive query-string parameter values from a URL before logging it.

    Args:
        url: the URL to redact, which may contain an access token or other secret
            in its query string (including Meta's absolute `paging.next` URLs).

    Returns:
        str: the same URL with the value of each sensitive parameter replaced by
        "REDACTED".
    """
    parts = urlsplit(url)
    query = parts.query
    for param in __SENSITIVE_PARAMS:
        query = re.sub(rf"(?<![\w]){param}=[^&]*", f"{param}=REDACTED", query)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))


def request_with_retries(
    method: str, url: str, *, params: Dict[str, Any] | None = None, cfg: Dict[str, Any]
) -> Dict[str, Any]:
    """Execute an HTTP request with exponential backoff, raising on unrecoverable failures.

    Args:
        method: the HTTP method to use (for example, "GET").
        url: the absolute or relative URL to request.
        params: optional query-string parameters for the request.
        cfg: the validated connector configuration, used here for `request_timeout_seconds`.

    Returns:
        dict: the decoded JSON response body on success.

    Raises:
        RuntimeError: if the request fails after exhausting retries, the source returns a
            non-retryable error status, or the response body is not valid JSON. A raised
            error fails the sync instead of silently truncating it.
    """
    safe_url = _redact_url(url)
    attempt = 0
    while True:
        attempt += 1
        try:
            resp: Response = requests.request(
                method, url, params=params, timeout=cfg["request_timeout_seconds"]
            )
        except requests.RequestException as e:
            log.warning(f"Network error (attempt {attempt}/{__MAX_ATTEMPTS}) url={safe_url}: {e}")
            if attempt >= __MAX_ATTEMPTS:
                raise RuntimeError(
                    f"Request to {safe_url} failed after {__MAX_ATTEMPTS} attempts: {e}"
                ) from e
            time.sleep(__BACKOFF_BASE**attempt)
            continue
        if resp.status_code == 200:
            try:
                return resp.json()
            except ValueError as e:
                log.error(f"Invalid JSON response from {safe_url}")
                raise RuntimeError(f"Invalid JSON response from {safe_url}") from e
        if resp.status_code in __RETRY_STATUS_CODES and attempt < __MAX_ATTEMPTS:
            wait = __BACKOFF_BASE**attempt
            log.warning(
                f"Retryable status {resp.status_code} attempt {attempt}/{__MAX_ATTEMPTS} "
                f"waiting {wait:.1f}s url={safe_url}"
            )
            time.sleep(wait)
            continue
        log.error(f"Non-success status {resp.status_code} url={safe_url}")
        raise RuntimeError(f"Request to {safe_url} failed with status {resp.status_code}")


def _graph_get(path: str, params: Dict[str, Any] | None, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Issue a Graph API GET request for a relative path or an absolute URL.

    Args:
        path: a relative Graph API path, or an absolute URL such as a `paging.next` link.
        params: optional query-string parameters for the request.
        cfg: the validated connector configuration.

    Returns:
        dict: the decoded JSON response body.
    """
    if path.startswith("http://") or path.startswith("https://"):
        url = path
    else:
        base = BASE_URL_TEMPLATE.format(version=cfg["graph_version"])
        url = f"{base}/{path}"
    return request_with_retries("GET", url, params=params, cfg=cfg)
