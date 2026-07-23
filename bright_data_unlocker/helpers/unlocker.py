"""Bright Data Web Unlocker helper functions."""

# For parsing JSON payloads
import json

# For retry backoff delays
import time

# For type hints
from typing import Any, Union

# For making HTTP requests to the Bright Data API
import requests

# For HTTP error handling
from requests import RequestException, Response

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

__BRIGHT_DATA_BASE_URL = "https://api.brightdata.com"
__DEFAULT_UNLOCKER_ZONE = "web_unlocker1"
__DEFAULT_TIMEOUT_SECONDS = 120
__RETRY_STATUS_CODES = {408, 429, 500, 502, 503, 504}


def _parse_response_payload(response: Response) -> Any:
    """Return JSON payload when available, otherwise raw text.
    Args:
        response: The response from the Bright Data API.
    Returns:
        A dictionary with the parsed response payload.
    """
    try:
        return response.json()
    except ValueError:
        return response.text


def _extract_error_detail(response: Response) -> str:
    """Extract error detail from a failed Bright Data response.
    Args:
        response: The response from the Bright Data API.
    Returns:
        A string with the error detail.
    """
    try:
        payload = response.json()
        if isinstance(payload, dict):
            for key in ("error", "message", "detail", "details"):
                if key in payload:
                    return str(payload[key])
            return str(payload)
        return str(payload)
    except ValueError:
        return response.text


def perform_web_unlocker(
    api_token: str,
    url: Union[str, list],
    zone: str | None = __DEFAULT_UNLOCKER_ZONE,
    country: str | None = "us",
    method: str | None = "GET",
    format_param: str | None = "json",
    data_format: str | None = "markdown",
    timeout: int = __DEFAULT_TIMEOUT_SECONDS,
    retries: int = 3,
    backoff_factor: float = 1.5,
) -> list:
    """Invoke Bright Data's Web Unlocker REST API.
    Args:
        api_token: The Bright Data API token.
        url: The URL to unlock.
        zone: The zone to use for the unlocker.
        country: The country to use for the unlocker.
        method: The method to use for the unlocker.
        format_param: The format to use for the unlocker.
        data_format: The data format to use for the unlocker.
        timeout: The timeout to use for the unlocker.
        retries: The number of retries to use for the unlocker.
        backoff_factor: The backoff factor to use for the unlocker.
    Returns:
        A list of dictionaries with the unlocked results.
    Raises:
        ValueError: If the API token is not valid.
        TypeError: If the URL is not a string or list of strings.
        ValueError: If the URL is empty.
        ValueError: If no non-empty URLs are provided.
    """
    if not api_token or not isinstance(api_token, str):
        raise ValueError("A valid Bright Data API token is required")

    if not url:
        raise ValueError("URL cannot be empty")

    if not isinstance(url, (str, list)):
        raise TypeError("URL must be a string or list of strings")

    if isinstance(url, list):
        urls = [item.strip() for item in url if isinstance(item, str) and item.strip()]
    else:
        urls = [url.strip()]

    if not urls:
        raise ValueError("At least one non-empty URL must be provided")

    zone_identifier = zone or __DEFAULT_UNLOCKER_ZONE
    aggregated_results: list = []

    for single_url in urls:
        payload: dict = {
            "zone": zone_identifier,
            "url": single_url,
            "format": format_param or "json",
        }

        if country:
            payload["country"] = country.lower()
        if method:
            payload["method"] = method
        if data_format:
            payload["data_format"] = data_format

        response_payload = _execute_unlocker_request(
            api_token=api_token,
            payload=payload,
            timeout=timeout,
            retries=retries,
            backoff_factor=backoff_factor,
        )

        aggregated_results.extend(_normalize_unlocker_result(response_payload, single_url))

    log.info(f"Unlocker completed successfully. Retrieved {len(aggregated_results)} result(s)")
    return aggregated_results


def _execute_unlocker_request(
    api_token: str,
    payload: dict,
    timeout: int,
    retries: int,
    backoff_factor: float,
) -> Any:
    """Execute a single unlocker API request with retry logic.
    Args:
        api_token: The Bright Data API token.
        payload: The payload to send to the unlocker.
        timeout: The timeout to use for the unlocker.
        retries: The number of retries to use for the unlocker.
        backoff_factor: The backoff factor to use for the unlocker.
    Returns:
        A dictionary with the unlocked result.
    Raises:
        RuntimeError: If the Bright Data Unlocker request fails.
    """
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    attempt = 0
    backoff = backoff_factor

    while attempt <= retries:
        try:
            response = requests.post(
                f"{__BRIGHT_DATA_BASE_URL}/request?async=true",
                headers=headers,
                json=payload,
                timeout=timeout,
            )

            if response.status_code == 200:
                return _parse_response_payload(response)

            if response.status_code in __RETRY_STATUS_CODES and attempt < retries:
                attempt += 1
                log.warning(
                    f"Bright Data Unlocker request retry {attempt}/{retries} "
                    f"for URL '{payload.get('url')}' (status code: {response.status_code})"
                )
                time.sleep(backoff)
                backoff *= backoff_factor
                continue

            error_detail = _extract_error_detail(response)
            raise RuntimeError(
                f"Bright Data Unlocker request failed for URL '{payload.get('url')}': "
                f"{error_detail}"
            )

        except RequestException as exc:
            if attempt < retries:
                attempt += 1
                log.warning(
                    f"Error contacting Bright Data Unlocker API for URL '{payload.get('url')}': "
                    f"{str(exc)}. Retrying ({attempt}/{retries})"
                )
                time.sleep(backoff)
                backoff *= backoff_factor
                continue
            raise RuntimeError(
                f"Failed to execute Bright Data Unlocker request for URL "
                f"'{payload.get('url')}' after {retries} retries: {str(exc)}"
            ) from exc

    raise RuntimeError("Failed to trigger Bright Data Unlocker request after retries")


def _normalize_unlocker_result(payload: Any, source_url: str) -> list:
    """Normalize the unlocker result.
    Args:
        payload: The payload to normalize.
        source_url: The source URL.
    Returns:
        A list of dictionaries with the normalized result.
    """
    normalized: list = []

    if isinstance(payload, list):
        for item in payload:
            normalized.extend(_normalize_unlocker_result(item, source_url))
        return normalized

    if isinstance(payload, dict):
        result = payload.copy()
        result.setdefault("requested_url", source_url)
        return [result]

    if isinstance(payload, str):
        try:
            parsed = json.loads(payload)
            return _normalize_unlocker_result(parsed, source_url)
        except ValueError:
            pass
    return [{"requested_url": source_url, "raw_response": payload}]
