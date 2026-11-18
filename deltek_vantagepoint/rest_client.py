"""HTTP client for the Deltek Vantagepoint API with retry logic and automatic token refresh.

This module implements the HTTP request layer with:
1. Retry logic - exponential backoff for transient errors (timeouts, connection errors, 429, 5xx)
2. Automatic token refresh - re-authenticates and retries once on a 401
3. Security - never logs response bodies, since this connector's endpoints return personnel data
   (SSNs, birth dates, home addresses, etc.) and an API error body could echo back request or
   record content
"""

# For sleeping between retry attempts
import time

# For making GET requests to Vantagepoint endpoints
import requests

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# Retry/backoff/timeout limits shared with authentication.py
from sync_limits import __BASE_DELAY_SECONDS, __MAX_RETRIES, __REQUEST_TIMEOUT_SECONDS

# For refreshing an expired access token on a 401 response
from authentication import get_access_token


class ApiStatusError(RuntimeError):
    """
    Raised when a Vantagepoint API request fails after retries. Carries the HTTP status code (when
    the failure came from an HTTP response at all - None for a timeout or connection error) and
    the request URL as attributes, so callers can branch on error.status_code directly instead of
    parsing the exception message. Still a RuntimeError, so existing `except RuntimeError` callers
    keep working unchanged.
    """

    def __init__(self, message, status_code=None, url=None):
        super().__init__(message, status_code, url)
        self.status_code = status_code
        self.url = url

    def __str__(self):
        # All constructor args are forwarded to super().__init__() so this exception survives
        # pickle/copy correctly, but that makes the default BaseException.__str__() show a tuple
        # of all three args - override it to keep str(error) as just the message, matching every
        # log message in this connector that interpolates the error directly.
        return str(self.args[0])


def get_api_response(url, params, access_token, base_url, configuration):
    """
    Make a GET request to a Vantagepoint Hubs endpoint with retry logic, exponential backoff, and
    automatic token refresh on 401 responses.
    Args:
        url: The full API endpoint URL.
        params: Query parameters for the request (pagination, ordering, filterHash).
        access_token: Current OAuth2 bearer access token.
        base_url: The Vantagepoint API base URL (needed to refresh the token if it expires).
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        tuple: (list of record dicts, possibly-refreshed access_token).
    Raises:
        ApiStatusError: If the request fails after retries.
    """
    for attempt in range(__MAX_RETRIES):
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        try:
            response = requests.get(
                url, headers=headers, params=params, timeout=__REQUEST_TIMEOUT_SECONDS
            )

            if response.status_code == 200:
                return response.json(), access_token

            if response.status_code == 401:
                log.warning(
                    f"Access token expired or invalid (attempt {attempt + 1}/{__MAX_RETRIES}), refreshing"
                )
                access_token = get_access_token(base_url, configuration)
                continue

            if response.status_code in (429, 500, 502, 503, 504):
                _handle_retryable_error(attempt, response.status_code, url)
                continue

            # Deliberately do NOT log response.text: this connector's endpoints return personnel
            # data (SSNs, birth dates, home addresses, etc.), and an API error body could echo back
            # request or record content. Log only the status code and URL - never response content.
            log.error(f"Unexpected API error: {response.status_code} for {url}")
            raise ApiStatusError(
                f"API error: {response.status_code} for {url}",
                status_code=response.status_code,
                url=url,
            )

        except requests.exceptions.Timeout:
            _handle_timeout_error(attempt, url)
            continue
        except requests.exceptions.RequestException as error:
            _handle_request_exception(attempt, url, error)
            continue

    raise ApiStatusError(f"API request to {url} failed after maximum retries", url=url)


def _handle_retryable_error(attempt, status_code, url):
    """
    Sleep with exponential backoff for a retryable HTTP status code, or raise once retries are
    exhausted.
    Args:
        attempt: The current retry attempt number (0-indexed).
        status_code: The HTTP status code that triggered the retry.
        url: The request URL, used in log messages and the raised exception.
    Raises:
        ApiStatusError: If the maximum number of retry attempts has been reached.
    """
    operation_name = f"Request to {url}"
    if attempt < __MAX_RETRIES - 1:
        delay = __BASE_DELAY_SECONDS * (2**attempt)
        log.warning(
            f"{operation_name} failed with status {status_code}, retrying in {delay} seconds "
            f"(attempt {attempt + 1}/{__MAX_RETRIES})"
        )
        time.sleep(delay)
    else:
        log.error(f"{operation_name} failed after {__MAX_RETRIES} attempts. Status: {status_code}")
        raise ApiStatusError(
            f"{operation_name} failed after {__MAX_RETRIES} attempts",
            status_code=status_code,
            url=url,
        )


def _handle_timeout_error(attempt, url):
    """
    Sleep with exponential backoff after a request timeout, or raise once retries are exhausted.
    Args:
        attempt: The current retry attempt number (0-indexed).
        url: The request URL, used in log messages and the raised exception.
    Raises:
        ApiStatusError: If the maximum number of retry attempts has been reached. status_code is
            None - a timeout has no HTTP status code to carry.
    """
    operation_name = f"Request to {url}"
    if attempt < __MAX_RETRIES - 1:
        delay = __BASE_DELAY_SECONDS * (2**attempt)
        log.warning(
            f"{operation_name} timed out, retrying in {delay} seconds "
            f"(attempt {attempt + 1}/{__MAX_RETRIES})"
        )
        time.sleep(delay)
    else:
        log.error(f"{operation_name} timed out after {__MAX_RETRIES} attempts")
        raise ApiStatusError(f"{operation_name} timed out after {__MAX_RETRIES} attempts", url=url)


def _handle_request_exception(attempt, url, error):
    """
    Sleep with exponential backoff after a generic request exception, or raise once retries are
    exhausted.
    Args:
        attempt: The current retry attempt number (0-indexed).
        url: The request URL, used in log messages and the raised exception.
        error: The exception that was raised by the request.
    Raises:
        ApiStatusError: If the maximum number of retry attempts has been reached. status_code is
            None - a connection error has no HTTP status code to carry.
    """
    operation_name = f"Request to {url}"
    if attempt < __MAX_RETRIES - 1:
        delay = __BASE_DELAY_SECONDS * (2**attempt)
        log.warning(
            f"{operation_name} failed, retrying in {delay} seconds (attempt {attempt + 1}/{__MAX_RETRIES})"
        )
        time.sleep(delay)
    else:
        log.error(f"{operation_name} failed after {__MAX_RETRIES} attempts")
        raise ApiStatusError(f"{operation_name} failed: {str(error)}", url=url)
