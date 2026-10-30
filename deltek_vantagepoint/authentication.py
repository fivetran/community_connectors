"""Configuration validation and OAuth2 password-grant authentication for Deltek Vantagepoint.

This module handles:
1. Configuration validation - ensures required credential fields are present
2. Token acquisition - obtains a bearer access token via Vantagepoint's OAuth2 password grant

Deliberately self-contained (does not import from rest_client.py): rest_client.py's
get_api_response() imports get_access_token() from here to refresh an expired token on a 401, so
this module must not import back from rest_client.py, or the two would form a circular import.
That's why the small retry/backoff logic below is written directly in this module instead of
sharing rest_client.py's retry helpers.
"""

# For sleeping between retry attempts
import time

# For making the OAuth2 token request
import requests

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# Retry/backoff/timeout limits shared with rest_client.py
from sync_limits import __BASE_DELAY_SECONDS, __MAX_RETRIES, __REQUEST_TIMEOUT_SECONDS


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has
    all necessary configuration values. Checks that each required value is present AND a
    non-blank string, not just that the key exists - a present-but-null or whitespace-only value
    (e.g. "base_url": null) would otherwise pass validation here and then fail later with a
    confusing AttributeError instead of this descriptive configuration error.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or blank.
    """
    required_configs = [
        "base_url",
        "username",
        "password",
        "database",
        "client_id",
        "client_secret",
    ]
    for key in required_configs:
        value = configuration.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Missing required configuration value: {key}")


def get_access_token(base_url, configuration):
    """
    Obtain an OAuth2 access token from Vantagepoint's token endpoint using the password grant.
    Args:
        base_url: The Vantagepoint API base URL.
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        str: The bearer access token for API authentication.
    Raises:
        RuntimeError: If authentication fails after retries.
    """
    url = f"{base_url}/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    # Field names/casing below match a confirmed working configuration against a live instance:
    # lowercase "client_id"/"integrated" (not "Client_Id"/"Integrated" as shown in the public API
    # docs), plus "culture", which the public docs mark optional but a live setup provided.
    body = {
        "grant_type": "password",
        "Username": configuration.get("username"),
        "Password": configuration.get("password"),
        "database": configuration.get("database"),
        "client_id": configuration.get("client_id"),
        "client_secret": configuration.get("client_secret"),
        "integrated": "N",
        "culture": configuration.get("culture") or "en-US",
    }

    for attempt in range(__MAX_RETRIES):
        try:
            response = requests.post(
                url, headers=headers, data=body, timeout=__REQUEST_TIMEOUT_SECONDS
            )

            if response.status_code == 200:
                access_token = response.json().get("access_token")
                if not access_token:
                    log.error("Access token missing from authentication response")
                    raise RuntimeError("Access token missing from authentication response")
                log.info("Successfully obtained Vantagepoint access token")
                return access_token

            if response.status_code in (401, 403):
                log.error("Authentication failed: invalid Vantagepoint credentials")
                raise RuntimeError(f"Authentication failed: {response.status_code}")

            if response.status_code in (429, 500, 502, 503, 504):
                _handle_retryable_error(attempt, response.status_code, "Authentication")
                continue

            log.error(f"Unexpected authentication error: {response.status_code}")
            raise RuntimeError(f"Authentication error: {response.status_code}")

        except requests.exceptions.Timeout:
            _handle_timeout_error(attempt, "Authentication")
            continue
        except requests.exceptions.RequestException as error:
            _handle_request_exception(attempt, "Authentication", error)
            continue

    raise RuntimeError("Authentication failed after maximum retry attempts")


def _handle_retryable_error(attempt, status_code, operation_name):
    """
    Sleep with exponential backoff for a retryable HTTP status code, or raise once retries are
    exhausted.
    Args:
        attempt: The current retry attempt number (0-indexed).
        status_code: The HTTP status code that triggered the retry.
        operation_name: A short description of the operation being retried, used in log messages.
    Raises:
        RuntimeError: If the maximum number of retry attempts has been reached.
    """
    if attempt < __MAX_RETRIES - 1:
        delay = __BASE_DELAY_SECONDS * (2**attempt)
        log.warning(
            f"{operation_name} failed with status {status_code}, retrying in {delay} seconds "
            f"(attempt {attempt + 1}/{__MAX_RETRIES})"
        )
        time.sleep(delay)
    else:
        log.error(f"{operation_name} failed after {__MAX_RETRIES} attempts. Status: {status_code}")
        raise RuntimeError(f"{operation_name} failed after {__MAX_RETRIES} attempts")


def _handle_timeout_error(attempt, operation_name):
    """
    Sleep with exponential backoff after a request timeout, or raise once retries are exhausted.
    Args:
        attempt: The current retry attempt number (0-indexed).
        operation_name: A short description of the operation being retried, used in log messages.
    Raises:
        RuntimeError: If the maximum number of retry attempts has been reached.
    """
    if attempt < __MAX_RETRIES - 1:
        delay = __BASE_DELAY_SECONDS * (2**attempt)
        log.warning(
            f"{operation_name} timed out, retrying in {delay} seconds "
            f"(attempt {attempt + 1}/{__MAX_RETRIES})"
        )
        time.sleep(delay)
    else:
        log.error(f"{operation_name} timed out after {__MAX_RETRIES} attempts")
        raise RuntimeError(f"{operation_name} timed out after {__MAX_RETRIES} attempts")


def _handle_request_exception(attempt, operation_name, error):
    """
    Sleep with exponential backoff after a generic request exception, or raise once retries are
    exhausted.
    Args:
        attempt: The current retry attempt number (0-indexed).
        operation_name: A short description of the operation being retried, used in log messages.
        error: The exception that was raised by the request.
    Raises:
        RuntimeError: If the maximum number of retry attempts has been reached.
    """
    if attempt < __MAX_RETRIES - 1:
        delay = __BASE_DELAY_SECONDS * (2**attempt)
        log.warning(
            f"{operation_name} failed, retrying in {delay} seconds (attempt {attempt + 1}/{__MAX_RETRIES})"
        )
        time.sleep(delay)
    else:
        log.error(f"{operation_name} failed after {__MAX_RETRIES} attempts")
        raise RuntimeError(f"{operation_name} failed: {str(error)}")
