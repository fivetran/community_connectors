"""
OAuth2 token refresh for the Oura Ring connector.

Oura's server-side flow issues a single-use refresh token: every refresh returns a new access
token and a new refresh token, and the old refresh token stops working. A deployed connection
writes the new refresh token back into its own configuration with the Fivetran REST API, so the
next sync reads it from there and no secret is ever stored in state. A local `fivetran debug` run
has no connection to update, so it keeps the rotated token in a file under the local files/
folder, which `fivetran deploy` never packages.
"""

# For checking the Fivetran API key's encoding
import base64

# For fingerprinting the configuration's refresh token, so a new consent replaces the local copy
import hashlib

# For reading and writing the local debug token file
import json

# For the deployment model and connection ID the SDK sets, and the local debug token file
import os

# For checking the short error code Oura returns from the token endpoint, and the connection ID
import re

# For writing the local debug token file atomically
import tempfile

# For waiting between token request retries
import time

# For making HTTP requests to the Oura token endpoint and the Fivetran REST API
import requests

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

__TOKEN_URL = "https://api.ouraring.com/oauth/token"
__TOKEN_TIMEOUT_SECONDS = 30
__TOKEN_MAX_RETRIES = 3
__TOKEN_BASE_DELAY_SECONDS = 2
__TOKEN_MAX_RETRY_AFTER_SECONDS = 300
# These usually mean Oura did not complete the request, so the refresh token is unspent. A gateway
# 502 or 504 can still arrive after a rotation; the next refresh then reports invalid_grant.
__TOKEN_RETRYABLE_STATUS_CODES = [429, 500, 502, 503, 504]
# OAuth error codes are short lowercase words; anything else in the body is not repeated in errors
__OAUTH_ERROR_CODE_PATTERN = re.compile(r"^[a-z_]{1,40}$")
# A Retry-After given in seconds; [0-9] matches ASCII digits only
__RETRY_AFTER_SECONDS_PATTERN = re.compile(r"[0-9]+")
__RETRY_AFTER_MAX_DIGITS = 18

__FIVETRAN_CONNECTIONS_URL = "https://api.fivetran.com/v1/connections"
__FIVETRAN_API_TIMEOUT_SECONDS = 30
__FIVETRAN_API_MAX_RETRIES = 3
__FIVETRAN_API_BASE_DELAY_SECONDS = 2
__FIVETRAN_API_MAX_RETRY_AFTER_SECONDS = 300
# Every call is a PATCH that sets fixed values, so a retry after any of these is safe
__FIVETRAN_API_RETRYABLE_STATUS_CODES = [429, 500, 502, 503, 504]
# Fivetran connection IDs are short words joined by underscores; anything else never reaches a URL
__CONNECTION_ID_PATTERN = re.compile(r"^[A-Za-z0-9_]{1,64}$")

# The SDK sets FIVETRAN_DEPLOYMENT_MODEL to local_debug during `fivetran debug`
__LOCAL_DEBUG_MODEL = "local_debug"
# The SDK keeps its local warehouse and state in files/, and `fivetran deploy` never packages that
# folder. It is not excluded from git unless the project's .gitignore lists it
__LOCAL_TOKEN_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "files", "oura_refresh_token.json"
)


class OuraAuthError(RuntimeError):
    """The token endpoint refused the refresh, so a new consent is needed."""

    def __init__(self, message: str, code=None):
        super().__init__(message)
        self.code = code


def fingerprint(token: str) -> str:
    """
    Return the SHA-256 hex digest of a token, so the local debug file can recognise the
    configured token without holding it.

    Args:
        token: the refresh token from the configuration

    Returns:
        64-character hex digest
    """
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def is_local_debug() -> bool:
    """
    Report whether the connector runs under `fivetran debug`, where there is no connection to
    update.

    Returns:
        True for a local debug run, False for a deployed connection
    """
    return os.environ.get("FIVETRAN_DEPLOYMENT_MODEL") == __LOCAL_DEBUG_MODEL


def check_fivetran_api_key(value) -> None:
    """
    Confirm the fivetran_api_key is the base64 encoding of <api_key>:<api_secret>, the value the
    Fivetran REST API takes in a Basic Authorization header.

    Args:
        value: the fivetran_api_key configuration value

    Raises:
        ValueError: if the value is not base64 or does not decode to a key and secret
    """
    message = "fivetran_api_key must be the base64 encoding of <api_key>:<api_secret>"
    if not isinstance(value, str):
        raise ValueError(message)
    try:
        decoded = base64.b64decode(value, validate=True).decode("utf-8")
    except (ValueError, UnicodeDecodeError) as e:
        raise ValueError(message) from e
    api_key, separator, api_secret = decoded.partition(":")
    if not (api_key and separator and api_secret):
        raise ValueError(message)


def read_local_refresh_token(configured_token: str):
    """
    Read the token a previous local debug run rotated, when it came from the configured token.

    Args:
        configured_token: the refresh_token from the configuration

    Returns:
        the rotated refresh token, or None when there is none or the configuration holds a new
        consent
    """
    if not os.path.exists(__LOCAL_TOKEN_PATH):
        return None
    try:
        with open(__LOCAL_TOKEN_PATH, "r") as f:
            saved = json.load(f)
    except (OSError, ValueError):
        log.warning("The local debug token file cannot be read, so the configured token is used")
        return None
    if not isinstance(saved, dict) or saved.get("source_sha256") != fingerprint(configured_token):
        log.info("The configuration holds a new refresh token, so it replaces the local one")
        return None
    token = saved.get("refresh_token")
    return token if isinstance(token, str) and token else None


def write_local_refresh_token(configured_token: str, new_token: str) -> None:
    """
    Keep the rotated token for the next local debug run, in a file only the user can read.

    Args:
        configured_token: the refresh_token from the configuration, recorded as a fingerprint
        new_token: the refresh token Oura returned
    """
    directory = os.path.dirname(__LOCAL_TOKEN_PATH)
    os.makedirs(directory, mode=0o700, exist_ok=True)
    saved = {"source_sha256": fingerprint(configured_token), "refresh_token": new_token}
    fd, temp_path = tempfile.mkstemp(dir=directory, prefix=".oura-", suffix=".tmp")
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump(saved, f)
        os.replace(temp_path, __LOCAL_TOKEN_PATH)
    finally:
        # After a successful replace the temporary file is gone; otherwise it is removed here
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def oauth_error_code(response):
    """
    Read the OAuth error code from a failed token response, without keeping the rest of the body.

    Args:
        response: the token endpoint response

    Returns:
        the error code when the body is JSON with a plain lowercase code, otherwise None
    """
    try:
        body = response.json()
    except ValueError:
        return None
    code = body.get("error") if isinstance(body, dict) else None
    if not isinstance(code, str):
        return None
    code = code.lower()
    return code if __OAUTH_ERROR_CODE_PATTERN.match(code) else None


def parse_retry_after(value):
    """
    Read a Retry-After header given in seconds.

    Args:
        value: the header value, or None

    Returns:
        the number of seconds when the value is ASCII digits with optional spaces or tabs around
        them, otherwise None (an HTTP date, a sign, a decimal or a non-ASCII digit all fall back to
        the caller's backoff). A value too long to be a real wait reads as the longest one.
    """
    if value is None:
        return None
    text = str(value).strip(" \t")
    if not __RETRY_AFTER_SECONDS_PATTERN.fullmatch(text):
        return None
    # Leading zeros carry no value, so they do not count toward the length guard
    text = text.lstrip("0") or "0"
    # int() refuses digit strings over 4,300 characters, and any value this long is past the cap
    return int(text) if len(text) <= __RETRY_AFTER_MAX_DIGITS else 10**__RETRY_AFTER_MAX_DIGITS


def retry_delay(response, attempt: int, base_seconds: int, cap_seconds: int) -> int:
    """
    Choose the wait before a retry: exponential backoff, or the Retry-After header when it asks
    for longer, capped. Shared by the data requests and the token request.

    Args:
        response: the response being retried
        attempt: zero-based attempt number
        base_seconds: backoff for the first retry
        cap_seconds: the longest wait a Retry-After header can set

    Returns:
        seconds to wait
    """
    backoff = base_seconds * (2**attempt)
    headers = getattr(response, "headers", None) or {}
    retry_after = parse_retry_after(headers.get("Retry-After"))
    if retry_after is None:
        return backoff
    return min(max(retry_after, backoff), cap_seconds)


def token_retry_delay(response, attempt: int) -> int:
    """
    Choose the wait before retrying the token request.

    Args:
        response: the token endpoint response
        attempt: zero-based attempt number

    Returns:
        seconds to wait
    """
    return retry_delay(
        response, attempt, __TOKEN_BASE_DELAY_SECONDS, __TOKEN_MAX_RETRY_AFTER_SECONDS
    )


def describe_token_failure(response) -> str:
    """
    Build an error message from a failed token response without repeating its body.

    Args:
        response: the token endpoint response

    Returns:
        message with the HTTP status and, when it is a plain OAuth code, the error code
    """
    code = oauth_error_code(response)
    suffix = f" ({code})" if code else ""
    return f"Oura token refresh failed with HTTP {response.status_code}{suffix}"


def post_refresh(configuration: dict, refresh_token: str, user_agent: str) -> dict:
    """
    Exchange a refresh token at the Oura token endpoint, retrying only failures Oura did not
    process.

    Args:
        configuration: connector configuration with client_id and client_secret
        refresh_token: the single-use refresh token to spend
        user_agent: the connector's User-Agent header

    Returns:
        the token endpoint's JSON body

    Raises:
        OuraAuthError: if Oura rejects the refresh token or the client credentials
        RuntimeError: if the endpoint stays unavailable or the response is malformed
    """
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": configuration["client_id"],
        "client_secret": configuration["client_secret"],
    }
    headers = {"User-Agent": user_agent, "Accept": "application/json"}
    for attempt in range(__TOKEN_MAX_RETRIES + 1):
        try:
            response = requests.post(
                __TOKEN_URL, data=payload, headers=headers, timeout=__TOKEN_TIMEOUT_SECONDS
            )
        except requests.exceptions.ConnectionError as e:
            # A connection error usually means the request never reached Oura, so it is retried
            if attempt == __TOKEN_MAX_RETRIES:
                raise RuntimeError("Oura token endpoint unreachable") from e
            time.sleep(__TOKEN_BASE_DELAY_SECONDS * (2**attempt))
            continue
        except requests.exceptions.RequestException as e:
            # A read timeout may come after Oura rotated the token, so a retry could only fail
            raise RuntimeError(
                f"Oura token request failed ({type(e).__name__}). If the next sync reports "
                "invalid_grant, complete the OAuth consent again and set the new refresh_token"
            ) from e

        if response.status_code == 200:
            try:
                body = response.json()
            except ValueError as e:
                raise RuntimeError("Oura token endpoint returned a non-JSON response") from e
            has_tokens = isinstance(body, dict) and body.get("access_token")
            if not has_tokens or not body.get("refresh_token"):
                raise RuntimeError("Oura token response is missing access_token or refresh_token")
            return body
        is_retryable = response.status_code in __TOKEN_RETRYABLE_STATUS_CODES
        if is_retryable and attempt < __TOKEN_MAX_RETRIES:
            delay_seconds = token_retry_delay(response, attempt)
            log.info(
                f"Oura token request returned HTTP {response.status_code}, "
                f"retrying in {delay_seconds}s"
            )
            time.sleep(delay_seconds)
            continue
        message = describe_token_failure(response)
        if response.status_code not in (400, 401):
            raise RuntimeError(message)
        code = oauth_error_code(response)
        if code is None:
            # Not an OAuth error body, so a proxy may have answered and Oura may not have seen it
            raise RuntimeError(
                f"{message}. The response is not an OAuth error, so it may not have come from "
                "Oura and the refresh token may be unspent. Retry the sync before completing "
                "the consent again"
            )
        if code == "invalid_grant":
            raise OuraAuthError(
                f"{message}. Oura reports the refresh token as already used or revoked. "
                "Complete the OAuth consent again and set the new refresh_token",
                code,
            )
        raise OuraAuthError(
            f"{message}. Check client_id and client_secret. If they are right, complete the "
            "OAuth consent again and set the new refresh_token",
            code,
        )
    raise RuntimeError("Oura token refresh did not complete")


def connection_url() -> str:
    """
    Build the Fivetran REST API URL of this connection from the ID the SDK sets.

    Returns:
        https://api.fivetran.com/v1/connections/<connection_id>

    Raises:
        RuntimeError: if FIVETRAN_CONNECTION_ID is missing or is not a plain connection ID
    """
    connection_id = os.environ.get("FIVETRAN_CONNECTION_ID") or ""
    if not __CONNECTION_ID_PATTERN.match(connection_id):
        raise RuntimeError(
            "FIVETRAN_CONNECTION_ID is missing or malformed, so the connector cannot update its "
            "configuration"
        )
    return f"{__FIVETRAN_CONNECTIONS_URL}/{connection_id}"


def fivetran_api_request(method: str, configuration: dict, payload=None):
    """
    Call the Fivetran REST API for this connection, retrying network failures and the statuses
    that mean Fivetran did not process the request. Every call this connector makes is a PATCH
    that sets fixed values, so sending it twice is safe.

    Args:
        method: the HTTP method, PATCH
        configuration: connector configuration with fivetran_api_key
        payload: the JSON body of a PATCH

    Returns:
        the final response, which may be an error status

    Raises:
        RuntimeError: if the Fivetran REST API stays unreachable
    """
    url = connection_url()
    headers = {
        "Accept": "application/json;version=2",
        "Authorization": f"Basic {configuration['fivetran_api_key']}",
        "Content-Type": "application/json",
    }
    for attempt in range(__FIVETRAN_API_MAX_RETRIES + 1):
        try:
            response = requests.request(
                method,
                url,
                headers=headers,
                json=payload,
                timeout=__FIVETRAN_API_TIMEOUT_SECONDS,
                allow_redirects=False,
            )
        except requests.exceptions.RequestException as e:
            if attempt == __FIVETRAN_API_MAX_RETRIES:
                raise RuntimeError(f"Fivetran REST API unreachable ({type(e).__name__})") from e
            time.sleep(__FIVETRAN_API_BASE_DELAY_SECONDS * (2**attempt))
            continue
        is_retryable = response.status_code in __FIVETRAN_API_RETRYABLE_STATUS_CODES
        if is_retryable and attempt < __FIVETRAN_API_MAX_RETRIES:
            delay_seconds = retry_delay(
                response,
                attempt,
                __FIVETRAN_API_BASE_DELAY_SECONDS,
                __FIVETRAN_API_MAX_RETRY_AFTER_SECONDS,
            )
            log.info(
                f"Fivetran REST API returned HTTP {response.status_code}, "
                f"retrying in {delay_seconds}s"
            )
            time.sleep(delay_seconds)
            continue
        return response
    raise RuntimeError("Fivetran REST API request did not complete")


def fivetran_api_succeeded(response) -> bool:
    """
    Report whether a Fivetran REST API response is a success Fivetran itself sent: a 2xx with
    the JSON body code "Success". A 2xx page from anything in between does not count.

    Args:
        response: the Fivetran REST API response

    Returns:
        True when the call succeeded
    """
    if not 200 <= response.status_code < 300:
        return False
    try:
        body = response.json()
    except ValueError:
        return False
    return isinstance(body, dict) and body.get("code") == "Success"


def configuration_payload(configuration: dict) -> dict:
    """
    Build the PATCH body that sets this connection's configuration. The PATCH replaces every
    configuration value, so the payload carries all of them.

    Args:
        configuration: the configuration values to set

    Returns:
        the JSON body for PATCH /v1/connections/<connection_id>
    """
    return {
        "config": {
            "secrets_list": [
                {"key": str(key), "value": str(value)}
                for key, value in configuration.items()
                if value is not None
            ]
        }
    }


def check_configuration_access(configuration: dict) -> None:
    """
    Confirm the Fivetran API key can edit this connection before the single-use Oura token is
    spent, by writing the current configuration back unchanged. A key that cannot edit the
    connection then fails the sync without costing a new consent.

    Args:
        configuration: connector configuration with fivetran_api_key

    Raises:
        RuntimeError: if the Fivetran REST API refuses the update or cannot be reached
    """
    not_spent = (
        "Check that fivetran_api_key is the base64 encoding of an API key and secret whose user "
        "can edit this connection. The Oura refresh token was not spent"
    )
    try:
        response = fivetran_api_request(
            "PATCH", configuration, configuration_payload(configuration)
        )
    except RuntimeError as e:
        raise RuntimeError(f"{e}. {not_spent}") from e
    if not fivetran_api_succeeded(response):
        raise RuntimeError(
            f"Fivetran REST API returned HTTP {response.status_code} without a success response "
            f"when the connector checked it can update this connection. {not_spent}"
        )


def save_refresh_token_to_configuration(configuration: dict, new_token: str) -> None:
    """
    Write the rotated refresh token into this connection's configuration with the Fivetran REST
    API, so the next sync reads it from there.

    Args:
        configuration: connector configuration, sent back with the new refresh_token
        new_token: the refresh token Oura returned

    Raises:
        RuntimeError: if the configuration could not be updated; Oura has already invalidated
            the configured token by then
    """
    payload = configuration_payload(dict(configuration, refresh_token=new_token))
    lost_token = (
        "Oura has already invalidated the configured refresh_token, so complete the OAuth "
        "consent again and set the new refresh_token"
    )
    try:
        response = fivetran_api_request("PATCH", configuration, payload)
    except RuntimeError as e:
        raise RuntimeError(f"{e} while saving the rotated refresh token. {lost_token}") from e
    if not fivetran_api_succeeded(response):
        raise RuntimeError(
            f"Fivetran REST API returned HTTP {response.status_code} without a success response "
            f"while saving the rotated refresh token. {lost_token}"
        )


def refresh_access_token(configuration: dict, user_agent: str) -> str:
    """
    Spend the current refresh token, save its replacement where the next sync reads it, and
    return the new access token. A deployed connection saves it to its own configuration through
    the Fivetran REST API; a local debug run saves it to the local token file.

    Args:
        configuration: connector configuration with client_id, client_secret, refresh_token,
            and fivetran_api_key for a deployed connection
        user_agent: the connector's User-Agent header

    Returns:
        an access token for the Oura API
    """
    configured_token = configuration["refresh_token"]
    local_debug = is_local_debug()
    if local_debug:
        refresh_token = read_local_refresh_token(configured_token) or configured_token
    else:
        check_configuration_access(configuration)
        refresh_token = configured_token

    body = post_refresh(configuration, refresh_token, user_agent)

    if local_debug:
        write_local_refresh_token(configured_token, body["refresh_token"])
        log.info("Saved the rotated refresh token to the local debug token file")
    else:
        save_refresh_token_to_configuration(configuration, body["refresh_token"])
        log.info("Saved the rotated refresh token to the connection configuration")
    log.info("Refreshed the Oura access token")
    return body["access_token"]
