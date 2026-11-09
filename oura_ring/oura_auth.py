"""
OAuth2 token refresh for the Oura Ring connector.

Oura's server-side flow issues a single-use refresh token: every refresh returns a new access
token and a new refresh token, and the old refresh token stops working. The connector therefore
keeps the newest refresh token in state, encrypted with the Fernet key from the configuration, and
checkpoints it before syncing any data. The configuration's refresh_token only seeds the first
sync, or a new consent.
"""

# For fingerprinting the configuration's refresh token, so a new consent replaces the state copy
import hashlib

# For checking the short error code Oura returns from the token endpoint
import re

# For waiting between token request retries
import time

# For making HTTP requests to the Oura token endpoint
import requests

# For encrypting the rotated refresh token before it is written to state
from cryptography.fernet import Fernet, InvalidToken

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

__TOKEN_URL = "https://api.ouraring.com/oauth/token"
__TOKEN_TIMEOUT_SECONDS = 30
__TOKEN_MAX_RETRIES = 3
__TOKEN_BASE_DELAY_SECONDS = 2
# These usually mean Oura did not complete the request, so the refresh token is unspent. A gateway
# 502 or 504 can still arrive after a rotation; the next refresh then reports invalid_grant.
__TOKEN_RETRYABLE_STATUS_CODES = [429, 500, 502, 503, 504]
# OAuth error codes are short lowercase words; anything else in the body is not repeated in errors
__OAUTH_ERROR_CODE_PATTERN = re.compile(r"^[a-z_]{1,40}$")

STATE_TOKEN_KEY = "encrypted_refresh_token"
STATE_SOURCE_KEY = "refresh_token_source_sha256"


class OuraAuthError(RuntimeError):
    """The token endpoint refused the refresh, so a new consent is needed."""


def fingerprint(token: str) -> str:
    """
    Return the SHA-256 hex digest of a token, so state can recognise it without holding it.

    Args:
        token: the refresh token from the configuration

    Returns:
        64-character hex digest
    """
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def check_fernet_key(key) -> None:
    """
    Confirm the configuration's fernet_key is a valid Fernet key.

    Args:
        key: the fernet_key configuration value

    Raises:
        ValueError: if the key is not 32 url-safe base64-encoded bytes
    """
    try:
        Fernet(key)
    except (TypeError, ValueError) as e:
        raise ValueError(
            "fernet_key must be a Fernet key (32 url-safe base64-encoded bytes)"
        ) from e


def select_refresh_token(configuration: dict, state: dict) -> str:
    """
    Choose the refresh token to spend: the rotated one in state, unless the configuration holds a
    newer consent or the state copy cannot be decrypted.

    Args:
        configuration: connector configuration with refresh_token and fernet_key
        state: connector state from the previous sync

    Returns:
        the refresh token to exchange
    """
    configured_token = configuration["refresh_token"]
    encrypted_token = state.get(STATE_TOKEN_KEY)
    if not encrypted_token:
        return configured_token
    if state.get(STATE_SOURCE_KEY) != fingerprint(configured_token):
        log.info("The configuration holds a new refresh token, so it replaces the one in state")
        return configured_token
    try:
        return Fernet(configuration["fernet_key"]).decrypt(encrypted_token.encode()).decode()
    except InvalidToken:
        log.warning(
            "The refresh token in state cannot be decrypted with this fernet_key; "
            "using the configuration's refresh token"
        )
        return configured_token


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
    if isinstance(code, str) and __OAUTH_ERROR_CODE_PATTERN.match(code):
        return code
    return None


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
            time.sleep(__TOKEN_BASE_DELAY_SECONDS * (2**attempt))
            continue
        message = describe_token_failure(response)
        if response.status_code not in (400, 401):
            raise RuntimeError(message)
        if oauth_error_code(response) == "invalid_grant":
            raise OuraAuthError(
                f"{message}. Oura reports the refresh token as already used or revoked. "
                "Complete the OAuth consent again and set the new refresh_token"
            )
        raise OuraAuthError(
            f"{message}. Check client_id and client_secret. If they are right, complete the "
            "OAuth consent again and set the new refresh_token"
        )
    raise RuntimeError("Oura token refresh did not complete")


def refresh_access_token(configuration: dict, state: dict, user_agent: str) -> str:
    """
    Spend the current refresh token, store its replacement encrypted in state, and return the new
    access token. The caller must checkpoint state before any other API call.

    Args:
        configuration: connector configuration with client_id, client_secret, refresh_token
            and fernet_key
        state: connector state, updated in place with the encrypted replacement token
        user_agent: the connector's User-Agent header

    Returns:
        an access token for the Oura API
    """
    state_had_token = bool(state.get(STATE_TOKEN_KEY))
    refresh_token = select_refresh_token(configuration, state)
    try:
        body = post_refresh(configuration, refresh_token, user_agent)
    except OuraAuthError as e:
        if state_had_token:
            raise
        raise OuraAuthError(
            f"{e}. State held no saved token (a first sync, a full re-sync or a state reset), so "
            "the configured refresh_token was used, and it is spent once any earlier sync or "
            "local debug run has used it"
        ) from e
    fernet = Fernet(configuration["fernet_key"])
    state[STATE_TOKEN_KEY] = fernet.encrypt(body["refresh_token"].encode()).decode()
    state[STATE_SOURCE_KEY] = fingerprint(configuration["refresh_token"])
    log.info("Refreshed the Oura access token")
    return body["access_token"]
