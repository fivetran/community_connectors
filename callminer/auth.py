"""
Authentication and token management for CallMiner API.
"""

import requests
import time
from typing import Tuple, Callable
from datetime import datetime, timedelta
from functools import wraps
from fivetran_connector_sdk import Logging as log


def retry_on_500_error(max_retries: int = 3, initial_delay: int = 1, backoff_factor: int = 2):
    """
    Decorator to retry API calls on 500-level errors with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        initial_delay: Initial delay in seconds before first retry (default: 1)
        backoff_factor: Multiplier for exponential backoff (default: 2)

    Returns:
        Decorated function with retry logic
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)

                except requests.exceptions.HTTPError as e:
                    last_exception = e

                    # Check if it's a 500-level error
                    if e.response is not None and e.response.status_code >= 500:
                        if attempt < max_retries:
                            # Calculate delay with exponential backoff
                            delay = initial_delay * (backoff_factor**attempt)

                            log.warning(
                                f"HTTP {e.response.status_code} error in "
                                f"{func.__name__}. Retrying in {delay} seconds "
                                f"(attempt {attempt + 1}/{max_retries})..."
                            )

                            time.sleep(delay)
                            continue
                        else:
                            log.severe(
                                f"HTTP {e.response.status_code} error in "
                                f"{func.__name__}. Max retries "
                                f"({max_retries}) exceeded."
                            )

                    # Re-raise if not a 500 error or max retries exceeded
                    raise

                except requests.exceptions.RequestException as e:
                    # For non-HTTP errors (timeout, connection, etc), don't retry
                    log.severe(f"Request exception in {func.__name__}: {e}")
                    raise

            # If we get here, we've exhausted retries
            if last_exception:
                raise last_exception

        return wrapper

    return decorator


@retry_on_500_error(max_retries=3, initial_delay=1, backoff_factor=2)
def get_access_token(client_id: str, client_secret: str) -> Tuple[str, int]:
    """
    Get an access token using OAuth2 client credentials flow.

    Args:
        client_id: Client ID for authentication
        client_secret: Client secret for authentication

    Returns:
        Tuple of (access_token, expires_in_seconds)
    """
    url = "https://idp.callminer.net/connect/token"

    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
    }

    log.info("Requesting access token")

    try:
        response = requests.post(url, headers=headers, data=data, timeout=(30, 60), verify=True)
        response.raise_for_status()

        token_data = response.json()
        access_token = token_data.get("access_token")
        expires_in = token_data.get("expires_in", 3600)

        if not access_token:
            log.severe("No access_token in response")
            raise ValueError("Failed to obtain access token")

        log.info("Successfully obtained access token")
        return access_token, expires_in

    except requests.exceptions.RequestException as e:
        log.severe(f"Error obtaining access token: {e}")
        raise


def get_token(client_id: str, client_secret: str) -> Tuple[str, datetime]:
    """
    Get a fresh access token and its expiration time.

    Args:
        client_id: Client ID for authentication
        client_secret: Client secret for authentication

    Returns:
        Tuple of (bearer_token, expiration_datetime)
    """
    log.info("Requesting new access token")
    bearer_token, expires_in = get_access_token(client_id, client_secret)

    # Calculate expiration with 5 minute buffer
    expires_at = datetime.utcnow() + timedelta(seconds=expires_in - 300)
    log.info(f"Token expires at: {expires_at.strftime('%Y-%m-%dT%H:%M:%SZ')}")

    return bearer_token, expires_at


def refresh_token_if_needed(
    client_id: str, client_secret: str, current_token: str, expires_at: datetime
) -> Tuple[str, datetime]:
    """
    Check if token needs refresh and get new one if needed.

    Args:
        client_id: Client ID for authentication
        client_secret: Client secret for authentication
        current_token: Current bearer token
        expires_at: When current token expires

    Returns:
        Tuple of (bearer_token, expiration_datetime)
    """
    if datetime.utcnow() >= expires_at:
        log.info("Token expired, refreshing...")
        return get_token(client_id, client_secret)
    else:
        return current_token, expires_at
