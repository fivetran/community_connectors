# For making HTTP requests to the PI Web API
import time  # For sleep-based backoff between retry attempts
import requests

# For HTTP Basic authentication
from requests.auth import HTTPBasicAuth

# For enabling logs in the connector
from fivetran_connector_sdk import Logging as log

# Maximum retry attempts for transient server/network errors before raising
__MAX_RETRIES = 3


def build_session(configuration: dict) -> requests.Session:
    """
    Create an authenticated requests.Session for PI Web API calls.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        A configured requests.Session with Basic auth and JSON accept headers.
    """
    session = requests.Session()
    session.auth = HTTPBasicAuth(configuration["username"], configuration["password"])
    session.headers.update(
        {
            "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest",
        }
    )
    # Allow users to disable TLS verification for self-signed PI Web API certificates.
    # Only enable verification when the value is explicitly "true".
    session.verify = str(configuration.get("verify_ssl", "true")).lower() == "true"
    return session


def base_url(configuration: dict) -> str:
    """
    Return the PI Web API base URL with any trailing slash removed.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        The base URL string.
    """
    return configuration["base_url"].rstrip("/")


def api_get(session: requests.Session, url: str, params: dict = None) -> dict:
    """
    GET a PI Web API endpoint and return the parsed JSON body.

    Raises ValueError immediately on 4xx responses (auth failures, not-found) —
    these are not worth retrying. Retries up to __MAX_RETRIES times on 5xx or
    network/connection errors using exponential backoff (honoring Retry-After when present).

    Args:
        session: an authenticated requests.Session.
        url: the full URL to GET.
        params: optional query parameters dict.
    Returns:
        Parsed JSON response body as a dict.
    Raises:
        ValueError: on 4xx HTTP responses.
        requests.exceptions.ConnectionError: after __MAX_RETRIES consecutive transient failures.
    last_exc: Exception = RuntimeError("No request attempted")
    for attempt in range(1, __MAX_RETRIES + 1):
        try:
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code in (401, 403):
                raise ValueError(
                    f"Authentication error ({resp.status_code}) for {url}: {resp.text[:200]}"
                )
            # 408 (Request Timeout) and 429 (Too Many Requests) are transient — retry them
            if resp.status_code in (408, 429):
                raise requests.exceptions.HTTPError(
                    f"Retryable HTTP {resp.status_code} for {url}", response=resp
                )
            if 400 <= resp.status_code < 500:
                raise ValueError(f"Client error ({resp.status_code}) for {url}: {resp.text[:200]}")
            resp.raise_for_status()
            return resp.json()
        except ValueError:
            # Auth / client errors — surface immediately, no retry
            raise
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            log.warning(f"Request attempt {attempt}/{__MAX_RETRIES} failed for {url}: {exc}")

    raise requests.exceptions.ConnectionError(
        f"Could not reach PI Web API after {__MAX_RETRIES} attempts. URL: {url}"
    ) from last_exc


def paginate(session: requests.Session, url: str, params: dict = None):
    """
    Yield every item from a paginated PI Web API response.

    PI Web API paginates via a 'Links.Next' URL embedded in each response body.
    Parameters are only sent with the first request; subsequent pages use the
    full Next URL returned by the API.

    Args:
        session: an authenticated requests.Session.
        url: the initial URL to GET.
        params: optional query parameters for the first request.
    """
    next_url = url
    next_params = params
    while next_url:
        body = api_get(session, next_url, next_params)
        for item in body.get("Items", []):
            yield item
        next_url = body.get("Links", {}).get("Next")
        next_params = None  # Parameters are already encoded in the Next URL


def get_database_web_id(session: requests.Session, base: str, database_name: str) -> str:
    """
    Find and return the WebId of the target AF database.

    Searches all asset servers visible to this PI Web API instance. If
    database_name is provided, returns the first database with that exact name.
    If database_name is None, returns the first database found on any server.

    Args:
        session: an authenticated requests.Session.
        base: the PI Web API base URL.
        database_name: target AF database name, or None to use the first found.
    Returns:
        The WebId string of the matching database.
    Raises:
        ValueError: if no matching database can be found.
    """
    servers = api_get(session, f"{base}/assetservers").get("Items", [])
    if not servers:
        raise ValueError("No PI Asset Servers found via PI Web API. Check base_url.")

    for server in servers:
        databases = api_get(session, f"{base}/assetservers/{server['WebId']}/assetdatabases").get(
            "Items", []
        )
        for db in databases:
            if database_name is None or db.get("Name") == database_name:
                log.info(f"Connected to database '{db['Name']}' on server '{server.get('Name')}'")
                return db["WebId"]

    target = f"'{database_name}'" if database_name else "any database"
    raise ValueError(f"Could not find {target} on any PI Asset Server. Check database_name.")
