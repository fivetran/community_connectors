# For making HTTP requests to the PI Web API
import time  # For sleep-based backoff between retry attempts
import requests

# For HTTP Basic authentication
from requests.auth import HTTPBasicAuth

# For enabling logs in the connector
from fivetran_connector_sdk import Logging as log

# Maximum retry attempts for transient server/network errors before raising
__MAX_RETRIES = 3


class PiApiError(Exception):
    """
    Raised by api_get() for non-retryable HTTP 4xx responses from PI Web API.

    Attributes:
        status_code: the HTTP status code returned by the server.
    """

    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code


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
    # Default to True (verification enabled). Only disable when the user explicitly sets
    # verify_ssl to "false"; any other value (including template placeholders) keeps TLS on.
    _verify_ssl = str(configuration.get("verify_ssl", "true"))
    session.verify = False if _verify_ssl.lower() == "false" else True
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

    Raises PiApiError immediately on most 4xx responses (auth failures, not-found).
    Treats 408 (Request Timeout) and 429 (Too Many Requests) as transient and retries
    them. Retries up to __MAX_RETRIES times on 5xx or network/connection errors using
    exponential backoff with a cap of 60 seconds.

    Args:
        session: an authenticated requests.Session.
        url: the full URL to GET.
        params: optional query parameters dict.
    Returns:
        Parsed JSON response body as a dict.
    Raises:
        PiApiError: on non-retryable 4xx HTTP responses; status_code carries the HTTP status.
        requests.exceptions.ConnectionError: after __MAX_RETRIES consecutive transient failures.
    """
    last_exc: Exception = RuntimeError("No request attempted")
    for attempt in range(1, __MAX_RETRIES + 1):
        try:
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code in (401, 403):
                raise PiApiError(
                    status_code=resp.status_code,
                    message=f"Authentication error ({resp.status_code}) for {url}: {resp.text[:200]}",
                )
            # 408 (Request Timeout) and 429 (Too Many Requests) are transient — retry them
            if resp.status_code in (408, 429):
                raise requests.exceptions.HTTPError(
                    f"Retryable HTTP {resp.status_code} for {url}", response=resp
                )
            if 400 <= resp.status_code < 500:
                raise PiApiError(
                    status_code=resp.status_code,
                    message=f"Client error ({resp.status_code}) for {url}: {resp.text[:200]}",
                )
            resp.raise_for_status()
            return resp.json()
        except PiApiError:
            # Auth / client errors — surface immediately, no retry
            raise
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            log.warning(f"Request attempt {attempt}/{__MAX_RETRIES} failed for {url}: {exc}")
            if attempt < __MAX_RETRIES:
                time.sleep(min(60, 2 ** (attempt - 1)))

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


def get_database_web_id(session: requests.Session, base: str, database_name: str | None) -> str:
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
    found_any_server = False
    for server in paginate(session, f"{base}/assetservers"):
        found_any_server = True
        server_web_id = server.get("WebId", "")
        if not server_web_id:
            continue
        for db in paginate(session, f"{base}/assetservers/{server_web_id}/assetdatabases"):
            db_name = db.get("Name", "")
            db_web_id = db.get("WebId", "")
            if not db_web_id:
                continue
            if database_name is None or db_name == database_name:
                log.info(f"Connected to database '{db_name}' on server '{server.get('Name', '')}'")
                return db_web_id

    if not found_any_server:
        raise ValueError("No PI Asset Servers found via PI Web API. Check base_url.")
    target = f"'{database_name}'" if database_name else "any database"
    raise ValueError(f"Could not find {target} on any PI Asset Server. Check database_name.")
