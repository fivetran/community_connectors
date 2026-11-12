"""HTTP client for the USDA AMS Livestock Mandatory Reporting (LMR) DataMart API.

The API needs no key. AMS asks callers not to send high-frequency automated requests and temporarily blocks
addresses that do, so every request waits at least the configured delay after the previous one. A request returns
at most 100,000 records and a date range may span at most 180 days.

The API answers HTTP 200 for a date range with no report, with the JSON string "No Results Found. " instead of an
object, and also answers HTTP 200 for a misspelled section, with a different string. Only the first means "no
rows"; any other string fails the sync.
"""

# For pacing requests and waiting between retries
import time

# For encoding the report id and section name into the URL path
from urllib.parse import quote

# For making HTTP requests to external APIs
import requests

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

__BASE_URL = "https://mpr.datamart.ams.usda.gov/services/v1.1/reports"
__API_TIMEOUT_SECONDS = 60
__USER_AGENT = "Fivetran-USDA-Boxed-Beef-Connector/1.0"

__MAX_RETRIES = 3
__BASE_DELAY_SECONDS = 2
__MAX_RETRY_AFTER_SECONDS = 120
__RETRYABLE_STATUS_CODES = [429, 500, 502, 503, 504]

# The body the API returns for a valid query that matches no report, such as a weekend or a federal holiday
__NO_RESULTS_MESSAGE = "No Results Found."

# The API returns US-format dates in a query, joined by a colon for a range
__QUERY_DATE_FORMAT = "%m/%d/%Y"

# Characters the API accepts unencoded in a section name; spaces and everything else are percent-encoded
__PATH_SAFE_CHARACTERS = ""


class UsdaApiError(RuntimeError):
    """The API returned something the connector cannot use, so the sync stops instead of losing data."""


def parse_retry_after(value):
    """
    Read a Retry-After header given in seconds.

    Args:
        value: the header value, or None

    Returns:
        The number of seconds when the value is a whole number of seconds, otherwise None (an HTTP date or any
        other form falls back to the caller's backoff)
    """
    if value is None:
        return None
    text = str(value).strip(" \t")
    if not text.isascii() or not text.isdigit():
        return None
    # Any value this long is past the cap, and int() refuses very long digit strings
    return int(text) if len(text) <= 18 else 10**18


def retry_delay(response, attempt: int) -> int:
    """
    Choose the wait before a retry: exponential backoff, or the Retry-After header when it asks for longer.

    Args:
        response: the response being retried
        attempt: zero-based attempt number

    Returns:
        Seconds to wait, capped so a large header cannot stall the sync
    """
    backoff = __BASE_DELAY_SECONDS * (2**attempt)
    headers = getattr(response, "headers", None) or {}
    retry_after = parse_retry_after(headers.get("Retry-After"))
    if retry_after is None:
        return backoff
    return min(max(retry_after, backoff), __MAX_RETRY_AFTER_SECONDS)


def _stat(stats: dict, name: str):
    """
    Read a row count from a response's stats block, whose keys carry a trailing colon ("totalRows:").

    Args:
        stats: the stats object
        name: the count's name without the colon

    Returns:
        The count as an int, or None when the API did not send it
    """
    value = stats.get(f"{name}:", stats.get(name))
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def rows_from_body(body, label: str) -> list:
    """
    Turn a decoded response body into the section's rows.

    Args:
        body: the decoded JSON body
        label: the report, section and range, for error messages

    Returns:
        The section's rows; an empty list for the API's documented no-results body or an empty results list

    Raises:
        UsdaApiError: for any other string, a body of the wrong shape, a result that is not a row, or a
            response the API truncated
    """
    if isinstance(body, str):
        if body.strip() == __NO_RESULTS_MESSAGE:
            return []
        raise UsdaApiError(
            f"{label}: the API returned a message instead of data: {body.strip()[:120]!r}"
        )
    if not isinstance(body, dict) or not isinstance(body.get("results"), list):
        raise UsdaApiError(f"{label}: the response is not a report section")
    stats = body.get("stats") if isinstance(body.get("stats"), dict) else {}
    total_rows = _stat(stats, "totalRows")
    returned_rows = _stat(stats, "returnedRows")
    if total_rows is not None and returned_rows is not None and returned_rows < total_rows:
        raise UsdaApiError(
            f"{label}: the API returned {returned_rows} of {total_rows} rows. Lower max_window_days."
        )
    if not all(isinstance(row, dict) for row in body["results"]):
        raise UsdaApiError(f"{label}: the response holds a result that is not a row")
    return body["results"]


def create_session():
    """
    Create the requests session every call of a sync shares.

    Returns:
        requests.Session with the connector's User-Agent and a JSON Accept header
    """
    session = requests.Session()
    session.headers.update({"User-Agent": __USER_AGENT, "Accept": "application/json"})
    return session


class UsdaClient:
    """A requests session plus the pacing state shared by every request of one sync."""

    def __init__(self, request_delay_seconds: float):
        """
        Open the session.

        Args:
            request_delay_seconds: the least time between the start of one request and the next
        """
        self.request_delay_seconds = request_delay_seconds
        self.request_count = 0
        self.last_request_at = None
        self.session = create_session()

    def close(self):
        """Close the session."""
        self.session.close()


def wait_for_turn(client: UsdaClient):
    """
    Sleep until the configured delay has passed since the previous request.

    Args:
        client: the client whose last request time to read and update
    """
    if client.last_request_at is not None:
        remaining = client.request_delay_seconds - (time.monotonic() - client.last_request_at)
        if remaining > 0:
            time.sleep(remaining)
    client.last_request_at = time.monotonic()


def fetch_section(
    client: UsdaClient, slug_id: str, section_name: str, field: str, start, end
) -> list:
    """
    Fetch one section of one report for a date range, both ends inclusive.

    Args:
        client: the paced session
        slug_id: the report id, such as "2453"
        section_name: the section, such as "Choice Cuts"
        field: the date field to filter on, report_date or published_date
        start: the first date of the range
        end: the last date of the range

    Returns:
        The section's rows for the range

    Raises:
        UsdaApiError: if the API keeps failing or returns something the connector cannot use
    """
    path = f"{quote(slug_id, safe=__PATH_SAFE_CHARACTERS)}/{quote(section_name, safe=__PATH_SAFE_CHARACTERS)}"
    query = f"{field}={start.strftime(__QUERY_DATE_FORMAT)}:{end.strftime(__QUERY_DATE_FORMAT)}"
    label = f"report {slug_id} {section_name} {field} {start.isoformat()} to {end.isoformat()}"
    return rows_from_body(
        get_json_with_retry(client, f"{__BASE_URL}/{path}", {"q": query}, label), label
    )


def get_json_with_retry(client: UsdaClient, url: str, params: dict, label: str):
    """
    GET a URL with pacing and exponential backoff, and decode the JSON body.

    Args:
        client: the paced session
        url: the request URL
        params: query parameters
        label: what is being fetched, for log and error messages

    Returns:
        The decoded JSON body

    Raises:
        UsdaApiError: if every attempt fails, the status is not retryable, or the body is not JSON
    """
    for attempt in range(__MAX_RETRIES):
        is_last_attempt = attempt == __MAX_RETRIES - 1
        wait_for_turn(client)
        client.request_count += 1
        try:
            response = client.session.get(url, params=params, timeout=__API_TIMEOUT_SECONDS)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if is_last_attempt:
                raise UsdaApiError(
                    f"{label}: {type(e).__name__} after {__MAX_RETRIES} attempts"
                ) from e
            delay_seconds = __BASE_DELAY_SECONDS * (2**attempt)
            log.warning(f"{label}: {type(e).__name__}, retrying in {delay_seconds}s")
            time.sleep(delay_seconds)
            continue
        except requests.exceptions.RequestException as e:
            raise UsdaApiError(f"{label}: request failed ({type(e).__name__})") from e

        status_code = response.status_code
        if status_code in __RETRYABLE_STATUS_CODES and not is_last_attempt:
            delay_seconds = retry_delay(response, attempt)
            log.warning(f"{label}: HTTP {status_code}, retrying in {delay_seconds}s")
            time.sleep(delay_seconds)
            continue
        if status_code >= 400:
            raise UsdaApiError(f"{label}: HTTP {status_code} after {attempt + 1} attempt(s)")
        try:
            return response.json()
        except ValueError as e:
            raise UsdaApiError(f"{label}: the response body is not JSON") from e

    raise UsdaApiError(f"{label}: no response after {__MAX_RETRIES} attempts")
