"""This connector demonstrates how to sync Sopheon Accolade report data into the destination.
Each configured Accolade report is run through the REST API with $top/$skip paging and loaded into
its own table.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading the report list and configuration.json
import json

# For retry backoff
import time

# For percent-encoding the report name in the request URL
from urllib.parse import quote

# For issuing HTTP requests to the Accolade token and report endpoints
import requests

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

__DEFAULT_PAGE_SIZE = 10000
__DEFAULT_REQUEST_TIMEOUT_SECONDS = 300
__AUTH_TIMEOUT_SECONDS = 30
__RETRY_ATTEMPTS = 4
__MAX_BACKOFF_SECONDS = 60
__RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
__REQUIRED_KEYS = ("auth_url", "base_url", "refresh_token", "reports")
# Keys under which Accolade may wrap the list of rows in a report response.
__ROW_CONTAINER_KEYS = ("value", "results", "table", "data", "items")


def is_unset(value) -> bool:
    """Treat missing, empty, and '<PLACEHOLDER>' values as not configured."""
    return value in (None, "") or str(value).strip().startswith("<")


def is_valid_name(value) -> bool:
    """Return True for a non-empty string that is not an unreplaced '<PLACEHOLDER>'."""
    return isinstance(value, str) and not is_unset(value.strip())


def __int_setting(configuration: dict, key: str, default: int) -> int:
    """
    Read an optional positive integer from the configuration.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        key: name of the configuration value.
        default: value used when the key is missing or empty.
    Raises:
        ValueError: if the value is present but is not a positive integer.
    """
    raw = configuration.get(key)
    if is_unset(raw):
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise ValueError(f"{key} must be a positive integer")
    if value <= 0:
        raise ValueError(f"{key} must be a positive integer")
    return value


def get_reports(configuration: dict) -> list:
    """
    Parse the reports setting, a JSON list of {"report_name": ..., "primary_key": [...]} objects.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if the setting is not valid JSON or an entry is incomplete.
    """
    try:
        reports = json.loads(configuration["reports"])
    except json.JSONDecodeError as e:
        raise ValueError(f"reports must be a valid JSON list: {e}")
    if not isinstance(reports, list) or not reports:
        raise ValueError("reports must be a non-empty JSON list")
    for report in reports:
        if not isinstance(report, dict):
            raise ValueError("each report must be a JSON object")
        keys = report.get("primary_key")
        if not is_valid_name(report.get("report_name")):
            raise ValueError("each report needs a non-empty report_name that is not a placeholder")
        if not isinstance(keys, list) or not keys or not all(is_valid_name(k) for k in keys):
            raise ValueError("each report needs a primary_key list of non-empty column names")
    return reports


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if a required value is missing or invalid.
    """
    for key in __REQUIRED_KEYS:
        if is_unset(configuration.get(key)):
            raise ValueError(f"Missing required configuration value: {key}")
    get_reports(configuration)
    __int_setting(configuration, "page_size", __DEFAULT_PAGE_SIZE)
    __int_setting(configuration, "request_timeout_seconds", __DEFAULT_REQUEST_TIMEOUT_SECONDS)


def __verify(configuration: dict):
    """Return the TLS verification setting: the CA bundle path if set, otherwise True."""
    ca_bundle_path = configuration.get("ca_bundle_path")
    return True if is_unset(ca_bundle_path) else ca_bundle_path


def schema(configuration: dict):
    """
    Define the table schema for the connector.
    Report columns are defined in Accolade, so only the table name and primary key are declared.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        A list of table definitions, one per configured report.
    """
    return [
        {"table": report["report_name"], "primary_key": report["primary_key"]}
        for report in get_reports(configuration)
    ]


def request_with_retry(method: str, url: str, request_timeout_seconds: int, **kwargs):
    """
    Send an HTTP request, retrying timeouts, connection errors, HTTP 429 and 5xx with
    exponential backoff. Any other response, including 4xx errors, is returned to the caller.
    Args:
        method: HTTP method.
        url: request URL.
        request_timeout_seconds: per-request timeout.
    Raises:
        RuntimeError: if every attempt fails.
    """
    for attempt in range(1, __RETRY_ATTEMPTS + 1):
        try:
            response = requests.request(method, url, timeout=request_timeout_seconds, **kwargs)
            if response.status_code not in __RETRY_STATUS_CODES:
                return response
            error = f"HTTP {response.status_code}"
        except (requests.Timeout, requests.ConnectionError) as e:
            error = type(e).__name__
        if attempt == __RETRY_ATTEMPTS:
            raise RuntimeError(f"Request failed after {__RETRY_ATTEMPTS} attempts: {error}")
        delay_seconds = min(__MAX_BACKOFF_SECONDS, 2**attempt)
        log.warning(f"{error}; retry {attempt}/{__RETRY_ATTEMPTS - 1} in {delay_seconds}s")
        time.sleep(delay_seconds)


def authenticate(configuration: dict) -> str:
    """
    Exchange the refresh token for an access token. Transient failures are retried, while
    permanent authentication errors such as HTTP 400 or 401 fail immediately.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        The access token.
    Raises:
        requests.HTTPError: on a permanent HTTP error.
        RuntimeError: if retries are exhausted or the response has no access token.
    """
    response = request_with_retry(
        "POST",
        configuration["auth_url"],
        __AUTH_TIMEOUT_SECONDS,
        data={"grant_type": "refresh_token", "refresh_token": configuration["refresh_token"]},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        verify=__verify(configuration),
    )
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
        raise RuntimeError("Authentication succeeded but no access_token was returned")
    return token


def build_headers(configuration: dict) -> dict:
    """Authenticate and return the request headers that carry the bearer token."""
    return {
        "Authorization": f"Bearer {authenticate(configuration)}",
        "Accept": "application/json",
    }


def get_page(url: str, headers: dict, params: dict, configuration: dict):
    """
    Fetch one page of a report, retrying transient failures with exponential backoff.
    Args:
        url: report endpoint URL.
        headers: request headers with the bearer token.
        params: $top and $skip paging parameters.
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        The parsed JSON response.
    Raises:
        RuntimeError: if every attempt fails.
        requests.HTTPError: on a non-retryable HTTP error.
    """
    request_timeout_seconds = __int_setting(
        configuration, "request_timeout_seconds", __DEFAULT_REQUEST_TIMEOUT_SECONDS
    )
    response = request_with_retry(
        "GET",
        url,
        request_timeout_seconds,
        headers=headers,
        params=params,
        verify=__verify(configuration),
    )
    response.raise_for_status()
    return response.json()


def get_page_with_renewal(url: str, headers: dict, params: dict, configuration: dict):
    """
    Fetch one page and, if the token was rejected with HTTP 401, renew it once and retry.
    The headers dictionary is updated in place, so later pages and reports use the new token.
    A second 401 is treated as a real authorization failure and raised.
    Args:
        url: report endpoint URL.
        headers: request headers with the bearer token. Updated in place on renewal.
        params: $top and $skip paging parameters.
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    try:
        return get_page(url, headers, params, configuration)
    except requests.HTTPError as e:
        if e.response is None or e.response.status_code != 401:
            raise
    log.warning("Access token was rejected; renewing it and retrying the page once")
    headers.update(build_headers(configuration))
    return get_page(url, headers, params, configuration)


def build_report_url(base_url: str, report_name: str) -> str:
    """
    Build the RunReport endpoint URL. Apostrophes are doubled inside the OData string literal
    and the name is percent-encoded, so characters such as '#' or '/' are sent intact.
    Args:
        base_url: Accolade API base URL.
        report_name: name of the report.
    """
    literal = quote(report_name.replace("'", "''"), safe="")
    return f"{base_url}/core/RunReport('{literal}')"


def extract_rows(payload, report_name: str) -> list:
    """
    Pull the list of rows out of a report response, which may be a list or a wrapper object.
    Args:
        payload: parsed JSON response.
        report_name: report name, used in error messages.
    Raises:
        RuntimeError: if the payload type is not supported.
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in __ROW_CONTAINER_KEYS:
            if key in payload:
                rows = payload[key]
                return rows if isinstance(rows, list) else [rows]
        return [payload]
    raise RuntimeError(f"Unexpected response type for report {report_name}: {type(payload)}")


def sync_report(report: dict, base_url: str, headers: dict, configuration: dict, state: dict):
    """
    Page through one report with $top/$skip and upsert every row.
    Paging stops when a page returns fewer rows than the page size.
    Args:
        report: report definition with report_name and primary_key.
        base_url: Accolade API base URL.
        headers: request headers with the bearer token.
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
    """
    report_name = report["report_name"]
    page_size = __int_setting(configuration, "page_size", __DEFAULT_PAGE_SIZE)
    url = build_report_url(base_url, report_name)
    params = {"$top": page_size, "$skip": 0}
    total = 0

    while True:
        log.info(f"Report {report_name}: fetching rows from offset {params['$skip']}")
        page = get_page_with_renewal(url, headers, params, configuration)
        rows = extract_rows(page, report_name)
        for row in rows:
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table=report_name, data=row)
        total += len(rows)

        # Save the progress by checkpointing the state. This is important for ensuring that
        # the sync process can resume after the last committed batch in case of interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state)

        if len(rows) < page_size:
            break
        params["$skip"] += page_size
    log.info(f"Report {report_name}: loaded {total} rows")


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
    """
    log.warning("Example: Source Examples : Sopheon Accolade")

    validate_configuration(configuration)
    headers = build_headers(configuration)
    base_url = configuration["base_url"].rstrip("/")
    log.info("Authenticated with Accolade")

    for report in get_reports(configuration):
        log.info(f"Processing report {report['report_name']}")
        sync_report(report, base_url, headers, configuration, state)


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
