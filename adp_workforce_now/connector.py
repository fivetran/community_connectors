"""This connector demonstrates syncing the ADP Workforce Now departments validation table from
the ADP Workforce Now HCM API, authenticating with OAuth2 client credentials over mutual TLS.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For serializing raw department records into the raw_json column
import json

# For setting restrictive permissions on temporary certificate/key files
import os

# For sleeping between retry attempts
import time

# For checking that the timeout is a finite number
import math

# For writing inline PEM strings to temporary files that `requests` can use for mTLS
import tempfile

# For normalizing inline PEM strings to properly wrapped PEM format
import textwrap

# For type hints
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

# For resolving relative pagination links returned by ADP against the current request URL
from urllib.parse import urljoin

# For issuing HTTP requests to the ADP Workforce Now API
import requests

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Default endpoints for ADP Workforce Now validation tables; override via configuration if needed
__DEFAULT_TOKEN_URL = "https://api.adp.com/auth/oauth/v2/token"
__DEFAULT_DEPARTMENTS_URL = "https://api.adp.com/hcm/v1/validation-tables/departments"
__DEFAULT_TIMEOUT = 30  # seconds
__RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
__MAX_ATTEMPTS = 4
__MAX_BACKOFF_SECONDS = 60  # cap applied only to locally computed exponential backoff
__MAX_RETRY_AFTER_SECONDS = 300  # fail rather than retry early if the server asks for longer


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or invalid.
    """
    required_configs = ["client_id", "client_secret"]
    for key in required_configs:
        if not configuration.get(key):
            raise ValueError(f"Missing required configuration value: '{key}'")

    has_paths = configuration.get("cert_path") and configuration.get("key_path")
    has_strings = configuration.get("cert_string") and configuration.get("key_string")
    if not (has_paths or has_strings):
        raise ValueError(
            "Provide either cert_path/key_path or cert_string/key_string in configuration."
        )

    # Validate numeric options up front, before any certificate files are created
    _validate_int_option(configuration, "page_size", 1000, 1)
    _validate_int_option(configuration, "start_skip", 0, 0)
    _get_timeout(configuration)


def _validate_int_option(configuration: dict, key: str, default: int, minimum: int) -> int:
    """Return an integer option from configuration, raising ValueError if it is invalid."""
    raw = configuration.get(key, default)
    if isinstance(raw, bool):
        raise ValueError(f"Invalid {key} value: {raw!r}")
    try:
        value = int(raw)
        if isinstance(raw, float) and raw != value:
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {key} value: {raw!r}; must be an integer") from exc
    if value < minimum:
        raise ValueError(f"Invalid {key} value: {value}; must be >= {minimum}")
    return value


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "departments",
            "primary_key": ["department_code", "payroll_group_code"],
        },
    ]


def _safe_get(payload: Dict[str, Any], path: List[str]) -> Optional[Any]:
    """Traverse a nested dict safely, returning None if any key along the path is missing."""
    current: Any = payload
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def _first_truthy(*values: Any) -> Optional[Any]:
    """Return the first truthy value among the arguments, or None if all are falsy."""
    for value in values:
        if value:
            return value
    return None


def _parse_retry_after(value: Optional[str]) -> Optional[float]:
    """Parse a Retry-After header as delay-seconds. The server-provided delay is not capped here.

    Args:
        value: the raw Retry-After header value, or None if absent.

    Returns:
        The delay in seconds to wait, or None if the header is absent or not a plain
        delay-seconds value (ADP's APIs use this form rather than an HTTP-date).
    """
    if not value:
        return None
    stripped = value.strip()
    if stripped.isdigit():
        return float(stripped)
    return None


def _request_with_backoff(
    method: str, url: str, cert: Tuple[str, str], timeout: Optional[float], **kwargs
) -> requests.Response:
    """Issue an HTTP request against ADP, retrying transient failures with exponential backoff.

    Retries (up to __MAX_ATTEMPTS): connection errors, timeouts, and HTTP 429/500/502/503/504
    (honoring Retry-After when present). Any other response, including a permanent client
    error, is returned as-is for the caller to inspect or raise on.

    Args:
        method: the HTTP method to use.
        url: the absolute URL to request.
        cert: the (cert_path, key_path) tuple used for mutual TLS.
        timeout: the request timeout in seconds, or None for no timeout.
        **kwargs: additional keyword arguments forwarded to requests.request().

    Returns:
        The requests.Response object for the final attempt.

    Raises:
        RuntimeError: if every attempt fails with a connection error or timeout.
    """
    last_exception: Optional[Exception] = None
    for attempt in range(1, __MAX_ATTEMPTS + 1):
        try:
            response = requests.request(method, url, cert=cert, timeout=timeout, **kwargs)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            last_exception = exc
            if attempt >= __MAX_ATTEMPTS:
                raise RuntimeError(
                    f"{method} {url} failed after {__MAX_ATTEMPTS} attempts: {exc}"
                ) from exc
            log.warning(
                f"{method} {url} attempt {attempt}/{__MAX_ATTEMPTS} failed ({exc}), retrying"
            )
            time.sleep(min(__MAX_BACKOFF_SECONDS, 2**attempt))
            continue

        if response.status_code in __RETRY_STATUS_CODES and attempt < __MAX_ATTEMPTS:
            wait_seconds = _parse_retry_after(response.headers.get("Retry-After"))
            if wait_seconds is None:
                wait_seconds = min(__MAX_BACKOFF_SECONDS, 2**attempt)
            elif wait_seconds > __MAX_RETRY_AFTER_SECONDS:
                raise RuntimeError(
                    f"{method} {url} returned HTTP {response.status_code} with Retry-After "
                    f"{wait_seconds:.0f}s, which exceeds the maximum of "
                    f"{__MAX_RETRY_AFTER_SECONDS}s; try again later"
                )
            log.warning(
                f"{method} {url} got HTTP {response.status_code} on attempt "
                f"{attempt}/{__MAX_ATTEMPTS}; retrying in {wait_seconds:.0f}s"
            )
            time.sleep(wait_seconds)
            continue

        return response

    raise RuntimeError(f"{method} {url} failed after {__MAX_ATTEMPTS} attempts: {last_exception}")


def get_access_token(configuration: dict, cert: Tuple[str, str]) -> str:
    """Request an OAuth2 access token from ADP using client credentials and mutual TLS.

    Args:
        configuration: the validated connector configuration.
        cert: the (cert_path, key_path) tuple used for mutual TLS.

    Returns:
        str: the access token.

    Raises:
        RuntimeError: if the request fails, or the response has no access_token.
    """
    token_url = configuration.get("token_url", __DEFAULT_TOKEN_URL)
    timeout = _get_timeout(configuration)

    payload = {
        "grant_type": "client_credentials",
        "client_id": configuration["client_id"],
        "client_secret": configuration["client_secret"],
    }

    try:
        response = _request_with_backoff(
            "POST",
            token_url,
            cert,
            timeout,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=payload,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to fetch access token: {exc}") from exc

    token = response.json().get("access_token")
    if not token:
        raise RuntimeError("Access token missing from response.")
    return token


def _resolve_next_link(payload: Any, current_url: str) -> Optional[str]:
    """Extract the next page link if provided by ADP; non-dict payloads have none."""
    if not isinstance(payload, dict):
        return None
    links = payload.get("links") or payload.get("_links")
    next_href: Optional[str] = None

    if isinstance(links, list):
        for link in links:
            if isinstance(link, dict) and link.get("rel") == "next" and link.get("href"):
                next_href = link["href"]
                break
    elif isinstance(links, dict):
        # Support both {"next": {"href": "..."}} and {"next": "..."}
        if isinstance(links.get("next"), dict):
            next_href = links["next"].get("href")
        elif links.get("next"):
            next_href = links.get("next")
        elif isinstance(links.get("nextPage"), dict):
            next_href = links["nextPage"].get("href")

    if next_href:
        return urljoin(current_url, next_href)
    return None


def _extract_departments(payload: Any) -> List[Dict[str, Any]]:
    """
    Pull department records from various possible ADP response shapes.
    Falls back to returning the payload as a single record if no obvious list is found.
    """
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if not isinstance(payload, dict):
        return []

    candidates = [
        payload.get("departments"),
        payload.get("items"),
        payload.get("content"),
    ]

    data_obj = payload.get("data")
    if isinstance(data_obj, dict):
        candidates.extend([data_obj.get("departments"), data_obj.get("items")])
    elif isinstance(data_obj, list):
        candidates.append(data_obj)

    for candidate in candidates:
        if isinstance(candidate, list):
            return [item for item in candidate if isinstance(item, dict)]

    # Fallback: search for the first list of dicts in values
    if isinstance(payload, dict):
        for value in payload.values():
            if isinstance(value, list) and any(isinstance(item, dict) for item in value):
                return [item for item in value if isinstance(item, dict)]

    # Fallback to single record if nothing else matches
    return [payload] if payload else []


def _materialize_cert_files(configuration: dict) -> Tuple[Tuple[str, str], Callable[[], None]]:
    """
    Resolve the client certificate/key from either file paths or inline PEM strings.

    Args:
        configuration: the validated connector configuration.

    Returns:
        A ((cert_path, key_path), cleanup) tuple. cleanup() removes any temporary files
        created for inline PEM strings (logging any removal failure); it is a no-op when
        cert_path/key_path were used. The caller must invoke it when finished. If setup fails,
        any files already created are removed before the exception propagates.
    """
    has_strings = configuration.get("cert_string") and configuration.get("key_string")
    has_paths = configuration.get("cert_path") and configuration.get("key_path")

    if has_strings:
        created: List[str] = []

        def _cleanup():
            for path in created:
                try:
                    os.remove(path)
                except FileNotFoundError:
                    pass
                except OSError as exc:
                    log.warning(f"Failed to remove temporary file {path}: {exc}")
            created.clear()

        try:
            paths = []
            for content, suffix in (
                (configuration["cert_string"], ".pem"),
                (configuration["key_string"], ".key"),
            ):
                normalized = _normalize_pem(content)
                # mkstemp creates the file with 0600 permissions
                fd, path = tempfile.mkstemp(suffix=suffix)
                created.append(path)
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    handle.write(normalized)
                os.chmod(path, 0o600)
                paths.append(path)
        except BaseException:
            _cleanup()
            raise

        return (paths[0], paths[1]), _cleanup

    if has_paths:

        def _noop():
            return None

        return (configuration["cert_path"], configuration["key_path"]), _noop

    raise ValueError("No certificate configuration found.")


def _normalize_pem(pem_text: str) -> str:
    """Normalize a PEM string to properly line-wrapped PEM with LF line endings."""
    if not pem_text:
        raise ValueError("Empty PEM data provided.")
    cleaned = textwrap.dedent(pem_text).replace("\r", "").strip()
    if "-----BEGIN" not in cleaned or "-----END" not in cleaned:
        raise ValueError("Invalid PEM format.")

    blocks: List[str] = []
    begin: Optional[str] = None
    body: List[str] = []
    for raw_line in cleaned.splitlines():
        line = raw_line.strip()
        if line.startswith("-----BEGIN"):
            if begin is not None:
                raise ValueError("PEM block missing END marker.")
            begin, body = line, []
        elif line.startswith("-----END"):
            if begin is None:
                raise ValueError("PEM END marker without BEGIN marker.")
            wrapped_body = "\n".join(textwrap.wrap("".join(body), 64))
            blocks.append(f"{begin}\n{wrapped_body}\n{line}\n")
            begin = None
        elif begin is not None:
            body.append(line)
    if begin is not None or not blocks:
        raise ValueError("PEM missing BEGIN/END markers.")
    return "".join(blocks)


def _get_timeout(configuration: dict) -> Optional[float]:
    """Normalize the configured request_timeout to a float, or None to disable it."""
    timeout = configuration.get("request_timeout", __DEFAULT_TIMEOUT)
    if timeout is None or timeout == "":
        return None
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float, str)):
        raise ValueError(f"Unsupported request_timeout type: {type(timeout)}")
    try:
        value = float(timeout)
    except ValueError as exc:
        raise ValueError(f"Invalid request_timeout value: {timeout}") from exc
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"Invalid request_timeout value: {timeout}; must be finite and > 0")
    return value


def fetch_departments(
    configuration: dict, access_token: str, cert: Tuple[str, str]
) -> Iterable[List[Dict[str, Any]]]:
    """Stream departments from ADP page by page, following pagination until exhausted.

    Args:
        configuration: the validated connector configuration.
        access_token: the current OAuth2 access token.
        cert: the (cert_path, key_path) tuple used for mutual TLS.

    Yields:
        A list of the raw department records from each page returned by the API.

    Raises:
        RuntimeError: if a page request fails.
    """
    departments_url = configuration.get("departments_url", __DEFAULT_DEPARTMENTS_URL)
    start_skip = _validate_int_option(configuration, "start_skip", 0, 0)
    page_size = _validate_int_option(configuration, "page_size", 1000, 1)
    timeout = _get_timeout(configuration)
    headers = {
        "Authorization": f"Bearer {access_token}",
        "accept": "application/json",
        "content-type": "application/json",
    }

    next_url: Optional[str] = departments_url
    params: Optional[Dict[str, Any]] = {"$skip": start_skip, "$top": page_size}
    while next_url:
        try:
            response = _request_with_backoff(
                "GET", next_url, cert, timeout, headers=headers, params=params
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise RuntimeError(f"Failed to fetch departments: {exc}") from exc

        payload = response.json()
        records = _extract_departments(payload)
        yield records

        # Prefer server-provided pagination link; otherwise, fall back to OData $skip/$top.
        next_link = _resolve_next_link(payload, next_url)
        if next_link:
            next_url = next_link
            params = None  # next link already includes query params
            continue

        if params is None:
            # Already following server links; a missing next link means we are done, and the
            # stale local offset must not be used.
            next_url = None
        elif records and len(records) == page_size:
            start_skip += page_size
            next_url = departments_url
            params = {"$skip": start_skip, "$top": page_size}
        else:
            next_url = None


def _flatten_department(record: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a department record into the target schema.

    Args:
        record: a single raw department record as returned by the ADP API.

    Returns:
        A flat dict matching the departments table schema, plus the original record
        serialized in raw_json for fields not explicitly mapped above.
    """
    department_code = _first_truthy(
        _safe_get(record, ["departmentCode", "codeValue"]),
        _safe_get(record, ["code"]),
        _safe_get(record, ["nameCode", "code"]),
        _safe_get(record, ["id"]),
        _safe_get(record, ["itemID", "idValue"]),
    )
    short_name = _first_truthy(
        _safe_get(record, ["nameCode", "shortName"]),
        _safe_get(record, ["nameCode", "name"]),
        record.get("shortName"),
    )
    long_name = _first_truthy(
        _safe_get(record, ["nameCode", "longName"]),
        _safe_get(record, ["nameCode", "name"]),
        record.get("longName"),
        record.get("description"),
    )
    # The inactiveIndicator-derived fallback always produces a truthy value, so it must come
    # after statusCode to avoid permanently shadowing it.
    status = _first_truthy(
        record.get("status"),
        _safe_get(record, ["statusCode", "codeValue"]),
        record.get("statusCode"),
        "INACTIVE" if record.get("inactiveIndicator") else "ACTIVE",
    )
    updated_at = _first_truthy(
        _safe_get(record, ["metadata", "lastUpdateDateTime"]),
        record.get("lastUpdateDateTime"),
        record.get("lastUpdated"),
    )
    # payroll_group_code is part of the table's composite primary key alongside
    # department_code, since ADP scopes department codes per payroll group.
    payroll_group_code = record.get("payrollGroupCode", "").strip()

    return {
        "department_code": department_code,
        "payroll_group_code": payroll_group_code,
        "short_name": short_name,
        "long_name": long_name,
        "status": status,
        "updated_at": updated_at,
        "raw_json": json.dumps(record, separators=(",", ":")),
    }


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    validate_configuration(configuration=configuration)
    cert, cleanup = _materialize_cert_files(configuration)
    try:
        access_token = get_access_token(configuration, cert)

        upserted = 0
        # Departments is effectively a reference table; we fetch all rows each sync.
        for page in fetch_departments(configuration, access_token, cert):
            for record in page:
                flattened = _flatten_department(record)
                if not flattened["department_code"]:
                    log.warning(f"Skipping department without code: {record}")
                    continue
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(table="departments", data=flattened)
                upserted += 1

            # Save the progress by checkpointing the state after every page. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # No pagination offset is stored in state, so the next sync still rereads all rows.
            op.checkpoint(state)

        log.info(f"Upserted {upserted} departments")
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        op.checkpoint(state)
    finally:
        cleanup()


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
#
# IMPORTANT: The recommended way to test your connector is using the Fivetran debug command:
#   fivetran debug
#
# This local testing block is provided as a convenience for quick debugging during development,
# such as using IDE debug tools (breakpoints, step-through debugging, etc.).
# Note: This method is not called by Fivetran when executing your connector in production.
# Always test using 'fivetran debug' prior to finalizing and deploying your connector.
if __name__ == "__main__":
    with open("configuration.json", "r", encoding="utf-8") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
