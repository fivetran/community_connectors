"""This connector demonstrates how to run Investran Report Wizard reports and load the resulting CSV
into the destination. Each report is started through the REST API, polled until it completes, and
streamed to a temporary file before its rows are upserted.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For hashing rows that have no configured primary key
import hashlib

# For reading the CSV report and configuration.json
import csv
import json

# For normalizing CSV headers into column names
import re

# For the temporary file that holds a downloaded report
import os
import tempfile

# For polling, retry backoff and token expiry checks
import time

# For issuing HTTP requests to the token and report endpoints
import requests

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

__SUCCESS_STATUSES = {"succeeded", "success", "completed"}
__FAILED_STATUSES = {"failed", "error", "cancelled", "canceled"}
__RETRY_ATTEMPTS = 5
__MAX_BACKOFF_SECONDS = 60
__DEFAULT_POLL_INTERVAL_SECONDS = 5
__DEFAULT_POLL_TIMEOUT_SECONDS = 900
__DEFAULT_CHECKPOINT_ROWS = 2000
# Renew the access token when less than a minute of its lifetime is left.
__REFRESH_MARGIN_MS = 60 * 1000
__RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
__TOKEN_ATTEMPTS = 4
__DOWNLOAD_ATTEMPTS = 4
__DOWNLOAD_CHUNK_BYTES = 1 << 20
__REQUIRED_KEYS = (
    "token_url",
    "client_id",
    "redirect_uri",
    "refresh_token",
    "base_url",
    "report_ids",
)


def is_unset(value) -> bool:
    """Treat missing, empty, and '<PLACEHOLDER>' values as not configured."""
    return value in (None, "") or str(value).strip().startswith("<")


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


def __report_ids(configuration: dict) -> list:
    """Split the comma or semicolon separated report_ids setting into integer ids."""
    raw = str(configuration["report_ids"]).replace(";", ",")
    return [int(item) for item in raw.split(",") if item.strip()]


def __primary_keys(configuration: dict) -> list:
    """Return the configured primary key columns, or an empty list when none are set."""
    raw = "" if is_unset(configuration.get("primary_keys")) else str(configuration["primary_keys"])
    return [column_name(item) for item in raw.split(",") if item.strip()]


def __table_for(configuration: dict, report_id: int) -> str:
    """Name the destination table '<table_prefix>_<report_id>', one table per report."""
    prefix = configuration.get("table_prefix")
    prefix = "report" if is_unset(prefix) else prefix.strip()
    return f"{prefix}_{report_id}"


def column_name(header: str) -> str:
    """Normalize a CSV header such as 'Unrealized Asset - AVM' to 'unrealized_asset_avm'."""
    return re.sub(r"[^0-9a-z]+", "_", (header or "").strip().lower()).strip("_")


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if a required value is missing or a numeric value is invalid.
    """
    for key in __REQUIRED_KEYS:
        if is_unset(configuration.get(key)):
            raise ValueError(f"Missing required configuration value: {key}")
    try:
        if not __report_ids(configuration):
            raise ValueError("report_ids must contain at least one report id")
    except ValueError:
        raise ValueError("report_ids must be a comma-separated list of integer report ids")
    for key, default in (
        ("poll_interval_seconds", __DEFAULT_POLL_INTERVAL_SECONDS),
        ("poll_timeout_seconds", __DEFAULT_POLL_TIMEOUT_SECONDS),
        ("checkpoint_rows", __DEFAULT_CHECKPOINT_ROWS),
    ):
        __int_setting(configuration, key, default)


def schema(configuration: dict):
    """
    Define the table schema for the connector.
    Reports return different columns, so only the table name and primary key are declared and the
    columns are inferred from the data. When primary_keys is empty, a row_hash column is used.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        A list of table definitions, one per configured report.
    """
    keys = __primary_keys(configuration) or ["row_hash"]
    return [
        {"table": __table_for(configuration, report_id), "primary_key": keys}
        for report_id in __report_ids(configuration)
    ]


def __expiry_ms(expires_in) -> int:
    """Convert expires_in to an absolute epoch in ms; accepts epoch ms or seconds-from-now."""
    value = int(expires_in)
    if value > 10**11:
        return value
    return int(time.time() * 1000) + value * 1000


def __request_token(configuration: dict, refresh_token: str):
    """
    Exchange a refresh token for an access token at the identity provider.
    HTTP 429 and 5xx responses are retried with exponential backoff, because the provider did not
    process them. Timeouts and connection errors are not replayed: the exchange may have succeeded
    and rotated the refresh token, so sending the old one again could invalidate the session.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        refresh_token: the refresh token to exchange.
    Raises:
        RuntimeError: if the provider keeps returning a retryable status or the request is ambiguous.
    """
    for attempt in range(1, __TOKEN_ATTEMPTS + 1):
        try:
            resp = requests.post(
                configuration["token_url"],
                data={
                    "grant_type": "refresh_token",
                    "client_id": configuration["client_id"],
                    "redirect_uri": configuration["redirect_uri"],
                    "refresh_token": refresh_token,
                },
                headers={"Accept": "application/json"},
                timeout=60,
            )
        except (requests.Timeout, requests.ConnectionError) as e:
            raise RuntimeError(
                f"Token exchange did not complete ({type(e).__name__}). It is not retried, because "
                "the refresh token may already have been rotated; run the sync again."
            )
        if resp.status_code not in __RETRY_STATUS_CODES or attempt == __TOKEN_ATTEMPTS:
            return resp
        delay_seconds = min(__MAX_BACKOFF_SECONDS, 5 * 2 ** (attempt - 1))
        log.warning(
            f"Token exchange -> HTTP {resp.status_code}; retry {attempt}/{__TOKEN_ATTEMPTS - 1} "
            f"in {delay_seconds}s"
        )
        time.sleep(delay_seconds)


def get_access_token(configuration: dict, state: dict, force_refresh: bool = False) -> str:
    """
    Reuse the cached access token until it is near expiry, otherwise exchange a refresh token.
    The identity provider rotates the refresh token on every exchange, so the newest one is kept in
    state and the configured one is only the starting point. A refresh token newly pasted into the
    configuration is tried first; if it is rejected, the one in state is tried. Every new token is
    checkpointed straight away, so a rotated refresh token is never lost.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector. It is updated in place.
        force_refresh: ignore the cached access token, for example after a 401 response.
    Raises:
        RuntimeError: if every refresh token is rejected.
    """
    configured = configuration["refresh_token"]
    seen = state.get("configured_refresh_token")
    if seen is not None and seen != configured:
        # A new token was pasted into the configuration, so drop what was derived from the old one.
        for key in ("refresh_token", "access_token", "access_expires_ms"):
            state.pop(key, None)
    state["configured_refresh_token"] = configured

    expires_ms = state.get("access_expires_ms", 0)
    is_cached_token_valid = expires_ms - __REFRESH_MARGIN_MS > time.time() * 1000
    if state.get("access_token") and is_cached_token_valid and not force_refresh:
        return state["access_token"]

    candidates = list(
        dict.fromkeys(token for token in (state.get("refresh_token"), configured) if token)
    )
    resp = None
    for token in candidates:
        resp = __request_token(configuration, token)
        if resp.ok or resp.status_code not in (400, 401):
            break
    if resp is None or not resp.ok:
        status = resp.status_code if resp is not None else "?"
        raise RuntimeError(
            f"Token exchange failed (HTTP {status}). If the refresh token was used elsewhere, "
            "set a new refresh_token in the connector configuration."
        )
    body = resp.json()
    if body.get("refresh_token"):
        state["refresh_token"] = body["refresh_token"]
    state["access_token"] = body["access_token"]
    state["access_expires_ms"] = __expiry_ms(body.get("expires_in", 0))
    # The refresh token has rotated, so persist the new one before using the access token.
    op.checkpoint(state)
    return body["access_token"]


def auth_headers(configuration: dict, state: dict, force_refresh: bool = False) -> dict:
    """Build request headers with a valid bearer token, renewing it when it is near expiry."""
    token = get_access_token(configuration, state, force_refresh)
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def api_call(method: str, url: str, attempts: int = __RETRY_ATTEMPTS, **kwargs):
    """
    Send an HTTP request, retrying timeouts, connection errors, 429 and 5xx with exponential backoff.
    Args:
        method: HTTP method.
        url: request URL.
        attempts: maximum number of attempts.
    Raises:
        RuntimeError: if every attempt fails.
    """
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.request(method, url, **kwargs)
            if resp.status_code != 429 and resp.status_code < 500:
                return resp
            error = f"HTTP {resp.status_code}"
        except (requests.Timeout, requests.ConnectionError) as e:
            error = type(e).__name__
        if attempt == attempts:
            raise RuntimeError(f"{method} {url} failed after {attempts} attempts: {error}")
        delay_seconds = min(__MAX_BACKOFF_SECONDS, 5 * 2 ** (attempt - 1))
        log.warning(
            f"{method} {url} -> {error}; retry {attempt}/{attempts - 1} in {delay_seconds}s"
        )
        time.sleep(delay_seconds)


def authed_call(configuration: dict, state: dict, method: str, url: str, **kwargs):
    """
    Call the API with a valid bearer token and retry once with a fresh token on HTTP 401.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
        method: HTTP method.
        url: request URL.
    """
    resp = api_call(method, url, headers=auth_headers(configuration, state), **kwargs)
    if resp.status_code == 401:
        log.warning("Received HTTP 401; renewing the access token and retrying once")
        resp = api_call(
            method, url, headers=auth_headers(configuration, state, force_refresh=True), **kwargs
        )
    return resp


def start_execution(
    configuration: dict, state: dict, base: str, report_id: int, timeout_seconds: int
):
    """
    Start a report execution and return its id.
    The API allows one identical execution at a time, so when one is already running this waits and
    tries again. The POST is never retried after a timeout, because that could start a second run.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
        base: base URL of the Report Wizard API.
        report_id: id of the report to run.
        timeout_seconds: seconds to keep waiting for a running execution to finish.
    Raises:
        RuntimeError: if the response has no execution id.
    """
    deadline = time.time() + timeout_seconds
    while True:
        resp = authed_call(
            configuration,
            state,
            "POST",
            f"{base}/executions",
            attempts=1,
            json={"reportId": report_id, "outputFormat": "application/csv"},
            timeout=300,
        )
        is_already_running = resp.status_code == 400 and "already in progress" in resp.text
        if is_already_running and time.time() < deadline:
            log.warning(f"Report {report_id} already has an execution in progress; waiting")
            time.sleep(15)
            continue
        break
    resp.raise_for_status()
    body = resp.json()
    exec_id = body.get("id") or body.get("executionId") if isinstance(body, dict) else body
    if exec_id is None:
        raise RuntimeError("Could not find an execution id in the response")
    log.info(f"Started execution {exec_id} for report {report_id}")
    return exec_id


def wait_for_success(
    configuration: dict,
    state: dict,
    base: str,
    exec_id,
    poll_interval_seconds: int,
    timeout_seconds: int,
):
    """
    Poll the execution status until it succeeds.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
        base: base URL of the Report Wizard API.
        exec_id: execution id.
        poll_interval_seconds: seconds between polls.
        timeout_seconds: seconds to wait before giving up.
    Raises:
        RuntimeError: if the execution fails.
        TimeoutError: if it does not finish in time.
    """
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        resp = authed_call(
            configuration, state, "GET", f"{base}/executions/{exec_id}/status", timeout=120
        )
        resp.raise_for_status()
        try:
            body = resp.json()
        except ValueError:
            body = resp.text.strip().strip('"')
        status = (
            (body.get("status") or body.get("executionStatus")) if isinstance(body, dict) else body
        )
        log.info(f"Execution {exec_id} status: {status}")
        if str(status).lower() in __SUCCESS_STATUSES:
            return
        if str(status).lower() in __FAILED_STATUSES:
            raise RuntimeError(f"Report execution {exec_id} ended with status {status}")
        time.sleep(poll_interval_seconds)
    raise TimeoutError(f"Report execution {exec_id} did not succeed within {timeout_seconds}s")


def is_transient_error(error: Exception) -> bool:
    """Return True for network errors and HTTP 429/5xx responses, which are worth retrying."""
    if isinstance(error, requests.HTTPError):
        return error.response is not None and error.response.status_code in __RETRY_STATUS_CODES
    return isinstance(
        error,
        (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError),
    )


def stream_result_to_file(
    configuration: dict,
    state: dict,
    base: str,
    exec_id,
    path: str,
    is_retry_after_401: bool = False,
):
    """
    Stream the CSV result of an execution into the file at path.
    On HTTP 401 the access token is renewed and the request is repeated once.
    """
    headers = auth_headers(configuration, state, force_refresh=is_retry_after_401)
    with requests.get(
        f"{base}/executions/{exec_id}/result",
        headers={**headers, "Accept": "application/csv"},
        timeout=(30, 300),
        stream=True,
    ) as resp:
        if resp.status_code == 401 and not is_retry_after_401:
            log.warning("Received HTTP 401 on download; renewing the access token")
        else:
            resp.raise_for_status()
            with open(path, "wb") as output:
                for chunk in resp.iter_content(chunk_size=__DOWNLOAD_CHUNK_BYTES):
                    output.write(chunk)
            return
    stream_result_to_file(configuration, state, base, exec_id, path, is_retry_after_401=True)


def download_result(configuration: dict, state: dict, base: str, exec_id) -> str:
    """
    Stream the CSV result to a temporary file and return its path.
    Reports can exceed 100 MB. The body is saved to disk first, because processing rows while
    streaming makes the API wait on slow writes and it drops slow readers. The file is read from
    disk afterwards, so memory use stays flat. Network errors and HTTP 429/5xx are retried with
    exponential backoff, permanent errors are raised, and the temporary file is removed whenever
    an attempt fails for any reason, including disk-write errors.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
        base: base URL of the Report Wizard API.
        exec_id: execution id.
    """
    for attempt in range(1, __DOWNLOAD_ATTEMPTS + 1):
        file_descriptor, path = tempfile.mkstemp(prefix="report_", suffix=".csv")
        os.close(file_descriptor)
        try:
            stream_result_to_file(configuration, state, base, exec_id, path)
            return path
        except Exception as e:
            os.unlink(path)
            if not is_transient_error(e) or attempt == __DOWNLOAD_ATTEMPTS:
                raise
            delay_seconds = min(__MAX_BACKOFF_SECONDS, 5 * attempt)
            log.warning(
                f"Result download failed ({type(e).__name__}); "
                f"retry {attempt}/{__DOWNLOAD_ATTEMPTS - 1} in {delay_seconds}s"
            )
            time.sleep(delay_seconds)


def row_hash(row: dict) -> str:
    """
    Build a stable key from every column of a row, used when no primary key is configured.
    The row is serialized as JSON with sorted keys, so a null value and the text 'None' hash
    differently and the key does not depend on column order.
    """
    payload = json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalize_headers(headers) -> list:
    """
    Normalize CSV headers and fail clearly if two headers collide or a header is empty.
    Args:
        headers: the header names from the CSV file.
    Raises:
        ValueError: if a header is empty or two headers normalize to the same column name.
    """
    names = [column_name(header) for header in (headers or [])]
    if not names:
        raise ValueError("The report result has no CSV header row")
    if any(not name for name in names):
        raise ValueError("The report result has an empty or unusable CSV column header")
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ValueError(f"CSV headers collide after normalization: {', '.join(duplicates)}")
    return names


def load_report(configuration: dict, state: dict, base: str, report_id: int):
    """
    Run one report and upsert its rows.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
        base: base URL of the Report Wizard API.
        report_id: id of the report to run.
    """
    table = __table_for(configuration, report_id)
    poll_interval_seconds = __int_setting(
        configuration, "poll_interval_seconds", __DEFAULT_POLL_INTERVAL_SECONDS
    )
    timeout_seconds = __int_setting(
        configuration, "poll_timeout_seconds", __DEFAULT_POLL_TIMEOUT_SECONDS
    )
    checkpoint_rows = __int_setting(configuration, "checkpoint_rows", __DEFAULT_CHECKPOINT_ROWS)
    use_hash = not __primary_keys(configuration)

    exec_id = start_execution(configuration, state, base, report_id, timeout_seconds)
    wait_for_success(configuration, state, base, exec_id, poll_interval_seconds, timeout_seconds)

    path = download_result(configuration, state, base, exec_id)
    count = 0
    try:
        # The CSV is read row by row from disk, so the whole report is never held in memory.
        with open(path, encoding="utf-8-sig", newline="") as stream:
            reader = csv.reader(stream)
            names = normalize_headers(next(reader, None))
            for values in reader:
                if not values:
                    continue
                if len(values) > len(names):
                    raise ValueError(
                        f"Row {count + 2} has more values than the header has columns"
                    )
                values = values + [""] * (len(names) - len(values))
                row = {
                    name: (value if value != "" else None) for name, value in zip(names, values)
                }
                if use_hash:
                    row["row_hash"] = row_hash(row)

                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(table=table, data=row)
                count += 1

                if count % checkpoint_rows == 0:
                    # Save the progress by checkpointing the state. This is important for ensuring that
                    # the sync process can resume after the last committed batch in case of interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(state)
    finally:
        os.unlink(path)
    log.info(f"Report {report_id}: loaded {count} rows into {table}")

    state.setdefault("last_execution_ids", {})[str(report_id)] = exec_id
    op.checkpoint(state)


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector, including the rotating refresh token.
    """
    log.warning("Example: Source Examples : Investran Report Wizard")

    validate_configuration(configuration)
    base = configuration["base_url"].rstrip("/")
    log.info(f"Syncing {len(__report_ids(configuration))} report(s)")

    # Obtain the first access token up front; get_access_token checkpoints any rotated refresh token.
    get_access_token(configuration, state)
    log.info("Access token ready")

    for report_id in __report_ids(configuration):
        load_report(configuration, state, base, report_id)


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
