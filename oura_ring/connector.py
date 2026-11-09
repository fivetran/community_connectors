"""
Oura Ring connector - Syncs daily activity, sleep, readiness, stress, heart rate, and detailed
sleep period data from the Oura API v2 to your Fivetran destination.

The connector authenticates with Oura's OAuth2 server-side flow: each sync spends the single-use
refresh token, keeps its replacement encrypted in state, and checkpoints it before reading any data.
It tracks a per-table date cursor, re-reads a
short overlap window on every sync because Oura revises recent days after the ring syncs, and
follows next_token pagination. Nested objects are flattened and arrays are serialized to JSON
strings for warehouse compatibility.

See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For time-based operations and rate limiting
import time

# For date calculations and incremental sync ranges
from datetime import date, datetime, timedelta, timezone

# For making HTTP requests to external APIs
import requests

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For the setup form and the result type returned by its setup tests
from fivetran_connector_sdk import ConfigurationForm, Test

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# The setup form's field layout, kept in its own module so the connector stays readable
from setup_form import build_form

# The endpoint and column definition of every table
from tables import TABLE_CONFIG

# OAuth2 refresh with the rotated refresh token kept encrypted in state
from oura_auth import check_fernet_key, refresh_access_token

# API Configuration Constants
__BASE_URL = "https://api.ouraring.com"
__API_TIMEOUT_SECONDS = 30
# Oura may reject a request that has no User-Agent header
__USER_AGENT = "Fivetran-Oura-Ring-Connector/2.0"

# Retry and Rate Limiting Constants
__MAX_RETRIES = 3
__BASE_DELAY_SECONDS = 1
__MAX_RETRY_AFTER_SECONDS = 300
__RETRYABLE_STATUS_CODES = [429, 500, 502, 503, 504]
# 403 means a missing scope or a lapsed Oura subscription; 426 means the account needs an upgrade
__ACCESS_RESTRICTED_STATUS_CODES = [403, 426]

# Sync Configuration Constants
__DEFAULT_LOOKBACK_DAYS = 90
__MAX_LOOKBACK_DAYS = 3650
# Days can reach Oura late, after the ring syncs to the app, so every sync re-reads this many days
__INCREMENTAL_OVERLAP_DAYS = 7


class OuraAccessError(RuntimeError):
    """The Oura API refused the token or the account, so retrying cannot help."""

    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code


def _is_placeholder(value) -> bool:
    """
    Check for an unset value or an unedited angle-bracket placeholder.

    Args:
        value: the configuration value to check

    Returns:
        True when the value is missing, empty, or a <PLACEHOLDER> string
    """
    if value is None:
        return True
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    return not stripped or (stripped.startswith("<") and stripped.endswith(">"))


def _optional_int(configuration: dict, key: str, default: int) -> int:
    """
    Read an optional integer, treating a missing or placeholder value as unset.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        key: the configuration key to read
        default: the value to use when the key is unset

    Returns:
        The configured integer, or the default
    """
    value = configuration.get(key)
    if _is_placeholder(value):
        return default
    return int(value)


def flatten_dict(d, parent_key="", sep="_"):
    """
    Flatten nested dictionaries and serialize lists to JSON strings.
    REQUIRED for Fivetran compatibility.

    Args:
        d: Dictionary to flatten
        parent_key: Prefix for nested keys (used in recursion)
        sep: Separator between nested key levels

    Returns:
        Flattened dictionary with all nested structures resolved
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, (list, tuple)):
            items.append((new_key, json.dumps(v) if v else None))
        else:
            items.append((new_key, v))
    return dict(items)


def flatten_record(record: dict, columns: dict) -> dict:
    """
    Flatten one API record into a row holding every declared column.

    Oura returns a nested object such as heart_rate or readiness as an explicit null when the data
    is missing, which would otherwise add a stray column beside the flattened ones. Declared columns
    are always present, and a field the API adds later still passes through once it has a value.

    Args:
        record: one record from an Oura API response
        columns: the table's declared columns

    Returns:
        Flattened dictionary with every declared column, plus any undeclared field with a value
    """
    flattened = flatten_dict(record)
    row = {column: flattened.get(column) for column in columns}
    row.update(
        {key: value for key, value in flattened.items() if key not in row and value is not None}
    )
    return row


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure all required parameters are present and valid.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: if any required configuration parameter is missing or invalid.
    """
    for key in ("client_id", "client_secret", "refresh_token", "fernet_key"):
        if _is_placeholder(configuration.get(key)):
            raise ValueError(f"Missing required configuration value: {key}")
    check_fernet_key(configuration["fernet_key"])

    lookback_days = configuration.get("lookback_days")
    if not _is_placeholder(lookback_days):
        try:
            lookback_value = int(lookback_days)
        except (TypeError, ValueError) as e:
            raise ValueError("lookback_days must be a valid positive integer") from e
        if lookback_value <= 0 or lookback_value > __MAX_LOOKBACK_DAYS:
            raise ValueError(f"lookback_days must be between 1 and {__MAX_LOOKBACK_DAYS}")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": table_name, "primary_key": config["primary_key"], "columns": config["columns"]}
        for table_name, config in TABLE_CONFIG.items()
    ]


def create_session(access_token: str):
    """
    Create an authenticated requests session for the Oura API.

    Args:
        access_token: OAuth2 access token

    Returns:
        requests.Session configured with Bearer token authentication
    """
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {access_token}",
            "User-Agent": __USER_AGENT,
        }
    )
    return session


def get_retry_delay(response, attempt: int) -> int:
    """
    Choose how long to wait before retrying a failed response.

    Args:
        response: the HTTP response, whose Retry-After header wins when it holds a number of seconds
        attempt: zero-based attempt number, used for exponential backoff

    Returns:
        The number of seconds to wait, capped so a large header cannot stall the sync
    """
    backoff_seconds = __BASE_DELAY_SECONDS * (2**attempt)
    retry_after = response.headers.get("Retry-After")
    if retry_after is None:
        return backoff_seconds
    try:
        return min(max(int(retry_after), backoff_seconds), __MAX_RETRY_AFTER_SECONDS)
    except ValueError:
        return backoff_seconds


def fetch_data_with_retry(session, url, params=None):
    """
    Fetch data from API with exponential backoff retry logic.

    Error messages carry the status and the URL path only. A response body can hold health data,
    so it is never logged or raised.

    Args:
        session: requests.Session object for connection pooling
        url: Full URL to fetch
        params: Optional query parameters

    Returns:
        JSON response as dictionary

    Raises:
        OuraAccessError: If the API refuses the token or the account
        RuntimeError: If all retry attempts fail
    """
    for attempt in range(__MAX_RETRIES):
        is_last_attempt = attempt == __MAX_RETRIES - 1
        try:
            response = session.get(url, params=params, timeout=__API_TIMEOUT_SECONDS)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if is_last_attempt:
                raise RuntimeError(
                    f"{type(e).__name__} after {__MAX_RETRIES} attempts: {url}"
                ) from e
            delay_seconds = __BASE_DELAY_SECONDS * (2**attempt)
            log.warning(f"{type(e).__name__} for {url}, retrying in {delay_seconds}s")
            time.sleep(delay_seconds)
            continue
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Request failed ({type(e).__name__}): {url}") from e

        status_code = response.status_code
        if status_code == 401:
            raise OuraAccessError(
                status_code,
                f"HTTP 401: Oura rejected the access token from this sync's refresh. URL: {url}",
            )
        if status_code in __ACCESS_RESTRICTED_STATUS_CODES:
            raise OuraAccessError(
                status_code,
                f"HTTP {status_code}: check the token's scopes and the Oura subscription. "
                f"URL: {url}",
            )
        if status_code in __RETRYABLE_STATUS_CODES and not is_last_attempt:
            delay_seconds = get_retry_delay(response, attempt)
            log.warning(f"HTTP {status_code} for {url}, retrying in {delay_seconds}s")
            time.sleep(delay_seconds)
            continue
        if status_code >= 400:
            raise RuntimeError(
                f"HTTP {status_code} after {attempt + 1} attempt(s): {url}",
            )
        try:
            return response.json()
        except ValueError as e:
            raise RuntimeError(f"The API returned a body that is not JSON: {url}") from e

    raise RuntimeError(f"No response after {__MAX_RETRIES} attempts: {url}")


def build_date_params(table_config: dict, window_start: date, window_end: date) -> dict:
    """
    Build date query parameters appropriate for the endpoint type.

    Daily endpoints use start_date/end_date (YYYY-MM-DD format).
    The heart rate endpoint uses start_datetime/end_datetime (ISO 8601 format, in UTC).

    Args:
        table_config: Configuration dict for this table
        window_start: First day of the window
        window_end: Last day of the window

    Returns:
        Dictionary of query parameters for the API request
    """
    start_value = window_start.isoformat()
    end_value = window_end.isoformat()
    if table_config.get("uses_datetime"):
        start_value = f"{start_value}T00:00:00Z"
        end_value = f"{end_value}T23:59:59Z"
    return {table_config["start_param"]: start_value, table_config["end_param"]: end_value}


def generate_date_chunks(start_date: date, end_date: date, max_days=None) -> list:
    """
    Split a date range into chunks of a maximum number of days.

    Used for endpoints like heart_rate that have API-enforced date range limits.

    Args:
        start_date: First day of the range
        end_date: Last day of the range
        max_days: Maximum number of days per chunk, or None for a single chunk

    Returns:
        List of (chunk_start, chunk_end) date tuples, both inclusive
    """
    if max_days is None:
        return [(start_date, end_date)]

    chunks = []
    current_start = start_date
    while current_start <= end_date:
        current_end = min(current_start + timedelta(days=max_days - 1), end_date)
        chunks.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)
    return chunks


def get_current_date() -> date:
    """
    Return today's date in UTC.

    Returns:
        The current UTC date
    """
    return datetime.now(timezone.utc).date()


def resolve_start_date(state: dict, table_name: str, today: date, lookback_days: int) -> date:
    """
    Work out the first day a table's sync reads.

    Args:
        state: State dictionary holding each table's synced_through date
        table_name: Name of the table being synced
        today: The current UTC date
        lookback_days: Days to read on the table's first sync

    Returns:
        The saved date minus the overlap window, or the lookback start on a first sync

    Raises:
        RuntimeError: If the saved date cannot be parsed
    """
    synced_through = state.get(f"{table_name}_synced_through")
    if not synced_through:
        return today - timedelta(days=lookback_days)
    try:
        saved_date = date.fromisoformat(synced_through)
    except (TypeError, ValueError) as e:
        raise RuntimeError(f"Saved state for {table_name} is not a date") from e
    return saved_date - timedelta(days=__INCREMENTAL_OVERLAP_DAYS)


def sync_window(session, table_name, table_config, window_start, window_end, state):
    """
    Sync one date window of a table, following next_token until the window is drained.

    The next_token is never saved to state. It is only valid for this window, and a window that
    is interrupted is read again from its start, which upserts the same primary keys.

    Args:
        session: Authenticated requests session
        table_name: Name of the destination table
        table_config: Configuration dict with endpoint, primary_key, and date params
        window_start: First day of the window
        window_end: Last day of the window
        state: State dictionary for tracking sync progress

    Returns:
        Tuple of (records upserted, records skipped because they had no primary key)

    Raises:
        RuntimeError: If API requests fail, the response has the wrong shape, or the API repeats
            a next_token
    """
    primary_key_field = table_config["primary_key"][0]
    url = f"{__BASE_URL}{table_config['endpoint']}"
    next_token = None
    record_count = 0
    skipped_count = 0

    while True:
        params = build_date_params(table_config, window_start, window_end)
        if next_token:
            params["next_token"] = next_token

        page = fetch_data_with_retry(session, url, params=params)
        if not isinstance(page, dict) or not isinstance(page.get("data"), list):
            raise RuntimeError(f"Unexpected response shape for {table_name}")

        for record in page["data"]:
            if not isinstance(record, dict) or record.get(primary_key_field) is None:
                skipped_count += 1
                continue

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table=table_name, data=flatten_record(record, table_config["columns"]))
            record_count += 1

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state=state)

        previous_token = next_token
        next_token = page.get("next_token")
        if not next_token:
            return record_count, skipped_count
        if next_token == previous_token:
            raise RuntimeError(f"The API repeated a next_token for {table_name}")


def sync_table(session, table_name, table_config, start_date, today, state):
    """
    Sync a table from its start date through today, one window at a time.

    The query runs through tomorrow because Oura dates records in the wearer's time zone, which can
    be ahead of UTC. The saved date never passes today, so the next sync re-reads that edge.

    Args:
        session: Authenticated requests session
        table_name: Name of the destination table
        table_config: Configuration dict with endpoint, primary_key, and date params
        start_date: First day to read
        today: The current UTC date
        state: State dictionary for tracking sync progress

    Returns:
        Tuple of (records upserted, records skipped because they had no primary key)

    Raises:
        RuntimeError: If API requests fail after max retries
    """
    end_date = today + timedelta(days=1)
    chunks = generate_date_chunks(start_date, end_date, table_config.get("max_range_days"))
    total_count = 0
    total_skipped = 0

    log.info(f"{table_name}: syncing {start_date} to {end_date} in {len(chunks)} window(s)")
    for window_start, window_end in chunks:
        record_count, skipped_count = sync_window(
            session, table_name, table_config, window_start, window_end, state
        )
        total_count += record_count
        total_skipped += skipped_count
        state[f"{table_name}_synced_through"] = min(window_end, today).isoformat()

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state=state)

    log.info(f"{table_name}: {total_count} records synced")
    return total_count, total_skipped


def report_degraded_sync(restricted_tables: list, skipped_count: int):
    """
    Report anything the sync left out, in the connector log and on the Fivetran dashboard.

    Args:
        restricted_tables: Optional tables skipped because the API refused access
        skipped_count: Records skipped because they had no primary key
    """
    if restricted_tables:
        message = (
            f"Skipped {', '.join(restricted_tables)}: access restricted. "
            "Check the token's scopes and the Oura subscription."
        )
        log.warning(message)
        op.warning(message)
    if skipped_count:
        message = f"Skipped {skipped_count} record(s) that had no primary key."
        log.warning(message)
        op.warning(message)


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
    log.warning("Example: connectors : oura_ring")

    validate_configuration(configuration)

    lookback_days = _optional_int(configuration, "lookback_days", __DEFAULT_LOOKBACK_DAYS)
    today = get_current_date()
    access_token = refresh_access_token(configuration, state, __USER_AGENT)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state=state)

    session = create_session(access_token)
    restricted_tables = []
    skipped_count = 0

    try:
        for table_name, table_config in TABLE_CONFIG.items():
            start_date = resolve_start_date(state, table_name, today, lookback_days)
            try:
                _, table_skipped = sync_table(
                    session, table_name, table_config, start_date, today, state
                )
                skipped_count += table_skipped
            except OuraAccessError as e:
                is_restricted = e.status_code in __ACCESS_RESTRICTED_STATUS_CODES
                if not (table_config.get("is_optional") and is_restricted):
                    raise OuraAccessError(
                        e.status_code,
                        f"{table_name} needs the {table_config['scope']} scope. {e}",
                    ) from e
                restricted_tables.append(table_name)

        report_degraded_sync(restricted_tables, skipped_count)

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state=state)

        log.info("Sync complete for all tables")

    finally:
        session.close()


def verify_configuration(configuration: dict):
    """
    Setup test: validate the configuration without calling Oura.

    Oura's refresh tokens are single-use and a setup test cannot save the replacement, so this
    test never spends one. The first sync reports a rejected token or wrong client credentials.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Returns:
        Test().success() or Test().failure(reason)
    """
    test = Test()
    try:
        validate_configuration(configuration)
    except ValueError as e:
        return test.failure(str(e))
    return test.success()


def configuration_form() -> ConfigurationForm:
    """
    Declare the setup form Fivetran renders for this connector and the setup test it can run.

    The field layout lives in setup_form.py; this function supplies the default and the setup
    test, which needs the connector's own validation code.

    Returns:
        ConfigurationForm with one field per configuration key and one setup test
    """
    return build_form(
        defaults={"lookback_days": __DEFAULT_LOOKBACK_DAYS},
        tests=[("Check the OAuth configuration (no Oura call)", verify_configuration)],
    )


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema, configuration_form=configuration_form)

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
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
