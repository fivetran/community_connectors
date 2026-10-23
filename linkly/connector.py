"""Linkly connector for the Fivetran Connector SDK.
This connector syncs workspaces, links, custom domains, daily click totals and conversions from the
Linkly URL shortener API (https://linklyhq.com, endpoint reference at
https://api.linklyhq.com/api/openapi) into your destination.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.
"""

# For reading configuration from a JSON file
import json

# For pausing between retries of rate-limited or failed requests
import time

# For computing the date windows used by the click history sync
from datetime import date, datetime, timedelta, timezone

# For parsing the HTTP-date form of the Retry-After header
from email.utils import parsedate_to_datetime

# For validating the optional base_url configuration value
from urllib.parse import urlparse

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For making HTTP requests to the Linkly API (pre-installed in the Connector SDK runtime)
import requests

# Constants for API configuration
__DEFAULT_BASE_URL = "https://api.linklyhq.com/api/v1"  # Production Linkly API
__DEFAULT_START_DATE = "2019-01-01"  # Earliest date Linkly holds per-day click counts for
__USER_AGENT = "fivetran-connector-sdk-linkly/1.0"

# Constants for pagination and windowing
__LINK_PAGE_SIZE = 100  # Links per list_links request
__CONVERSION_LIMIT = 1000  # Maximum rows the conversions endpoint returns; it has no pagination
__CLICK_REPLAY_DAYS = 2  # Re-sync the last N days so in-progress daily totals are corrected
__CLICK_WINDOW_DAYS = 366  # Days of click history requested per API call during backfill

# Constants for retry behaviour
__MAX_RETRIES = 6  # Attempts per request before the sync fails
__BACKOFF_BASE_SEC = 2  # First retry delay in seconds; doubles on every attempt
__BACKOFF_MAX_SEC = 120  # Upper bound for a single retry delay in seconds
__REQUEST_TIMEOUT_SEC = 60  # Timeout for each API request in seconds
__RATE_LIMIT_STATUS_CODE = 429  # HTTP status code Linkly returns when the API key is rate limited
__SERVER_ERROR_MIN_STATUS = 500  # Minimum HTTP status code treated as a transient server error
__UNAUTHORIZED_STATUS_CODE = 401  # HTTP status code for a missing or invalid API key

# Table names
__WORKSPACE_TABLE = "workspace"
__LINK_TABLE = "link"
__DOMAIN_TABLE = "domain"
__CLICK_DAILY_TABLE = "click_daily"
__CONVERSION_TABLE = "conversion"

# State keys
__STATE_LINK_RESUME_PAGE = "link_resume_page"  # {"<workspace_id>:<active|deleted>": page}
__STATE_CLICK_CURSOR = "click_cursor_by_workspace"  # {"<workspace_id>": "YYYY-MM-DD"}
__STATE_CONVERSION_CURSOR = "conversion_cursor"  # Highest conversion id (ULID) delivered
__STATE_DOMAIN_NAMES = "domain_names_by_workspace"  # {"<workspace_id>": ["<domain name>", ...]}

# The list_links endpoint returns active links by default and trashed links with deleted=true.
__LINK_MODES = ((False, "active"), (True, "deleted"))


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector
    has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or invalid.
    """
    api_key = configuration.get("api_key")
    if not isinstance(api_key, str) or not api_key.strip():
        raise ValueError(
            "Missing required configuration value: api_key. Create one in your Linkly "
            "workspace settings (https://linklyhq.com/support/api)."
        )

    start_date = configuration.get("start_date")
    if start_date not in (None, ""):
        parsed_start_date = parse_start_date(start_date)
        if parsed_start_date > datetime.now(timezone.utc).date():
            raise ValueError("Configuration value 'start_date' must not be in the future")

    workspace_ids = configuration.get("workspace_ids")
    if workspace_ids not in (None, ""):
        if not isinstance(workspace_ids, str):
            raise ValueError(
                "Configuration value 'workspace_ids' must be a string containing a "
                f"comma-separated list of numeric workspace ids, got {workspace_ids!r}"
            )
        for workspace_id in workspace_ids.split(","):
            if not workspace_id.strip().isdigit():
                raise ValueError(
                    "Configuration value 'workspace_ids' must be a comma-separated list of "
                    f"numeric workspace ids, got {workspace_ids!r}"
                )

    base_url = configuration.get("base_url")
    if base_url not in (None, "") and not is_http_url(base_url):
        raise ValueError(
            "Configuration value 'base_url' must be an http(s) URL with a host, for example "
            f"https://api.linklyhq.com/api/v1, got {base_url!r}"
        )


def parse_start_date(start_date):
    """
    Parse the start_date configuration value, accepting only the canonical YYYY-MM-DD form.
    date.fromisoformat() also accepts other ISO 8601 forms such as 20260601, so the parsed date
    is formatted again and compared with the input.
    Args:
        start_date: The raw configuration value.
    Returns:
        The parsed date.
    Raises:
        ValueError: if the value is not a string in YYYY-MM-DD format.
    """
    error_message = f"Configuration value 'start_date' must be YYYY-MM-DD, got {start_date!r}"
    if not isinstance(start_date, str):
        raise ValueError(error_message)
    try:
        parsed_start_date = date.fromisoformat(start_date)
    except ValueError:
        raise ValueError(error_message)
    if parsed_start_date.isoformat() != start_date:
        raise ValueError(error_message)
    return parsed_start_date


def is_http_url(value):
    """
    Check that a configuration value is an absolute http or https URL with a network location.
    Args:
        value: The raw configuration value.
    Returns:
        True if the value is a string with an http or https scheme and a host, otherwise False.
    """
    if not isinstance(value, str):
        return False
    parsed_url = urlparse(value.strip())
    return parsed_url.scheme in ("http", "https") and bool(parsed_url.netloc)


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    # Only primary keys and the columns whose type must not be inferred are declared here.
    # Every other column is inferred from the API payload, which lets new Linkly fields flow
    # through without a connector change.
    return [
        {
            "table": __WORKSPACE_TABLE,
            "primary_key": ["id"],
            "columns": {"id": "LONG"},
        },
        {
            "table": __LINK_TABLE,
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "workspace_id": "LONG",
                "rules": "JSON",  # Routing rules (geo, device, A/B split) as returned by the API
                "sparkline": "JSON",  # Daily human clicks for the last 30 days, oldest first
            },
        },
        {
            "table": __DOMAIN_TABLE,
            "primary_key": ["workspace_id", "name"],
            "columns": {"workspace_id": "LONG"},
        },
        {
            "table": __CLICK_DAILY_TABLE,
            "primary_key": ["workspace_id", "date"],
            "columns": {"workspace_id": "LONG", "date": "NAIVE_DATE"},
        },
        {
            "table": __CONVERSION_TABLE,
            "primary_key": ["id"],
            "columns": {
                "link_id": "LONG",
                "amount_cents": "LONG",
                "metadata": "JSON",  # Custom key/value pairs attached by the reporting integration
                "occurred_at": "UTC_DATETIME",
                "inserted_at": "UTC_DATETIME",
            },
        },
    ]


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
    log.warning("Example: SaaS & APIs : Linkly")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    session = build_session(configuration["api_key"].strip())
    base_url = (configuration.get("base_url") or __DEFAULT_BASE_URL).rstrip("/")
    start_date = date.fromisoformat(configuration.get("start_date") or __DEFAULT_START_DATE)
    workspace_filter = parse_workspace_ids(configuration.get("workspace_ids"))

    workspaces = sync_workspaces(session, base_url, workspace_filter, state)

    for workspace in workspaces:
        workspace_id = int(workspace["id"])
        sync_links(session, base_url, workspace_id, state)
        sync_domains(session, base_url, workspace_id, state)
        sync_click_daily(session, base_url, workspace_id, start_date, state)

    sync_conversions(session, base_url, state)
    log.info("Linkly sync complete")


def parse_workspace_ids(workspace_ids: str):
    """
    Parse the optional comma-separated workspace id filter from the configuration.
    Args:
        workspace_ids: The raw configuration value, for example "42,43", or None.
    Returns:
        A set of integer workspace ids, or None when every accessible workspace should be synced.
    """
    if not workspace_ids or not workspace_ids.strip():
        return None
    return {int(workspace_id) for workspace_id in workspace_ids.split(",") if workspace_id.strip()}


def build_session(api_key: str):
    """
    Create an HTTP session that authenticates every request with the Linkly API key.
    Linkly recommends sending the key as a bearer token rather than as a query parameter.
    Args:
        api_key: The Linkly API key from the configuration.
    Returns:
        A requests.Session with the authorization headers set.
    """
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "User-Agent": __USER_AGENT,
        }
    )
    return session


def get_json(session: requests.Session, base_url: str, path: str, params: dict = None):
    """
    GET a JSON resource from the Linkly API with retries and exponential backoff.
    Rate limits (429), server errors (5xx), timeouts and connection errors are retried.
    Every other 4xx response is permanent and fails the sync immediately.
    Args:
        session: The authenticated HTTP session.
        base_url: The API base URL, for example https://api.linklyhq.com/api/v1.
        path: The endpoint path relative to base_url.
        params: Optional query parameters.
    Returns:
        The decoded JSON response body.
    Raises:
        RuntimeError: on a permanent client error or when every retry attempt has failed.
    """
    url = f"{base_url}/{path.lstrip('/')}"
    last_error = None
    for attempt in range(1, __MAX_RETRIES + 1):
        try:
            response = session.get(url, params=params, timeout=__REQUEST_TIMEOUT_SEC)
        except (requests.ConnectionError, requests.Timeout) as error:
            last_error = error
            wait_before_retry(attempt, f"network error calling {path}: {error}")
            continue

        if response.ok:
            return response.json()

        if response.status_code == __RATE_LIMIT_STATUS_CODE:
            # Linkly's 429 body carries current_usage and limit but no Retry-After header, so the
            # delay comes from the exponential backoff schedule (Retry-After is honoured if sent).
            last_error = requests.HTTPError(f"HTTP 429 for {path}")
            wait_before_retry(
                attempt,
                f"rate limited on {path}: {describe_error_response(response)}",
                response.headers.get("Retry-After"),
            )
            continue

        if response.status_code >= __SERVER_ERROR_MIN_STATUS:
            last_error = requests.HTTPError(f"HTTP {response.status_code} for {path}")
            wait_before_retry(attempt, f"server error {response.status_code} on {path}")
            continue

        raise_permanent_error(response, path)

    raise RuntimeError(f"Giving up on {path} after {__MAX_RETRIES} attempts: {last_error}")


def raise_permanent_error(response: requests.Response, path: str):
    """
    Fail fast on a non-retryable client error with a message that points at the likely cause.
    Args:
        response: The HTTP response with a 4xx status code.
        path: The endpoint path, for the error message.
    Raises:
        RuntimeError: always.
    """
    detail = describe_error_response(response)
    if response.status_code == __UNAUTHORIZED_STATUS_CODE:
        raise RuntimeError(
            "Linkly rejected the API key (HTTP 401). Check the 'api_key' configuration value; "
            "keys are managed in your Linkly workspace settings (https://linklyhq.com/support/api)."
        )
    raise RuntimeError(f"Linkly returned HTTP {response.status_code} for {path}: {detail}")


def describe_error_response(response: requests.Response):
    """
    Extract a short, human-readable description from a Linkly error response body.
    Args:
        response: The HTTP error response.
    Returns:
        The error or message field from the JSON body, with rate-limit usage when present.
    """
    try:
        body = response.json()
    except ValueError:
        return response.text[:200]
    if not isinstance(body, dict):
        return str(body)[:200]
    message = body.get("message") or body.get("error") or str(body)[:200]
    if "limit" in body:
        # RateLimitError bodies include the current usage against the key's limit.
        return f"{message} (usage {body.get('current_usage')}/{body.get('limit')})"
    return message


def wait_before_retry(attempt: int, reason: str, retry_after_header: str = None):
    """
    Sleep before the next retry using exponential backoff, or the Retry-After header if present.
    Args:
        attempt: The 1-based attempt number that just failed.
        reason: Description of the failure for the log.
        retry_after_header: Optional Retry-After header value, in seconds or as an HTTP-date.
    """
    if attempt >= __MAX_RETRIES:
        return
    delay_sec = min(__BACKOFF_BASE_SEC * (2 ** (attempt - 1)), __BACKOFF_MAX_SEC)
    retry_after_sec = parse_retry_after(retry_after_header)
    if retry_after_sec is not None:
        delay_sec = min(retry_after_sec, __BACKOFF_MAX_SEC)
    log.warning(f"{reason}; retrying in {delay_sec}s (attempt {attempt}/{__MAX_RETRIES})")
    time.sleep(delay_sec)


def parse_retry_after(retry_after_header):
    """
    Convert a Retry-After header value into a delay in seconds.
    RFC 9110 allows either a number of seconds or an HTTP-date; both forms are supported.
    Args:
        retry_after_header: The raw header value, or None when the header is absent.
    Returns:
        The delay in whole seconds (never negative), or None if the header is absent or invalid.
    """
    if not retry_after_header:
        return None
    value = retry_after_header.strip()
    if value.isdigit():
        return int(value)
    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    if retry_at is None:
        return None
    if retry_at.tzinfo is None:
        # HTTP-dates are always GMT; parsedate_to_datetime returns a naive value for "-0000".
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    return max(0, int((retry_at - datetime.now(timezone.utc)).total_seconds()))


def sync_workspaces(session: requests.Session, base_url: str, workspace_filter, state: dict):
    """
    Upsert every workspace visible to the API key and return the ones to sync.
    The workspaces endpoint returns a small, unpaginated list.
    Args:
        session: The authenticated HTTP session.
        base_url: The API base URL.
        workspace_filter: Set of workspace ids to keep, or None for all.
        state: The connector state, checkpointed after the table is written.
    Returns:
        The list of workspace dictionaries to iterate over.
    """
    workspaces = get_json(session, base_url, "workspaces") or []
    if workspace_filter is not None:
        workspaces = [
            workspace for workspace in workspaces if int(workspace["id"]) in workspace_filter
        ]
    for workspace in workspaces:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(
            table=__WORKSPACE_TABLE,
            data={"id": int(workspace["id"]), "name": workspace.get("name")},
        )
    log.info(f"Synced {len(workspaces)} workspace(s)")

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state)
    return workspaces


def sync_links(session: requests.Session, base_url: str, workspace_id: int, state: dict):
    """
    Re-import every active and trashed link in one workspace.
    The Linkly API exposes no modification timestamp on links and no 'modified since' filter, so
    links are re-read on every sync and their lifetime click counters are refreshed each time.
    Pagination is page-number based (page, page_size, total_pages). Pages are sorted by id so the
    sequence stays stable while links are being created, and the next page number is
    checkpointed after every page so an interrupted sync resumes mid-workspace.
    Args:
        session: The authenticated HTTP session.
        base_url: The API base URL.
        workspace_id: The workspace whose links are synced.
        state: The connector state; link_resume_page holds the page to resume from.
    """
    resume_pages = state.setdefault(__STATE_LINK_RESUME_PAGE, {})
    for is_deleted, mode in __LINK_MODES:
        resume_key = f"{workspace_id}:{mode}"
        page = int(resume_pages.get(resume_key, 1))
        row_count = 0
        while True:
            links, total_pages = fetch_links_page(
                session, base_url, workspace_id, is_deleted, page
            )
            for link in links:
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(table=__LINK_TABLE, data=build_link_row(link, workspace_id, is_deleted))
            row_count += len(links)

            # Exit on the last page or on an empty page so a miscounted total cannot loop forever.
            if page >= total_pages or not links:
                break
            page += 1
            resume_pages[resume_key] = page

            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(state)

        # The table is complete for this mode, so the next sync starts again from page 1.
        resume_pages.pop(resume_key, None)
        log.info(f"Workspace {workspace_id}: synced {row_count} {mode} link(s) in {page} page(s)")

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state)


def fetch_links_page(
    session: requests.Session, base_url: str, workspace_id: int, is_deleted: bool, page: int
):
    """
    Fetch one page of links for a workspace.
    Args:
        session: The authenticated HTTP session.
        base_url: The API base URL.
        workspace_id: The workspace to list links for.
        is_deleted: True to list trashed links instead of active ones.
        page: The 1-based page number.
    Returns:
        A tuple of (links on this page, total number of pages).
    """
    params = {
        "page": page,
        "page_size": __LINK_PAGE_SIZE,
        "sort_by": "id",
        "sort_dir": "asc",
    }
    if is_deleted:
        params["deleted"] = "true"
    body = get_json(session, base_url, f"workspace/{workspace_id}/list_links", params)
    return body.get("links") or [], int(body.get("total_pages") or 1)


def build_link_row(link: dict, workspace_id: int, is_deleted: bool):
    """
    Build the destination row for a link.
    The API payload is passed through so new fields are picked up automatically; the two
    fields the connector relies on for keys and filtering are set explicitly.
    Args:
        link: The link object from the list_links response.
        workspace_id: The workspace the link belongs to.
        is_deleted: Whether the link came from the trashed-links listing.
    Returns:
        The row dictionary to upsert.
    """
    row = dict(link)
    row["id"] = int(link["id"])
    row["workspace_id"] = int(link.get("workspace_id") or workspace_id)
    row["deleted"] = bool(link.get("deleted")) or is_deleted
    return row


def sync_domains(session: requests.Session, base_url: str, workspace_id: int, state: dict):
    """
    Re-import the custom domains configured for one workspace. The endpoint is not paginated.
    The domain names delivered for each workspace are kept in state, so a domain that was
    removed in Linkly since the previous sync is deleted from the destination.
    Args:
        session: The authenticated HTTP session.
        base_url: The API base URL.
        workspace_id: The workspace whose domains are synced.
        state: The connector state; domain_names_by_workspace holds the names last delivered.
    """
    body = get_json(session, base_url, f"workspace/{workspace_id}/domains")
    domain_names = {domain["name"] for domain in body.get("domains") or [] if domain.get("name")}
    for domain_name in sorted(domain_names):
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table=__DOMAIN_TABLE, data={"workspace_id": workspace_id, "name": domain_name})

    domain_state = state.setdefault(__STATE_DOMAIN_NAMES, {})
    removed_names = set(domain_state.get(str(workspace_id)) or []) - domain_names
    for domain_name in sorted(removed_names):
        # The 'delete' operation is used to delete data from the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the primary key of the record to be deleted.
        op.delete(table=__DOMAIN_TABLE, keys={"workspace_id": workspace_id, "name": domain_name})
    domain_state[str(workspace_id)] = sorted(domain_names)
    log.info(
        f"Workspace {workspace_id}: synced {len(domain_names)} domain(s), "
        f"deleted {len(removed_names)} removed domain(s)"
    )

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state)


def sync_click_daily(
    session: requests.Session, base_url: str, workspace_id: int, start_date: date, state: dict
):
    """
    Incrementally sync clicks per UTC day for one workspace.
    The cursor is the last date delivered for the workspace. Each sync restarts
    __CLICK_REPLAY_DAYS before the cursor so that the current and previous day, whose totals are
    still moving, are overwritten with final numbers. History is backfilled in windows of
    __CLICK_WINDOW_DAYS with a checkpoint after every window.
    Args:
        session: The authenticated HTTP session.
        base_url: The API base URL.
        workspace_id: The workspace whose clicks are synced.
        start_date: The first day to sync when the workspace has no cursor yet.
        state: The connector state; click_cursor_by_workspace holds the per-workspace cursor.
    """
    cursors = state.setdefault(__STATE_CLICK_CURSOR, {})
    today = datetime.now(timezone.utc).date()
    cursor = cursors.get(str(workspace_id))
    if cursor:
        window_start = max(
            start_date, date.fromisoformat(cursor) - timedelta(days=__CLICK_REPLAY_DAYS)
        )
    else:
        window_start = start_date

    day_count = 0
    while window_start <= today:
        window_end = min(window_start + timedelta(days=__CLICK_WINDOW_DAYS - 1), today)
        day_count += upsert_click_window(session, base_url, workspace_id, window_start, window_end)
        cursors[str(workspace_id)] = window_end.isoformat()

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state)
        window_start = window_end + timedelta(days=1)

    log.info(f"Workspace {workspace_id}: synced {day_count} day(s) of clicks")


def upsert_click_window(
    session: requests.Session,
    base_url: str,
    workspace_id: int,
    window_start: date,
    window_end: date,
):
    """
    Fetch one date window of daily click totals (with and without bots) and upsert it.
    Args:
        session: The authenticated HTTP session.
        base_url: The API base URL.
        workspace_id: The workspace to fetch clicks for.
        window_start: First day of the window, inclusive.
        window_end: Last day of the window, inclusive.
    Returns:
        The number of days upserted.
    """
    params = {
        "start": window_start.isoformat(),
        "end": window_end.isoformat(),
        "frequency": "day",
        "timezone": "UTC",
    }
    all_clicks = fetch_click_series(session, base_url, workspace_id, params)
    human_clicks = fetch_click_series(session, base_url, workspace_id, {**params, "bots": "false"})
    days = sorted(set(all_clicks) | set(human_clicks))
    for day in days:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(
            table=__CLICK_DAILY_TABLE,
            data={
                "workspace_id": workspace_id,
                "date": day,
                "clicks": all_clicks.get(day, 0),
                "human_clicks": human_clicks.get(day, 0),
            },
        )
    return len(days)


def fetch_click_series(session: requests.Session, base_url: str, workspace_id: int, params: dict):
    """
    Call the workspace clicks endpoint and return the daily series as a dictionary.
    Args:
        session: The authenticated HTTP session.
        base_url: The API base URL.
        workspace_id: The workspace to fetch clicks for.
        params: Query parameters (date range, frequency, timezone, optional bots filter).
    Returns:
        A dictionary of {"YYYY-MM-DD": clicks}.
    """
    body = get_json(session, base_url, f"workspace/{workspace_id}/clicks", params)
    series = {}
    for point in body.get("traffic") or []:
        if point.get("t"):
            series[str(point["t"])[:10]] = int(point.get("y") or 0)
    return series


def sync_conversions(session: requests.Session, base_url: str, state: dict):
    """
    Incrementally sync conversion events.
    The conversions endpoint returns the most recent rows (at most __CONVERSION_LIMIT) with no
    pagination and no date filter. Ids are ULIDs, which sort chronologically, so the highest id
    delivered is the cursor and only rows with a greater id are new. Because ids are unique the
    comparison is strict. If the endpoint returns its full __CONVERSION_LIMIT rows and every one
    is newer than the cursor (or there is no cursor yet, on the initial sync), older conversions
    may not have been returned and cannot be recovered from this endpoint, so a warning is logged.
    Args:
        session: The authenticated HTTP session.
        base_url: The API base URL.
        state: The connector state; conversion_cursor holds the highest id delivered.
    """
    cursor = state.get(__STATE_CONVERSION_CURSOR)
    body = get_json(session, base_url, "conversions", {"limit": __CONVERSION_LIMIT})
    conversions = body.get("conversions") or []
    new_count = 0
    highest_id = cursor
    # Rows are upserted as they are read; no second copy of the response is built in memory.
    for conversion in conversions:
        if cursor and conversion["id"] <= cursor:
            continue
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table=__CONVERSION_TABLE, data=build_conversion_row(conversion))
        new_count += 1
        if not highest_id or conversion["id"] > highest_id:
            highest_id = conversion["id"]

    if new_count:
        state[__STATE_CONVERSION_CURSOR] = highest_id
    if new_count == len(conversions) == __CONVERSION_LIMIT:
        if cursor:
            log.warning(
                f"All {__CONVERSION_LIMIT} returned conversions are newer than the cursor; "
                "conversions recorded between syncs may have been missed. Sync more frequently."
            )
        else:
            log.warning(
                f"The initial sync received the maximum of {__CONVERSION_LIMIT} conversions; "
                "older conversions may exist that the Linkly conversions endpoint cannot return."
            )
    log.info(f"Synced {new_count} new conversion(s) of {len(conversions)} returned")

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state)


def build_conversion_row(conversion: dict):
    """
    Build the destination row for a conversion.
    The API payload is passed through; metadata stays a Python object because the column is
    declared as JSON in schema() and the SDK serialises it (pre-encoding would double-encode).
    Args:
        conversion: The conversion object from the conversions response.
    Returns:
        The row dictionary to upsert.
    """
    row = dict(conversion)
    if row.get("link_id") is not None:
        row["link_id"] = int(row["link_id"])
    return row


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
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
