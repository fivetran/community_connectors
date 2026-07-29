"""beehiiv API Connector for Fivetran Connector SDK.
This connector syncs newsletter data from the beehiiv API into your destination.
It supports incremental sync for high-volume tables and full sync for low-volume reference data.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.
"""

# For adding delay between retries
import time

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For making HTTP requests to the beehiiv API
import requests

# For date manipulation in engagement date-range queries
from datetime import datetime, timedelta, timezone

# Constants for API configuration
__API_BASE_URL = "https://api.beehiiv.com/v2"
__PAGE_LIMIT = 100  # Maximum records per API request
__MAX_RETRIES = 5  # Maximum number of retry attempts for rate-limited or failed requests
__INITIAL_BACKOFF_SEC = 1  # Initial backoff delay in seconds for retries
__MAX_BACKOFF_SEC = 60  # Maximum backoff delay in seconds
__REQUEST_TIMEOUT_SEC = 30  # Timeout for each API request in seconds
__CHECKPOINT_INTERVAL = 1000  # Checkpoint state after every N records for high-volume tables
__ENGAGEMENTS_LOOKBACK_DAYS = 90  # Default lookback window for first engagement sync
__ENGAGEMENTS_MAX_DAYS_PER_REQUEST = 31  # Maximum days per engagement API request
__RATE_LIMIT_STATUS_CODE = 429  # HTTP status code for rate limiting
__SERVER_ERROR_MIN_STATUS = 500  # Minimum HTTP status code for server errors


def validate_configuration(configuration: dict):
    """Validate the configuration dictionary to ensure it contains all required parameters.

    Args:
        configuration: A dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: If any required configuration parameter is missing.
    """
    required_configs = ["api_key", "publication_id"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def schema(configuration: dict):
    """Define the schema for all destination tables.

    All tables use 'id' as the primary key (STRING) except engagements which uses 'date'.
    Nested objects are stored as VARIANT columns
    (the SDK infers JSON columns automatically when dicts/lists are upserted).

    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema

    Args:
        configuration: A dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": "publications", "primary_key": ["id"]},
        {"table": "subscriptions", "primary_key": ["id"]},
        {"table": "posts", "primary_key": ["id"]},
        {"table": "email_blasts", "primary_key": ["id"]},
        {"table": "automations", "primary_key": ["id"]},
        {"table": "automation_journeys", "primary_key": ["id"]},
        {"table": "authors", "primary_key": ["id"]},
        {"table": "segments", "primary_key": ["id"]},
        {"table": "custom_fields", "primary_key": ["id"]},
        {"table": "newsletter_lists", "primary_key": ["id"]},
        {"table": "tiers", "primary_key": ["id"]},
        {"table": "referral_program", "primary_key": ["id"]},
        {"table": "polls", "primary_key": ["id"]},
        {"table": "condition_sets", "primary_key": ["id"]},
        {"table": "post_templates", "primary_key": ["id"]},
        {"table": "engagements", "primary_key": ["date"]},
        {"table": "advertisement_opportunities", "primary_key": ["id"]},
    ]


def update(configuration: dict, state: dict):
    """Sync data from the beehiiv API into all destination tables.

    This function is called by Fivetran during each sync. It handles both incremental
    and full-sync tables, checkpointing state after each table completes.

    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

    Args:
        configuration: A dictionary containing connection details (api_key, publication_id).
        state: A dictionary containing state information from previous runs.
               The state dictionary is empty for the first sync or for any full re-sync.
    """
    log.info("beehiiv connector: starting sync")

    validate_configuration(configuration)

    api_key = configuration["api_key"]
    publication_id = configuration["publication_id"]

    # Sync each table, updating state progressively
    state = sync_publications(api_key, publication_id, state)
    state = sync_subscriptions(api_key, publication_id, state)
    state = sync_posts(api_key, publication_id, state)
    state = sync_email_blasts(api_key, publication_id, state)
    state = sync_automations_and_journeys(api_key, publication_id, state)
    state = sync_simple_page_table(api_key, publication_id, state, "authors", "/authors")
    state = sync_simple_page_table(
        api_key, publication_id, state, "segments", "/segments", expand=["stats"]
    )
    state = sync_simple_page_table(
        api_key, publication_id, state, "custom_fields", "/custom_fields"
    )
    state = sync_simple_page_table(
        api_key, publication_id, state, "newsletter_lists", "/newsletter_lists"
    )
    state = sync_simple_page_table(
        api_key, publication_id, state, "tiers", "/tiers", expand=["stats", "prices"]
    )
    state = sync_referral_program(api_key, publication_id, state)
    state = sync_simple_cursor_table(
        api_key, publication_id, state, "polls", "/polls", expand=["stats"]
    )
    state = sync_simple_cursor_table(
        api_key, publication_id, state, "condition_sets", "/condition_sets"
    )
    state = sync_simple_page_table(
        api_key, publication_id, state, "post_templates", "/post_templates"
    )
    state = sync_engagements(api_key, publication_id, state)
    state = sync_advertisement_opportunities(api_key, publication_id, state)

    log.info("beehiiv connector: sync completed")


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def make_api_request(url, api_key, params=None):
    """Make an authenticated GET request to the beehiiv API with retry logic.

    Implements exponential backoff for rate-limited (429) and server error (5xx) responses.

    Args:
        url: The full API endpoint URL.
        api_key: The beehiiv API bearer token.
        params: Optional dictionary of query parameters.

    Returns:
        The parsed JSON response body as a dictionary.

    Raises:
        RuntimeError: If the request fails after all retries.
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    backoff = __INITIAL_BACKOFF_SEC

    for attempt in range(1, __MAX_RETRIES + 1):
        try:
            response = requests.get(
                url, headers=headers, params=params, timeout=__REQUEST_TIMEOUT_SEC
            )

            if response.status_code == __RATE_LIMIT_STATUS_CODE:
                if attempt == __MAX_RETRIES:
                    raise RuntimeError(f"Rate limited after {__MAX_RETRIES} retries: {url}")
                log.warning(
                    f"Rate limited on {url}. Retrying in {backoff}s "
                    f"(attempt {attempt}/{__MAX_RETRIES})"
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, __MAX_BACKOFF_SEC)
                continue

            if __SERVER_ERROR_MIN_STATUS <= response.status_code < 600:
                if attempt == __MAX_RETRIES:
                    raise RuntimeError(
                        f"Server error {response.status_code} after {__MAX_RETRIES} "
                        f"retries: {url}"
                    )
                log.warning(
                    f"Server error {response.status_code} on {url}. "
                    f"Retrying in {backoff}s (attempt {attempt}/{__MAX_RETRIES})"
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, __MAX_BACKOFF_SEC)
                continue

            response.raise_for_status()
            return response.json()

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt == __MAX_RETRIES:
                raise RuntimeError(f"Request failed after {__MAX_RETRIES} retries: {url}: {e}")
            log.warning(
                f"{type(e).__name__} on {url}. Retrying in {backoff}s "
                f"(attempt {attempt}/{__MAX_RETRIES})"
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, __MAX_BACKOFF_SEC)

    raise RuntimeError(f"Unexpected failure after {__MAX_RETRIES} retries: {url}")


# ---------------------------------------------------------------------------
# Pagination helpers
# ---------------------------------------------------------------------------


def paginate_page_number(url, api_key, extra_params=None):
    """Yield records from a page-number paginated beehiiv endpoint.

    Loops page=1,2,... with limit=100 until page >= total_pages.

    Args:
        url: The full API endpoint URL (without pagination params).
        api_key: The beehiiv API bearer token.
        extra_params: Optional dict of additional query parameters (expand, status, etc.).

    Yields:
        Individual record dictionaries from the 'data' array in each response.
    """
    page = 1
    total_pages = 1  # Will be updated from first response

    while page <= total_pages:
        params = {"limit": __PAGE_LIMIT, "page": page}
        if extra_params:
            params.update(extra_params)

        response_json = make_api_request(url, api_key, params)
        data = response_json.get("data", [])
        total_pages = response_json.get("total_pages", 1)

        for record in data:
            yield record

        page += 1


def paginate_cursor(url, api_key, extra_params=None):
    """Yield records from a cursor-based paginated beehiiv endpoint.

    Passes cursor from next_cursor in each response. Loops until has_more == false.

    Args:
        url: The full API endpoint URL (without pagination params).
        api_key: The beehiiv API bearer token.
        extra_params: Optional dict of additional query parameters (expand, etc.).

    Yields:
        Individual record dictionaries from the 'data' array in each response.
    """
    cursor = None

    while True:
        params = {"limit": __PAGE_LIMIT}
        if cursor:
            params["cursor"] = cursor
        if extra_params:
            params.update(extra_params)

        response_json = make_api_request(url, api_key, params)
        data = response_json.get("data", [])

        for record in data:
            yield record

        if not response_json.get("has_more", False):
            break

        cursor = response_json.get("next_cursor")
        if not cursor:
            break


# ---------------------------------------------------------------------------
# Table sync functions
# ---------------------------------------------------------------------------


def sync_publications(api_key, publication_id, state):
    """Sync the publications table (single record for the configured publication).

    Args:
        api_key: The beehiiv API bearer token.
        publication_id: The publication to fetch.
        state: Current connector state.

    Returns:
        Updated state dictionary.
    """
    log.info("Syncing publications")
    url = f"{__API_BASE_URL}/publications/{publication_id}"
    params = {"expand[]": "stats"}
    response_json = make_api_request(url, api_key, params)

    # The show endpoint returns the publication directly (not wrapped in 'data' array)
    publication = response_json.get("data", response_json)
    op.upsert(table="publications", data=publication)

    op.checkpoint(state)
    log.info("Publications sync complete")
    return state


def sync_subscriptions(api_key, publication_id, state):
    """Sync subscriptions using cursor-based pagination with incremental sync.

    Uses the created timestamp to fetch only new subscriptions since the last sync.

    Args:
        api_key: The beehiiv API bearer token.
        publication_id: The publication to fetch subscriptions for.
        state: Current connector state.

    Returns:
        Updated state dictionary.
    """
    log.info("Syncing subscriptions")
    state_key = "subscriptions_last_created"
    last_created = state.get(state_key)

    url = f"{__API_BASE_URL}/publications/{publication_id}/subscriptions"
    expand_params = {
        "expand[]": [
            "stats",
            "custom_fields",
            "subscription_premium_tiers",
            "newsletter_lists",
        ]
    }

    records_processed = 0
    new_last_created = last_created

    for record in paginate_cursor(url, api_key, expand_params):
        record_created = record.get("created")

        # Skip records we have already synced (client-side incremental filter)
        if last_created and record_created and record_created <= last_created:
            continue

        op.upsert(table="subscriptions", data=record)
        records_processed += 1

        if record_created and (new_last_created is None or record_created > new_last_created):
            new_last_created = record_created

        # Checkpoint periodically for high-volume table
        if records_processed % __CHECKPOINT_INTERVAL == 0:
            state[state_key] = new_last_created
            op.checkpoint(state)
            log.info(f"Subscriptions: checkpointed after {records_processed} records")

    if new_last_created is not None:
        state[state_key] = new_last_created

    op.checkpoint(state)
    log.info(f"Subscriptions sync complete: {records_processed} records")
    return state


def sync_posts(api_key, publication_id, state):
    """Sync posts using page-number pagination with incremental sync.

    Orders by created ascending and tracks the last created timestamp. Strips any
    accidentally included content fields to avoid syncing large HTML blobs.

    Args:
        api_key: The beehiiv API bearer token.
        publication_id: The publication to fetch posts for.
        state: Current connector state.

    Returns:
        Updated state dictionary.
    """
    log.info("Syncing posts")
    state_key = "posts_last_created"
    last_created = state.get(state_key)

    url = f"{__API_BASE_URL}/publications/{publication_id}/posts"
    extra_params = {
        "expand[]": "stats",
        "order_by": "created",
        "direction": "asc",
    }

    records_processed = 0
    new_last_created = last_created

    for record in paginate_page_number(url, api_key, extra_params):
        record_created = record.get("created")

        # Skip records we have already synced (incremental filter)
        if last_created and record_created and record_created <= last_created:
            continue

        # Remove content fields that may have leaked via expand params
        for content_key in (
            "free_email_content",
            "premium_email_content",
            "free_web_content",
            "premium_web_content",
            "free_rss_content",
            "content",
        ):
            record.pop(content_key, None)

        op.upsert(table="posts", data=record)
        records_processed += 1

        if record_created and (new_last_created is None or record_created > new_last_created):
            new_last_created = record_created

    if new_last_created is not None:
        state[state_key] = new_last_created

    op.checkpoint(state)
    log.info(f"Posts sync complete: {records_processed} records")
    return state


def sync_email_blasts(api_key, publication_id, state):
    """Sync email blasts using page-number pagination with incremental sync.

    Fetches all statuses (active and inactive) and tracks the created timestamp.

    Args:
        api_key: The beehiiv API bearer token.
        publication_id: The publication to fetch email blasts for.
        state: Current connector state.

    Returns:
        Updated state dictionary.
    """
    log.info("Syncing email_blasts")
    state_key = "email_blasts_last_created"
    last_created = state.get(state_key)

    url = f"{__API_BASE_URL}/publications/{publication_id}/email_blasts"
    extra_params = {
        "expand[]": "stats",
        "status": "all",
    }

    records_processed = 0
    new_last_created = last_created

    for record in paginate_page_number(url, api_key, extra_params):
        record_created = record.get("created")

        if last_created and record_created and record_created <= last_created:
            continue

        op.upsert(table="email_blasts", data=record)
        records_processed += 1

        if record_created and (new_last_created is None or record_created > new_last_created):
            new_last_created = record_created

    if new_last_created is not None:
        state[state_key] = new_last_created

    op.checkpoint(state)
    log.info(f"Email blasts sync complete: {records_processed} records")
    return state


def sync_automations_and_journeys(api_key, publication_id, state):
    """Sync automations and their nested automation journeys.

    Automations are fetched with page-number pagination. For each automation, its
    journeys are fetched from the nested endpoint and upserted into a separate table.

    Args:
        api_key: The beehiiv API bearer token.
        publication_id: The publication to fetch automations for.
        state: Current connector state.

    Returns:
        Updated state dictionary.
    """
    log.info("Syncing automations and automation_journeys")

    automations_url = f"{__API_BASE_URL}/publications/{publication_id}/automations"
    extra_params = {"expand[]": "stats"}
    automation_count = 0
    journey_count = 0

    for automation in paginate_page_number(automations_url, api_key, extra_params):
        op.upsert(table="automations", data=automation)
        automation_count += 1

        # Fetch journeys for this automation
        automation_id = automation.get("id")
        if automation_id:
            journeys_url = (
                f"{__API_BASE_URL}/publications/{publication_id}"
                f"/automations/{automation_id}/journeys"
            )
            for journey in paginate_page_number(journeys_url, api_key):
                op.upsert(table="automation_journeys", data=journey)
                journey_count += 1

    op.checkpoint(state)
    log.info(
        f"Automations sync complete: {automation_count} automations, " f"{journey_count} journeys"
    )
    return state


def sync_simple_page_table(api_key, publication_id, state, table_name, endpoint_path, expand=None):
    """Sync a low-volume table using page-number pagination (full sync each run).

    Args:
        api_key: The beehiiv API bearer token.
        publication_id: The publication to fetch data for.
        state: Current connector state.
        table_name: The destination table name.
        endpoint_path: The API path suffix (e.g., '/authors').
        expand: Optional list of expand parameter values.

    Returns:
        Updated state dictionary.
    """
    log.info(f"Syncing {table_name}")
    url = f"{__API_BASE_URL}/publications/{publication_id}{endpoint_path}"
    extra_params = {}
    if expand:
        extra_params["expand[]"] = expand

    count = 0
    for record in paginate_page_number(url, api_key, extra_params or None):
        op.upsert(table=table_name, data=record)
        count += 1

    op.checkpoint(state)
    log.info(f"{table_name} sync complete: {count} records")
    return state


def sync_simple_cursor_table(
    api_key, publication_id, state, table_name, endpoint_path, expand=None
):
    """Sync a table using cursor-based pagination (full sync each run).

    Args:
        api_key: The beehiiv API bearer token.
        publication_id: The publication to fetch data for.
        state: Current connector state.
        table_name: The destination table name.
        endpoint_path: The API path suffix (e.g., '/polls').
        expand: Optional list of expand parameter values.

    Returns:
        Updated state dictionary.
    """
    log.info(f"Syncing {table_name}")
    url = f"{__API_BASE_URL}/publications/{publication_id}{endpoint_path}"
    extra_params = {}
    if expand:
        extra_params["expand[]"] = expand

    count = 0
    for record in paginate_cursor(url, api_key, extra_params or None):
        op.upsert(table=table_name, data=record)
        count += 1

    op.checkpoint(state)
    log.info(f"{table_name} sync complete: {count} records")
    return state


def sync_referral_program(api_key, publication_id, state):
    """Sync referral program milestones using page-number pagination.

    The referral_program endpoint returns a paginated list of milestone records.

    Args:
        api_key: The beehiiv API bearer token.
        publication_id: The publication to fetch the referral program for.
        state: Current connector state.

    Returns:
        Updated state dictionary.
    """
    log.info("Syncing referral_program")
    url = f"{__API_BASE_URL}/publications/{publication_id}/referral_program"

    try:
        count = 0
        for record in paginate_page_number(url, api_key):
            op.upsert(table="referral_program", data=record)
            count += 1
        log.info(f"Referral program sync complete: {count} records")
    except (RuntimeError, requests.exceptions.HTTPError) as e:
        log.warning(f"Could not fetch referral_program: {e}")

    op.checkpoint(state)
    return state


def sync_engagements(api_key, publication_id, state):
    """Sync daily engagement metrics using date-range based fetching.

    Fetches from the last synced date (or 90 days ago on first sync) to today,
    making requests in chunks of up to 31 days (API maximum).

    Args:
        api_key: The beehiiv API bearer token.
        publication_id: The publication to fetch engagements for.
        state: Current connector state.

    Returns:
        Updated state dictionary.
    """
    log.info("Syncing engagements")
    state_key = "engagements_last_date"
    last_date_str = state.get(state_key)

    today = datetime.now(timezone.utc).date()

    if last_date_str:
        start_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()
    else:
        start_date = today - timedelta(days=__ENGAGEMENTS_LOOKBACK_DAYS)

    url = f"{__API_BASE_URL}/publications/{publication_id}/engagements"
    records_processed = 0

    last_successful_date = start_date

    while start_date <= today:
        days_to_fetch = min(__ENGAGEMENTS_MAX_DAYS_PER_REQUEST, (today - start_date).days + 1)
        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "number_of_days": days_to_fetch,
        }

        try:
            response_json = make_api_request(url, api_key, params)
            data = response_json.get("data", [])
            for record in data:
                op.upsert(table="engagements", data=record)
                records_processed += 1
            last_successful_date = start_date + timedelta(days=days_to_fetch)
        except (RuntimeError, requests.exceptions.HTTPError) as e:
            log.warning(f"Engagements fetch failed for {start_date}: {e}")
            break

        start_date += timedelta(days=days_to_fetch)

    state[state_key] = last_successful_date.strftime("%Y-%m-%d")
    op.checkpoint(state)
    log.info(f"Engagements sync complete: {records_processed} records")
    return state


def sync_advertisement_opportunities(api_key, publication_id, state):
    """Sync advertisement opportunities (single response, no pagination).

    Args:
        api_key: The beehiiv API bearer token.
        publication_id: The publication to fetch ad opportunities for.
        state: Current connector state.

    Returns:
        Updated state dictionary.
    """
    log.info("Syncing advertisement_opportunities")
    url = f"{__API_BASE_URL}/publications/{publication_id}" f"/advertisement_opportunities"

    try:
        response_json = make_api_request(url, api_key)
        data = response_json.get("data", [])
        count = 0
        for record in data:
            op.upsert(table="advertisement_opportunities", data=record)
            count += 1
        log.info(f"Advertisement opportunities sync complete: {count} records")
    except (RuntimeError, requests.exceptions.HTTPError) as e:
        log.warning(f"Could not fetch advertisement_opportunities: {e}")

    op.checkpoint(state)
    return state


# Create the connector object used by Fivetran to run the connector
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    import json

    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    # Run the connector in test mode with the loaded configuration
    connector.debug(configuration=configuration)
