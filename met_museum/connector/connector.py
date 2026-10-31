"""This connector syncs public-domain objects from The Metropolitan Museum of Art Collection API, with
optional Snowflake Cortex vision enrichment of each object's image.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file and for the configuration fingerprint
import json

# For adding jitter to retry delays
import random

# For waiting between requests and retries
import time

# For computing the incremental metadataDate cursor
from datetime import datetime, timedelta, timezone

# For making HTTP requests to the Collection API
import requests

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For the optional Cortex vision enrichment
import cortex

# For mapping API responses to destination rows
from transform import (
    as_int,
    build_constituent_rows,
    build_department_row,
    build_object_row,
    build_tag_rows,
    tag_terms,
)

# The Collection API. It needs no authentication.
__BASE_URL = "https://collectionapi.metmuseum.org/public/collection/v1"

# Identifies this example in the Met's server logs.
__USER_AGENT = "fivetran-connector-sdk-met-museum-example"

__DEPARTMENT_TABLE = "department"
__OBJECT_TABLE = "object"
__CONSTITUENT_TABLE = "object_constituent"
__TAG_TABLE = "object_tag"
__VISUAL_ATTRIBUTE_TABLE = "object_visual_attribute"
__DEPICTED_ELEMENT_TABLE = "object_depicted_element"

# The API's CDN blocks a client for about a minute once it sends about 80 requests within a minute. A gap
# of 0.8 seconds between requests is 75 a minute, so a long sync never reaches the block.
__MIN_REQUEST_INTERVAL_SECONDS = 0.8

# Attempts for transient failures, with exponential backoff and up to a second of jitter.
__REQUEST_TIMEOUT_SECONDS = 30
__MAX_RETRIES = 5
__BASE_DELAY_SECONDS = 2
__MAX_JITTER_SECONDS = 1
__MAX_DELAY_SECONDS = 60
__RETRYABLE_STATUS_CODES = (429, 500, 502, 503, 504)

# The CDN answers a client it has rate-blocked with HTTP 403. The block clears in under a minute.
__BLOCKED_STATUS_CODE = 403
__BLOCKED_DELAY_SECONDS = 60

# The body of the 404 the API returns for an object id it does not have. Any other 404 is an error,
# never a reason to delete a row.
__OBJECT_NOT_FOUND_MESSAGE = "ObjectID not found"
__NOT_FOUND_STATUS_CODE = 404

# The title of the page the image host returns for a file it does not have.
__IMAGE_NOT_FOUND_TITLE = "404 - File or directory not found."

# How much of an error response body goes into the error message.
__ERROR_BODY_PREVIEW_CHARS = 300

# Objects processed between checkpoints. At three requests a second this is under ten seconds of work.
__CHECKPOINT_EVERY_OBJECTS = 25

# Object detail requests per sync. A capped sync resumes at the next object id on the next sync.
__DEFAULT_MAX_OBJECTS_PER_SYNC = 1000
__MAX_OBJECTS_PER_SYNC_CEILING = 100000

# Cortex calls per sync, across every department.
__DEFAULT_MAX_ENRICHMENTS_PER_SYNC = 25
__MAX_ENRICHMENTS_PER_SYNC_CEILING = 5000

# Vision needs a Claude model; claude-sonnet-5 reads images through the Cortex REST API.
__DEFAULT_CORTEX_MODEL = "claude-sonnet-5"

# The Cortex request limit is 20 MiB, and base64 adds a third. The small images are far below this.
__MAX_IMAGE_BYTES = 10 * 1024 * 1024
__IMAGE_CONTENT_TYPE_PREFIX = "image/"
__IMAGE_ACCEPT = "image/*"

# metadataDate takes a date, not a time, and returns objects changed on or after it. The cursor is the
# day before the pass started, which re-reads a day of changes and absorbs the daily update batch.
__METADATA_DATE_FORMAT = "%Y-%m-%d"
__CURSOR_OVERLAP = timedelta(days=1)

__TRUE_TOKENS = ("true", "yes", "1")
__FALSE_TOKENS = ("false", "no", "0")

# Every state key. resolve_state gives each one a value, so the sync reads any key directly.
__STATE_KEYS = (
    "department_fingerprint",
    "enrichment_model",
    "metadata_cursor",
    "pending_cursor",
    "departments_done",
    "current_department_id",
    "after_object_id",
    "department_ids_synced",
    "enrichment_hashes",
    "pending_enrichment",
)


def is_placeholder(value) -> bool:
    """
    Report whether a configuration value is unset: missing, blank, or an angle-bracket placeholder.
    Args:
        value: the raw configuration value.
    Returns:
        True when the value is unset.
    """
    if value is None:
        return True
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    return not stripped or (stripped.startswith("<") and stripped.endswith(">"))


def get_config_value(configuration: dict, key: str, default=None):
    """
    Read an optional configuration value, treating missing, blank and placeholder values as unset.
    Args:
        configuration: the configuration dictionary.
        key: the configuration key.
        default: the value to return when the key is unset.
    Returns:
        The stripped string value, or the default.
    """
    value = configuration.get(key)
    if is_placeholder(value):
        return default
    return value.strip() if isinstance(value, str) else value


def parse_department_ids(value) -> list:
    """
    Parse department_ids, a comma-separated list of Met department ids such as "11" or "11, 21".
    Args:
        value: the raw configuration value.
    Returns:
        The distinct ids, sorted.
    Raises:
        ValueError: if the value is unset, or any entry is not a positive whole number.
    """
    if is_placeholder(value) or not isinstance(value, str):
        raise ValueError(
            "Missing required configuration value: department_ids (for example 11 for European Paintings)."
        )
    ids = set()
    for part in value.split(","):
        entry = part.strip()
        if not entry.isdigit() or int(entry) <= 0:
            raise ValueError(
                f"Invalid department_ids entry {entry!r}: expected positive whole numbers separated by commas."
            )
        ids.add(int(entry))
    return sorted(ids)


def parse_positive_int(configuration: dict, key: str, default: int, ceiling: int) -> int:
    """
    Parse an optional positive whole-number setting.
    Args:
        configuration: the configuration dictionary.
        key: the configuration key.
        default: the value when the key is unset.
        ceiling: the largest accepted value.
    Returns:
        The parsed value, or the default.
    Raises:
        ValueError: if the value is not a whole number from 1 to the ceiling.
    """
    value = get_config_value(configuration, key)
    if value is None:
        return default
    text = str(value)
    if not text.isdigit() or not 0 < int(text) <= ceiling:
        raise ValueError(f"Invalid {key} {value!r}: expected a whole number from 1 to {ceiling}.")
    return int(text)


def parse_bool(configuration: dict, key: str) -> bool:
    """
    Parse an optional true/false setting, which defaults to false.
    Args:
        configuration: the configuration dictionary.
        key: the configuration key.
    Returns:
        True or False.
    Raises:
        ValueError: if the value is not a recognized true or false word.
    """
    value = get_config_value(configuration, key, "false")
    token = str(value).lower()
    if token in __TRUE_TOKENS:
        return True
    if token in __FALSE_TOKENS:
        return False
    raise ValueError(f"Invalid {key} {value!r}: expected true or false.")


def validate_configuration(configuration: dict) -> dict:
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        The parsed settings.
    Raises:
        ValueError: if any configuration value is missing or invalid.
    """
    settings = {
        "department_ids": parse_department_ids(configuration.get("department_ids")),
        "max_objects_per_sync": parse_positive_int(
            configuration,
            "max_objects_per_sync",
            __DEFAULT_MAX_OBJECTS_PER_SYNC,
            __MAX_OBJECTS_PER_SYNC_CEILING,
        ),
        "enable_cortex": parse_bool(configuration, "enable_cortex"),
        "max_enrichments_per_sync": parse_positive_int(
            configuration,
            "max_enrichments_per_sync",
            __DEFAULT_MAX_ENRICHMENTS_PER_SYNC,
            __MAX_ENRICHMENTS_PER_SYNC_CEILING,
        ),
        "snowflake_account": get_config_value(configuration, "snowflake_account"),
        "snowflake_pat_token": get_config_value(configuration, "snowflake_pat_token"),
        "cortex_model": get_config_value(configuration, "cortex_model", __DEFAULT_CORTEX_MODEL),
    }
    if settings["enable_cortex"]:
        account = settings["snowflake_account"] or ""
        if account.lower().startswith(("http://", "https://")):
            raise ValueError(
                "snowflake_account must be a hostname such as abc12345.us-east-1.snowflakecomputing.com, "
                "without https://."
            )
        cortex.validate_settings(
            settings["snowflake_account"],
            settings["snowflake_pat_token"],
            settings["cortex_model"],
        )
    return settings


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
            "table": __OBJECT_TABLE,
            "primary_key": ["object_id"],
            "columns": {
                "object_id": "INT",
                "department_id": "INT",
                "department": "STRING",
                "is_public_domain": "BOOLEAN",
                "is_highlight": "BOOLEAN",
                "is_timeline_work": "BOOLEAN",
                "accession_number": "STRING",
                "accession_year": "STRING",
                "primary_image": "STRING",
                "primary_image_small": "STRING",
                "additional_image_count": "INT",
                "object_name": "STRING",
                "title": "STRING",
                "culture": "STRING",
                "period": "STRING",
                "dynasty": "STRING",
                "reign": "STRING",
                "portfolio": "STRING",
                "artist_role": "STRING",
                "artist_prefix": "STRING",
                "artist_display_name": "STRING",
                "artist_display_bio": "STRING",
                "artist_suffix": "STRING",
                "artist_alpha_sort": "STRING",
                "artist_nationality": "STRING",
                "artist_begin_date": "STRING",
                "artist_end_date": "STRING",
                "artist_gender": "STRING",
                "artist_wikidata_url": "STRING",
                "artist_ulan_url": "STRING",
                "object_date": "STRING",
                "object_begin_date": "INT",
                "object_end_date": "INT",
                "medium": "STRING",
                "dimensions": "STRING",
                "credit_line": "STRING",
                "geography_type": "STRING",
                "city": "STRING",
                "state_province": "STRING",
                "county": "STRING",
                "country": "STRING",
                "region": "STRING",
                "subregion": "STRING",
                "locale": "STRING",
                "locus": "STRING",
                "excavation": "STRING",
                "river": "STRING",
                "classification": "STRING",
                "rights_and_reproduction": "STRING",
                "link_resource": "STRING",
                "repository": "STRING",
                "object_url": "STRING",
                "object_wikidata_url": "STRING",
                "gallery_number": "STRING",
                "metadata_date": "UTC_DATETIME",
            },
        },
        {
            "table": __DEPARTMENT_TABLE,
            "primary_key": ["department_id"],
            "columns": {"department_id": "INT", "display_name": "STRING"},
        },
        {
            "table": __CONSTITUENT_TABLE,
            "primary_key": ["object_id", "constituent_id", "role"],
            "columns": {
                "object_id": "INT",
                "constituent_id": "INT",
                "role": "STRING",
                "name": "STRING",
                "gender": "STRING",
                "constituent_ulan_url": "STRING",
                "constituent_wikidata_url": "STRING",
                "object_metadata_date": "UTC_DATETIME",
            },
        },
        {
            "table": __TAG_TABLE,
            "primary_key": ["object_id", "term"],
            "columns": {
                "object_id": "INT",
                "term": "STRING",
                "aat_url": "STRING",
                "wikidata_url": "STRING",
                "object_metadata_date": "UTC_DATETIME",
            },
        },
        {
            "table": __VISUAL_ATTRIBUTE_TABLE,
            "primary_key": ["object_id"],
            "columns": {
                "object_id": "INT",
                "subject_category": "STRING",
                "scene_type": "STRING",
                "style_period": "STRING",
                "dominant_colors": "JSON",
                "materials_visible": "JSON",
                "visual_description": "STRING",
                "depicted_element_count": "INT",
                "curator_tag_agreement": "DOUBLE",
                "curator_tags_matched": "INT",
                "curator_tags_missed": "INT",
                "enrichment_model": "STRING",
                "enrichment_hash": "STRING",
                "enriched_at": "UTC_DATETIME",
            },
        },
        {
            "table": __DEPICTED_ELEMENT_TABLE,
            "primary_key": ["object_id", "element"],
            "columns": {"object_id": "INT", "element": "STRING", "enrichment_hash": "STRING"},
        },
    ]


def create_client() -> dict:
    """
    Create the Collection API client: one pooled session for the whole sync, and the time of the
    previous request, which spaces requests out.
    Returns:
        The client dictionary. The caller closes its session.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": __USER_AGENT, "Accept": "application/json"})
    return {"session": session, "last_request_at": 0.0}


def throttle(client: dict):
    """
    Wait until __MIN_REQUEST_INTERVAL_SECONDS have passed since the previous request.
    Args:
        client: the API client.
    """
    wait = client["last_request_at"] + __MIN_REQUEST_INTERVAL_SECONDS - time.monotonic()
    if wait > 0:
        time.sleep(wait)
    client["last_request_at"] = time.monotonic()


def send_request(client: dict, url: str, params=None, not_found_ok: bool = False, accept=None):
    """
    Send a GET request, retrying HTTP 403 (the CDN's rate block), 429 and 5xx, connection errors
    and timeouts. Any other error status fails at once.
    Args:
        client: the API client.
        url: the full URL.
        params: the query parameters.
        not_found_ok: return a 404 response to the caller instead of failing.
        accept: an Accept header to send instead of the session's application/json.
    Returns:
        The response, with status 200, or 404 when not_found_ok is true.
    Raises:
        RuntimeError: on a non-retryable status, any other request error, or when every attempt failed.
    """
    failure = None
    for attempt in range(1, __MAX_RETRIES + 1):
        delay = retry_delay(attempt)
        throttle(client)
        try:
            headers = {"Accept": accept} if accept else None
            response = client["session"].get(
                url, params=params, headers=headers, timeout=__REQUEST_TIMEOUT_SECONDS
            )
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as error:
            failure = f"{type(error).__name__}: {error}"
        except requests.exceptions.RequestException as error:
            raise RuntimeError(f"Met request to {url} failed: {error}") from error
        else:
            status = response.status_code
            if status == 200 or (status == __NOT_FOUND_STATUS_CODE and not_found_ok):
                return response
            if status == __BLOCKED_STATUS_CODE:
                delay = __BLOCKED_DELAY_SECONDS
            elif status not in __RETRYABLE_STATUS_CODES:
                raise RuntimeError(
                    f"Met API returned HTTP {status} for {url}: "
                    f"{response.text[:__ERROR_BODY_PREVIEW_CHARS]}"
                )
            failure = f"HTTP {status}"
        if attempt < __MAX_RETRIES:
            log.warning(f"Met request failed ({failure}); retrying in {delay:.0f}s")
            time.sleep(delay)
    raise RuntimeError(f"Met request to {url} failed after {__MAX_RETRIES} attempts: {failure}")


def get_json(client: dict, path: str, params=None) -> dict:
    """
    Request a JSON object from the API.
    Args:
        client: the API client.
        path: the path below __BASE_URL, for example /departments.
        params: the query parameters.
    Returns:
        The parsed JSON object.
    Raises:
        RuntimeError: if the request fails or the body is not a JSON object.
    """
    response = send_request(client, f"{__BASE_URL}{path}", params=params)
    return parse_json_object(response, path)


def get_object(client: dict, object_id: int):
    """
    Request one object's detail.
    Args:
        client: the API client.
        object_id: the object id.
    Returns:
        The object detail, or None when the API reports that the object does not exist.
    Raises:
        RuntimeError: if the request fails, or a 404 does not carry the API's not-found message.
    """
    path = f"/objects/{object_id}"
    response = send_request(client, f"{__BASE_URL}{path}", not_found_ok=True)
    body = parse_json_object(response, path)
    if response.status_code == __NOT_FOUND_STATUS_CODE:
        if body.get("message") != __OBJECT_NOT_FOUND_MESSAGE:
            raise RuntimeError(f"Unexpected 404 body for {path}: {body}")
        return None
    return body


def get_image(client: dict, url: str):
    """
    Download an object image for enrichment.
    Args:
        client: the API client.
        url: the primaryImageSmall URL.
    Returns:
        A tuple (bytes, content_type), or None when the image is missing, not an image, or too large.
    Raises:
        RuntimeError: if the request fails after retries, or a 404 is not the image host's not-found page.
    """
    # The image host answers some files with HTTP 406 when the request asks only for JSON.
    response = send_request(client, url, not_found_ok=True, accept=__IMAGE_ACCEPT)
    if response.status_code == __NOT_FOUND_STATUS_CODE:
        if __IMAGE_NOT_FOUND_TITLE not in response.text:
            raise RuntimeError(f"Unexpected 404 body for image {url}")
        return None
    content_type = response.headers.get("Content-Type", "").split(";")[0].strip().lower()
    if not content_type.startswith(__IMAGE_CONTENT_TYPE_PREFIX):
        return None
    if len(response.content) > __MAX_IMAGE_BYTES:
        return None
    return response.content, content_type


def retry_delay(attempt: int) -> float:
    """
    Compute the exponential backoff with jitter before the next attempt.
    Args:
        attempt: the attempt that just failed, starting at 1.
    Returns:
        The number of seconds to wait, capped at __MAX_DELAY_SECONDS.
    """
    delay = min(__BASE_DELAY_SECONDS * 2 ** (attempt - 1), __MAX_DELAY_SECONDS)
    return delay + random.uniform(0, __MAX_JITTER_SECONDS)


def parse_json_object(response: requests.Response, path: str) -> dict:
    """
    Parse a response body that must be a JSON object.
    Args:
        response: the response.
        path: the request path, for the error message.
    Returns:
        The parsed object.
    Raises:
        RuntimeError: if the body is not valid JSON or not an object.
    """
    try:
        body = response.json()
    except requests.exceptions.JSONDecodeError as error:
        raise RuntimeError(f"Met API returned invalid JSON for {path}: {error}") from error
    if not isinstance(body, dict):
        raise RuntimeError(f"Met API returned {type(body).__name__} for {path}, not an object")
    return body


def fetch_object_ids(client: dict, department_id: int, metadata_date) -> list:
    """
    List a department's object ids, or only those changed on or after metadata_date.
    Args:
        client: the API client.
        department_id: the department id.
        metadata_date: a YYYY-MM-DD date, or None for every object.
    Returns:
        The distinct ids, sorted, so the walk can resume after the last processed id.
    Raises:
        RuntimeError: if the response does not have the documented total and objectIDs shape.
    """
    params = {"departmentIds": department_id}
    if metadata_date:
        params["metadataDate"] = metadata_date
    body = get_json(client, "/objects", params=params)
    total, object_ids = body.get("total"), body.get("objectIDs")
    # An empty result is total 0 with objectIDs null. Anything else without a list is not "no objects".
    if object_ids is None and total == 0:
        return []
    if not isinstance(object_ids, list):
        raise RuntimeError(f"Unexpected object list for department {department_id}: {body}")
    return sorted({value for value in (as_int(item) for item in object_ids) if value is not None})


def save_state(state: dict):
    """
    Checkpoint the state dictionary, which always holds every state key.
    Args:
        state: the sync state.
    """
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state=state)


def reset_walk(state: dict):
    """
    Start the historical walk again: every object of every configured department is read.
    Args:
        state: the sync state.
    """
    state["metadata_cursor"] = state["pending_cursor"] = None
    state["departments_done"] = []
    state["current_department_id"] = state["after_object_id"] = None
    # The walk reads every object again, so it finds the failed enrichments itself.
    state["pending_enrichment"] = {}


def resolve_state(state: dict, settings: dict):
    """
    Prepare the state in place: make sure every key exists, then restart the historical walk when
    the configured departments changed, or when enrichment was turned on or switched to another
    model, so objects synced before are read and enriched.
    Args:
        state: the state from the previous sync, empty on the first sync.
        settings: the parsed configuration.
    """
    for key in __STATE_KEYS:
        state.setdefault(key, None)
    state["departments_done"] = state["departments_done"] or []
    state["department_ids_synced"] = state["department_ids_synced"] or []
    state["enrichment_hashes"] = state["enrichment_hashes"] or {}
    state["pending_enrichment"] = state["pending_enrichment"] or {}

    fingerprint = json.dumps(settings["department_ids"])
    model = settings["cortex_model"] if settings["enable_cortex"] else None
    if state["department_fingerprint"] != fingerprint:
        if state["department_fingerprint"] is not None:
            log.warning(
                "department_ids changed since the last sync, so the historical sync restarts"
            )
        reset_walk(state)
    elif model is not None and state["enrichment_model"] != model:
        log.warning(f"Enrichment with {model} is new, so every object is read again to enrich it")
        reset_walk(state)
    state["department_fingerprint"] = fingerprint
    if model is not None:
        state["enrichment_model"] = model


def sync_departments(client: dict, state: dict, settings: dict):
    """
    Upsert every department, delete departments the API no longer lists, and check that every
    configured department exists.
    Args:
        client: the API client.
        state: the sync state.
        settings: the parsed configuration.
    Raises:
        ValueError: if a configured department id is not in the API's list.
        RuntimeError: if the response has no departments list.
    """
    departments = get_json(client, "/departments").get("departments")
    if not isinstance(departments, list) or not departments:
        raise RuntimeError("The Met API returned no departments list")
    rows = [build_department_row(item) for item in departments if isinstance(item, dict)]
    rows = [row for row in rows if row["department_id"] is not None]
    for row in rows:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table=__DEPARTMENT_TABLE, data=row)

    current = sorted(row["department_id"] for row in rows)
    for department_id in sorted(set(state["department_ids_synced"]) - set(current)):
        # The 'delete' operation marks a row as deleted in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the primary key of the row.
        op.delete(table=__DEPARTMENT_TABLE, keys={"department_id": department_id})
    state["department_ids_synced"] = current

    unknown = sorted(set(settings["department_ids"]) - set(current))
    if unknown:
        valid = ", ".join(str(department_id) for department_id in current)
        raise ValueError(f"department_ids {unknown} are not Met departments. Valid ids: {valid}")
    log.info(f"Synced {len(rows)} departments")


def delete_object(object_id: int, state: dict):
    """
    Delete an object's row, and its enrichment row when it has one. Child rows keep their stamps and
    no longer match a live object row.
    Args:
        object_id: the object id.
        state: the sync state.
    """
    # The 'delete' operation marks a row as deleted in the destination table.
    # The first argument is the name of the destination table.
    # The second argument is a dictionary containing the primary key of the row.
    op.delete(table=__OBJECT_TABLE, keys={"object_id": object_id})
    state["pending_enrichment"].pop(str(object_id), None)
    if state["enrichment_hashes"].pop(str(object_id), None) is not None:
        # The 'delete' operation marks a row as deleted in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the primary key of the row.
        op.delete(table=__VISUAL_ATTRIBUTE_TABLE, keys={"object_id": object_id})


def upsert_object(obj: dict, department_id: int):
    """
    Upsert an object row and its constituent and tag rows.
    Args:
        obj: the object detail response.
        department_id: the department whose list returned the object.
    """
    # The 'upsert' operation is used to insert or update data in the destination table.
    # The first argument is the name of the destination table.
    # The second argument is a dictionary containing the record to be upserted.
    op.upsert(table=__OBJECT_TABLE, data=build_object_row(obj, department_id))
    for row in build_constituent_rows(obj):
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table=__CONSTITUENT_TABLE, data=row)
    for row in build_tag_rows(obj):
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table=__TAG_TABLE, data=row)


def enrich_object(
    context: dict, obj: dict, department_id: int, object_id: int, state: dict, counts: dict
):
    """
    Enrich one object with Cortex vision, unless its inputs are unchanged since its last enrichment.
    An object whose image could not be downloaded or whose enrichment failed is kept in
    pending_enrichment, so a later incremental sync retries it even when the Met does not edit it.
    Args:
        context: the sync context: the clients, the settings and the remaining enrichment budget.
        obj: the object detail response.
        department_id: the department whose list returned the object, kept for the retry.
        object_id: the object id.
        state: the sync state, which keeps each enriched object's enrichment hash and the pending ids.
        counts: the per-sync counts of skipped work, reported in one warning at the end.
    """
    settings = context["settings"]
    pending = state["pending_enrichment"]
    if not obj.get("primaryImageSmall"):
        counts["no_image"] += 1
        pending.pop(str(object_id), None)
        return
    terms = tag_terms(obj)
    inputs = cortex.enrichment_inputs(obj, terms, settings["cortex_model"])
    input_hash = cortex.enrichment_hash(inputs)
    if state["enrichment_hashes"].get(str(object_id)) == input_hash:
        pending.pop(str(object_id), None)
        return
    image = get_image(context["client"], obj["primaryImageSmall"])
    if image is None:
        counts["image_unavailable"] += 1
        pending[str(object_id)] = department_id
        return
    context["enrichments_left"] -= 1
    validated = cortex.enrich(
        context["cortex_session"], settings["snowflake_account"], inputs, image[0], image[1]
    )
    if validated is None:
        counts["enrichment_failed"] += 1
        pending[str(object_id)] = department_id
        return
    # The 'upsert' operation is used to insert or update data in the destination table.
    # The first argument is the name of the destination table.
    # The second argument is a dictionary containing the record to be upserted.
    op.upsert(
        table=__VISUAL_ATTRIBUTE_TABLE,
        data=cortex.build_visual_attribute_row(
            object_id, validated, terms, settings["cortex_model"], input_hash
        ),
    )
    for row in cortex.build_depicted_element_rows(object_id, validated, input_hash):
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table=__DEPICTED_ELEMENT_TABLE, data=row)
    state["enrichment_hashes"][str(object_id)] = input_hash
    pending.pop(str(object_id), None)


def process_object(context: dict, department_id: int, object_id: int, state: dict, counts: dict):
    """
    Read one object and upsert it, or delete it when the API no longer has it or it is no longer
    public domain.
    Args:
        context: the sync context.
        department_id: the department whose list returned the object.
        object_id: the object id.
        state: the sync state.
        counts: the per-sync counts.
    """
    obj = get_object(context["client"], object_id)
    if obj is None:
        delete_object(object_id, state)
        counts["deleted_not_found"] += 1
        return
    if as_int(obj.get("objectID")) != object_id:
        counts["mismatched_id"] += 1
        state["pending_enrichment"].pop(str(object_id), None)
        return
    if obj.get("isPublicDomain") is not True:
        delete_object(object_id, state)
        counts["deleted_not_public_domain"] += 1
        return
    upsert_object(obj, department_id)
    counts["upserted"] += 1
    if context["cortex_session"] is not None:
        enrich_object(context, obj, department_id, object_id, state, counts)


def budget_exhausted(context: dict) -> bool:
    """
    Report whether this sync has used its object or enrichment budget.
    Args:
        context: the sync context.
    Returns:
        True when no object request, or no Cortex call while enrichment is on, is left.
    """
    if context["objects_left"] <= 0:
        return True
    return context["cortex_session"] is not None and context["enrichments_left"] <= 0


def walk_department(context: dict, department_id: int, state: dict, counts: dict) -> bool:
    """
    Process a department's object ids in ascending order, after the last id a previous sync processed.
    Args:
        context: the sync context.
        department_id: the department id.
        state: the sync state, checkpointed every __CHECKPOINT_EVERY_OBJECTS objects.
        counts: the per-sync counts.
    Returns:
        True when the department is finished, False when the budget ran out first.
    """
    after = state["after_object_id"] if state["current_department_id"] == department_id else None
    state["current_department_id"], state["after_object_id"] = department_id, after
    object_ids = fetch_object_ids(context["client"], department_id, state["metadata_cursor"])
    since_checkpoint = 0
    for position, object_id in enumerate(object_ids, start=1):
        if after is not None and object_id <= after:
            continue
        if budget_exhausted(context):
            save_state(state)
            return False
        context["objects_left"] -= 1
        process_object(context, department_id, object_id, state, counts)
        state["after_object_id"] = object_id
        since_checkpoint += 1
        if since_checkpoint == __CHECKPOINT_EVERY_OBJECTS:
            save_state(state)
            since_checkpoint = 0
            # A historical walk runs for hours; this line shows how far it has come.
            log.info(
                f"Checkpoint in department {department_id} after object {object_id}: "
                f"{position} of {len(object_ids)} object ids read, {counts['upserted']} objects upserted this sync"
            )
    state["departments_done"].append(department_id)
    state["current_department_id"] = state["after_object_id"] = None
    save_state(state)
    return True


def retry_pending_enrichments(context: dict, state: dict, counts: dict):
    """
    Read and enrich again the objects whose enrichment failed on an earlier sync. An incremental
    sync reads only the objects changed since the cursor, so without this pass a failed object
    would wait until the Met edits it. The retries share the per-sync object and enrichment
    budgets; an id leaves the list once it is enriched, deleted or no longer public domain.
    Args:
        context: the sync context.
        state: the sync state.
        counts: the per-sync counts.
    """
    pending = state["pending_enrichment"]
    if context["cortex_session"] is None or not state["metadata_cursor"] or not pending:
        return
    queued = sorted(pending.items(), key=lambda item: int(item[0]))
    retried = 0
    for object_id, department_id in queued:
        if budget_exhausted(context):
            break
        context["objects_left"] -= 1
        process_object(context, department_id, int(object_id), state, counts)
        retried += 1
    save_state(state)
    log.info(
        f"Retried {retried} of {len(queued)} pending enrichments; {len(pending)} are still pending"
    )


def run_walk(context: dict, state: dict, counts: dict):
    """
    Walk every configured department. With no metadata_cursor this is the historical sync, which reads
    every object; with one, it reads only the objects changed since. The objects the API no longer has
    or that are no longer public domain are deleted in both, so a full re-sync over earlier rows removes
    them too. When every department is finished, the cursor moves to the day before the walk started.
    Args:
        context: the sync context.
        state: the sync state.
        counts: the per-sync counts.
    """
    if state["pending_cursor"] is None:
        started = datetime.now(timezone.utc) - __CURSOR_OVERLAP
        state["pending_cursor"] = started.strftime(__METADATA_DATE_FORMAT)
    mode = "incremental" if state["metadata_cursor"] else "historical"
    retry_pending_enrichments(context, state, counts)
    for department_id in context["settings"]["department_ids"]:
        if department_id in state["departments_done"]:
            continue
        if not walk_department(context, department_id, state, counts):
            # The retries can use the whole budget before the walk reads an object.
            position = (
                f"after object {state['after_object_id']}"
                if state["after_object_id"] is not None
                else "before its first object"
            )
            log.info(
                f"The {mode} sync stopped at the per-sync limit in department {department_id} "
                f"{position}; the next sync continues from there"
            )
            return
    state["metadata_cursor"], state["pending_cursor"] = state["pending_cursor"], None
    state["departments_done"] = []
    save_state(state)
    log.info(
        f"The {mode} sync is complete; the next sync reads objects changed since {state['metadata_cursor']}"
    )


def report_counts(counts: dict):
    """
    Log what the sync did, and raise one dashboard warning when it skipped any object or enrichment.
    Args:
        counts: the per-sync counts.
    """
    log.info(
        f"Objects upserted: {counts['upserted']}, deleted as not found: {counts['deleted_not_found']}, "
        f"deleted as no longer public domain: {counts['deleted_not_public_domain']}"
    )
    skipped = {
        "objects whose detail carried a different id": counts["mismatched_id"],
        "objects without an image to enrich": counts["no_image"],
        "images that could not be downloaded or were too large": counts["image_unavailable"],
        "enrichments that failed or returned no valid field": counts["enrichment_failed"],
    }
    details = [f"{count} {label}" for label, count in skipped.items() if count]
    if details:
        # The 'warning' operation logs the message and shows it in the Fivetran dashboard without failing the sync.
        op.warning("This sync skipped: " + "; ".join(details))


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
    log.warning("Example: Source Examples : Met Museum")

    settings = validate_configuration(configuration=configuration)
    resolve_state(state, settings)

    counts = {
        "upserted": 0,
        "deleted_not_found": 0,
        "deleted_not_public_domain": 0,
        "mismatched_id": 0,
        "no_image": 0,
        "image_unavailable": 0,
        "enrichment_failed": 0,
    }
    context = {
        "settings": settings,
        "client": create_client(),
        "cortex_session": None,
        "objects_left": settings["max_objects_per_sync"],
        "enrichments_left": settings["max_enrichments_per_sync"],
    }
    try:
        sync_departments(context["client"], state, settings)
        save_state(state)
        # The Cortex session is created only when enrichment is on, so a data-only sync needs no token.
        if settings["enable_cortex"]:
            context["cortex_session"] = cortex.create_session(settings["snowflake_pat_token"])
        run_walk(context, state, counts)
    finally:
        context["client"]["session"].close()
        if context["cortex_session"] is not None:
            context["cortex_session"].close()
    report_counts(counts)


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
