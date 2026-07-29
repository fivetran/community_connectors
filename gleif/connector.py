"""This connector syncs Legal Entity Identifier (LEI) reference data from the GLEIF API to a Fivetran destination.
See the Technical Reference documentation
(https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation
(https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For computing the upper bound of the incremental sync window
from datetime import datetime, timezone

# For handling request delays during retries
import time

# For building query strings safely
import urllib.parse

# For making HTTP requests to the GLEIF API
import requests

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Base URL for the GLEIF LEI records API
__GLEIF_BASE_URL = "https://api.gleif.org/api/v1/lei-records"

# Maximum number of retry attempts for API requests
__MAX_RETRIES = 3

# Base delay in seconds for exponential backoff retries
__BASE_DELAY_SECONDS = 2

# Timeout in seconds for HTTP requests
__REQUEST_TIMEOUT_SECONDS = 60

# Maximum page size the GLEIF API accepts. Requesting more returns HTTP 400
# "The page.size must be between 1 and 200."
__MAX_PAGE_SIZE = 200

# Number of records processed between checkpoints
__CHECKPOINT_INTERVAL = 1000

# Earliest LEI registration data available, used when no state exists and no
# start date is configured.
__DEFAULT_SYNC_START_DATE = "2026-01-01T00:00:00Z"

# Timestamp format used by the GLEIF API for both filters and record fields
__TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has
    all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any configuration parameter is missing, malformed, or out of range.
    """
    # The GLEIF API requires no authentication, so there are no required secrets.
    # Every value below is optional with a documented default, but any value that
    # IS supplied must be usable -- a malformed page size should fail here, at
    # configuration time, rather than as an HTTP 400 midway through a sync.
    page_size = configuration.get("page_size", str(__MAX_PAGE_SIZE))
    if not str(page_size).isdigit() or not 1 <= int(page_size) <= __MAX_PAGE_SIZE:
        raise ValueError(
            f"Invalid configuration value for page_size: {page_size}. "
            f"Must be an integer between 1 and {__MAX_PAGE_SIZE}."
        )

    max_records = configuration.get("max_records_per_sync", "0")
    if not str(max_records).isdigit():
        raise ValueError(
            f"Invalid configuration value for max_records_per_sync: {max_records}. "
            "Must be a non-negative integer, where 0 means no limit."
        )

    start_date = configuration.get("initial_sync_start_date", __DEFAULT_SYNC_START_DATE)
    try:
        datetime.strptime(start_date, __TIMESTAMP_FORMAT)
    except ValueError:
        raise ValueError(
            f"Invalid configuration value for initial_sync_start_date: {start_date}. "
            f"Must match {__TIMESTAMP_FORMAT}, for example 2026-01-01T00:00:00Z."
        )


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
            "table": "lei_record",
            "primary_key": ["lei"],
            "columns": {
                "lei": "STRING",
                "legal_name": "STRING",
                "legal_name_language": "STRING",
                "entity_status": "STRING",
                "entity_category": "STRING",
                "entity_sub_category": "STRING",
                "legal_jurisdiction": "STRING",
                "legal_form_id": "STRING",
                "legal_form_other": "STRING",
                "registered_as": "STRING",
                "registered_at_id": "STRING",
                "entity_creation_date": "UTC_DATETIME",
                "entity_expiration_date": "UTC_DATETIME",
                "entity_expiration_reason": "STRING",
                "successor_lei": "STRING",
                "successor_name": "STRING",
                "associated_entity_lei": "STRING",
                "associated_entity_name": "STRING",
                "legal_address_lines": "STRING",
                "legal_address_city": "STRING",
                "legal_address_region": "STRING",
                "legal_address_country": "STRING",
                "legal_address_postal_code": "STRING",
                "headquarters_address_lines": "STRING",
                "headquarters_address_city": "STRING",
                "headquarters_address_region": "STRING",
                "headquarters_address_country": "STRING",
                "headquarters_address_postal_code": "STRING",
                "registration_status": "STRING",
                "initial_registration_date": "UTC_DATETIME",
                "last_update_date": "UTC_DATETIME",
                "next_renewal_date": "UTC_DATETIME",
                "managing_lou": "STRING",
                "corroboration_level": "STRING",
                "validated_at_id": "STRING",
                "validated_as": "STRING",
                "bic_codes": "STRING",
                "mic_codes": "STRING",
                "ocid": "STRING",
            },
        }
    ]


def get_api_response(url: str):
    """
    Send a GET request to the GLEIF API and return the decoded JSON:API response.
    Retries on transient transport and server errors with exponential backoff.
    Args:
        url: the fully-qualified request URL, including any query string.
    Returns:
        The decoded JSON response body as a dictionary.
    Raises:
        RuntimeError: if the API cannot be reached successfully within the retry budget.
    """
    last_error = None
    for attempt in range(__MAX_RETRIES):
        try:
            response = requests.get(url, timeout=__REQUEST_TIMEOUT_SECONDS)

            # A 4xx other than 429 means the request itself is wrong -- a bad
            # filter, an out-of-range page size. Retrying cannot fix it and only
            # delays a failure the operator needs to see, so fail immediately.
            if 400 <= response.status_code < 500 and response.status_code != 429:
                raise RuntimeError(
                    f"GLEIF API rejected the request with HTTP {response.status_code}: "
                    f"{response.text[:500]}"
                )

            response.raise_for_status()
            return response.json()
        except (requests.exceptions.RequestException, ValueError) as error:
            last_error = error
            if attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(
                    f"GLEIF API request failed (attempt {attempt + 1} of {__MAX_RETRIES}), "
                    f"retrying in {delay}s: {error}"
                )
                time.sleep(delay)

    raise RuntimeError(f"GLEIF API request failed after {__MAX_RETRIES} attempts: {last_error}")


def build_initial_url(start_timestamp: str, end_timestamp: str, page_size: int):
    """
    Build the first request URL for an incremental sync window.
    Cursor-based pagination is used rather than page numbers because the GLEIF API
    refuses any request where page[number] * page[size] exceeds 10000. A page-number
    walk therefore silently caps a sync at 10000 records, which is fewer than a
    single busy day of updates.
    Args:
        start_timestamp: inclusive lower bound of the update window.
        end_timestamp: upper bound of the update window.
        page_size: number of records to request per page.
    Returns:
        The fully-qualified URL for the first page of the window.
    """
    # The date filter requires BOTH bounds. An open-ended "<from>.." is rejected
    # with HTTP 400 "Date must not be empty."
    query = {
        "filter[registration.lastUpdateDate]": f"{start_timestamp}..{end_timestamp}",
        "sort": "registration.lastUpdateDate",
        "page[size]": str(page_size),
        "page[cursor]": "*",
    }
    return f"{__GLEIF_BASE_URL}?{urllib.parse.urlencode(query)}"


def join_codes(value):
    """
    Flatten a GLEIF list-valued identifier field into a delimited string.
    The bic and mic fields are lists when populated and null otherwise. Sampling a
    handful of records is not enough to discover this -- most entities carry neither,
    so a schema inferred from a small sample types them as strings and only fails in
    production on the first bank.
    Args:
        value: the raw field value, which may be a list, a string, or None.
    Returns:
        A comma-separated string, or None when the field is absent.
    """
    if value is None:
        return None
    if isinstance(value, list):
        return ",".join(str(item) for item in value) if value else None
    return str(value)


def join_address_lines(address: dict):
    """
    Flatten the addressLines array of a GLEIF address block into a single string.
    Args:
        address: a legalAddress or headquartersAddress object, possibly empty.
    Returns:
        The address lines joined by a space, or None when there are none.
    """
    return " ".join(address.get("addressLines") or []) or None


def flatten_record(record: dict):
    """
    Flatten one JSON:API LEI record into a single destination row.
    Args:
        record: one element of the API response "data" array.
    Returns:
        A dictionary whose keys match the lei_record table columns.
    """
    # `or {}` rather than a `{}` default at every level. dict.get's default
    # applies only when the key is ABSENT; a key present with an explicit null
    # returns None, and the next `.get()` then raises AttributeError and stops
    # the whole sync. GLEIF sends null for optional nested objects.
    attributes = record.get("attributes") or {}
    entity = attributes.get("entity") or {}
    registration = attributes.get("registration") or {}
    legal_address = entity.get("legalAddress") or {}
    headquarters_address = entity.get("headquartersAddress") or {}

    return {
        "lei": attributes.get("lei"),
        "legal_name": (entity.get("legalName") or {}).get("name"),
        "legal_name_language": (entity.get("legalName") or {}).get("language"),
        "entity_status": entity.get("status"),
        "entity_category": entity.get("category"),
        "entity_sub_category": entity.get("subCategory"),
        "legal_jurisdiction": entity.get("jurisdiction"),
        "legal_form_id": (entity.get("legalForm") or {}).get("id"),
        "legal_form_other": (entity.get("legalForm") or {}).get("other"),
        "registered_as": entity.get("registeredAs"),
        "registered_at_id": (entity.get("registeredAt") or {}).get("id"),
        "entity_creation_date": entity.get("creationDate"),
        "entity_expiration_date": (entity.get("expiration") or {}).get("date"),
        "entity_expiration_reason": (entity.get("expiration") or {}).get("reason"),
        "successor_lei": (entity.get("successorEntity") or {}).get("lei"),
        "successor_name": (entity.get("successorEntity") or {}).get("name"),
        "associated_entity_lei": (entity.get("associatedEntity") or {}).get("lei"),
        "associated_entity_name": (entity.get("associatedEntity") or {}).get("name"),
        "legal_address_lines": join_address_lines(legal_address),
        "legal_address_city": legal_address.get("city"),
        "legal_address_region": legal_address.get("region"),
        "legal_address_country": legal_address.get("country"),
        "legal_address_postal_code": legal_address.get("postalCode"),
        "headquarters_address_lines": join_address_lines(headquarters_address),
        "headquarters_address_city": headquarters_address.get("city"),
        "headquarters_address_region": headquarters_address.get("region"),
        "headquarters_address_country": headquarters_address.get("country"),
        "headquarters_address_postal_code": headquarters_address.get("postalCode"),
        "registration_status": registration.get("status"),
        "initial_registration_date": registration.get("initialRegistrationDate"),
        "last_update_date": registration.get("lastUpdateDate"),
        "next_renewal_date": registration.get("nextRenewalDate"),
        "managing_lou": registration.get("managingLou"),
        "corroboration_level": registration.get("corroborationLevel"),
        "validated_at_id": (registration.get("validatedAt") or {}).get("id"),
        "validated_as": registration.get("validatedAs"),
        "bic_codes": join_codes(attributes.get("bic")),
        "mic_codes": join_codes(attributes.get("mic")),
        "ocid": attributes.get("ocid"),
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
    log.warning("Example: Source Examples - GLEIF LEI Records")

    validate_configuration(configuration=configuration)

    page_size = int(configuration.get("page_size", __MAX_PAGE_SIZE))
    max_records = int(configuration.get("max_records_per_sync", "0"))
    start_timestamp = state.get(
        "last_update_date",
        configuration.get("initial_sync_start_date", __DEFAULT_SYNC_START_DATE),
    )

    # The filter needs a closed range, so the window is bounded at the moment the
    # sync starts. Records updated mid-sync fall into the next window rather than
    # being missed, because the cursor never advances past this bound.
    end_timestamp = datetime.now(timezone.utc).strftime(__TIMESTAMP_FORMAT)

    log.info(
        f"Syncing LEI records updated between {start_timestamp} and {end_timestamp} "
        f"(page size {page_size}, record limit {max_records or 'none'})"
    )

    url = build_initial_url(start_timestamp, end_timestamp, page_size)
    cursor_timestamp = start_timestamp
    record_count = 0
    limit_reached = False

    # Timestamp at which max_records_per_sync was met. The sync keeps consuming
    # records that share it, and stops at the first record of the next group.
    #
    # Stopping mid-group would deadlock the connector. The cursor is a timestamp
    # and the API's lower bound is inclusive, so a sync that halts partway through
    # a group of records sharing one timestamp checkpoints that same timestamp,
    # and the next sync reopens the window on the identical group. With a limit
    # smaller than the group it never escapes. Verified on 2026-07-29 against the
    # live API: max_records_per_sync=5 starting at 2026-07-28T00:00:24Z, a second
    # containing 18 records -- three consecutive syncs each upserted the same 5
    # records and the cursor never moved off 00:00:24Z.
    #
    # So the limit is a floor rather than a ceiling. Overshoot is bounded by the
    # size of a single timestamp group.
    limit_boundary = None

    while url:
        response = get_api_response(url)
        records = response.get("data", [])

        for record in records:
            flattened = flatten_record(record)

            # A record with no LEI cannot be keyed in the destination. Skip it
            # loudly rather than upserting a row with a null primary key.
            if not flattened["lei"]:
                log.warning("Skipping a record with no LEI value")
                continue

            # The limit has been met and this record starts a new timestamp group.
            # Stop before upserting it, and move the cursor ONTO it.
            #
            # Checkpointing the completed group's own timestamp is not enough:
            # the lower bound is inclusive, so the next sync would reopen the
            # window on the group it just finished and stop in the same place
            # forever. Advancing to this record's timestamp is both unstalling and
            # lossless, because the record has not been upserted yet and the next
            # sync's inclusive lower bound picks it up first.
            if limit_boundary is not None and flattened["last_update_date"] != limit_boundary:
                cursor_timestamp = flattened["last_update_date"]
                log.warning(
                    f"Reached the configured max_records_per_sync limit of {max_records}. "
                    f"Synced {record_count} records through the {limit_boundary} boundary. "
                    f"The next sync resumes at {cursor_timestamp}."
                )
                limit_reached = True
                break

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table="lei_record", data=flattened)

            record_count += 1
            if flattened["last_update_date"]:
                cursor_timestamp = max(cursor_timestamp, flattened["last_update_date"])

            if record_count % __CHECKPOINT_INTERVAL == 0:
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
                # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
                op.checkpoint(state={"last_update_date": cursor_timestamp})
                log.info(f"Checkpointed after {record_count} records at {cursor_timestamp}")

            if max_records and record_count >= max_records and limit_boundary is None:
                limit_boundary = flattened["last_update_date"]

        if limit_reached:
            break

        # Follow the JSON:API cursor. The final page of a window arrives with a
        # next link still present and an empty data array, so the loop must
        # terminate on the ABSENCE of the link rather than on a non-empty page.
        url = (response.get("links") or {}).get("next")

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state={"last_update_date": cursor_timestamp})

    log.info(f"Sync complete. Upserted {record_count} LEI records up to {cursor_timestamp}")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
