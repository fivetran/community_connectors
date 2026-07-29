"""This connector syncs Federal Register documents (rules, proposed rules, notices, and presidential documents) from the Federal Register API to a Fivetran destination.
See the Technical Reference documentation
(https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation
(https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For validating the configured start date
from datetime import datetime

# For handling request delays during retries
import time

# For building query strings safely
import urllib.parse

# For making HTTP requests to the Federal Register API
import requests

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Base URL for the Federal Register documents API
__BASE_URL = "https://www.federalregister.gov/api/v1/documents.json"

# Fields requested from the API. Pinning the field set keeps the delivered schema
# stable regardless of new fields the API may add later.
__FIELDS = [
    "document_number",
    "publication_date",
    "type",
    "title",
    "abstract",
    "excerpts",
    "html_url",
    "pdf_url",
    "public_inspection_pdf_url",
    "agencies",
]

# Maximum number of retry attempts for API requests
__MAX_RETRIES = 3

# Base delay in seconds for exponential backoff retries
__BASE_DELAY_SECONDS = 2

# Timeout in seconds for HTTP requests
__REQUEST_TIMEOUT_SECONDS = 60

# Maximum page size the Federal Register API accepts.
__MAX_PAGE_SIZE = 1000

# Default page size when none is configured.
__DEFAULT_PAGE_SIZE = 100

# Number of records processed between checkpoints
__CHECKPOINT_INTERVAL = 1000

# Publication date used when no state exists and no start date is configured.
__DEFAULT_SYNC_START_DATE = "2024-01-01"

# Date format the API uses for the publication_date filter and field.
__DATE_FORMAT = "%Y-%m-%d"


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
    # The Federal Register API requires no authentication, so there are no
    # required secrets. Every value below is optional with a documented default,
    # but any value that IS supplied must be usable -- a malformed page size
    # should fail here, at configuration time, rather than as an HTTP error
    # midway through a sync.
    page_size = configuration.get("page_size", str(__DEFAULT_PAGE_SIZE))
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
        datetime.strptime(start_date, __DATE_FORMAT)
    except ValueError:
        raise ValueError(
            f"Invalid configuration value for initial_sync_start_date: {start_date}. "
            f"Must match {__DATE_FORMAT}, for example 2024-01-01."
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
            "table": "document",
            "primary_key": ["document_number"],
            "columns": {
                "document_number": "STRING",
                "publication_date": "NAIVE_DATE",
                "document_type": "STRING",
                "title": "STRING",
                "abstract": "STRING",
                "excerpts": "STRING",
                "html_url": "STRING",
                "pdf_url": "STRING",
                "public_inspection_pdf_url": "STRING",
                "agency_ids": "STRING",
                "agency_names": "STRING",
            },
        }
    ]


def get_api_response(url: str):
    """
    Send a GET request to the Federal Register API and return the decoded JSON response.
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
                    f"Federal Register API rejected the request with HTTP "
                    f"{response.status_code}: {response.text[:500]}"
                )

            response.raise_for_status()
            return response.json()
        except (requests.exceptions.RequestException, ValueError) as error:
            last_error = error
            if attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(
                    f"Federal Register API request failed (attempt {attempt + 1} of "
                    f"{__MAX_RETRIES}), retrying in {delay}s: {error}"
                )
                time.sleep(delay)

    raise RuntimeError(
        f"Federal Register API request failed after {__MAX_RETRIES} attempts: {last_error}"
    )


def build_initial_url(start_date: str, page_size: int):
    """
    Build the first request URL for an incremental sync.
    Documents are returned oldest first so the compound (publication_date,
    document_number) cursor advances monotonically and a bounded run can resume
    exactly where it stopped.
    Args:
        start_date: inclusive lower bound of the sync window, as YYYY-MM-DD.
        page_size: number of records to request per page.
    Returns:
        The fully-qualified URL for the first page.
    """
    # urlencode encodes the configured start_date and page size, so no
    # config-derived value reaches the URL unescaped.
    query = [
        ("conditions[publication_date][gte]", start_date),
        ("order", "oldest"),
        ("per_page", str(page_size)),
    ]
    query += [("fields[]", field) for field in __FIELDS]
    return f"{__BASE_URL}?{urllib.parse.urlencode(query)}"


def join_agency_ids(agencies: list):
    """
    Flatten the agency id list of a document into a delimited string.
    Args:
        agencies: the raw agencies array, which may be empty.
    Returns:
        A comma-separated string of agency ids, or None when there are none.
    """
    ids = [str(agency.get("id")) for agency in agencies if agency.get("id") is not None]
    return ",".join(ids) if ids else None


def join_agency_names(agencies: list):
    """
    Flatten the agency name list of a document into a delimited string.
    The API exposes both a cleaned name and a raw_name; the cleaned name is
    preferred and raw_name is the fallback.
    Args:
        agencies: the raw agencies array, which may be empty.
    Returns:
        A semicolon-separated string of agency names, or None when there are none.
    """
    names = [agency.get("name") or agency.get("raw_name") for agency in agencies]
    names = [name for name in names if name]
    return "; ".join(names) if names else None


def flatten_document(record: dict):
    """
    Flatten one Federal Register API document into a single destination row.
    Args:
        record: one element of the API response "results" array.
    Returns:
        A dictionary whose keys match the document table columns.
    """
    agencies = record.get("agencies") or []

    return {
        "document_number": record.get("document_number"),
        "publication_date": record.get("publication_date"),
        # 'type' is renamed to 'document_type': 'type' reads ambiguously as a
        # column and is refused by some warehouses as an identifier.
        "document_type": record.get("type"),
        "title": record.get("title"),
        "abstract": record.get("abstract"),
        "excerpts": record.get("excerpts"),
        "html_url": record.get("html_url"),
        "pdf_url": record.get("pdf_url"),
        "public_inspection_pdf_url": record.get("public_inspection_pdf_url"),
        "agency_ids": join_agency_ids(agencies),
        "agency_names": join_agency_names(agencies),
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
    log.warning("Example: Source Examples - Federal Register Documents")

    validate_configuration(configuration=configuration)

    page_size = int(configuration.get("page_size", __DEFAULT_PAGE_SIZE))
    max_records = int(configuration.get("max_records_per_sync", "0"))

    # The cursor is compound: a publication_date plus the document_number within
    # that date. The API's publication_date filter is inclusive and day-granular,
    # and a single day carries many documents, so a date-only cursor would either
    # re-fetch a whole day every sync or skip records. The document_number
    # tiebreaker lets a bounded run stop between two documents on the same day and
    # resume exactly there. document_number is the same value the API sorts on
    # (its search_after cursor encodes publication_date and document_number), so
    # this ordering matches the server's.
    last_date = state.get(
        "last_publication_date",
        configuration.get("initial_sync_start_date", __DEFAULT_SYNC_START_DATE),
    )
    last_document = state.get("last_document_number", "")

    log.info(
        f"Syncing Federal Register documents published on or after {last_date} "
        f"(page size {page_size}, record limit {max_records or 'none'})"
    )

    url = build_initial_url(last_date, page_size)
    cursor_date = last_date
    cursor_document = last_document
    record_count = 0
    limit_reached = False

    while url:
        response = get_api_response(url)
        records = response.get("results", [])

        for record in records:
            document_number = record.get("document_number")
            publication_date = record.get("publication_date")

            # A record with no document_number cannot be keyed in the
            # destination. Skip it loudly rather than upserting a null key.
            if not document_number:
                log.warning("Skipping a record with no document_number")
                continue

            # The lower bound is inclusive, so a resume re-requests the last
            # synced day. Skip any document at or before the compound cursor,
            # which is exactly the set already delivered by the previous run.
            if (publication_date, document_number) <= (last_date, last_document):
                continue

            flattened = flatten_document(record)

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table="document", data=flattened)

            record_count += 1
            cursor_date = publication_date
            cursor_document = document_number

            if record_count % __CHECKPOINT_INTERVAL == 0:
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
                # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
                op.checkpoint(
                    state={
                        "last_publication_date": cursor_date,
                        "last_document_number": cursor_document,
                    }
                )
                log.info(
                    f"Checkpointed after {record_count} records at "
                    f"{cursor_date}/{cursor_document}"
                )

            # The compound cursor makes max_records_per_sync a true ceiling:
            # because document_number uniquely orders documents within a day, the
            # run can stop on any single record and the next sync resumes on the
            # very next one. There is no timestamp-group overshoot to accommodate.
            if max_records and record_count >= max_records:
                log.warning(
                    f"Reached the configured max_records_per_sync limit of {max_records}. "
                    f"Synced {record_count} records through {cursor_date}/{cursor_document}. "
                    f"The next sync resumes immediately after it."
                )
                limit_reached = True
                break

        if limit_reached:
            break

        # Follow the API's own pagination link, which carries a search_after
        # cursor for stable deep pagination. The walk terminates when the link is
        # absent.
        url = response.get("next_page_url")

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(
        state={
            "last_publication_date": cursor_date,
            "last_document_number": cursor_document,
        }
    )

    log.info(
        f"Sync complete. Upserted {record_count} documents up to "
        f"{cursor_date}/{cursor_document}"
    )


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
