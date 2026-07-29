"""This connector syncs species occurrence records from the GBIF (Global
Biodiversity Information Facility) occurrence search API to a Fivetran destination.
See the Technical Reference documentation
(https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation
(https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For handling request delays during retries
import time

# For building query strings safely
import urllib.parse

# For making HTTP requests to the GBIF API
import requests

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Base URL for the GBIF occurrence search API.
__BASE_URL = "https://api.gbif.org/v1/occurrence/search"

# Maximum page size the GBIF occurrence search API accepts. Requesting more is
# silently capped by the API to this value.
__MAX_PAGE_SIZE = 300

# Default page size when none is configured.
__DEFAULT_PAGE_SIZE = 300

# GBIF caps deep pagination: offset + limit must be <= 100000. A request past it
# returns HTTP 400 rather than data, so the connector must stop at the cap and
# tell the operator to narrow the query rather than silently skip everything
# beyond it. Verified live 2026-07-29: offset=100001 returned HTTP 400.
__OFFSET_CAP = 100000

# Maximum number of retry attempts for API requests.
__MAX_RETRIES = 3

# Base delay in seconds for exponential backoff retries.
__BASE_DELAY_SECONDS = 2

# Timeout in seconds for HTTP requests.
__REQUEST_TIMEOUT_SECONDS = 60

# Number of records processed between checkpoints.
__CHECKPOINT_INTERVAL = 1000


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
    # The GBIF occurrence API requires no authentication, so there are no
    # required secrets. Every value below is optional, but any value that IS
    # supplied must be usable -- a malformed page size or country code should
    # fail here, at configuration time, rather than as an HTTP error midway
    # through a sync.
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

    taxon_key = configuration.get("taxon_key", "").strip()
    if taxon_key and (not taxon_key.isdigit() or int(taxon_key) <= 0):
        raise ValueError(
            f"Invalid configuration value for taxon_key: {taxon_key}. "
            "Must be a positive integer GBIF taxon key, for example 212 for birds."
        )

    country = configuration.get("country", "").strip()
    if country and not re_two_letter(country):
        raise ValueError(
            f"Invalid configuration value for country: {country}. "
            "Must be a two-letter ISO 3166-1 alpha-2 code, for example US."
        )


def re_two_letter(value: str):
    """
    Return True when the value is exactly two ASCII letters.
    Kept as a small helper so the country check reads clearly and stays testable.
    Args:
        value: the candidate country code.
    Returns:
        True if the value is two ASCII letters, otherwise False.
    """
    return len(value) == 2 and value.isalpha() and value.isascii()


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
            "table": "occurrence",
            "primary_key": ["gbif_id"],
            "columns": {
                "gbif_id": "STRING",
                "dataset_key": "STRING",
                "publishing_country": "STRING",
                "basis_of_record": "STRING",
                "occurrence_status": "STRING",
                "scientific_name": "STRING",
                "accepted_scientific_name": "STRING",
                "taxon_key": "LONG",
                "kingdom": "STRING",
                "phylum": "STRING",
                # The API fields 'class' and 'order' are reserved SQL keywords and
                # fail an unquoted CREATE TABLE on most warehouses, so they are
                # renamed at the source to taxon_class and taxon_order.
                "taxon_class": "STRING",
                "taxon_order": "STRING",
                "family": "STRING",
                "genus": "STRING",
                "species": "STRING",
                "taxon_rank": "STRING",
                "taxonomic_status": "STRING",
                "country_code": "STRING",
                "country": "STRING",
                "locality": "STRING",
                "decimal_latitude": "DOUBLE",
                "decimal_longitude": "DOUBLE",
                "event_date": "STRING",
                "event_year": "INT",
                "event_month": "INT",
                "event_day": "INT",
                "recorded_by": "STRING",
                "institution_code": "STRING",
                "catalog_number": "STRING",
                "last_interpreted": "STRING",
                "license": "STRING",
            },
        }
    ]


def as_text(value):
    """
    Coerce a value that the API may return as either a scalar or a list into a
    single scalar string, so a list never lands stringified in a scalar column.
    Args:
        value: a scalar, a list, or None.
    Returns:
        A delimited string for a non-empty list, the value unchanged for a
        scalar, or None when there is nothing to store.
    """
    if isinstance(value, list):
        parts = [str(item) for item in value if item is not None]
        return "; ".join(parts) if parts else None
    return value


def get_api_response(url: str):
    """
    Send a GET request to the GBIF API and return the decoded JSON response.
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
            # filter or an offset past the deep-paging cap. Retrying cannot fix
            # it and only delays a failure the operator needs to see, so fail
            # immediately.
            if 400 <= response.status_code < 500 and response.status_code != 429:
                raise RuntimeError(
                    f"GBIF API rejected the request with HTTP "
                    f"{response.status_code}: {response.text[:500]}"
                )

            response.raise_for_status()
            return response.json()
        except (requests.exceptions.RequestException, ValueError) as error:
            last_error = error
            if attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(
                    f"GBIF API request failed (attempt {attempt + 1} of "
                    f"{__MAX_RETRIES}), retrying in {delay}s: {error}"
                )
                time.sleep(delay)

    raise RuntimeError(f"GBIF API request failed after {__MAX_RETRIES} attempts: {last_error}")


def build_url(offset: int, page_size: int, taxon_key: str, country: str):
    """
    Build a request URL for the occurrence search at a given offset.
    Args:
        offset: the number of records to skip, which doubles as the resume cursor.
        page_size: number of records to request in this page.
        taxon_key: optional GBIF taxon key filter, or an empty string.
        country: optional ISO 3166-1 alpha-2 country filter, or an empty string.
    Returns:
        The fully-qualified URL for the requested page.
    """
    # urlencode encodes every value, so no config-derived filter reaches the URL
    # unescaped.
    query = [("offset", str(offset)), ("limit", str(page_size))]
    if taxon_key:
        query.append(("taxonKey", taxon_key))
    if country:
        query.append(("country", country.upper()))
    return f"{__BASE_URL}?{urllib.parse.urlencode(query)}"


def flatten_occurrence(record: dict):
    """
    Flatten one GBIF occurrence record into a single destination row.
    Args:
        record: one element of the API response "results" array.
    Returns:
        A dictionary whose keys match the occurrence table columns.
    """
    gbif_id = record.get("gbifID")

    return {
        "gbif_id": str(gbif_id) if gbif_id is not None else None,
        "dataset_key": record.get("datasetKey"),
        "publishing_country": record.get("publishingCountry"),
        "basis_of_record": record.get("basisOfRecord"),
        "occurrence_status": record.get("occurrenceStatus"),
        "scientific_name": record.get("scientificName"),
        "accepted_scientific_name": record.get("acceptedScientificName"),
        "taxon_key": record.get("taxonKey"),
        "kingdom": record.get("kingdom"),
        "phylum": record.get("phylum"),
        # 'class' and 'order' are reserved SQL keywords; they are delivered under
        # non-colliding names so an unquoted CREATE TABLE cannot fail on them.
        "taxon_class": record.get("class"),
        "taxon_order": record.get("order"),
        "family": record.get("family"),
        "genus": record.get("genus"),
        "species": record.get("species"),
        "taxon_rank": record.get("taxonRank"),
        "taxonomic_status": record.get("taxonomicStatus"),
        "country_code": record.get("countryCode"),
        "country": record.get("country"),
        "locality": record.get("locality"),
        "decimal_latitude": record.get("decimalLatitude"),
        "decimal_longitude": record.get("decimalLongitude"),
        "event_date": as_text(record.get("eventDate")),
        "event_year": record.get("year"),
        "event_month": record.get("month"),
        "event_day": record.get("day"),
        # recordedBy is usually a string but is a list in some datasets; coerce
        # so a list never lands stringified in a scalar column.
        "recorded_by": as_text(record.get("recordedBy")),
        "institution_code": record.get("institutionCode"),
        "catalog_number": record.get("catalogNumber"),
        "last_interpreted": record.get("lastInterpreted"),
        "license": record.get("license"),
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
    log.warning("Example: Source Examples - GBIF Occurrences")

    validate_configuration(configuration=configuration)

    page_size = int(configuration.get("page_size", __DEFAULT_PAGE_SIZE))
    max_records = int(configuration.get("max_records_per_sync", "0"))
    taxon_key = configuration.get("taxon_key", "").strip()
    country = configuration.get("country", "").strip()

    # The cursor is the offset. GBIF occurrence search exposes no stable
    # modified-time ordering (lastInterpreted is rewritten whenever a record is
    # reprocessed), so a time cursor would skip or duplicate. The offset is a
    # true resume point for a bounded query as long as the result ordering is
    # stable for the duration of the backfill, which GBIF guarantees within the
    # deep-paging cap. The offset is advanced per record, not per page, so a run
    # stopped mid-page by max_records_per_sync resumes on the exact next record
    # rather than skipping the remainder of the page.
    offset = int(state.get("offset", 0))

    log.info(
        f"Syncing GBIF occurrences from offset {offset} "
        f"(page size {page_size}, record limit {max_records or 'none'}, "
        f"taxon_key {taxon_key or 'any'}, country {country or 'any'})"
    )

    record_count = 0
    limit_reached = False
    reported_total = False

    while offset < __OFFSET_CAP:
        # Never request past the deep-paging cap: offset + limit must stay <=
        # __OFFSET_CAP or the API returns HTTP 400.
        effective_limit = min(page_size, __OFFSET_CAP - offset)
        url = build_url(offset, effective_limit, taxon_key, country)
        response = get_api_response(url)

        # The first response carries the total match count. When it exceeds the
        # deep-paging cap, offset pagination cannot reach the whole set, so warn
        # loudly rather than silently deliver a truncated table.
        if not reported_total:
            total = response.get("count", 0)
            log.info(f"GBIF reports {total} occurrences match this query")
            if total > __OFFSET_CAP:
                log.warning(
                    f"The query matches {total} occurrences but GBIF only allows paging "
                    f"through the first {__OFFSET_CAP}. Only those are reachable here; "
                    "narrow the query with taxon_key or country to sync the rest."
                )
            reported_total = True

        results = response.get("results", [])

        for record in results:
            gbif_id = record.get("gbifID")

            # A record with no gbifID cannot be keyed in the destination. Skip it
            # loudly rather than upserting a null key.
            if gbif_id is None:
                log.warning("Skipping a record with no gbifID")
                offset += 1
                continue

            flattened = flatten_occurrence(record)

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table="occurrence", data=flattened)

            record_count += 1
            offset += 1

            if record_count % __CHECKPOINT_INTERVAL == 0:
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
                # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
                op.checkpoint(state={"offset": offset})
                log.info(f"Checkpointed after {record_count} records at offset {offset}")

            # Because the offset advances per record, max_records_per_sync is a
            # true ceiling: the run stops on the exact record and the next sync
            # resumes on the following one, with no page remainder skipped.
            if max_records and record_count >= max_records:
                log.warning(
                    f"Reached the configured max_records_per_sync limit of {max_records}. "
                    f"Synced {record_count} records through offset {offset}. "
                    "The next sync resumes immediately after it."
                )
                limit_reached = True
                break

        if limit_reached:
            break

        # endOfRecords marks the last page for this query. An empty page is the
        # same terminal signal and guards against a missing flag.
        if response.get("endOfRecords") or not results:
            break

    if offset >= __OFFSET_CAP and not limit_reached:
        log.warning(
            f"Stopped at the GBIF deep-paging cap of {__OFFSET_CAP} records. "
            "Narrow the query to reach occurrences beyond it."
        )

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state={"offset": offset})

    log.info(f"Sync complete. Upserted {record_count} occurrences up to offset {offset}")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
