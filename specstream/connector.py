"""This connector syncs your SpecStream lists of live web data (companies, local businesses, people, job posts,
filings, tenders and grants) into your destination, with the page each value was read from.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For pausing between retries
import time

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For reading a list's records one at a time from the response stream, so a large list is never held in memory
import ijson

# For calling the SpecStream API (pre-installed in the Connector SDK runtime)
import requests

__BASE_URL = "https://api.specstream.ai"
__LISTS_PAGE_SIZE = 100
__CHECKPOINT_INTERVAL = 1000
__REQUEST_TIMEOUT_SECONDS = 60
__MAX_RETRIES = 5
__RETRY_BASE_DELAY_SECONDS = 2

# Columns the SpecStream workspace shows as buttons rather than facts; they are not synced.
__BUTTON_COLUMNS = {"matches", "open"}


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    api_key = configuration.get("api_key")
    if not api_key or api_key.startswith("<"):
        raise ValueError("Missing required configuration value: api_key")


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
            "table": "list",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "project_id": "STRING",
                "imported": "BOOLEAN",
                "created_at": "UTC_DATETIME",
                "row_count": "INT",
            },
        },
        {
            "table": "record",
            "primary_key": ["list_id", "record_id"],
            "columns": {
                "list_id": "STRING",
                "record_id": "STRING",
                "entity_id": "STRING",
                "values": "JSON",
                "sources": "JSON",
            },
        },
        {
            "table": "record_value",
            "primary_key": ["list_id", "record_id", "column_id"],
            "columns": {
                "list_id": "STRING",
                "record_id": "STRING",
                "column_id": "STRING",
                "label": "STRING",
                "value": "STRING",
                "raw_value": "JSON",
                "source_url": "STRING",
                "observed_at": "UTC_DATETIME",
            },
        },
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    The first sync reads every list the API key can open. Later syncs read only the lists created since the last one:
    a SpecStream schedule delivers new and changed records as a new list, so new lists are where new data arrives.
    Lists are read one page at a time, newest first, and each list is synced before the next page is requested.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    validate_configuration(configuration=configuration)
    session = create_session(configuration["api_key"])
    synced_through = state.get("lists_created_through")
    synced_ids = set(state.get("list_ids_at_boundary") or [])
    # A pass that an earlier sync started and did not finish: it resumes after the last list it synced.
    in_progress = state.get("pass_in_progress") or {}
    after = in_progress.get("after")
    resumed = bool(after)
    synced_count = 0

    while True:
        page = fetch_lists_page(session, after)
        summaries = page.get("collections") or []
        next_after = page.get("next_after")
        if resumed and not summaries and not next_after:
            # The list the pass stopped at is gone, so its position is lost. Start the pass again from the newest
            # list: upserts make it safe to read the same lists twice.
            log.warning("The list the last sync stopped at is gone; reading the new lists again")
            state.pop("pass_in_progress", None)
            in_progress, after, resumed = {}, None, False
            continue
        resumed = False

        reached_synced = False
        for position, summary in enumerate(summaries):
            created_at = summary["created_at"]
            if synced_through and created_at < synced_through:
                reached_synced = True
                break
            # Lists can share a creation time, so the lists already synced at the boundary are known by ID.
            if created_at == synced_through and summary["id"] in synced_ids:
                continue
            sync_list(session, summary, state)
            synced_count += 1
            if "newest_created_at" not in in_progress:
                in_progress = {"newest_created_at": created_at, "newest_ids": []}
            if created_at == in_progress["newest_created_at"]:
                in_progress["newest_ids"].append(summary["id"])
            in_progress["after"] = summary["id"]
            state["pass_in_progress"] = in_progress
            if next_after or position < len(summaries) - 1:
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process
                # can resume from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
                op.checkpoint(state)

        if reached_synced or not next_after:
            break
        after = next_after

    if in_progress:
        # The pass is complete: everything created up to its newest list is synced.
        newest_ids = set(in_progress["newest_ids"])
        if in_progress["newest_created_at"] == synced_through:
            newest_ids |= synced_ids
        state["lists_created_through"] = in_progress["newest_created_at"]
        state["list_ids_at_boundary"] = sorted(newest_ids)
        state.pop("pass_in_progress", None)
    if synced_count:
        log.info(f"Synced {synced_count} SpecStream list(s)")
    else:
        log.info("No new SpecStream lists since the last sync")
    # Save the completed pass, so the next sync reads only the lists created after it.
    op.checkpoint(state)


def create_session(api_key: str):
    """
    Create an HTTP session that sends the SpecStream API key on every request.
    Args:
        api_key: a SpecStream API key.
    Returns:
        A requests session.
    """
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "User-Agent": "fivetran-connector-sdk-specstream",
        }
    )
    return session


def send_request(session, path: str, params=None, stream: bool = False):
    """
    Call the SpecStream API and return the successful response.
    Retries rate limits and server errors with exponential backoff; fails at once on a refused API key.
    Args:
        session: the HTTP session.
        path: the API path, starting with /v1.
        params: optional query parameters.
        stream: True to leave the body unread, so the caller can read it in pieces.
    Returns:
        The response.
    Raises:
        RuntimeError: if the API key is refused or the API keeps failing.
    """
    for attempt in range(__MAX_RETRIES):
        try:
            response = session.get(
                __BASE_URL + path,
                params=params,
                timeout=__REQUEST_TIMEOUT_SECONDS,
                stream=stream,
            )
        except (requests.ConnectionError, requests.Timeout) as error:
            if attempt == __MAX_RETRIES - 1:
                raise RuntimeError(f"SpecStream did not answer {path}: {error}") from error
            log.warning(f"SpecStream did not answer {path}; retrying")
            time.sleep(__RETRY_BASE_DELAY_SECONDS * 2**attempt)
            continue
        if response.status_code in (401, 403):
            response.close()
            raise RuntimeError(
                "SpecStream refused the API key. Make a key under Agents & API in SpecStream and update the connection."
            )
        if response.status_code == 429 or response.status_code >= 500:
            response.close()
            if attempt == __MAX_RETRIES - 1:
                raise RuntimeError(f"SpecStream answered {response.status_code} for {path}")
            retry_after = response.headers.get("Retry-After", "")
            delay_seconds = (
                int(retry_after)
                if retry_after.isdigit()
                else __RETRY_BASE_DELAY_SECONDS * 2**attempt
            )
            log.warning(
                f"SpecStream answered {response.status_code} for {path}; retrying in {delay_seconds}s"
            )
            time.sleep(delay_seconds)
            continue
        response.raise_for_status()
        return response
    raise RuntimeError(f"SpecStream did not answer {path}")


def fetch_lists_page(session, after):
    """
    Read one page of the lists the API key can open, newest first.
    The API returns `next_after` while more pages remain. Only one page of list summaries is held at a time.
    Args:
        session: the HTTP session.
        after: the ID of the list the page starts after, or None for the first page.
    Returns:
        The page, with its `collections` and its `next_after`.
    """
    params = {"limit": __LISTS_PAGE_SIZE, "sort": "recent"}
    if after:
        params["after"] = after
    return send_request(session, "/v1/workspace/collections", params).json()["data"]


def stream_list_records(session, list_id: str):
    """
    Read the records of one list from the response stream, one record at a time.
    The list endpoint returns every record in one response, so the body is parsed as it arrives.
    Args:
        session: the HTTP session.
        list_id: the list to read.
    Returns:
        A generator of the list's records.
    """
    with send_request(session, f"/v1/workspace/collections/{list_id}", stream=True) as response:
        # Let urllib3 decompress the body while it is read.
        response.raw.decode_content = True
        yield from ijson.items(response.raw, "data.rows.item", use_float=True)


def sync_list(session, summary: dict, state: dict):
    """
    Read one list with its records and upsert the list, each record, and each value with its source page.
    Args:
        session: the HTTP session.
        summary: the list summary from the lists endpoint.
        state: the sync state, which does not count this list as synced until update() says so.
    """
    list_id = summary["id"]
    record_count = 0
    for row in stream_list_records(session, list_id):
        upsert_record(list_id, row)
        record_count += 1
        if record_count % __CHECKPOINT_INTERVAL == 0:
            # Checkpoint inside a large list to send its records on. The state still points before this list, so
            # an interrupted sync reads the list again from its first record.
            op.checkpoint(state)
    # The 'upsert' operation is used to insert or update data in the destination table.
    # The list is upserted after its records, so a list in the destination always has all of its records.
    op.upsert(
        table="list",
        data={
            "id": list_id,
            "name": summary.get("name"),
            "project_id": summary.get("project_id"),
            "imported": bool(summary.get("imported")),
            "created_at": summary.get("created_at"),
            "row_count": record_count,
        },
    )
    log.info(f"Synced list {list_id} with {record_count} record(s)")


def upsert_record(list_id: str, row: dict):
    """
    Upsert one record and its values. A value SpecStream couldn't read is synced as null.
    Records SpecStream found carry cells with the page each value came from; rows of a list you imported carry the
    columns you provided.
    Args:
        list_id: the list the record belongs to.
        row: the record as returned by the list endpoint.
    """
    if "provided" in row:
        upsert_imported_record(list_id, row)
        return
    record_id, entity_id, cells = read_found_record(row)
    values = {}
    sources = []
    for cell in cells:
        column_id = cell.get("column_id")
        if not column_id or column_id in __BUTTON_COLUMNS:
            continue
        missing = bool(cell.get("absence"))
        provenance = cell.get("provenance") or {}
        source_url = provenance.get("source_url")
        label = cell.get("label") or column_id
        values[label if label not in values else column_id] = (
            None if missing else cell.get("display")
        )
        if source_url and source_url not in sources:
            sources.append(source_url)
        op.upsert(
            table="record_value",
            data={
                "list_id": list_id,
                "record_id": record_id,
                "column_id": column_id,
                "label": label,
                "value": None if missing else cell.get("display"),
                "raw_value": None if missing else cell.get("value"),
                "source_url": source_url,
                "observed_at": provenance.get("observed_at"),
            },
        )
    op.upsert(
        table="record",
        data={
            "list_id": list_id,
            "record_id": record_id,
            "entity_id": entity_id,
            "values": values,
            "sources": sources,
        },
    )


def read_found_record(row: dict):
    """
    Read the record ID, the entity ID and the cells of a record SpecStream found.
    A list you made by combining lists keeps each column's values under `field_values`, with the value the list
    shows first.
    Args:
        row: the record as returned by the list endpoint.
    Returns:
        A tuple of the record ID, the entity ID (or None) and the list of cells.
    """
    if "field_values" not in row:
        return str(row["ordinal"]), row.get("entity_id"), row.get("cells") or []
    cells = []
    for column_id, entries in (row.get("field_values") or {}).items():
        if not entries:
            continue
        shown = entries[0]
        cells.append(
            {
                "column_id": column_id,
                "label": shown.get("label"),
                "value": shown.get("value"),
                "display": shown.get("display"),
                "absence": shown.get("absence"),
                "provenance": shown.get("provenance"),
            }
        )
    entity_id = next(
        (
            (source.get("row") or {}).get("entity_id")
            for source in row.get("sources") or []
            if (source.get("row") or {}).get("entity_id")
        ),
        None,
    )
    return str(row["id"]), entity_id, cells


def upsert_imported_record(list_id: str, row: dict):
    """
    Upsert one row of a list you imported, with the columns you provided.
    Args:
        list_id: the list the row belongs to.
        row: the row as returned by the list endpoint, with its id and provided columns.
    """
    record_id = str(row["id"])
    provided = row.get("provided") or {}
    for column, value in provided.items():
        op.upsert(
            table="record_value",
            data={
                "list_id": list_id,
                "record_id": record_id,
                "column_id": column,
                "label": column,
                "value": None if value is None else str(value),
                "raw_value": value,
                "source_url": None,
                "observed_at": None,
            },
        )
    op.upsert(
        table="record",
        data={
            "list_id": list_id,
            "record_id": record_id,
            "entity_id": None,
            "values": provided,
            "sources": [],
        },
    )


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
