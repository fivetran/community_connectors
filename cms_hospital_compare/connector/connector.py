"""CMS Hospital Compare connector for the CMS Provider Data Catalog (Care Compare) hospital datasets.

This connector reads five hospital datasets from the public CMS Provider Data Catalog datastore API
(https://data.cms.gov/provider-data): Hospital General Information, Timely and Effective Care, Unplanned Hospital
Visits (hospital and national) and the Hospital Readmissions Reduction Program. The API needs no key.

CMS publishes each dataset as a full-replacement snapshot on a release cycle, and the API has no row-level change
cursor. The connector reads each dataset's release version from the catalog metadata, skips a dataset whose release
it has already loaded, and pages a new release in full. Rows are keyed on the natural key plus the release version,
so each release lands beside the previous ones, and the `dataset_release` table records every release that loaded
completely. Read the latest complete release for current values, or compare releases for a trend.

Every value is written as the API returns it, as text. Typing (for example "Not Available" in a numeric score) is
left to the warehouse.

See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file and serialising unexpected container values
import json

# For time-based operations and rate limiting
import time

# For the timestamp written when a release finishes loading
from datetime import datetime, timezone

# For URL-encoding dataset identifiers into request paths
from urllib.parse import quote

# For making HTTP requests to external APIs
import requests

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For building the setup form shown to users when configuring the connector in Fivetran
from fivetran_connector_sdk import ConfigurationForm

# For defining individual form fields
from fivetran_connector_sdk import form_field

# For returning success/failure responses from the setup test below
from fivetran_connector_sdk import Test

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

__BASE_URL = "https://data.cms.gov/provider-data/api/1"
__API_TIMEOUT_SECONDS = 60
__MAX_RETRIES = 4
__BASE_DELAY_SECONDS = 2
__RETRYABLE_STATUS_CODES = [429, 500, 502, 503, 504]
__MAX_PAGE_SIZE = 1500  # the datastore API rejects a larger limit with HTTP 400
__DEFAULT_DELAY_SECONDS = 0.2
__MAX_DELAY_SECONDS = 10.0
__MAX_PAGES_PER_DATASET = (
    2000  # a guard against a paging loop; the largest dataset is about 100 pages
)
__RELEASE_TABLE = "dataset_release"
__RELEASE_COLUMN = "cms_release_version"
__RELEASED_COLUMN = "cms_released"

# Source columns renamed before they reach the destination: `sample` is a reserved word in several SQL engines,
# and a leading underscore is not portable across destinations.
__COLUMN_RENAMES = {"sample": "sample_size", "_condition": "measure_condition"}

# One entry per dataset: destination table, CMS dataset identifier, and the columns that identify a row within
# one release. The datastore query is sorted on the natural key so offset paging is stable.
__DATASETS = (
    {"table": "hospital", "dataset_id": "xubh-q36u", "natural_key": ["facility_id"]},
    {
        "table": "timely_effective_care",
        "dataset_id": "yv7e-xc69",
        "natural_key": ["facility_id", "measure_id"],
    },
    {
        "table": "unplanned_hospital_visit",
        "dataset_id": "632h-zaca",
        "natural_key": ["facility_id", "measure_id"],
    },
    {
        "table": "unplanned_hospital_visit_national",
        "dataset_id": "cvcs-xecj",
        "natural_key": ["measure_id"],
    },
    {"table": "hrrp", "dataset_id": "9n3s-kdb3", "natural_key": ["facility_id", "measure_name"]},
)


def _is_placeholder(value) -> bool:
    """
    Type-safe check for an unset or placeholder configuration value.

    Args:
        value: the raw configuration value

    Returns:
        True when the value is None, empty, or an angle-bracket placeholder such as "<PAGE_SIZE>"
    """
    if value is None or value == "":
        return True
    if not isinstance(value, str):
        return False
    return value.strip().startswith("<") and value.strip().endswith(">")


def _optional_page_size(value) -> int:
    """
    Parse page_size; unset or placeholder falls back to the API maximum.

    Args:
        value: the raw page_size configuration value

    Returns:
        the number of rows requested per page

    Raises:
        ValueError: when the value is not a whole number between 1 and the API maximum
    """
    if _is_placeholder(value):
        return __MAX_PAGE_SIZE
    # bool is a subclass of int, so reject it explicitly
    if isinstance(value, bool):
        raise ValueError(f"page_size must be a whole number, got: {value!r}")
    try:
        size = int(str(value).strip())
    except ValueError as e:
        raise ValueError(f"page_size must be a whole number, got: {value!r}") from e
    if size <= 0 or size > __MAX_PAGE_SIZE:
        raise ValueError(f"page_size must be between 1 and {__MAX_PAGE_SIZE}, got: {value!r}")
    return size


def _optional_delay_seconds(value) -> float:
    """
    Parse request_delay_seconds; unset or placeholder falls back to the default.

    Args:
        value: the raw request_delay_seconds configuration value

    Returns:
        the pause between page requests in seconds

    Raises:
        ValueError: when the value is not a number between 0 and the ceiling
    """
    if _is_placeholder(value):
        return __DEFAULT_DELAY_SECONDS
    if isinstance(value, bool):
        raise ValueError(f"request_delay_seconds must be a number, got: {value!r}")
    try:
        seconds = float(str(value).strip())
    except ValueError as e:
        raise ValueError(f"request_delay_seconds must be a number, got: {value!r}") from e
    if seconds < 0 or seconds > __MAX_DELAY_SECONDS:
        raise ValueError(
            f"request_delay_seconds must be between 0 and {__MAX_DELAY_SECONDS}, got: {value!r}"
        )
    return seconds


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary
    configuration values.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: if any required configuration parameter is missing or invalid.
    """
    _optional_page_size(configuration.get("page_size"))
    _optional_delay_seconds(configuration.get("request_delay_seconds"))


def _as_text(value):
    """
    Keep an API value as text, the way the datastore API returns it.

    Args:
        value: the API value

    Returns:
        the value as a string, None when absent, or JSON text for an unexpected list or object
    """
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def column_name(source_name: str) -> str:
    """
    The destination column name for one source column.

    Args:
        source_name: the column name the API returns

    Returns:
        the lowercase name, with reserved or non-portable names renamed
    """
    name = source_name.strip().lower()
    return __COLUMN_RENAMES.get(name, name)


def build_row(record: dict, release: dict) -> dict:
    """
    Shape one datastore record into a destination row stamped with its release.

    Args:
        record: one element of the API's `results` array
        release: the dataset's release, from get_release

    Returns:
        a flat dict of text values plus the release columns
    """
    row = {column_name(key): _as_text(value) for key, value in record.items()}
    row[__RELEASE_COLUMN] = release["release_version"]
    row[__RELEASED_COLUMN] = release["released"]
    return row


def natural_key(row: dict, dataset: dict):
    """
    The natural key of a row within one release.

    Args:
        row: a row built by build_row
        dataset: the dataset entry

    Returns:
        a tuple of key values, or None when any key value is missing or blank
    """
    values = tuple(row.get(column) for column in dataset["natural_key"])
    if any(value is None or not value.strip() for value in values):
        return None
    return values


def create_session() -> requests.Session:
    """
    Create the HTTP session reused for every call in a sync.

    Returns:
        a requests.Session that asks for JSON
    """
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    return session


def fetch_json(session: requests.Session, url: str, params: dict, label: str) -> dict:
    """
    GET one API URL and decode the JSON body, retrying transient failures with exponential backoff.

    Args:
        session: the shared HTTP session
        url: the full request URL
        params: query parameters
        label: a short description of the request, used in error messages

    Returns:
        the decoded JSON object

    Raises:
        RuntimeError: when the request still fails after the retries, fails with a non-retryable status, or the
            body is not a JSON object
    """
    for attempt in range(__MAX_RETRIES):
        is_last_attempt = attempt == __MAX_RETRIES - 1
        try:
            response = session.get(url, params=params, timeout=__API_TIMEOUT_SECONDS)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if is_last_attempt:
                raise RuntimeError(
                    f"{label}: request failed after {__MAX_RETRIES} attempts: {e}"
                ) from e
            time.sleep(__BASE_DELAY_SECONDS * (2**attempt))
            continue
        if response.status_code in __RETRYABLE_STATUS_CODES:
            if is_last_attempt:
                raise RuntimeError(
                    f"{label}: HTTP {response.status_code} after {__MAX_RETRIES} attempts"
                )
            time.sleep(__BASE_DELAY_SECONDS * (2**attempt))
            continue
        if not response.ok:
            raise RuntimeError(f"{label}: HTTP {response.status_code}: {response.text[:200]}")
        try:
            payload = response.json()
        except ValueError as e:
            raise RuntimeError(f"{label}: response is not JSON") from e
        if not isinstance(payload, dict):
            raise RuntimeError(f"{label}: expected a JSON object, got {type(payload).__name__}")
        return payload
    raise RuntimeError(f"{label}: no response after {__MAX_RETRIES} attempts")


def get_release(session: requests.Session, dataset_id: str) -> dict:
    """
    Read a dataset's current release from the catalog metadata.

    The distribution's file version (a Unix epoch) changes whenever CMS replaces the data file, so it is the
    release identifier. The catalog's `%modified` timestamp is the fallback when no version is published.

    Args:
        session: the shared HTTP session
        dataset_id: the CMS dataset identifier

    Returns:
        a dict with release_version, released (the catalog release date) and modified

    Raises:
        RuntimeError: when the metadata carries neither a file version nor a modified timestamp
    """
    url = f"{__BASE_URL}/metastore/schemas/dataset/items/{quote(dataset_id, safe='')}"
    payload = fetch_json(session, url, {"show-reference-ids": "true"}, f"{dataset_id} metadata")
    version = None
    for distribution in payload.get("distribution") or []:
        references = ((distribution or {}).get("data") or {}).get("%Ref:downloadURL") or []
        for reference in references:
            candidate = ((reference or {}).get("data") or {}).get("version")
            if candidate:
                version = str(candidate)
                break
        if version:
            break
    modified = payload.get("%modified") or payload.get("modified")
    if not version and not modified:
        raise RuntimeError(f"{dataset_id}: catalog metadata has no file version or modified date")
    return {
        "release_version": version or str(modified),
        "released": _as_text(payload.get("released")),
        "modified": _as_text(modified),
    }


def fetch_page(session: requests.Session, dataset: dict, offset: int, page_size: int) -> tuple:
    """
    Fetch one page of a dataset, sorted on its natural key so offset paging is stable.

    Args:
        session: the shared HTTP session
        dataset: the dataset entry
        offset: the number of rows already read from this release
        page_size: rows per page

    Returns:
        (list of records, total row count reported by the API)
    """
    url = f"{__BASE_URL}/datastore/query/{quote(dataset['dataset_id'], safe='')}/0"
    params = {"limit": page_size, "offset": offset, "count": "true", "results": "true"}
    for index, column in enumerate(dataset["natural_key"]):
        params[f"sorts[{index}][property]"] = column
        params[f"sorts[{index}][order]"] = "asc"
    payload = fetch_json(session, url, params, f"{dataset['dataset_id']} offset {offset}")
    results = payload.get("results")
    count = payload.get("count")
    if not isinstance(results, list):
        raise RuntimeError(
            f"{dataset['dataset_id']} offset {offset}: response has no results list"
        )
    if isinstance(count, bool) or not isinstance(count, int):
        raise RuntimeError(f"{dataset['dataset_id']} offset {offset}: response has no row count")
    return results, count


def copy_datasets_state(datasets_state: dict) -> dict:
    """
    Copy the per-dataset progress, so a checkpoint holds every dataset, not only the one being synced.

    Args:
        datasets_state: dataset_id -> {"release_version", "offset", "complete"}

    Returns:
        an independent copy of the progress dict
    """
    return {key: dict(value) for key, value in datasets_state.items()}


def sync_dataset(
    session: requests.Session,
    dataset: dict,
    datasets_state: dict,
    page_size: int,
    delay_seconds: float,
    totals: dict,
):
    """
    Load one dataset's current release, resuming a partly loaded release from its saved offset.

    Args:
        session: the shared HTTP session
        dataset: the dataset entry
        datasets_state: dataset_id -> saved progress; updated in place and checkpointed after every page
        page_size: rows per page
        delay_seconds: pause between page requests
        totals: running counters for the sync summary; updated in place
    """
    dataset_id = dataset["dataset_id"]
    table = dataset["table"]
    release = get_release(session, dataset_id)
    saved = datasets_state.get(dataset_id) or {}
    if saved.get("release_version") == release["release_version"] and saved.get("complete"):
        log.info(f"{table}: release {release['release_version']} already loaded, skipping")
        return
    if saved.get("release_version") == release["release_version"]:
        offset = int(saved.get("offset") or 0)
        log.info(f"{table}: resuming release {release['release_version']} at offset {offset}")
    else:
        offset = 0
        log.info(f"{table}: loading release {release['release_version']} ({release['released']})")

    seen_keys = set()
    count = 0
    for _ in range(__MAX_PAGES_PER_DATASET):
        records, count = fetch_page(session, dataset, offset, page_size)
        for record in records:
            if not isinstance(record, dict):
                totals["skipped"] += 1
                continue
            row = build_row(record, release)
            key = natural_key(row, dataset)
            if key is None:
                totals["skipped"] += 1
                continue
            if key in seen_keys:
                totals["duplicates"] += 1
            seen_keys.add(key)
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table=table, data=row)
            totals["upserted"] += 1
        # Every record on the page was consumed, so the offset advances by the page's length.
        offset += len(records)
        datasets_state[dataset_id] = {
            "release_version": release["release_version"],
            "offset": offset,
            "complete": False,
        }
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state={"datasets": copy_datasets_state(datasets_state)})
        if len(records) < page_size or offset >= count:
            break
        time.sleep(delay_seconds)
    else:
        raise RuntimeError(
            f"{table}: more than {__MAX_PAGES_PER_DATASET} pages; the API may be ignoring the offset"
        )

    # CMS can replace a file while it is being paged. Mark the release complete only if it is still current;
    # otherwise the next sync loads the new release from the start.
    current = get_release(session, dataset_id)
    if current["release_version"] != release["release_version"]:
        totals["superseded"].append(table)
        log.warning(
            f"{table}: release {release['release_version']} was replaced by {current['release_version']} during the sync"
        )
        return

    # The 'upsert' operation is used to insert or update data in the destination table.
    # The first argument is the name of the destination table.
    # The second argument is a dictionary containing the record to be upserted.
    op.upsert(
        table=__RELEASE_TABLE,
        data={
            "dataset_id": dataset_id,
            "release_version": release["release_version"],
            "table_name": table,
            "cms_released": release["released"],
            "cms_modified": release["modified"],
            "row_count": offset,
            "api_row_count": count,
            "loaded_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    )
    datasets_state[dataset_id] = {
        "release_version": release["release_version"],
        "offset": offset,
        "complete": True,
    }
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state={"datasets": copy_datasets_state(datasets_state)})
    log.info(f"{table}: release {release['release_version']} loaded, {offset} rows")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    tables = []
    for dataset in __DATASETS:
        columns = {column: "STRING" for column in dataset["natural_key"]}
        columns[__RELEASE_COLUMN] = "STRING"
        columns[__RELEASED_COLUMN] = "STRING"
        tables.append(
            {
                "table": dataset["table"],
                "primary_key": dataset["natural_key"] + [__RELEASE_COLUMN],
                "columns": columns,
            }
        )
    tables.append(
        {
            "table": __RELEASE_TABLE,
            "primary_key": ["dataset_id", "release_version"],
            "columns": {
                "dataset_id": "STRING",
                "release_version": "STRING",
                "table_name": "STRING",
                "cms_released": "STRING",
                "cms_modified": "STRING",
                "row_count": "INT",
                "api_row_count": "INT",
                "loaded_at": "UTC_DATETIME",
            },
        }
    )
    return tables


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
    log.warning("Example: Source Examples : CMS Hospital Compare")
    validate_configuration(configuration)

    page_size = _optional_page_size(configuration.get("page_size"))
    delay_seconds = _optional_delay_seconds(configuration.get("request_delay_seconds"))
    datasets_state = dict(state.get("datasets") or {})
    log.info(f"Syncing {len(__DATASETS)} CMS datasets, {page_size} rows per page")

    totals = {"upserted": 0, "skipped": 0, "duplicates": 0, "superseded": []}
    session = create_session()
    try:
        for dataset in __DATASETS:
            sync_dataset(session, dataset, datasets_state, page_size, delay_seconds, totals)
    finally:
        session.close()

    if totals["skipped"]:
        message = f"Skipped {totals['skipped']} CMS records with a missing natural key value."
        log.warning(message)
        op.warning(message)
    if totals["duplicates"]:
        message = f"{totals['duplicates']} CMS records repeated a natural key within one release; the last one was kept."
        log.warning(message)
        op.warning(message)
    if totals["superseded"]:
        message = (
            f"CMS replaced a release during the sync for {', '.join(totals['superseded'])}; "
            "the next sync loads the new release."
        )
        op.warning(message)
    log.info(f"Sync complete: {totals['upserted']} rows upserted")

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state={"datasets": copy_datasets_state(datasets_state)})


def verify_api_access(configuration: dict):
    """
    Setup test: validate the configuration, read the hospital dataset's release and fetch one row.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Returns:
        Test().success() or Test().failure(reason)
    """
    test = Test()
    try:
        validate_configuration(configuration)
        session = create_session()
        try:
            get_release(session, __DATASETS[0]["dataset_id"])
            fetch_page(session, __DATASETS[0], 0, 1)
        finally:
            session.close()
    except (ValueError, RuntimeError) as e:
        return test.failure(str(e))
    return test.success()


def configuration_form() -> ConfigurationForm:
    """
    Declare the setup form Fivetran renders for this connector and the setup test it can run.

    Returns:
        ConfigurationForm with one field per configuration key and one setup test
    """
    config_form = ConfigurationForm()

    # Both fields are optional; an empty value falls back to the default shown as the placeholder
    config_form.add_field(
        form_field.TextField(
            "page_size",
            "Rows per page",
            form_field.TextField.plain_text,
            description=f"Optional. Rows requested per API call, 1 to {__MAX_PAGE_SIZE}. Defaults to {__MAX_PAGE_SIZE}.",
            required=False,
            placeholder=str(__MAX_PAGE_SIZE),
        )
    )
    config_form.add_field(
        form_field.TextField(
            "request_delay_seconds",
            "Pause between API calls (seconds)",
            form_field.TextField.plain_text,
            description=f"Optional. Defaults to {__DEFAULT_DELAY_SECONDS}.",
            required=False,
            placeholder=str(__DEFAULT_DELAY_SECONDS),
        )
    )

    # Fivetran runs this test when the user clicks "Test Connection"
    config_form.add_test(label="Call the CMS Provider Data Catalog API", func=verify_api_access)

    return config_form


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema, configuration_form=configuration_form)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
#
# IMPORTANT: The recommended way to test your connector is using the Fivetran debug command:
#   fivetran debug
#
# This local testing environment is designed to mirror Fivetran's production runtime and avoids
# unexpected discrepancies when deploying to Fivetran.
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
