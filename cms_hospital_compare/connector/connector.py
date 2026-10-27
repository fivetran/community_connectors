"""CMS Hospital Compare connector for the CMS Provider Data Catalog (Care Compare) hospital datasets.

This connector reads five hospital datasets from the public CMS Provider Data Catalog datastore API
(https://data.cms.gov/provider-data): Hospital General Information, Timely and Effective Care, Unplanned Hospital
Visits (hospital and national) and the Hospital Readmissions Reduction Program. The API needs no key.

CMS publishes each dataset as a full-replacement snapshot on a release cycle, and the API has no row-level change
cursor. The connector reads each dataset's release version from the catalog metadata, skips a dataset whose release
it has already loaded, and pages a new release in full. Rows are keyed on the natural key plus the release version,
so each release lands beside the previous ones, and the `dataset_release` table records every release that loaded
to the end. Read the latest recorded release for current values, or compare releases for a trend.

Every value is written as the API returns it, as text. Typing (for example "Not Available" in a numeric score) is
left to the warehouse.

See the Technical Reference documentation (https://fivetran.com/docs/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connector-sdk#connectordevelopmentbestpractices) for details
"""

# For fingerprinting the natural keys of a page, so the repeated-page guard survives a resume
import hashlib

# For reading configuration from a JSON file and serialising unexpected container values
import json

# For sizing the paging guard from the row count the API reports, and rejecting a non-finite delay
import math

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
# Request errors a retry can fix: a dropped connection, a timeout, or a body cut off mid-transfer
__RETRYABLE_REQUEST_ERRORS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)
__MAX_PAGE_SIZE = 1500  # the datastore API rejects a larger limit with HTTP 400
__DEFAULT_DELAY_SECONDS = 0.2
__MAX_DELAY_SECONDS = 10.0
# Extra pages allowed beyond the count the first page reports, so a release that grows while it is paged still
# finishes. Past that the API is not advancing, and the sync stops instead of paging forever.
__PAGE_LIMIT_SLACK = 10
# Distinct natural keys that fingerprint a page: two, so a page of one repeated key is never compared
__SIGNATURE_KEY_COUNT = 2
# Characters of an error response body kept in the error message
__ERROR_BODY_PREVIEW_CHARS = 200
# Rows the setup test reads, enough to prove the datastore query works
__SETUP_TEST_PAGE_SIZE = 1
__LOADED_AT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
__RELEASE_TABLE = "dataset_release"
__RELEASE_COLUMN = "cms_release_version"
__RELEASED_COLUMN = "cms_released"
# Summary counts a resume cannot count again, because it skips the pages and releases that produced them. They are
# carried in every checkpoint and cleared only by the final checkpoint, after op.warning has reported them.
__PENDING_WARNINGS_KEY = "pending_warnings"
__CARRIED_COUNTS = ("skipped", "duplicates")
__CARRIED_LISTS = ("superseded", "short", "recount")

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
    # float() accepts "nan" and "inf", and NaN fails every comparison, so it would pass the range check below
    # and only fail later, at time.sleep, in the middle of a sync.
    if not math.isfinite(seconds):
        raise ValueError(f"request_delay_seconds must be a finite number, got: {value!r}")
    if seconds < 0 or seconds > __MAX_DELAY_SECONDS:
        raise ValueError(
            f"request_delay_seconds must be between 0 and {__MAX_DELAY_SECONDS}, got: {value!r}"
        )
    return seconds


def validate_configuration(configuration: dict) -> tuple:
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary
    configuration values.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Returns:
        (page_size, delay_seconds), parsed once so update() does not parse the configuration again

    Raises:
        ValueError: if any required configuration parameter is missing or invalid.
    """
    page_size = _optional_page_size(configuration.get("page_size"))
    delay_seconds = _optional_delay_seconds(configuration.get("request_delay_seconds"))
    return page_size, delay_seconds


def _convert_to_text(value):
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


def get_column_name(source_name: str) -> str:
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
    row = {get_column_name(key): _convert_to_text(value) for key, value in record.items()}
    row[__RELEASE_COLUMN] = release["release_version"]
    row[__RELEASED_COLUMN] = release["released"]
    return row


def get_natural_key(row: dict, dataset: dict):
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


def compute_page_signature(page_keys: list):
    """
    Fingerprint a page by its first two distinct natural keys, so a repeated page is recognised across syncs.

    Rows are sorted on the full natural key, so no later page of the same release can start with the same two
    distinct keys (as long as the API's sort treats two keys as equal only when the strings are equal, which holds
    for CMS's numeric facility and measure codes); an API that ignores the offset returns them again. Using the
    leading keys, not the whole page, keeps the fingerprint the same if page_size changes between a kill and the
    resume.

    Args:
        page_keys: the natural key of each record on the page, None for a record without one

    Returns:
        a hex digest, or None when the page has fewer than two distinct keys (a page of one key can repeat in
        real data: page_size 1 with a duplicated key, or a run of rows with a blank key)
    """
    distinct = []
    for key in page_keys:
        if key is not None and key not in distinct:
            distinct.append(key)
            if len(distinct) == __SIGNATURE_KEY_COUNT:
                return hashlib.sha256(json.dumps(distinct).encode("utf-8")).hexdigest()
    return None


def create_session() -> requests.Session:
    """
    Create the HTTP session reused for every call in a sync.

    Returns:
        a requests.Session that asks for JSON
    """
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    return session


def wait_before_retry(label: str, attempt: int, reason: str):
    """
    Log a failed attempt and sleep before the next one, doubling the delay after each attempt.

    Args:
        label: a short description of the request, used in the log message
        attempt: the zero-based number of the attempt that failed
        reason: why the attempt failed
    """
    retry_delay_seconds = __BASE_DELAY_SECONDS * (2**attempt)
    # INFO, not WARNING: a retry that then succeeds needs no action; a request that never succeeds fails the sync
    log.info(
        f"{label}: {reason}; retrying in {retry_delay_seconds}s "
        f"(attempt {attempt + 1} of {__MAX_RETRIES})"
    )
    time.sleep(retry_delay_seconds)


def get_with_retries(
    session: requests.Session, url: str, params: dict, label: str
) -> requests.Response:
    """
    GET one API URL, retrying transient failures with exponential backoff.

    A dropped connection, a timeout, a body cut off mid-transfer and a retryable status are retried. Any other
    request error (an invalid URL, too many redirects) will not succeed on a retry, so it fails at once.

    Args:
        session: the shared HTTP session
        url: the full request URL
        params: query parameters
        label: a short description of the request, used in error messages

    Returns:
        the response, with a status that is not retryable

    Raises:
        RuntimeError: when the request still fails after the retries, or fails with an error a retry cannot fix
    """
    for attempt in range(__MAX_RETRIES):
        is_last_attempt = attempt == __MAX_RETRIES - 1
        try:
            response = session.get(url, params=params, timeout=__API_TIMEOUT_SECONDS)
        except __RETRYABLE_REQUEST_ERRORS as e:
            if is_last_attempt:
                raise RuntimeError(
                    f"{label}: request failed after {__MAX_RETRIES} attempts: {e}"
                ) from e
            wait_before_retry(label, attempt, f"request failed: {e}")
            continue
        except requests.exceptions.RequestException as e:
            # Raised as RuntimeError like every other request failure, so update() and the setup test handle it
            raise RuntimeError(f"{label}: request failed: {e}") from e
        if response.status_code in __RETRYABLE_STATUS_CODES:
            if is_last_attempt:
                raise RuntimeError(
                    f"{label}: HTTP {response.status_code} after {__MAX_RETRIES} attempts"
                )
            wait_before_retry(label, attempt, f"HTTP {response.status_code}")
            continue
        return response
    raise RuntimeError(f"{label}: no response after {__MAX_RETRIES} attempts")


def parse_json_object(response: requests.Response, label: str) -> dict:
    """
    Decode a response body that must be a JSON object.

    Args:
        response: the response from get_with_retries
        label: a short description of the request, used in error messages

    Returns:
        the decoded JSON object

    Raises:
        RuntimeError: when the status is an error, or the body is not a JSON object
    """
    if not response.ok:
        raise RuntimeError(
            f"{label}: HTTP {response.status_code}: {response.text[:__ERROR_BODY_PREVIEW_CHARS]}"
        )
    try:
        payload = response.json()
    except ValueError as e:
        raise RuntimeError(f"{label}: response is not JSON") from e
    if not isinstance(payload, dict):
        raise RuntimeError(f"{label}: expected a JSON object, got {type(payload).__name__}")
    return payload


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
        RuntimeError: when the request still fails after the retries, fails with a non-retryable status or
            request error, or the body is not a JSON object
    """
    return parse_json_object(get_with_retries(session, url, params, label), label)


def get_file_version(distribution) -> str:
    """
    The data file version of one catalog distribution.

    Every level of the metadata is type-checked, so an entry of an unexpected shape is skipped instead of
    raising mid-sync or in the setup test.

    Args:
        distribution: one element of the catalog metadata's `distribution` list

    Returns:
        the file version as text, or None when the distribution carries none
    """
    data = distribution.get("data") if isinstance(distribution, dict) else None
    references = data.get("%Ref:downloadURL") if isinstance(data, dict) else None
    for reference in references if isinstance(references, list) else []:
        reference_data = reference.get("data") if isinstance(reference, dict) else None
        candidate = reference_data.get("version") if isinstance(reference_data, dict) else None
        if candidate:
            return str(candidate)
    return None


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
    distributions = payload.get("distribution")
    for distribution in distributions if isinstance(distributions, list) else []:
        version = get_file_version(distribution)
        if version:
            break
    modified = payload.get("%modified") or payload.get("modified")
    if not version and not modified:
        raise RuntimeError(f"{dataset_id}: catalog metadata has no file version or modified date")
    return {
        "release_version": version or str(modified),
        "released": _convert_to_text(payload.get("released")),
        "modified": _convert_to_text(modified),
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

    Raises:
        RuntimeError: when the request fails, or the response has no results list or row count
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
        datasets_state: dataset_id -> {"release_version", "offset", "complete"}, plus the paging evidence
            of a release in progress: "first_page_signature", "last_page_signature", "last_key" and, after a
            short page, "short_page_offset"

    Returns:
        an independent copy of the progress dict
    """
    return {key: dict(value) for key, value in datasets_state.items()}


def build_state(datasets_state: dict, totals: dict | None = None) -> dict:
    """
    Build the state to checkpoint: every dataset's progress, plus the summary counts not yet reported.

    Args:
        datasets_state: dataset_id -> saved progress
        totals: running counters for the sync summary, or None once op.warning has reported them

    Returns:
        the state dict to checkpoint
    """
    state = {"datasets": copy_datasets_state(datasets_state)}
    if totals is not None:
        pending = {key: dict(totals[key]) for key in __CARRIED_COUNTS if totals[key]}
        pending.update({key: list(totals[key]) for key in __CARRIED_LISTS if totals[key]})
        if pending:
            state[__PENDING_WARNINGS_KEY] = pending
    return state


def restore_totals(state: dict) -> dict:
    """
    Start the sync summary from the counts a failed or killed sync checkpointed but never reported.

    Args:
        state: the state the sync starts from

    Returns:
        the running counters for the sync summary
    """
    totals = {
        "upserted": 0,
        "skipped": {},
        "duplicates": {},
        "superseded": [],
        "incomplete": [],
        "short": [],
        "empty": [],
        "recount": [],
    }
    pending = state.get(__PENDING_WARNINGS_KEY)
    if not isinstance(pending, dict):
        return totals
    for key in __CARRIED_COUNTS:
        value = pending.get(key)
        if isinstance(value, dict):
            totals[key] = {
                release_key: count
                for release_key, count in value.items()
                if isinstance(release_key, str) and is_positive_count(count)
            }
    for key in __CARRIED_LISTS:
        value = pending.get(key)
        if isinstance(value, list):
            totals[key] = [item for item in value if isinstance(item, str)]
    if any(totals[key] for key in __CARRIED_COUNTS + __CARRIED_LISTS):
        log.info("Carrying summary counts from an earlier sync that did not finish")
    return totals


def is_positive_count(value) -> bool:
    """
    Check that a carried count read back from state is a positive integer.

    Args:
        value: the value read from state

    Returns:
        True for an int above zero, False for anything else, a bool included
    """
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def release_count_key(dataset_id: str, release_version: str) -> str:
    """
    Name the bucket that holds one release's skipped and repeated record counts.

    Args:
        dataset_id: the CMS dataset identifier
        release_version: the release being paged

    Returns:
        the key under which the release's counts are kept and carried in state
    """
    return f"{dataset_id}:{release_version}"


def drop_restarted_counts(totals: dict, dataset_id: str, saved: dict, release: dict):
    """
    Forget the counts of a release that is paged again from offset 0, so its records are not counted twice.

    The counts of a release that loaded completely stay: that release is not paged again, so its counts are reported
    once, by whichever sync reaches the summary.

    Args:
        totals: running counters for the sync summary; updated in place
        dataset_id: the CMS dataset identifier
        saved: the dataset's saved progress before this sync, or an empty dict
        release: the release that now loads from offset 0
    """
    restarted = [release["release_version"]]
    if saved.get("release_version") and not saved.get("complete"):
        restarted.append(saved["release_version"])
    for version in restarted:
        for key in __CARRIED_COUNTS:
            totals[key].pop(release_count_key(dataset_id, version), None)


def resolve_start(table: str, release: dict, saved: dict) -> tuple:
    """
    Decide whether a release resumes from its saved progress or loads from the start.

    Args:
        table: the destination table, used in log messages
        release: the dataset's current release, from get_release
        saved: the dataset's saved progress, or an empty dict

    Returns:
        (is_resuming, the progress to resume from, or an empty dict when the release loads from the start)
    """
    is_resuming = saved.get("release_version") == release["release_version"]
    if is_resuming and not saved.get("offset"):
        # Nothing to resume: no page was read, or the release wrote no rows and was saved at offset 0 to be read again
        is_resuming = False
    elif is_resuming and ("first_page_signature" not in saved or "rows_written" not in saved):
        # State saved before the paging evidence and the rows written were persisted cannot show which pages it saw
        # or whether any row reached the table, so the guards could not check the resume. Rows are keyed on the
        # release, so loading it again is idempotent.
        log.info(
            f"{table}: saved progress has no paging evidence or rows written; reloading the release from the start"
        )
        is_resuming = False
    if is_resuming:
        offset = int(saved.get("offset") or 0)
        log.info(f"{table}: resuming release {release['release_version']} at offset {offset}")
        return True, saved
    log.info(f"{table}: loading release {release['release_version']} ({release['released']})")
    return False, {}


def check_page_signature(table: str, page_keys: list, paging: dict):
    """
    Stop the sync when a page repeats the release's first page or the page before it.

    An API that does not apply the offset returns the same page again. The offset advances regardless, so without
    this check the loop would end at the count and record a release of repeated rows as complete. Both signatures
    are saved in state, so the first page after a resume is checked too.

    Args:
        table: the destination table, used in the error message
        page_keys: the natural key of each record on the page, None for a record without one
        paging: the release's paging progress; the signatures are updated in place

    Raises:
        RuntimeError: when the page repeats an earlier page
    """
    signature = compute_page_signature(page_keys)
    if paging["offset"] == 0:
        paging["first_page_signature"] = signature
    elif signature is not None and signature in (
        paging["first_page_signature"],
        paging["last_page_signature"],
    ):
        raise RuntimeError(
            f"{table}: the page at offset {paging['offset']} repeats an earlier page; "
            "the API is not applying the offset"
        )
    if signature is not None:
        # A page without a signature (empty, or one key) keeps the last one: no earlier page's leading keys can
        # come back later in sorted data, so the older signature still catches a repeat.
        paging["last_page_signature"] = signature


def upsert_page_rows(
    table: str, rows: list, page_keys: list, paging: dict, totals: dict, count_key: str
):
    """
    Upsert the rows of one page, skipping rows without a natural key and counting repeated keys.

    Args:
        table: the destination table
        rows: the rows built from the page, None for a record that is not an object
        page_keys: the natural key of each row, None for a row without one
        paging: the release's paging progress; the last natural key and the rows written are updated in place
        totals: running counters for the sync summary; updated in place
        count_key: the release's bucket for skipped and repeated record counts
    """
    for row, key in zip(rows, page_keys):
        if row is None or key is None:
            totals["skipped"][count_key] = totals["skipped"].get(count_key, 0) + 1
            continue
        # Rows are sorted on the full natural key, so a repeated key is next to the row it repeats. Comparing
        # with the previous key keeps memory flat however large the release is.
        if key == paging["previous_key"]:
            totals["duplicates"][count_key] = totals["duplicates"].get(count_key, 0) + 1
        paging["previous_key"] = key
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table=table, data=row)
        totals["upserted"] += 1
        paging["rows_written"] += 1


def save_page_progress(
    dataset_id: str,
    release: dict,
    paging: dict,
    page_size: int,
    datasets_state: dict,
    totals: dict,
):
    """
    Save a release's paging progress and evidence after a page, and checkpoint every dataset's progress.

    Args:
        dataset_id: the CMS dataset identifier
        release: the release being paged
        paging: the release's paging progress after the page
        page_size: rows per page
        datasets_state: dataset_id -> saved progress; updated in place
        totals: running counters for the sync summary, checkpointed with the progress
    """
    progress = {
        "release_version": release["release_version"],
        "offset": paging["offset"],
        "complete": False,
        "first_page_signature": paging["first_page_signature"],
        "last_page_signature": paging["last_page_signature"],
        "last_key": list(paging["previous_key"]) if paging["previous_key"] else None,
        "rows_written": paging["rows_written"],
    }
    if paging["last_page_length"] < page_size and paging["offset"] < paging["count"]:
        # Saved with the offset in one checkpoint, so the next sync knows the API itself stopped short here,
        # not that a sync was killed after a full page.
        progress["short_page_offset"] = paging["offset"]
    datasets_state[dataset_id] = progress
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state=build_state(datasets_state, totals))


def page_release(
    session: requests.Session,
    dataset: dict,
    release: dict,
    paging: dict,
    datasets_state: dict,
    page_size: int,
    delay_seconds: float,
    totals: dict,
):
    """
    Page a release from the saved offset until the API returns a short or empty page.

    Args:
        session: the shared HTTP session
        dataset: the dataset entry
        release: the release being paged
        paging: the release's paging progress; updated in place after every page
        datasets_state: dataset_id -> saved progress; updated in place and checkpointed after every page
        page_size: rows per page
        delay_seconds: pause between page requests
        totals: running counters for the sync summary; updated in place

    Raises:
        RuntimeError: when a page repeats an earlier page or returns records past the reported count, or the
            release needs more pages than the count allows
    """
    table = dataset["table"]
    max_pages = None
    while True:
        records, paging["count"] = fetch_page(session, dataset, paging["offset"], page_size)
        paging["pages"] += 1
        paging["last_page_length"] = len(records)
        rows = [
            build_row(record, release) if isinstance(record, dict) else None for record in records
        ]
        page_keys = [get_natural_key(row, dataset) if row is not None else None for row in rows]
        check_page_signature(table, page_keys, paging)
        if records and paging["offset"] + len(records) > paging["count"]:
            # Records past the reported count mean the count is too low, and the release would be recorded with
            # a count that hides them. A count that grows during paging still passes: each page is held to its own
            # count. An empty page has no records past the count: a file that shrank under the same version, or was
            # replaced by a smaller one, ends paging, and finish_release reads it again or reports it replaced.
            raise RuntimeError(
                f"{table}: the page at offset {paging['offset']} returned {len(records)} records, past the "
                f"{paging['count']} rows CMS reports"
            )
        if max_pages is None:
            # Size the guard from the first page's count, so it scales with page_size and the release size.
            remaining = max(paging["count"] - paging["offset"], 0)
            max_pages = math.ceil(remaining / page_size) + __PAGE_LIMIT_SLACK
        upsert_page_rows(
            table,
            rows,
            page_keys,
            paging,
            totals,
            release_count_key(dataset["dataset_id"], release["release_version"]),
        )
        # Every record on the page was consumed, so the offset advances by the page's length.
        paging["offset"] += len(records)
        save_page_progress(
            dataset["dataset_id"], release, paging, page_size, datasets_state, totals
        )
        # Stop only on a short page. A full page that ends at the reported count is followed by one more request,
        # so a count too low by whole pages still meets the check above instead of completing a truncated release.
        if len(records) < page_size:
            return
        if paging["pages"] >= max_pages:
            raise RuntimeError(
                f"{table}: more than {max_pages} pages for about {paging['count']} rows; the reported row count kept growing"
            )
        time.sleep(delay_seconds)


def accept_short_release(
    table: str, paging: dict, is_resuming: bool, saved: dict, totals: dict
) -> bool:
    """
    Decide whether a release that stopped short of the reported count can be recorded as loaded.

    Args:
        table: the destination table, used in log and summary messages
        paging: the release's paging progress at the end of the sync
        is_resuming: whether the sync resumed the release from saved progress
        saved: the progress the sync resumed from, or an empty dict
        totals: running counters for the sync summary; updated in place

    Returns:
        True when the count is overstated and the release is recorded with the rows returned, False when the
        release stays incomplete for the next sync to resume
    """
    offset = paging["offset"]
    count = paging["count"]
    is_empty_again = is_resuming and paging["pages"] == 1 and paging["last_page_length"] == 0
    if is_empty_again and offset > 0 and saved.get("short_page_offset") == offset:
        # The previous sync saw the API stop short at this offset, and it still returns nothing past it, so
        # the count is overstated rather than a page lost. Never at offset 0: a release recorded with no rows
        # would become the latest release and empty its views. Record the release with the rows CMS returned:
        # otherwise it never completes and the views, which join on dataset_release, never show it.
        # row_count below api_row_count keeps the gap between the count and the records returned visible in the
        # ledger. Records skipped for a missing key are inside row_count and reported by their own warning.
        entry = f"{table}: {offset} of {count} rows"
        if entry not in totals["short"]:
            totals["short"].append(entry)
        log.warning(f"{table}: CMS reported {count} rows but returned {offset} on two syncs")
        return True
    totals["incomplete"].append(f"{table}: {offset} of {count} rows")
    log.warning(f"{table}: paging stopped at {offset} of {count} rows; the next sync resumes here")
    return False


def finish_release(
    session: requests.Session,
    dataset: dict,
    release: dict,
    paging: dict,
    is_resuming: bool,
    saved: dict,
    datasets_state: dict,
    totals: dict,
):
    """
    Record a paged release in dataset_release and mark it complete, unless it wrote no rows, is short, was
    replaced, or now has a count below the rows already read.

    Args:
        session: the shared HTTP session
        dataset: the dataset entry
        release: the release that was paged
        paging: the release's paging progress at the end of the sync
        is_resuming: whether the sync resumed the release from saved progress
        saved: the progress the sync resumed from, or an empty dict
        datasets_state: dataset_id -> saved progress; updated in place and checkpointed when the release completes
            or wrote no rows
        totals: running counters for the sync summary; updated in place
    """
    dataset_id = dataset["dataset_id"]
    table = dataset["table"]
    offset = paging["offset"]
    if paging["rows_written"] == 0:
        # No row of this release reached the table: CMS returned none, or every record was skipped for a missing
        # natural key (a renamed key column) or for not being an object. Recording it would make it the latest
        # release and empty every view that reads it, with row_count equal to api_row_count, so no gap would show.
        # It stays incomplete and the earlier release stays the latest. It is saved at offset 0 without paging
        # evidence, so the next sync reads it again from the start: once its records can be loaded (CMS fixes the
        # data under the same version, or a redeploy maps the renamed key), it loads in full. This runs before the
        # short-page check, so a release that stopped short with no rows is not resumed past the records it skipped.
        entry = f"{table}: release {release['release_version']}"
        if offset > 0:
            entry += f" ({offset} records, none could be loaded)"
        totals["empty"].append(entry)
        log.warning(
            f"{table}: no rows written for release {release['release_version']} from {offset} records"
        )
        datasets_state[dataset_id] = {
            "release_version": release["release_version"],
            "offset": 0,
            "complete": False,
        }
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state=build_state(datasets_state, totals))
        return
    # A short page before the reported count means rows are missing. Leave the release incomplete so the next sync
    # resumes at this offset, instead of recording a truncated release as complete.
    if offset < paging["count"] and not accept_short_release(
        table, paging, is_resuming, saved, totals
    ):
        return

    # CMS can replace a file while it is being paged. Mark the release complete only if it is still current;
    # otherwise the next sync loads the new release from the start.
    current = get_release(session, dataset_id)
    if current["release_version"] != release["release_version"]:
        # A carried entry and a second replacement on the resume name the same table once
        if table not in totals["superseded"]:
            totals["superseded"].append(table)
        log.warning(
            f"{table}: release {release['release_version']} was replaced by {current['release_version']} during the sync"
        )
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state=build_state(datasets_state, totals))
        return

    if offset > paging["count"]:
        # Under the same version, CMS now reports fewer rows than were already read: one bad or empty response
        # mid-release (a count of 0 from a proxy or a datastore re-import), or a file that shrank in place.
        # Recording it would mark a truncated release complete for the whole version, or keep rows CMS no longer
        # reports, with row_count above api_row_count and no warning. It stays incomplete and is saved at offset 0
        # without paging evidence, so the next sync reads it again from the start and records the count it finds.
        # A smaller replacement under a new version is handled by the check above.
        entry = f"{table}: {offset} rows read, {paging['count']} reported"
        if entry not in totals["recount"]:
            totals["recount"].append(entry)
        log.warning(
            f"{table}: CMS now reports {paging['count']} rows for release {release['release_version']}, "
            f"fewer than the {offset} already read; the next sync reads it again from the start"
        )
        datasets_state[dataset_id] = {
            "release_version": release["release_version"],
            "offset": 0,
            "complete": False,
        }
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state=build_state(datasets_state, totals))
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
            "api_row_count": paging["count"],
            "rows_written": paging["rows_written"],
            "loaded_at": datetime.now(timezone.utc).strftime(__LOADED_AT_FORMAT),
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
    op.checkpoint(state=build_state(datasets_state, totals))
    log.info(f"{table}: release {release['release_version']} loaded, {offset} rows")


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
    is_resuming, start = resolve_start(table, release, saved)
    if not is_resuming:
        drop_restarted_counts(totals, dataset_id, saved, release)
    saved = start

    # The paging guards live in state, not only in memory, so a resume is held to the same checks as a sync that
    # was never interrupted.
    paging = {
        "offset": int(saved.get("offset") or 0),
        "count": 0,
        "pages": 0,
        "last_page_length": 0,
        "first_page_signature": saved.get("first_page_signature"),
        "last_page_signature": saved.get("last_page_signature"),
        "previous_key": tuple(saved["last_key"]) if saved.get("last_key") else None,
        "rows_written": int(saved.get("rows_written") or 0),
    }
    page_release(
        session, dataset, release, paging, datasets_state, page_size, delay_seconds, totals
    )
    finish_release(session, dataset, release, paging, is_resuming, saved, datasets_state, totals)


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-methods#schema
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
                "rows_written": "INT",
                "loaded_at": "UTC_DATETIME",
            },
        }
    )
    return tables


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-methods#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: Source Examples : CMS Hospital Compare")
    page_size, delay_seconds = validate_configuration(configuration)
    datasets_state = dict(state.get("datasets") or {})
    log.info(f"Syncing {len(__DATASETS)} CMS datasets, {page_size} rows per page")

    totals = restore_totals(state)
    session = create_session()
    try:
        for dataset in __DATASETS:
            sync_dataset(session, dataset, datasets_state, page_size, delay_seconds, totals)
    finally:
        session.close()

    skipped = sum(totals["skipped"].values())
    duplicates = sum(totals["duplicates"].values())
    if skipped:
        message = (
            f"Skipped {skipped} CMS records that had no natural key value or were not objects."
        )
        log.warning(message)
        op.warning(message)
    if duplicates:
        message = f"{duplicates} CMS records repeated a natural key within one release; the last one was kept."
        log.warning(message)
        op.warning(message)
    if totals["superseded"]:
        message = (
            f"CMS replaced a release during the sync for {', '.join(totals['superseded'])}; "
            "the partial release was not recorded, and the new release is loaded from offset 0 by the sync after it."
        )
        log.warning(message)
        op.warning(message)
    if totals["incomplete"]:
        message = (
            f"CMS returned fewer rows than it reported for {'; '.join(totals['incomplete'])}; "
            "those releases stay incomplete and the next sync resumes them."
        )
        log.warning(message)
        op.warning(message)
    if totals["empty"]:
        message = (
            f"CMS returned no rows that could be loaded for {'; '.join(totals['empty'])}; those releases were not "
            "recorded, so the views keep the earlier release if there is one, and the next sync tries them again."
        )
        log.warning(message)
        op.warning(message)
    if totals["short"]:
        message = (
            f"CMS reported more rows than it returned on two syncs in a row for {'; '.join(totals['short'])}; "
            "those releases are recorded with the rows returned, and dataset_release shows row_count below "
            "api_row_count."
        )
        log.warning(message)
        op.warning(message)
    if totals["recount"]:
        message = (
            f"CMS reported fewer rows than were already read for {'; '.join(totals['recount'])}; those releases "
            "were not recorded, and the next sync reads them again from the start."
        )
        log.warning(message)
        op.warning(message)
    log.info(f"Sync complete: {totals['upserted']} rows upserted")

    # The summary is reported above, so this checkpoint drops the carried counts. A kill before it reports them
    # again on the next sync: a warning can repeat, but it is never lost.
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state=build_state(datasets_state))


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
            fetch_page(session, __DATASETS[0], 0, __SETUP_TEST_PAGE_SIZE)
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
