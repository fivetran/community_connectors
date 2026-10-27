"""Fixed-list mode helpers: report-date parsing, row shaping, the per-vehicle fetch and the checkpoint state.

Nothing here writes to the destination; connector.py owns every upsert, delete, truncate and checkpoint.
"""

# For time-based operations and rate limiting
import time

# For parsing the API's DD/MM/YYYY report dates
from datetime import date, datetime

# For making HTTP requests to external APIs
import requests

# For logging retries
from fivetran_connector_sdk import Logging as log

# The state-key separator shared with the configuration parser
from models_config import KEY_SEPARATOR

__RECALLS_URL = "https://api.nhtsa.gov/recalls/recallsByVehicle"
__API_TIMEOUT_SECONDS = 30
__MAX_RETRIES = 3
__BASE_DELAY_SECONDS = 2
__RETRYABLE_STATUS_CODES = [429, 500, 502, 503, 504]
# The API answers 400, not an empty list, for a vehicle year it lists no recalls for, known or not
__UNKNOWN_VEHICLE_STATUS_CODE = 400
# A protected-key campaign placeholder meaning "every campaign of this model year"
__YEAR_MARKER = "*"
__REPORT_DATE_FORMAT = "%d/%m/%Y"


def parse_report_date(value):
    """
    Parse the API's ReportReceivedDate, which is DD/MM/YYYY.

    Args:
        value: the raw ReportReceivedDate

    Returns:
        the parsed date, or None when it is missing or malformed
    """
    if not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value.strip(), __REPORT_DATE_FORMAT).date()
    except ValueError:
        return None


def _as_bool(value):
    """
    Coerce an API flag to a bool.

    Args:
        value: the API value; NHTSA sends JSON booleans, and occasionally the strings "true" or "false"

    Returns:
        True, False, or None when the value is absent or unrecognised
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in ("true", "false"):
        return value.strip().lower() == "true"
    return None


def _as_text(value):
    """
    Coerce an API text field to a stripped string.

    Args:
        value: the API value

    Returns:
        the stripped string, or None when the value is absent or blank
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def row_key(row: dict) -> str:
    """
    The per-model state key for one row: campaign number and model year.

    Args:
        row: a recalls row built by build_recall_row

    Returns:
        "campaign|model_year"
    """
    return f"{row['nhtsa_campaign_number']}{KEY_SEPARATOR}{row['model_year']}"


def build_recall_row(record: dict, model: dict, model_year: int, reported: date) -> dict:
    """
    Shape one API result into a recalls row.

    make and model are the configured names, so the row joins to vehicle data that uses marketing names;
    nhtsa_make and nhtsa_model are what the API returned.

    Args:
        record: one element of the API's `results` array
        model: the configured model entry the call was made for
        model_year: the model year the call was made for
        reported: the parsed report received date

    Returns:
        a flat dict whose keys match the declared columns
    """
    return {
        "nhtsa_campaign_number": _as_text(record.get("NHTSACampaignNumber")),
        "make": model["make"],
        "model": model["model"],
        "model_year": model_year,
        "nhtsa_make": _as_text(record.get("Make")),
        "nhtsa_model": _as_text(record.get("Model")),
        "manufacturer": _as_text(record.get("Manufacturer")),
        "component": _as_text(record.get("Component")),
        "summary": _as_text(record.get("Summary")),
        "consequence": _as_text(record.get("Consequence")),
        "remedy": _as_text(record.get("Remedy")),
        "notes": _as_text(record.get("Notes")),
        "report_received_date": reported.isoformat(),
        "park_it": _as_bool(record.get("parkIt")),
        "park_outside": _as_bool(record.get("parkOutSide")),
        "over_the_air_update": _as_bool(record.get("overTheAirUpdate")),
    }


def create_list_session() -> requests.Session:
    """
    Create the HTTP session reused for every recalls call in a fixed-list sync.

    Returns:
        a requests.Session that asks for JSON
    """
    session = requests.Session()
    # The API's edge answers 403 to some custom User-Agent strings, so the library default is kept.
    session.headers.update({"Accept": "application/json"})
    return session


class UnknownVehicle(list):
    """The empty result of a call NHTSA answered with no recalls: its 400 no-recalls body, or a 200 with a zero
    Count. NHTSA sends that answer both for a vehicle year it lists no recalls for and for a vehicle it does not
    know, such as a misspelled make or nhtsa_model, so the two cannot be told apart.
    """


def _is_unknown_vehicle_body(response: requests.Response) -> bool:
    """
    Check that a 400 carries the body NHTSA sends for a vehicle year with no recalls, or a vehicle it does not know.

    Args:
        response: the HTTP 400 response

    Returns:
        True only for a JSON object whose `Count` is 0 and whose `results` is an empty list
    """
    try:
        payload = response.json()
    except ValueError:
        return False
    if not isinstance(payload, dict):
        return False
    count = payload.get("Count")
    # bool is a subclass of int, so False would otherwise pass as a zero count
    is_zero_count = isinstance(count, int) and not isinstance(count, bool) and count == 0
    return is_zero_count and payload.get("results") == []


def fetch_vehicle_recalls(
    session: requests.Session, make: str, nhtsa_model: str, model_year: int
) -> list:
    """
    Fetch the recall campaigns for one make, model and model year, retrying transient failures.

    Args:
        session: the shared HTTP session
        make: the make, as NHTSA spells it (matching is case-insensitive)
        nhtsa_model: the model, as NHTSA spells it
        model_year: the model year

    Returns:
        the API's `results` list; an empty UnknownVehicle when the API answered with no recalls (its 400
        no-recalls body, or a 200 with a zero Count)

    Raises:
        RuntimeError: when the request still fails after the retries, fails with a non-retryable status, answers
            400 without NHTSA's no-recalls body, or returns a body that is not a JSON object with a list
            `results` (a missing or null `results` passes only with a Count of 0)
    """
    params = {"make": make, "model": nhtsa_model, "modelYear": model_year}
    for attempt in range(__MAX_RETRIES):
        is_last_attempt = attempt == __MAX_RETRIES - 1
        try:
            response = session.get(__RECALLS_URL, params=params, timeout=__API_TIMEOUT_SECONDS)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if is_last_attempt:
                raise RuntimeError(
                    f"{make} {nhtsa_model} {model_year}: request failed after {__MAX_RETRIES} attempts: {e}"
                ) from e
            delay = __BASE_DELAY_SECONDS * (2**attempt)
            log.warning(
                f"{make} {nhtsa_model} {model_year}: {type(e).__name__}, retrying in {delay}s "
                f"(attempt {attempt + 1} of {__MAX_RETRIES})"
            )
            time.sleep(delay)
            continue
        except requests.exceptions.RequestException as e:
            # Any other request error (an invalid URL, too many redirects) will not succeed on retry
            raise RuntimeError(f"{make} {nhtsa_model} {model_year}: request failed: {e}") from e
        if response.status_code == __UNKNOWN_VEHICLE_STATUS_CODE:
            if _is_unknown_vehicle_body(response):
                return UnknownVehicle()
            # Any other 400 (an edge or WAF page, a changed API) is not proof the vehicle has no campaigns, and an
            # empty result here would make reconciliation delete the vehicle's tracked campaigns.
            raise RuntimeError(
                f"{make} {nhtsa_model} {model_year}: HTTP 400 without NHTSA's no-recalls body: "
                f"{response.text[:200]}"
            )
        if response.status_code in __RETRYABLE_STATUS_CODES:
            if is_last_attempt:
                raise RuntimeError(
                    f"{make} {nhtsa_model} {model_year}: HTTP {response.status_code} after {__MAX_RETRIES} attempts"
                )
            delay = __BASE_DELAY_SECONDS * (2**attempt)
            log.warning(
                f"{make} {nhtsa_model} {model_year}: HTTP {response.status_code}, retrying in {delay}s "
                f"(attempt {attempt + 1} of {__MAX_RETRIES})"
            )
            time.sleep(delay)
            continue
        if not response.ok:
            raise RuntimeError(
                f"{make} {nhtsa_model} {model_year}: HTTP {response.status_code}: {response.text[:200]}"
            )
        try:
            payload = response.json()
        except ValueError as e:
            raise RuntimeError(f"{make} {nhtsa_model} {model_year}: response is not JSON") from e
        # A missing or null `results` with a zero Count means no campaigns, and a list must hold Count records. Any
        # other shape is refused before a write, because an empty result here makes reconciliation delete the
        # vehicle's tracked campaigns.
        if not isinstance(payload, dict):
            raise RuntimeError(
                f"{make} {nhtsa_model} {model_year}: expected a JSON object, got {type(payload).__name__}"
            )
        results = payload.get("results")
        if results is None:
            # Only a zero Count says the vehicle has no campaigns. Without it, a JSON object with no `results` (a
            # proxy's error object, a changed API) is unknown, and returning [] would delete the tracked campaigns.
            count = payload.get("Count")
            if isinstance(count, int) and not isinstance(count, bool) and count == 0:
                return UnknownVehicle()
            raise RuntimeError(
                f"{make} {nhtsa_model} {model_year}: response has no `results` list and Count is {count!r}"
            )
        if not isinstance(results, list):
            raise RuntimeError(
                f"{make} {nhtsa_model} {model_year}: expected `results` to be a list, got {type(results).__name__}"
            )
        # NHTSA's Count is the length of `results`. A list that disagrees with it, or has no integer Count beside it
        # (a proxy's object with an empty list), is not a complete answer, and reconciling it would delete campaigns.
        count = payload.get("Count")
        if not isinstance(count, int) or isinstance(count, bool) or count != len(results):
            raise RuntimeError(
                f"{make} {nhtsa_model} {model_year}: `results` holds {len(results)} records but Count is {count!r}"
            )
        if not results:
            # A zero Count is the same no-recalls answer as the 400, so the caller treats the two alike
            return UnknownVehicle()
        return results
    raise RuntimeError(f"{make} {nhtsa_model} {model_year}: no request was attempted")


def collect_model_rows(
    session: requests.Session,
    model: dict,
    date_from: date,
    date_to: date,
    pause_seconds: float,
) -> tuple:
    """
    Fetch every configured model year for one model and keep the campaigns in the report-date window.

    The API can list one campaign more than once for a vehicle, so rows are de-duplicated on the primary key.
    A result that is skipped is unknown, not absent, so it protects the tracked rows it could stand for from the
    reconciliation delete: the one campaign when its number is readable, otherwise every campaign of that year.

    Args:
        session: the shared HTTP session
        model: a parsed model entry
        date_from: first report received date kept, inclusive
        date_to: last report received date kept, inclusive
        pause_seconds: pause after each API call

    Returns:
        (rows keyed by row_key, count of results skipped for a missing campaign number or report date,
        protected row keys, where a campaign of "*" covers the whole model year (see is_protected), set of
        model years NHTSA answered with no recalls)
    """
    rows = {}
    skipped = 0
    protected = set()
    no_recall_years = set()
    for model_year in model["years"]:
        results = fetch_vehicle_recalls(session, model["make"], model["nhtsa_model"], model_year)
        if isinstance(results, UnknownVehicle):
            no_recall_years.add(model_year)
        for record in results:
            if not isinstance(record, dict):
                skipped += 1
                protected.add(f"{__YEAR_MARKER}{KEY_SEPARATOR}{model_year}")
                continue
            reported = parse_report_date(record.get("ReportReceivedDate"))
            campaign = _as_text(record.get("NHTSACampaignNumber"))
            if reported is None or not campaign:
                skipped += 1
                marker = campaign or __YEAR_MARKER
                protected.add(f"{marker}{KEY_SEPARATOR}{model_year}")
                continue
            if not (date_from <= reported <= date_to):
                continue
            row = build_recall_row(record, model, model_year, reported)
            rows[row_key(row)] = row
        time.sleep(pause_seconds)
    return rows, skipped, protected, no_recall_years


def is_protected(key: str, protected: set) -> bool:
    """
    Check whether a tracked row key is covered by a skipped result and so must not be deleted.

    Args:
        key: a tracked "campaign|model_year" key
        protected: the protected keys returned by collect_model_rows

    Returns:
        True when the key itself, or its whole model year, is protected
    """
    model_year = key.rsplit(KEY_SEPARATOR, 1)[1]
    return key in protected or f"{__YEAR_MARKER}{KEY_SEPARATOR}{model_year}" in protected


def model_fingerprint(model: dict, date_from: date, date_to: date) -> str:
    """
    Describe everything that decides one model's rows, so a resumed sync can tell a finished model from a changed one.

    Args:
        model: a parsed model entry
        date_from: first report received date kept
        date_to: last report received date kept

    Returns:
        "nhtsa_model|first_year-last_year|date_from|date_to"
    """
    years = model["years"]
    return f"{model['nhtsa_model']}{KEY_SEPARATOR}{years[0]}-{years[-1]}{KEY_SEPARATOR}{date_from}{KEY_SEPARATOR}{date_to}"


def model_query(model: dict, date_from: date, date_to: date) -> str:
    """
    Describe what a sync asks NHTSA for one model and which of its answers it keeps: the spelling sent and the
    report-date window. A tracked campaign is kept on a no-recalls answer only when this query covers the one it was
    tracked under (see query_covers), because a no-recalls answer to another spelling says nothing about it.

    Args:
        model: a parsed model entry
        date_from: first report received date kept
        date_to: last report received date kept

    Returns:
        "nhtsa_model|date_from|date_to"
    """
    return f"{model['nhtsa_model']}{KEY_SEPARATOR}{date_from}{KEY_SEPARATOR}{date_to}"


def query_covers(tracked_query, query: str) -> bool:
    """
    Check that a no-recalls answer to `query` speaks for the campaigns tracked under `tracked_query`.

    NHTSA matches the model spelling without regard to ASCII letter case, and the report window is applied by the
    connector, not sent to NHTSA, so the answer covers the tracked campaigns when the spelling is the same in any
    letter case and the window still contains the one they were tracked under. A widened window then keeps them; a
    narrowed one does not, since a tracked campaign may fall outside it.

    Args:
        tracked_query: the model_query that tracked every row the destination holds for one model year, or None
            when no single query did
        query: the model_query of this sync

    Returns:
        True when the tracked campaigns may be kept on a no-recalls answer to `query`
    """
    if not isinstance(tracked_query, str):
        return False
    # rsplit, because only the two dates are known to hold no separator
    tracked = tracked_query.rsplit(KEY_SEPARATOR, 2)
    current = query.rsplit(KEY_SEPARATOR, 2)
    if len(tracked) != 3:
        return False
    # NHTSA matches ASCII letters without regard to case but not Unicode folds such as German sharp s to "ss", so
    # only an ASCII spelling is compared case-insensitively.
    old_spelling, new_spelling = tracked[0], current[0]
    both_ascii = old_spelling.isascii() and new_spelling.isascii()
    same_case_insensitive = both_ascii and old_spelling.lower() == new_spelling.lower()
    same_spelling = old_spelling == new_spelling or same_case_insensitive
    # ISO dates compare correctly as strings
    return same_spelling and current[1] <= tracked[1] and tracked[2] <= current[2]


def build_state(
    next_model: str,
    synced_keys: dict,
    synced_years: dict,
    last_completed_sync,
    pass_done: dict,
    synced_queries: dict,
) -> dict:
    """
    Assemble the full state written at every checkpoint, so no checkpoint drops a key.

    Args:
        next_model: "make|model" of the next model the pass will sync; None when a full pass completed
        synced_keys: "make|model" -> sorted "campaign|model_year" keys currently in the recalls table
        synced_years: "make|model" -> sorted model years currently in the configured_vehicle table
        last_completed_sync: UTC timestamp of the last completed pass, or None
        pass_done: "make|model" -> model_fingerprint of each model finished in the current pass; empty after a
            completed pass
        synced_queries: "make|model" -> {model year: the model_query that tracked every row of that year in
            synced_keys, or None when rows kept across a query change were tracked under an earlier one}

    Returns:
        the state dict
    """
    return {
        "next_model": next_model,
        "pass_done": dict(pass_done),
        "synced_keys": {label: list(keys) for label, keys in synced_keys.items()},
        "synced_years": {label: list(years) for label, years in synced_years.items()},
        "synced_queries": dict(synced_queries),
        "last_completed_sync": last_completed_sync,
    }
