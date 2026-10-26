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
__UNKNOWN_VEHICLE_STATUS_CODE = (
    400  # the API answers 400, not an empty list, for a vehicle it does not know
)
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
        the API's `results` list; empty when the API does not know the vehicle

    Raises:
        RuntimeError: when the request still fails after the retries, or fails with a non-retryable status
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
            return []
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
        results = (payload or {}).get("results") if isinstance(payload, dict) else None
        return results if isinstance(results, list) else []
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

    Args:
        session: the shared HTTP session
        model: a parsed model entry
        date_from: first report received date kept, inclusive
        date_to: last report received date kept, inclusive
        pause_seconds: pause after each API call

    Returns:
        (rows keyed by row_key, count of results skipped for a missing campaign number or report date)
    """
    rows = {}
    skipped = 0
    for model_year in model["years"]:
        for record in fetch_vehicle_recalls(
            session, model["make"], model["nhtsa_model"], model_year
        ):
            if not isinstance(record, dict):
                skipped += 1
                continue
            reported = parse_report_date(record.get("ReportReceivedDate"))
            if reported is None or not _as_text(record.get("NHTSACampaignNumber")):
                skipped += 1
                continue
            if not (date_from <= reported <= date_to):
                continue
            row = build_recall_row(record, model, model_year, reported)
            rows[row_key(row)] = row
        time.sleep(pause_seconds)
    return rows, skipped


def build_state(
    next_model: str, synced_keys: dict, synced_years: dict, last_completed_sync
) -> dict:
    """
    Assemble the full state written at every checkpoint, so no checkpoint drops a key.

    Args:
        next_model: "make|model" of the model a resumed sync starts at; None when a full pass completed
        synced_keys: "make|model" -> sorted "campaign|model_year" keys currently in the recalls table
        synced_years: "make|model" -> sorted model years currently in the configured_vehicle table
        last_completed_sync: UTC timestamp of the last completed pass, or None

    Returns:
        the state dict
    """
    return {
        "next_model": next_model,
        "synced_keys": {label: list(keys) for label, keys in synced_keys.items()},
        "synced_years": {label: list(years) for label, years in synced_years.items()},
        "last_completed_sync": last_completed_sync,
    }
