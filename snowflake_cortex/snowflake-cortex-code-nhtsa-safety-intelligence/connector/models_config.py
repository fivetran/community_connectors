"""Fixed-list mode configuration parsing: placeholders, dates, the pause and the model list."""

# For decoding the models JSON array
import json

# For rejecting NaN and infinity in the configured pause
import math

# For the configured ISO dates and the current model year
from datetime import date, datetime, timezone

DEFAULT_PAUSE_SECONDS = 0.2
__MAX_PAUSE_SECONDS = 10.0
__MAX_MODELS = 200
__MIN_MODEL_YEAR = 1949
__MAX_MODEL_YEAR_AHEAD = 2  # model years run up to two years ahead of the calendar
KEY_SEPARATOR = "|"


def _is_placeholder(value) -> bool:
    """
    Type-safe check for an unset or placeholder configuration value.

    Args:
        value: the raw configuration value

    Returns:
        True when the value is None, empty, or an angle-bracket placeholder such as "<MODELS>"
    """
    if value is None or value == "":
        return True
    if not isinstance(value, str):
        return False
    return value.startswith("<") and value.endswith(">")


def _parse_iso_date(value, key: str) -> date:
    """
    Parse a YYYY-MM-DD configuration value.

    Args:
        value: the raw configuration value
        key: the configuration key, used in the error message

    Returns:
        the parsed date

    Raises:
        ValueError: when the value is missing, a placeholder, or not a YYYY-MM-DD date
    """
    if _is_placeholder(value):
        raise ValueError(f"Missing required configuration value: {key}")
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError as e:
        raise ValueError(f"{key} must be a YYYY-MM-DD date, got: {value!r}") from e


def _optional_pause_seconds(value) -> float:
    """
    Parse request_pause_seconds; unset or placeholder falls back to the default.

    Args:
        value: the raw request_pause_seconds configuration value

    Returns:
        the pause between API calls in seconds

    Raises:
        ValueError: when the value is not a number between 0 and the ceiling
    """
    if _is_placeholder(value):
        return DEFAULT_PAUSE_SECONDS
    try:
        seconds = float(value)
    except (TypeError, ValueError) as e:
        raise ValueError(f"request_pause_seconds must be a number, got: {value!r}") from e
    # float() accepts 'nan' and 'inf', and NaN passes both range comparisons
    if not math.isfinite(seconds) or seconds < 0 or seconds > __MAX_PAUSE_SECONDS:
        raise ValueError(
            f"request_pause_seconds must be between 0 and {__MAX_PAUSE_SECONDS}, got: {value!r}"
        )
    return seconds


def _parse_model_entry(entry, index: int, max_model_year: int) -> dict:
    """
    Validate one entry of the model list.

    Args:
        entry: one decoded element of the `models` JSON array
        index: its position, used in error messages
        max_model_year: the latest model year accepted

    Returns:
        a dict with make, model, nhtsa_model and the list of model years

    Raises:
        ValueError: when a field is missing, blank, or out of range
    """
    if not isinstance(entry, dict):
        raise ValueError(f"models[{index}] must be an object, got: {type(entry).__name__}")
    parsed = {}
    for field in ("make", "model", "nhtsa_model"):
        text = entry.get(field)
        if not isinstance(text, str) or not text.strip():
            raise ValueError(f"models[{index}].{field} must be a non-empty string")
        if KEY_SEPARATOR in text:
            raise ValueError(f"models[{index}].{field} must not contain {KEY_SEPARATOR!r}")
        parsed[field] = text.strip()
    years = []
    for field in ("year_from", "year_to"):
        year = entry.get(field)
        # bool is a subclass of int, so reject it explicitly
        if isinstance(year, bool) or not isinstance(year, int):
            raise ValueError(f"models[{index}].{field} must be an integer model year")
        if year < __MIN_MODEL_YEAR or year > max_model_year:
            raise ValueError(
                f"models[{index}].{field} must be between {__MIN_MODEL_YEAR} and {max_model_year}"
            )
        years.append(year)
    if years[0] > years[1]:
        raise ValueError(f"models[{index}].year_from must not be after year_to")
    parsed["years"] = list(range(years[0], years[1] + 1))
    return parsed


def parse_models(raw) -> list:
    """
    Decode and validate the `models` configuration value.

    Args:
        raw: the raw models configuration value, a JSON array encoded as a string

    Returns:
        the list of model entries, in configured order

    Raises:
        ValueError: when the value is missing, not a JSON array, empty, too long, or holds a duplicate make and model, compared case-insensitively
    """
    if _is_placeholder(raw):
        raise ValueError("Missing required configuration value: models")
    try:
        decoded = json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError as e:
        raise ValueError(f"models must be a JSON array of objects: {e}") from e
    if not isinstance(decoded, list) or not decoded:
        raise ValueError("models must be a non-empty JSON array of objects")
    if len(decoded) > __MAX_MODELS:
        raise ValueError(f"models holds {len(decoded)} entries; the maximum is {__MAX_MODELS}")
    max_model_year = datetime.now(timezone.utc).year + __MAX_MODEL_YEAR_AHEAD
    models = [_parse_model_entry(entry, i, max_model_year) for i, entry in enumerate(decoded)]
    # The API matches make and model case-insensitively, so "Toyota|Camry" and "TOYOTA|CAMRY" are the same vehicle
    labels = [model_label(m).casefold() for m in models]
    duplicates = sorted({label for label in labels if labels.count(label) > 1})
    if duplicates:
        raise ValueError(f"models lists the same make and model more than once: {duplicates}")
    return models


def model_label(model: dict) -> str:
    """
    The state key for one configured model.

    Args:
        model: a parsed model entry

    Returns:
        "make|model", unique within a validated model list
    """
    return f"{model['make']}{KEY_SEPARATOR}{model['model']}"
