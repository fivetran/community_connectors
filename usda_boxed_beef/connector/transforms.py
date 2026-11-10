"""Row building for the USDA boxed beef connector: number, date and item parsing with no HTTP, config or state.

The LMR DataMart API returns every number as a string with thousands separators ("1,382.45") and drops the
leading zero of a fraction ("-.77"). Dates are MM/DD/YYYY, and published times are MM/DD/YYYY HH:MM:SS in US
Central time. An item with no trades that day is still returned, with its numbers null, empty or ".00".
"""

# For rejecting NaN and infinity before a value reaches a DOUBLE column
import math

# For matching the IMPS number and fat limitation inside an item description
import re

# For parsing USDA dates and converting published times to UTC
from datetime import datetime, timezone

# For the US Central time zone the API reports published times in, with daylight saving applied per timestamp
from zoneinfo import ZoneInfo

# The table names, the volume classes and the cutout field names
from tables import (
    CUT_PRICES_TABLE,
    CUTOUT_FIELDS,
    CUTOUT_VALUES_TABLE,
    GROUND_BEEF_PRICES_TABLE,
    PRIMAL_VALUES_TABLE,
    REPORTS_TABLE,
    TRIMMINGS_PRICES_TABLE,
    VOLUME_CLASSES,
    VOLUME_TABLE,
)

# The API reports published times in US Central time
SOURCE_TIME_ZONE = ZoneInfo("America/Chicago")

__REPORT_DATE_FORMAT = "%m/%d/%Y"
__PUBLISHED_FORMAT = "%m/%d/%Y %H:%M:%S"

# "Rib, ribeye, lip-on, bn-in (109E  1)": the cut name, then the IMPS number and the fat limitation (1 to 6) in
# parentheses. The IMPS number is blank on some rows, as in "Loin, strip loin bnls. 1x1 (  1)".
__IMPS_SUFFIX_PATTERN = re.compile(r"^(?P<name>.*?)\s*\((?P<imps>[0-9A-Z]*)\s+(?P<fl>[1-6])\)$")

# "121D 4  Plate, Inside Skirt": the IMPS number and the fat limitation first, then the cut name after two or more
# spaces. The IMPS number is blank on some rows, as in "     4  Pectoral Meat".
__IMPS_PREFIX_PATTERN = re.compile(
    r"^\s*(?:(?P<imps>[0-9][0-9A-Z]*)\s+)?(?P<fl>[1-6])\s{2,}(?P<name>\S.*)$"
)

__PRICE_FIELDS = ("price_range_low", "price_range_high", "weighted_average")


def as_float(value):
    """
    Convert one API number to a float.

    Args:
        value: the API value, usually a string such as "1,382.45" or "-.77"

    Returns:
        The number, or None for null, an empty string or a bool

    Raises:
        ValueError: if the value is not a number, or is NaN or infinite
    """
    if value is None or isinstance(value, bool):
        return None
    text = str(value).replace(",", "").strip()
    if not text:
        return None
    number = float(text)
    if not math.isfinite(number):
        raise ValueError(f"not a finite number: {value!r}")
    return number


def parse_report_date(value) -> str:
    """
    Convert a USDA report date to ISO format.

    Args:
        value: a date such as "11/06/2026"

    Returns:
        The date as YYYY-MM-DD

    Raises:
        ValueError: if the value is not a MM/DD/YYYY date
    """
    return datetime.strptime(str(value).strip(), __REPORT_DATE_FORMAT).date().isoformat()


def parse_published_at(value):
    """
    Convert a published time in US Central time to UTC.

    The offset is looked up for each timestamp, so a time on either side of a daylight saving change converts
    correctly.

    Args:
        value: a time such as "11/06/2026 14:44:18", or None

    Returns:
        The UTC time as YYYY-MM-DDTHH:MM:SSZ, or None when the value is missing

    Raises:
        ValueError: if the value is present but not in the API's format
    """
    if value is None or not str(value).strip():
        return None
    local = datetime.strptime(str(value).strip(), __PUBLISHED_FORMAT).replace(
        tzinfo=SOURCE_TIME_ZONE
    )
    return local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def split_item_description(description):
    """
    Split an item description into its IMPS number, fat limitation and cut name.

    Args:
        description: the item text from a cuts section, in either of the two layouts the API uses

    Returns:
        Tuple of (imps_code, fat_limitation, cut_name). The first two are None when the text carries no code.
    """
    text = (description or "").rstrip()
    for pattern in (__IMPS_SUFFIX_PATTERN, __IMPS_PREFIX_PATTERN):
        match = pattern.match(text)
        if match:
            return match.group("imps") or None, match.group("fl"), match.group("name").strip()
    return None, None, text.strip() or None


class ReportLayoutError(ValueError):
    """Raised when rows from the API lack a field this connector reads, which means the report layout changed."""


class RowIssues:
    """Counts of values a sync could not use, reported once at the end of the sync."""

    def __init__(self):
        self.unparseable_numbers = 0
        self.unparseable_published_dates = 0
        self.rows_without_key = 0
        self.prices_without_trades = 0


def _require(raw: dict, field: str):
    """
    Fail when a row lacks a field this connector reads.

    Every row the API sends carries every field of its section, with null for an empty value. A missing field means
    the layout changed, and reading it as null would overwrite values already loaded.

    Args:
        raw: one API row
        field: the field the connector reads

    Raises:
        ReportLayoutError: if the field is not in the row
    """
    if field not in raw:
        raise ReportLayoutError(
            f"Rows from the API have no {field!r} field, so the report layout has changed"
        )


def _number(raw: dict, field: str, issues: RowIssues):
    """
    Read one numeric field, counting a value that is not a number instead of failing the sync.

    Args:
        raw: one API row
        field: the field to read
        issues: the counts to add to

    Returns:
        The number, or None
    """
    _require(raw, field)
    try:
        return as_float(raw.get(field))
    except ValueError:
        issues.unparseable_numbers += 1
        return None


def _key_fields(slug_id: str, raw: dict, issues: RowIssues):
    """
    Read the report date and published time every row carries.

    Args:
        slug_id: the report id
        raw: one API row
        issues: the counts to add to

    Returns:
        A dict with slug_id, report_date and published_at, or None when the report date is missing or unreadable
    """
    try:
        report_date = parse_report_date(raw.get("report_date"))
    except ValueError:
        issues.rows_without_key += 1
        return None
    try:
        published_at = parse_published_at(raw.get("published_date"))
    except ValueError:
        issues.unparseable_published_dates += 1
        published_at = None
    return {"slug_id": slug_id, "report_date": report_date, "published_at": published_at}


def _price_measures(raw: dict, issues: RowIssues) -> dict:
    """
    Read the trade count, pounds and prices of one negotiated-sales row.

    An item with no trades that day comes back with ".00" prices. Those are not prices of zero, so they are
    written as null. A price is kept only on a row with at least one trade and is never zero, so a row whose
    trade count is missing or unreadable carries no price.

    Args:
        raw: one API row
        issues: the counts to add to

    Returns:
        A dict of the five measures
    """
    measures = {
        "number_trades": _number(raw, "number_trades", issues),
        "total_pounds": _number(raw, "total_pounds", issues),
    }
    traded = (measures["number_trades"] or 0) > 0
    for field in __PRICE_FIELDS:
        price = _number(raw, field, issues)
        if price and not traded:
            # A price on a row without a readable trade count above zero is not written, and is counted
            issues.prices_without_trades += 1
        measures[field] = price if traded and price else None
    if not traded and not measures["total_pounds"]:
        measures["total_pounds"] = None
    return measures


def build_report_rows(slug_id: str, section: dict, raw: dict, issues: RowIssues) -> list:
    """
    Build the reports row from a Summary section row.

    Args:
        slug_id: the report id
        section: the section's entry in tables.REPORTS
        raw: one API row
        issues: the counts to add to

    Returns:
        A list with one row, or an empty list when the row has no report date
    """
    for field in ("report_title", "is_correction", "narrative"):
        _require(raw, field)
    row = _key_fields(slug_id, raw, issues)
    if row is None:
        return []
    narrative = (raw.get("narrative") or "").strip() or None
    row.update(
        {
            "report_title": raw.get("report_title"),
            "is_correction": str(raw.get("is_correction") or "").strip().upper() == "Y",
            "narrative": narrative,
        }
    )
    return [row]


def build_cutout_rows(slug_id: str, section: dict, raw: dict, issues: RowIssues) -> list:
    """
    Build a partial cutout_values row from a Current Cutout Values or Change From Prior Day row.

    The connector merges the two partial rows of a report date before it upserts.

    Args:
        slug_id: the report id
        section: the section's entry in tables.REPORTS
        raw: one API row
        issues: the counts to add to

    Returns:
        A list with one partial row, or an empty list when the row has no report date
    """
    row = _key_fields(slug_id, raw, issues)
    if row is None:
        return []
    for column, field in CUTOUT_FIELDS[section["name"]].items():
        row[column] = _number(raw, field, issues)
    return [row]


def build_primal_rows(slug_id: str, section: dict, raw: dict, issues: RowIssues) -> list:
    """
    Build a primal_values row from a Composite Primal Values row.

    Args:
        slug_id: the report id
        section: the section's entry in tables.REPORTS
        raw: one API row
        issues: the counts to add to

    Returns:
        A list with one row, or an empty list when the row has no report date or primal name
    """
    _require(raw, "primal_desc")
    row = _key_fields(slug_id, raw, issues)
    primal = (raw.get("primal_desc") or "").strip()
    if row is None or not primal:
        if row is not None:
            issues.rows_without_key += 1
        return []
    row.update(
        {
            "primal": primal,
            "choice_value": _number(raw, "choice_600_900", issues),
            "select_value": _number(raw, "select_600_900", issues),
        }
    )
    return [row]


def build_volume_rows(slug_id: str, section: dict, raw: dict, issues: RowIssues) -> list:
    """
    Build one volume row per volume class the report's Current Volume section carries.

    Args:
        slug_id: the report id
        section: the section's entry in tables.REPORTS
        raw: one API row
        issues: the counts to add to

    Returns:
        A list with one row per volume class in the section's volume_classes
    """
    key = _key_fields(slug_id, raw, issues)
    if key is None:
        return []
    rows = []
    for volume_class in section["volume_classes"]:
        loads_field, pounds_field = VOLUME_CLASSES[volume_class]
        rows.append(
            {
                **key,
                "volume_class": volume_class,
                "loads": _number(raw, loads_field, issues),
                "pounds": _number(raw, pounds_field, issues),
            }
        )
    return rows


def _raw_description(raw: dict) -> str:
    """
    Return the item text of a price row as sent.

    The API names the field item_description or trim_description, depending on the section. A row with neither
    means the layout changed.

    Args:
        raw: one API row

    Returns:
        The item text, or an empty string
    """
    if "item_description" not in raw and "trim_description" not in raw:
        _require(raw, "item_description")
    return raw.get("item_description") or raw.get("trim_description") or ""


def _description(raw: dict):
    """
    Return the item text of a price row with surrounding whitespace removed.

    Args:
        raw: one API row

    Returns:
        The item text, or None when it is empty
    """
    return _raw_description(raw).strip() or None


def build_cut_rows(slug_id: str, section: dict, raw: dict, issues: RowIssues) -> list:
    """
    Build a cut_prices row from a Choice, Select, Choice and Select, or Prime cuts row.

    Args:
        slug_id: the report id
        section: the section's entry in tables.REPORTS, which carries the grade
        raw: one API row
        issues: the counts to add to

    Returns:
        A list with one row, or an empty list when the row has no report date or item
    """
    row = _key_fields(slug_id, raw, issues)
    raw_text = _raw_description(raw)
    description = _description(raw)
    if row is None or description is None:
        if row is not None:
            issues.rows_without_key += 1
        return []
    imps_code, fat_limitation, cut_name = split_item_description(raw_text)
    row.update(
        {
            "grade": section["grade"],
            "item_description": description,
            "imps_code": imps_code,
            "fat_limitation": fat_limitation,
            "cut_name": cut_name,
            **_price_measures(raw, issues),
        }
    )
    return [row]


def build_ground_beef_rows(slug_id: str, section: dict, raw: dict, issues: RowIssues) -> list:
    """
    Build a ground_beef_prices row from a Ground Beef or Blended Ground Beef row.

    Args:
        slug_id: the report id
        section: the section's entry in tables.REPORTS
        raw: one API row
        issues: the counts to add to

    Returns:
        A list with one row, or an empty list when the row has no report date or item
    """
    row = _key_fields(slug_id, raw, issues)
    description = _description(raw)
    if row is None or description is None:
        if row is not None:
            issues.rows_without_key += 1
        return []
    row.update(
        {
            "item_description": description,
            "section": section["name"],
            **_price_measures(raw, issues),
        }
    )
    return [row]


def build_trimmings_rows(slug_id: str, section: dict, raw: dict, issues: RowIssues) -> list:
    """
    Build a trimmings_prices row from a Beef Trimmings row.

    Args:
        slug_id: the report id
        section: the section's entry in tables.REPORTS
        raw: one API row
        issues: the counts to add to

    Returns:
        A list with one row, or an empty list when the row has no report date or item
    """
    row = _key_fields(slug_id, raw, issues)
    description = _description(raw)
    if row is None or description is None:
        if row is not None:
            issues.rows_without_key += 1
        return []
    row.update({"item_description": description, **_price_measures(raw, issues)})
    return [row]


# Row builders by the "builder" name each section carries in tables.REPORTS
BUILDERS = {
    "report": build_report_rows,
    "cutout": build_cutout_rows,
    "primal": build_primal_rows,
    "volume": build_volume_rows,
    "cut": build_cut_rows,
    "ground_beef": build_ground_beef_rows,
    "trimmings": build_trimmings_rows,
}

# Every table a builder can write, so a test can check the map against schema()
BUILDER_TABLES = {
    "report": REPORTS_TABLE,
    "cutout": CUTOUT_VALUES_TABLE,
    "primal": PRIMAL_VALUES_TABLE,
    "volume": VOLUME_TABLE,
    "cut": CUT_PRICES_TABLE,
    "ground_beef": GROUND_BEEF_PRICES_TABLE,
    "trimmings": TRIMMINGS_PRICES_TABLE,
}
