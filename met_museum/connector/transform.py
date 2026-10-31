"""Record shaping for the Met Museum connector: maps Collection API responses to destination rows.
Nothing here makes a request or reads configuration or state, so every function can be tested directly.
"""

# For parsing the API's ISO 8601 metadataDate timestamps
from datetime import datetime, timezone

# The Collection API sends this format for metadataDate, for example 2026-10-29T08:26:26.043Z.
__TIMESTAMP_SUFFIX = "Z"
__UTC_OFFSET = "+00:00"


def text(value):
    """
    Return a string field, or None when it is missing, not a string or blank. The API sends an
    empty string for most unset fields, and None keeps those distinct from real values.
    Args:
        value: the raw value.
    Returns:
        The stripped string, or None.
    """
    if not isinstance(value, str):
        return None
    return value.strip() or None


def as_int(value):
    """
    Return a whole-number value as an int, so an INT column never receives a float, a string or a bool.
    Args:
        value: the raw value.
    Returns:
        The int, or None when the value is missing, a boolean or not a whole number.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def as_float(value):
    """
    Return a numeric value as a float, so a DOUBLE column never receives an int, a string or a bool.
    Args:
        value: the raw value.
    Returns:
        The float, or None when the value is missing, a boolean or not a number.
    """
    if isinstance(value, bool):
        return None
    return float(value) if isinstance(value, (int, float)) else None


def as_bool(value):
    """
    Return a boolean field only when the API sent a real boolean.
    Args:
        value: the raw value.
    Returns:
        True, False, or None when the value is missing or not a boolean.
    """
    return value if isinstance(value, bool) else None


def parse_timestamp(value):
    """
    Parse an API timestamp such as 2026-10-29T08:26:26.043Z to an aware UTC datetime.
    Args:
        value: the raw metadataDate value.
    Returns:
        The datetime, or None when the value is missing or not a valid timestamp.
    """
    raw = text(value)
    if raw is None:
        return None
    if raw.endswith(__TIMESTAMP_SUFFIX):
        raw = raw[: -len(__TIMESTAMP_SUFFIX)] + __UTC_OFFSET
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def dict_items(value) -> list:
    """
    Return the dictionaries of a list field, skipping entries of any other shape, so a malformed
    constituents or tags entry cannot raise.
    Args:
        value: the raw list field, which the API can send as null.
    Returns:
        A list of dictionaries, empty when the field is missing or not a list.
    """
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def build_department_row(department: dict) -> dict:
    """
    Map one entry of the departments response to a department row.
    Args:
        department: one element of the response's departments array.
    Returns:
        A dictionary whose keys match the department table columns.
    """
    return {
        "department_id": as_int(department.get("departmentId")),
        "display_name": text(department.get("displayName")),
    }


def build_object_row(obj: dict, department_id) -> dict:
    """
    Map one object detail response to an object row. API fields are renamed to snake_case, and
    the list fields go to their own tables, so every value here is a scalar.
    Args:
        obj: the object detail response.
        department_id: the id of the department whose object list returned this object.
    Returns:
        A dictionary whose keys match the object table columns.
    """
    return {
        "object_id": as_int(obj.get("objectID")),
        "department_id": as_int(department_id),
        "department": text(obj.get("department")),
        "is_public_domain": as_bool(obj.get("isPublicDomain")),
        "is_highlight": as_bool(obj.get("isHighlight")),
        "is_timeline_work": as_bool(obj.get("isTimelineWork")),
        "accession_number": text(obj.get("accessionNumber")),
        "accession_year": text(obj.get("accessionYear")),
        "primary_image": text(obj.get("primaryImage")),
        "primary_image_small": text(obj.get("primaryImageSmall")),
        "additional_image_count": len(obj.get("additionalImages") or []),
        "object_name": text(obj.get("objectName")),
        "title": text(obj.get("title")),
        "culture": text(obj.get("culture")),
        "period": text(obj.get("period")),
        "dynasty": text(obj.get("dynasty")),
        "reign": text(obj.get("reign")),
        "portfolio": text(obj.get("portfolio")),
        "artist_role": text(obj.get("artistRole")),
        "artist_prefix": text(obj.get("artistPrefix")),
        "artist_display_name": text(obj.get("artistDisplayName")),
        "artist_display_bio": text(obj.get("artistDisplayBio")),
        "artist_suffix": text(obj.get("artistSuffix")),
        "artist_alpha_sort": text(obj.get("artistAlphaSort")),
        "artist_nationality": text(obj.get("artistNationality")),
        "artist_begin_date": text(obj.get("artistBeginDate")),
        "artist_end_date": text(obj.get("artistEndDate")),
        "artist_gender": text(obj.get("artistGender")),
        "artist_wikidata_url": text(obj.get("artistWikidata_URL")),
        "artist_ulan_url": text(obj.get("artistULAN_URL")),
        "object_date": text(obj.get("objectDate")),
        "object_begin_date": as_int(obj.get("objectBeginDate")),
        "object_end_date": as_int(obj.get("objectEndDate")),
        "medium": text(obj.get("medium")),
        "dimensions": text(obj.get("dimensions")),
        "credit_line": text(obj.get("creditLine")),
        "geography_type": text(obj.get("geographyType")),
        "city": text(obj.get("city")),
        "state_province": text(obj.get("state")),
        "county": text(obj.get("county")),
        "country": text(obj.get("country")),
        "region": text(obj.get("region")),
        "subregion": text(obj.get("subregion")),
        "locale": text(obj.get("locale")),
        "locus": text(obj.get("locus")),
        "excavation": text(obj.get("excavation")),
        "river": text(obj.get("river")),
        "classification": text(obj.get("classification")),
        "rights_and_reproduction": text(obj.get("rightsAndReproduction")),
        "link_resource": text(obj.get("linkResource")),
        "repository": text(obj.get("repository")),
        "object_url": text(obj.get("objectURL")),
        "object_wikidata_url": text(obj.get("objectWikidata_URL")),
        "gallery_number": text(obj.get("GalleryNumber")),
        "metadata_date": parse_timestamp(obj.get("metadataDate")),
    }


def build_constituent_rows(obj: dict) -> list:
    """
    Map an object's constituents (artists, makers, publishers) to object_constituent rows. Each row
    carries the object's metadata_date, so a row from an earlier version of the object can be told
    apart from a current one. An entry without a constituent id is skipped.
    Args:
        obj: the object detail response.
    Returns:
        A list of dictionaries whose keys match the object_constituent table columns.
    """
    object_id = as_int(obj.get("objectID"))
    stamp = parse_timestamp(obj.get("metadataDate"))
    rows = {}
    for item in dict_items(obj.get("constituents")):
        constituent_id = as_int(item.get("constituentID"))
        if constituent_id is None:
            continue
        # An empty role is kept as an empty string, because role is part of the primary key.
        role = text(item.get("role")) or ""
        rows[(constituent_id, role)] = {
            "object_id": object_id,
            "constituent_id": constituent_id,
            "role": role,
            "name": text(item.get("name")),
            "gender": text(item.get("gender")),
            "constituent_ulan_url": text(item.get("constituentULAN_URL")),
            "constituent_wikidata_url": text(item.get("constituentWikidata_URL")),
            "object_metadata_date": stamp,
        }
    return list(rows.values())


def build_tag_rows(obj: dict) -> list:
    """
    Map an object's curator subject tags to object_tag rows, stamped with the object's
    metadata_date like the constituent rows. An entry without a term is skipped.
    Args:
        obj: the object detail response.
    Returns:
        A list of dictionaries whose keys match the object_tag table columns.
    """
    object_id = as_int(obj.get("objectID"))
    stamp = parse_timestamp(obj.get("metadataDate"))
    rows = {}
    for item in dict_items(obj.get("tags")):
        term = text(item.get("term"))
        if term is None:
            continue
        rows[term] = {
            "object_id": object_id,
            "term": term,
            "aat_url": text(item.get("AAT_URL")),
            "wikidata_url": text(item.get("Wikidata_URL")),
            "object_metadata_date": stamp,
        }
    return list(rows.values())


def tag_terms(obj: dict) -> list:
    """
    Return an object's curator tag terms, in API order, without duplicates.
    Args:
        obj: the object detail response.
    Returns:
        A list of non-empty term strings.
    """
    terms = (text(item.get("term")) for item in dict_items(obj.get("tags")))
    return list(dict.fromkeys(term for term in terms if term))
