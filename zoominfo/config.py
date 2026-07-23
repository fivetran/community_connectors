"""Configuration parsing, validation, and Search-filter construction.

Turns the raw string-valued configuration dict Fivetran passes in into typed
values (booleans, lists, output-field lists), validates required credentials
and bounded options, and builds the per-endpoint Search filter including the
incremental "since" predicate. Pure logic — no network calls.
"""

from fivetran_connector_sdk import Logging as log

from constants import (
    DEFAULT_COUNTRY,
    DEFAULT_CONTACT_ENRICH_FIELDS,
)
from transforms import _iso_to_yyyymmdd


# ─────────────────────────────────────────────
# CONFIGURATION HELPERS
# ─────────────────────────────────────────────
def _bool_config(configuration: dict, key: str) -> bool:
    """Parses a boolean config value that may be a string or bool."""
    val = configuration.get(key, False)
    if isinstance(val, str):
        return val.lower() in ("true", "1", "yes")
    return bool(val)


def _list_config(configuration: dict, key: str) -> list:
    """Parses a comma-separated config value into a list of stripped strings."""
    raw = configuration.get(key, "").strip()
    return [v.strip() for v in raw.split(",") if v.strip()] if raw else []


def _fields_config(configuration: dict, key: str, defaults: list) -> list:
    """Parses output fields config, falling back to defaults if blank."""
    raw = configuration.get(key, "").strip()
    return [f.strip() for f in raw.split(",") if f.strip()] if raw else defaults


# ─────────────────────────────────────────────
# CONFIGURATION VALIDATION
# ─────────────────────────────────────────────
def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required
    parameters. This function is called at the start of the update method to
    ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for
            the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or any
            optional parameter holds an invalid value.
    """
    # Required credentials for the OAuth Client Credentials Flow.
    required_configs = ["client_id", "client_secret"]
    for key in required_configs:
        if not configuration.get(key):
            raise ValueError(f"Missing required configuration value: {key}")

    # enrich_filter, when contact enrichment is enabled, must be one of the
    # supported eligibility filters — an invalid value would silently spend
    # credits on the wrong contacts.
    if _bool_config(configuration, "enrich_contacts"):
        enrich_filter = configuration.get("enrich_filter", "has_email_or_phone").strip()
        valid_filters = {"has_email", "has_phone", "has_email_or_phone", "all"}
        if enrich_filter and enrich_filter not in valid_filters:
            raise ValueError(
                f"Invalid enrich_filter '{enrich_filter}'. "
                f"Valid values: {', '.join(sorted(valid_filters))}"
            )

    # intent_topics is capped at 50 by the ZoomInfo API.
    topics = _list_config(configuration, "intent_topics")
    if len(topics) > 50:
        raise ValueError(
            f"Too many intent_topics ({len(topics)}). The ZoomInfo Intent API "
            f"accepts at most 50 topics per request."
        )


# ─────────────────────────────────────────────
# SEARCH FILTER + INCREMENTAL CURSOR
# ─────────────────────────────────────────────
def build_search_filter(configuration: dict) -> dict:
    """
    Builds the JSON:API `attributes` filter used by every Search endpoint.

    Reads `countries` from configuration (comma-separated). Defaults to
    DEFAULT_COUNTRY when blank. Currently supports a single country only —
    if multiple are provided, the first is used and a warning is logged.
    """
    raw = configuration.get("countries", "").strip()
    countries = [c.strip() for c in raw.split(",") if c.strip()] if raw else [DEFAULT_COUNTRY]

    if len(countries) > 1:
        log.warning(
            f"Multiple countries configured ({countries}) but the ZoomInfo Search API "
            f"only accepts a single country per request. Using first value: '{countries[0]}'. "
            f"To sync additional countries, run a separate connector instance per country."
        )

    return {"country": countries[0]}


def apply_incremental_filter(
    base_filter: dict,
    configuration: dict,
    state: dict,
    state_key: str,
    api_field: str,
) -> dict:
    """
    Returns a new filter dict that includes an incremental "since" predicate
    when prior state exists and `full_refresh` is not enabled.

    ZoomInfo Search filter API uses flat per-entity field names with a
    YYYY-MM-DD date format (no time, no operators). Field names confirmed
    via /gtm/data/v1/lookup/search?filter[entity]=<entity>&filter[fieldType]=input:

      contacts: lastUpdatedDateAfter
      companies: (no incremental filter available — always full-replace)
      scoops:    publishedStartDate
      intent:    signalStartDate
      news:      pageDateMin

    Caller passes the correct `api_field` for the endpoint it's syncing.
    If `api_field` is None or empty, no incremental predicate is added
    (used for companies, which has no server-side incremental option).
    """
    if _bool_config(configuration, "full_refresh"):
        log.info(f"full_refresh=true — running full sync for {state_key}")
        return dict(base_filter)

    since = state.get(state_key)
    if not since:
        log.info(f"No prior state for {state_key} — running full sync (first run)")
        return dict(base_filter)

    if not api_field:
        log.info(
            f"No server-side incremental filter available for {state_key} — running full sync. "
            f"State is still tracked for future use."
        )
        return dict(base_filter)

    since_date = _iso_to_yyyymmdd(since)
    log.info(f"Incremental sync for {state_key}: {api_field} >= {since_date}")
    return {**base_filter, api_field: since_date}


# ─────────────────────────────────────────────
# ENRICHMENT CONFIG + ELIGIBILITY
# ─────────────────────────────────────────────
def get_enrich_config(configuration: dict) -> dict:
    """
    Parses and validates the contact-enrichment configuration. Returns a dict
    of enrichment options, or None if enrichment is disabled.
    """
    enrich_enabled = _bool_config(configuration, "enrich_contacts")
    if not enrich_enabled:
        return None

    enrich_filter = configuration.get("enrich_filter", "has_email_or_phone").strip()
    valid_filters = {"has_email", "has_phone", "has_email_or_phone", "all"}
    if enrich_filter not in valid_filters:
        log.warning(
            f"Invalid enrich_filter '{enrich_filter}' — defaulting to 'has_email_or_phone'. "
            f"Valid values: {', '.join(sorted(valid_filters))}"
        )
        enrich_filter = "has_email_or_phone"

    mgmt_levels = _list_config(configuration, "enrich_management_levels")
    output_fields = _fields_config(
        configuration, "enrich_output_fields", DEFAULT_CONTACT_ENRICH_FIELDS
    )

    log.info(
        f"Contact enrichment enabled — filter={enrich_filter}, "
        f"managementLevels={mgmt_levels or 'all'}, "
        f"outputFields={len(output_fields)} fields"
    )

    return {
        "filter": enrich_filter,
        "mgmt_levels": mgmt_levels,
        "output_fields": output_fields,
    }


def should_enrich(record_attributes: dict, enrich_filter: str) -> bool:
    """
    Determines whether a contact from Search results should be enriched,
    based on the customer's chosen filter and ZoomInfo's availability hints.

    Using hints (hasEmail, hasDirectPhone, hasMobilePhone) avoids spending
    credits on contacts where ZoomInfo doesn't have the requested data.
    """
    if enrich_filter == "all":
        return True

    has_email = record_attributes.get("hasEmail", False)
    has_phone = record_attributes.get("hasDirectPhone", False) or record_attributes.get(
        "hasMobilePhone", False
    )

    if enrich_filter == "has_email":
        return bool(has_email)
    if enrich_filter == "has_phone":
        return bool(has_phone)
    if enrich_filter == "has_email_or_phone":
        return bool(has_email or has_phone)

    return False


def matches_mgmt_level(record_attributes: dict, mgmt_levels: list) -> bool:
    """
    Returns True if the contact's management level is in the customer's
    configured list, or if no filter is set (enrich all levels).

    managementLevel from the Enrich API returns a LIST (e.g. ["C-Level"]),
    not a string. We check if any configured level matches any value in the list.
    """
    if not mgmt_levels:
        return True
    raw = record_attributes.get("managementLevel", [])
    if isinstance(raw, str):
        raw = [raw]
    contact_levels = [v.lower() for v in raw]
    # Exact (case-insensitive) match — NOT substring. Substring containment
    # would treat a configured "Manager" as matching the API value
    # "Non Manager", enriching extra contacts and spending credits.
    return any(
        configured.lower() == contact_level
        for configured in mgmt_levels
        for contact_level in contact_levels
    )
