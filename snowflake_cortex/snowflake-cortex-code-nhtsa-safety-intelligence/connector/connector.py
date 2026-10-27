"""
Snowflake Cortex Code NHTSA Safety Intelligence - Agent-Driven Discovery Connector

Syncs vehicle recall campaigns, consumer safety complaints, and vehicle specifications
from NHTSA (National Highway Traffic Safety Administration) free public APIs, then uses
Snowflake Cortex Agent to analyze the data and autonomously discover related vehicles
to investigate. This is the Agent-Driven Discovery pattern: the AI navigates the data,
deciding which vehicles to fetch next based on its analysis of safety patterns.

Built entirely with Snowflake Cortex Code and the Fivetran Connector Builder Skill.

The connector runs in one of two modes:
- Seed mode (the default): one seed vehicle, optionally followed by Cortex Agent discovery.
- Fixed-list mode (when `models` is set): every make x model x model year in a configured list, with no AI
  calls. It keeps the campaigns whose report received date falls in a configured window, writes the configured
  names to `make` and `model` and NHTSA's spelling to `nhtsa_make` and `nhtsa_model`, and records every
  configured vehicle in `configured_vehicle`, so a vehicle with no recalls in the window is recorded as checked
  rather than simply absent. It checkpoints after each model, deletes rows a later sync no longer returns, and
  truncates both tables when a sync starts with no fixed-list state, so the recalls and configured_vehicle
  tables end equal to the filter.

APIs Used:
1. NHTSA Recalls API - Vehicle recall campaigns
2. NHTSA Complaints API - Consumer safety complaints
3. NHTSA vPIC API - Vehicle specifications (models for a make)
4. Snowflake Cortex Agent - Discovery analysis and cross-vehicle synthesis

See the Technical Reference documentation (https://fivetran.com/docs/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connector-sdk#connectordevelopmentbestpractices) for details
"""

# For reading configuration from a JSON file
import json

# For time-based operations and rate limiting
import time

# For making HTTP requests to external APIs
import requests

# For URL-encoding user-supplied path segments (vPIC GetModelsForMake/{make})
import urllib.parse

# For the fixed-list sync timestamp
from datetime import datetime, timezone

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Fixed-list mode configuration parsing, kept in its own module so the connector stays readable
from models_config import (
    KEY_SEPARATOR,
    _optional_pause_seconds,
    _parse_iso_date,
    model_label,
    parse_models,
)

# Fixed-list mode fetch, row shaping and state, kept in their own module for the same reason
from fixed_list import (
    build_state,
    collect_model_rows,
    create_list_session,
    is_protected,
    model_fingerprint,
    model_query,
    parse_report_date,
    query_covers,
)

# API Configuration Constants
__BASE_URL_NHTSA = "https://api.nhtsa.gov"
__BASE_URL_VPIC = "https://vpic.nhtsa.dot.gov/api/vehicles"
__API_TIMEOUT_SECONDS = 30

# Retry and Rate Limiting Constants
__MAX_RETRIES = 3
__BASE_DELAY_SECONDS = 1
__RETRYABLE_STATUS_CODES = [429, 500, 502, 503, 504]
__NHTSA_RATE_LIMIT_DELAY = 0.5

# Discovery Configuration Defaults
__DEFAULT_DISCOVERY_DEPTH = 2
__DEFAULT_MAX_DISCOVERIES = 3
__DEFAULT_MAX_ENRICHMENTS = 10
__DEFAULT_CORTEX_TIMEOUT = 60
__DEFAULT_CORTEX_MODEL = "claude-sonnet-4-6"

# Cortex Agent Configuration
__CORTEX_AGENT_ENDPOINT = "/api/v2/cortex/inference:complete"
__SNOWFLAKE_HOST_SUFFIX = ".snowflakecomputing.com"

# Destination tables written in fixed-list mode
__RECALLS_TABLE = "recalls"
__VEHICLE_TABLE = "configured_vehicle"
# The state keys fixed-list mode writes (fixed_list.build_state); seed mode drops them
__FIXED_LIST_STATE_KEYS = (
    "next_model",
    "pass_done",
    "synced_keys",
    "synced_years",
    "synced_queries",
    "last_completed_sync",
)


def _is_placeholder(value) -> bool:
    """Return True when value is an angle-bracket placeholder (not a real credential)."""
    return isinstance(value, str) and value.startswith("<") and value.endswith(">")


def _parse_bool(value, default: bool = False) -> bool:
    """Parse a config value as boolean, handling both string and native bool inputs."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return default


def _optional_str(value, default: str) -> str:
    """Return default when value is None, empty, or a placeholder."""
    if value is None or _is_placeholder(value):
        return default
    return str(value) if value else default


def _optional_int(value, default: int) -> int:
    """Return default when value is None, empty, or a placeholder; otherwise cast to int."""
    if value is None or _is_placeholder(value):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def flatten_dict(d, parent_key="", sep="_"):
    """
    Flatten nested dictionaries and serialize lists to JSON strings.
    REQUIRED for Fivetran compatibility.

    Args:
        d: Dictionary to flatten
        parent_key: Prefix for nested keys (used in recursion)
        sep: Separator between nested key levels

    Returns:
        Flattened dictionary with all nested structures resolved
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, (list, tuple)):
            items.append((new_key, json.dumps(v) if v else None))
        else:
            items.append((new_key, v))
    return dict(items)


def is_fixed_list_mode(configuration: dict) -> bool:
    """
    Fixed-list mode is on when `models` holds a value; unset, empty or a placeholder means seed mode.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Returns:
        True when the connector syncs the configured model list instead of a seed vehicle
    """
    models = configuration.get("models")
    if models is None or _is_placeholder(models):
        return False
    return not (isinstance(models, str) and not models.strip())


def validate_fixed_list_configuration(configuration: dict):
    """
    Validate the fixed-list mode parameters: the model list, the report-date window and the pause.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: if any fixed-list parameter is missing or invalid.
    """
    parse_models(configuration.get("models"))
    date_from = _parse_iso_date(configuration.get("report_date_from"), "report_date_from")
    date_to = _parse_iso_date(configuration.get("report_date_to"), "report_date_to")
    if date_from > date_to:
        raise ValueError("report_date_from must not be after report_date_to")
    _optional_pause_seconds(configuration.get("request_pause_seconds"))


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure all required parameters are present and valid.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: if any required configuration parameter is missing or invalid.
    """
    if is_fixed_list_mode(configuration):
        validate_fixed_list_configuration(configuration)
        return

    required_configs = ["seed_make", "seed_model", "seed_year"]
    for key in required_configs:
        if not configuration.get(key) or _is_placeholder(configuration[key]):
            raise ValueError(f"Missing required configuration value: {key}")

    # Validate numeric configuration values; an unset or placeholder value falls back to its default
    for key in ["discovery_depth", "max_discoveries", "max_enrichments"]:
        if configuration.get(key) not in (None, "") and not _is_placeholder(configuration[key]):
            try:
                value = int(configuration[key])
                if value < 0:
                    raise ValueError(f"{key} must be non-negative")
            except (TypeError, ValueError) as e:
                if "non-negative" in str(e):
                    raise
                raise ValueError(f"Invalid {key} value: must be a number")

    # Validate Cortex configuration if enabled
    is_cortex_enabled = _parse_bool(configuration.get("enable_cortex"), default=False)
    if is_cortex_enabled:
        snowflake_account = configuration.get("snowflake_account", "")
        snowflake_pat_token = configuration.get("snowflake_pat_token", "")

        if not snowflake_account or _is_placeholder(snowflake_account):
            raise ValueError("snowflake_account is required when enable_cortex is true")
        if not snowflake_pat_token or _is_placeholder(snowflake_pat_token):
            raise ValueError("snowflake_pat_token is required when enable_cortex is true")
        if snowflake_account.startswith(("http://", "https://")):
            raise ValueError(
                "snowflake_account must be a hostname (no scheme), " f"got: {snowflake_account}"
            )
        # Require the leading dot and a label before it, so a lookalike such as "evilsnowflakecomputing.com" fails
        if snowflake_account == __SNOWFLAKE_HOST_SUFFIX[1:] or not snowflake_account.endswith(
            __SNOWFLAKE_HOST_SUFFIX
        ):
            raise ValueError(
                "snowflake_account must be a hostname ending in '.snowflakecomputing.com'"
            )


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    if is_fixed_list_mode(configuration):
        return [
            {
                "table": __RECALLS_TABLE,
                "primary_key": ["nhtsa_campaign_number", "make", "model", "model_year"],
                "columns": {
                    "nhtsa_campaign_number": "STRING",
                    "make": "STRING",
                    "model": "STRING",
                    "model_year": "INT",
                    "nhtsa_make": "STRING",
                    "nhtsa_model": "STRING",
                    "manufacturer": "STRING",
                    "component": "STRING",
                    "summary": "STRING",
                    "consequence": "STRING",
                    "remedy": "STRING",
                    "notes": "STRING",
                    "report_received_date": "NAIVE_DATE",
                    "park_it": "BOOLEAN",
                    "park_outside": "BOOLEAN",
                    "over_the_air_update": "BOOLEAN",
                },
            },
            {
                "table": __VEHICLE_TABLE,
                "primary_key": ["make", "model", "model_year"],
                "columns": {
                    "make": "STRING",
                    "model": "STRING",
                    "model_year": "INT",
                    "nhtsa_model": "STRING",
                    "campaigns_returned": "INT",
                    "last_synced_at": "UTC_DATETIME",
                },
            },
        ]

    return [
        {
            "table": "recalls",
            "primary_key": ["nhtsa_campaign_number", "make", "model", "model_year"],
            "columns": {"report_received_date": "NAIVE_DATE"},
        },
        {"table": "complaints", "primary_key": ["odi_number"]},
        {"table": "vehicle_specs", "primary_key": ["make_id", "model_id"]},
        {"table": "discovery_insights", "primary_key": ["insight_id"]},
        {"table": "safety_analysis", "primary_key": ["analysis_id"]},
    ]


def create_session():
    """
    Create a requests session for NHTSA API calls.

    Returns:
        requests.Session configured with appropriate headers
    """
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Fivetran-NHTSA-Safety-Intelligence/1.0",
            "Accept": "application/json",
        }
    )
    return session


def fetch_data_with_retry(session, url, params=None, headers=None):
    """
    Fetch data from API with exponential backoff retry logic.

    Args:
        session: requests.Session object for connection pooling
        url: Full URL to fetch
        params: Optional query parameters
        headers: Optional request headers

    Returns:
        JSON response as dictionary

    Raises:
        RuntimeError: If all retry attempts fail
    """
    for attempt in range(__MAX_RETRIES):
        try:
            response = session.get(
                url, params=params, headers=headers, timeout=__API_TIMEOUT_SECONDS
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.ConnectionError as e:
            log.warning(f"Connection error for {url}: {str(e)}")
            if attempt < __MAX_RETRIES - 1:
                delay_seconds = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(
                    f"Retrying in {delay_seconds}s (attempt {attempt + 1}/{__MAX_RETRIES})"
                )
                time.sleep(delay_seconds)
            else:
                log.error(f"Connection failed after {__MAX_RETRIES} attempts: {url}")
                raise RuntimeError(f"Connection failed after {__MAX_RETRIES} attempts: {e}") from e

        except requests.exceptions.Timeout as e:
            log.warning(f"Timeout for {url}: {str(e)}")
            if attempt < __MAX_RETRIES - 1:
                delay_seconds = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(
                    f"Retrying in {delay_seconds}s (attempt {attempt + 1}/{__MAX_RETRIES})"
                )
                time.sleep(delay_seconds)
            else:
                log.error(f"Timeout after {__MAX_RETRIES} attempts: {url}")
                raise RuntimeError(f"Timeout after {__MAX_RETRIES} attempts: {e}") from e

        except requests.exceptions.RequestException as e:
            status_code = (
                e.response.status_code
                if hasattr(e, "response") and e.response is not None
                else None
            )

            # Handle authentication/authorization errors immediately (no retry)
            if status_code in (401, 403):
                msg = (
                    f"HTTP {status_code}: Check your API credentials "
                    f"and permissions. URL: {url}"
                )
                log.error(msg)
                raise RuntimeError(msg) from e

            should_retry = status_code in __RETRYABLE_STATUS_CODES

            if should_retry and attempt < __MAX_RETRIES - 1:
                delay_seconds = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(
                    f"Request failed with status {status_code}, "
                    f"retrying in {delay_seconds}s "
                    f"(attempt {attempt + 1}/{__MAX_RETRIES})"
                )
                time.sleep(delay_seconds)
            else:
                attempts = attempt + 1
                log.error(
                    f"Request failed after {attempts} attempt(s). "
                    f"URL: {url}, Status: {status_code or 'N/A'}, "
                    f"Error: {str(e)}"
                )
                raise RuntimeError(f"API request failed after {attempts} attempt(s): {e}") from e


def fetch_recalls(session, make, model, year):
    """
    Fetch recall campaigns for a specific vehicle from NHTSA Recalls API.

    Args:
        session: requests.Session object
        make: Vehicle manufacturer (e.g., "Tesla")
        model: Vehicle model (e.g., "Model 3")
        year: Model year (e.g., "2023")

    Returns:
        List of recall record dictionaries
    """
    url = f"{__BASE_URL_NHTSA}/recalls/recallsByVehicle"
    params = {"make": make, "model": model, "modelYear": year}

    data = fetch_data_with_retry(session, url, params=params)
    results = data.get("results", [])
    log.info(f"Fetched {len(results)} recalls for {make} {model} {year}")
    time.sleep(__NHTSA_RATE_LIMIT_DELAY)
    return results


def fetch_complaints(session, make, model, year):
    """
    Fetch consumer safety complaints for a specific vehicle from NHTSA Complaints API.

    Args:
        session: requests.Session object
        make: Vehicle manufacturer (e.g., "Tesla")
        model: Vehicle model (e.g., "Model 3")
        year: Model year (e.g., "2023")

    Returns:
        List of complaint record dictionaries
    """
    url = f"{__BASE_URL_NHTSA}/complaints/complaintsByVehicle"
    params = {"make": make, "model": model, "modelYear": year}

    data = fetch_data_with_retry(session, url, params=params)
    results = data.get("results", [])
    log.info(f"Fetched {len(results)} complaints for {make} {model} {year}")
    time.sleep(__NHTSA_RATE_LIMIT_DELAY)
    return results


def fetch_vehicle_specs(session, make):
    """
    Fetch vehicle model specifications from NHTSA vPIC API.

    Args:
        session: requests.Session object
        make: Vehicle manufacturer (e.g., "Tesla")

    Returns:
        List of vehicle spec record dictionaries
    """
    # URL-encode the make path segment. AI-recommended makes (or user-supplied
    # seed_make values) may contain spaces ("Land Rover"), slashes
    # ("Mercedes/Benz"), or other URL-reserved characters that would otherwise
    # produce malformed URLs or inject unintended path segments.
    encoded_make = urllib.parse.quote(make, safe="")
    url = f"{__BASE_URL_VPIC}/GetModelsForMake/{encoded_make}"
    params = {"format": "json"}

    data = fetch_data_with_retry(session, url, params=params)
    results = data.get("Results", [])
    log.info(f"Fetched {len(results)} model specs for make: {make}")
    time.sleep(__NHTSA_RATE_LIMIT_DELAY)
    return results


def upsert_recalls(recalls, make, model, year, source_label):
    """
    Flatten and upsert recall records to the recalls table.

    Args:
        recalls: List of raw recall records from NHTSA API
        make: Vehicle make for record tagging
        model: Vehicle model for record tagging
        year: Model year for record tagging
        source_label: Discovery source label (e.g., "seed" or "discovered")

    Returns:
        Number of records upserted
    """
    record_count = 0
    for record in recalls:
        campaign_number = record.get("NHTSACampaignNumber")
        if not campaign_number:
            log.warning("Skipping recall record without NHTSACampaignNumber")
            continue

        # The API sends ReportReceivedDate as DD/MM/YYYY; write it as an ISO date so the column is a NAIVE_DATE.
        # An unparseable value is written as NULL rather than dropping the recall.
        reported = parse_report_date(record.get("ReportReceivedDate"))

        # Normalize field names to snake_case for warehouse compatibility
        normalized = {
            "nhtsa_campaign_number": campaign_number,
            "manufacturer": record.get("Manufacturer"),
            "make": record.get("Make", make),
            "model": record.get("Model", model),
            "model_year": record.get("ModelYear", year),
            "component": record.get("Component"),
            "summary": record.get("Summary"),
            "consequence": record.get("Consequence"),
            "remedy": record.get("Remedy"),
            "notes": record.get("Notes"),
            "report_received_date": reported.isoformat() if reported else None,
            "park_it": record.get("parkIt"),
            "park_outside": record.get("parkOutSide"),
            "over_the_air_update": record.get("overTheAirUpdate"),
            "nhtsa_action_number": record.get("NHTSAActionNumber"),
            "source": source_label,
        }

        flattened = flatten_dict(normalized)

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="recalls", data=flattened)
        record_count += 1

    return record_count


def upsert_complaints(complaints, source_label):
    """
    Flatten and upsert complaint records to the complaints table.

    Args:
        complaints: List of raw complaint records from NHTSA API
        source_label: Discovery source label (e.g., "seed" or "discovered")

    Returns:
        Number of records upserted
    """
    record_count = 0
    for record in complaints:
        odi_number = record.get("odiNumber")
        if not odi_number:
            log.warning("Skipping complaint record without odiNumber")
            continue

        # Normalize field names to snake_case
        normalized = {
            "odi_number": odi_number,
            "manufacturer": record.get("manufacturer"),
            "crash": record.get("crash"),
            "fire": record.get("fire"),
            "number_of_injuries": record.get("numberOfInjuries"),
            "number_of_deaths": record.get("numberOfDeaths"),
            "date_of_incident": record.get("dateOfIncident"),
            "date_complaint_filed": record.get("dateComplaintFiled"),
            "vin": record.get("vin"),
            "components": record.get("components"),
            "summary": record.get("summary"),
            "products": record.get("products"),
            "source": source_label,
        }

        flattened = flatten_dict(normalized)

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="complaints", data=flattened)
        record_count += 1

    return record_count


def upsert_vehicle_specs(specs, source_label):
    """
    Flatten and upsert vehicle specification records.

    Args:
        specs: List of raw vehicle spec records from vPIC API
        source_label: Discovery source label (e.g., "seed" or "discovered")

    Returns:
        Number of records upserted
    """
    record_count = 0
    for record in specs:
        make_id = record.get("Make_ID")
        model_id = record.get("Model_ID")
        if not make_id or not model_id:
            log.warning("Skipping vehicle spec without Make_ID or Model_ID")
            continue

        normalized = {
            "make_id": make_id,
            "make_name": record.get("Make_Name"),
            "model_id": model_id,
            "model_name": record.get("Model_Name"),
            "source": source_label,
        }

        flattened = flatten_dict(normalized)

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="vehicle_specs", data=flattened)
        record_count += 1

    return record_count


def build_discovery_prompt(make, model, year, recalls, complaints):
    """
    Build the Cortex Agent prompt for discovery analysis.

    Args:
        make: Seed vehicle make
        model: Seed vehicle model
        year: Seed vehicle year
        recalls: List of recall records for context
        complaints: List of complaint records for context

    Returns:
        Prompt string for the Cortex Agent
    """
    # Summarize top components from recalls
    recall_components = {}
    for r in recalls:
        comp = r.get("Component") or r.get("component", "Unknown")
        recall_components[comp] = recall_components.get(comp, 0) + 1

    top_recall_components = sorted(recall_components.items(), key=lambda x: x[1], reverse=True)[:5]

    # Count crash/fire incidents from complaints
    crash_count = sum(1 for c in complaints if c.get("crash") or c.get("Crash"))
    fire_count = sum(1 for c in complaints if c.get("fire") or c.get("Fire"))

    # Summarize top complaint components
    complaint_components = {}
    for c in complaints:
        comp = c.get("components") or c.get("Components", "Unknown")
        complaint_components[comp] = complaint_components.get(comp, 0) + 1

    top_complaint_components = sorted(
        complaint_components.items(), key=lambda x: x[1], reverse=True
    )[:5]

    recall_summary = "\n".join(
        [f"  - {comp}: {count} recalls" for comp, count in top_recall_components]
    )
    complaint_summary = "\n".join(
        [f"  - {comp}: {count} complaints" for comp, count in top_complaint_components]
    )

    return f"""You are an automotive safety analyst. Analyze these NHTSA recalls and complaints
for {make} {model} {year}.

DATA SUMMARY:
- Total recalls: {len(recalls)}
- Total complaints: {len(complaints)}
- Crash incidents: {crash_count}
- Fire incidents: {fire_count}

TOP RECALL COMPONENTS:
{recall_summary}

TOP COMPLAINT COMPONENTS:
{complaint_summary}

YOUR TASKS:
1. Identify the top 3 failing components with severity ranking
2. Note any crash or fire incidents and their patterns
3. Recommend 2-3 related vehicles to investigate next. Consider:
   - Same manufacturer, different models (shared platform/components)
   - Competitor vehicles in same class (industry-wide issue detection)
   - Same model, adjacent years (ongoing vs one-time defect)

Return ONLY valid JSON (no markdown, no code fences):
{{
  "top_components": [
    {{"component": "...", "recall_count": 0, "complaint_count": 0, "severity": "HIGH/MEDIUM/LOW"}}
  ],
  "severity_score": 7,
  "crash_count": {crash_count},
  "fire_count": {fire_count},
  "recommended_vehicles": [
    {{"make": "...", "model": "...", "year": "...", "reason": "..."}}
  ],
  "analysis_summary": "..."
}}"""


def build_synthesis_prompt(vehicles_investigated, aggregates):
    """
    Build the Cortex Agent prompt for cross-vehicle safety synthesis.

    Args:
        vehicles_investigated: List of (make, model, year) tuples
        aggregates: Dict with recall/complaint component counts and totals

    Returns:
        Prompt string for the Cortex Agent
    """
    vehicle_list = "\n".join([f"  - {v[0]} {v[1]} {v[2]}" for v in vehicles_investigated])

    # Use pre-computed aggregates instead of iterating full lists
    recall_components = aggregates.get("recall_components", {})
    top_components = sorted(recall_components.items(), key=lambda x: x[1], reverse=True)[:10]

    component_summary = "\n".join(
        [f"  - {comp}: {count} recalls" for comp, count in top_components]
    )

    total_recalls = aggregates.get("total_recalls", 0)
    total_complaints = aggregates.get("total_complaints", 0)
    total_crashes = aggregates.get("crash_count", 0)
    total_fires = aggregates.get("fire_count", 0)

    return f"""You are an automotive safety analyst. You have investigated multiple vehicles:
{vehicle_list}

AGGREGATE DATA:
- Total recalls across all vehicles: {total_recalls}
- Total complaints across all vehicles: {total_complaints}
- Total crash incidents: {total_crashes}
- Total fire incidents: {total_fires}

TOP COMPONENTS ACROSS ALL VEHICLES:
{component_summary}

Analyze ALL recalls and complaints together:
1. Cross-vehicle component risk scores (which components fail across multiple vehicles?)
2. Manufacturer response quality (were OTA updates available? were recalls timely?)
3. Fleet-wide safety assessment
4. Are these isolated issues or systemic industry problems?

Return ONLY valid JSON (no markdown, no code fences):
{{
  "component_risk_rankings": [
    {{"component": "...", "affected_vehicles": 0, "total_recalls": 0, "risk_score": 8}}
  ],
  "manufacturer_response_score": 7,
  "systemic_issues": [
    {{"issue": "...", "affected_makes": ["..."], "severity": "HIGH/MEDIUM/LOW"}}
  ],
  "fleet_safety_grade": "B",
  "executive_summary": "..."
}}"""


def _create_cortex_session(configuration):
    """
    Create a requests.Session for Cortex API calls with connection pooling.

    Separate from the NHTSA session since it carries different auth headers.

    Args:
        configuration: Configuration dictionary with Snowflake credentials

    Returns:
        requests.Session configured for Cortex API
    """
    cortex_session = requests.Session()
    pat_token = configuration.get("snowflake_pat_token")
    cortex_session.headers.update(
        {
            "Authorization": f"Bearer {pat_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    )
    return cortex_session


def call_cortex_agent(configuration, prompt, cortex_session=None):
    """
    Call Snowflake Cortex Agent via REST API for safety analysis.

    Uses SSE streaming response parsing with retry logic for transient failures.
    Accepts an optional session for connection reuse across multiple calls.

    Args:
        configuration: Configuration dictionary with Snowflake credentials
        prompt: Analysis prompt for the agent
        cortex_session: Optional requests.Session for connection reuse

    Returns:
        Parsed JSON response as dictionary, or None if call fails
    """
    snowflake_account = configuration.get("snowflake_account")
    cortex_model = _optional_str(configuration.get("cortex_model"), __DEFAULT_CORTEX_MODEL)
    timeout = _optional_int(configuration.get("cortex_timeout"), __DEFAULT_CORTEX_TIMEOUT)

    url = f"https://{snowflake_account}{__CORTEX_AGENT_ENDPOINT}"

    payload = {
        "model": cortex_model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens": 2000,
    }

    for attempt in range(__MAX_RETRIES):
        agent_response = ""
        try:
            if cortex_session:
                response = cortex_session.post(url, json=payload, timeout=timeout, stream=True)
            else:
                pat_token = configuration.get("snowflake_pat_token")
                headers = {
                    "Authorization": f"Bearer {pat_token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                }
                response = requests.post(
                    url, headers=headers, json=payload, timeout=timeout, stream=True
                )
            response.raise_for_status()

            try:
                for line in response.iter_lines():
                    if line:
                        line_text = line.decode("utf-8")
                        if line_text.startswith("data: "):
                            try:
                                data = json.loads(line_text[6:])
                                if not isinstance(data, dict):
                                    continue

                                choices = data.get("choices", [])
                                for choice in choices:
                                    delta = choice.get("delta", {})
                                    content = delta.get("content", "")
                                    if content:
                                        agent_response += content

                            except json.JSONDecodeError:
                                continue

                        elif line_text.startswith("event: done"):
                            break
            finally:
                response.close()

            if not agent_response:
                log.warning("Empty response from Cortex Agent")
                return None

            # Parse the JSON response, handling potential markdown fences
            cleaned = agent_response.strip()
            if cleaned.startswith("```"):
                lines = cleaned.split("\n")
                lines = [ln for ln in lines if not ln.strip().startswith("```")]
                cleaned = "\n".join(lines).strip()

            return json.loads(cleaned)

        except requests.exceptions.Timeout:
            log.warning(f"Cortex Agent timeout after {timeout}s (attempt {attempt + 1})")
            if attempt < __MAX_RETRIES - 1:
                time.sleep(__BASE_DELAY_SECONDS * (2**attempt))
            else:
                return None
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else None
            if status_code in (401, 403):
                log.error(f"Cortex Agent auth error {status_code}: check PAT token")
                return None
            if status_code in __RETRYABLE_STATUS_CODES and attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(f"Cortex Agent HTTP {status_code}, retrying in {delay}s")
                time.sleep(delay)
            else:
                error_body = ""
                try:
                    error_body = e.response.text[:500]
                except (AttributeError, TypeError):
                    pass
                log.warning(f"Cortex Agent HTTP error: {e}")
                if error_body:
                    log.warning(f"Response body: {error_body}")
                return None
        except requests.exceptions.ConnectionError as e:
            log.warning(f"Cortex Agent connection error: {e}")
            if attempt < __MAX_RETRIES - 1:
                time.sleep(__BASE_DELAY_SECONDS * (2**attempt))
            else:
                return None
        except json.JSONDecodeError as e:
            log.warning(f"Failed to parse Cortex Agent response as JSON: {e}")
            if agent_response:
                log.warning(f"Raw response: {agent_response[:500]}")
            return None
        except requests.exceptions.RequestException as e:
            log.warning(f"Cortex Agent request error: {e}")
            return None

    return None


def _build_aggregates(
    recall_components,
    complaint_components,
    crash_count,
    fire_count,
    total_recalls,
    total_complaints,
):
    """Build aggregates dict for synthesis prompt."""
    return {
        "recall_components": recall_components,
        "complaint_components": complaint_components,
        "crash_count": crash_count,
        "fire_count": fire_count,
        "total_recalls": total_recalls,
        "total_complaints": total_complaints,
    }


def run_discovery_phase(
    session, configuration, seed_recalls, seed_complaints, state, cortex_session=None
):
    """
    Run the Agent-Driven Discovery phase: analyze seed data and fetch related vehicles.

    Args:
        session: requests.Session for NHTSA API calls
        configuration: Configuration dictionary
        seed_recalls: Recall records from seed vehicle
        seed_complaints: Complaint records from seed vehicle
        state: State dictionary for checkpointing
        cortex_session: Optional requests.Session for Cortex connection reuse

    Returns:
        Tuple of (all_recalls, all_complaints, vehicles_investigated)
    """
    seed_make = configuration.get("seed_make")
    seed_model = configuration.get("seed_model")
    seed_year = configuration.get("seed_year")
    max_discoveries = _optional_int(
        configuration.get("max_discoveries"), __DEFAULT_MAX_DISCOVERIES
    )
    max_enrichments = _optional_int(
        configuration.get("max_enrichments"), __DEFAULT_MAX_ENRICHMENTS
    )
    enrichment_count = 0

    # Track aggregates instead of full in-memory lists for synthesis prompt
    recall_components = {}
    for r in seed_recalls:
        comp = r.get("Component", r.get("component", "Unknown"))
        recall_components[comp] = recall_components.get(comp, 0) + 1
    complaint_components = {}
    crash_count = 0
    fire_count = 0
    for c in seed_complaints:
        comp = c.get("components") or c.get("Components", "Unknown")
        complaint_components[comp] = complaint_components.get(comp, 0) + 1
        if c.get("crash") or c.get("Crash"):
            crash_count += 1
        if c.get("fire") or c.get("Fire"):
            fire_count += 1
    total_recalls = len(seed_recalls)
    total_complaints = len(seed_complaints)
    vehicles_investigated = [(seed_make, seed_model, seed_year)]

    # Check enrichment budget before calling Cortex
    if enrichment_count >= max_enrichments:
        log.info(f"Enrichment budget exhausted ({max_enrichments}), skipping discovery")
        return (
            _build_aggregates(
                recall_components,
                complaint_components,
                crash_count,
                fire_count,
                total_recalls,
                total_complaints,
            ),
            vehicles_investigated,
            enrichment_count,
        )

    # Build discovery prompt and call Cortex Agent
    discovery_prompt = build_discovery_prompt(
        seed_make, seed_model, seed_year, seed_recalls, seed_complaints
    )
    log.info("Calling Cortex Agent for discovery analysis")
    discovery_result = call_cortex_agent(configuration, discovery_prompt, cortex_session)
    enrichment_count += 1

    if not discovery_result:
        log.warning("Cortex Agent returned no discovery results, skipping discovery phase")
        return (
            _build_aggregates(
                recall_components,
                complaint_components,
                crash_count,
                fire_count,
                total_recalls,
                total_complaints,
            ),
            vehicles_investigated,
            enrichment_count,
        )

    # Upsert discovery insight
    insight_record = {
        "insight_id": f"discovery_{seed_make}_{seed_model}_{seed_year}",
        "seed_make": seed_make,
        "seed_model": seed_model,
        "seed_year": seed_year,
        "top_components": discovery_result.get("top_components"),
        "severity_score": discovery_result.get("severity_score"),
        "crash_count": discovery_result.get("crash_count"),
        "fire_count": discovery_result.get("fire_count"),
        "recommended_vehicles": discovery_result.get("recommended_vehicles"),
        "analysis_summary": discovery_result.get("analysis_summary"),
    }
    flattened_insight = flatten_dict(insight_record)

    # The 'upsert' operation is used to insert or update data in the destination table.
    # The first argument is the name of the destination table.
    # The second argument is a dictionary containing the record to be upserted.
    op.upsert(table="discovery_insights", data=flattened_insight)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync
    # process can resume from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is
    # safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state=state)

    # Fetch data for agent-recommended vehicles (type-check LLM output)
    recommended = discovery_result.get("recommended_vehicles", [])
    if not isinstance(recommended, list):
        log.warning(
            f"Expected recommended_vehicles to be a list, got {type(recommended).__name__}. "
            f"Skipping discovery fetches."
        )
        return (
            _build_aggregates(
                recall_components,
                complaint_components,
                crash_count,
                fire_count,
                total_recalls,
                total_complaints,
            ),
            vehicles_investigated,
            enrichment_count,
        )
    vehicles_to_fetch = [v for v in recommended if isinstance(v, dict)][:max_discoveries]

    log.info(f"Agent recommended {len(recommended)} vehicles, fetching {len(vehicles_to_fetch)}")

    for vehicle in vehicles_to_fetch:
        v_make = vehicle.get("make", "")
        v_model = vehicle.get("model", "")
        v_year = vehicle.get("year", "")

        if not v_make or not v_model or not v_year:
            log.warning(f"Skipping incomplete vehicle recommendation: {vehicle}")
            continue

        log.info(f"Fetching discovered vehicle: {v_make} {v_model} {v_year}")

        try:
            discovered_recalls = fetch_recalls(session, v_make, v_model, v_year)
            discovered_complaints = fetch_complaints(session, v_make, v_model, v_year)

            recall_count = upsert_recalls(
                discovered_recalls, v_make, v_model, v_year, "discovered"
            )
            complaint_count = upsert_complaints(discovered_complaints, "discovered")

            log.info(
                f"Discovered vehicle {v_make} {v_model} {v_year}: "
                f"{recall_count} recalls, {complaint_count} complaints"
            )

            # Also fetch vehicle specs for the discovered make
            discovered_specs = fetch_vehicle_specs(session, v_make)
            upsert_vehicle_specs(discovered_specs, "discovered")

            # Track aggregates for synthesis prompt
            for r in discovered_recalls:
                comp = r.get("Component", r.get("component", "Unknown"))
                recall_components[comp] = recall_components.get(comp, 0) + 1
            for c in discovered_complaints:
                comp = c.get("components") or c.get("Components", "Unknown")
                complaint_components[comp] = complaint_components.get(comp, 0) + 1
                if c.get("crash") or c.get("Crash"):
                    crash_count += 1
                if c.get("fire") or c.get("Fire"):
                    fire_count += 1
            total_recalls += len(discovered_recalls)
            total_complaints += len(discovered_complaints)
            vehicles_investigated.append((v_make, v_model, v_year))
        except (RuntimeError, requests.exceptions.RequestException) as e:
            log.warning(
                f"Failed to fetch data for discovered vehicle "
                f"{v_make} {v_model} {v_year}: {e}. Skipping."
            )

        # Save the progress by checkpointing the state. This is important for ensuring that the
        # sync process can resume from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran
        # it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state=state)

    return (
        _build_aggregates(
            recall_components,
            complaint_components,
            crash_count,
            fire_count,
            total_recalls,
            total_complaints,
        ),
        vehicles_investigated,
        enrichment_count,
    )


def run_synthesis_phase(
    configuration, vehicles_investigated, aggregates, state, cortex_session=None
):
    """
    Run the cross-vehicle synthesis phase: generate fleet-wide safety analysis.

    Args:
        configuration: Configuration dictionary
        vehicles_investigated: List of (make, model, year) tuples
        aggregates: Dict with recall/complaint component counts and totals
        state: State dictionary for checkpointing
        cortex_session: Optional requests.Session for Cortex connection reuse
    """
    seed_make = configuration.get("seed_make")
    seed_model = configuration.get("seed_model")
    seed_year = configuration.get("seed_year")

    synthesis_prompt = build_synthesis_prompt(vehicles_investigated, aggregates)
    log.info("Calling Cortex Agent for cross-vehicle synthesis")
    synthesis_result = call_cortex_agent(configuration, synthesis_prompt, cortex_session)

    if not synthesis_result:
        log.warning("Cortex Agent returned no synthesis results")
        return

    analysis_record = {
        "analysis_id": f"synthesis_{seed_make}_{seed_model}_{seed_year}",
        "seed_make": seed_make,
        "seed_model": seed_model,
        "seed_year": seed_year,
        "vehicles_analyzed": len(vehicles_investigated),
        "total_recalls_analyzed": aggregates.get("total_recalls", 0),
        "total_complaints_analyzed": aggregates.get("total_complaints", 0),
        "component_risk_rankings": synthesis_result.get("component_risk_rankings"),
        "manufacturer_response_score": synthesis_result.get("manufacturer_response_score"),
        "systemic_issues": synthesis_result.get("systemic_issues"),
        "fleet_safety_grade": synthesis_result.get("fleet_safety_grade"),
        "executive_summary": synthesis_result.get("executive_summary"),
    }
    flattened_analysis = flatten_dict(analysis_record)

    # The 'upsert' operation is used to insert or update data in the destination table.
    # The first argument is the name of the destination table.
    # The second argument is a dictionary containing the record to be upserted.
    op.upsert(table="safety_analysis", data=flattened_analysis)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync
    # process can resume from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is
    # safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state=state)

    log.info(
        f"Synthesis complete: fleet grade {synthesis_result.get('fleet_safety_grade')}, "
        f"{len(vehicles_investigated)} vehicles analyzed"
    )


def _delete_recall_rows(label: str, row_keys) -> int:
    """
    Delete the recalls rows for one configured model whose keys a fixed-list sync no longer returns.

    Args:
        label: the model's "make|model" state key
        row_keys: "campaign|model_year" keys to delete

    Returns:
        the number of rows deleted
    """
    make, model = label.split(KEY_SEPARATOR, 1)
    deleted = 0
    for key in sorted(row_keys):
        campaign, model_year = key.rsplit(KEY_SEPARATOR, 1)
        # The 'delete' operation is used to mark a row as deleted in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary holding the primary key values of the row to delete.
        op.delete(
            table=__RECALLS_TABLE,
            keys={
                "nhtsa_campaign_number": campaign,
                "make": make,
                "model": model,
                "model_year": int(model_year),
            },
        )
        deleted += 1
    return deleted


def upsert_vehicle_rows(model: dict, row_keys: set, synced_at: str):
    """
    Write one configured_vehicle row per configured model year of one model.

    campaigns_returned counts every recalls row the destination holds for the vehicle after this model is synced:
    the rows upserted now and the tracked rows kept because a skipped result could stand for them or because NHTSA
    answered no recalls for their model year.

    Args:
        model: a parsed model entry
        row_keys: that model's "campaign|model_year" keys held in the destination
        synced_at: UTC timestamp of this sync
    """
    campaigns_by_year = {}
    for key in row_keys:
        model_year = int(key.rsplit(KEY_SEPARATOR, 1)[1])
        campaigns_by_year[model_year] = campaigns_by_year.get(model_year, 0) + 1
    for model_year in model["years"]:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(
            table=__VEHICLE_TABLE,
            data={
                "make": model["make"],
                "model": model["model"],
                "model_year": model_year,
                "nhtsa_model": model["nhtsa_model"],
                "campaigns_returned": campaigns_by_year.get(model_year, 0),
                "last_synced_at": synced_at,
            },
        )


def _delete_vehicle_rows(label: str, model_years) -> int:
    """
    Delete the configured_vehicle rows for model years of one model that are no longer configured.

    Args:
        label: the model's "make|model" state key
        model_years: the model years to delete

    Returns:
        the number of rows deleted
    """
    make, model = label.split(KEY_SEPARATOR, 1)
    for model_year in sorted(model_years):
        # The 'delete' operation is used to mark a row as deleted in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary holding the primary key values of the row to delete.
        op.delete(
            table=__VEHICLE_TABLE,
            keys={"make": make, "model": model, "model_year": int(model_year)},
        )
    return len(model_years)


def _models_to_sync(models: list, labels: list, fingerprints: list, state: dict) -> list:
    """
    Pick the indexes of the models a sync still has to do in the current pass.

    A model is done only when this pass synced it with the same fingerprint, so a model inserted into the list
    or changed between an interruption and the resume is synced, not skipped. A state written before pass_done
    existed falls back to resuming at next_model.

    Args:
        models: the parsed model list
        labels: "make|model" per model
        fingerprints: model_fingerprint per model
        state: the state from the previous sync

    Returns:
        the model indexes to sync, in configured order
    """
    pass_done = state.get("pass_done")
    if isinstance(pass_done, dict):
        return [i for i in range(len(models)) if pass_done.get(labels[i]) != fingerprints[i]]
    resume_from = state.get("next_model")
    start_index = labels.index(resume_from) if resume_from in labels else 0
    return list(range(start_index, len(models)))


def sync_fixed_list(configuration: dict, state: dict):
    """
    Fixed-list mode: sync every configured make x model x model year, one checkpoint per model.

    The recalls API has no pagination and no incremental filter, so every sync re-reads the configured vehicles.
    An interrupted sync resumes with the models the pass has not finished, and rows a later sync no longer returns
    are deleted.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: the state from the previous sync; empty for the first sync or a full re-sync
    """
    models = parse_models(configuration.get("models"))
    date_from = _parse_iso_date(configuration.get("report_date_from"), "report_date_from")
    date_to = _parse_iso_date(configuration.get("report_date_to"), "report_date_to")
    pause_seconds = _optional_pause_seconds(configuration.get("request_pause_seconds"))

    # synced_keys is written by every fixed-list checkpoint, so a state without it is empty (the first sync or a
    # full re-sync) or was written by seed mode before the configuration switched to fixed-list mode.
    is_fixed_list_state = isinstance(state.get("synced_keys"), dict)
    if not is_fixed_list_state:
        state = {}
    synced_keys = dict(state.get("synced_keys") or {})
    synced_years = dict(state.get("synced_years") or {})
    synced_queries = dict(state.get("synced_queries") or {})
    last_completed_sync = state.get("last_completed_sync")
    labels = [model_label(m) for m in models]
    fingerprints = [model_fingerprint(m, date_from, date_to) for m in models]
    pass_done = dict(state.get("pass_done") or {})
    to_sync = _models_to_sync(models, labels, fingerprints, state)
    log.info(
        f"Fixed-list mode: {len(models)} models, report dates {date_from} to {date_to}, "
        f"{len(to_sync)} left in this pass"
    )

    if not is_fixed_list_state:
        # A re-sync keeps every destination row, and seed mode leaves its own recalls rows, so rows outside the
        # filter would never be deleted. Truncating marks every existing row deleted; the pass below upserts the
        # rows that still match, so the destination ends equal to the filter.
        log.info("No fixed-list state: truncating both tables before the full pass")
        op.truncate(table=__RECALLS_TABLE)
        op.truncate(table=__VEHICLE_TABLE)

    synced_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    totals = {"upserted": 0, "deleted": 0, "skipped": 0}
    session = create_list_session()
    try:
        for position, index in enumerate(to_sync):
            model = models[index]
            label = labels[index]
            rows, skipped, protected, no_recall_years = collect_model_rows(
                session, model, date_from, date_to, pause_seconds
            )
            query = model_query(model, date_from, date_to)
            # A tracked row the API no longer returns is deleted, unless a skipped result could stand for it.
            missing = set(synced_keys.get(label, [])) - set(rows)
            kept = {key for key in missing if is_protected(key, protected)}
            # NHTSA dropping every campaign of a model year it listed before is more likely a fault than a
            # withdrawal, so those campaigns are kept, not deleted. That holds only when this sync's query covers the
            # one that tracked them: a no-recalls answer to another nhtsa_model spelling, or over a window that no
            # longer contains theirs, says nothing about them, so they are deleted as usual. The query is recorded
            # per model year, as the rows and the skip protection are, so a year that cannot vouch for its rows
            # leaves the other years' keep in place.
            prior_queries = synced_queries.get(label)
            if not isinstance(prior_queries, dict):
                prior_queries = {}
            covered_years = {
                str(year)
                for year in model["years"]
                if query_covers(prior_queries.get(str(year)), query)
            }
            no_recall_labels = {str(year) for year in no_recall_years} & covered_years
            tracked = {
                key
                for key in missing - kept
                if key.rsplit(KEY_SEPARATOR, 1)[1] in no_recall_labels
            }
            if tracked:
                kept |= tracked
                years = sorted({int(key.rsplit(KEY_SEPARATOR, 1)[1]) for key in tracked})
                # One campaign can cover several model years, so the warning counts campaigns, not tracked rows.
                campaigns = len({key.rsplit(KEY_SEPARATOR, 1)[0] for key in tracked})
                year_text = ("model year " if len(years) == 1 else "model years ") + ", ".join(
                    str(year) for year in years
                )
                held = f"{campaigns} campaign" if campaigns == 1 else f"{campaigns} campaigns"
                them = "it" if campaigns == 1 else "them"
                message = (
                    f"{label}: NHTSA listed no recalls for {model['make']} {model['nhtsa_model']} in {year_text}, "
                    "but an earlier sync, with this nhtsa_model in the same or another letter case and over a "
                    f"report window this one contains, tracked {held} there. The connector kept {them} instead "
                    f"of deleting {them}. If NHTSA withdrew {them}, run a full re-sync of the connection to clear "
                    f"{them}."
                )
                log.warning(message)
                # Surface the kept campaigns as a sync warning in the Fivetran dashboard, not only in the logs.
                op.warning(message)
            elif len(no_recall_years) == len(model["years"]):
                # NHTSA gives the same no-recalls answer for a year with no recalls and for a vehicle it does not
                # know, so a misspelled make or nhtsa_model looks like a model with no recalls in any year. The
                # model is synced as having no campaigns, as the filter says, but it should not pass unnoticed.
                message = (
                    f"{label}: NHTSA listed no recalls for {model['make']} {model['nhtsa_model']} in any "
                    "configured model year. A misspelled make or nhtsa_model gives the same answer, so check "
                    "them if this model should have recalls. The model is recorded with no campaigns."
                )
                log.warning(message)
                # Surface the model as a sync warning in the Fivetran dashboard, not only in the logs.
                op.warning(message)
            for key in sorted(rows):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(table=__RECALLS_TABLE, data=rows[key])
            totals["deleted"] += _delete_recall_rows(label, missing - kept)
            upsert_vehicle_rows(model, set(rows) | kept, synced_at)
            totals["deleted"] += _delete_vehicle_rows(
                label, set(synced_years.get(label, [])) - set(model["years"])
            )
            totals["upserted"] += len(rows)
            totals["skipped"] += skipped
            if skipped:
                # Warn before this model's checkpoint, not after the pass: a sync interrupted after the checkpoint
                # resumes past this model, and a count held in memory until the end would never be reported.
                message = (
                    f"{label}: skipped {skipped} API results with no campaign number or an unparseable report "
                    "date; the tracked rows they could stand for were kept."
                )
                log.warning(message)
                # Surface the skip count as a sync warning in the Fivetran dashboard, not only in the logs.
                op.warning(message)
            synced_keys[label] = sorted(set(rows) | kept)
            synced_years[label] = list(model["years"])
            # Each year's recorded query must have tracked every row the destination holds for that year. A row kept
            # because a skipped result could stand for it was tracked under the earlier query, so after a query
            # change that does not cover the earlier one, no single query tracked that year's rows and none is
            # recorded for it.
            kept_years = {key.rsplit(KEY_SEPARATOR, 1)[1] for key in kept}
            synced_queries[label] = {
                str(year): (
                    query if str(year) in covered_years or str(year) not in kept_years else None
                )
                for year in model["years"]
            }
            pass_done[label] = fingerprints[index]
            next_model = labels[to_sync[position + 1]] if position + 1 < len(to_sync) else None
            log.debug(f"{label}: {len(rows)} rows across {len(model['years'])} model years")

            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(
                state=build_state(
                    next_model,
                    synced_keys,
                    synced_years,
                    last_completed_sync,
                    pass_done,
                    synced_queries,
                )
            )
    finally:
        session.close()

    # A full pass is complete: remove rows for models no longer in the configured list.
    for label in sorted(set(synced_keys) - set(labels)):
        totals["deleted"] += _delete_recall_rows(label, synced_keys.pop(label))
    for label in sorted(set(synced_years) - set(labels)):
        totals["deleted"] += _delete_vehicle_rows(label, synced_years.pop(label))
    for label in sorted(set(synced_queries) - set(labels)):
        synced_queries.pop(label)
    log.info(
        f"Fixed-list sync complete: {totals['upserted']} rows upserted, {totals['deleted']} deleted, "
        f"{totals['skipped']} API results skipped"
    )

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(
        state=build_state(None, synced_keys, synced_years, synced_at, {}, synced_queries)
    )


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
    log.warning(
        "Example: all_things_ai/tutorials : snowflake-cortex-code-nhtsa-safety-intelligence"
    )

    validate_configuration(configuration)

    if is_fixed_list_mode(configuration):
        sync_fixed_list(configuration, state)
        return

    # Seed mode checkpoints the state it was given, so fixed-list keys would survive a switch to seed mode. A later
    # switch back would then find fixed-list state, skip the truncate and keep the seed-mode recalls rows. Dropping
    # them makes the next fixed-list sync start with the truncate.
    state = {key: value for key, value in state.items() if key not in __FIXED_LIST_STATE_KEYS}

    seed_make = configuration.get("seed_make")
    seed_model = configuration.get("seed_model")
    seed_year = configuration.get("seed_year")
    is_cortex_enabled = _parse_bool(configuration.get("enable_cortex"), default=False)
    discovery_depth = _optional_int(
        configuration.get("discovery_depth"), __DEFAULT_DISCOVERY_DEPTH
    )

    session = create_session()

    try:
        # Phase 1: Fetch seed vehicle data
        log.info(f"Phase 1: Fetching seed vehicle data for {seed_make} {seed_model} {seed_year}")

        seed_recalls = fetch_recalls(session, seed_make, seed_model, seed_year)
        seed_complaints = fetch_complaints(session, seed_make, seed_model, seed_year)
        seed_specs = fetch_vehicle_specs(session, seed_make)

        recall_count = upsert_recalls(seed_recalls, seed_make, seed_model, seed_year, "seed")
        complaint_count = upsert_complaints(seed_complaints, "seed")
        spec_count = upsert_vehicle_specs(seed_specs, "seed")

        log.info(
            f"Phase 1 complete: {recall_count} recalls, "
            f"{complaint_count} complaints, {spec_count} vehicle specs"
        )

        # Save the progress by checkpointing the state. This is important for ensuring that the
        # sync process can resume from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran
        # it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state=state)

        # Phase 2 & 3: Agent-Driven Discovery (if Cortex is enabled)
        if is_cortex_enabled and discovery_depth >= 1:
            log.info("Phase 2: Starting Agent-Driven Discovery")

            # Create Cortex session for connection pooling across discovery + synthesis
            cortex_session = _create_cortex_session(configuration)

            max_enrichments = _optional_int(
                configuration.get("max_enrichments"), __DEFAULT_MAX_ENRICHMENTS
            )

            aggregates, vehicles_investigated, enrichment_count = run_discovery_phase(
                session,
                configuration,
                seed_recalls,
                seed_complaints,
                state,
                cortex_session,
            )

            # Phase 3: Cross-vehicle synthesis. Gated on three conditions:
            # - discovery_depth >= 2 (user opted into synthesis)
            # - len(vehicles_investigated) > 1 (something to synthesize across)
            # - enrichment_count < max_enrichments (budget remaining for one more Cortex call)
            budget_remaining = enrichment_count < max_enrichments
            enough_vehicles = len(vehicles_investigated) > 1
            if discovery_depth >= 2 and enough_vehicles and budget_remaining:
                log.info("Phase 3: Starting cross-vehicle synthesis")
                run_synthesis_phase(
                    configuration,
                    vehicles_investigated,
                    aggregates,
                    state,
                    cortex_session,
                )

            cortex_session.close()
        else:
            if not is_cortex_enabled:
                log.info(
                    "Cortex enrichment disabled, skipping discovery phases. "
                    "Set enable_cortex=true to enable Agent-Driven Discovery."
                )

        # Final state update
        state["last_sync_make"] = seed_make
        state["last_sync_model"] = seed_model
        state["last_sync_year"] = seed_year

        # Save the progress by checkpointing the state. This is important for ensuring that the
        # sync process can resume from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran
        # it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state=state)

        log.info("Sync complete for all phases")

    finally:
        session.close()


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
