"""This connector syncs issuer-level equity data from the Carta Issuer API into Fivetran.
It replicates option grants, restricted stock units and awards, certificates, vesting events,
stakeholders, share classes, 409A fair market values, vesting schedule templates, convertible
notes and stakeholder capitalization table holdings for one or more Carta issuers.
See the Technical Reference documentation (https://fivetran.com/docs/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connector-sdk/best-practices)
for details.
"""

# For reading the configuration from a JSON file during local debugging
import json

# For pacing retries between failed API requests
import time

# For type hints on the helper signatures
from typing import Any, Iterable, Optional

# For HTTP calls to the Carta Issuer API. requests is pre-installed in the Fivetran environment
import requests

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Carta publishes the Issuer API under an alpha version prefix. Override it in the configuration
# if Carta promotes the API to a new version.
__DEFAULT_API_VERSION = "v1alpha1"
__DEFAULT_TOKEN_URL = "https://login.app.carta.com/o/access_token/"
__DEFAULT_API_BASE_URL = "https://api.carta.com"

__USER_AGENT = "fivetran-carta-connector/1.0"
__HTTP_TIMEOUT_SECONDS = 40
__MAX_RETRIES = 3
__RETRY_BACKOFF_BASE_SECONDS = 15
__RETRY_MAX_WAIT_SECONDS = 60
__DEFAULT_RETRY_AFTER_SECONDS = 60

# Carta caps the page size at 50 regardless of what the request asks for.
__PAGE_SIZE = 50

# Emit a progress log every N pages so long resources show movement without logging per record.
__PROGRESS_EVERY_PAGES = 10

# Checkpoint once this many upserts have accumulated inside a single resource. The interval is
# measured in upserts rather than parent records because one option grant can expand into
# hundreds of vesting events, and a single very large commit can fail as one batch.
__CHECKPOINT_INTERVAL_UPSERTS = 10000

# Requested at token time. Carta grants scopes per OAuth app: if the app is not registered for a
# scope, the resources behind it return HTTP 403 and this connector skips them with a warning.
__DEFAULT_SCOPES = [
    "read_issuer_info",
    "read_issuer_securities",
    "read_issuer_securitiestemplates",
    "read_issuer_shareclasses",
    "read_issuer_valuations",
    "read_issuer_stakeholders",
    "read_issuer_stakeholdercapitalizationtable",
    "read_corporation_info",
]

__REQUIRED_CONFIGURATION_KEYS = ["client_id", "client_secret", "issuer_ids"]

# Resource path segment mapped to its destination table. These four resources accept the
# lastModifiedDatetimeAfter cursor and are the only ones that can sync incrementally.
__SECURITY_RESOURCES = {
    "optionGrants": "option_grants",
    "restrictedStockUnits": "restricted_stock_units",
    "restrictedStockAwards": "restricted_stock_awards",
    "certificates": "certificates",
}


class InsufficientScopeError(RuntimeError):
    """Raised when the OAuth app is not granted the scope a resource requires (HTTP 403)."""


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has
    all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or malformed.
    """
    for key in __REQUIRED_CONFIGURATION_KEYS:
        if not configuration.get(key):
            raise ValueError(f"Missing required configuration value: {key}")

    if not parse_issuer_ids(configuration):
        raise ValueError("Configuration value issuer_ids must list at least one issuer id")

    for key in ("token_url", "api_base_url"):
        value = configuration.get(key)
        if value and not str(value).startswith("https://"):
            raise ValueError(f"Configuration value {key} must be an https URL")


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
            "table": "option_grants",
            "primary_key": ["issuer_id", "id"],
            # Quantities and prices are declared as STRING on purpose. Carta returns
            # high-precision decimals such as 99.00000000000000000000, and a float type would
            # silently lose precision on equity data. Cast them downstream if needed.
            "columns": {
                "id": "STRING",
                "issuer_id": "STRING",
                "stakeholder_id": "STRING",
                "equity_incentive_plan_name": "STRING",
                "security_label": "STRING",
                "stock_option_type": "STRING",
                "iso_nso_split": "BOOLEAN",
                "early_exercisable": "BOOLEAN",
                "issue_date": "NAIVE_DATE",
                "grant_expiration_date": "NAIVE_DATE",
                "last_exercisable_date": "NAIVE_DATE",
                "quantity": "STRING",
                "outstanding_quantity": "STRING",
                "vested_quantity": "STRING",
                "exercised_quantity": "STRING",
                "canceled_quantity": "STRING",
                "forfeited_quantity": "STRING",
                "expired_quantity": "STRING",
                "returned_to_pool_quantity": "STRING",
                "returned_to_treasury_quantity": "STRING",
                "exercise_price_amount": "STRING",
                "exercise_price_currency": "STRING",
                "security_id": "STRING",
                "share_class_id": "STRING",
                "vesting_schedule_template_id": "STRING",
                "exercise_periods": "JSON",
                "last_modified_datetime": "UTC_DATETIME",
            },
        },
        {
            "table": "restricted_stock_units",
            "primary_key": ["issuer_id", "id"],
            "columns": {
                "id": "STRING",
                "issuer_id": "STRING",
                "stakeholder_id": "STRING",
                "equity_incentive_plan_name": "STRING",
                "security_label": "STRING",
                "issue_date": "NAIVE_DATE",
                "vesting_start_date": "NAIVE_DATE",
                "board_approval_date": "NAIVE_DATE",
                "stakeholder_acceptance_date": "NAIVE_DATE",
                "quantity": "STRING",
                "vested_quantity": "STRING",
                "released_quantity": "STRING",
                "net_settled_quantity": "STRING",
                "canceled_quantity": "STRING",
                "forfeited_quantity": "STRING",
                "expired_quantity": "STRING",
                "returned_to_pool_quantity": "STRING",
                "returned_to_treasury_quantity": "STRING",
                "security_id": "STRING",
                "share_class_id": "STRING",
                "vesting_schedule_template_id": "STRING",
                "vesting_schedule_name": "STRING",
                "vesting_schedule_start_date": "NAIVE_DATE",
                "vesting_schedule_end_date": "NAIVE_DATE",
                "settlements": "JSON",
                "last_modified_datetime": "UTC_DATETIME",
            },
        },
        {
            "table": "restricted_stock_awards",
            "primary_key": ["issuer_id", "id"],
            "columns": {
                "id": "STRING",
                "issuer_id": "STRING",
                "stakeholder_id": "STRING",
                "equity_incentive_plan_name": "STRING",
                "share_class_name": "STRING",
                "security_label": "STRING",
                "issue_date": "NAIVE_DATE",
                "vesting_start_date": "NAIVE_DATE",
                "quantity": "STRING",
                "vested_quantity": "STRING",
                "canceled_quantity": "STRING",
                "returned_to_pool_quantity": "STRING",
                "returned_to_treasury_quantity": "STRING",
                "price_per_share_amount": "STRING",
                "price_per_share_currency": "STRING",
                "security_id": "STRING",
                "share_class_id": "STRING",
                "vesting_schedule_template_id": "STRING",
                "vesting_schedule_name": "STRING",
                "vesting_schedule_start_date": "NAIVE_DATE",
                "vesting_schedule_end_date": "NAIVE_DATE",
                "last_modified_datetime": "UTC_DATETIME",
            },
        },
        {
            "table": "certificates",
            "primary_key": ["issuer_id", "id"],
            "columns": {
                "id": "STRING",
                "issuer_id": "STRING",
                "stakeholder_id": "STRING",
                "share_class_name": "STRING",
                "security_label": "STRING",
                "issue_date": "NAIVE_DATE",
                "quantity": "STRING",
                "canceled_quantity": "STRING",
                "returned_to_pool_quantity": "STRING",
                "returned_to_treasury_quantity": "STRING",
                "price_per_share_amount": "STRING",
                "price_per_share_currency": "STRING",
                "security_id": "STRING",
                "share_class_id": "STRING",
                "vesting_schedule_template_id": "STRING",
                "preceded_by": "STRING",
                "last_modified_datetime": "UTC_DATETIME",
            },
        },
        {
            "table": "vesting_events",
            "primary_key": ["issuer_id", "security_type", "security_id", "id"],
            "columns": {
                "id": "STRING",
                "issuer_id": "STRING",
                "security_type": "STRING",
                "security_id": "STRING",
                "stakeholder_id": "STRING",
                "vest_date": "NAIVE_DATE",
                "quantity": "STRING",
                "vested_quantity": "STRING",
                "max_quantity": "STRING",
                "target_quantity": "STRING",
                "vested": "BOOLEAN",
                "performance_condition": "BOOLEAN",
            },
        },
        {
            "table": "option_grant_exercises",
            "primary_key": ["issuer_id", "option_grant_id", "exercise_index"],
            "columns": {
                "issuer_id": "STRING",
                "option_grant_id": "STRING",
                "exercise_index": "INT",
                "exercise_id": "STRING",
                "quantity": "STRING",
                "exercise_date": "NAIVE_DATE",
                "status": "STRING",
                "exercise_type": "STRING",
                "certificate_id": "STRING",
                "qualified": "BOOLEAN",
            },
        },
        {
            "table": "stakeholders",
            "primary_key": ["issuer_id", "id"],
            "columns": {
                "id": "STRING",
                "issuer_id": "STRING",
                "full_name": "STRING",
                "email": "STRING",
                "employee_id": "STRING",
                "relationship": "STRING",
                # Carta calls this field "group". The destination column is renamed because
                # "group" is a reserved word in most warehouses.
                "stakeholder_group": "STRING",
                "entity_type": "STRING",
            },
        },
        {
            "table": "share_classes",
            "primary_key": ["issuer_id", "id"],
            "columns": {
                "id": "STRING",
                "issuer_id": "STRING",
                "name": "STRING",
                "prefix": "STRING",
                "type": "STRING",
                "par_value": "STRING",
                "seniority": "INT",
                "pari_passu": "JSON",
            },
        },
        {
            "table": "fair_market_values",
            "primary_key": ["issuer_id", "id"],
            "columns": {
                "id": "STRING",
                "issuer_id": "STRING",
                "effective_date": "NAIVE_DATE",
                "expiration_date": "NAIVE_DATE",
            },
        },
        {
            "table": "fair_market_value_share_class_valuations",
            "primary_key": ["issuer_id", "fair_market_value_id", "share_class_id"],
            "columns": {
                "issuer_id": "STRING",
                "fair_market_value_id": "STRING",
                "share_class_id": "STRING",
                "share_class_name": "STRING",
                "is_common": "BOOLEAN",
                "price_amount": "STRING",
                "price_currency": "STRING",
            },
        },
        {
            "table": "vesting_schedule_templates",
            "primary_key": ["issuer_id", "id"],
            "columns": {
                "id": "STRING",
                "issuer_id": "STRING",
                "uuid": "STRING",
                "name": "STRING",
                "description": "STRING",
                "vesting_schedule_type": "STRING",
                "periods": "JSON",
            },
        },
        {
            "table": "stakeholder_holdings",
            "primary_key": ["issuer_id", "stakeholder_id"],
            "columns": {
                "issuer_id": "STRING",
                "stakeholder_id": "STRING",
                "stakeholder_name": "STRING",
                "stakeholder_group_id": "STRING",
                "stakeholder_group_name": "STRING",
                "as_of_date": "NAIVE_DATE",
                "fully_diluted_shares": "STRING",
                "outstanding_shares": "STRING",
                "cash_raised_amount": "STRING",
                "cash_raised_currency": "STRING",
            },
        },
        {
            "table": "stakeholder_share_class_holdings",
            "primary_key": ["issuer_id", "stakeholder_id", "share_class_id"],
            "columns": {
                "issuer_id": "STRING",
                "stakeholder_id": "STRING",
                "share_class_id": "STRING",
                "share_class_name": "STRING",
                "fully_diluted_shares": "STRING",
                "outstanding_shares": "STRING",
                "cash_raised_amount": "STRING",
                "cash_raised_currency": "STRING",
            },
        },
        {
            "table": "convertible_notes",
            "primary_key": ["issuer_id", "id"],
            "columns": {
                "id": "STRING",
                "issuer_id": "STRING",
                "stakeholder_id": "STRING",
                "security_label": "STRING",
                "security_id": "STRING",
                "issue_at": "UTC_DATETIME",
                "conversion_at": "UTC_DATETIME",
                "canceled_at": "UTC_DATETIME",
                "maturity_at": "UTC_DATETIME",
                "cash_paid_amount": "STRING",
                "cash_paid_currency": "STRING",
                "interest_amount": "STRING",
                "interest_currency": "STRING",
                "interest_rate": "STRING",
                "interest_accrual_period": "STRING",
                "interest_compounding_period": "STRING",
                "day_count_basis": "STRING",
                "price_cap_amount": "STRING",
                "price_cap_currency": "STRING",
                "discount_percentage": "STRING",
                "change_in_control_percent": "STRING",
                "canceled_quantity": "STRING",
                "note_block_id": "STRING",
                "note_block_name": "STRING",
                "note_block_prefix": "STRING",
                "note_block_type": "STRING",
                "note_block_status": "STRING",
            },
        },
        {
            "table": "issuers",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "legal_name": "STRING",
                "doing_business_as_name": "STRING",
                "website": "STRING",
            },
        },
        {
            "table": "corporations",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "legal_name": "STRING",
                "doing_business_as_name": "STRING",
                "website": "STRING",
            },
        },
    ]


def parse_issuer_ids(configuration: dict) -> list:
    """
    Read the comma separated issuer_ids configuration value into a list of issuer ids.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        A list of issuer id strings with surrounding whitespace removed.
    """
    raw_issuer_ids = str(configuration.get("issuer_ids", ""))
    return [issuer_id.strip() for issuer_id in raw_issuer_ids.split(",") if issuer_id.strip()]


def get_api_version(configuration: dict) -> str:
    """
    Resolve the Issuer API version to call.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        The configured API version, or the default alpha version.
    """
    return configuration.get("api_version") or __DEFAULT_API_VERSION


def get_api_base_url(configuration: dict) -> str:
    """
    Resolve the Issuer API base URL to call.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        The configured base URL, or the Carta production base URL.
    """
    return (configuration.get("api_base_url") or __DEFAULT_API_BASE_URL).rstrip("/")


def backoff_seconds(attempt: int) -> int:
    """
    Calculate the exponential backoff delay for a retry attempt.
    Args:
        attempt: the 1-based attempt number that just failed.
    Returns:
        The number of seconds to wait, capped so a retry never stalls the sync.
    """
    return min(__RETRY_MAX_WAIT_SECONDS, __RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)))


def get_access_token(configuration: dict) -> tuple:
    """
    Request an OAuth2 client credentials access token from Carta.
    Carta requires the client id and secret as HTTP Basic credentials and requires an explicit
    scope parameter. A token minted without a scope is issued successfully but is rejected with
    HTTP 403 by every data endpoint, so an empty granted scope is treated as a failure.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        A tuple of the access token and the list of scopes Carta actually granted.
    Raises:
        RuntimeError: if the token request fails or the granted scope is empty.
    """
    requested_scopes = configuration.get("scopes")
    if isinstance(requested_scopes, str):
        requested_scopes = requested_scopes.split()
    scope_parameter = " ".join(requested_scopes or __DEFAULT_SCOPES)

    response = requests.post(
        configuration.get("token_url") or __DEFAULT_TOKEN_URL,
        auth=(configuration["client_id"], configuration["client_secret"]),
        data={"grant_type": "client_credentials", "scope": scope_parameter},
        headers={"User-Agent": __USER_AGENT},
        timeout=__HTTP_TIMEOUT_SECONDS,
    )
    if response.status_code != 200:
        log.error(f"Carta token request failed with HTTP {response.status_code}")
        raise RuntimeError(f"Carta token request failed: HTTP {response.status_code}")

    payload = response.json()
    granted_scopes = payload.get("scope", "")
    if not granted_scopes:
        log.error("Carta issued a token with an empty scope. Check the OAuth app registration")
        raise RuntimeError("Carta issued a token with an empty scope")

    log.info(f"Acquired a Carta access token with {len(granted_scopes.split())} scopes")
    return payload["access_token"], granted_scopes.split()


def create_session(configuration: dict) -> dict:
    """
    Build the authenticated request session shared by every fetch in this sync.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        A session dictionary holding the configuration, the access token and granted scopes.
    """
    access_token, granted_scopes = get_access_token(configuration)
    return {
        "configuration": configuration,
        "access_token": access_token,
        "granted_scopes": granted_scopes,
    }


def refresh_access_token(session: dict):
    """
    Replace the session access token with a freshly minted one.
    The Carta production token lifetime is one hour, which a large first sync can outlive.
    Args:
        session: the session dictionary created by create_session.
    """
    access_token, granted_scopes = get_access_token(session["configuration"])
    session["access_token"] = access_token
    session["granted_scopes"] = granted_scopes
    log.info("Refreshed the Carta access token mid-sync")


def request_json(url: str, session: dict, params: Optional[dict] = None) -> Any:
    """
    Perform a GET request against the Carta Issuer API and return the decoded JSON body.
    Transient failures (HTTP 429, HTTP 5xx, connection errors and timeouts) are retried with
    exponential backoff. An expired token (HTTP 401) triggers one token refresh and one retry.
    A missing scope (HTTP 403) is permanent for that resource and is raised as
    InsufficientScopeError so the caller can skip the resource instead of failing the sync.
    Args:
        url: the fully qualified request URL.
        session: the session dictionary created by create_session.
        params: optional query string parameters.
    Returns:
        The decoded JSON response body.
    Raises:
        InsufficientScopeError: if Carta rejects the request for a missing scope.
        RuntimeError: if the request still fails after the retry budget is spent.
    """
    has_refreshed_token = False
    for attempt in range(1, __MAX_RETRIES + 1):
        headers = {
            "Authorization": f"Bearer {session['access_token']}",
            "User-Agent": __USER_AGENT,
            "Accept": "application/json",
        }
        try:
            response = requests.get(
                url, headers=headers, params=params, timeout=__HTTP_TIMEOUT_SECONDS
            )

            if response.status_code == 429:
                if attempt == __MAX_RETRIES:
                    log.error(f"Rate limited by Carta after {__MAX_RETRIES} attempts: {url}")
                    raise RuntimeError(f"HTTP 429: {url}")
                # Carta reports how long to wait, so honor the header ahead of the backoff.
                wait_seconds = int(
                    response.headers.get("Retry-After", __DEFAULT_RETRY_AFTER_SECONDS)
                )
                log.warning(f"Rate limited by Carta, retrying in {wait_seconds}s")
                time.sleep(wait_seconds)
                continue

            if response.status_code == 401 and not has_refreshed_token:
                log.warning("Carta rejected the access token, refreshing it and retrying")
                refresh_access_token(session)
                has_refreshed_token = True
                continue

            if response.status_code == 403:
                raise InsufficientScopeError(f"HTTP 403: {url}")

            if response.status_code >= 500:
                if attempt == __MAX_RETRIES:
                    log.error(
                        f"Carta returned HTTP {response.status_code} after "
                        f"{__MAX_RETRIES} attempts: {url}"
                    )
                    raise RuntimeError(f"HTTP {response.status_code}: {url}")
                wait_seconds = backoff_seconds(attempt)
                log.warning(
                    f"Carta returned HTTP {response.status_code} on attempt "
                    f"{attempt}/{__MAX_RETRIES}, retrying in {wait_seconds}s"
                )
                time.sleep(wait_seconds)
                continue

            if response.status_code >= 400:
                log.error(f"Carta returned HTTP {response.status_code}: {url}")
                raise RuntimeError(f"HTTP {response.status_code}: {url}")

            return response.json()
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ) as error:
            if attempt == __MAX_RETRIES:
                log.error(f"Request to Carta failed after {__MAX_RETRIES} attempts: {url}")
                raise error
            wait_seconds = backoff_seconds(attempt)
            log.warning(
                f"Request to Carta failed on attempt {attempt}/{__MAX_RETRIES}, "
                f"retrying in {wait_seconds}s"
            )
            time.sleep(wait_seconds)


def paginate(
    base_url: str,
    api_version: str,
    resource_path: str,
    response_key: str,
    session: dict,
    params: Optional[dict] = None,
) -> Iterable[dict]:
    """
    Yield every record of a paginated Issuer API resource one record at a time.
    Records are yielded as they arrive rather than collected into a list, so memory stays flat
    no matter how large the resource is. Pagination ends when Carta stops returning a page token.
    Args:
        base_url: the Issuer API base URL.
        api_version: the Issuer API version path segment.
        resource_path: the resource path below the version segment.
        response_key: the key holding the record list in the response body.
        session: the session dictionary created by create_session.
        params: optional query string parameters, for example an incremental cursor.
    Yields:
        One record dictionary at a time.
    """
    page_token = None
    page_count = 0
    record_count = 0
    while True:
        page_params = dict(params or {})
        page_params["pageSize"] = __PAGE_SIZE
        if page_token:
            page_params["pageToken"] = page_token

        page = request_json(f"{base_url}/{api_version}/{resource_path}", session, page_params)
        for record in page.get(response_key, []) or []:
            record_count += 1
            yield record

        page_count += 1
        if page_count % __PROGRESS_EVERY_PAGES == 0:
            log.info(f"Fetched {page_count} pages and {record_count} records of {response_key}")

        page_token = page.get("nextPageToken")
        if not page_token:
            break


def unwrap_value(field: Any) -> Any:
    """
    Unwrap Carta's single key value envelope, for example {"value": "100"} to "100".
    Args:
        field: a raw field value from a Carta response.
    Returns:
        The inner value when the field is a value envelope, otherwise the field unchanged.
    """
    if isinstance(field, dict) and "value" in field and len(field) == 1:
        return field["value"]
    return field


def split_money(money: Optional[dict]) -> tuple:
    """
    Split a Carta money object into its amount and currency code.
    Args:
        money: a Carta money object, or None.
    Returns:
        A tuple of the amount and the currency code, both None when money is absent.
    """
    if not isinstance(money, dict):
        return None, None
    return unwrap_value(money.get("amount")), unwrap_value(money.get("currencyCode"))


def build_option_grant_row(issuer_id: str, record: dict) -> dict:
    """
    Map a Carta option grant record to its destination row.
    Args:
        issuer_id: the Carta issuer id that owns the record.
        record: the raw option grant record.
    Returns:
        The destination row for the option_grants table.
    """
    exercise_price_amount, exercise_price_currency = split_money(record.get("exercisePrice"))
    return {
        "id": record.get("id"),
        "issuer_id": issuer_id,
        "stakeholder_id": record.get("stakeholderId"),
        "equity_incentive_plan_name": record.get("equityIncentivePlanName"),
        "security_label": record.get("securityLabel"),
        "stock_option_type": record.get("stockOptionType"),
        "iso_nso_split": record.get("isoNsoSplit"),
        "early_exercisable": record.get("earlyExercisable"),
        "issue_date": unwrap_value(record.get("issueDate")),
        "grant_expiration_date": unwrap_value(record.get("grantExpirationDate")),
        "last_exercisable_date": unwrap_value(record.get("lastExercisableDate")),
        "quantity": unwrap_value(record.get("quantity")),
        "outstanding_quantity": unwrap_value(record.get("outstandingQuantity")),
        "vested_quantity": unwrap_value(record.get("vestedQuantity")),
        "exercised_quantity": unwrap_value(record.get("exercisedQuantity")),
        "canceled_quantity": unwrap_value(record.get("canceledQuantity")),
        "forfeited_quantity": unwrap_value(record.get("forfeitedQuantity")),
        "expired_quantity": unwrap_value(record.get("expiredQuantity")),
        "returned_to_pool_quantity": unwrap_value(record.get("returnedToPoolQuantity")),
        "returned_to_treasury_quantity": unwrap_value(record.get("returnedToTreasuryQuantity")),
        "exercise_price_amount": exercise_price_amount,
        "exercise_price_currency": exercise_price_currency,
        "security_id": record.get("securityId"),
        "share_class_id": record.get("shareClassId"),
        "vesting_schedule_template_id": record.get("vestingScheduleTemplateId"),
        "exercise_periods": record.get("exercisePeriods"),
        "last_modified_datetime": unwrap_value(record.get("lastModifiedDatetime")),
    }


def build_restricted_stock_unit_row(issuer_id: str, record: dict) -> dict:
    """
    Map a Carta restricted stock unit record to its destination row.
    Args:
        issuer_id: the Carta issuer id that owns the record.
        record: the raw restricted stock unit record.
    Returns:
        The destination row for the restricted_stock_units table.
    """
    vesting_schedule = record.get("vestingSchedule") or {}
    return {
        "id": record.get("id"),
        "issuer_id": issuer_id,
        "stakeholder_id": record.get("stakeholderId"),
        "equity_incentive_plan_name": record.get("equityIncentivePlanName"),
        "security_label": record.get("securityLabel"),
        "issue_date": unwrap_value(record.get("issueDate")),
        "vesting_start_date": unwrap_value(record.get("vestingStartDate")),
        "board_approval_date": unwrap_value(record.get("boardApprovalDate")),
        "stakeholder_acceptance_date": unwrap_value(record.get("stakeholderAcceptanceDate")),
        "quantity": unwrap_value(record.get("quantity")),
        "vested_quantity": unwrap_value(record.get("vestedQuantity")),
        "released_quantity": unwrap_value(record.get("releasedQuantity")),
        "net_settled_quantity": unwrap_value(record.get("netSettledQuantity")),
        "canceled_quantity": unwrap_value(record.get("canceledQuantity")),
        "forfeited_quantity": unwrap_value(record.get("forfeitedQuantity")),
        "expired_quantity": unwrap_value(record.get("expiredQuantity")),
        "returned_to_pool_quantity": unwrap_value(record.get("returnedToPoolQuantity")),
        "returned_to_treasury_quantity": unwrap_value(record.get("returnedToTreasuryQuantity")),
        "security_id": record.get("securityId"),
        "share_class_id": record.get("shareClassId"),
        "vesting_schedule_template_id": record.get("vestingScheduleTemplateId"),
        "vesting_schedule_name": vesting_schedule.get("name"),
        "vesting_schedule_start_date": unwrap_value(vesting_schedule.get("startDate")),
        "vesting_schedule_end_date": unwrap_value(vesting_schedule.get("endDate")),
        "settlements": record.get("settlements"),
        "last_modified_datetime": unwrap_value(record.get("lastModifiedDatetime")),
    }


def build_restricted_stock_award_row(issuer_id: str, record: dict) -> dict:
    """
    Map a Carta restricted stock award record to its destination row.
    Args:
        issuer_id: the Carta issuer id that owns the record.
        record: the raw restricted stock award record.
    Returns:
        The destination row for the restricted_stock_awards table.
    """
    vesting_schedule = record.get("vestingSchedule") or {}
    price_per_share_amount, price_per_share_currency = split_money(record.get("pricePerShare"))
    return {
        "id": record.get("id"),
        "issuer_id": issuer_id,
        "stakeholder_id": record.get("stakeholderId"),
        "equity_incentive_plan_name": record.get("equityIncentivePlanName"),
        "share_class_name": record.get("shareClassName"),
        "security_label": record.get("securityLabel"),
        "issue_date": unwrap_value(record.get("issueDate")),
        "vesting_start_date": unwrap_value(record.get("vestingStartDate")),
        "quantity": unwrap_value(record.get("quantity")),
        "vested_quantity": unwrap_value(record.get("vestedQuantity")),
        "canceled_quantity": unwrap_value(record.get("canceledQuantity")),
        "returned_to_pool_quantity": unwrap_value(record.get("returnedToPoolQuantity")),
        "returned_to_treasury_quantity": unwrap_value(record.get("returnedToTreasuryQuantity")),
        "price_per_share_amount": price_per_share_amount,
        "price_per_share_currency": price_per_share_currency,
        "security_id": record.get("securityId"),
        "share_class_id": record.get("shareClassId"),
        "vesting_schedule_template_id": record.get("vestingScheduleTemplateId"),
        "vesting_schedule_name": vesting_schedule.get("name"),
        "vesting_schedule_start_date": unwrap_value(vesting_schedule.get("startDate")),
        "vesting_schedule_end_date": unwrap_value(vesting_schedule.get("endDate")),
        "last_modified_datetime": unwrap_value(record.get("lastModifiedDatetime")),
    }


def build_certificate_row(issuer_id: str, record: dict) -> dict:
    """
    Map a Carta certificate record to its destination row.
    Args:
        issuer_id: the Carta issuer id that owns the record.
        record: the raw certificate record.
    Returns:
        The destination row for the certificates table.
    """
    price_per_share_amount, price_per_share_currency = split_money(record.get("pricePerShare"))
    return {
        "id": record.get("id"),
        "issuer_id": issuer_id,
        "stakeholder_id": record.get("stakeholderId"),
        "share_class_name": record.get("shareClassName"),
        "security_label": record.get("securityLabel"),
        "issue_date": unwrap_value(record.get("issueDate")),
        "quantity": unwrap_value(record.get("quantity")),
        "canceled_quantity": unwrap_value(record.get("canceledQuantity")),
        "returned_to_pool_quantity": unwrap_value(record.get("returnedToPoolQuantity")),
        "returned_to_treasury_quantity": unwrap_value(record.get("returnedToTreasuryQuantity")),
        "price_per_share_amount": price_per_share_amount,
        "price_per_share_currency": price_per_share_currency,
        "security_id": record.get("securityId"),
        "share_class_id": record.get("shareClassId"),
        "vesting_schedule_template_id": record.get("vestingScheduleTemplateId"),
        "preceded_by": record.get("precededBy"),
        "last_modified_datetime": unwrap_value(record.get("lastModifiedDatetime")),
    }


# Row builder and vesting event security type per security resource. Certificates carry no
# vesting events, so their security type is None.
__SECURITY_BUILDERS = {
    "optionGrants": (build_option_grant_row, "option_grant"),
    "restrictedStockUnits": (build_restricted_stock_unit_row, "restricted_stock_unit"),
    "restrictedStockAwards": (build_restricted_stock_award_row, "restricted_stock_award"),
    "certificates": (build_certificate_row, None),
}


def upsert_vesting_events(issuer_id: str, security_type: str, record: dict) -> int:
    """
    Flatten the vesting events nested inside a security record into their own table.
    Args:
        issuer_id: the Carta issuer id that owns the record.
        security_type: the kind of security the events belong to.
        record: the parent security record.
    Returns:
        The number of vesting event rows upserted.
    """
    upsert_count = 0
    for event in record.get("vestingEvents", []) or []:
        # The 'upsert' operation inserts or updates the record in the destination table.
        # The first argument is the destination table name, the second is the record itself.
        op.upsert(
            table="vesting_events",
            data={
                "id": event.get("id"),
                "issuer_id": issuer_id,
                "security_type": security_type,
                "security_id": record.get("id"),
                "stakeholder_id": record.get("stakeholderId"),
                "vest_date": unwrap_value(event.get("vestDate")),
                "quantity": unwrap_value(event.get("quantity")),
                "vested_quantity": unwrap_value(event.get("vestedQuantity")),
                "max_quantity": unwrap_value(event.get("maxQuantity")),
                "target_quantity": unwrap_value(event.get("targetQuantity")),
                "vested": event.get("vested"),
                "performance_condition": event.get("performanceCondition"),
            },
        )
        upsert_count += 1
    return upsert_count


def upsert_option_grant_exercises(issuer_id: str, grant: dict) -> int:
    """
    Flatten the exercises nested inside an option grant into their own table.
    Carta does not always supply an exercise id, so the row is keyed by its position in the
    grant's exercises array, which is stable for a given grant.
    Args:
        issuer_id: the Carta issuer id that owns the grant.
        grant: the parent option grant record.
    Returns:
        The number of exercise rows upserted.
    """
    upsert_count = 0
    for exercise_index, exercise in enumerate(grant.get("exercises", []) or []):
        # The 'upsert' operation inserts or updates the record in the destination table.
        op.upsert(
            table="option_grant_exercises",
            data={
                "issuer_id": issuer_id,
                "option_grant_id": grant.get("id"),
                "exercise_index": exercise_index,
                "exercise_id": exercise.get("exerciseId") or None,
                "quantity": unwrap_value(exercise.get("quantity")),
                "exercise_date": unwrap_value(exercise.get("exerciseDate")),
                "status": exercise.get("status"),
                "exercise_type": exercise.get("exerciseType"),
                "certificate_id": exercise.get("certificateId"),
                "qualified": exercise.get("qualified"),
            },
        )
        upsert_count += 1
    return upsert_count


def sync_security_resource(
    configuration: dict, issuer_id: str, resource: str, table: str, session: dict, state: dict
):
    """
    Sync one incremental security resource and the child rows nested inside its records.
    The state cursor is the highest lastModifiedDatetime seen for this issuer and resource. It is
    written only once the resource finishes, because Carta does not return records in
    lastModifiedDatetime order and a cursor advanced mid-resource could skip older records that
    have not been fetched yet. Mid-resource checkpoints therefore persist the rows already sent
    without moving the cursor, and an interrupted sync safely refetches the resource.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        issuer_id: the Carta issuer id to sync.
        resource: the Issuer API resource path segment.
        table: the destination table for the parent records.
        session: the session dictionary created by create_session.
        state: a dictionary containing state information from previous runs.
    """
    base_url = get_api_base_url(configuration)
    api_version = get_api_version(configuration)
    row_builder, security_type = __SECURITY_BUILDERS[resource]

    cursor_key = f"{issuer_id}_{resource}_last_modified"
    cursor = state.get(cursor_key)
    # Carta's only working server side filter. It is inclusive of the boundary record, which is
    # harmless because upserts are idempotent.
    params = {"lastModifiedDatetimeAfter": cursor} if cursor else {}

    parent_count = 0
    vesting_event_count = 0
    exercise_count = 0
    upserts_since_checkpoint = 0
    highest_last_modified = cursor

    for record in paginate(
        base_url, api_version, f"issuers/{issuer_id}/{resource}", resource, session, params
    ):
        # The 'upsert' operation inserts or updates the record in the destination table.
        op.upsert(table=table, data=row_builder(issuer_id, record))
        parent_count += 1

        child_vesting_events = 0
        if security_type:
            child_vesting_events = upsert_vesting_events(issuer_id, security_type, record)
        child_exercises = 0
        if resource == "optionGrants":
            child_exercises = upsert_option_grant_exercises(issuer_id, record)

        vesting_event_count += child_vesting_events
        exercise_count += child_exercises
        upserts_since_checkpoint += 1 + child_vesting_events + child_exercises

        last_modified = unwrap_value(record.get("lastModifiedDatetime"))
        if last_modified and (
            highest_last_modified is None or last_modified > highest_last_modified
        ):
            highest_last_modified = last_modified

        if upserts_since_checkpoint >= __CHECKPOINT_INTERVAL_UPSERTS:
            # Save the progress by checkpointing the state. This is important for ensuring that
            # the sync process can resume from the correct position in case of interruptions.
            # Checkpointing mid-resource also keeps each commit small: a single very large
            # commit at the end of a resource can fail and roll back everything already sent.
            op.checkpoint(state)
            log.info(f"Checkpointed {table} after {parent_count} records for issuer {issuer_id}")
            upserts_since_checkpoint = 0

    if highest_last_modified:
        state[cursor_key] = highest_last_modified

    # Save the progress by checkpointing the state once the resource and its cursor are complete.
    op.checkpoint(state)
    log.info(
        f"Synced {parent_count} {table}, {vesting_event_count} vesting events and "
        f"{exercise_count} exercises for issuer {issuer_id}"
    )


def sync_stakeholders(configuration: dict, issuer_id: str, session: dict, state: dict):
    """
    Sync the stakeholders of one issuer. Carta exposes no cursor here, so this is a full refresh.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        issuer_id: the Carta issuer id to sync.
        session: the session dictionary created by create_session.
        state: a dictionary containing state information from previous runs.
    """
    base_url = get_api_base_url(configuration)
    api_version = get_api_version(configuration)
    upsert_count = 0
    for record in paginate(
        base_url, api_version, f"issuers/{issuer_id}/stakeholders", "stakeholders", session
    ):
        # The 'upsert' operation inserts or updates the record in the destination table.
        op.upsert(
            table="stakeholders",
            data={
                "id": record.get("id"),
                "issuer_id": issuer_id,
                "full_name": record.get("fullName"),
                "email": record.get("email"),
                "employee_id": record.get("employeeId"),
                "relationship": record.get("relationship"),
                "stakeholder_group": record.get("group"),
                "entity_type": record.get("entityType"),
            },
        )
        upsert_count += 1

    # Save the progress by checkpointing the state so the next sync resumes after this resource.
    op.checkpoint(state)
    log.info(f"Synced {upsert_count} stakeholders for issuer {issuer_id}")


def sync_share_classes(configuration: dict, issuer_id: str, session: dict, state: dict):
    """
    Sync the share classes of one issuer as a full refresh.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        issuer_id: the Carta issuer id to sync.
        session: the session dictionary created by create_session.
        state: a dictionary containing state information from previous runs.
    """
    base_url = get_api_base_url(configuration)
    api_version = get_api_version(configuration)
    upsert_count = 0
    for record in paginate(
        base_url, api_version, f"issuers/{issuer_id}/shareClasses", "shareClasses", session
    ):
        # The 'upsert' operation inserts or updates the record in the destination table.
        op.upsert(
            table="share_classes",
            data={
                "id": record.get("id"),
                "issuer_id": issuer_id,
                "name": record.get("name"),
                "prefix": record.get("prefix"),
                "type": record.get("type"),
                "par_value": unwrap_value(record.get("parValue")),
                "seniority": record.get("seniority"),
                "pari_passu": record.get("pariPassu"),
            },
        )
        upsert_count += 1

    # Save the progress by checkpointing the state so the next sync resumes after this resource.
    op.checkpoint(state)
    log.info(f"Synced {upsert_count} share classes for issuer {issuer_id}")


def sync_fair_market_values(configuration: dict, issuer_id: str, session: dict, state: dict):
    """
    Sync 409A fair market values and the per share class valuations nested inside them.
    The nested valuations carry the authoritative price per share class.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        issuer_id: the Carta issuer id to sync.
        session: the session dictionary created by create_session.
        state: a dictionary containing state information from previous runs.
    """
    base_url = get_api_base_url(configuration)
    api_version = get_api_version(configuration)
    valuation_count = 0
    share_class_valuation_count = 0
    for record in paginate(
        base_url, api_version, f"issuers/{issuer_id}/fairMarketValues", "fairMarketValues", session
    ):
        fair_market_value_id = record.get("id")
        # The 'upsert' operation inserts or updates the record in the destination table.
        op.upsert(
            table="fair_market_values",
            data={
                "id": fair_market_value_id,
                "issuer_id": issuer_id,
                "effective_date": unwrap_value(record.get("effectiveDate")),
                "expiration_date": unwrap_value(record.get("expirationDate")),
            },
        )
        valuation_count += 1

        for share_class_valuation in record.get("shareClassValuations", []) or []:
            price_amount, price_currency = split_money(share_class_valuation.get("price"))
            # The 'upsert' operation inserts or updates the record in the destination table.
            op.upsert(
                table="fair_market_value_share_class_valuations",
                data={
                    "issuer_id": issuer_id,
                    "fair_market_value_id": fair_market_value_id,
                    "share_class_id": share_class_valuation.get("shareClassId"),
                    "share_class_name": share_class_valuation.get("shareClassName"),
                    "is_common": share_class_valuation.get("common"),
                    "price_amount": price_amount,
                    "price_currency": price_currency,
                },
            )
            share_class_valuation_count += 1

    # Save the progress by checkpointing the state so the next sync resumes after this resource.
    op.checkpoint(state)
    log.info(
        f"Synced {valuation_count} fair market values and {share_class_valuation_count} "
        f"share class valuations for issuer {issuer_id}"
    )


def sync_vesting_schedule_templates(
    configuration: dict, issuer_id: str, session: dict, state: dict
):
    """
    Sync the vesting schedule templates of one issuer as a full refresh.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        issuer_id: the Carta issuer id to sync.
        session: the session dictionary created by create_session.
        state: a dictionary containing state information from previous runs.
    """
    base_url = get_api_base_url(configuration)
    api_version = get_api_version(configuration)
    upsert_count = 0
    for record in paginate(
        base_url,
        api_version,
        f"issuers/{issuer_id}/vestingScheduleTemplates",
        "vestingScheduleTemplates",
        session,
    ):
        # The 'upsert' operation inserts or updates the record in the destination table.
        op.upsert(
            table="vesting_schedule_templates",
            data={
                "id": record.get("id"),
                "issuer_id": issuer_id,
                "uuid": record.get("uuid"),
                "name": record.get("name"),
                "description": record.get("description"),
                "vesting_schedule_type": record.get("vestingScheduleType"),
                "periods": record.get("periods"),
            },
        )
        upsert_count += 1

    # Save the progress by checkpointing the state so the next sync resumes after this resource.
    op.checkpoint(state)
    log.info(f"Synced {upsert_count} vesting schedule templates for issuer {issuer_id}")


def sync_capitalization_table(configuration: dict, issuer_id: str, session: dict, state: dict):
    """
    Sync stakeholder capitalization table holdings and their per share class breakdown.
    This endpoint returns a nested object rather than a flat list, so it needs its own pagination
    loop instead of the shared paginate helper. outstanding_shares here is the authoritative
    issued share count per stakeholder.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        issuer_id: the Carta issuer id to sync.
        session: the session dictionary created by create_session.
        state: a dictionary containing state information from previous runs.
    """
    base_url = get_api_base_url(configuration)
    api_version = get_api_version(configuration)
    url = f"{base_url}/{api_version}/issuers/{issuer_id}/stakeholderCapitalizationTable"

    holding_count = 0
    share_class_holding_count = 0
    page_token = None
    while True:
        params = {"pageSize": __PAGE_SIZE}
        if page_token:
            params["pageToken"] = page_token

        page = request_json(url, session, params)
        capitalization_table = page.get("stakeholderCapitalizationTable") or {}
        as_of_date = unwrap_value(capitalization_table.get("asOfDate"))

        for group in capitalization_table.get("stakeholderGroups", []) or []:
            group_id = group.get("stakeholderGroupId")
            group_name = group.get("stakeholderGroupName")

            for stakeholder in group.get("stakeholders", []) or []:
                stakeholder_id = stakeholder.get("stakeholderId")
                summary = stakeholder.get("summary") or {}
                cash_raised_amount, cash_raised_currency = split_money(summary.get("cashRaised"))
                # The 'upsert' operation inserts or updates the record in the destination table.
                op.upsert(
                    table="stakeholder_holdings",
                    data={
                        "issuer_id": issuer_id,
                        "stakeholder_id": stakeholder_id,
                        "stakeholder_name": stakeholder.get("stakeholderName"),
                        "stakeholder_group_id": group_id,
                        "stakeholder_group_name": group_name,
                        "as_of_date": as_of_date,
                        "fully_diluted_shares": unwrap_value(summary.get("fullyDilutedShares")),
                        "outstanding_shares": unwrap_value(summary.get("outstandingShares")),
                        "cash_raised_amount": cash_raised_amount,
                        "cash_raised_currency": cash_raised_currency,
                    },
                )
                holding_count += 1

                for share_class_summary in stakeholder.get("shareClassSummaries", []) or []:
                    amount, currency = split_money(share_class_summary.get("cashRaised"))
                    # The 'upsert' operation inserts or updates the destination record.
                    op.upsert(
                        table="stakeholder_share_class_holdings",
                        data={
                            "issuer_id": issuer_id,
                            "stakeholder_id": stakeholder_id,
                            "share_class_id": share_class_summary.get("shareClassId"),
                            "share_class_name": share_class_summary.get("name"),
                            "fully_diluted_shares": unwrap_value(
                                share_class_summary.get("fullyDilutedShares")
                            ),
                            "outstanding_shares": unwrap_value(
                                share_class_summary.get("outstandingShares")
                            ),
                            "cash_raised_amount": amount,
                            "cash_raised_currency": currency,
                        },
                    )
                    share_class_holding_count += 1

        page_token = page.get("nextPageToken")
        if not page_token:
            break

    # Save the progress by checkpointing the state so the next sync resumes after this resource.
    op.checkpoint(state)
    log.info(
        f"Synced {holding_count} stakeholder holdings and {share_class_holding_count} "
        f"share class holdings for issuer {issuer_id}"
    )


def sync_convertible_notes(configuration: dict, issuer_id: str, session: dict, state: dict):
    """
    Sync convertible notes, including SAFEs, for one issuer as a full refresh.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        issuer_id: the Carta issuer id to sync.
        session: the session dictionary created by create_session.
        state: a dictionary containing state information from previous runs.
    """
    base_url = get_api_base_url(configuration)
    api_version = get_api_version(configuration)
    upsert_count = 0
    for record in paginate(
        base_url, api_version, f"issuers/{issuer_id}/convertibleNotes", "convertibleNotes", session
    ):
        note_block = record.get("noteBlock") or {}
        cash_paid_amount, cash_paid_currency = split_money(record.get("cashPaid"))
        interest_amount, interest_currency = split_money(record.get("interest"))
        price_cap_amount, price_cap_currency = split_money(record.get("priceCap"))
        # The 'upsert' operation inserts or updates the record in the destination table.
        op.upsert(
            table="convertible_notes",
            data={
                "id": record.get("id"),
                "issuer_id": issuer_id,
                "stakeholder_id": record.get("stakeholderId"),
                "security_label": record.get("securityLabel"),
                "security_id": record.get("securityId"),
                "issue_at": unwrap_value(record.get("issueDatetime")),
                "conversion_at": unwrap_value(record.get("conversionDatetime")),
                "canceled_at": unwrap_value(record.get("canceledDatetime")),
                "maturity_at": unwrap_value(record.get("maturityDatetime")),
                "cash_paid_amount": cash_paid_amount,
                "cash_paid_currency": cash_paid_currency,
                "interest_amount": interest_amount,
                "interest_currency": interest_currency,
                "interest_rate": unwrap_value(record.get("interestRate")),
                "interest_accrual_period": record.get("interestAccrualPeriod"),
                "interest_compounding_period": record.get("interestCompoundingPeriod"),
                "day_count_basis": record.get("dayCountBasis"),
                "price_cap_amount": price_cap_amount,
                "price_cap_currency": price_cap_currency,
                "discount_percentage": unwrap_value(record.get("discountPercentage")),
                "change_in_control_percent": unwrap_value(record.get("changeInControlPercent")),
                "canceled_quantity": unwrap_value(record.get("canceledQuantity")),
                "note_block_id": note_block.get("id"),
                "note_block_name": note_block.get("name"),
                "note_block_prefix": note_block.get("prefix"),
                "note_block_type": note_block.get("noteType"),
                "note_block_status": note_block.get("status"),
            },
        )
        upsert_count += 1

    # Save the progress by checkpointing the state so the next sync resumes after this resource.
    op.checkpoint(state)
    log.info(f"Synced {upsert_count} convertible notes for issuer {issuer_id}")


def sync_issuer_details(configuration: dict, issuer_id: str, session: dict, state: dict):
    """
    Sync the issuer detail record.
    Not every Carta OAuth app can reach the issuer detail endpoint, so a failure here is logged
    and skipped rather than raised.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        issuer_id: the Carta issuer id to sync.
        session: the session dictionary created by create_session.
        state: a dictionary containing state information from previous runs.
    """
    base_url = get_api_base_url(configuration)
    api_version = get_api_version(configuration)
    try:
        body = request_json(f"{base_url}/{api_version}/issuers/{issuer_id}", session)
    except (InsufficientScopeError, RuntimeError, requests.exceptions.RequestException) as error:
        log.warning(f"Skipped the issuer detail for issuer {issuer_id}: {error}")
        return

    issuer = body.get("issuer") or {}
    if not issuer:
        log.warning(f"Carta returned no issuer detail for issuer {issuer_id}")
        return

    # The 'upsert' operation inserts or updates the record in the destination table.
    op.upsert(
        table="issuers",
        data={
            "id": issuer.get("id"),
            "legal_name": issuer.get("legalName"),
            "doing_business_as_name": issuer.get("doingBusinessAsName"),
            "website": issuer.get("website"),
        },
    )

    # Save the progress by checkpointing the state so the next sync resumes after this resource.
    op.checkpoint(state)
    log.info(f"Synced the issuer detail for issuer {issuer_id}")


def sync_corporations(configuration: dict, session: dict, state: dict):
    """
    Sync the corporations the OAuth app can see. This is issuer independent, so it runs once.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        session: the session dictionary created by create_session.
        state: a dictionary containing state information from previous runs.
    """
    base_url = get_api_base_url(configuration)
    api_version = get_api_version(configuration)
    upsert_count = 0
    for record in paginate(base_url, api_version, "corporations", "corporations", session):
        # The 'upsert' operation inserts or updates the record in the destination table.
        op.upsert(
            table="corporations",
            data={
                "id": record.get("id"),
                "legal_name": record.get("legalName"),
                "doing_business_as_name": record.get("doingBusinessAsName"),
                "website": record.get("website"),
            },
        )
        upsert_count += 1

    # Save the progress by checkpointing the state so the next sync resumes after this resource.
    op.checkpoint(state)
    log.info(f"Synced {upsert_count} corporations")


def sync_scoped_resource(resource_label: str, sync_function, *arguments):
    """
    Run one resource sync, tolerating a scope the OAuth app was not granted.
    Carta grants scopes all or nothing per OAuth app, so a resource the app cannot read should
    cost that resource only, not the whole sync. Any other failure is left to propagate so it is
    not hidden.
    Args:
        resource_label: the resource name to name in the warning if it is skipped.
        sync_function: the sync function to call.
        arguments: the positional arguments to pass to the sync function.
    """
    try:
        sync_function(*arguments)
    except InsufficientScopeError as error:
        log.warning(
            f"Skipped {resource_label} because the OAuth app lacks the required scope: {error}"
        )


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during
    each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: Source Examples - Carta Issuer API Connector")

    validate_configuration(configuration)
    issuer_ids = parse_issuer_ids(configuration)
    session = create_session(configuration)
    log.info(f"Starting the Carta sync for {len(issuer_ids)} issuer(s)")

    for issuer_id in issuer_ids:
        # Incremental resources first: they are the largest, and each one persists its own
        # cursor, so an interruption costs at most the resource in flight.
        for resource, table in __SECURITY_RESOURCES.items():
            sync_scoped_resource(
                table,
                sync_security_resource,
                configuration,
                issuer_id,
                resource,
                table,
                session,
                state,
            )

        # Full refresh resources. Carta exposes no cursor for any of these.
        sync_scoped_resource(
            "convertible_notes", sync_convertible_notes, configuration, issuer_id, session, state
        )
        sync_scoped_resource(
            "stakeholders", sync_stakeholders, configuration, issuer_id, session, state
        )
        sync_scoped_resource(
            "share_classes", sync_share_classes, configuration, issuer_id, session, state
        )
        sync_scoped_resource(
            "fair_market_values", sync_fair_market_values, configuration, issuer_id, session, state
        )
        sync_scoped_resource(
            "vesting_schedule_templates",
            sync_vesting_schedule_templates,
            configuration,
            issuer_id,
            session,
            state,
        )
        sync_scoped_resource(
            "stakeholder_holdings",
            sync_capitalization_table,
            configuration,
            issuer_id,
            session,
            state,
        )
        sync_scoped_resource(
            "issuers", sync_issuer_details, configuration, issuer_id, session, state
        )

    # Corporations are not scoped to a single issuer, so they are synced once per sync.
    sync_scoped_resource("corporations", sync_corporations, configuration, session, state)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync
    # process can resume from the correct position in case of next sync or interruptions.
    op.checkpoint(state)
    log.info("Finished the Carta sync")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the
# command line or IDE 'run' button.
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
    with open("configuration.json", "r") as configuration_file:
        connector_configuration = json.load(configuration_file)

    # Test the connector locally
    connector.debug(configuration=connector_configuration)
