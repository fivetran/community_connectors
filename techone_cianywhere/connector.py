"""This connector demonstrates syncing general ledger reference and transaction data from
the TechOne CiAnywhere web services API: ledgers, AR/GL chart of accounts (with user-defined
fields), ledger accounts, and transactions with their linked debits/credits.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

import hashlib  # For deriving stable synthetic primary keys from natural-key parts
import re  # For validating the base_url format
import time  # For sleeping between retry attempts
from datetime import datetime, timezone  # For interpreting a Retry-After HTTP-date header
from email.utils import parsedate_to_datetime  # For parsing a Retry-After HTTP-date header

import requests  # For issuing HTTP requests to the TechOne CiAnywhere web services API

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

__TOKEN_PATH = "/oauth2/access_token"
__PAGE_SIZE = 100
__STATUSES = ("A", "I")
__RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
__MAX_ATTEMPTS = 3
__CHECKPOINT_EVERY_PAGES = 10


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or invalid.
    """
    for key in ("base_url", "client_id", "client_secret"):
        if not str(configuration.get(key, "")).strip():
            raise ValueError(f"Missing required configuration value: '{key}'")

    base_url = configuration["base_url"].strip()
    if not re.match(r"^https?://", base_url, re.IGNORECASE):
        raise ValueError(f"'base_url' must start with http:// or https://. Got: '{base_url}'")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    account_columns = {
        "chart_name": "STRING",
        "accnbri": "STRING",
        "accnbr": "STRING",
        "vers": "LONG",
        "descr_1": "STRING",
        "descr_2": "STRING",
        "sdescr": "STRING",
        "stat_ind": "STRING",
        "soundex_name": "STRING",
        "profile_type": "STRING",
        "profile_code": "STRING",
        "ldg_code_def": "STRING",
        "accnbri_def": "STRING",
        "vat_type_def": "STRING",
        "vat_rate_code_def": "STRING",
        "frgn_ccy_ind": "STRING",
        "ccy_code": "STRING",
        "exch_rate_table_name": "STRING",
        **{f"seln_type_{i}_code": "STRING" for i in range(1, 41)},
    }

    user_field_columns = {
        "chart_name": "STRING",
        "accnbri": "STRING",
        "vers": "LONG",
        **{f"user_fld_{i}": "STRING" for i in range(1, 13)},
        **{f"user_num_{i}": "DOUBLE" for i in range(1, 13)},
        **{f"user_datei_{i}": "NAIVE_DATETIME" for i in range(1, 13)},
    }

    ledger_account_columns = {
        "ldg_acct_rid": "STRING",
        "ldg_name": "STRING",
        "chart_name": "STRING",
        "accnbri": "STRING",
        "accnbr": "STRING",
        "stat_ind": "STRING",
        "bal_units_1": "DOUBLE",
        "bal_amt_1": "DOUBLE",
        "commitment_total": "DOUBLE",
        "total_balance": "DOUBLE",
        "descr": "STRING",
        "sdescr": "STRING",
    }

    transaction_columns = {
        "ldg_trans_rid": "STRING",
        "ldg_name": "STRING",
        "accnbri": "STRING",
        "accnbr": "STRING",
        "trans_nbr": "DOUBLE",
        "seqnbr": "LONG",
        "period": "INT",
        "bat_name": "STRING",
        "doc_type": "STRING",
        "doc_datei_1": "NAIVE_DATETIME",
        "doc_datei_2": "NAIVE_DATETIME",
        "doc_datei_3": "NAIVE_DATETIME",
        "doc_datei_4": "NAIVE_DATETIME",
        "doc_ref_1": "STRING",
        "doc_ref_2": "STRING",
        "doc_ref_3": "STRING",
        "source": "STRING",
        "source_id": "STRING",
        "source_datei": "NAIVE_DATETIME",
        "source_timei": "STRING",
        "narr_1": "STRING",
        "narr_2": "STRING",
        "narr_3": "STRING",
        "status": "STRING",
        "doc_unique_id": "STRING",
        "attach_ind": "STRING",
        "item_type": "STRING",
        "item_code": "STRING",
        "rate_amt": "DOUBLE",
        "units_1": "DOUBLE",
        "units_2": "DOUBLE",
        "units_3": "DOUBLE",
        "units_4": "DOUBLE",
        "amt_1": "DOUBLE",
        "amt_2": "DOUBLE",
        "amt_3": "DOUBLE",
        "amt_4": "DOUBLE",
        "ccy_code": "STRING",
        "ccy_amt": "DOUBLE",
        "exch_rate_amt": "DOUBLE",
        "vat_type": "STRING",
        "vat_rate_code": "STRING",
        "vat_rate_amt": "DOUBLE",
        "vat_amt": "DOUBLE",
        "vat_exc_amt": "DOUBLE",
        "vat_inc_amt": "DOUBLE",
        **{f"seln_type_{i}_code": "STRING" for i in range(1, 41)},
    }

    transaction_detail_columns = {
        "ldg_transd_rid": "STRING",
        "ldg_name": "STRING",
        "accnbri": "STRING",
        "trans_nbr": "DOUBLE",
        "seqnbr": "LONG",
        "period": "INT",
        "doc_type": "STRING",
        "doc_datei_1": "NAIVE_DATETIME",
        "doc_ref_1": "STRING",
        "source": "STRING",
        "narr_1": "STRING",
        "item_type": "STRING",
        "item_code": "STRING",
        "amt_1": "DOUBLE",
        "vat_amt": "DOUBLE",
        "vat_exc_amt": "DOUBLE",
        "linked_ldg_name": "STRING",
        "linked_accnbri": "STRING",
        "linked_doc_type": "STRING",
        "linked_doc_datei_1": "NAIVE_DATETIME",
        "linked_doc_ref_1": "STRING",
        "linked_source": "STRING",
        "linked_amt_1": "DOUBLE",
        "linked_vat_amt": "DOUBLE",
        "linked_vat_exc_amt": "DOUBLE",
    }

    return [
        {
            "table": "glf_ldg_ctl",
            "primary_key": ["ldg_name"],
            "columns": {
                "ldg_name": "STRING",
                "descr": "STRING",
                "chart_name": "STRING",
                "stat_ind": "STRING",
                "system_code": "STRING",
                "chart_type": "STRING",
            },
        },
        {
            "table": "glf_chart_acct",
            "primary_key": ["chart_name", "accnbri"],
            "columns": account_columns,
        },
        {
            "table": "glf_chart_acc_usf",
            "primary_key": ["chart_name", "accnbri"],
            "columns": user_field_columns,
        },
        {
            "table": "glf_ldg_acct",
            "primary_key": ["ldg_acct_rid"],
            "columns": ledger_account_columns,
        },
        {
            "table": "glf_ldg_acc_trans",
            "primary_key": ["ldg_trans_rid"],
            "columns": transaction_columns,
        },
        {
            "table": "glf_ldg_acc_transd",
            "primary_key": ["ldg_transd_rid"],
            "columns": transaction_detail_columns,
        },
    ]


def _parse_retry_after(value):
    """Parse a Retry-After header as either delay-seconds or an HTTP-date, capped at 60s.

    Args:
        value: the raw Retry-After header value, or None if absent.

    Returns:
        The delay in seconds to wait, or None if the header is absent or unparsable.
    """
    if not value:
        return None
    stripped = value.strip()
    if stripped.isdigit():
        return min(60.0, float(stripped))
    try:
        retry_at = parsedate_to_datetime(stripped)
    except (TypeError, ValueError):
        return None
    if retry_at is None:
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    delay = (retry_at - datetime.now(timezone.utc)).total_seconds()
    return max(0.0, min(60.0, delay))


def _request_with_backoff(method, url, headers, **kwargs):
    """Issue an HTTP request, retrying transient failures with exponential backoff.

    Retries (up to __MAX_ATTEMPTS): connection errors, timeouts, and HTTP 429/500/502/503/504
    (honoring Retry-After when present). Any other response, including a permanent client
    error, is returned as-is for the caller to inspect or raise on.

    Args:
        method: the HTTP method to use.
        url: the absolute URL to request.
        headers: the request headers.
        **kwargs: additional keyword arguments forwarded to requests.request().

    Returns:
        The requests.Response object for the final attempt.

    Raises:
        RuntimeError: if every attempt fails with a connection error or timeout.
    """
    last_exception = None
    for attempt in range(1, __MAX_ATTEMPTS + 1):
        try:
            response = requests.request(method, url, headers=headers, timeout=60, **kwargs)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            last_exception = exc
            if attempt >= __MAX_ATTEMPTS:
                raise RuntimeError(
                    f"{method} {url} failed after {__MAX_ATTEMPTS} attempts: {exc}"
                ) from exc
            log.warning(
                f"{method} {url} attempt {attempt}/{__MAX_ATTEMPTS} failed ({exc}), retrying"
            )
            time.sleep(min(60, 2**attempt))
            continue

        if response.status_code in __RETRY_STATUS_CODES and attempt < __MAX_ATTEMPTS:
            wait_seconds = _parse_retry_after(response.headers.get("Retry-After"))
            if wait_seconds is None:
                wait_seconds = min(60, 2**attempt)
            log.warning(
                f"{method} {url} got HTTP {response.status_code} on attempt "
                f"{attempt}/{__MAX_ATTEMPTS}; retrying in {wait_seconds:.0f}s"
            )
            time.sleep(wait_seconds)
            continue

        return response

    raise RuntimeError(f"{method} {url} failed after {__MAX_ATTEMPTS} attempts: {last_exception}")


def _get_token(configuration):
    """Request a new OAuth2 client-credentials access token from TechOne, with retry/backoff.

    Args:
        configuration: the validated connector configuration.

    Returns:
        str: the access token.

    Raises:
        RuntimeError: if the request fails, or the response has no access_token.
    """
    response = _request_with_backoff(
        "POST",
        f"{configuration['base_url']}{__TOKEN_PATH}",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "client_id": configuration["client_id"],
            "client_secret": configuration["client_secret"],
            "grant_type": "client_credentials",
        },
    )
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
        raise RuntimeError("TechOne OAuth response did not include access_token")
    return token


def _headers(token):
    """Build the bearer-auth and JSON headers used on every TechOne API request."""
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _request(method, path, token_holder, configuration, **kwargs):
    """Issue an HTTP request, refreshing the access token once on a 401.

    The token refresh is a separate retry cycle from the transient-error backoff in
    _request_with_backoff(): a refreshed token always gets its own full retry budget,
    instead of sharing attempts with earlier transient failures.

    Args:
        method: the HTTP method to use.
        path: the API path, relative to the configured base_url.
        token_holder: a single-item list holding the current access token, updated in place
            on refresh so the caller's subsequent requests reuse the new token.
        configuration: the validated connector configuration.
        **kwargs: additional keyword arguments forwarded to requests.request().

    Returns:
        The decoded JSON response body, or an empty dict for an empty response body.
    """
    url = f"{configuration['base_url']}{path}"
    token_refreshed = False
    while True:
        response = _request_with_backoff(method, url, headers=_headers(token_holder[0]), **kwargs)
        if response.status_code == 401 and not token_refreshed:
            log.warning(f"{method} {path} got 401, refreshing token and retrying")
            token_holder[0] = _get_token(configuration)
            token_refreshed = True
            continue
        response.raise_for_status()
        return response.json() if response.content else {}


def _post(path, payload, token_holder, configuration):
    """Issue a POST request against the TechOne API."""
    return _request("POST", path, token_holder, configuration, json=payload)


def _get(path, params, token_holder, configuration):
    """Issue a GET request against the TechOne API."""
    return _request("GET", path, token_holder, configuration, params=params)


def _stable_key(*parts):
    """Derive a stable synthetic primary key by hashing the given natural-key parts."""
    raw = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _to_int(value):
    """Convert a value to int, returning None for empty or unparsable input."""
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value):
    """Convert a value to float, returning None for empty or unparsable input."""
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_flag(value):
    """Convert a truthy/falsy value to the source's Y/N flag convention."""
    if value is None:
        return None
    return "Y" if value else "N"


def _to_naive_datetime(value):
    """Normalize a source timestamp string to a naive ISO8601 datetime string."""
    if not value:
        return None
    value = str(value).strip()
    if value.endswith("Z"):
        value = value[:-1]
    if len(value) > 6 and value[-6] in {"+", "-"} and value[-3] == ":":
        value = value[:-6]
    if "." in value:
        base, fraction = value.split(".", 1)
        value = f"{base}.{fraction[:6]}"
    if "T" not in value:
        return value.replace(" ", "T", 1) if " " in value else f"{value}T00:00:00"
    return value


def _list_ledgers(token_holder, configuration):
    """Fetch all active and inactive ledgers, de-duplicated by ledger name."""
    ledgers = {}
    for status in __STATUSES:
        response = _post(
            "/Api/T1/WS/v1/Ledger/ListLedgers",
            {"Items": [{"LedgerName": "", "Status": status}]},
            token_holder,
            configuration,
        )
        for item in response.get("Items", []):
            for ledger in item.get("Ledgers", []):
                if ledger.get("LedgerName"):
                    ledgers[ledger["LedgerName"]] = ledger
    return list(ledgers.values())


def _chart_names(ledgers, chart_type):
    """Return the sorted, de-duplicated chart names used by ledgers of the given chart type."""
    names = set()
    for ledger in ledgers:
        chart_name = ledger.get("ChartName")
        is_matching_type = str(ledger.get("ChartType", "")).upper() == chart_type
        if chart_name and is_matching_type:
            names.add(chart_name)
    return sorted(names)


def _paged_post(path, request_item, response_key, token_holder, configuration):
    """POST to a paginated list endpoint, yielding each page of rows until a short page ends it.

    This generator yields raw data pages, not SDK operations, so it remains a generator under
    SDK v2 as well; callers call op.upsert()/op.checkpoint() directly for each page's rows.
    """
    page = 1
    while True:
        payload = {"Items": [{**request_item, "PageNumber": page, "PageSize": __PAGE_SIZE}]}
        response = _post(path, payload, token_holder, configuration)
        rows = []
        for item in response.get("Items", []):
            rows.extend(item.get(response_key, []))
        if not rows:
            break
        yield rows
        if len(rows) < __PAGE_SIZE:
            break
        page += 1


def _emit_account(account, fallback_chart_name=None):
    """Upsert a chart-of-accounts row and its user-defined-fields row (or delete it if empty).

    Args:
        account: the raw account dictionary returned by the source.
        fallback_chart_name: the chart name to use when the account itself has none.
    """
    chart_name = account.get("ChartName") or fallback_chart_name
    accnbri = account.get("AccountNumberInternal") or account.get("AccountNumber")
    if not chart_name or not accnbri:
        return

    row = {
        "chart_name": chart_name,
        "accnbri": accnbri,
        "accnbr": account.get("AccountNumberExternal") or account.get("AccountNumber"),
        "vers": _to_int(account.get("Vers")),
        "descr_1": account.get("Description1") or account.get("AccountName"),
        "descr_2": account.get("Description2") or account.get("AccountName2"),
        "sdescr": account.get("ShortDescription"),
        "stat_ind": account.get("Status"),
        "soundex_name": account.get("SoundsLikeName"),
        "profile_type": account.get("DefaultProfileType"),
        "profile_code": account.get("DefaultProfileCode"),
        "ldg_code_def": account.get("DefaultDissectionLedgerCode"),
        "accnbri_def": account.get("DefaultDissectionAccountNumber"),
        "vat_type_def": account.get("VatRateTypeDefault"),
        "vat_rate_code_def": account.get("VatRateCodeDefault"),
        "frgn_ccy_ind": _to_flag(account.get("IsForeignCurrency")),
        "ccy_code": account.get("CurrencyCode"),
        "exch_rate_table_name": account.get("ExchangeRateTableName"),
    }
    for i in range(1, 41):
        row[f"seln_type_{i}_code"] = account.get(f"SelectionCode{i}")
    # The 'upsert' operation is used to insert or update data in the destination table.
    # The first argument is the name of the destination table.
    # The second argument is a dictionary containing the record to be upserted.
    op.upsert(table="glf_chart_acct", data=row)

    user_fields_key = {"chart_name": chart_name, "accnbri": accnbri}
    usf = {**user_fields_key, "vers": _to_int(account.get("Vers"))}
    has_user_fields = False
    for i in range(1, 13):
        alpha = account.get(f"Userfield{i}Alpha")
        numeric = account.get(f"Userfield{i}Numeric")
        date_value = account.get(f"Userfield{i}Date")
        usf[f"user_fld_{i}"] = alpha
        usf[f"user_num_{i}"] = _to_float(numeric)
        usf[f"user_datei_{i}"] = _to_naive_datetime(date_value)
        # Check explicitly for a populated value (including numeric 0) rather than truthiness,
        # so a user-defined field whose only value is 0 is not treated as unpopulated.
        if alpha not in (None, "") or numeric not in (None, "") or date_value not in (None, ""):
            has_user_fields = True
    if has_user_fields:
        op.upsert(table="glf_chart_acc_usf", data=usf)
    else:
        # If this account previously had user-defined fields and the source has since cleared
        # all of them, remove the stale row instead of leaving it behind.
        op.delete(table="glf_chart_acc_usf", keys=user_fields_key)


def _sync_chart_accounts(token_holder, configuration, ledgers, state):
    """Sync AR and GL chart-of-accounts entries for every chart name found on the ledgers."""
    for chart_name in _chart_names(ledgers, "AR"):
        log.info(f"Syncing AR accounts for chart {chart_name}")
        page_count = 0
        for status in __STATUSES:
            for page in _paged_post(
                "/Api/T1/WS/v1/ChartAccountAR/ListDebtors",
                {"ChartName": chart_name, "Status": status},
                "ReadDebtors",
                token_holder,
                configuration,
            ):
                for debtor in page:
                    _emit_account(debtor, chart_name)
                page_count += 1
                if page_count % __CHECKPOINT_EVERY_PAGES == 0:
                    log.info(f"Checkpoint at page {page_count} for chart {chart_name}")
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    op.checkpoint(state=state)

    for chart_name in _chart_names(ledgers, "GL"):
        log.info(f"Syncing GL accounts for chart {chart_name}")
        page_count = 0
        for status in __STATUSES:
            for page in _paged_post(
                "/Api/T1/WS/v1/ChartAccountGL/ListGlAccounts",
                {"ChartName": chart_name, "Status": status},
                "ReadGlAccounts",
                token_holder,
                configuration,
            ):
                for account in page:
                    _emit_account(account, chart_name)
                page_count += 1
                if page_count % __CHECKPOINT_EVERY_PAGES == 0:
                    log.info(f"Checkpoint at page {page_count} for chart {chart_name}")
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    op.checkpoint(state=state)


def _transaction_row(transaction, ledger_name, accnbri):
    """Build the destination row for a single ledger transaction."""
    row = {
        "ldg_trans_rid": _stable_key(
            "glf_ldg_acc_trans",
            ledger_name,
            accnbri,
            transaction.get("TransactionNumber"),
            transaction.get("SequenceNumber"),
            transaction.get("DocumentUniqueId"),
        ),
        "ldg_name": ledger_name,
        "accnbri": accnbri,
        "accnbr": transaction.get("AccountNumberExternal"),
        "trans_nbr": _to_float(transaction.get("TransactionNumber")),
        "seqnbr": _to_int(transaction.get("SequenceNumber")),
        "period": _to_int(transaction.get("Period")),
        "bat_name": transaction.get("BatchName"),
        "doc_type": transaction.get("DocumentType"),
        "doc_datei_1": _to_naive_datetime(transaction.get("DocumentDate1")),
        "doc_datei_2": _to_naive_datetime(transaction.get("DocumentDate2")),
        "doc_datei_3": _to_naive_datetime(transaction.get("DocumentDate3")),
        "doc_datei_4": _to_naive_datetime(transaction.get("DocumentDate4")),
        "doc_ref_1": transaction.get("DocumentReference1"),
        "doc_ref_2": transaction.get("DocumentReference2"),
        "doc_ref_3": transaction.get("DocumentReference3"),
        "source": transaction.get("Source"),
        "source_id": transaction.get("SourceId"),
        "source_datei": _to_naive_datetime(transaction.get("PostingDate")),
        "source_timei": transaction.get("PostingTime"),
        "narr_1": transaction.get("Narration1"),
        "narr_2": transaction.get("Narration2"),
        "narr_3": transaction.get("Narration3"),
        "status": transaction.get("Status"),
        "doc_unique_id": transaction.get("DocumentUniqueId"),
        "attach_ind": _to_flag(transaction.get("HasAttachment")),
        "item_type": transaction.get("ItemType"),
        "item_code": transaction.get("ItemCode"),
        "rate_amt": _to_float(transaction.get("RateAmount")),
        "units_1": _to_float(transaction.get("Units1")),
        "units_2": _to_float(transaction.get("Units2")),
        "units_3": _to_float(transaction.get("Units3")),
        "units_4": _to_float(transaction.get("Units4")),
        "amt_1": _to_float(transaction.get("Amount1")),
        "amt_2": _to_float(transaction.get("Amount2")),
        "amt_3": _to_float(transaction.get("Amount3")),
        "amt_4": _to_float(transaction.get("Amount4")),
        "ccy_code": transaction.get("CurrencyCode"),
        "ccy_amt": _to_float(transaction.get("CurrencyAmount")),
        "exch_rate_amt": _to_float(transaction.get("ExchangeRateAmount")),
        "vat_type": transaction.get("VatRateType"),
        "vat_rate_code": transaction.get("VatRateCode"),
        "vat_rate_amt": _to_float(transaction.get("VatRateAmount")),
        "vat_amt": _to_float(transaction.get("VatAmount")),
        "vat_exc_amt": _to_float(transaction.get("VatExclusiveAmount")),
        "vat_inc_amt": _to_float(transaction.get("VatInclusiveAmount")),
    }
    for i in range(1, 41):
        row[f"seln_type_{i}_code"] = transaction.get(f"SelectionCode{i}")
    return row


def _sync_transaction_details(token_holder, configuration, transaction):
    """Sync the linked debits and credits for a single transaction, if any exist."""
    if transaction["trans_nbr"] is None:
        return

    response = _post(
        "/Api/T1/WS/v1/Ledger/ListLinkedDebitsAndCreditsForTransaction",
        {
            "Items": [
                {
                    "LedgerName": transaction["ldg_name"],
                    "TransactionNumber": transaction["trans_nbr"],
                    "AccountNumber": transaction["accnbri"],
                }
            ]
        },
        token_holder,
        configuration,
    )
    for item in response.get("Items", []):
        for base in item.get("Transactions", []):
            linked_rows = base.get("LinkedDebitsAndCredits", [])
            for index, linked in enumerate(linked_rows, start=1):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(
                    table="glf_ldg_acc_transd",
                    data={
                        "ldg_transd_rid": _stable_key(
                            "glf_ldg_acc_transd",
                            transaction["ldg_name"],
                            transaction["accnbri"],
                            transaction["trans_nbr"],
                            index,
                            linked.get("LedgerName"),
                            linked.get("AccountNumber"),
                            linked.get("DocumentReference1"),
                        ),
                        "ldg_name": transaction["ldg_name"],
                        "accnbri": transaction["accnbri"],
                        "trans_nbr": transaction["trans_nbr"],
                        "seqnbr": index,
                        "period": _to_int(base.get("Period")),
                        "doc_type": base.get("DocumentType"),
                        "doc_datei_1": _to_naive_datetime(base.get("DocumentDate1")),
                        "doc_ref_1": base.get("DocumentReference1"),
                        "source": base.get("Source"),
                        "narr_1": base.get("Narration1"),
                        "item_type": base.get("ItemType"),
                        "item_code": base.get("ItemCode"),
                        "amt_1": _to_float(base.get("Amount")),
                        "vat_amt": _to_float(base.get("VatAmount")),
                        "vat_exc_amt": _to_float(base.get("VatExclusiveAmount")),
                        "linked_ldg_name": linked.get("LedgerName"),
                        "linked_accnbri": linked.get("AccountNumber"),
                        "linked_doc_type": linked.get("DocumentType"),
                        "linked_doc_datei_1": _to_naive_datetime(linked.get("DocumentDate1")),
                        "linked_doc_ref_1": linked.get("DocumentReference1"),
                        "linked_source": linked.get("Source"),
                        "linked_amt_1": _to_float(linked.get("Amount")),
                        "linked_vat_amt": _to_float(linked.get("VatAmount")),
                        "linked_vat_exc_amt": _to_float(linked.get("VatExclusiveAmount")),
                    },
                )


def _sync_transactions_for_account(token_holder, configuration, ledger_name, accnbri, state):
    """Sync transactions (and their linked debits/credits) for a single ledger account.

    Checkpoints periodically (every __CHECKPOINT_EVERY_PAGES pages) so a large transaction
    history does not build up an unbounded unflushed write batch before the next checkpoint.
    """
    page_count = 0
    for page in _paged_post(
        "/Api/T1/WS/v1/Ledger/ListTransactionsByLedgerName",
        {"PageResult": True, "LedgerName": ledger_name, "AccountNumber": accnbri},
        "Transactions",
        token_holder,
        configuration,
    ):
        for transaction in page:
            row = _transaction_row(transaction, ledger_name, accnbri)
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table="glf_ldg_acc_trans", data=row)
            _sync_transaction_details(token_holder, configuration, row)
        page_count += 1
        if page_count % __CHECKPOINT_EVERY_PAGES == 0:
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            op.checkpoint(state=state)


def _sync_ledger_accounts_and_transactions(token_holder, configuration, ledgers, state):
    """Sync each ledger account and, immediately after, its transactions.

    Each account's transactions are synced as soon as the account itself is upserted, instead
    of first collecting every account for the whole tenant into memory. This keeps memory use
    bounded by one ledger/status page at a time regardless of how many accounts the tenant has.
    """
    account_count = 0
    for ledger in ledgers:
        ledger_name = ledger.get("LedgerName")
        if not ledger_name:
            continue
        for status in __STATUSES:
            response = _post(
                "/Api/T1/WS/v1/Ledger/ListAccountsByLedgerName",
                {
                    "Items": [
                        {
                            "LedgerName": ledger_name,
                            "Status": status,
                            "Search": "",
                            "AccountsWithABalance": False,
                        }
                    ]
                },
                token_holder,
                configuration,
            )
            for item in response.get("Items", []):
                for account in item.get("LedgerAccounts", []):
                    accnbri = account.get("AccountNumberInternal") or account.get("AccountNumber")
                    if not accnbri:
                        continue
                    descr = account.get("AccountName2") or account.get("AccountName")
                    row = {
                        "ldg_acct_rid": _stable_key("glf_ldg_acct", ledger_name, accnbri),
                        "ldg_name": ledger_name,
                        "chart_name": account.get("ChartName"),
                        "accnbri": accnbri,
                        "accnbr": account.get("AccountNumberExternal"),
                        "stat_ind": account.get("Status"),
                        "bal_units_1": _to_float(account.get("BalanceUnits1")),
                        "bal_amt_1": _to_float(account.get("BalanceAmount1")),
                        "commitment_total": _to_float(account.get("CommitmentTotal")),
                        "total_balance": _to_float(account.get("TotalBalance")),
                        "descr": descr,
                        "sdescr": account.get("AccountName"),
                    }
                    # The 'upsert' operation is used to insert or update data in the destination table.
                    # The first argument is the name of the destination table.
                    # The second argument is a dictionary containing the record to be upserted.
                    op.upsert(table="glf_ldg_acct", data=row)
                    account_count += 1
                    _sync_transactions_for_account(
                        token_holder, configuration, ledger_name, accnbri, state
                    )
    log.info(f"Synced {account_count} ledger accounts across all ledgers")


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
    log.warning("Example: Databases : TechOne CiAnywhere")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration)

    token_holder = [_get_token(configuration)]

    # glf_ldg_ctl — all ledgers
    ledgers = _list_ledgers(token_holder, configuration)
    for ledger in ledgers:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(
            table="glf_ldg_ctl",
            data={
                "ldg_name": ledger.get("LedgerName"),
                "descr": ledger.get("Description"),
                "chart_name": ledger.get("ChartName"),
                "stat_ind": ledger.get("Status"),
                "system_code": ledger.get("SystemProfileCode"),
                "chart_type": ledger.get("ChartType"),
            },
        )

    # glf_chart_acct + glf_chart_acc_usf — AR + GL accounts
    _sync_chart_accounts(token_holder, configuration, ledgers, state)

    # glf_ldg_acct + glf_ldg_acc_trans + glf_ldg_acc_transd — ledger accounts, their
    # transactions, and linked debits/credits, synced together account by account.
    _sync_ledger_accounts_and_transactions(token_holder, configuration, ledgers, state)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    op.checkpoint(state=state)


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
    connector.debug()
