"""
TechOne CiAnywhere Fivetran Connector.

Syncs general ledger reference and transaction data from the TechOne CiAnywhere
web services API (ledgers, chart of accounts, ledger accounts, and transactions).
"""

import hashlib
import time

import requests
from fivetran_connector_sdk import Connector, Logging as log, Operations as op

__TOKEN_PATH = "/oauth2/access_token"
__PAGE_SIZE = 100
__STATUSES = ("A", "I")
__RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
__CHECKPOINT_EVERY_PAGES = 10


def schema(configuration: dict):
    """Define ledger, chart-of-accounts, ledger account, and transaction tables and their primary keys."""
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


def _get_token(configuration):
    """Request a new OAuth2 client-credentials access token from TechOne."""
    response = requests.post(
        f"{configuration['base_url']}{__TOKEN_PATH}",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "client_id": configuration["client_id"],
            "client_secret": configuration["client_secret"],
            "grant_type": "client_credentials",
        },
        timeout=30,
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
    """Issue an HTTP request with token refresh on 401 and retry/backoff on transient errors."""
    url = f"{configuration['base_url']}{path}"
    token_refreshed = False
    for attempt in range(1, 4):
        try:
            response = requests.request(
                method, url, headers=_headers(token_holder[0]), timeout=60, **kwargs
            )
        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ) as exc:
            if attempt < 3:
                log.warning(
                    f"{method} {path} attempt {attempt} failed ({exc}), retrying in {2**attempt}s"
                )
                time.sleep(2**attempt)
                continue
            raise
        if response.status_code == 401 and not token_refreshed:
            log.warning(f"{method} {path} got 401, refreshing token and retrying")
            token_holder[0] = _get_token(configuration)
            token_refreshed = True
            continue
        if response.status_code in __RETRY_STATUS_CODES and attempt < 3:
            retry_after = response.headers.get("Retry-After")
            time.sleep(float(retry_after) if retry_after else 2**attempt)
            continue
        response.raise_for_status()
        return response.json() if response.content else {}
    raise RuntimeError(f"{method} {path} failed after retries")


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
    """POST to a paginated list endpoint, yielding each page of rows until a short page ends it."""
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
    """Upsert a chart-of-accounts row and, if populated, its user-defined-fields row."""
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
        "vat_type_def": account.get("VateRateTypeDefault"),
        "vat_rate_code_def": account.get("VatRateCodeDefault"),
        "frgn_ccy_ind": _to_flag(account.get("IsForeignCurrency")),
        "ccy_code": account.get("CurrencyCode"),
        "exch_rate_table_name": account.get("ExchangeRateTableName"),
    }
    for i in range(1, 41):
        row[f"seln_type_{i}_code"] = account.get(f"SelectionCode{i}")
    yield op.upsert("glf_chart_acct", row)

    usf = {
        "chart_name": chart_name,
        "accnbri": accnbri,
        "vers": _to_int(account.get("Vers")),
    }
    has_user_fields = False
    for i in range(1, 13):
        alpha = account.get(f"Userfield{i}Alpha")
        numeric = account.get(f"Userfield{i}Numeric")
        date_value = account.get(f"Userfield{i}Date")
        usf[f"user_fld_{i}"] = alpha
        usf[f"user_num_{i}"] = _to_float(numeric)
        usf[f"user_datei_{i}"] = _to_naive_datetime(date_value)
        has_user_fields = has_user_fields or bool(alpha or numeric or date_value)
    if has_user_fields:
        yield op.upsert("glf_chart_acc_usf", usf)


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
                    yield from _emit_account(debtor, chart_name)
                page_count += 1
                if page_count % __CHECKPOINT_EVERY_PAGES == 0:
                    log.info(f"Checkpoint at page {page_count} for chart {chart_name}")
                    yield op.checkpoint(state)

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
                    yield from _emit_account(account, chart_name)
                page_count += 1
                if page_count % __CHECKPOINT_EVERY_PAGES == 0:
                    log.info(f"Checkpoint at page {page_count} for chart {chart_name}")
                    yield op.checkpoint(state)


def _sync_ledger_accounts(token_holder, configuration, ledgers):
    """Sync per-ledger accounts and return the synced rows for use by the transaction sync."""
    ledger_accounts = []
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
                    ledger_accounts.append(row)
                    yield op.upsert("glf_ldg_acct", row)
    return ledger_accounts


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


def _sync_transactions(token_holder, configuration, ledger_accounts):
    """Sync transactions for every ledger account, including their linked debits/credits."""
    for account in ledger_accounts:
        ledger_name = account["ldg_name"]
        accnbri = account["accnbri"]
        for page in _paged_post(
            "/Api/T1/WS/v1/Ledger/ListTransactionsByLedgerName",
            {"PageResult": True, "LedgerName": ledger_name, "AccountNumber": accnbri},
            "Transactions",
            token_holder,
            configuration,
        ):
            for transaction in page:
                row = _transaction_row(transaction, ledger_name, accnbri)
                yield op.upsert("glf_ldg_acc_trans", row)
                yield from _sync_transaction_details(token_holder, configuration, row)


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
                yield op.upsert(
                    "glf_ldg_acc_transd",
                    {
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


def update(configuration: dict, state: dict):
    """Sync ledgers, chart of accounts, ledger accounts, and transactions from TechOne."""
    token_holder = [_get_token(configuration)]

    # glf_ldg_ctl — all ledgers
    ledgers = _list_ledgers(token_holder, configuration)
    for ledger in ledgers:
        yield op.upsert(
            "glf_ldg_ctl",
            {
                "ldg_name": ledger.get("LedgerName"),
                "descr": ledger.get("Description"),
                "chart_name": ledger.get("ChartName"),
                "stat_ind": ledger.get("Status"),
                "system_code": ledger.get("SystemProfileCode"),
                "chart_type": ledger.get("ChartType"),
            },
        )

    # glf_chart_acct + glf_chart_acc_usf — AR + GL accounts
    yield from _sync_chart_accounts(token_holder, configuration, ledgers, state)

    # glf_ldg_acct — ledger accounts
    ledger_accounts = []
    account_stream = _sync_ledger_accounts(token_holder, configuration, ledgers)
    while True:
        try:
            yield next(account_stream)
        except StopIteration as done:
            ledger_accounts = done.value or []
            break
    log.info(f"Synced {len(ledger_accounts)} ledger accounts across all ledgers")

    # glf_ldg_acc_trans + glf_ldg_acc_transd — transactions + linked debits/credits
    yield from _sync_transactions(token_holder, configuration, ledger_accounts)

    yield op.checkpoint(state)


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
