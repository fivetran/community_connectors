"""Fivetran Connector SDK source connector for CubeTally.

Read only. It sends GET requests to the CubeTally public REST API with one API
key, so everything it can see belongs to the single organization that key was
created in. It never writes to CubeTally.

The API has no updated-since filter and no delete feed, so every table is walked
with its cursor on every sync. Rows are then filtered locally:

    filtered   products, purchase orders, work orders, invoices, estimates
               Rows are sent when updated_at is at or after the last sync.
    append     stock movements, transfers
               Rows are sent when created_at is at or after the last sync.
    snapshot   customers, suppliers, warehouses, vehicles
               Every row is sent, because these records have no reliable
               changed date. Fivetran counts a row once per month.

A row with no usable date is always sent. Deleted CubeTally records are not
detected. See README.md.
"""

import json
import re
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

import requests
from fivetran_connector_sdk import ConfigurationForm, Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Test, form_field

DEFAULT_BASE_URL = "https://cubetally.com/v1"
USER_AGENT = "cubetally-fivetran-connector/1.0"
PAGE_SIZE = 100
REQUEST_TIMEOUT_SECONDS = 30
MAX_ATTEMPTS = 6
MAX_WAIT_SECONDS = 120
MAX_CURSOR_RESTARTS = 3
CHECKPOINT_EVERY_PAGES = 10
OVERLAP = timedelta(minutes=10)

KEY_PATTERN = re.compile(r"^ct_(live|test)_[A-Za-z0-9_-]+$")
LOCAL_HOSTS = ("localhost", "127.0.0.1")

COMMON_DATES = {"created_at": "UTC_DATETIME", "updated_at": "UTC_DATETIME"}
CREATED_ONLY = {"created_at": "UTC_DATETIME"}

# name: table name in the destination. path: list route. scope: key scope that
# route needs. mode: how rows are filtered. columns: declared types, the rest
# are inferred by Fivetran.
TABLES = (
    {
        "name": "products",
        "path": "products",
        "scope": "products:read",
        "mode": "filtered",
        "columns": COMMON_DATES,
    },
    {
        "name": "customers",
        "path": "customers",
        "scope": "customers:read",
        "mode": "snapshot",
        "columns": CREATED_ONLY,
    },
    {
        "name": "suppliers",
        "path": "suppliers",
        "scope": "suppliers:read",
        "mode": "snapshot",
        "columns": CREATED_ONLY,
    },
    {
        "name": "warehouses",
        "path": "warehouses",
        "scope": "warehouses:read",
        "mode": "snapshot",
        "columns": CREATED_ONLY,
    },
    {
        "name": "vehicles",
        "path": "vehicles",
        "scope": "vehicles:read",
        "mode": "snapshot",
        "columns": {"inventory": "JSON"},
    },
    {
        "name": "purchase_orders",
        "path": "purchase-orders",
        "scope": "purchaseOrders:read",
        "mode": "filtered",
        "columns": {**COMMON_DATES, "line_items": "JSON"},
    },
    {
        "name": "work_orders",
        "path": "work-orders",
        "scope": "workOrders:read",
        "mode": "filtered",
        "columns": {**COMMON_DATES, "completed_at": "UTC_DATETIME", "line_items": "JSON"},
    },
    {
        "name": "invoices",
        "path": "invoices",
        "scope": "invoices:read",
        "mode": "filtered",
        "columns": {**COMMON_DATES, "line_items": "JSON"},
    },
    {
        "name": "estimates",
        "path": "estimates",
        "scope": "estimates:read",
        "mode": "filtered",
        "columns": {**COMMON_DATES, "line_items": "JSON"},
    },
    {
        "name": "stock_movements",
        "path": "stock-movements",
        "scope": "products:read",
        "mode": "append",
        "columns": CREATED_ONLY,
    },
    {
        "name": "transfers",
        "path": "transfers",
        "scope": "vehicles:read",
        "mode": "append",
        "columns": CREATED_ONLY,
    },
)


class CubeTallyError(Exception):
    """A failure with a message safe to show. It never contains the API key."""


class InvalidCursor(Exception):
    """The record a page cursor pointed at was deleted. The walk must restart."""


def snake_case(name):
    """Convert a camelCase API field name to snake_case."""
    return re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name).lower()


def parse_time(value):
    """Return an aware UTC datetime, or None when the value is not a date."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_time(value):
    """Format a datetime the way Fivetran expects a UTC_DATETIME string."""
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def validate_configuration(configuration):
    """Return (api_key, base_url), or raise a clear error."""
    api_key = str(configuration.get("api_key") or "").strip()
    if not KEY_PATTERN.match(api_key):
        raise ValueError("api_key must be a CubeTally key that starts with ct_live_ or ct_test_.")
    base_url = str(configuration.get("base_url") or DEFAULT_BASE_URL).strip().rstrip("/")
    parts = urlsplit(base_url)
    if parts.scheme != "https" and parts.hostname not in LOCAL_HOSTS:
        raise ValueError("base_url must use https.")
    if not parts.hostname:
        raise ValueError("base_url is not a valid address.")
    return api_key, base_url


def selected_tables(configuration):
    """Apply the optional comma separated tables setting."""
    wanted = str(configuration.get("tables") or "").strip()
    if not wanted:
        return list(TABLES)
    names = {name.strip() for name in wanted.split(",") if name.strip()}
    unknown = sorted(names - {table["name"] for table in TABLES})
    if unknown:
        raise ValueError("Unknown tables: " + ", ".join(unknown))
    return [table for table in TABLES if table["name"] in names]


def wait_seconds(response, attempt):
    """Honor Retry-After when present, otherwise back off."""
    header = response.headers.get("Retry-After") if response is not None else None
    try:
        wait = float(header)
    except (TypeError, ValueError):
        wait = 2.0**attempt
    return max(0.0, min(wait, MAX_WAIT_SECONDS))


def error_code(response):
    try:
        body = response.json()
    except ValueError:
        return "", ""
    if not isinstance(body, dict):
        return "", ""
    return str(body.get("error") or ""), str(body.get("message") or "")


def api_get(session, base_url, path, params=None, sleep=time.sleep):
    """GET one API path with retries. This connector sends no other method."""
    url = base_url + "/" + path
    last_problem = "no response"
    for attempt in range(MAX_ATTEMPTS):
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException as error:
            # The exception type only: its text can carry request details.
            last_problem = type(error).__name__
            sleep(wait_seconds(None, attempt))
            continue
        if response.status_code == 200:
            return response.json()
        code, message = error_code(response)
        if response.status_code == 429 and code == "monthly_api_allowance_exhausted":
            raise CubeTallyError(
                "The CubeTally monthly API allowance is used up. Progress is saved and the "
                "next sync picks up where this one stopped. Reduce the sync frequency or "
                "add capacity in CubeTally."
            )
        if response.status_code == 429 or response.status_code >= 500:
            last_problem = "HTTP " + str(response.status_code)
            sleep(wait_seconds(response, attempt))
            continue
        if response.status_code == 400 and "cursor" in message.lower():
            raise InvalidCursor()
        if response.status_code == 401:
            raise CubeTallyError("CubeTally rejected the API key. Check or replace it.")
        raise CubeTallyError(
            f"CubeTally refused {path} ({response.status_code} {code}): {message}"
        )
    raise CubeTallyError("CubeTally did not answer " + path + " after retries: " + last_problem)


def transform(row, columns):
    """Convert an API record to a destination row of the declared column types."""
    out = {}
    for key, value in row.items():
        name = snake_case(key)
        kind = columns.get(name)
        if kind == "UTC_DATETIME":
            parsed = parse_time(value)
            out[name] = format_time(parsed) if parsed else None
        elif kind == "JSON" or isinstance(value, (list, dict)):
            out[name] = json.dumps(value)
        else:
            out[name] = value
    return out


def should_send(row, mode, since):
    """Decide whether a record goes to the destination this sync."""
    if mode == "snapshot" or since is None:
        return True
    field = "updatedAt" if mode == "filtered" else "createdAt"
    stamp = parse_time(row.get(field))
    # An unusable date must not hide a record.
    return stamp is None or stamp >= since


def sync_table(session, base_url, table, state, now, sleep=time.sleep):
    """Walk one table. Progress is kept in state so a stopped sync can resume."""
    tables_state = state.setdefault("tables", {})
    entry = tables_state.setdefault(table["name"], {})
    # The cursor moves only after a full walk, to the time that walk began, so a
    # change made during the walk is picked up by the next sync.
    started = entry.get("walk_started") or format_time(now())
    entry["walk_started"] = started
    last = parse_time(entry.get("cursor"))
    since = last - OVERLAP if last else None
    page_cursor = entry.get("page_cursor")
    pages = 0
    sent = 0
    restarts = 0
    while True:
        params = {"limit": PAGE_SIZE}
        if page_cursor:
            params["cursor"] = page_cursor
        try:
            body = api_get(session, base_url, table["path"], params, sleep)
        except InvalidCursor:
            restarts += 1
            if restarts > MAX_CURSOR_RESTARTS:
                raise CubeTallyError(
                    "CubeTally rejected the page cursor for " + table["name"] + " repeatedly."
                )
            log.warning("Restarting " + table["name"] + ": the page cursor is no longer valid.")
            page_cursor = None
            entry.pop("page_cursor", None)
            continue
        for row in body.get("data") or []:
            if should_send(row, table["mode"], since):
                op.upsert(table=table["name"], data=transform(row, table["columns"]))
                sent += 1
        pages += 1
        page_cursor = body.get("nextCursor")
        if not page_cursor:
            break
        entry["page_cursor"] = page_cursor
        if pages % CHECKPOINT_EVERY_PAGES == 0:
            op.checkpoint(state=state)
    entry["cursor"] = started
    entry.pop("page_cursor", None)
    entry.pop("walk_started", None)
    op.checkpoint(state=state)
    log.info(table["name"] + ": sent " + str(sent) + " rows from " + str(pages) + " pages.")
    return sent


def make_session(api_key):
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": "Bearer " + api_key,
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        }
    )
    return session


def tables_for_key(whoami, tables):
    """Split tables into those the key can read and those it cannot."""
    scopes = set(whoami.get("scopes") or [])
    usable = [table for table in tables if table["scope"] in scopes]
    skipped = [table for table in tables if table["scope"] not in scopes]
    return usable, skipped


def schema(configuration):
    """Declare primary keys and the columns whose type must not be inferred."""
    return [
        {"table": table["name"], "primary_key": ["id"], "columns": table["columns"]}
        for table in selected_tables(configuration)
    ]


def update(configuration, state):
    """Run one sync."""
    api_key, base_url = validate_configuration(configuration)
    tables = selected_tables(configuration)
    with make_session(api_key) as session:
        whoami = api_get(session, base_url, "whoami")
        usable, skipped = tables_for_key(whoami, tables)
        for table in skipped:
            log.warning("Skipping " + table["name"] + ": the key lacks " + table["scope"] + ".")
        if not usable:
            raise CubeTallyError("The API key has no read scope for any selected table.")
        for table in usable:
            sync_table(session, base_url, table, state, lambda: datetime.now(timezone.utc))


def connection_test(configuration):
    """Setup form check: confirm the key works and show what it can read."""
    try:
        api_key, base_url = validate_configuration(configuration)
        with make_session(api_key) as session:
            whoami = api_get(session, base_url, "whoami", sleep=lambda _: None)
        usable, skipped = tables_for_key(whoami, list(TABLES))
    except (ValueError, CubeTallyError) as error:
        return Test().failure(str(error))
    if not usable:
        return Test().failure("The key works but has no read scope for any table.")
    log.info("Connected. Can read " + str(len(usable)) + " of " + str(len(TABLES)) + " tables.")
    if skipped:
        log.warning("Add scopes to read: " + ", ".join(table["name"] for table in skipped) + ".")
    return Test().success()


def configuration_form():
    form = ConfigurationForm()
    form.add_field(
        form_field.TextField(
            "api_key",
            "CubeTally API key",
            form_field.TextField.password,
            description="Create it in CubeTally under Settings, Developer. A ct_test_ key "
            "with read scopes reads real data and cannot write.",
            required=True,
            placeholder="ct_test_...",
        )
    )
    form.add_field(
        form_field.TextField(
            "tables",
            "Tables",
            description="Optional. A comma separated list. Leave empty to sync every table "
            "the key can read.",
        )
    )
    form.add_test(label="Test connection", func=connection_test)
    return form


connector = Connector(update=update, schema=schema, configuration_form=configuration_form)

if __name__ == "__main__":
    connector.debug()
