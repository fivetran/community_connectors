import json
import os
import sys
from datetime import datetime, timezone

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import connector as ct  # noqa: E402
from mock_api import GOOD_KEY, MockApi  # noqa: E402

# The SDK sets the log level when it runs a connector. Tests call update directly.
ct.log.LOG_LEVEL = ct.log.Level.INFO

NOW = datetime(2026, 10, 30, 12, 0, 0, tzinfo=timezone.utc)
ROWS = 12


@pytest.fixture
def mock():
    api = MockApi().start()
    yield api
    api.stop()


@pytest.fixture
def out(monkeypatch):
    captured = {"upserts": [], "checkpoints": []}

    def upsert(table, data):
        captured["upserts"].append((table, data))

    def checkpoint(state):
        captured["checkpoints"].append(json.loads(json.dumps(state)))

    monkeypatch.setattr(ct.op, "upsert", upsert)
    monkeypatch.setattr(ct.op, "checkpoint", checkpoint)
    monkeypatch.setattr(ct.time, "sleep", lambda _: None)
    monkeypatch.setattr(ct.api_get, "__defaults__", (None, lambda _: None))
    monkeypatch.setattr(ct.sync_table, "__defaults__", (lambda _: None,))
    return captured


def config(mock, **extra):
    return {"api_key": GOOD_KEY, "base_url": mock.base_url, **extra}


def sent(captured, table):
    return [row for name, row in captured["upserts"] if name == table]


def test_first_sync_sends_every_row_of_every_table(mock, out):
    state = {}
    ct.update(config(mock), state)
    assert {name for name, _ in out["upserts"]} == {table["name"] for table in ct.TABLES}
    assert len(sent(out, "products")) == 3
    assert len(sent(out, "stock_movements")) == 2
    assert set(state["tables"]) == {table["name"] for table in ct.TABLES}
    for entry in state["tables"].values():
        assert "page_cursor" not in entry
        assert "walk_started" not in entry
        assert ct.parse_time(entry["cursor"]) is not None


def test_rows_are_shaped_for_the_destination(mock, out):
    ct.update(config(mock), {})
    work_order = sent(out, "work_orders")[0]
    assert work_order["created_at"] == "2026-01-05T10:00:00Z"
    assert work_order["updated_at"] == "2026-10-29T10:00:00Z"
    assert work_order["completed_at"] == "2026-10-29T10:00:00Z"
    assert json.loads(work_order["line_items"])[0]["description"] == "Spring"
    assert "lineItems" not in work_order
    invoice = sent(out, "invoices")[0]
    assert invoice["due_date"] == "2026-11-15"
    assert invoice["invoice_number"] == "INV-1"
    assert json.loads(sent(out, "vehicles")[0]["inventory"])[0]["productId"] == "p1"


def test_second_sync_sends_only_changed_rows_and_keeps_snapshots(mock, out):
    state = {"tables": {t["name"]: {"cursor": "2026-10-01T00:00:00Z"} for t in ct.TABLES}}
    ct.update(config(mock), state)
    # p2 changed after the cursor, p3 has no date, p1 is old.
    assert sorted(row["id"] for row in sent(out, "products")) == ["p2", "p3"]
    assert sent(out, "estimates") == []
    assert sent(out, "purchase_orders") == []
    assert [row["id"] for row in sent(out, "work_orders")] == ["wo1"]
    assert [row["id"] for row in sent(out, "stock_movements")] == ["m1"]
    assert sent(out, "transfers") == []
    assert len(sent(out, "customers")) == 1
    assert len(sent(out, "vehicles")) == 1


def test_overlap_window_catches_a_row_just_before_the_cursor(mock, out):
    state = {"tables": {"products": {"cursor": "2026-10-29T10:05:00Z"}}}
    ct.sync_table(ct.make_session(GOOD_KEY), mock.base_url, ct.TABLES[0], state, lambda: NOW)
    assert "p2" in [row["id"] for row in sent(out, "products")]


def test_cursor_advances_to_walk_start_not_to_the_newest_row(mock, out):
    state = {}
    ct.sync_table(ct.make_session(GOOD_KEY), mock.base_url, ct.TABLES[0], state, lambda: NOW)
    assert state["tables"]["products"]["cursor"] == "2026-10-30T12:00:00Z"


def many_products(mock, count):
    mock.data["products"] = [
        {"id": "p" + str(i), "name": "Item", "updatedAt": "2026-10-29T00:00:00Z"}
        for i in range(count)
    ]


def test_pages_are_walked_and_checkpointed_about_every_ten_pages(mock, out):
    many_products(mock, 2550)
    state = {}
    ct.sync_table(ct.make_session(GOOD_KEY), mock.base_url, ct.TABLES[0], state, lambda: NOW)
    assert len(sent(out, "products")) == 2550
    assert len(mock.list_requests("products")) == 26
    # Pages 10 and 20, and the end of the table.
    assert len(out["checkpoints"]) == 3
    assert "page_cursor" in out["checkpoints"][0]["tables"]["products"]


def test_monthly_allowance_stops_without_moving_the_cursor_and_resumes(mock, out):
    many_products(mock, 1500)
    session = ct.make_session(GOOD_KEY)
    state = {"tables": {"products": {"cursor": "2026-01-01T00:00:00Z"}}}
    allowance = {
        "status": 429,
        "body": {"error": "monthly_api_allowance_exhausted", "message": "Allowance reached."},
        "headers": {"Retry-After": "86400"},
    }
    original = mock.list_requests

    class StopAfter:
        def __init__(self):
            self.calls = 0

        def __call__(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 12:
                mock.faults.append(allowance)
            return real_get(*args, **kwargs)

    real_get = session.get
    session.get = StopAfter()
    with pytest.raises(ct.CubeTallyError, match="monthly API allowance"):
        ct.sync_table(session, mock.base_url, ct.TABLES[0], state, lambda: NOW)
    saved = out["checkpoints"][-1]["tables"]["products"]
    assert saved["cursor"] == "2026-01-01T00:00:00Z"
    assert saved["page_cursor"]
    assert saved["walk_started"] == "2026-10-30T12:00:00Z"

    # The next sync starts from the saved page and keeps the first walk's start.
    resumed = json.loads(json.dumps(out["checkpoints"][-1]))
    mock.requests.clear()
    later = datetime(2026, 11, 1, tzinfo=timezone.utc)
    ct.sync_table(ct.make_session(GOOD_KEY), mock.base_url, ct.TABLES[0], resumed, lambda: later)
    assert original("products")[0]["query"]["cursor"] == [saved["page_cursor"]]
    assert resumed["tables"]["products"]["cursor"] == "2026-10-30T12:00:00Z"


def test_rate_limit_is_retried_and_honors_retry_after(mock, out):
    waits = []
    mock.faults.append(
        {
            "status": 429,
            "body": {"error": "rate_limited", "message": "Slow down."},
            "headers": {"Retry-After": "7"},
        }
    )
    body = ct.api_get(
        ct.make_session(GOOD_KEY), mock.base_url, "products", {"limit": 100}, waits.append
    )
    assert waits == [7.0]
    assert len(body["data"]) == 3


def test_server_errors_are_retried_then_fail_clearly(mock, out):
    for _ in range(ct.MAX_ATTEMPTS):
        mock.faults.append({"status": 503, "body": {"error": "unavailable", "message": "x"}})
    with pytest.raises(ct.CubeTallyError, match="after retries: HTTP 503"):
        ct.api_get(ct.make_session(GOOD_KEY), mock.base_url, "products", {}, lambda _: None)


def test_deleted_page_anchor_restarts_the_walk(mock, out):
    state = {"tables": {"products": {"page_cursor": "Z29uZQ"}}}
    ct.sync_table(ct.make_session(GOOD_KEY), mock.base_url, ct.TABLES[0], state, lambda: NOW)
    assert len(sent(out, "products")) == 3
    assert "page_cursor" not in state["tables"]["products"]


def test_endless_invalid_cursor_gives_up(mock, out):
    for _ in range(ct.MAX_CURSOR_RESTARTS + 1):
        mock.faults.append(
            {"status": 400, "body": {"error": "invalid_request", "message": "Invalid cursor."}}
        )
    with pytest.raises(ct.CubeTallyError, match="repeatedly"):
        ct.sync_table(ct.make_session(GOOD_KEY), mock.base_url, ct.TABLES[0], {}, lambda: NOW)


def test_tables_without_a_scope_are_skipped(mock, out):
    mock.scopes = ["products:read"]
    state = {}
    ct.update(config(mock), state)
    assert {name for name, _ in out["upserts"]} == {"products", "stock_movements"}
    assert mock.list_requests("invoices") == []


def test_a_key_with_no_usable_scope_fails(mock, out):
    mock.scopes = ["webhooks:read"]
    with pytest.raises(ct.CubeTallyError, match="no read scope"):
        ct.update(config(mock), {})


def test_tables_setting_limits_the_sync(mock, out):
    ct.update(config(mock, tables="invoices, customers"), {})
    assert {name for name, _ in out["upserts"]} == {"invoices", "customers"}
    with pytest.raises(ValueError, match="Unknown tables: nope"):
        ct.update(config(mock, tables="nope"), {})


def test_bad_key_is_rejected_and_never_appears_in_the_error(mock, out):
    bad = "ct_live_notarealkey"
    with pytest.raises(ct.CubeTallyError) as error:
        ct.update({"api_key": bad, "base_url": mock.base_url}, {})
    assert bad not in str(error.value)
    with pytest.raises(ValueError):
        ct.validate_configuration({"api_key": "sk_live_abc"})


def test_only_https_or_a_local_host_is_allowed():
    with pytest.raises(ValueError, match="https"):
        ct.validate_configuration({"api_key": GOOD_KEY, "base_url": "http://cubetally.com/v1"})
    assert ct.validate_configuration({"api_key": GOOD_KEY})[1] == ct.DEFAULT_BASE_URL
    assert ct.validate_configuration(
        {"api_key": GOOD_KEY, "base_url": "http://localhost:9/v1/"}
    ) == (GOOD_KEY, "http://localhost:9/v1")


def test_the_connector_only_sends_get(mock, out):
    ct.update(config(mock), {})
    assert set(mock.methods) == {"GET"}
    with open(ct.__file__, encoding="utf-8") as source:
        text = source.read()
    for verb in ("session.post", "session.put", "session.patch", "session.delete"):
        assert verb not in text


def test_schema_declares_a_primary_key_for_every_table():
    tables = ct.schema({})
    assert len(tables) == len(ct.TABLES)
    assert all(table["primary_key"] == ["id"] for table in tables)


def test_connection_test_reports_success_and_failure(mock):
    assert ct.connection_test(config(mock)) == ct.Test().success()
    failed = ct.connection_test({"api_key": "wrong"})
    assert failed != ct.Test().success()


def test_snake_case_and_dates():
    assert ct.snake_case("poNumber") == "po_number"
    assert ct.snake_case("id") == "id"
    assert ct.parse_time("2026-11-15").tzinfo is not None
    assert ct.parse_time("next week") is None
    assert ct.parse_time(None) is None
    assert ct.transform({"createdAt": "garbage"}, ct.CREATED_ONLY) == {"created_at": None}


def test_sample_fixture_size_is_known():
    assert sum(len(rows) for rows in MockApi().data.values()) == ROWS + 2
