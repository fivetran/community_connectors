from datetime import datetime

import connector


def test_schema_contains_customers_definition():
    result = connector.schema({})
    assert len(result) == 1
    assert result[0]["table"] == "customers"
    assert result[0]["primary_key"] == ["customer_id"]
    assert result[0]["columns"]["customer_id"] == "INT"
    assert result[0]["columns"]["updated_at"] == "UTC_DATETIME"


def test_dt2str_formats_timestamp():
    dt = datetime(2024, 1, 31, 23, 4, 39)
    assert connector.dt2str(dt) == "2024-01-31T23:04:39Z"


def test_connect_to_redshift_passes_configuration(monkeypatch):
    calls = []

    def fake_connect(**kwargs):
        calls.append(kwargs)
        return "conn"

    monkeypatch.setattr(connector.redshift_connector, "connect", fake_connect)

    config = {
        "host": "example",
        "database": "db",
        "port": 5439,
        "user": "user",
        "password": "pass",
    }
    result = connector.connect_to_redshift(config)

    assert result == "conn"
    assert calls == [config]


def test_update_upserts_rows_and_checkpoints(monkeypatch):
    upserts = []
    checkpoints = []
    executed_queries = []

    class Cursor:
        def __init__(self):
            self._batches = [
                [
                    (2, "Joe", "Doe", "joe@fivetran.com", datetime(2024, 1, 31, 23, 4, 39)),
                    (4, "John", "William", "john@fivetran.com", datetime(2024, 2, 14, 22, 59, 59)),
                ],
                [
                    (5, "Ricky", "Roma", "ricky@fivetran.com", datetime(2024, 3, 16, 16, 40, 29)),
                ],
                [],
            ]

        def execute(self, query):
            executed_queries.append(query)

        def fetchmany(self, _size):
            return self._batches.pop(0)

    class Conn:
        @staticmethod
        def cursor():
            return Cursor()

    monkeypatch.setattr(connector, "connect_to_redshift", lambda _configuration: Conn())
    monkeypatch.setattr(connector.log, "warning", lambda _message: None)
    monkeypatch.setattr(connector.log, "debug", lambda _message: None)
    monkeypatch.setattr(
        connector.op,
        "upsert",
        lambda **kwargs: upserts.append((kwargs["table"], kwargs["data"])),
    )
    monkeypatch.setattr(
        connector.op,
        "checkpoint",
        lambda state: checkpoints.append(state.copy()),
    )

    state = {}
    connector.update(
        {
            "host": "example",
            "database": "db",
            "port": 5439,
            "user": "user",
            "password": "pass",
        },
        state,
    )

    assert len(executed_queries) == 1
    assert "WHERE updated_at > '2024-01-01T00:00:00Z'" in executed_queries[0]
    assert len(upserts) == 3
    assert upserts[0][0] == "customers"
    assert upserts[-1][1]["updated_at"] == "2024-03-16T16:40:29Z"
    assert checkpoints == [
        {"last_updated_at": "2024-02-14T22:59:59Z"},
        {"last_updated_at": "2024-03-16T16:40:29Z"},
    ]
    assert state["last_updated_at"] == "FALSE"


def test_update_uses_existing_state_cursor(monkeypatch):
    executed_queries = []

    class Cursor:
        @staticmethod
        def execute(query):
            executed_queries.append(query)

        @staticmethod
        def fetchmany(_size):
            return []

    class Conn:
        @staticmethod
        def cursor():
            return Cursor()

    monkeypatch.setattr(connector, "connect_to_redshift", lambda _configuration: Conn())
    monkeypatch.setattr(connector.log, "warning", lambda _message: None)
    monkeypatch.setattr(connector.log, "debug", lambda _message: None)

    state = {"last_updated_at": "2025-01-01T00:00:00Z"}
    connector.update(
        {
            "host": "example",
            "database": "db",
            "port": 5439,
            "user": "user",
            "password": "pass",
        },
        state,
    )

    assert len(executed_queries) == 1
    assert "WHERE updated_at > '2025-01-01T00:00:00Z'" in executed_queries[0]
