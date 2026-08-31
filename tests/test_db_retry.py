import psycopg2
import pytest


def test_db_operation_discards_broken_connection_and_retries_once(app_module, monkeypatch):
    app = app_module
    first_connection = object()
    second_connection = object()
    connections = [first_connection, second_connection]
    released = []
    attempts = {"count": 0}

    monkeypatch.setattr(app, "get_db", lambda: connections.pop(0))
    monkeypatch.setattr(
        app,
        "release_db",
        lambda connection, discard=False: released.append((connection, discard)),
    )

    def operation(_connection):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise psycopg2.OperationalError("broken SSL connection")
        return "ok"

    assert app.run_db_operation(operation, "test") == "ok"
    assert attempts["count"] == 2
    assert released == [
        (first_connection, True),
        (second_connection, False),
    ]


def test_db_operation_does_not_retry_application_errors(app_module, monkeypatch):
    app = app_module
    connection = object()
    released = []
    attempts = {"count": 0}

    monkeypatch.setattr(app, "get_db", lambda: connection)
    monkeypatch.setattr(
        app,
        "release_db",
        lambda current, discard=False: released.append((current, discard)),
    )

    def operation(_connection):
        attempts["count"] += 1
        raise ValueError("invalid")

    with pytest.raises(ValueError):
        app.run_db_operation(operation, "test")

    assert attempts["count"] == 1
    assert released == [(connection, False)]
