from types import SimpleNamespace

import pytest
from databricks.sdk.service.sql import StatementState

from mempalace.config import DatabricksConfig
from mempalace.databricks_backend import DatabricksBackend


def _response(state, statement_id="stmt-1", rows=None):
    result = None
    manifest = None
    if rows is not None:
        columns = [SimpleNamespace(name=name) for name in rows[0].keys()] if rows else []
        manifest = SimpleNamespace(schema=SimpleNamespace(columns=columns))
        result = SimpleNamespace(data_array=[list(row.values()) for row in rows])
    return SimpleNamespace(
        statement_id=statement_id,
        status=SimpleNamespace(state=state, error=None),
        manifest=manifest,
        result=result,
    )


class _FakeStatementExecution:
    def __init__(self, responses):
        self._responses = list(responses)
        self.cancelled = []

    def execute_statement(self, **_kwargs):
        return self._responses.pop(0)

    def get_statement(self, statement_id):
        assert statement_id == "stmt-1"
        return self._responses.pop(0)

    def cancel_execution(self, statement_id):
        self.cancelled.append(statement_id)


def _backend(fake_statement_execution):
    backend = DatabricksBackend(
        DatabricksConfig(catalog="scratch", schema="mitch_grabner", warehouse_id="wh")
    )
    backend._ws_client = SimpleNamespace(statement_execution=fake_statement_execution)
    return backend


def test_sql_polls_until_statement_succeeds(monkeypatch):
    monkeypatch.setattr("mempalace.databricks_backend.SQL_POLL_INTERVAL_SECONDS", 0.0)
    statement_execution = _FakeStatementExecution(
        [
            _response(StatementState.PENDING),
            _response(StatementState.RUNNING),
            _response(StatementState.SUCCEEDED, rows=[{"cnt": "247"}]),
        ]
    )

    rows = _backend(statement_execution).sql("SELECT COUNT(*) AS cnt FROM drawers")

    assert rows == [{"cnt": "247"}]
    assert statement_execution.cancelled == []


def test_sql_timeout_cancels_running_statement(monkeypatch):
    monkeypatch.setattr("mempalace.databricks_backend.SQL_POLL_TIMEOUT_SECONDS", 0.0)
    monkeypatch.setattr("mempalace.databricks_backend.SQL_POLL_INTERVAL_SECONDS", 0.0)
    statement_execution = _FakeStatementExecution([_response(StatementState.RUNNING)])

    with pytest.raises(TimeoutError):
        _backend(statement_execution).sql("SELECT COUNT(*) AS cnt FROM drawers")

    assert statement_execution.cancelled == ["stmt-1"]
