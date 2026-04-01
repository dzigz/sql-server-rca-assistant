from pathlib import Path
import importlib.util
import sys
import types

if importlib.util.find_spec("clickhouse_connect") is None:
    sys.modules["clickhouse_connect"] = types.SimpleNamespace()
if importlib.util.find_spec("mssql_python") is None:
    class _DummyProgrammingError(Exception):
        pass

    class _DummyConnection:
        pass

    def _dummy_connect(*args, **kwargs):
        raise RuntimeError("mssql_python stub connection should not be used in this test")

    sys.modules["mssql_python"] = types.SimpleNamespace(
        ProgrammingError=_DummyProgrammingError,
        Connection=_DummyConnection,
        connect=_dummy_connect,
    )

from sim.rca.tools.health_tools import GetServerConfigTool, RunSpBlitzTool


def test_run_sp_blitz_returns_install_offer_when_missing(monkeypatch):
    tool = RunSpBlitzTool(
        sqlserver_host="sql.example.com",
        sqlserver_password="secret",
        offer_install_prompt=True,
    )
    monkeypatch.setattr(tool, "_resolve_sp_blitz_proc_direct", lambda: None)

    result = tool.execute()

    assert result.success is True
    assert result.data["status"] == "install_required"
    assert result.data["install_offer"]["type"] == "blitz_install_offer"
    assert "sp_Blitz" in result.data["message"]


def test_run_sp_blitz_without_offer_returns_failure(monkeypatch):
    tool = RunSpBlitzTool(
        sqlserver_host="sql.example.com",
        sqlserver_password="secret",
        offer_install_prompt=False,
    )
    monkeypatch.setattr(tool, "_resolve_sp_blitz_proc_direct", lambda: None)

    result = tool.execute()

    assert result.success is False
    assert "not installed" in (result.error or "").lower()


def test_install_first_responder_kit_direct_executes_scripts(monkeypatch, tmp_path):
    tool = RunSpBlitzTool(
        sqlserver_host="sql.example.com",
        sqlserver_password="secret",
    )

    scripts = ["a.sql", "b.sql"]
    for script in scripts:
        (tmp_path / script).write_text("SELECT 1;\nGO\n", encoding="utf-8")

    monkeypatch.setattr("sim.rca.tools.health_tools.FRK_SCRIPT_DIR", Path(tmp_path))
    monkeypatch.setattr("sim.rca.tools.health_tools.FRK_SCRIPT_FILES", scripts)
    monkeypatch.setattr(
        tool,
        "_get_installed_frk_procedures_direct",
        lambda database="master": [
            "sp_Blitz",
            "sp_BlitzFirst",
            "sp_BlitzCache",
            "sp_BlitzWho",
            "sp_BlitzIndex",
            "sp_BlitzLock",
        ],
    )

    executed_scripts = []

    class DummyConnection:
        def cursor(self):
            return object()

        def commit(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_execute(cursor, script_path):
        executed_scripts.append(script_path.name)
        return 2

    monkeypatch.setattr(tool, "_get_connection", lambda database=None: DummyConnection())
    monkeypatch.setattr(tool, "_execute_sql_script_direct", fake_execute)

    result = tool.install_first_responder_kit_direct()

    assert result["status"] == "installed"
    assert result["scripts_executed"] == scripts
    assert executed_scripts == scripts
    assert result["executed_batches"] == 4


def test_get_server_config_casts_sql_variant_columns(monkeypatch):
    tool = GetServerConfigTool(
        sqlserver_host="sql.example.com",
        sqlserver_password="secret",
    )

    executed_sql = []

    class DummyCursor:
        description = [
            ("name",),
            ("value",),
            ("value_in_use",),
            ("minimum",),
            ("maximum",),
            ("description",),
        ]

        def execute(self, sql, params=None):
            executed_sql.append((sql, params))

        def fetchall(self):
            return [
                ("max degree of parallelism", 4, 4, 0, 32767, "Maximum degree"),
            ]

    class DummyConnection:
        def cursor(self):
            return DummyCursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(tool, "_get_connection", lambda: DummyConnection())

    result = tool.execute()

    assert result.success is True
    assert result.data["configurations"][0]["value"] == 4
    assert len(executed_sql) == 2
    select_sql, params = executed_sql[1]
    assert params is None
    assert "CAST(value AS bigint) AS value" in select_sql
    assert "CAST(value_in_use AS bigint) AS value_in_use" in select_sql
    assert "CAST(minimum AS bigint) AS minimum" in select_sql
    assert "CAST(maximum AS bigint) AS maximum" in select_sql
