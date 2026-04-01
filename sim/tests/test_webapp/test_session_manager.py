from datetime import datetime, timedelta
import json

from sim.webapp.backend import session_manager as sm


def test_prune_sessions_removes_expired(tmp_path, monkeypatch):
    monkeypatch.setattr(sm, "SESSION_BACKEND", "file")
    monkeypatch.setattr(sm, "SESSIONS_DIR", tmp_path)
    monkeypatch.setattr(sm, "SESSION_TTL_HOURS", 1)

    manager = sm.SessionManager()
    old_session = sm.ChatSession(
        session_id="old",
        created_at=datetime.now() - timedelta(hours=2),
    )
    manager._sessions["old"] = old_session

    manager.list_sessions()

    assert "old" not in manager._sessions


def test_sessions_do_not_expire_when_ttl_disabled(tmp_path, monkeypatch):
    monkeypatch.setattr(sm, "SESSION_BACKEND", "file")
    monkeypatch.setattr(sm, "SESSIONS_DIR", tmp_path)
    monkeypatch.setattr(sm, "SESSION_TTL_HOURS", 0)

    manager = sm.SessionManager()
    old_session = sm.ChatSession(
        session_id="old_no_ttl",
        created_at=datetime.now() - timedelta(days=30),
    )
    manager._sessions["old_no_ttl"] = old_session

    manager.list_sessions()

    assert "old_no_ttl" in manager._sessions


def test_resolve_sqlserver_password_uses_env(tmp_path, monkeypatch):
    monkeypatch.setattr(sm, "SESSION_BACKEND", "file")
    monkeypatch.setattr(sm, "SESSIONS_DIR", tmp_path)
    manager = sm.SessionManager()

    monkeypatch.setenv("SA_PASSWORD", "dev-password")
    assert manager._resolve_sqlserver_password() == "dev-password"


def test_get_session_loads_from_disk(tmp_path, monkeypatch):
    monkeypatch.setattr(sm, "SESSION_BACKEND", "file")
    monkeypatch.setattr(sm, "SESSIONS_DIR", tmp_path)

    session_payload = {
        "session_id": "persisted",
        "created_at": datetime.now().isoformat(),
        "message_history": [{"role": "user", "content": "hello"}],
        "metadata": {"clickhouse_host": "localhost"},
    }
    (tmp_path / "persisted.json").write_text(json.dumps(session_payload), encoding="utf-8")

    manager = sm.SessionManager()
    loaded = manager.get_session("persisted")

    assert loaded is not None
    assert loaded.session_id == "persisted"
    assert loaded.message_history[0]["content"] == "hello"


def test_get_engine_rehydrates_history_from_persisted_session(tmp_path, monkeypatch):
    monkeypatch.setattr(sm, "SESSION_BACKEND", "file")
    monkeypatch.setattr(sm, "SESSIONS_DIR", tmp_path)
    monkeypatch.setenv("SA_PASSWORD", "dev-password")

    session_payload = {
        "session_id": "rehydrate",
        "created_at": datetime.now().isoformat(),
        "message_history": [
            {"role": "user", "content": "first question"},
            {"role": "assistant", "content": "first answer"},
        ],
        "metadata": {
            "clickhouse_host": "localhost",
            "clickhouse_port": 8123,
            "clickhouse_database": "rca_metrics",
        },
    }
    (tmp_path / "rehydrate.json").write_text(json.dumps(session_payload), encoding="utf-8")

    manager = sm.SessionManager()

    class DummyEngine:
        def __init__(self):
            self._system_prompt = "system prompt"
            self._message_history = []

    dummy = DummyEngine()
    monkeypatch.setattr(manager, "_create_engine", lambda **kwargs: dummy)

    engine = manager.get_engine("rehydrate")

    assert engine is dummy
    assert dummy._message_history[0]["role"] == "system"
    assert dummy._message_history[1]["role"] == "user"
    assert dummy._message_history[2]["role"] == "assistant"


def test_update_metadata_persists_to_disk(tmp_path, monkeypatch):
    monkeypatch.setattr(sm, "SESSION_BACKEND", "file")
    monkeypatch.setattr(sm, "SESSIONS_DIR", tmp_path)
    manager = sm.SessionManager()

    session = sm.ChatSession(
        session_id="meta",
        created_at=datetime.now(),
        metadata={"blitz_install_declined": False},
    )
    manager._sessions["meta"] = session

    assert manager.update_metadata("meta", {"blitz_install_declined": True}) is True
    assert manager._sessions["meta"].metadata["blitz_install_declined"] is True

    saved = json.loads((tmp_path / "meta.json").read_text(encoding="utf-8"))
    assert saved["metadata"]["blitz_install_declined"] is True


def test_get_tool_returns_tool_from_engine(tmp_path, monkeypatch):
    monkeypatch.setattr(sm, "SESSION_BACKEND", "file")
    monkeypatch.setattr(sm, "SESSIONS_DIR", tmp_path)
    manager = sm.SessionManager()

    class DummyTools:
        def __init__(self):
            self._tool = object()

        def get(self, name):
            if name == "run_sp_blitz":
                return self._tool
            return None

    class DummyEngine:
        def __init__(self):
            self.tools = DummyTools()

    session = sm.ChatSession(
        session_id="tool",
        created_at=datetime.now(),
        metadata={},
    )
    session.engine = DummyEngine()
    manager._sessions["tool"] = session

    tool = manager.get_tool("tool", "run_sp_blitz")
    assert tool is session.engine.tools._tool
