from datetime import datetime, timedelta
import json

from sim.webapp.backend import session_manager as sm


def _configure_sqlite_backend(monkeypatch, tmp_path):
    monkeypatch.setattr(sm, "SESSION_BACKEND", "sqlite")
    monkeypatch.setattr(sm, "SQLITE_PATH", tmp_path / "sessions.db")
    monkeypatch.setattr(sm, "SESSIONS_DIR", tmp_path / "legacy_sessions")
    monkeypatch.setattr(sm, "SESSION_MIGRATION_DELETE_SOURCE", False)


def test_sqlite_store_persists_and_loads_messages(tmp_path, monkeypatch):
    _configure_sqlite_backend(monkeypatch, tmp_path)
    manager = sm.SessionManager()

    session = sm.ChatSession(
        session_id="abc12345",
        created_at=datetime.now(),
        metadata={"clickhouse_host": "localhost"},
        message_history=[
            {
                "role": "user",
                "content": "hello",
                "timestamp": datetime.now().isoformat(),
            },
            {
                "role": "assistant",
                "content": "world",
                "timestamp": datetime.now().isoformat(),
                "blocks": [{"type": "text", "content": "world"}],
            },
        ],
    )
    manager._save_session(session)

    reloaded = sm.SessionManager()
    loaded = reloaded.get_session("abc12345")

    assert loaded is not None
    assert loaded.session_id == "abc12345"
    assert [m["role"] for m in loaded.message_history] == ["user", "assistant"]
    assert loaded.message_history[1]["blocks"][0]["type"] == "text"


def test_sqlite_migrates_legacy_json_sessions_once(tmp_path, monkeypatch):
    _configure_sqlite_backend(monkeypatch, tmp_path)

    legacy_dir = tmp_path / "legacy_sessions"
    legacy_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "session_id": "legacy01",
        "created_at": datetime.now().isoformat(),
        "message_history": [{"role": "user", "content": "hello"}],
        "metadata": {"source": "json"},
    }
    (legacy_dir / "legacy01.json").write_text(json.dumps(payload), encoding="utf-8")

    first_manager = sm.SessionManager()
    first = first_manager.get_session("legacy01")
    assert first is not None
    assert first.metadata["source"] == "json"

    second_manager = sm.SessionManager()
    summaries = second_manager.list_session_summaries()
    matching = [s for s in summaries if s["session_id"] == "legacy01"]

    assert len(matching) == 1
    assert (legacy_dir / "legacy01.json").exists()


def test_sqlite_migration_can_delete_legacy_json_sources(tmp_path, monkeypatch):
    _configure_sqlite_backend(monkeypatch, tmp_path)
    monkeypatch.setattr(sm, "SESSION_MIGRATION_DELETE_SOURCE", True)

    legacy_dir = tmp_path / "legacy_sessions"
    legacy_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "session_id": "legacy02",
        "created_at": datetime.now().isoformat(),
        "message_history": [],
        "metadata": {},
    }
    legacy_file = legacy_dir / "legacy02.json"
    legacy_file.write_text(json.dumps(payload), encoding="utf-8")

    manager = sm.SessionManager()
    loaded = manager.get_session("legacy02")

    assert loaded is not None
    assert not legacy_file.exists()


def test_sqlite_prunes_expired_sessions_from_persistence(tmp_path, monkeypatch):
    _configure_sqlite_backend(monkeypatch, tmp_path)
    monkeypatch.setattr(sm, "SESSION_TTL_HOURS", 1)

    manager = sm.SessionManager()
    expired = sm.ChatSession(
        session_id="expired01",
        created_at=datetime.now() - timedelta(hours=2),
        metadata={},
    )
    manager._save_session(expired)

    # Trigger prune path
    _ = manager.list_sessions()

    assert manager.get_session("expired01") is None


def test_session_summaries_exclude_empty_chats(tmp_path, monkeypatch):
    _configure_sqlite_backend(monkeypatch, tmp_path)
    manager = sm.SessionManager()

    manager._save_session(
        sm.ChatSession(
            session_id="empty01",
            created_at=datetime.now(),
            metadata={},
            message_history=[],
        )
    )
    manager._save_session(
        sm.ChatSession(
            session_id="nonempty01",
            created_at=datetime.now(),
            metadata={},
            message_history=[
                {
                    "role": "user",
                    "content": "hello",
                    "timestamp": datetime.now().isoformat(),
                }
            ],
        )
    )

    summaries = manager.list_session_summaries()
    summaries_by_id = {s["session_id"]: s for s in summaries}
    summary_ids = {s["session_id"] for s in summaries}

    assert "nonempty01" in summary_ids
    assert "empty01" not in summary_ids
    assert summaries_by_id["nonempty01"]["title"] == "hello"


def test_session_summary_title_uses_first_user_message(tmp_path, monkeypatch):
    _configure_sqlite_backend(monkeypatch, tmp_path)
    manager = sm.SessionManager()

    manager._save_session(
        sm.ChatSession(
            session_id="title01",
            created_at=datetime.now(),
            metadata={},
            message_history=[
                {
                    "role": "assistant",
                    "content": "intro",
                    "timestamp": datetime.now().isoformat(),
                },
                {
                    "role": "user",
                    "content": "  Investigate long blocking chain in orders table  ",
                    "timestamp": datetime.now().isoformat(),
                },
                {
                    "role": "assistant",
                    "content": "working on it",
                },
            ],
        )
    )

    summaries = manager.list_session_summaries()
    summary = next(s for s in summaries if s["session_id"] == "title01")

    assert summary["title"] == "Investigate long blocking chain in orders table"
    assert isinstance(summary["last_message_at"], str)
