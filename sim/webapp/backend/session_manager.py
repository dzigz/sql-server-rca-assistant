"""
Session Manager for Web Chat.

Manages chat sessions, storing engine instances and message history.
Supports file-based and SQLite persistence backends.
"""

from __future__ import annotations

import os
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from sim.webapp.backend.session_models import ChatSession
from sim.webapp.backend.session_store import SessionStore
from sim.webapp.backend.session_store_file import FileSessionStore
from sim.webapp.backend.session_store_sqlite import SQLiteSessionStore

# Legacy JSON session storage directory (also migration source for sqlite backend)
SESSIONS_DIR = Path(os.environ.get("SIM_SESSIONS_DIR", str(Path.home() / ".sim" / "sessions"))).expanduser()

# SQLite session database path (new default backend)
SQLITE_PATH = Path(os.environ.get("SIM_SQLITE_PATH", str(Path.home() / ".sim" / "sessions.db"))).expanduser()

# Persistence backend: "sqlite" (default) or "file"
SESSION_BACKEND = (os.environ.get("SIM_SESSION_BACKEND", "sqlite") or "sqlite").strip().lower()

# Session expiry.
# Set SIM_SESSION_TTL_HOURS <= 0 to disable session expiry.
try:
    SESSION_TTL_HOURS = int((os.environ.get("SIM_SESSION_TTL_HOURS", "0") or "0").strip())
except ValueError:
    SESSION_TTL_HOURS = 0

# If set, remove migrated JSON files after sqlite import
SESSION_MIGRATION_DELETE_SOURCE = (
    os.environ.get("SIM_SESSION_MIGRATION_DELETE_SOURCE", "0").strip().lower() in {"1", "true", "yes"}
)


class SessionManager:
    """
    Manages chat sessions for the web app.

    Sessions are cached in memory during runtime and persisted via the configured
    storage backend for recovery.
    """

    def __init__(self):
        self._sessions: Dict[str, ChatSession] = {}
        self._lock = threading.RLock()
        self._store: SessionStore = self._create_store()
        self._maybe_migrate_legacy_file_sessions()
        with self._lock:
            self._load_sessions_from_store()

    def _create_store(self) -> SessionStore:
        """Create the configured persistence backend."""
        if SESSION_BACKEND == "sqlite":
            try:
                return SQLiteSessionStore(SQLITE_PATH)
            except Exception as exc:
                fallback = Path("/tmp/sim_sessions.db")
                print(
                    f"[SessionManager] Failed to initialize sqlite store at {SQLITE_PATH}: {exc}. "
                    f"Falling back to {fallback}."
                )
                return SQLiteSessionStore(fallback)
        if SESSION_BACKEND == "file":
            try:
                return FileSessionStore(SESSIONS_DIR)
            except Exception as exc:
                fallback = Path("/tmp/sim_sessions")
                print(
                    f"[SessionManager] Failed to initialize file store at {SESSIONS_DIR}: {exc}. "
                    f"Falling back to {fallback}."
                )
                return FileSessionStore(fallback)

        print(
            f"[SessionManager] Unknown SIM_SESSION_BACKEND='{SESSION_BACKEND}', "
            "falling back to file storage."
        )
        return FileSessionStore(SESSIONS_DIR)

    def _maybe_migrate_legacy_file_sessions(self) -> None:
        """
        Import legacy JSON sessions into sqlite storage on startup.

        Migration is idempotent and skips sessions that already exist in sqlite.
        """
        if SESSION_BACKEND != "sqlite":
            return

        try:
            legacy_store = FileSessionStore(SESSIONS_DIR)
        except Exception as exc:
            print(f"[SessionManager] Skipping legacy session migration from {SESSIONS_DIR}: {exc}")
            return
        migrated = 0
        deleted_sources = 0
        for legacy_session in legacy_store.list_sessions():
            try:
                existing = self._store.load_session(legacy_session.session_id)
                if existing is None:
                    self._store.save_session(legacy_session)
                    migrated += 1
            except Exception as exc:
                print(
                    "[SessionManager] Skipping legacy migration write for "
                    f"{legacy_session.session_id}: {exc}"
                )
                continue
            if SESSION_MIGRATION_DELETE_SOURCE and legacy_store.delete_session(legacy_session.session_id):
                deleted_sources += 1

        if migrated:
            print(f"[SessionManager] Migrated {migrated} legacy session(s) to sqlite: {SQLITE_PATH}")
        if deleted_sources:
            print(f"[SessionManager] Removed {deleted_sources} migrated legacy JSON session file(s)")

    def _resolve_sqlserver_password(self, explicit_password: Optional[str] = None) -> str:
        """Resolve SQL Server password from explicit value or environment defaults."""
        from sim.config import DEFAULT_SA_PASSWORD

        resolved = (
            explicit_password
            or os.environ.get("SQLSERVER_PASSWORD")
            or os.environ.get("SA_PASSWORD")
            or os.environ.get("SIM_SA_PASSWORD")
            or DEFAULT_SA_PASSWORD
        )
        if resolved == DEFAULT_SA_PASSWORD:
            raise ValueError(
                "SQL Server password is not configured. Set SQLSERVER_PASSWORD, SA_PASSWORD, "
                "or SIM_SA_PASSWORD "
                "(or set SIM_ALLOW_INSECURE_DEFAULTS=1 for local demo defaults)."
            )
        return resolved

    def _resolve_sqlserver_settings(
        self,
        sqlserver_host: Optional[str] = None,
        sqlserver_port: Optional[int] = None,
        sqlserver_user: Optional[str] = None,
        sqlserver_database: Optional[str] = None,
        sqlserver_container: Optional[str] = None,
        sqlserver_password: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Resolve SQL Server connection settings from args/environment defaults."""
        port_value = (
            sqlserver_port
            if sqlserver_port is not None
            else os.environ.get("SQLSERVER_PORT")
        )
        try:
            resolved_port = int(port_value) if port_value is not None else 1433
        except (TypeError, ValueError):
            resolved_port = 1433

        return {
            "sqlserver_host": sqlserver_host or os.environ.get("SQLSERVER_HOST"),
            "sqlserver_port": resolved_port,
            "sqlserver_user": sqlserver_user or os.environ.get("SQLSERVER_USER", "sa"),
            "sqlserver_database": sqlserver_database or os.environ.get("SQLSERVER_DATABASE", "master"),
            "sqlserver_container": sqlserver_container or os.environ.get("SQLSERVER_CONTAINER", "sqlserver"),
            "sqlserver_password": sqlserver_password or os.environ.get("SQLSERVER_PASSWORD"),
        }

    def _resolve_monitoring_settings(
        self,
        enable_monitoring: Optional[bool] = None,
        clickhouse_host: Optional[str] = None,
        clickhouse_port: Optional[int] = None,
        clickhouse_database: Optional[str] = None,
        clickhouse_user: Optional[str] = None,
        clickhouse_password: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Resolve optional monitoring backend settings."""
        def _to_bool(value: Any) -> bool:
            if isinstance(value, bool):
                return value
            if value is None:
                return False
            return str(value).strip().lower() in {"1", "true", "yes", "on"}

        if enable_monitoring is None:
            enable_monitoring = _to_bool(os.environ.get("SIM_ENABLE_MONITORING"))

        port_value = (
            clickhouse_port
            if clickhouse_port is not None
            else os.environ.get("CLICKHOUSE_PORT")
        )
        try:
            resolved_port = int(port_value) if port_value is not None else 8123
        except (TypeError, ValueError):
            resolved_port = 8123

        return {
            "enabled": _to_bool(enable_monitoring),
            "clickhouse_host": clickhouse_host or os.environ.get("CLICKHOUSE_HOST", "localhost"),
            "clickhouse_port": resolved_port,
            "clickhouse_database": clickhouse_database or os.environ.get("CLICKHOUSE_DATABASE", "rca_metrics"),
            "clickhouse_user": clickhouse_user or os.environ.get("CLICKHOUSE_USER", "rca"),
            "clickhouse_password": clickhouse_password or os.environ.get("CLICKHOUSE_PASSWORD", "rca_password"),
        }

    def _resolve_auto_install_blitz(self, value: Optional[bool] = None) -> bool:
        """Resolve Blitz auto-install setting from input/environment."""
        if value is not None:
            return bool(value)
        raw = os.environ.get("SIM_AUTO_INSTALL_BLITZ")
        if raw is None:
            return True
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}

    def _maybe_auto_install_blitz(self, session_id: str, engine) -> Dict[str, Any]:
        """Attempt FRK auto-install for direct SQL sessions."""
        status: Dict[str, Any] = {
            "status": "skipped",
            "installed": False,
            "session_id": session_id,
        }
        tool = getattr(engine, "tools", None).get("run_sp_blitz") if getattr(engine, "tools", None) else None
        if not tool:
            status["reason"] = "run_sp_blitz tool unavailable"
            return status
        if not hasattr(tool, "install_first_responder_kit_direct"):
            status["reason"] = "direct SQL install not supported in this mode"
            return status

        try:
            # If already installed, skip work.
            if hasattr(tool, "_check_sp_blitz_installed_direct") and tool._check_sp_blitz_installed_direct():
                status["status"] = "already_installed"
                status["installed"] = True
                return status

            result = tool.install_first_responder_kit_direct(target_database="master")
            status["status"] = "installed"
            status["installed"] = True
            status["details"] = result
            return status
        except Exception as exc:
            status["status"] = "failed"
            status["error"] = str(exc)
            return status

    def _create_engine(
        self,
        session_id: str,
        repo_path: Optional[str],
        sqlserver_settings: Dict[str, Any],
        monitoring_settings: Dict[str, Any],
        offer_blitz_install_prompt: bool = True,
    ):
        """Create a chat engine instance for a session."""
        from sim.rca import AgentRCAEngine, RCAConfig
        from sim.rca.engine.unified_prompt import UNIFIED_DBA_SYSTEM_PROMPT
        from sim.rca.tools.base import ToolRegistry
        from sim.rca.tools.health_tools import create_health_tool_registry

        if not sqlserver_settings.get("sqlserver_host"):
            raise ValueError(
                "SQL Server host is required for stage-1 direct analysis. "
                "Set SQLSERVER_HOST or provide sqlserver_host when creating a session."
            )

        data_source = None
        tools = ToolRegistry()

        if monitoring_settings.get("enabled"):
            from sim.rca import ClickHouseDataSource, create_clickhouse_tool_registry

            data_source = ClickHouseDataSource(
                host=monitoring_settings.get("clickhouse_host", "localhost"),
                port=monitoring_settings.get("clickhouse_port", 8123),
                database=monitoring_settings.get("clickhouse_database", "rca_metrics"),
                username=monitoring_settings.get("clickhouse_user", "default"),
                password=monitoring_settings.get("clickhouse_password", ""),
            )
            clickhouse_registry = create_clickhouse_tool_registry(
                data_source,
                f"chat_{session_id}",
                include_code_analysis=False,
            )
            for tool in clickhouse_registry.get_all_tools():
                tools.register(tool)
            print(
                "[SessionManager] Monitoring enabled via ClickHouse "
                f"{monitoring_settings.get('clickhouse_host')}:{monitoring_settings.get('clickhouse_port')}"
            )
        else:
            print("[SessionManager] Monitoring disabled - using direct SQL Server tools only")

        health_tools = create_health_tool_registry(
            sqlserver_host=sqlserver_settings.get("sqlserver_host"),
            sqlserver_port=sqlserver_settings.get("sqlserver_port", 1433),
            sqlserver_user=sqlserver_settings.get("sqlserver_user", "sa"),
            sqlserver_database=sqlserver_settings.get("sqlserver_database", "master"),
            sqlserver_container=sqlserver_settings.get("sqlserver_container", "sqlserver"),
            sqlserver_password=self._resolve_sqlserver_password(
                sqlserver_settings.get("sqlserver_password")
            ),
            offer_install_prompt=offer_blitz_install_prompt,
        )
        for tool in health_tools._tools.values():
            tools.register(tool)
        print(f"[SessionManager] Registered health tools: {[t.name for t in health_tools._tools.values()]}")

        if repo_path:
            try:
                from sim.rca.tools.code_analysis_tools import create_code_analysis_tools, CLAUDE_AGENT_SDK_AVAILABLE

                if CLAUDE_AGENT_SDK_AVAILABLE:
                    code_tools = create_code_analysis_tools()
                    for tool in code_tools:
                        tools.register(tool)
                    print(f"[SessionManager] Registered code analysis tools: {[t.name for t in code_tools]}")
                else:
                    print("[SessionManager] Code analysis tools unavailable: claude-agent-sdk not installed")
                    print(f"[SessionManager] repo_path={repo_path} will be passed to agent but tools won't work")
            except Exception as e:
                print(f"[SessionManager] Failed to register code analysis tools: {e}")

        if monitoring_settings.get("enabled"):
            try:
                from sim.rca.tools.grafana_tools import EmbedChartTool, CreateChartTool

                tools.register(EmbedChartTool())
                tools.register(CreateChartTool())
                print("[SessionManager] Registered Grafana visualization tools: embed_chart, create_chart")
            except Exception as e:
                print(f"[SessionManager] Failed to register Grafana tools: {e}")

        config = RCAConfig()
        return AgentRCAEngine(
            config=config,
            tools=tools,
            data_source=data_source,
            system_prompt=UNIFIED_DBA_SYSTEM_PROMPT,
            analysis_mode="unified",
        )

    def _is_expired(self, session: ChatSession) -> bool:
        """Return True if the session is past its TTL."""
        if SESSION_TTL_HOURS <= 0:
            return False

        activity_at = session.created_at
        if session.message_history:
            last_timestamp = session.message_history[-1].get("timestamp")
            if isinstance(last_timestamp, str) and last_timestamp:
                try:
                    activity_at = datetime.fromisoformat(last_timestamp.replace("Z", "+00:00"))
                except ValueError:
                    activity_at = session.created_at
        now = datetime.now(activity_at.tzinfo) if activity_at.tzinfo else datetime.now()
        age = now - activity_at
        return age.total_seconds() > SESSION_TTL_HOURS * 3600

    def _prune_sessions(self) -> None:
        """Remove expired sessions from memory and persistence."""
        expired = [sid for sid, s in self._sessions.items() if self._is_expired(s)]
        for session_id in expired:
            self.delete_session(session_id)

    def create_session(
        self,
        repo_path: Optional[str] = None,
        sqlserver_host: Optional[str] = None,
        sqlserver_port: Optional[int] = None,
        sqlserver_user: Optional[str] = None,
        sqlserver_database: Optional[str] = None,
        sqlserver_container: Optional[str] = None,
        sqlserver_password: Optional[str] = None,
        enable_monitoring: Optional[bool] = None,
        clickhouse_host: Optional[str] = None,
        clickhouse_port: Optional[int] = None,
        clickhouse_database: Optional[str] = None,
        clickhouse_user: Optional[str] = None,
        clickhouse_password: Optional[str] = None,
        auto_install_blitz: Optional[bool] = None,
    ) -> ChatSession:
        """
        Create a new chat session with initialized AgentRCAEngine.

        Args:
            repo_path: Optional path to application repository for code analysis
            sqlserver_host: SQL Server host (direct mode when set)
            sqlserver_port: SQL Server port
            sqlserver_user: SQL Server user
            sqlserver_database: SQL Server database
            sqlserver_container: Docker SQL Server container name (legacy fallback)
            sqlserver_password: SQL Server password
            enable_monitoring: Enable ClickHouse monitoring tools
            clickhouse_host: ClickHouse host for monitoring mode
            clickhouse_port: ClickHouse port for monitoring mode
            clickhouse_database: ClickHouse database for monitoring mode
            clickhouse_user: ClickHouse user for monitoring mode
            clickhouse_password: ClickHouse password for monitoring mode
            auto_install_blitz: Install FRK scripts automatically when missing

        Returns:
            New ChatSession with engine ready for chat
        """
        session_id = str(uuid.uuid4())[:8]
        with self._lock:
            self._prune_sessions()
        sqlserver_settings = self._resolve_sqlserver_settings(
            sqlserver_host=sqlserver_host,
            sqlserver_port=sqlserver_port,
            sqlserver_user=sqlserver_user,
            sqlserver_database=sqlserver_database,
            sqlserver_container=sqlserver_container,
            sqlserver_password=sqlserver_password,
        )
        monitoring_settings = self._resolve_monitoring_settings(
            enable_monitoring=enable_monitoring,
            clickhouse_host=clickhouse_host,
            clickhouse_port=clickhouse_port,
            clickhouse_database=clickhouse_database,
            clickhouse_user=clickhouse_user,
            clickhouse_password=clickhouse_password,
        )
        auto_install = self._resolve_auto_install_blitz(auto_install_blitz)
        metadata = {
            "repo_path": repo_path,
            "sqlserver_host": sqlserver_settings["sqlserver_host"],
            "sqlserver_port": sqlserver_settings["sqlserver_port"],
            "sqlserver_user": sqlserver_settings["sqlserver_user"],
            "sqlserver_database": sqlserver_settings["sqlserver_database"],
            "sqlserver_container": sqlserver_settings["sqlserver_container"],
            "monitoring_enabled": bool(monitoring_settings.get("enabled")),
            "clickhouse_host": monitoring_settings["clickhouse_host"],
            "clickhouse_port": monitoring_settings["clickhouse_port"],
            "clickhouse_database": monitoring_settings["clickhouse_database"],
            "clickhouse_user": monitoring_settings["clickhouse_user"],
            "auto_install_blitz": auto_install,
            "blitz_install_declined": False,
        }
        engine = self._create_engine(
            session_id=session_id,
            repo_path=repo_path,
            sqlserver_settings=sqlserver_settings,
            monitoring_settings=monitoring_settings,
            offer_blitz_install_prompt=not bool(metadata.get("blitz_install_declined")),
        )

        auto_install_result = None
        if auto_install:
            auto_install_result = self._maybe_auto_install_blitz(session_id, engine)
            metadata["blitz_auto_install"] = auto_install_result

        session = ChatSession(
            session_id=session_id,
            created_at=datetime.now(),
            metadata=metadata,
        )
        session.engine = engine

        with self._lock:
            self._sessions[session_id] = session
            self._save_session(session)

        return session

    def get_session(self, session_id: str) -> Optional[ChatSession]:
        """Get a session by ID."""
        with self._lock:
            self._prune_sessions()
            session = self._sessions.get(session_id)
            if session:
                return session

            loaded = self._load_session(session_id)
            if not loaded:
                return None
            if self._is_expired(loaded):
                self.delete_session(session_id)
                return None

            self._sessions[session_id] = loaded
            return loaded

    def get_engine(self, session_id: str):
        """Get the engine for a session."""
        session = self.get_session(session_id)
        if not session:
            return None
        if session.engine:
            return session.engine

        metadata = session.metadata or {}
        sqlserver_settings = self._resolve_sqlserver_settings(
            sqlserver_host=metadata.get("sqlserver_host"),
            sqlserver_port=metadata.get("sqlserver_port"),
            sqlserver_user=metadata.get("sqlserver_user"),
            sqlserver_database=metadata.get("sqlserver_database"),
            sqlserver_container=metadata.get("sqlserver_container"),
        )
        monitoring_settings = self._resolve_monitoring_settings(
            enable_monitoring=metadata.get("monitoring_enabled"),
            clickhouse_host=metadata.get("clickhouse_host"),
            clickhouse_port=metadata.get("clickhouse_port"),
            clickhouse_database=metadata.get("clickhouse_database"),
            clickhouse_user=metadata.get("clickhouse_user"),
        )
        session.engine = self._create_engine(
            session_id=session.session_id,
            repo_path=metadata.get("repo_path"),
            sqlserver_settings=sqlserver_settings,
            monitoring_settings=monitoring_settings,
            offer_blitz_install_prompt=not bool(metadata.get("blitz_install_declined")),
        )

        # Rehydrate basic chat context for follow-up mode after process restart.
        message_history = [{"role": "system", "content": session.engine._system_prompt}]
        for message in session.message_history:
            role = message.get("role")
            content = message.get("content")
            if role in {"user", "assistant"} and content:
                message_history.append({"role": role, "content": content})
        if len(message_history) > 1:
            session.engine._message_history = message_history

        return session.engine

    def add_message(
        self,
        session_id: str,
        role: str,
        content: str,
        blocks: Optional[list] = None,
    ) -> None:
        """
        Add a message to session history.

        Args:
            session_id: Session ID
            role: Message role ('user' or 'assistant')
            content: Message content (final text)
            blocks: Optional list of structured blocks for assistant messages
                    Each block has: type ('thinking'|'tool'|'text'), content, etc.
        """
        with self._lock:
            self._prune_sessions()
            session = self._sessions.get(session_id)
            if session:
                message = {
                    "role": role,
                    "content": content,
                    "timestamp": datetime.now().isoformat(),
                }
                if blocks:
                    message["blocks"] = blocks
                session.message_history.append(message)
                self._save_session(session)

    def update_metadata(self, session_id: str, updates: Dict[str, Any]) -> bool:
        """Update session metadata and persist."""
        with self._lock:
            self._prune_sessions()
            session = self._sessions.get(session_id)
            if not session:
                session = self._load_session(session_id)
                if not session:
                    return False
                if self._is_expired(session):
                    self.delete_session(session_id)
                    return False
                self._sessions[session_id] = session

            if session.metadata is None:
                session.metadata = {}
            session.metadata.update(updates)
            self._save_session(session)
            return True

    def get_tool(self, session_id: str, tool_name: str):
        """Get a registered tool instance by name for the session engine."""
        engine = self.get_engine(session_id)
        if not engine or not getattr(engine, "tools", None):
            return None
        return engine.tools.get(tool_name)

    def list_sessions(self) -> list[dict]:
        """List all active sessions with full payload."""
        with self._lock:
            self._load_sessions_from_store()
            self._prune_sessions()
            sessions = sorted(self._sessions.values(), key=lambda s: s.created_at, reverse=True)
            return [s.to_dict() for s in sessions]

    def list_session_summaries(self) -> list[dict]:
        """List active sessions with lightweight metadata for UI history panels."""
        with self._lock:
            self._load_sessions_from_store()
            self._prune_sessions()
            sessions = sorted(self._sessions.values(), key=lambda s: s.created_at, reverse=True)
            return [
                {
                    "session_id": session.session_id,
                    "created_at": session.created_at.isoformat(),
                    "message_count": len(session.message_history),
                    "title": self._build_session_title(session),
                    "last_message_at": (
                        self._get_last_message_timestamp(session)
                        or session.created_at.isoformat()
                    ),
                }
                for session in sessions
                if len(session.message_history) > 0
            ]

    def _build_session_title(self, session: ChatSession) -> str:
        """Build a human-friendly session title from the first user message."""
        for message in session.message_history:
            if message.get("role") != "user":
                continue
            content = (message.get("content") or "").strip()
            if not content:
                continue
            compact = " ".join(content.split())
            return compact[:80] if len(compact) > 80 else compact
        return "New analysis"

    def _get_last_message_timestamp(self, session: ChatSession) -> Optional[str]:
        """Return the most recent message timestamp, if present."""
        for message in reversed(session.message_history):
            timestamp = message.get("timestamp")
            if isinstance(timestamp, str) and timestamp:
                return timestamp
        return None

    def delete_session(self, session_id: str) -> bool:
        """Delete a session."""
        with self._lock:
            deleted = False
            session = self._sessions.pop(session_id, None)
            if session:
                if session.engine and session.engine.data_source:
                    try:
                        session.engine.data_source.close()
                    except Exception:
                        pass
                deleted = True

            if self._store.delete_session(session_id):
                deleted = True

            return deleted

    def _save_session(self, session: ChatSession) -> None:
        """Persist a session snapshot."""
        self._store.save_session(session)

    def _load_session(self, session_id: str) -> Optional[ChatSession]:
        """Load a session from persistent storage (without engine)."""
        return self._store.load_session(session_id)

    def _load_sessions_from_store(self) -> None:
        """Load persisted sessions into memory."""
        for session in self._store.list_sessions():
            existing = self._sessions.get(session.session_id)
            if existing and existing.engine:
                # Keep live engine object, refresh persisted data.
                existing.created_at = session.created_at
                existing.message_history = session.message_history
                existing.metadata = session.metadata
            else:
                self._sessions[session.session_id] = session

    # Backward-compatible alias used by existing tests and callers.
    def _load_sessions_from_disk(self) -> None:
        self._load_sessions_from_store()


# Global session manager instance
session_manager = SessionManager()
