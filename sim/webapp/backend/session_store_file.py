"""
JSON file-based session storage backend.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from sim.webapp.backend.session_models import ChatSession
from sim.webapp.backend.session_store import SessionStore


class FileSessionStore(SessionStore):
    """Session storage implementation backed by JSON files."""

    def __init__(self, sessions_dir: Path):
        self._sessions_dir = sessions_dir.expanduser()
        self._sessions_dir.mkdir(parents=True, exist_ok=True)

    def _session_file(self, session_id: str) -> Path:
        return self._sessions_dir / f"{session_id}.json"

    def load_session(self, session_id: str) -> Optional[ChatSession]:
        session_file = self._session_file(session_id)
        if not session_file.exists():
            return None
        try:
            with open(session_file, encoding="utf-8") as f:
                data = json.load(f)
            return ChatSession.from_dict(data)
        except Exception:
            return None

    def list_sessions(self) -> list[ChatSession]:
        sessions: list[ChatSession] = []
        for session_file in sorted(self._sessions_dir.glob("*.json")):
            session_id = session_file.stem
            session = self.load_session(session_id)
            if session:
                sessions.append(session)
        return sessions

    def save_session(self, session: ChatSession) -> None:
        session_file = self._session_file(session.session_id)
        with open(session_file, "w", encoding="utf-8") as f:
            json.dump(session.to_dict(), f, indent=2)

    def delete_session(self, session_id: str) -> bool:
        session_file = self._session_file(session_id)
        if not session_file.exists():
            return False
        session_file.unlink()
        return True
