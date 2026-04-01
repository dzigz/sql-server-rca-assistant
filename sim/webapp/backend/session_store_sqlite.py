"""
SQLite-backed session storage backend.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Optional

from sim.webapp.backend.session_models import ChatSession
from sim.webapp.backend.session_store import SessionStore


class SQLiteSessionStore(SessionStore):
    """Session storage implementation backed by SQLite."""

    def __init__(self, db_path: Path):
        self._db_path = db_path.expanduser()
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    last_activity_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    message_index INTEGER NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    timestamp TEXT,
                    blocks_json TEXT,
                    FOREIGN KEY(session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
                    UNIQUE(session_id, message_index)
                )
                """
            )

    def _fetch_messages(self, conn: sqlite3.Connection, session_id: str) -> list[dict]:
        rows = conn.execute(
            """
            SELECT role, content, timestamp, blocks_json
            FROM messages
            WHERE session_id = ?
            ORDER BY message_index ASC
            """,
            (session_id,),
        ).fetchall()

        messages: list[dict] = []
        for row in rows:
            message = {
                "role": row["role"],
                "content": row["content"],
            }
            if row["timestamp"]:
                message["timestamp"] = row["timestamp"]
            if row["blocks_json"]:
                try:
                    blocks = json.loads(row["blocks_json"])
                    if blocks:
                        message["blocks"] = blocks
                except Exception:
                    pass
            messages.append(message)
        return messages

    def load_session(self, session_id: str) -> Optional[ChatSession]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT session_id, created_at, metadata_json
                FROM sessions
                WHERE session_id = ?
                """,
                (session_id,),
            ).fetchone()
            if not row:
                return None

            metadata = {}
            if row["metadata_json"]:
                try:
                    metadata = json.loads(row["metadata_json"])
                except Exception:
                    metadata = {}

            payload = {
                "session_id": row["session_id"],
                "created_at": row["created_at"],
                "metadata": metadata,
                "message_history": self._fetch_messages(conn, session_id),
            }
            return ChatSession.from_dict(payload)

    def list_sessions(self) -> list[ChatSession]:
        sessions: list[ChatSession] = []
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT session_id, created_at, metadata_json
                FROM sessions
                ORDER BY created_at DESC
                """
            ).fetchall()
            for row in rows:
                metadata = {}
                if row["metadata_json"]:
                    try:
                        metadata = json.loads(row["metadata_json"])
                    except Exception:
                        metadata = {}
                payload = {
                    "session_id": row["session_id"],
                    "created_at": row["created_at"],
                    "metadata": metadata,
                    "message_history": self._fetch_messages(conn, row["session_id"]),
                }
                sessions.append(ChatSession.from_dict(payload))
        return sessions

    def save_session(self, session: ChatSession) -> None:
        with self._connect() as conn:
            with conn:
                conn.execute(
                    """
                    INSERT INTO sessions (session_id, created_at, metadata_json, last_activity_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(session_id) DO UPDATE SET
                        created_at = excluded.created_at,
                        metadata_json = excluded.metadata_json,
                        last_activity_at = excluded.last_activity_at
                    """,
                    (
                        session.session_id,
                        session.created_at.isoformat(),
                        json.dumps(session.metadata or {}),
                        (
                            session.message_history[-1].get("timestamp")
                            if session.message_history
                            else session.created_at.isoformat()
                        ),
                    ),
                )
                conn.execute(
                    "DELETE FROM messages WHERE session_id = ?",
                    (session.session_id,),
                )
                for idx, message in enumerate(session.message_history):
                    blocks = message.get("blocks")
                    conn.execute(
                        """
                        INSERT INTO messages (
                            session_id,
                            message_index,
                            role,
                            content,
                            timestamp,
                            blocks_json
                        )
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            session.session_id,
                            idx,
                            message.get("role", ""),
                            message.get("content", ""),
                            message.get("timestamp"),
                            json.dumps(blocks) if blocks is not None else None,
                        ),
                    )

    def delete_session(self, session_id: str) -> bool:
        with self._connect() as conn:
            with conn:
                deleted = conn.execute(
                    "DELETE FROM sessions WHERE session_id = ?",
                    (session_id,),
                ).rowcount
        return bool(deleted)
