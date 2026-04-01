"""
Storage abstraction for chat sessions.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from sim.webapp.backend.session_models import ChatSession


class SessionStore(ABC):
    """Abstract persistence contract for chat sessions."""

    @abstractmethod
    def load_session(self, session_id: str) -> Optional[ChatSession]:
        """Load a session by ID."""

    @abstractmethod
    def list_sessions(self) -> list[ChatSession]:
        """List all persisted sessions."""

    @abstractmethod
    def save_session(self, session: ChatSession) -> None:
        """Persist a full session snapshot."""

    @abstractmethod
    def delete_session(self, session_id: str) -> bool:
        """Delete a persisted session."""
