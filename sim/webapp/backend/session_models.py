"""
Session data models shared by session manager and storage backends.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from sim.rca.engine.agent_engine import AgentRCAEngine


@dataclass
class ChatSession:
    """Represents a chat session."""

    session_id: str
    created_at: datetime
    message_history: list = field(default_factory=list)
    engine: Optional["AgentRCAEngine"] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary (excluding engine)."""
        return {
            "session_id": self.session_id,
            "created_at": self.created_at.isoformat(),
            "message_history": self.message_history,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ChatSession":
        """Create from dictionary."""
        return cls(
            session_id=data["session_id"],
            created_at=datetime.fromisoformat(data["created_at"]),
            message_history=data.get("message_history", []),
            metadata=data.get("metadata", {}),
        )
