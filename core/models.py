"""Domain models shared across the application."""

from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class ChatSession:
    """Represents a chat session with metadata."""

    session_id: str
    title: str
    created_at: datetime
    messages: List[Dict[str, str]]
    data_source: Optional[Dict[str, Any]] = None
    runtime_context: Optional[Dict[str, Any]] = None

    def add_message(self, role: str, content: str) -> None:
        """Add a message to the session."""
        if not role or not content:
            return
        self.messages.append({"role": role, "content": content})

    def get_history(self) -> List[Dict[str, str]]:
        """Return full message history."""
        return list(self.messages)

    def get_last_n(self, n: int = 10) -> List[Dict[str, str]]:
        """Return the last n messages."""
        if n <= 0:
            return []
        return self.messages[-n:]

    def clear_messages(self) -> None:
        """Clear all messages in the session."""
        self.messages.clear()


@dataclass
class Project:
    """Represents a project that contains multiple chat sessions."""

    project_id: str
    title: str
    description: str
    created_at: datetime
    chats: Dict[str, ChatSession] = field(default_factory=dict)
    data_source: Optional[Dict[str, Any]] = None
    semantic_layer: Optional[Dict[str, Any]] = None

    def add_chat(self, chat: ChatSession) -> None:
        """Add a chat session to the project."""
        self.chats[chat.session_id] = chat

    def get_chat(self, chat_id: str) -> Optional[ChatSession]:
        """Get a chat session by ID."""
        return self.chats.get(chat_id)

    def get_all_chats(self) -> List[ChatSession]:
        """Get all chat sessions in the project."""
        return list(self.chats.values())

    def delete_chat(self, chat_id: str) -> bool:
        """Delete a chat session from the project."""
        if chat_id in self.chats:
            del self.chats[chat_id]
            return True
        return False
