"""Conversation model dataclasses."""

from dataclasses import dataclass, field


@dataclass
class ConversationRef:
    """Reference to a conversation at a specific point."""

    conversation_id: str
    message_id: str | None = None  # None = latest
    layer: int | None = None  # None = latest


@dataclass
class MessageRef:
    """Reference to a specific message, includes role as metadata."""

    conversation_id: str
    message_id: str
    layer: int
    role: str
    meta: dict = field(
        default_factory=dict
    )  # arbitrary JSON metadata; well-known key: "labels" (comma-separated)
    event_time: int = (
        0  # event_id at creation time — logical clock for correlating with events
    )


@dataclass
class Message:
    """A message with content, returned by read_messages."""

    ref: MessageRef
    content: str

    @property
    def role(self):
        return self.ref.role
