"""Operation dataclasses yielded by workflows."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Literal, TypedDict

from workflows.isolation.base import Isolation
from workflows.models.conversation import ConversationRef, MessageRef


class ChatMessage(TypedDict, total=False):
    role: str
    content: str
    meta: dict


@dataclass
class WaitOp:
    deps: list[str]
    mode: Literal["wait", "wait_all", "wait_any"]


@dataclass
class SleepOp:
    seconds: float


@dataclass
class ShellOp:
    command: str
    isolation: Isolation
    public_env: dict[str, str] | None = None
    private_env: dict[str, str] | None = None


@dataclass
class ShellStreamStartOp:
    command: str
    isolation: Isolation
    public_env: dict[str, str] | None = None
    private_env: dict[str, str] | None = None


@dataclass
class ShellStreamNextOp:
    stream_id: str
    private_env: dict[str, str] | None = None


@dataclass
class ReadFileOp:
    path: str


@dataclass
class WriteFileOp:
    path: str
    content: str


@dataclass
class UserPromptOp:
    pass


@dataclass
class AiResponseOp:
    text: str


@dataclass
class AiOp:
    messages: list[ChatMessage] | None = None
    conversation: ConversationRef | None = None
    model: str = "anthropic/claude-sonnet-4-20250514"
    max_tokens: int | None = None
    temperature: float = 0.0
    system: str | None = None
    tools: list[dict] | None = None


@dataclass
class ConvAppendOp:
    role: str
    content: str | dict | list
    meta: dict = field(default_factory=dict)


@dataclass
class ConvListOp:
    """List message refs in range. Returns [MessageRef]."""

    conversation: ConversationRef | None = None
    start_message_id: str | None = None
    end_message_id: str | None = None
    role_filter: str | None = None
    pattern: str | None = None  # LIKE pattern on content


@dataclass
class ConvReadOp:
    """Read messages by refs. Returns [Message]."""

    refs: list[MessageRef]


@dataclass
class ConvReplaceWithOp:
    new_messages: list[ChatMessage]
    start_ref: MessageRef | None = None
    end_ref: MessageRef | None = None
