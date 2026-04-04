"""Event payload dataclasses for the inbox/outbox event log."""

from dataclasses import dataclass, field
from typing import Literal

from workflows.isolation.docker import DockerIsolation
from workflows.models.conversation import ConversationRef, MessageRef
from workflows.models.operations import ChatMessage


@dataclass
class WorkflowYielded:
    value: object

@dataclass
class WorkflowFinished:
    result: object

@dataclass
class ShellRequest:
    command: str
    isolation_type: Literal['host', 'docker'] = 'host'
    isolation_config: DockerIsolation | None = None
    public_env: dict[str, str] | None = None

@dataclass
class ShellResult:
    command: str
    exit_code: int
    stdout: str
    stderr: str

@dataclass
class ShellStreamStartRequest:
    stream_id: str
    command: str
    isolation_type: Literal['host', 'docker'] = 'host'
    isolation_config: DockerIsolation | None = None
    public_env: dict[str, str] | None = None

@dataclass
class ShellStreamStartResult:
    stream_id: str

@dataclass
class ShellStreamNextRequest:
    stream_id: str

@dataclass
class ShellStreamLine:
    stream_id: str
    stdout: list[str] = field(default_factory=list)
    stderr: list[str] = field(default_factory=list)
    finished: bool = False
    exit_code: int | None = None

@dataclass
class FileReadRequest:
    path: str

@dataclass
class FileReadResult:
    path: str
    content: str

@dataclass
class FileWriteRequest:
    path: str
    content: str

@dataclass
class FileWriteResult:
    path: str
    size: int

@dataclass
class WaitStarted:
    mode: Literal['wait', 'wait_all', 'wait_any']
    deps: list[str]

@dataclass
class SleepStarted:
    seconds: float
    wake_at: float

@dataclass
class WorkflowSpawned:
    child_workflow_id: str
    name: str
    args: list[object]
    parent_workflow_id: str | None
    storage_mode: Literal['same', 'copy-full', 'copy-git', 'branch']

@dataclass
class LlmRequest:
    """Request to an LLM. Provider-agnostic.
    Either conversation_ref (lightweight) or messages (full) is set, not both."""
    model: str = 'anthropic/claude-sonnet-4-20250514'
    max_tokens: int | None = None
    temperature: float = 0.0
    system: str | None = None
    tools: list[dict] | None = None
    conversation_ref: ConversationRef | None = None

    messages: list[ChatMessage] | None = None

@dataclass
class LlmResponse:
    """Response from an LLM."""
    content: list[dict]             # [{"type": "text", "text": str} | {"type": "tool_use", ...}]
    model: str
    stop_reason: Literal['end_turn', 'tool_use', 'max_tokens', 'stop_sequence'] | None
    usage: dict | None              # {"input_tokens": int, "output_tokens": int}
    text: str = ''                  # concatenated text from text blocks
    tool_calls: list[dict] | None = None  # [{"id": str, "name": str, "input": dict}]
    message_id: str | None = None

@dataclass
class UserPromptRequest:
    request_id: str

@dataclass
class UserPromptResult:
    request_id: str
    response: str

@dataclass
class AiResponseEvent:
    text: str

@dataclass
class ConvAppendRequest:
    conversation_id: str
    role: str
    content: str
    meta: dict = field(default_factory=dict)

@dataclass
class ConvAppendResult:
    conversation_id: str
    message_id: str
    layer: int
    role: str
    meta: dict = field(default_factory=dict)

@dataclass
class ConvListRequest:
    conversation_id: str
    end_message_id: str | None = None
    layer: int | None = None
    start_message_id: str | None = None
    role_filter: str | None = None
    pattern: str | None = None

@dataclass
class ConvListResult:
    count: int
    message_refs: list[MessageRef]

@dataclass
class ConvReadRequest:
    message_refs: list[MessageRef]

@dataclass
class ConvReadResult:
    count: int

@dataclass
class ConvReplaceWithRequest:
    conversation_id: str
    new_messages: list[ChatMessage]
    start_message_id: str | None
    end_message_id: str | None

@dataclass
class ConvReplaceWithResult:
    conversation_id: str
    new_layer: int
    new_message_refs: list[MessageRef]
