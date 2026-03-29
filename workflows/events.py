"""Typed event payloads for the inbox/outbox event log.

Every event payload is a dataclass. JSON serialization includes a `_type`
discriminator so payloads can be reconstructed from storage.
"""

import json
import re
from dataclasses import dataclass, fields, asdict


# ---- payload types ----

@dataclass
class WorkflowYielded:
    value: object

@dataclass
class WorkflowFinished:
    result: object

@dataclass
class ShellRequest:
    command: str
    isolation_type: str = 'host'  # 'host' | 'docker'
    isolation_config: dict | None = None  # docker: {"image": ..., "network": ...}

@dataclass
class ShellResult:
    command: str
    exit_code: int
    stdout: str
    stderr: str

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
    mode: str           # 'wait' | 'wait_all' | 'wait_any'
    deps: list[str]

@dataclass
class SleepStarted:
    seconds: float
    wake_at: float

@dataclass
class WorkflowSpawned:
    child_workflow_id: str
    name: str
    args: list
    parent_workflow_id: str | None
    storage_mode: str   # 'same' | 'copy-full' | 'copy-git' | 'branch'

@dataclass
class LlmRequest:
    """Request to an LLM. Provider-agnostic.
    Either conversation_ref (lightweight) or messages (full) is set, not both."""
    model: str = 'anthropic/claude-sonnet-4-20250514'
    max_tokens: int | None = None
    temperature: float = 0.0
    system: str | None = None
    tools: list | None = None
    conversation_ref: dict | None = None  # {"conversation_id", "message_id", "layer"} if from conversation
    message_count: int | None = None      # number of messages (when using conversation_ref)
    messages: list | None = None          # full messages (only when no conversation)

@dataclass
class LlmResponse:
    """Response from an LLM."""
    content: list               # [{"type": "text", "text": str} | {"type": "tool_use", "id": str, "name": str, "input": dict}]
    model: str
    stop_reason: str | None     # "end_turn"|"tool_use"|"max_tokens"|"stop_sequence"
    usage: dict | None          # {"input_tokens": int, "output_tokens": int}
    text: str = ''              # concatenated text from text blocks
    tool_calls: list | None = None  # [{"id": str, "name": str, "input": dict}] parsed tool calls
    message_id: str | None = None


# ---- User prompt events ----

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


# ---- Conversation events ----

@dataclass
class ConvAppendRequest:
    conversation_id: str
    role: str
    content: str

@dataclass
class ConvAppendResult:
    conversation_id: str
    message_id: str
    layer: int
    role: str

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
    message_refs: list  # [{"conversation_id", "message_id", "layer", "role"}]

@dataclass
class ConvReadRequest:
    message_refs: list  # [{"conversation_id", "message_id", "layer", "role"}]

@dataclass
class ConvReadResult:
    count: int

@dataclass
class ConvReplaceWithRequest:
    conversation_id: str
    new_messages: list
    start_message_id: str | None
    end_message_id: str | None

@dataclass
class ConvReplaceWithResult:
    conversation_id: str
    new_layer: int
    new_message_refs: list


# ---- registry ----

def _to_snake(name: str) -> str:
    """Convert CamelCase to snake_case."""
    s = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    return s.lower()


_ALL_PAYLOADS = [
    WorkflowYielded, WorkflowFinished,
    ShellRequest, ShellResult,
    FileReadRequest, FileReadResult,
    FileWriteRequest, FileWriteResult,
    WaitStarted, SleepStarted,
    WorkflowSpawned,
    LlmRequest, LlmResponse,
    UserPromptRequest, UserPromptResult, AiResponseEvent,
    ConvAppendRequest, ConvAppendResult,
    ConvListRequest, ConvListResult,
    ConvReadRequest, ConvReadResult,
    ConvReplaceWithRequest, ConvReplaceWithResult,
]

PAYLOAD_REGISTRY: dict[str, type] = {
    _to_snake(cls.__name__): cls for cls in _ALL_PAYLOADS
}


def payload_type_name(payload) -> str:
    """Get the type name for a payload instance."""
    return _to_snake(type(payload).__name__)


# ---- serialization ----

def serialize_payload(payload) -> str:
    """Serialize a payload dataclass to JSON with _type discriminator."""
    data = asdict(payload)
    data['_type'] = payload_type_name(payload)
    return json.dumps(data, default=str)


def deserialize_payload(json_str: str):
    """Deserialize JSON with _type discriminator back to a payload dataclass."""
    data = json.loads(json_str)
    type_name = data.pop('_type')
    cls = PAYLOAD_REGISTRY.get(type_name)
    if cls is None:
        raise ValueError(f'Unknown payload type: {type_name}')
    # Only pass fields the dataclass expects
    valid_fields = {f.name for f in fields(cls)}
    filtered = {k: v for k, v in data.items() if k in valid_fields}
    return cls(**filtered)
