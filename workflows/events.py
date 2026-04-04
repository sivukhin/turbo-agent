"""Typed event payloads for the inbox/outbox event log.

Every event payload is a dataclass. JSON serialization includes a `_type`
discriminator so payloads can be reconstructed from storage.
"""

import json
import types
from dataclasses import fields, asdict, is_dataclass

from workflows.models.state import _to_snake

from workflows.models.events import (  # noqa: F401
    WorkflowYielded,
    WorkflowFinished,
    ShellRequest,
    ShellResult,
    ShellStreamStartRequest,
    ShellStreamStartResult,
    ShellStreamNextRequest,
    ShellStreamLineEvent,
    FileReadRequest,
    FileReadResult,
    FileWriteRequest,
    FileWriteResult,
    WaitStarted,
    SleepStarted,
    WorkflowSpawned,
    LlmRequest,
    LlmResponse,
    UserPromptRequest,
    UserPromptResult,
    AiResponseEvent,
    ConvAppendRequest,
    ConvAppendResult,
    ConvListRequest,
    ConvListResult,
    ConvReadRequest,
    ConvReadResult,
    ConvReplaceWithRequest,
    ConvReplaceWithResult,
)


# ---- registry ----


_ALL_PAYLOADS = [
    WorkflowYielded,
    WorkflowFinished,
    ShellRequest,
    ShellResult,
    ShellStreamStartRequest,
    ShellStreamStartResult,
    ShellStreamNextRequest,
    ShellStreamLineEvent,
    FileReadRequest,
    FileReadResult,
    FileWriteRequest,
    FileWriteResult,
    WaitStarted,
    SleepStarted,
    WorkflowSpawned,
    LlmRequest,
    LlmResponse,
    UserPromptRequest,
    UserPromptResult,
    AiResponseEvent,
    ConvAppendRequest,
    ConvAppendResult,
    ConvListRequest,
    ConvListResult,
    ConvReadRequest,
    ConvReadResult,
    ConvReplaceWithRequest,
    ConvReplaceWithResult,
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
    data["_type"] = payload_type_name(payload)
    return json.dumps(data, default=str)


def _unwrap_optional(tp):
    """Unwrap X | None to X."""
    if isinstance(tp, types.UnionType):
        args = [a for a in tp.__args__ if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return tp


def _reconstruct(tp, val):
    """Reconstruct a value from JSON-deserialized data using type hints."""
    tp = _unwrap_optional(tp)
    if isinstance(val, dict) and is_dataclass(tp):
        return tp(**val)
    if isinstance(val, list) and hasattr(tp, "__args__"):
        item_tp = tp.__args__[0] if tp.__args__ else None
        if item_tp and is_dataclass(item_tp):
            return [item_tp(**v) if isinstance(v, dict) else v for v in val]
    return val


def deserialize_payload(json_str: str):
    """Deserialize JSON with _type discriminator back to a payload dataclass."""
    data = json.loads(json_str)
    type_name = data.pop("_type")
    cls = PAYLOAD_REGISTRY.get(type_name)
    if cls is None:
        raise ValueError(f"Unknown payload type: {type_name}")
    filtered = {}
    for f in fields(cls):
        if f.name not in data:
            continue
        filtered[f.name] = _reconstruct(f.type, data[f.name])
    return cls(**filtered)
