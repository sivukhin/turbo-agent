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
