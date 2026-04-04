"""Workflow state dataclasses."""

from __future__ import annotations
import re
from dataclasses import dataclass, field
from typing import Literal

from workflows.isolation.base import StorageConfig
from workflows.isolation.docker import DockerIsolation


def _to_snake(name: str) -> str:
    s = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    return s.lower()


@dataclass
class ShellStreamLine:
    """Result returned to workflows from shell stream operations."""
    stdout: list[str]
    stderr: list[str]
    finished: bool = False
    exit_code: int | None = None


@dataclass
class Event:
    event_id: int
    execution_id: str
    workflow_id: str | None
    category: Literal['inbox', 'outbox']
    payload: object
    created_at: float = 0.0

    @property
    def type(self) -> str:
        return _to_snake(type(self.payload).__name__)


@dataclass
class WorkflowHandle:
    id: str
    workflow_name: str
    args: list[object]
    storage: StorageConfig | None = None
    description: str = ''

    def __repr__(self):
        return f'<{self.workflow_name}#{self.id}>'

@dataclass
class WorkflowState:
    name: str
    args: list[object]
    parent_workflow_id: str | None = None
    workdir: str | None = None
    branches: dict[str, str] | None = None
    conversation_id: str | None = None
    checkpoint: dict | None = None
    status: Literal['running', 'waiting', 'finished'] = 'running'
    result: object = None
    send_val: object = field(default=None, repr=False)
    description: str = ''

@dataclass
class HandlerState:
    handler_type: str
    state: dict

@dataclass
class StreamDef:
    """Persisted stream definition — enough info to restart a stream after crash.
    Only public env is stored. Private env (secrets) must be re-supplied."""
    stream_id: str
    command: str
    isolation_type: Literal['host', 'docker']
    isolation_config: DockerIsolation | None
    public_env: dict[str, str] | None
    workflow_id: str

@dataclass
class ExecutionState:
    workflows: dict[str, WorkflowState]
    handlers: dict[str, HandlerState]
    root_workflow_id: str
    source_file: str | None = None
    finished: bool = False
    description: str = ''
    streams: dict[str, StreamDef] = field(default_factory=dict)
