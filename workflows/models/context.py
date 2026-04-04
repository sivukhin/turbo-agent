"""Operation handler context."""

from __future__ import annotations
from dataclasses import dataclass

from workflows.models.state import WorkflowState, ExecutionState, Event
from workflows.store import Store


@dataclass
class OpContext:
    """Context passed to operation handlers."""
    execution_id: str
    workflow_id: str
    wf: WorkflowState
    state: ExecutionState
    store: Store | None
    new_events: list[Event]
    now: float
    workflow_event_handlers: dict | None = None
