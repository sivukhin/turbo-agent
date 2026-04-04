"""Base operation handler interface."""

from dataclasses import dataclass
from typing import Protocol


@dataclass
class OpContext:
    """Context passed to operation handlers."""
    execution_id: str
    workflow_id: str
    wf: object          # WorkflowState
    state: object       # ExecutionState
    store: object       # Store (may be None)
    new_events: list    # list of Event — handlers append to this
    now: float
    workflow_event_handlers: dict = None  # {name: WorkflowEventHandler}


class OpHandler(Protocol):
    """Protocol for operation handlers."""
    @staticmethod
    def handle(val, ctx: OpContext) -> None: ...


def op_handler(op_type):
    """Decorator that tags a handler class with the op type it handles.

    Usage:
        @op_handler(ShellOp)
        class ShellOpHandler:
            @staticmethod
            def handle(val, ctx): ...
    """
    def decorator(cls):
        cls._op_type = op_type
        return cls
    return decorator


