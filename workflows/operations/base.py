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


# Registry: op_type → handler class
_HANDLERS: dict[type, type] = {}


def register_handler(op_type):
    """Decorator to register an op handler for a specific op type.

    Usage:
        @register_handler(ShellOp)
        class ShellOpHandler:
            @staticmethod
            def handle(val, ctx): ...
    """
    def decorator(cls):
        _HANDLERS[op_type] = cls
        return cls
    return decorator


def handle_op(val, ctx: OpContext) -> bool:
    """Try to handle an op. Returns True if handled, False otherwise."""
    handler = _HANDLERS.get(type(val))
    if handler:
        handler.handle(val, ctx)
        return True
    return False
