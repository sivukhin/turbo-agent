"""Shared utilities for event handlers."""

from workflows.ops import Event
from workflows.events import payload_type_name


def resolve_wf(state, workflow_id, result):
    """Set workflow back to running with a result."""
    wf = state.workflows.get(workflow_id)
    if wf and wf.status == "waiting":
        wf.status = "running"
        wf.send_val = result
        state.handlers.pop(workflow_id, None)


def make_inbox_event(event, payload):
    """Create an inbox event from an outbox event."""
    return Event(
        event_id=0,
        execution_id=event.execution_id,
        workflow_id=event.workflow_id,
        category="inbox",
        payload=payload,
    )


# Registry: event payload type name → handler instance
_EVENT_HANDLERS: dict[str, object] = {}


def register_event_handler(payload_type):
    """Decorator to register an event handler for a specific event payload type.

    Usage:
        @register_event_handler(ev.ShellRequest)
        class ShellRequestHandler:
            def handle(self, event, store, state): ...
    """
    type_name = payload_type_name(payload_type.__new__(payload_type))
    # For dataclasses that need args, compute name from class directly
    import re

    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", payload_type.__name__)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    type_name = s.lower()

    def decorator(cls):
        cls._event_type_name = type_name
        return cls

    return decorator


def get_event_type_name(handler) -> str:
    """Get the event type name for a handler."""
    return getattr(
        handler, "_event_type_name", getattr(handler, "event_type", lambda: "")()
    )
