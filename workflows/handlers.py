"""Event handler system.

Two types of handlers:

1. EventHandler — global, targets an event type, can read/write DB and emit events.
2. WorkflowEventHandler — attached to a waiting workflow, accumulates state from
   events, has resolve() which can switch the workflow back to running.
"""

from dataclasses import dataclass
from typing import Protocol
from workflows.events import WorkflowFinished


# ---- Base protocols ----

class EventHandler(Protocol):
    """Global event handler. Targets a specific event type."""
    @staticmethod
    def event_type() -> str: ...
    @staticmethod
    def handle(event, store) -> list: ...  # returns new events to emit


class WorkflowEventHandler(Protocol):
    """Handler attached to a waiting workflow."""
    @staticmethod
    def initial_state(*args) -> dict: ...
    @staticmethod
    def on_event(event_type: str, event_workflow_id: str | None,
                 payload, state: dict) -> dict: ...
    @staticmethod
    def resolve(state: dict, wf, now: float) -> bool:
        """Try to resolve. If resolved, mutate wf (set status, send_val) and return True."""
        ...


# ---- WorkflowEventHandler implementations ----

class WaitHandler:
    @staticmethod
    def initial_state(deps):
        return {'dep': deps[0], 'result': None, 'resolved': False}

    @staticmethod
    def on_event(event_type, event_workflow_id, payload, state):
        if event_type == 'workflow_finished' and event_workflow_id == state['dep']:
            result = payload.result if isinstance(payload, WorkflowFinished) else payload
            return {**state, 'result': result, 'resolved': True}
        return state

    @staticmethod
    def resolve(state, wf, now):
        if state['resolved']:
            wf.status = 'running'
            wf.send_val = state['result']
            return True
        return False


class WaitAllHandler:
    @staticmethod
    def initial_state(deps):
        return {'deps': list(deps), 'results': {}}

    @staticmethod
    def on_event(event_type, event_workflow_id, payload, state):
        if event_type == 'workflow_finished' and event_workflow_id in state['deps']:
            result = payload.result if isinstance(payload, WorkflowFinished) else payload
            results = {**state['results'], event_workflow_id: result}
            return {**state, 'results': results}
        return state

    @staticmethod
    def resolve(state, wf, now):
        if all(d in state['results'] for d in state['deps']):
            ordered = [state['results'][d] for d in state['deps']]
            wf.status = 'running'
            wf.send_val = ordered
            return True
        return False


class WaitAnyHandler:
    @staticmethod
    def initial_state(deps):
        return {'deps': list(deps), 'results': {}}

    @staticmethod
    def on_event(event_type, event_workflow_id, payload, state):
        if event_type == 'workflow_finished' and event_workflow_id in state['deps']:
            result = payload.result if isinstance(payload, WorkflowFinished) else payload
            results = {**state['results'], event_workflow_id: result}
            return {**state, 'results': results}
        return state

    @staticmethod
    def resolve(state, wf, now):
        if state['results']:
            result = [
                (True, state['results'][d]) if d in state['results'] else (False, None)
                for d in state['deps']
            ]
            wf.status = 'running'
            wf.send_val = result
            return True
        return False


class SleepHandler:
    @staticmethod
    def initial_state(wake_at):
        return {'wake_at': wake_at}

    @staticmethod
    def on_event(event_type, event_workflow_id, payload, state):
        return state

    @staticmethod
    def resolve(state, wf, now):
        if now >= state['wake_at']:
            wf.status = 'running'
            wf.send_val = None
            return True
        return False


# ---- Registry ----

HANDLER_REGISTRY = {
    'wait': WaitHandler,
    'wait_all': WaitAllHandler,
    'wait_any': WaitAnyHandler,
    'sleep': SleepHandler,
}
