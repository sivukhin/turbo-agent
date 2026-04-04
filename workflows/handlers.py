"""Event handler system.

Two types of handlers:

1. EventHandler — global, targets an event type, can read/write DB and emit events.
2. WorkflowEventHandler — attached to a waiting workflow, accumulates state from
   events, has resolve() which can switch the workflow back to running.
"""

from typing import Protocol
from workflows.event_handlers.shell_stream import _active_streams, _streams_lock
from workflows.events import WorkflowFinished
from workflows.events import ShellStreamLineEvent
from workflows.models.handler_state import (
    WaitState,
    WaitAllState,
    WaitAnyState,
    SleepState,
    StreamNextState,
)
from workflows.models.state import ShellStreamLine
from workflows.operations.shell_stream_op import _stream_private_envs


# ---- Base protocols ----


class EventHandler(Protocol):
    """Global event handler. Targets a specific event type."""

    @staticmethod
    def event_type() -> str: ...
    @staticmethod
    def handle(event, store) -> list: ...


class WorkflowEventHandler(Protocol):
    """Handler attached to a waiting workflow."""

    @staticmethod
    def initial_state(*args): ...
    @staticmethod
    def on_event(event_type: str, event_workflow_id: str | None, payload, state): ...
    @staticmethod
    def resolve(state, wf, now: float) -> bool:
        """Try to resolve. If resolved, mutate wf (set status, send_val) and return True."""
        ...


# ---- WorkflowEventHandler implementations ----


class WaitHandler:
    @staticmethod
    def initial_state(deps) -> WaitState:
        return WaitState(dep=deps[0])

    @staticmethod
    def on_event(event_type, event_workflow_id, payload, state: WaitState) -> WaitState:
        if event_type == "workflow_finished" and event_workflow_id == state.dep:
            result = (
                payload.result if isinstance(payload, WorkflowFinished) else payload
            )
            return WaitState(dep=state.dep, result=result, resolved=True)
        return state

    @staticmethod
    def resolve(state: WaitState, wf, now) -> bool:
        if state.resolved:
            wf.status = "running"
            wf.send_val = state.result
            return True
        return False


class WaitAllHandler:
    @staticmethod
    def initial_state(deps) -> WaitAllState:
        return WaitAllState(deps=list(deps))

    @staticmethod
    def on_event(
        event_type, event_workflow_id, payload, state: WaitAllState
    ) -> WaitAllState:
        if event_type == "workflow_finished" and event_workflow_id in state.deps:
            result = (
                payload.result if isinstance(payload, WorkflowFinished) else payload
            )
            results = {**state.results, event_workflow_id: result}
            return WaitAllState(deps=state.deps, results=results)
        return state

    @staticmethod
    def resolve(state: WaitAllState, wf, now) -> bool:
        if all(d in state.results for d in state.deps):
            ordered = [state.results[d] for d in state.deps]
            wf.status = "running"
            wf.send_val = ordered
            return True
        return False


class WaitAnyHandler:
    @staticmethod
    def initial_state(deps) -> WaitAnyState:
        return WaitAnyState(deps=list(deps))

    @staticmethod
    def on_event(
        event_type, event_workflow_id, payload, state: WaitAnyState
    ) -> WaitAnyState:
        if event_type == "workflow_finished" and event_workflow_id in state.deps:
            result = (
                payload.result if isinstance(payload, WorkflowFinished) else payload
            )
            results = {**state.results, event_workflow_id: result}
            return WaitAnyState(deps=state.deps, results=results)
        return state

    @staticmethod
    def resolve(state: WaitAnyState, wf, now) -> bool:
        if state.results:
            result = [
                (True, state.results[d]) if d in state.results else (False, None)
                for d in state.deps
            ]
            wf.status = "running"
            wf.send_val = result
            return True
        return False


class SleepHandler:
    @staticmethod
    def initial_state(wake_at) -> SleepState:
        return SleepState(wake_at=wake_at)

    @staticmethod
    def on_event(
        event_type, event_workflow_id, payload, state: SleepState
    ) -> SleepState:
        return state

    @staticmethod
    def resolve(state: SleepState, wf, now) -> bool:
        if now >= state.wake_at:
            wf.status = "running"
            wf.send_val = None
            return True
        return False


class StreamNextHandler:
    """Polls the stream queue for the next line. Resolves when a line is available."""

    @staticmethod
    def initial_state(stream_id) -> StreamNextState:
        return StreamNextState(stream_id=stream_id)

    @staticmethod
    def on_event(
        event_type, event_workflow_id, payload, state: StreamNextState
    ) -> StreamNextState:
        return state

    @staticmethod
    def resolve(state: StreamNextState, wf, now) -> bool:
        if not state.stream_id:
            return False

        with _streams_lock:
            q = _active_streams.get(state.stream_id)
        if not q:
            # Stream not in memory — wait for event handler to restart it
            return False

        try:
            item = q.get_nowait()
        except Exception:
            return False

        if len(item) == 3:
            stdout_lines, stderr_lines, exit_code = item
            line = ShellStreamLine(
                stdout=stdout_lines,
                stderr=stderr_lines,
                finished=True,
                exit_code=exit_code,
            )
            wf.status = "running"
            wf.send_val = line
            state.emit_events.append(
                ShellStreamLineEvent(
                    stream_id=state.stream_id,
                    stdout=stdout_lines,
                    stderr=stderr_lines,
                    finished=True,
                    exit_code=exit_code,
                    meta=state.meta,
                )
            )
            with _streams_lock:
                _active_streams.pop(state.stream_id, None)
            _stream_private_envs.pop(state.stream_id, None)
            return True

        stdout_lines, stderr_lines = item
        line = ShellStreamLine(stdout=stdout_lines, stderr=stderr_lines, finished=False)
        wf.status = "running"
        wf.send_val = line
        state.emit_events.append(
            ShellStreamLineEvent(
                stream_id=state.stream_id,
                stdout=stdout_lines,
                stderr=stderr_lines,
                finished=False,
                meta=state.meta,
            )
        )
        return True


# ---- Registry ----

HANDLER_REGISTRY = {
    "wait": WaitHandler,
    "wait_all": WaitAllHandler,
    "wait_any": WaitAnyHandler,
    "sleep": SleepHandler,
    "stream_next": StreamNextHandler,
}
