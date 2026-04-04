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
    def on_event(
        event_type: str, event_workflow_id: str | None, payload, state: dict
    ) -> dict: ...
    @staticmethod
    def resolve(state: dict, wf, now: float) -> bool:
        """Try to resolve. If resolved, mutate wf (set status, send_val) and return True."""
        ...


# ---- WorkflowEventHandler implementations ----


class WaitHandler:
    @staticmethod
    def initial_state(deps):
        return {"dep": deps[0], "result": None, "resolved": False}

    @staticmethod
    def on_event(event_type, event_workflow_id, payload, state):
        if event_type == "workflow_finished" and event_workflow_id == state["dep"]:
            result = (
                payload.result if isinstance(payload, WorkflowFinished) else payload
            )
            return {**state, "result": result, "resolved": True}
        return state

    @staticmethod
    def resolve(state, wf, now):
        if state["resolved"]:
            wf.status = "running"
            wf.send_val = state["result"]
            return True
        return False


class WaitAllHandler:
    @staticmethod
    def initial_state(deps):
        return {"deps": list(deps), "results": {}}

    @staticmethod
    def on_event(event_type, event_workflow_id, payload, state):
        if event_type == "workflow_finished" and event_workflow_id in state["deps"]:
            result = (
                payload.result if isinstance(payload, WorkflowFinished) else payload
            )
            results = {**state["results"], event_workflow_id: result}
            return {**state, "results": results}
        return state

    @staticmethod
    def resolve(state, wf, now):
        if all(d in state["results"] for d in state["deps"]):
            ordered = [state["results"][d] for d in state["deps"]]
            wf.status = "running"
            wf.send_val = ordered
            return True
        return False


class WaitAnyHandler:
    @staticmethod
    def initial_state(deps):
        return {"deps": list(deps), "results": {}}

    @staticmethod
    def on_event(event_type, event_workflow_id, payload, state):
        if event_type == "workflow_finished" and event_workflow_id in state["deps"]:
            result = (
                payload.result if isinstance(payload, WorkflowFinished) else payload
            )
            results = {**state["results"], event_workflow_id: result}
            return {**state, "results": results}
        return state

    @staticmethod
    def resolve(state, wf, now):
        if state["results"]:
            result = [
                (True, state["results"][d]) if d in state["results"] else (False, None)
                for d in state["deps"]
            ]
            wf.status = "running"
            wf.send_val = result
            return True
        return False


class SleepHandler:
    @staticmethod
    def initial_state(wake_at):
        return {"wake_at": wake_at}

    @staticmethod
    def on_event(event_type, event_workflow_id, payload, state):
        return state

    @staticmethod
    def resolve(state, wf, now):
        if now >= state["wake_at"]:
            wf.status = "running"
            wf.send_val = None
            return True
        return False


class StreamNextHandler:
    """Polls the stream queue for the next line. Resolves when a line is available."""

    @staticmethod
    def initial_state(stream_id):
        return {"stream_id": stream_id}

    @staticmethod
    def on_event(event_type, event_workflow_id, payload, state):
        return state

    @staticmethod
    def resolve(state, wf, now):
        from workflows.ops import ShellStreamLine
        from workflows.events import ShellStreamLine as EvShellStreamLine
        from workflows.event_handlers.shell_stream import _active_streams, _streams_lock

        stream_id = state.get("stream_id")
        if not stream_id:
            return False

        with _streams_lock:
            q = _active_streams.get(stream_id)
        if not q:
            line = ShellStreamLine(stdout=[], stderr=[], finished=True, exit_code=-1)
            wf.status = "running"
            wf.send_val = line
            state.setdefault("_emit_events", []).append(
                EvShellStreamLine(
                    stream_id=stream_id,
                    stdout=[],
                    stderr=[],
                    finished=True,
                    exit_code=-1,
                )
            )
            return True

        try:
            item = q.get_nowait()
        except Exception:
            return False

        if len(item) == 3:
            # Sentinel: (stdout_lines, stderr_lines, exit_code)
            stdout_lines, stderr_lines, exit_code = item
            line = ShellStreamLine(
                stdout=stdout_lines,
                stderr=stderr_lines,
                finished=True,
                exit_code=exit_code,
            )
            wf.status = "running"
            wf.send_val = line
            state.setdefault("_emit_events", []).append(
                EvShellStreamLine(
                    stream_id=stream_id,
                    stdout=stdout_lines,
                    stderr=stderr_lines,
                    finished=True,
                    exit_code=exit_code,
                )
            )
            with _streams_lock:
                _active_streams.pop(stream_id, None)
            from workflows.operations.shell_stream_op import _stream_private_envs

            _stream_private_envs.pop(stream_id, None)
            return True

        # Regular line: (stdout_lines, stderr_lines)
        stdout_lines, stderr_lines = item
        line = ShellStreamLine(stdout=stdout_lines, stderr=stderr_lines, finished=False)
        wf.status = "running"
        wf.send_val = line
        state.setdefault("_emit_events", []).append(
            EvShellStreamLine(
                stream_id=stream_id,
                stdout=stdout_lines,
                stderr=stderr_lines,
                finished=False,
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
