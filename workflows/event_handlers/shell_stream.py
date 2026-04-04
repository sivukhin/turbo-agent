import os
import subprocess
import threading
import queue
from pathlib import Path
from workflows.isolation.host import HostIsolation
from workflows.isolation.docker import DockerIsolation
from workflows.event_handlers.base import (
    resolve_wf,
    make_inbox_event,
    register_event_handler,
)
from workflows.models.handler_state import StreamNextState
from workflows.models.state import HandlerState
from workflows.operations.shell_stream_op import _stream_private_envs
import workflows.events as ev


# Active streams: stream_id → queue of (text, stream) tuples
# Sentinel: ('', '', exit_code)
_active_streams: dict[str, queue.Queue] = {}
_streams_lock = threading.Lock()


def _build_cmd(iso_type, iso_config, workdir, command, env):
    if iso_type == "docker":
        iso = iso_config or DockerIsolation()
        cmd = [
            "docker",
            "run",
            "--rm",
            f"--network={iso.network}",
            "-v",
            f"{workdir.resolve()}:/workspace",
            "-w",
            "/workspace",
        ]
        for k, v in (env or {}).items():
            cmd.extend(["-e", f"{k}={v}"])
        cmd.extend([iso.image, "sh", "-c", command])
        return cmd, {}
    else:
        return ["sh", "-c", command], {"cwd": str(workdir), "env": env or None}


def _stream_worker(proc, q):
    """Push stdout lines one at a time. Collect stderr and include in sentinel."""
    stderr_lines = []

    def read_stderr():
        for line in proc.stderr:
            stderr_lines.append(line.rstrip("\n"))

    t = threading.Thread(target=read_stderr, daemon=True)
    t.start()
    while True:
        line = proc.stdout.readline()
        if not line:
            break
        q.put(([line.rstrip("\n")], []))
    t.join()
    exit_code = proc.wait()
    q.put(([], stderr_lines, exit_code))


def _ensure_stream(stream_id, command, iso_type, iso_config, env, workdir):
    """Start the stream process if not already running."""
    with _streams_lock:
        if stream_id in _active_streams:
            return
        cmd, kwargs = _build_cmd(iso_type, iso_config, workdir, command, env)
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,  # equivalent to Setpgid: isolate from parent signals
            **kwargs,
        )
        q = queue.Queue()
        _active_streams[stream_id] = q
        threading.Thread(target=_stream_worker, args=(proc, q), daemon=True).start()


@register_event_handler(ev.ShellStreamStartRequest)
class ShellStreamStartRequestHandler:
    def handle(self, event, store, state):
        payload = event.payload
        wf = state.workflows.get(event.workflow_id)
        if not wf or not wf.workdir:
            return []

        private_env = _stream_private_envs.get(payload.stream_id, {})
        merged_env = {**(payload.public_env or {}), **private_env}
        _ensure_stream(
            payload.stream_id,
            payload.command,
            payload.isolation_type,
            payload.isolation_config,
            merged_env or None,
            Path(wf.workdir),
        )

        resolve_wf(state, event.workflow_id, payload.stream_id)
        return [
            make_inbox_event(
                event, ev.ShellStreamStartResult(stream_id=payload.stream_id, meta=payload.meta)
            )
        ]


@register_event_handler(ev.ShellStreamNextRequest)
class ShellStreamNextRequestHandler:
    def handle(self, event, store, state):
        payload = event.payload
        stream_id = payload.stream_id
        wf = state.workflows.get(event.workflow_id)

        # Ensure stream is running (crash recovery: restart from StreamDef)
        with _streams_lock:
            running = stream_id in _active_streams
        if not running:
            stream_def = state.streams.get(stream_id)
            if stream_def and wf and wf.workdir:
                private_env = _stream_private_envs.get(stream_id, {})
                merged_env = {**(stream_def.public_env or {}), **private_env}
                _ensure_stream(
                    stream_id,
                    stream_def.command,
                    stream_def.isolation_type,
                    stream_def.isolation_config,
                    merged_env or None,
                    Path(wf.workdir),
                )

        state.handlers[event.workflow_id] = HandlerState(
            handler_type="stream_next",
            state=StreamNextState(stream_id=stream_id, meta=payload.meta),
        )

        return []
