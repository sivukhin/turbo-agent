"""Tests for shell_stream_start / shell_stream_next operations."""

import os
import tempfile
import pytest
from workflows import (
    workflow,
    Engine,
    EngineConfig,
    Store,
    shell,
    shell_stream_start,
    shell_stream_next,
    ShellStreamLine,
)
from workflows.isolation import HostIsolation


@workflow
def stream_echo():
    stream = yield shell_stream_start(
        'echo "line1"; echo "line2"; echo "line3"', isolation=HostIsolation()
    )
    lines = []
    line = yield shell_stream_next(stream)
    while not line.finished:
        lines.extend(line.stdout)
        line = yield shell_stream_next(stream)
    lines.extend(line.stdout)  # include final stdout
    return lines


@workflow
def stream_exit_code():
    stream = yield shell_stream_start("echo ok; exit 42", isolation=HostIsolation())
    line = yield shell_stream_next(stream)
    while not line.finished:
        line = yield shell_stream_next(stream)
    return line.exit_code


@workflow
def stream_stderr():
    stream = yield shell_stream_start(
        "echo out1; echo err1 >&2; echo out2; echo err2 >&2", isolation=HostIsolation()
    )
    stderr_lines = []
    line = yield shell_stream_next(stream)
    while not line.finished:
        stderr_lines.extend(line.stderr)
        line = yield shell_stream_next(stream)
    stderr_lines.extend(line.stderr)  # include final stderr
    return stderr_lines


@workflow
def stream_all():
    """Collect all stdout and stderr."""
    stream = yield shell_stream_start(
        "echo out1; echo err1 >&2; echo out2; echo err2 >&2", isolation=HostIsolation()
    )
    stdout = []
    stderr = []
    line = yield shell_stream_next(stream)
    while not line.finished:
        stdout.extend(line.stdout)
        stderr.extend(line.stderr)
        line = yield shell_stream_next(stream)
    stdout.extend(line.stdout)
    stderr.extend(line.stderr)
    return {"stdout": stdout, "stderr": stderr, "exit_code": line.exit_code}


@workflow
def shell_with_meta():
    result = yield shell('echo hi', isolation=HostIsolation(), meta={'tag': 'build'})
    return result


@workflow
def stream_with_meta():
    stream = yield shell_stream_start(
        'echo "hello"', isolation=HostIsolation(), meta={'source': 'test'}
    )
    line = yield shell_stream_next(stream)
    while not line.finished:
        line = yield shell_stream_next(stream)
    return 'done'


@pytest.fixture
def engine_and_store():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    store = Store(path)
    registry = {
        "stream_echo": stream_echo,
        "stream_exit_code": stream_exit_code,
        "stream_stderr": stream_stderr,
        "stream_all": stream_all,
        "shell_with_meta": shell_with_meta,
        "stream_with_meta": stream_with_meta,
    }
    engine = Engine(EngineConfig(workflows_registry=registry))
    yield engine, store
    store.close()
    os.unlink(path)


def _run_to_completion(engine, store, execution_id, max_steps=200):
    for _ in range(max_steps):
        state, _ = store.load_state(execution_id)
        if state.finished:
            return state
        engine.step(store, execution_id)
    raise RuntimeError("Did not finish")


class TestShellStream:
    def test_basic_stream(self, engine_and_store):
        engine, store = engine_and_store
        eid = engine.start(store, "stream_echo", [], workdir=tempfile.mkdtemp())
        state = _run_to_completion(engine, store, eid)
        root = state.workflows[state.root_workflow_id]
        assert root.result == ["line1", "line2", "line3"]

    def test_stream_exit_code(self, engine_and_store):
        engine, store = engine_and_store
        eid = engine.start(store, "stream_exit_code", [], workdir=tempfile.mkdtemp())
        state = _run_to_completion(engine, store, eid)
        root = state.workflows[state.root_workflow_id]
        assert root.result == 42

    def test_stream_stderr(self, engine_and_store):
        engine, store = engine_and_store
        eid = engine.start(store, "stream_stderr", [], workdir=tempfile.mkdtemp())
        state = _run_to_completion(engine, store, eid)
        root = state.workflows[state.root_workflow_id]
        assert root.result == ["err1", "err2"]

    def test_stream_all(self, engine_and_store):
        engine, store = engine_and_store
        eid = engine.start(store, "stream_all", [], workdir=tempfile.mkdtemp())
        state = _run_to_completion(engine, store, eid)
        root = state.workflows[state.root_workflow_id]
        assert root.result["stdout"] == ["out1", "out2"]
        assert root.result["stderr"] == ["err1", "err2"]
        assert root.result["exit_code"] == 0

    def test_stream_id_is_string(self, engine_and_store):
        engine, store = engine_and_store
        eid = engine.start(store, "stream_echo", [], workdir=tempfile.mkdtemp())
        state, _ = store.load_state(eid)
        assert len(state.streams) == 1
        stream_id = list(state.streams.keys())[0]
        assert isinstance(stream_id, str)
        assert len(stream_id) == 20

    def test_finished_includes_final_lines(self, engine_and_store):
        """The finished=True line should include any remaining stderr."""
        engine, store = engine_and_store
        eid = engine.start(store, "stream_stderr", [], workdir=tempfile.mkdtemp())
        state = _run_to_completion(engine, store, eid)
        # stderr is buffered and delivered with the sentinel
        root = state.workflows[state.root_workflow_id]
        assert len(root.result) > 0

    def test_stream_next_inherits_meta(self, engine_and_store):
        """ShellStreamNextRequest should inherit meta from the start operation."""
        engine, store = engine_and_store
        eid = engine.start(store, "stream_with_meta", [], workdir=tempfile.mkdtemp())
        state = _run_to_completion(engine, store, eid)
        outbox = store.read_outbox(eid)
        from workflows.events import ShellStreamStartRequest, ShellStreamNextRequest
        start_events = [e for e in outbox if isinstance(e.payload, ShellStreamStartRequest)]
        next_events = [e for e in outbox if isinstance(e.payload, ShellStreamNextRequest)]
        assert len(start_events) >= 1
        assert start_events[0].payload.meta == {'source': 'test'}
        assert len(next_events) >= 1
        for e in next_events:
            assert e.payload.meta.get('source') == 'test'

    def test_shell_meta_propagates_to_result(self, engine_and_store):
        """ShellResult should inherit meta from ShellRequest."""
        engine, store = engine_and_store
        eid = engine.start(store, "shell_with_meta", [], workdir=tempfile.mkdtemp())
        state = _run_to_completion(engine, store, eid)
        assert state.finished
        from workflows.events import ShellRequest, ShellResult
        outbox = store.read_outbox(eid)
        inbox = store.read_inbox(eid)
        requests = [e for e in outbox if isinstance(e.payload, ShellRequest)]
        results = [e for e in inbox if isinstance(e.payload, ShellResult)]
        assert len(requests) == 1
        assert requests[0].payload.meta == {'tag': 'build'}
        assert len(results) == 1
        assert results[0].payload.meta == {'tag': 'build'}
