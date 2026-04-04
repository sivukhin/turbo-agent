"""Tests for ClaudeStreamHandler — parses Claude Code stream-json lines into conversation events."""

import json
import os
import tempfile
import pytest
from workflows import (
    workflow,
    shell_stream_start,
    shell_stream_next,
    Engine,
    EngineConfig,
    Store,
)
from workflows.isolation import HostIsolation
from workflows.events import ConvAppendRequest, ConvAppendResult


def _make_stream_json(*events):
    """Build a shell command that echoes stream-json lines."""
    lines = [json.dumps(e) for e in events]
    echos = "; ".join(f"echo {repr(line)}" for line in lines)
    return echos


@workflow
def claude_stream_workflow(shell_cmd):
    stream = yield shell_stream_start(
        shell_cmd,
        isolation=HostIsolation(),
        meta={"claude_code": True},
    )
    while True:
        raw = yield shell_stream_next(stream)
        if raw.finished:
            break
    return "done"


@workflow
def non_claude_stream_workflow(shell_cmd):
    """Same but without claude_code meta — handler should ignore it."""
    stream = yield shell_stream_start(
        shell_cmd,
        isolation=HostIsolation(),
    )
    while True:
        raw = yield shell_stream_next(stream)
        if raw.finished:
            break
    return "done"


REGISTRY = {
    "claude_stream": claude_stream_workflow,
    "non_claude_stream": non_claude_stream_workflow,
}


@pytest.fixture
def engine_and_store():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    store = Store(path)
    engine = Engine(EngineConfig(workflows_registry=REGISTRY))
    yield engine, store
    store.close()
    os.unlink(path)


def _run(engine, store, eid, max_steps=200):
    for _ in range(max_steps):
        state, _ = store.load_state(eid)
        if state.finished:
            return state
        engine.step(store, eid)
    raise RuntimeError("Did not finish")


def _conv_requests(store, eid):
    """Get all ConvAppendRequest outbox events."""
    outbox = store.read_outbox(eid)
    return [e for e in outbox if isinstance(e.payload, ConvAppendRequest)]


def _conv_results(store, eid):
    """Get all ConvAppendResult inbox events."""
    inbox = store.read_inbox(eid)
    return [e for e in inbox if isinstance(e.payload, ConvAppendResult)]


class TestClaudeStreamHandler:
    def test_assistant_text_emits_conv_request(self, engine_and_store):
        engine, store = engine_and_store
        cmd = _make_stream_json(
            {
                "type": "assistant",
                "message": {"content": [{"type": "text", "text": "Hello world"}]},
            },
        )
        eid = engine.start(store, "claude_stream", [cmd], workdir=tempfile.mkdtemp())
        _run(engine, store, eid)

        requests = _conv_requests(store, eid)
        assert any(
            r.payload.role == "assistant" and "Hello world" in r.payload.content
            for r in requests
        )
        # Should also have been processed into results
        results = _conv_results(store, eid)
        assert any(r.payload.role == "assistant" for r in results)

    def test_system_init_emits_conv_request(self, engine_and_store):
        engine, store = engine_and_store
        cmd = _make_stream_json(
            {"type": "system", "subtype": "init", "model": "claude-test"},
        )
        eid = engine.start(store, "claude_stream", [cmd], workdir=tempfile.mkdtemp())
        _run(engine, store, eid)

        requests = _conv_requests(store, eid)
        assert len(requests) >= 1
        assert requests[0].payload.role == "assistant"
        assert "claude-test" in requests[0].payload.content

    def test_tool_use_emits_conv_request(self, engine_and_store):
        engine, store = engine_and_store
        cmd = _make_stream_json(
            {
                "type": "assistant",
                "message": {
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "tool-1",
                            "name": "bash",
                            "input": {"command": "ls"},
                        },
                    ]
                },
            },
        )
        eid = engine.start(store, "claude_stream", [cmd], workdir=tempfile.mkdtemp())
        _run(engine, store, eid)

        requests = _conv_requests(store, eid)
        tool_uses = [r for r in requests if r.payload.role == "tool_use"]
        assert len(tool_uses) >= 1

    def test_tool_result_emits_conv_request(self, engine_and_store):
        engine, store = engine_and_store
        cmd = _make_stream_json(
            {
                "type": "user",
                "message": {
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": "tool-1",
                            "content": "file1.py\nfile2.py",
                        },
                    ]
                },
            },
        )
        eid = engine.start(store, "claude_stream", [cmd], workdir=tempfile.mkdtemp())
        _run(engine, store, eid)

        requests = _conv_requests(store, eid)
        tool_results = [r for r in requests if r.payload.role == "tool_result"]
        assert len(tool_results) >= 1

    def test_non_claude_stream_ignored(self, engine_and_store):
        """Streams without claude_code meta should not produce conv events."""
        engine, store = engine_and_store
        cmd = _make_stream_json(
            {
                "type": "assistant",
                "message": {"content": [{"type": "text", "text": "ignored"}]},
            },
        )
        eid = engine.start(
            store, "non_claude_stream", [cmd], workdir=tempfile.mkdtemp()
        )
        _run(engine, store, eid)

        requests = _conv_requests(store, eid)
        assert len(requests) == 0

    def test_result_event_no_hidden_label(self, engine_and_store):
        """The final result event should not have hidden label."""
        engine, store = engine_and_store
        cmd = _make_stream_json(
            {"type": "result", "result": "Final answer"},
        )
        eid = engine.start(store, "claude_stream", [cmd], workdir=tempfile.mkdtemp())
        _run(engine, store, eid)

        requests = _conv_requests(store, eid)
        assert len(requests) >= 1
        result_reqs = [r for r in requests if "Final answer" in r.payload.content]
        assert len(result_reqs) == 1
        assert result_reqs[0].payload.meta.get("labels", "") == ""
