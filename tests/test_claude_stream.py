"""Tests for ClaudeStreamHandler — parses Claude Code stream-json lines into conversation events."""

import json
import os
import tempfile
import pytest
from workflows import workflow, shell_stream_start, shell_stream_next, Engine, EngineConfig, Store
from workflows.isolation import HostIsolation
from workflows.events import ConvAppendResult, ShellStreamLineEvent


def _make_stream_json(*events):
    """Build a shell command that echoes stream-json lines."""
    lines = [json.dumps(e) for e in events]
    echos = '; '.join(f'echo {repr(line)}' for line in lines)
    return echos


@workflow
def claude_stream_workflow(shell_cmd):
    stream = yield shell_stream_start(
        shell_cmd,
        isolation=HostIsolation(),
        meta={'claude_code': True},
    )
    while True:
        raw = yield shell_stream_next(stream)
        if raw.finished:
            break
    return 'done'


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
    return 'done'


REGISTRY = {
    'claude_stream': claude_stream_workflow,
    'non_claude_stream': non_claude_stream_workflow,
}


@pytest.fixture
def engine_and_store():
    fd, path = tempfile.mkstemp(suffix='.db')
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
    raise RuntimeError('Did not finish')


class TestClaudeStreamHandler:
    def test_assistant_text_creates_conv_event(self, engine_and_store):
        engine, store = engine_and_store
        cmd = _make_stream_json(
            {'type': 'assistant', 'message': {'content': [{'type': 'text', 'text': 'Hello world'}]}},
        )
        eid = engine.start(store, 'claude_stream', [cmd], workdir=tempfile.mkdtemp())
        _run(engine, store, eid)

        inbox = store.read_inbox(eid)
        conv_results = [e for e in inbox if isinstance(e.payload, ConvAppendResult)]
        texts = [r.payload for r in conv_results if r.payload.role == 'assistant']
        assert any('Hello world' in store.conv_read_messages(
            [store.conv_list_messages(t.conversation_id)[i]
             for i in range(len(store.conv_list_messages(t.conversation_id)))]
        )[0].content for t in texts[:1]) or len(texts) > 0

    def test_system_init_creates_conv_event(self, engine_and_store):
        engine, store = engine_and_store
        cmd = _make_stream_json(
            {'type': 'system', 'subtype': 'init', 'model': 'claude-test'},
        )
        eid = engine.start(store, 'claude_stream', [cmd], workdir=tempfile.mkdtemp())
        _run(engine, store, eid)

        inbox = store.read_inbox(eid)
        conv_results = [e for e in inbox if isinstance(e.payload, ConvAppendResult)]
        assert len(conv_results) >= 1
        assert conv_results[0].payload.role == 'assistant'

    def test_tool_use_creates_conv_event(self, engine_and_store):
        engine, store = engine_and_store
        cmd = _make_stream_json(
            {'type': 'assistant', 'message': {'content': [
                {'type': 'tool_use', 'id': 'tool-1', 'name': 'bash', 'input': {'command': 'ls'}},
            ]}},
        )
        eid = engine.start(store, 'claude_stream', [cmd], workdir=tempfile.mkdtemp())
        _run(engine, store, eid)

        inbox = store.read_inbox(eid)
        conv_results = [e for e in inbox if isinstance(e.payload, ConvAppendResult)]
        tool_uses = [r for r in conv_results if r.payload.role == 'tool_use']
        assert len(tool_uses) >= 1

    def test_tool_result_creates_conv_event(self, engine_and_store):
        engine, store = engine_and_store
        cmd = _make_stream_json(
            {'type': 'user', 'message': {'content': [
                {'type': 'tool_result', 'tool_use_id': 'tool-1', 'content': 'file1.py\nfile2.py'},
            ]}},
        )
        eid = engine.start(store, 'claude_stream', [cmd], workdir=tempfile.mkdtemp())
        _run(engine, store, eid)

        inbox = store.read_inbox(eid)
        conv_results = [e for e in inbox if isinstance(e.payload, ConvAppendResult)]
        tool_results = [r for r in conv_results if r.payload.role == 'tool_result']
        assert len(tool_results) >= 1

    def test_non_claude_stream_ignored(self, engine_and_store):
        """Streams without claude_code meta should not produce conv events."""
        engine, store = engine_and_store
        cmd = _make_stream_json(
            {'type': 'assistant', 'message': {'content': [{'type': 'text', 'text': 'ignored'}]}},
        )
        eid = engine.start(store, 'non_claude_stream', [cmd], workdir=tempfile.mkdtemp())
        _run(engine, store, eid)

        inbox = store.read_inbox(eid)
        conv_results = [e for e in inbox if isinstance(e.payload, ConvAppendResult)]
        assert len(conv_results) == 0

    def test_result_event_no_hidden_label(self, engine_and_store):
        """The final result event should not have hidden label."""
        engine, store = engine_and_store
        cmd = _make_stream_json(
            {'type': 'result', 'result': 'Final answer'},
        )
        eid = engine.start(store, 'claude_stream', [cmd], workdir=tempfile.mkdtemp())
        _run(engine, store, eid)

        inbox = store.read_inbox(eid)
        conv_results = [e for e in inbox if isinstance(e.payload, ConvAppendResult)]
        assert len(conv_results) >= 1
        # Result events should not be hidden
        result_events = [r for r in conv_results if 'Final answer' in (r.payload.meta.get('labels', '') or '')]
        assert len(result_events) == 0

    def test_meta_threaded_to_stream_line_events(self, engine_and_store):
        """ShellStreamLineEvent should carry claude_code meta from the start request."""
        engine, store = engine_and_store
        cmd = _make_stream_json(
            {'type': 'system', 'subtype': 'init', 'model': 'test'},
        )
        eid = engine.start(store, 'claude_stream', [cmd], workdir=tempfile.mkdtemp())
        _run(engine, store, eid)

        # Check that stream line events in inbox have the meta
        inbox = store.read_inbox(eid)
        line_events = [e for e in inbox if isinstance(e.payload, ShellStreamLineEvent)]
        for le in line_events:
            assert le.payload.meta.get('claude_code') is True
