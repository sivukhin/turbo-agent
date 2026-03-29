"""Tests for the conversation system."""

import pytest
from workflows import (
    workflow, wait, conv_append, conv_read, conv_search,
    conv_get, conv_replace_with, llm, Latest, Engine, Store,
)
from workflows.conversation import MessageRef, ConversationMessage


# ---- Store-level tests ----

@pytest.fixture
def store():
    return Store(':memory:')


class TestConversationStore:
    def test_create_and_append(self, store):
        store.create_conversation('c1')
        ref = store.conv_append_message('c1', 'user', 'hello')
        assert ref.conversation_id == 'c1'
        assert ref.layer == 0
        assert ref.message_id  # not empty

    def test_read_messages(self, store):
        store.create_conversation('c1')
        store.conv_append_message('c1', 'user', 'hello')
        store.conv_append_message('c1', 'assistant', 'hi there')
        msgs = store.conv_read_messages('c1')
        assert len(msgs) == 2
        assert msgs[0].role == 'user'
        assert msgs[0].content == 'hello'
        assert msgs[1].role == 'assistant'
        assert msgs[1].content == 'hi there'

    def test_read_preserves_order(self, store):
        store.create_conversation('c1')
        for i in range(10):
            store.conv_append_message('c1', 'user', f'msg {i}')
        msgs = store.conv_read_messages('c1')
        assert [m.content for m in msgs] == [f'msg {i}' for i in range(10)]

    def test_parent_chain(self, store):
        store.create_conversation('parent')
        store.conv_append_message('parent', 'user', 'from parent')
        ref = store.conv_resolve_ref('parent')

        store.create_conversation('child',
            parent_conversation_id='parent',
            parent_message_id=ref.message_id,
            parent_layer=ref.layer)
        store.conv_append_message('child', 'assistant', 'from child')

        msgs = store.conv_read_messages('child')
        assert len(msgs) == 2
        assert msgs[0].content == 'from parent'
        assert msgs[1].content == 'from child'

    def test_parent_chain_three_levels(self, store):
        store.create_conversation('a')
        store.conv_append_message('a', 'user', 'level-a')
        ref_a = store.conv_resolve_ref('a')

        store.create_conversation('b',
            parent_conversation_id='a',
            parent_message_id=ref_a.message_id,
            parent_layer=ref_a.layer)
        store.conv_append_message('b', 'assistant', 'level-b')
        ref_b = store.conv_resolve_ref('b')

        store.create_conversation('c',
            parent_conversation_id='b',
            parent_message_id=ref_b.message_id,
            parent_layer=ref_b.layer)
        store.conv_append_message('c', 'user', 'level-c')

        msgs = store.conv_read_messages('c')
        assert [m.content for m in msgs] == ['level-a', 'level-b', 'level-c']

    def test_layer_compaction(self, store):
        store.create_conversation('c1')
        r1 = store.conv_append_message('c1', 'user', 'old msg 1')
        r2 = store.conv_append_message('c1', 'user', 'old msg 2')
        r3 = store.conv_append_message('c1', 'user', 'keep this')

        new_refs = store.conv_replace_with('c1',
            [{'role': 'system', 'content': 'summary of 1+2'}],
            start_message_id=r1.message_id,
            end_message_id=r2.message_id)

        msgs = store.conv_read_messages('c1')
        assert len(msgs) == 2
        assert msgs[0].content == 'summary of 1+2'
        assert msgs[1].content == 'keep this'

    def test_replace_entire_conversation(self, store):
        store.create_conversation('c1')
        store.conv_append_message('c1', 'user', 'a')
        store.conv_append_message('c1', 'user', 'b')
        store.conv_append_message('c1', 'user', 'c')

        store.conv_replace_with('c1',
            [{'role': 'system', 'content': 'replaced all'}])

        msgs = store.conv_read_messages('c1')
        assert len(msgs) == 1
        assert msgs[0].content == 'replaced all'

    def test_replace_from_start(self, store):
        store.create_conversation('c1')
        store.conv_append_message('c1', 'user', 'a')
        r2 = store.conv_append_message('c1', 'user', 'b')
        store.conv_append_message('c1', 'user', 'c')

        store.conv_replace_with('c1',
            [{'role': 'system', 'content': 'replaced a+b'}],
            end_message_id=r2.message_id)

        msgs = store.conv_read_messages('c1')
        assert len(msgs) == 2
        assert msgs[0].content == 'replaced a+b'
        assert msgs[1].content == 'c'

    def test_search(self, store):
        store.create_conversation('c1')
        store.conv_append_message('c1', 'user', 'hello world')
        store.conv_append_message('c1', 'assistant', 'goodbye world')
        store.conv_append_message('c1', 'user', 'hello again')

        results = store.conv_search_messages('c1', '%hello%')
        assert len(results) == 2

    def test_get_by_refs(self, store):
        store.create_conversation('c1')
        r1 = store.conv_append_message('c1', 'user', 'first')
        r2 = store.conv_append_message('c1', 'user', 'second')
        store.conv_append_message('c1', 'user', 'third')

        results = store.conv_get_messages([r1, r2])
        assert len(results) == 2
        assert results[0].content == 'first'
        assert results[1].content == 'second'

    def test_resolve_ref(self, store):
        store.create_conversation('c1')
        store.conv_append_message('c1', 'user', 'a')
        store.conv_append_message('c1', 'user', 'b')
        ref = store.conv_resolve_ref('c1')
        assert ref.conversation_id == 'c1'
        assert ref.message_id  # not empty
        assert ref.layer == 0


# ---- Engine-level tests ----

@workflow
def chat_workflow():
    yield conv_append(role='user', content='Hello')
    yield conv_append(role='assistant', content='Hi there')
    messages = yield conv_read()
    return [m.content for m in messages]


@workflow
def search_workflow():
    yield conv_append(role='user', content='find this needle')
    yield conv_append(role='user', content='not this one')
    yield conv_append(role='user', content='another needle here')
    results = yield conv_search(pattern='%needle%')
    return len(results)


@workflow
def replace_workflow():
    yield conv_append(role='user', content='old 1')
    r2 = yield conv_append(role='user', content='old 2')
    yield conv_append(role='user', content='keep')
    yield conv_replace_with(
        [{'role': 'system', 'content': 'summary'}],
        end_ref=r2,
    )
    messages = yield conv_read()
    return [m.content for m in messages]


@workflow
def parent_with_conv():
    yield conv_append(role='user', content='parent message')
    child = child_reader()
    result = yield wait(child)
    return result


@workflow
def child_reader():
    messages = yield conv_read()
    return [m.content for m in messages]


REGISTRY = {
    'chat_workflow': chat_workflow,
    'search_workflow': search_workflow,
    'replace_workflow': replace_workflow,
    'parent_with_conv': parent_with_conv,
    'child_reader': child_reader,
}


def run_to_completion(engine, store, eid, max_steps=50):
    for _ in range(max_steps):
        state, _ = store.load_state(eid)
        if state.finished:
            break
        engine.step(store, eid)
    state, _ = store.load_state(eid)
    return state


class TestConversationEngine:
    def test_append_and_read(self):
        store = Store(':memory:')
        engine = Engine.from_registry(REGISTRY)
        eid = engine.start(store, 'chat_workflow', [])
        state = run_to_completion(engine, store, eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == ['Hello', 'Hi there']

    def test_search(self):
        store = Store(':memory:')
        engine = Engine.from_registry(REGISTRY)
        eid = engine.start(store, 'search_workflow', [])
        state = run_to_completion(engine, store, eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == 2

    def test_replace(self):
        store = Store(':memory:')
        engine = Engine.from_registry(REGISTRY)
        eid = engine.start(store, 'replace_workflow', [])
        state = run_to_completion(engine, store, eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == ['summary', 'keep']

    def test_child_sees_parent_conversation(self):
        store = Store(':memory:')
        engine = Engine.from_registry(REGISTRY)
        eid = engine.start(store, 'parent_with_conv', [])
        state = run_to_completion(engine, store, eid)
        assert state.finished
        # Child should see parent's message via forked conversation
        assert state.workflows[state.root_workflow_id].result == ['parent message']

    def test_conversation_events_in_log(self):
        store = Store(':memory:')
        engine = Engine.from_registry(REGISTRY)
        eid = engine.start(store, 'chat_workflow', [])
        run_to_completion(engine, store, eid)
        outbox = store.read_outbox(eid)
        conv_events = [e for e in outbox if 'conv' in e.type]
        assert len(conv_events) >= 2  # at least 2 appends + 1 read
