"""Tests for the conversation system."""

import pytest
from workflows import (
    workflow,
    wait,
    conv_append,
    conv_list,
    conv_read,
    conv_replace_with,
    Latest,
    Engine,
    EngineConfig,
    Store,
)
from workflows.conversation import MessageRef, Message


# ---- Store-level tests ----


@pytest.fixture
def store():
    return Store(":memory:")


class TestConversationStore:
    def test_create_and_append(self, store):
        store.create_conversation("c1")
        ref = store.conv_append_message("c1", "user", "hello")
        assert ref.conversation_id == "c1"
        assert ref.layer == 0
        assert ref.role == "user"

    def test_list_and_read(self, store):
        store.create_conversation("c1")
        store.conv_append_message("c1", "user", "hello")
        store.conv_append_message("c1", "assistant", "hi there")
        refs = store.conv_list_messages("c1")
        assert len(refs) == 2
        assert refs[0].role == "user"
        assert refs[1].role == "assistant"
        messages = store.conv_read_messages(refs)
        assert len(messages) == 2
        assert messages[0].content == "hello"
        assert messages[1].content == "hi there"

    def test_list_preserves_order(self, store):
        store.create_conversation("c1")
        for i in range(10):
            store.conv_append_message("c1", "user", f"msg {i}")
        refs = store.conv_list_messages("c1")
        messages = store.conv_read_messages(refs)
        assert [m.content for m in messages] == [f"msg {i}" for i in range(10)]

    def test_parent_chain(self, store):
        store.create_conversation("parent")
        store.conv_append_message("parent", "user", "from parent")
        ref = store.conv_resolve_ref("parent")
        store.create_conversation(
            "child",
            parent_conversation_id="parent",
            parent_message_id=ref.message_id,
            parent_layer=ref.layer,
        )
        store.conv_append_message("child", "assistant", "from child")
        refs = store.conv_list_messages("child")
        messages = store.conv_read_messages(refs)
        assert len(messages) == 2
        assert messages[0].content == "from parent"
        assert messages[1].content == "from child"

    def test_parent_chain_three_levels(self, store):
        store.create_conversation("a")
        store.conv_append_message("a", "user", "level-a")
        ref_a = store.conv_resolve_ref("a")
        store.create_conversation(
            "b",
            parent_conversation_id="a",
            parent_message_id=ref_a.message_id,
            parent_layer=ref_a.layer,
        )
        store.conv_append_message("b", "assistant", "level-b")
        ref_b = store.conv_resolve_ref("b")
        store.create_conversation(
            "c",
            parent_conversation_id="b",
            parent_message_id=ref_b.message_id,
            parent_layer=ref_b.layer,
        )
        store.conv_append_message("c", "user", "level-c")
        refs = store.conv_list_messages("c")
        messages = store.conv_read_messages(refs)
        assert [m.content for m in messages] == ["level-a", "level-b", "level-c"]

    def test_layer_compaction(self, store):
        store.create_conversation("c1")
        r1 = store.conv_append_message("c1", "user", "old msg 1")
        r2 = store.conv_append_message("c1", "user", "old msg 2")
        store.conv_append_message("c1", "user", "keep this")
        store.conv_replace_with(
            "c1",
            [{"role": "system", "content": "summary of 1+2"}],
            start_message_id=r1.message_id,
            end_message_id=r2.message_id,
        )
        refs = store.conv_list_messages("c1")
        messages = store.conv_read_messages(refs)
        assert len(messages) == 2
        assert messages[0].content == "summary of 1+2"
        assert messages[1].content == "keep this"

    def test_replace_entire_conversation(self, store):
        store.create_conversation("c1")
        store.conv_append_message("c1", "user", "a")
        store.conv_append_message("c1", "user", "b")
        store.conv_replace_with("c1", [{"role": "system", "content": "replaced all"}])
        refs = store.conv_list_messages("c1")
        messages = store.conv_read_messages(refs)
        assert len(messages) == 1
        assert messages[0].content == "replaced all"

    def test_replace_from_start(self, store):
        store.create_conversation("c1")
        store.conv_append_message("c1", "user", "a")
        r2 = store.conv_append_message("c1", "user", "b")
        store.conv_append_message("c1", "user", "c")
        store.conv_replace_with(
            "c1",
            [{"role": "system", "content": "replaced a+b"}],
            end_message_id=r2.message_id,
        )
        refs = store.conv_list_messages("c1")
        messages = store.conv_read_messages(refs)
        assert len(messages) == 2
        assert messages[0].content == "replaced a+b"
        assert messages[1].content == "c"

    def test_read_by_specific_refs(self, store):
        store.create_conversation("c1")
        r1 = store.conv_append_message("c1", "user", "first")
        r2 = store.conv_append_message("c1", "user", "second")
        store.conv_append_message("c1", "user", "third")
        messages = store.conv_read_messages([r1, r2])
        assert len(messages) == 2
        assert messages[0].content == "first"
        assert messages[1].content == "second"

    def test_message_ref_has_role(self, store):
        store.create_conversation("c1")
        ref = store.conv_append_message("c1", "assistant", "hello")
        assert ref.role == "assistant"
        refs = store.conv_list_messages("c1")
        assert refs[0].role == "assistant"

    def test_message_has_role_via_ref(self, store):
        store.create_conversation("c1")
        store.conv_append_message("c1", "user", "hello")
        refs = store.conv_list_messages("c1")
        messages = store.conv_read_messages(refs)
        assert messages[0].role == "user"

    def test_list_with_role_filter(self, store):
        store.create_conversation("c1")
        store.conv_append_message("c1", "user", "q1")
        store.conv_append_message("c1", "assistant", "a1")
        store.conv_append_message("c1", "user", "q2")
        refs = store.conv_list_messages("c1", role_filter="user")
        assert len(refs) == 2
        assert all(r.role == "user" for r in refs)

    def test_list_with_pattern(self, store):
        store.create_conversation("c1")
        store.conv_append_message("c1", "user", "hello world")
        store.conv_append_message("c1", "assistant", "goodbye world")
        store.conv_append_message("c1", "user", "hello again")
        refs = store.conv_list_messages("c1", pattern="%hello%")
        assert len(refs) == 2

    def test_resolve_ref(self, store):
        store.create_conversation("c1")
        store.conv_append_message("c1", "user", "a")
        ref = store.conv_resolve_ref("c1")
        assert ref.conversation_id == "c1"
        assert ref.message_id
        assert ref.layer == 0


# ---- Engine-level tests ----


@workflow
def chat_workflow():
    yield conv_append(role="user", content="Hello")
    yield conv_append(role="assistant", content="Hi there")
    refs = yield conv_list()
    messages = yield conv_read(refs)
    return [m.content for m in messages]


@workflow
def search_workflow():
    yield conv_append(role="user", content="find this needle")
    yield conv_append(role="user", content="not this one")
    yield conv_append(role="user", content="another needle here")
    refs = yield conv_list(pattern="%needle%")
    return len(refs)


@workflow
def replace_workflow():
    yield conv_append(role="user", content="old 1")
    r2 = yield conv_append(role="user", content="old 2")
    yield conv_append(role="user", content="keep")
    yield conv_replace_with(
        [{"role": "system", "content": "summary"}],
        end_ref=r2,
    )
    refs = yield conv_list()
    messages = yield conv_read(refs)
    return [m.content for m in messages]


@workflow
def parent_with_conv():
    yield conv_append(role="user", content="parent message")
    child = child_reader()
    result = yield wait(child)
    return result


@workflow
def child_reader():
    refs = yield conv_list()
    messages = yield conv_read(refs)
    return [m.content for m in messages]


REGISTRY = {
    "chat_workflow": chat_workflow,
    "search_workflow": search_workflow,
    "replace_workflow": replace_workflow,
    "parent_with_conv": parent_with_conv,
    "child_reader": child_reader,
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
        store = Store(":memory:")
        engine = Engine(EngineConfig(workflows_registry=REGISTRY))
        eid = engine.start(store, "chat_workflow", [])
        state = run_to_completion(engine, store, eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == ["Hello", "Hi there"]

    def test_search(self):
        store = Store(":memory:")
        engine = Engine(EngineConfig(workflows_registry=REGISTRY))
        eid = engine.start(store, "search_workflow", [])
        state = run_to_completion(engine, store, eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == 2

    def test_replace(self):
        store = Store(":memory:")
        engine = Engine(EngineConfig(workflows_registry=REGISTRY))
        eid = engine.start(store, "replace_workflow", [])
        state = run_to_completion(engine, store, eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == ["summary", "keep"]

    def test_child_sees_parent_conversation(self):
        store = Store(":memory:")
        engine = Engine(EngineConfig(workflows_registry=REGISTRY))
        eid = engine.start(store, "parent_with_conv", [])
        state = run_to_completion(engine, store, eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == ["parent message"]

    def test_conversation_events_in_log(self):
        store = Store(":memory:")
        engine = Engine(EngineConfig(workflows_registry=REGISTRY))
        eid = engine.start(store, "chat_workflow", [])
        run_to_completion(engine, store, eid)
        outbox = store.read_outbox(eid)
        conv_events = [e for e in outbox if "conv" in e.type]
        assert len(conv_events) >= 2
