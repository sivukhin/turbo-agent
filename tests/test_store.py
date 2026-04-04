import pytest
from workflows import workflow, wait, wait_all, wait_any, Engine, EngineConfig, Store
from workflows.events import WorkflowFinished, WorkflowYielded


@workflow
def counter(n):
    for i in range(n):
        yield i
    return n


@workflow
def parent_wait(n):
    child = counter(n)
    yield "started"
    result = yield wait(child)
    return result


@workflow
def fan_out(count):
    children = [counter(i + 1) for i in range(count)]
    yield f"launched {count}"
    results = yield wait_all(children)
    return sum(results)


REGISTRY = {
    "counter": counter,
    "parent_wait": parent_wait,
    "fan_out": fan_out,
}


def run_to_completion(engine, store, eid, max_steps=100):
    for _ in range(max_steps):
        state, _ = store.load_state(eid)
        if state.finished:
            break
        engine.step(store, eid)
    state, _ = store.load_state(eid)
    return state


@pytest.fixture
def env():
    store = Store(":memory:")
    engine = Engine(EngineConfig(workflows_registry=REGISTRY))
    return engine, store


class TestStoreBasic:
    def test_save_and_load(self, env):
        engine, store = env
        eid = engine.start(store, "counter", [5])
        state, _ = store.load_state(eid)
        assert not state.finished

    def test_load_nonexistent(self, env):
        _, store = env
        with pytest.raises(KeyError):
            store.load_state("nonexistent")

    def test_list_executions(self, env):
        engine, store = env
        eid1 = engine.start(store, "counter", [3])
        eid2 = engine.start(store, "counter", [5])
        execs = store.list_executions()
        assert len(execs) == 2
        ids = {e[0] for e in execs}
        assert eid1 in ids
        assert eid2 in ids


class TestStoreEvents:
    def test_append_and_read_inbox(self, env):
        _, store = env
        store.append_event("e1", "w1", "inbox", WorkflowFinished(result=1))
        store.append_event("e1", "w2", "inbox", WorkflowFinished(result=2))
        events = store.read_inbox("e1")
        assert len(events) == 2
        assert events[0].workflow_id == "w1"
        assert events[1].workflow_id == "w2"
        assert isinstance(events[0].payload, WorkflowFinished)
        assert events[0].payload.result == 1

    def test_read_after_event_id(self, env):
        _, store = env
        store.append_event("e1", None, "inbox", WorkflowFinished(result=0))
        store.append_event("e1", None, "inbox", WorkflowFinished(result=1))
        store.append_event("e1", None, "inbox", WorkflowFinished(result=2))
        events = store.read_inbox("e1")
        assert len(events) == 3
        events2 = store.read_inbox("e1", after_event_id=events[1].event_id)
        assert len(events2) == 1
        assert events2[0].event_id == events[2].event_id

    def test_outbox_separate_from_inbox(self, env):
        _, store = env
        store.append_event("e1", None, "inbox", WorkflowFinished(result=0))
        store.append_event("e1", "w1", "outbox", WorkflowYielded(value=42))
        assert len(store.read_inbox("e1")) == 1
        assert len(store.read_outbox("e1")) == 1

    def test_events_scoped_to_execution(self, env):
        _, store = env
        store.append_event("e1", None, "inbox", WorkflowFinished(result=0))
        store.append_event("e2", None, "inbox", WorkflowFinished(result=0))
        assert len(store.read_inbox("e1")) == 1
        assert len(store.read_inbox("e2")) == 1

    def test_payload_roundtrip(self, env):
        _, store = env
        store.append_event("e1", "w1", "outbox", WorkflowYielded(value="hello"))
        events = store.read_outbox("e1")
        assert isinstance(events[0].payload, WorkflowYielded)
        assert events[0].payload.value == "hello"
        assert events[0].type == "workflow_yielded"


class TestStoreResume:
    def test_step_by_step_via_store(self, env):
        engine, store = env
        eid = engine.start(store, "parent_wait", [3])
        state = run_to_completion(engine, store, eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == 3

    def test_fan_out_via_store(self, env):
        engine, store = env
        eid = engine.start(store, "fan_out", [4])
        state = run_to_completion(engine, store, eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == 10


class TestStoreOnDisk:
    def test_file_persistence(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        store = Store(db_path)
        engine = Engine(EngineConfig(workflows_registry=REGISTRY))
        eid = engine.start(store, "counter", [5])
        engine.step(store, eid)
        store.close()

        store2 = Store(db_path)
        engine2 = Engine(EngineConfig(workflows_registry=REGISTRY))
        state = run_to_completion(engine2, store2, eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == 5
        store2.close()

    def test_multiple_executions(self, tmp_path):
        db_path = str(tmp_path / "multi.db")
        store = Store(db_path)
        engine = Engine(EngineConfig(workflows_registry=REGISTRY))
        eid1 = engine.start(store, "counter", [3])
        eid2 = engine.start(store, "counter", [5])
        store.close()

        store2 = Store(db_path)
        assert len(store2.list_executions()) == 2
        store2.close()
