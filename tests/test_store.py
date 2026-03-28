"""Tests for the Turso database store backend."""

import os
import pytest
from workflows import workflow, wait, wait_all, wait_any, Engine, Store


@workflow
def counter(n):
    for i in range(n):
        yield i
    return n

@workflow
def adder(n):
    total = 0
    for i in range(n):
        total += i
        yield total
    return total

@workflow
def parent_wait(n):
    child = counter(n)
    yield 'started'
    result = yield wait(child)
    yield f'done: {result}'
    return result

@workflow
def parent_wait_all(n):
    a = counter(n)
    b = adder(n)
    yield 'started'
    ra, rb = yield wait_all([a, b])
    return ra + rb

@workflow
def race_wf():
    short = counter(1)
    long = counter(5)
    yield 'racing'
    winner_id, result = yield wait_any([short, long])
    return result

@workflow
def fan_out(count):
    children = [counter(i + 1) for i in range(count)]
    yield f'launched {count}'
    results = yield wait_all(children)
    return sum(results)


REGISTRY = {
    'counter': counter,
    'adder': adder,
    'parent_wait': parent_wait,
    'parent_wait_all': parent_wait_all,
    'race_wf': race_wf,
    'fan_out': fan_out,
}

DB_PATH = ':memory:'


def run_to_completion(engine, max_steps=100):
    all_outputs = []
    for _ in range(max_steps):
        if engine.state.finished:
            break
        outputs, _ = engine.step()
        all_outputs.extend(outputs)
    return all_outputs


class TestStoreBasic:
    def test_save_and_load(self):
        store = Store(DB_PATH)
        engine, _ = Engine.start(REGISTRY, 'counter', [5])
        engine.step()
        engine.step()

        store.save('test-1', engine.state)
        loaded = store.load('test-1')

        assert loaded.step == engine.state.step
        assert loaded.root == engine.state.root
        assert loaded.finished == engine.state.finished
        assert len(loaded.workflows) == len(engine.state.workflows)
        store.close()

    def test_load_nonexistent_raises(self):
        store = Store(DB_PATH)
        with pytest.raises(KeyError):
            store.load('nonexistent')
        store.close()

    def test_list_all(self):
        store = Store(DB_PATH)
        e1, _ = Engine.start(REGISTRY, 'counter', [3])
        e2, _ = Engine.start(REGISTRY, 'counter', [5])
        store.save('a', e1.state)
        store.save('b', e2.state)

        all_execs = store.list_all()
        assert len(all_execs) == 2
        ids = {eid for eid, _ in all_execs}
        assert ids == {'a', 'b'}
        store.close()

    def test_overwrite_existing(self):
        store = Store(DB_PATH)
        engine, _ = Engine.start(REGISTRY, 'counter', [5])
        store.save('x', engine.state)
        assert store.load('x').step == 0

        engine.step()
        store.save('x', engine.state)
        assert store.load('x').step == 1
        store.close()


class TestStoreResume:
    def test_resume_counter_from_store(self):
        store = Store(DB_PATH)
        engine, _ = Engine.start(REGISTRY, 'counter', [5])
        engine.step()
        engine.step()
        store.save('c1', engine.state)

        # Load and continue
        state2 = store.load('c1')
        engine2 = Engine(state2, REGISTRY)
        run_to_completion(engine2)
        assert engine2.state.finished
        assert engine2.state.workflows['0'].result == 5
        store.close()

    def test_resume_with_children(self):
        store = Store(DB_PATH)
        engine, _ = Engine.start(REGISTRY, 'parent_wait', [3])
        engine.step()
        engine.step()
        store.save('pw1', engine.state)

        state2 = store.load('pw1')
        engine2 = Engine(state2, REGISTRY)
        run_to_completion(engine2)
        assert engine2.state.finished
        assert engine2.state.workflows['0'].result == 3
        store.close()

    def test_resume_wait_all(self):
        store = Store(DB_PATH)
        engine, _ = Engine.start(REGISTRY, 'parent_wait_all', [3])
        engine.step()
        engine.step()
        store.save('wa1', engine.state)

        state2 = store.load('wa1')
        engine2 = Engine(state2, REGISTRY)
        run_to_completion(engine2)
        assert engine2.state.finished
        assert engine2.state.workflows['0'].result == 6
        store.close()

    def test_resume_fan_out(self):
        store = Store(DB_PATH)
        engine, _ = Engine.start(REGISTRY, 'fan_out', [4])
        engine.step()
        engine.step()
        store.save('fo1', engine.state)

        state2 = store.load('fo1')
        engine2 = Engine(state2, REGISTRY)
        run_to_completion(engine2)
        assert engine2.state.finished
        assert engine2.state.workflows['0'].result == 10
        store.close()


class TestStoreSendVal:
    """Verify _send_val persists across save/load (the key bug we fixed)."""

    def test_send_val_persists_after_wait_resolves(self):
        """When children finish and wait resolves, _send_val must survive save/load."""
        store = Store(DB_PATH)
        engine, _ = Engine.start(REGISTRY, 'parent_wait', [1])

        # Step until child finishes and wait resolves (sets _send_val)
        for _ in range(10):
            if engine.state.finished:
                break
            engine.step()
            store.save('sv1', engine.state)

            # Reload and continue from DB
            state = store.load('sv1')
            engine = Engine(state, REGISTRY)

        assert engine.state.finished
        assert engine.state.workflows['0'].result == 1
        store.close()

    def test_step_by_step_via_store(self):
        """Simulate CLI: each step is save → load → step → save."""
        store = Store(DB_PATH)
        engine, _ = Engine.start(REGISTRY, 'parent_wait_all', [2])
        store.save('sbs1', engine.state)

        for _ in range(20):
            state = store.load('sbs1')
            if state.finished:
                break
            engine = Engine(state, REGISTRY)
            engine.step()
            store.save('sbs1', engine.state)

        final = store.load('sbs1')
        assert final.finished
        assert final.workflows['0'].result == 2 + 1  # counter(2)=2, adder(2)=1
        store.close()

    def test_wait_any_via_store(self):
        store = Store(DB_PATH)
        engine, _ = Engine.start(REGISTRY, 'race_wf', [])
        store.save('race1', engine.state)

        for _ in range(20):
            state = store.load('race1')
            if state.finished:
                break
            engine = Engine(state, REGISTRY)
            engine.step()
            store.save('race1', engine.state)

        final = store.load('race1')
        assert final.finished
        assert final.workflows['0'].result == 1
        store.close()


class TestStoreWorkflowState:
    def test_workflow_fields_roundtrip(self):
        store = Store(DB_PATH)
        engine, _ = Engine.start(REGISTRY, 'parent_wait', [3])
        # Advance until parent is waiting
        for _ in range(5):
            engine.step()
            if engine.state.workflows['0'].status == 'waiting':
                break

        store.save('rt1', engine.state)
        loaded = store.load('rt1')

        root = loaded.workflows['0']
        assert root.status == 'waiting'
        assert root.wait_mode == 'all'
        assert len(root.wait_deps) == 1
        assert root.name == 'parent_wait'

        child_id = root.wait_deps[0]
        child = loaded.workflows[child_id]
        assert child.name == 'counter'
        assert child.status == 'running'
        assert child.args == [3]
        store.close()

    def test_finished_workflow_result_persists(self):
        store = Store(DB_PATH)
        engine, _ = Engine.start(REGISTRY, 'counter', [3])
        run_to_completion(engine)
        store.save('fin1', engine.state)

        loaded = store.load('fin1')
        assert loaded.finished
        assert loaded.workflows['0'].result == 3
        assert loaded.workflows['0'].status == 'finished'
        store.close()


class TestStoreOnDisk:
    """Test with actual file-based DB to verify persistence."""

    def test_file_based_db(self, tmp_path):
        db_path = str(tmp_path / 'test.db')

        # First "process": start and step
        store = Store(db_path)
        engine, _ = Engine.start(REGISTRY, 'counter', [5])
        engine.step()
        engine.step()
        store.save('disk1', engine.state)
        store.close()

        # Second "process": load and continue
        store2 = Store(db_path)
        state = store2.load('disk1')
        engine2 = Engine(state, REGISTRY)
        run_to_completion(engine2)
        store2.save('disk1', engine2.state)

        final = store2.load('disk1')
        assert final.finished
        assert final.workflows['0'].result == 5
        store2.close()

    def test_multiple_executions_on_disk(self, tmp_path):
        db_path = str(tmp_path / 'multi.db')
        store = Store(db_path)

        e1, _ = Engine.start(REGISTRY, 'counter', [3])
        e2, _ = Engine.start(REGISTRY, 'counter', [5])
        store.save('m1', e1.state)
        store.save('m2', e2.state)
        store.close()

        # Reopen
        store2 = Store(db_path)
        assert len(store2.list_all()) == 2
        s1 = store2.load('m1')
        s2 = store2.load('m2')
        assert s1.workflows['0'].args == [3]
        assert s2.workflows['0'].args == [5]
        store2.close()
