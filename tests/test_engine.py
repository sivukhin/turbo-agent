import pickle
import pytest
from workflows import workflow, wait, wait_all, wait_any, sleep, Engine, Store


# ---- test workflows ----

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
def parent_wait_any():
    short = counter(1)
    long = counter(5)
    yield 'racing'
    results = yield wait_any([short, long])
    finished = [r for done, r in results if done]
    return finished[0]

@workflow
def nested_parent(n):
    child = parent_wait(n)
    yield 'outer started'
    result = yield wait(child)
    return result

@workflow
def fan_out(count):
    children = [counter(i + 1) for i in range(count)]
    yield f'launched {count}'
    results = yield wait_all(children)
    return sum(results)

@workflow
def leaf(val):
    yield val
    return val

@workflow
def diamond():
    b = leaf(10)
    c = leaf(20)
    yield 'started'
    rb, rc = yield wait_all([b, c])
    return rb + rc

@workflow
def level3():
    yield 'L3'
    return 100

@workflow
def level2():
    child = level3()
    yield 'L2'
    result = yield wait(child)
    return result + 1

@workflow
def level1():
    child = level2()
    yield 'L1'
    result = yield wait(child)
    return result + 1

@workflow
def many_children():
    children = [counter(1) for _ in range(20)]
    yield 'go'
    results = yield wait_all(children)
    return sum(results)

@workflow
def slow_parent():
    child = counter(1)
    yield 'tick1'
    yield 'tick2'
    yield 'tick3'
    result = yield wait(child)
    return result

@workflow
def ordered():
    a = adder(3)
    b = counter(2)
    c = adder(2)
    yield 'go'
    results = yield wait_all([a, b, c])
    return results

@workflow
def mixed_speed():
    fast = counter(1)
    slow = counter(5)
    yield 'go'
    results = yield wait_all([fast, slow])
    return results

@workflow
def empty_wait():
    yield 'before'
    results = yield wait_all([])
    yield f'got {results}'
    return results

@workflow
def race_tuple():
    a = counter(1)
    b = counter(5)
    yield 'go'
    results = yield wait_any([a, b])
    # Find first finished
    for done, r in results:
        if done:
            return r

@workflow
def race_then_all():
    a = counter(1)
    b = counter(3)
    yield 'go'
    results = yield wait_any([a, b])
    first_result = next(r for done, r in results if done)
    yield f'first: {first_result}'
    second_result = yield wait(b)
    return first_result + second_result

@workflow
def waiter():
    child = counter(5)
    yield 'start'
    result = yield wait(child)
    yield f'got {result}'
    return result

@workflow
def status_test():
    child = counter(3)
    yield 'before wait'
    result = yield wait(child)
    yield 'after wait'
    return result

@workflow
def grandparent():
    child = fan_out(3)
    yield 'gp started'
    result = yield wait(child)
    return result

@workflow
def sleeper():
    yield 'before sleep'
    yield sleep(10.0)
    yield 'after sleep'
    return 'done'

@workflow
def multi_sleep():
    yield 'start'
    yield sleep(5.0)
    yield 'mid'
    yield sleep(3.0)
    yield 'end'
    return 'done'

@workflow
def sleep_with_children():
    child = counter(3)
    yield 'started'
    yield sleep(10.0)
    yield 'slept'
    result = yield wait(child)
    return result


REGISTRY = {
    'counter': counter,
    'adder': adder,
    'parent_wait': parent_wait,
    'parent_wait_all': parent_wait_all,
    'parent_wait_any': parent_wait_any,
    'nested_parent': nested_parent,
    'fan_out': fan_out,
    'leaf': leaf,
    'diamond': diamond,
    'level1': level1,
    'level2': level2,
    'level3': level3,
    'many_children': many_children,
    'slow_parent': slow_parent,
    'ordered': ordered,
    'mixed_speed': mixed_speed,
    'empty_wait': empty_wait,
    'race_tuple': race_tuple,
    'race_then_all': race_then_all,
    'waiter': waiter,
    'status_test': status_test,
    'grandparent': grandparent,
    'sleeper': sleeper,
    'multi_sleep': multi_sleep,
    'sleep_with_children': sleep_with_children,
}


def run_to_completion(engine, store, execution_id, max_steps=100):
    for _ in range(max_steps):
        state, _ = store.load_state(execution_id)
        if state.finished:
            break
        engine.step(store, execution_id)
    state, _ = store.load_state(execution_id)
    return state


@pytest.fixture
def env():
    store = Store(':memory:')
    engine = Engine(REGISTRY)
    return engine, store


class TestEngineBasic:
    def test_start_simple(self, env):
        engine, store = env
        eid = engine.start(store, 'counter', [3])
        state, _ = store.load_state(eid)
        assert not state.finished
        outbox = store.read_outbox(eid)
        assert len(outbox) == 1
        assert outbox[0].payload['value'] == 0

    def test_step_to_completion(self, env):
        engine, store = env
        eid = engine.start(store, 'counter', [3])
        state = run_to_completion(engine, store, eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == 3

    def test_counter_zero(self, env):
        engine, store = env
        eid = engine.start(store, 'counter', [0])
        state, _ = store.load_state(eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == 0


class TestWait:
    def test_child_auto_registered(self, env):
        engine, store = env
        eid = engine.start(store, 'parent_wait', [2])
        state, _ = store.load_state(eid)
        assert len(state.workflows) == 2

    def test_wait_resolves(self, env):
        engine, store = env
        eid = engine.start(store, 'parent_wait', [2])
        state = run_to_completion(engine, store, eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == 2

    def test_wait_already_finished(self, env):
        engine, store = env
        eid = engine.start(store, 'slow_parent', [])
        state = run_to_completion(engine, store, eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == 1


class TestWaitAll:
    def test_waits_for_both(self, env):
        engine, store = env
        eid = engine.start(store, 'parent_wait_all', [3])
        state = run_to_completion(engine, store, eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == 6

    def test_fan_out(self, env):
        engine, store = env
        eid = engine.start(store, 'fan_out', [4])
        state = run_to_completion(engine, store, eid)
        assert state.workflows[state.root_workflow_id].result == 10

    def test_ordering(self, env):
        engine, store = env
        eid = engine.start(store, 'ordered', [])
        state = run_to_completion(engine, store, eid)
        assert state.workflows[state.root_workflow_id].result == [3, 2, 1]

    def test_different_speeds(self, env):
        engine, store = env
        eid = engine.start(store, 'mixed_speed', [])
        state = run_to_completion(engine, store, eid)
        assert state.workflows[state.root_workflow_id].result == [1, 5]

    def test_empty(self, env):
        engine, store = env
        eid = engine.start(store, 'empty_wait', [])
        state = run_to_completion(engine, store, eid)
        assert state.workflows[state.root_workflow_id].result == []


class TestWaitAny:
    def test_first_finisher_wins(self, env):
        engine, store = env
        eid = engine.start(store, 'parent_wait_any', [])
        state = run_to_completion(engine, store, eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == 1

    def test_returns_result(self, env):
        engine, store = env
        eid = engine.start(store, 'race_tuple', [])
        state = run_to_completion(engine, store, eid)
        assert state.workflows[state.root_workflow_id].result == 1

    def test_then_wait_remaining(self, env):
        engine, store = env
        eid = engine.start(store, 'race_then_all', [])
        state = run_to_completion(engine, store, eid)
        assert state.workflows[state.root_workflow_id].result == 1 + 3


class TestNestedWorkflows:
    def test_grandchild(self, env):
        engine, store = env
        eid = engine.start(store, 'nested_parent', [2])
        state = run_to_completion(engine, store, eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == 2

    def test_diamond(self, env):
        engine, store = env
        eid = engine.start(store, 'diamond', [])
        state = run_to_completion(engine, store, eid)
        assert state.workflows[state.root_workflow_id].result == 30

    def test_chain_3_levels(self, env):
        engine, store = env
        eid = engine.start(store, 'level1', [])
        state = run_to_completion(engine, store, eid)
        assert state.workflows[state.root_workflow_id].result == 102

    def test_many_concurrent(self, env):
        engine, store = env
        eid = engine.start(store, 'many_children', [])
        state = run_to_completion(engine, store, eid)
        assert state.workflows[state.root_workflow_id].result == 20

    def test_grandparent_fan_out(self, env):
        engine, store = env
        eid = engine.start(store, 'grandparent', [])
        state = run_to_completion(engine, store, eid)
        assert state.workflows[state.root_workflow_id].result == 6


class TestConcurrency:
    def test_children_tick_in_parallel(self, env):
        engine, store = env
        eid = engine.start(store, 'parent_wait_all', [3])
        outbox_before = store.read_outbox(eid)
        last = outbox_before[-1].event_id if outbox_before else 0
        engine.step(store, eid)
        new_outbox = store.read_outbox(eid, after_event_id=last)
        child_outputs = [m for m in new_outbox if m.type == 'workflow_yielded'
                         and m.workflow_id != store.load_state(eid)[0].root_workflow_id]
        assert len(child_outputs) == 2


class TestEventLog:
    def test_events_are_appended(self, env):
        engine, store = env
        eid = engine.start(store, 'counter', [3])
        engine.step(store, eid)
        engine.step(store, eid)
        outbox = store.read_outbox(eid)
        assert len(outbox) >= 3  # at least 3 yields

    def test_outbox_records_yields(self, env):
        engine, store = env
        eid = engine.start(store, 'counter', [3])
        engine.step(store, eid)
        engine.step(store, eid)
        outbox = store.read_outbox(eid)
        values = [m.payload['value'] for m in outbox if m.type == 'workflow_yielded']
        assert values[:3] == [0, 1, 2]

    def test_inbox_records_finished(self, env):
        engine, store = env
        eid = engine.start(store, 'counter', [1])
        run_to_completion(engine, store, eid)
        inbox = store.read_inbox(eid)
        finished_msgs = [m for m in inbox if m.type == 'workflow_finished']
        assert len(finished_msgs) == 1


class TestCheckpointResume:
    def test_state_persists(self, env):
        engine, store = env
        eid = engine.start(store, 'counter', [5])
        engine.step(store, eid)
        engine.step(store, eid)
        state1, _ = store.load_state(eid)
        assert not state1.finished

        # Continue
        run_to_completion(engine, store, eid)
        state2, _ = store.load_state(eid)
        assert state2.finished
        assert state2.workflows[state2.root_workflow_id].result == 5

    def test_resume_with_children(self, env):
        engine, store = env
        eid = engine.start(store, 'fan_out', [3])
        engine.step(store, eid)

        # Simulate restart: create new engine
        engine2 = Engine(REGISTRY)
        run_to_completion(engine2, store, eid)
        state, _ = store.load_state(eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == 6

    def test_on_disk_persistence(self, tmp_path):
        db_path = str(tmp_path / 'test.db')
        store = Store(db_path)
        engine = Engine(REGISTRY)
        eid = engine.start(store, 'counter', [5])
        engine.step(store, eid)
        store.close()

        # Reopen
        store2 = Store(db_path)
        engine2 = Engine(REGISTRY)
        run_to_completion(engine2, store2, eid)
        state, _ = store2.load_state(eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == 5
        store2.close()


class TestSleep:
    def test_sleep_blocks_until_time(self, env):
        engine, store = env
        # start at t=100: yields 'before sleep'
        eid = engine.start(store, 'sleeper', [], now=100.0)
        # step at t=100: yields sleep(10) → waiting, wake_at=110
        engine.step(store, eid, now=100.0)
        state, _ = store.load_state(eid)
        root = state.workflows[state.root_workflow_id]
        assert root.status == 'waiting'

    def test_sleep_does_not_resolve_early(self, env):
        engine, store = env
        eid = engine.start(store, 'sleeper', [], now=100.0)
        engine.step(store, eid, now=100.0)  # registers sleep, wake_at=110
        engine.step(store, eid, now=105.0)  # too early
        state, _ = store.load_state(eid)
        root = state.workflows[state.root_workflow_id]
        assert root.status == 'waiting'

    def test_sleep_resolves_at_wake_time(self, env):
        engine, store = env
        eid = engine.start(store, 'sleeper', [], now=100.0)
        engine.step(store, eid, now=100.0)  # sleep registered, wake_at=110
        engine.step(store, eid, now=110.0)  # resolves
        state, _ = store.load_state(eid)
        root = state.workflows[state.root_workflow_id]
        assert root.status == 'running'

    def test_sleep_full_lifecycle(self, env):
        engine, store = env
        eid = engine.start(store, 'sleeper', [], now=0.0)
        engine.step(store, eid, now=0.0)    # sleep(10) → waiting, wake_at=10
        engine.step(store, eid, now=5.0)    # too early
        state, _ = store.load_state(eid)
        assert not state.finished

        engine.step(store, eid, now=10.0)   # resolves sleep
        engine.step(store, eid, now=10.0)   # yields 'after sleep'
        engine.step(store, eid, now=10.0)   # returns 'done'
        state, _ = store.load_state(eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == 'done'

    def test_multiple_sleeps(self, env):
        engine, store = env
        eid = engine.start(store, 'multi_sleep', [], now=0.0)
        # After start: yielded 'start'
        engine.step(store, eid, now=0.0)    # sleep(5) → waiting, wake_at=5

        engine.step(store, eid, now=3.0)    # too early
        state, _ = store.load_state(eid)
        assert not state.finished

        engine.step(store, eid, now=5.0)    # first sleep resolves
        engine.step(store, eid, now=5.0)    # yields 'mid'
        engine.step(store, eid, now=5.0)    # sleep(3) → waiting, wake_at=8

        engine.step(store, eid, now=7.0)    # too early
        state, _ = store.load_state(eid)
        assert not state.finished

        engine.step(store, eid, now=8.0)    # second sleep resolves
        engine.step(store, eid, now=8.0)    # yields 'end'
        engine.step(store, eid, now=8.0)    # returns 'done'
        state, _ = store.load_state(eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == 'done'

    def test_sleep_with_concurrent_children(self, env):
        """Children tick while parent sleeps."""
        engine, store = env
        eid = engine.start(store, 'sleep_with_children', [], now=0.0)
        # After start: yields 'started'. Child registered.
        engine.step(store, eid, now=0.0)    # parent: sleep(10), wake_at=10. child: yields 0
        engine.step(store, eid, now=1.0)    # child: yields 1
        engine.step(store, eid, now=2.0)    # child: yields 2
        engine.step(store, eid, now=3.0)    # child: finishes (counter(3) → return 3)

        state, _ = store.load_state(eid)
        assert not state.finished  # parent still sleeping

        engine.step(store, eid, now=10.0)   # sleep resolves
        engine.step(store, eid, now=10.0)   # yields 'slept'
        engine.step(store, eid, now=10.0)   # wait(child) → already done, resolves
        engine.step(store, eid, now=10.0)   # returns result
        state, _ = store.load_state(eid)
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == 3

    def test_sleep_outbox_messages(self, env):
        engine, store = env
        eid = engine.start(store, 'sleeper', [], now=0.0)
        outbox = store.read_outbox(eid)
        values = [m.payload['value'] for m in outbox if m.type == 'workflow_yielded']
        assert 'before sleep' in values

        engine.step(store, eid, now=0.0)    # sleep
        engine.step(store, eid, now=10.0)   # wake
        engine.step(store, eid, now=10.0)   # yields 'after sleep'
        outbox = store.read_outbox(eid)
        values = [m.payload['value'] for m in outbox if m.type == 'workflow_yielded']
        assert 'after sleep' in values

    def test_sleep_zero_resolves_immediately(self, env):
        """sleep(0) should resolve on the same tick."""
        @workflow
        def instant_sleep():
            yield 'before'
            yield sleep(0)
            yield 'after'
            return 'done'

        reg = {**REGISTRY, 'instant_sleep': instant_sleep}
        engine = Engine(reg)
        store = env[1]
        eid = engine.start(store, 'instant_sleep', [], now=100.0)
        engine.step(store, eid, now=100.0)  # sleep(0) → resolves immediately
        engine.step(store, eid, now=100.0)  # yields 'after'
        engine.step(store, eid, now=100.0)  # returns
        state, _ = store.load_state(eid)
        assert state.finished
