import pickle
import pytest
from workflows import workflow, wait, wait_all, wait_any, Engine


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
    short = counter(1)  # finishes after 1 yield
    long = counter(5)   # takes 5 yields
    yield 'racing'
    winner_id, result = yield wait_any([short, long])
    return result

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
def immediate_finish():
    yield 'done'
    return 42


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
def single_wait_all():
    c = counter(2)
    yield 'go'
    results = yield wait_all([c])
    return results

@workflow
def race_tuple():
    a = counter(1)
    b = counter(5)
    yield 'go'
    winner_id, result = yield wait_any([a, b])
    yield f'winner={winner_id}'
    return (winner_id, result)

@workflow
def tie_race():
    a = counter(2)
    b = counter(2)
    c = counter(2)
    yield 'go'
    winner_id, result = yield wait_any([a, b, c])
    return winner_id

@workflow
def race_then_all():
    a = counter(1)
    b = counter(3)
    yield 'go'
    winner_id, first_result = yield wait_any([a, b])
    yield f'first: {first_result}'
    second_result = yield wait(b)
    return first_result + second_result

@workflow
def multi_race():
    children = [counter(i + 1) for i in range(4)]
    yield 'go'
    results = []
    for _ in range(2):
        wid, result = yield wait_any(children)
        results.append(result)
        yield f'got {result}'
    return results

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


REGISTRY = {
    'counter': counter,
    'adder': adder,
    'parent_wait': parent_wait,
    'parent_wait_all': parent_wait_all,
    'parent_wait_any': parent_wait_any,
    'nested_parent': nested_parent,
    'fan_out': fan_out,
    'immediate_finish': immediate_finish,
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
    'single_wait_all': single_wait_all,
    'race_tuple': race_tuple,
    'tie_race': tie_race,
    'race_then_all': race_then_all,
    'multi_race': multi_race,
    'waiter': waiter,
    'status_test': status_test,
    'grandparent': grandparent,
}


def run_to_completion(engine, max_steps=100):
    """Helper: step engine until finished, return all outputs."""
    all_outputs = []
    for _ in range(max_steps):
        if engine.state.finished:
            break
        outputs, _ = engine.step()
        all_outputs.extend(outputs)
    return all_outputs


# ---- basic engine ----

class TestEngineBasic:
    def test_start_simple(self):
        engine, outputs = Engine.start(REGISTRY, 'counter', [3])
        assert len(outputs) == 1
        assert outputs[0] == ('0', 'counter', 0)
        assert not engine.state.finished

    def test_step_to_completion(self):
        engine, _ = Engine.start(REGISTRY, 'counter', [3])
        all_out = run_to_completion(engine)
        vals = [v for _, _, v in all_out]
        assert vals == [1, 2]
        assert engine.state.finished
        assert engine.state.workflows['0'].result == 3

    def test_single_yield_then_return(self):
        engine, outputs = Engine.start(REGISTRY, 'immediate_finish', [])
        assert not engine.state.finished
        run_to_completion(engine)
        assert engine.state.finished
        assert engine.state.workflows['0'].result == 42


# ---- wait (single child) ----

class TestWait:
    def test_child_auto_registered(self):
        engine, _ = Engine.start(REGISTRY, 'parent_wait', [2])
        assert len(engine.state.workflows) == 2  # parent + child

    def test_parent_waits_for_child(self):
        engine, outputs = Engine.start(REGISTRY, 'parent_wait', [2])
        # First output is parent's 'started'
        assert ('0', 'parent_wait', 'started') in outputs
        # Parent yielded 'started', next tick it will yield wait()
        engine.step()
        assert engine.state.workflows['0'].status == 'waiting'

    def test_wait_resolves(self):
        engine, _ = Engine.start(REGISTRY, 'parent_wait', [2])
        all_out = run_to_completion(engine)
        vals = [v for wid, _, v in all_out if wid == '0']
        assert 'done: 2' in vals
        assert engine.state.workflows['0'].result == 2

    def test_child_yields_visible(self):
        engine, outputs = Engine.start(REGISTRY, 'parent_wait', [3])
        all_out = outputs + run_to_completion(engine)
        child_vals = [v for wid, _, v in all_out if wid == '1']
        assert child_vals == [0, 1, 2]


# ---- wait_all ----

class TestWaitAll:
    def test_waits_for_both(self):
        engine, _ = Engine.start(REGISTRY, 'parent_wait_all', [3])
        run_to_completion(engine)
        assert engine.state.finished
        # counter(3) returns 3, adder(3) returns 0+1+2=3
        assert engine.state.workflows['0'].result == 6

    def test_fan_out(self):
        engine, _ = Engine.start(REGISTRY, 'fan_out', [4])
        run_to_completion(engine)
        # counter(1) returns 1, counter(2) returns 2, counter(3) returns 3, counter(4) returns 4
        assert engine.state.workflows['0'].result == 1 + 2 + 3 + 4


# ---- wait_any ----

class TestWaitAny:
    def test_first_finisher_wins(self):
        engine, _ = Engine.start(REGISTRY, 'parent_wait_any', [])
        run_to_completion(engine)
        assert engine.state.finished
        # counter(1) finishes first with result=1
        assert engine.state.workflows['0'].result == 1


# ---- nested workflows ----

class TestNestedWorkflows:
    def test_grandchild(self):
        engine, _ = Engine.start(REGISTRY, 'nested_parent', [2])
        run_to_completion(engine)
        assert engine.state.finished
        assert engine.state.workflows['0'].result == 2

    def test_grandchild_creates_three_workflows(self):
        engine, _ = Engine.start(REGISTRY, 'nested_parent', [2])
        # nested_parent -> parent_wait -> counter = 3 workflows
        run_to_completion(engine)
        assert len(engine.state.workflows) == 3


# ---- concurrent children tick together ----

class TestConcurrency:
    def test_children_tick_in_parallel(self):
        engine, _ = Engine.start(REGISTRY, 'parent_wait_all', [3])
        # After start: parent yielded 'started', both children exist
        outputs, _ = engine.step()
        # Both children should have yielded in the same step
        child_outputs = [(wid, v) for wid, _, v in outputs if wid != '0']
        assert len(child_outputs) == 2

    def test_fan_out_all_tick(self):
        engine, _ = Engine.start(REGISTRY, 'fan_out', [3])
        outputs, _ = engine.step()
        # 3 children should all tick
        child_ids = {wid for wid, _, _ in outputs if wid != '0'}
        assert len(child_ids) == 3


# ---- checkpoint / resume of engine state ----

class TestEngineCheckpoint:
    def test_state_is_picklable(self):
        engine, _ = Engine.start(REGISTRY, 'parent_wait', [3])
        engine.step()
        data = pickle.dumps(engine.state)
        state2 = pickle.loads(data)
        engine2 = Engine(state2, REGISTRY)
        run_to_completion(engine2)
        assert engine2.state.finished
        assert engine2.state.workflows['0'].result == 3

    def test_resume_mid_execution(self):
        engine, _ = Engine.start(REGISTRY, 'fan_out', [3])
        engine.step()
        engine.step()
        data = pickle.dumps(engine.state)

        # "New process"
        state2 = pickle.loads(data)
        engine2 = Engine(state2, REGISTRY)
        run_to_completion(engine2)
        assert engine2.state.finished
        assert engine2.state.workflows['0'].result == 1 + 2 + 3

    def test_multiple_resume_cycles(self):
        engine, _ = Engine.start(REGISTRY, 'counter', [10])
        engine.step()
        engine.step()
        data = pickle.dumps(engine.state)

        engine2 = Engine(pickle.loads(data), REGISTRY)
        engine2.step()
        engine2.step()
        data2 = pickle.dumps(engine2.state)

        engine3 = Engine(pickle.loads(data2), REGISTRY)
        run_to_completion(engine3)
        assert engine3.state.finished
        assert engine3.state.workflows['0'].result == 10


# ---- edge cases ----

class TestEdgeCases:
    def test_counter_zero(self):
        engine, _ = Engine.start(REGISTRY, 'counter', [0])
        assert engine.state.finished
        assert engine.state.workflows['0'].result == 0

    def test_wait_already_finished(self):
        """Child finishes before parent yields wait — should resolve immediately."""
        engine, _ = Engine.start(REGISTRY, 'slow_parent', [])
        run_to_completion(engine)
        assert engine.state.finished
        assert engine.state.workflows['0'].result == 1


# ---- wait_all advanced ----

class TestWaitAllAdvanced:
    def test_wait_all_ordering(self):
        """wait_all returns results in the same order as handles."""
        engine, _ = Engine.start(REGISTRY, 'ordered', [])
        run_to_completion(engine)
        assert engine.state.workflows['0'].result == [3, 2, 1]

    def test_wait_all_different_speeds(self):
        """Children finish at different times, wait_all waits for slowest."""
        engine, _ = Engine.start(REGISTRY, 'mixed_speed', [])
        run_to_completion(engine)
        assert engine.state.workflows['0'].result == [1, 5]

    def test_wait_all_empty_list(self):
        """wait_all with empty list resolves immediately."""
        engine, _ = Engine.start(REGISTRY, 'empty_wait', [])
        run_to_completion(engine)
        assert engine.state.workflows['0'].result == []

    def test_wait_all_single(self):
        """wait_all with single element."""
        engine, _ = Engine.start(REGISTRY, 'single_wait_all', [])
        run_to_completion(engine)
        result = engine.state.workflows['0'].result
        assert result == 2 or result == [2]


# ---- wait_any advanced ----

class TestWaitAnyAdvanced:
    def test_wait_any_returns_tuple(self):
        """wait_any returns (winner_id, result) tuple."""
        engine, _ = Engine.start(REGISTRY, 'race_tuple', [])
        run_to_completion(engine)
        wid, result = engine.state.workflows['0'].result
        assert result == 1

    def test_wait_any_all_same_speed(self):
        """When all children finish at the same time, first in list wins."""
        engine, _ = Engine.start(REGISTRY, 'tie_race', [])
        run_to_completion(engine)
        assert engine.state.workflows['0'].result is not None

    def test_wait_any_then_wait_remaining(self):
        """After wait_any, can still wait for the remaining children."""
        engine, _ = Engine.start(REGISTRY, 'race_then_all', [])
        run_to_completion(engine)
        assert engine.state.workflows['0'].result == 1 + 3

    def test_sequential_wait_any(self):
        """Multiple wait_any calls in sequence."""
        engine, _ = Engine.start(REGISTRY, 'multi_race', [])
        run_to_completion(engine)
        results = engine.state.workflows['0'].result
        assert len(results) == 2
        assert results[0] == 1


# ---- complex engine scenarios ----

class TestComplexScenarios:
    def test_diamond_dependency(self):
        """A -> B, A -> C, D waits for both B and C."""
        engine, _ = Engine.start(REGISTRY, 'diamond', [])
        run_to_completion(engine)
        assert engine.state.workflows['0'].result == 30

    def test_chain_of_waits(self):
        """A waits for B which waits for C — 3 levels deep."""
        engine, _ = Engine.start(REGISTRY, 'level1', [])
        run_to_completion(engine)
        assert engine.state.workflows['0'].result == 102

    def test_many_concurrent_children(self):
        """Parent spawns many children, waits for all."""
        engine, _ = Engine.start(REGISTRY, 'many_children', [])
        assert len(engine.state.workflows) == 21
        run_to_completion(engine)
        assert engine.state.workflows['0'].result == 20

    def test_child_spawns_grandchildren(self):
        """Parent -> child -> grandchildren, verify all finish."""
        engine, _ = Engine.start(REGISTRY, 'grandparent', [])
        run_to_completion(engine)
        assert engine.state.finished
        assert engine.state.workflows['0'].result == 1 + 2 + 3

    def test_checkpoint_resume_with_waiting(self):
        """Checkpoint while parent is waiting, resume and complete."""
        engine, _ = Engine.start(REGISTRY, 'waiter', [])
        engine.step()
        engine.step()

        data = pickle.dumps(engine.state)
        engine2 = Engine(pickle.loads(data), REGISTRY)
        run_to_completion(engine2)
        assert engine2.state.finished
        assert engine2.state.workflows['0'].result == 5

    def test_checkpoint_resume_with_concurrent_children(self):
        """Checkpoint mid-execution with multiple concurrent children."""
        engine, _ = Engine.start(REGISTRY, 'fan_out', [4])
        engine.step()
        engine.step()

        data = pickle.dumps(engine.state)
        engine2 = Engine(pickle.loads(data), REGISTRY)
        children = {k: v for k, v in engine2.state.workflows.items() if k != '0'}
        assert len(children) == 4

        run_to_completion(engine2)
        assert engine2.state.finished
        assert engine2.state.workflows['0'].result == 10

    def test_workflow_status_transitions(self):
        """Verify status transitions: running -> waiting -> running -> finished."""
        engine, _ = Engine.start(REGISTRY, 'status_test', [])
        root = engine.state.workflows['0']
        assert root.status == 'running'

        engine.step()
        assert root.status == 'waiting'

        for _ in range(10):
            if root.status == 'running':
                break
            engine.step()
        assert root.status == 'running'

        run_to_completion(engine)
        assert root.status == 'finished'
        assert root.result == 3
