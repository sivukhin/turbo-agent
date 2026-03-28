"""Unit tests for handler implementations."""

from workflows.handlers import WaitHandler, WaitAllHandler, WaitAnyHandler, SleepHandler


class TestWaitHandler:
    def test_initial_state(self):
        state = WaitHandler.initial_state(['dep-1'])
        assert state == {'dep': 'dep-1', 'result': None, 'resolved': False}

    def test_ignores_unrelated_messages(self):
        state = WaitHandler.initial_state(['dep-1'])
        state = WaitHandler.on_event('workflow_finished', 'dep-2', {'result': 99}, state)
        assert not state['resolved']

    def test_resolves_on_matching_finish(self):
        state = WaitHandler.initial_state(['dep-1'])
        state = WaitHandler.on_event('workflow_finished', 'dep-1', {'result': 42}, state)
        assert state['resolved']
        resolved, result = WaitHandler.try_resolve(state, 0)
        assert resolved
        assert result == 42

    def test_ignores_tick_messages(self):
        state = WaitHandler.initial_state(['dep-1'])
        state = WaitHandler.on_event('tick', None, {}, state)
        assert not state['resolved']

    def test_not_resolved_initially(self):
        state = WaitHandler.initial_state(['dep-1'])
        resolved, _ = WaitHandler.try_resolve(state, 0)
        assert not resolved


class TestWaitAllHandler:
    def test_initial_state(self):
        state = WaitAllHandler.initial_state(['a', 'b', 'c'])
        assert state['deps'] == ['a', 'b', 'c']
        assert state['results'] == {}

    def test_collects_results(self):
        state = WaitAllHandler.initial_state(['a', 'b'])
        state = WaitAllHandler.on_event('workflow_finished', 'a', {'result': 10}, state)
        resolved, _ = WaitAllHandler.try_resolve(state, 0)
        assert not resolved  # still waiting for b

        state = WaitAllHandler.on_event('workflow_finished', 'b', {'result': 20}, state)
        resolved, result = WaitAllHandler.try_resolve(state, 0)
        assert resolved
        assert result == [10, 20]

    def test_preserves_order(self):
        state = WaitAllHandler.initial_state(['x', 'y', 'z'])
        state = WaitAllHandler.on_event('workflow_finished', 'z', {'result': 3}, state)
        state = WaitAllHandler.on_event('workflow_finished', 'x', {'result': 1}, state)
        state = WaitAllHandler.on_event('workflow_finished', 'y', {'result': 2}, state)
        resolved, result = WaitAllHandler.try_resolve(state, 0)
        assert resolved
        assert result == [1, 2, 3]  # order matches deps, not arrival

    def test_single_dep_returns_list(self):
        state = WaitAllHandler.initial_state(['only'])
        state = WaitAllHandler.on_event('workflow_finished', 'only', {'result': 99}, state)
        resolved, result = WaitAllHandler.try_resolve(state, 0)
        assert resolved
        assert result == [99]  # always a list, use wait() for unwrapped

    def test_ignores_unrelated(self):
        state = WaitAllHandler.initial_state(['a', 'b'])
        state = WaitAllHandler.on_event('workflow_finished', 'c', {'result': 0}, state)
        resolved, _ = WaitAllHandler.try_resolve(state, 0)
        assert not resolved

    def test_empty_deps_resolves_immediately(self):
        state = WaitAllHandler.initial_state([])
        resolved, result = WaitAllHandler.try_resolve(state, 0)
        assert resolved
        assert result == []


class TestWaitAnyHandler:
    def test_initial_state(self):
        state = WaitAnyHandler.initial_state(['a', 'b', 'c'])
        assert state['deps'] == ['a', 'b', 'c']
        assert state['results'] == {}

    def test_resolves_on_first_finish(self):
        state = WaitAnyHandler.initial_state(['a', 'b', 'c'])
        state = WaitAnyHandler.on_event('workflow_finished', 'b', {'result': 42}, state)
        resolved, result = WaitAnyHandler.try_resolve(state, 0)
        assert resolved
        # Returns list: [(False, None), (True, 42), (False, None)]
        assert result == [(False, None), (True, 42), (False, None)]

    def test_multiple_finished(self):
        state = WaitAnyHandler.initial_state(['a', 'b', 'c'])
        state = WaitAnyHandler.on_event('workflow_finished', 'a', {'result': 1}, state)
        state = WaitAnyHandler.on_event('workflow_finished', 'c', {'result': 3}, state)
        resolved, result = WaitAnyHandler.try_resolve(state, 0)
        assert resolved
        assert result == [(True, 1), (False, None), (True, 3)]

    def test_not_resolved_initially(self):
        state = WaitAnyHandler.initial_state(['a', 'b'])
        resolved, _ = WaitAnyHandler.try_resolve(state, 0)
        assert not resolved

    def test_ignores_unrelated(self):
        state = WaitAnyHandler.initial_state(['a', 'b'])
        state = WaitAnyHandler.on_event('workflow_finished', 'c', {'result': 0}, state)
        resolved, _ = WaitAnyHandler.try_resolve(state, 0)
        assert not resolved


class TestSleepHandler:
    def test_initial_state(self):
        state = SleepHandler.initial_state(1000.0)
        assert state == {'wake_at': 1000.0}

    def test_not_resolved_before_wake(self):
        state = SleepHandler.initial_state(1000.0)
        resolved, _ = SleepHandler.try_resolve(state, 999.0)
        assert not resolved

    def test_resolves_at_exact_wake_time(self):
        state = SleepHandler.initial_state(1000.0)
        resolved, result = SleepHandler.try_resolve(state, 1000.0)
        assert resolved
        assert result is None

    def test_resolves_after_wake_time(self):
        state = SleepHandler.initial_state(1000.0)
        resolved, result = SleepHandler.try_resolve(state, 1500.0)
        assert resolved
        assert result is None

    def test_ignores_messages(self):
        state = SleepHandler.initial_state(1000.0)
        state2 = SleepHandler.on_event('tick', None, {}, state)
        assert state2 == state
        state3 = SleepHandler.on_event('workflow_finished', 'x', {'result': 1}, state)
        assert state3 == state
