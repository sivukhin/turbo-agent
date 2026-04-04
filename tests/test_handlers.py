"""Unit tests for handler implementations."""

from workflows.handlers import WaitHandler, WaitAllHandler, WaitAnyHandler, SleepHandler
from workflows.events import WorkflowFinished


class MockWf:
    """Mock workflow state for resolve() tests."""

    def __init__(self):
        self.status = "waiting"
        self.send_val = None


class TestWaitHandler:
    def test_initial_state(self):
        state = WaitHandler.initial_state(["dep-1"])
        assert state == {"dep": "dep-1", "result": None, "resolved": False}

    def test_ignores_unrelated_messages(self):
        state = WaitHandler.initial_state(["dep-1"])
        state = WaitHandler.on_event(
            "workflow_finished", "dep-2", WorkflowFinished(result=99), state
        )
        assert not state["resolved"]

    def test_resolves_on_matching_finish(self):
        state = WaitHandler.initial_state(["dep-1"])
        state = WaitHandler.on_event(
            "workflow_finished", "dep-1", WorkflowFinished(result=42), state
        )
        assert state["resolved"]
        wf = MockWf()
        assert WaitHandler.resolve(state, wf, 0)
        assert wf.status == "running"
        assert wf.send_val == 42

    def test_ignores_tick_messages(self):
        state = WaitHandler.initial_state(["dep-1"])
        state = WaitHandler.on_event("tick", None, {}, state)
        assert not state["resolved"]

    def test_not_resolved_initially(self):
        state = WaitHandler.initial_state(["dep-1"])
        wf = MockWf()
        assert not WaitHandler.resolve(state, wf, 0)
        assert wf.status == "waiting"


class TestWaitAllHandler:
    def test_initial_state(self):
        state = WaitAllHandler.initial_state(["a", "b", "c"])
        assert state["deps"] == ["a", "b", "c"]
        assert state["results"] == {}

    def test_collects_results(self):
        state = WaitAllHandler.initial_state(["a", "b"])
        state = WaitAllHandler.on_event(
            "workflow_finished", "a", WorkflowFinished(result=10), state
        )
        wf = MockWf()
        assert not WaitAllHandler.resolve(state, wf, 0)

        state = WaitAllHandler.on_event(
            "workflow_finished", "b", WorkflowFinished(result=20), state
        )
        assert WaitAllHandler.resolve(state, wf, 0)
        assert wf.send_val == [10, 20]

    def test_preserves_order(self):
        state = WaitAllHandler.initial_state(["x", "y", "z"])
        state = WaitAllHandler.on_event(
            "workflow_finished", "z", WorkflowFinished(result=3), state
        )
        state = WaitAllHandler.on_event(
            "workflow_finished", "x", WorkflowFinished(result=1), state
        )
        state = WaitAllHandler.on_event(
            "workflow_finished", "y", WorkflowFinished(result=2), state
        )
        wf = MockWf()
        WaitAllHandler.resolve(state, wf, 0)
        assert wf.send_val == [1, 2, 3]

    def test_single_dep_returns_list(self):
        state = WaitAllHandler.initial_state(["only"])
        state = WaitAllHandler.on_event(
            "workflow_finished", "only", WorkflowFinished(result=99), state
        )
        wf = MockWf()
        WaitAllHandler.resolve(state, wf, 0)
        assert wf.send_val == [99]

    def test_ignores_unrelated(self):
        state = WaitAllHandler.initial_state(["a", "b"])
        state = WaitAllHandler.on_event(
            "workflow_finished", "c", WorkflowFinished(result=0), state
        )
        wf = MockWf()
        assert not WaitAllHandler.resolve(state, wf, 0)

    def test_empty_deps_resolves_immediately(self):
        state = WaitAllHandler.initial_state([])
        wf = MockWf()
        assert WaitAllHandler.resolve(state, wf, 0)
        assert wf.send_val == []


class TestWaitAnyHandler:
    def test_initial_state(self):
        state = WaitAnyHandler.initial_state(["a", "b", "c"])
        assert state["deps"] == ["a", "b", "c"]
        assert state["results"] == {}

    def test_resolves_on_first_finish(self):
        state = WaitAnyHandler.initial_state(["a", "b", "c"])
        state = WaitAnyHandler.on_event(
            "workflow_finished", "b", WorkflowFinished(result=42), state
        )
        wf = MockWf()
        assert WaitAnyHandler.resolve(state, wf, 0)
        assert wf.send_val == [(False, None), (True, 42), (False, None)]

    def test_multiple_finished(self):
        state = WaitAnyHandler.initial_state(["a", "b", "c"])
        state = WaitAnyHandler.on_event(
            "workflow_finished", "a", WorkflowFinished(result=1), state
        )
        state = WaitAnyHandler.on_event(
            "workflow_finished", "c", WorkflowFinished(result=3), state
        )
        wf = MockWf()
        WaitAnyHandler.resolve(state, wf, 0)
        assert wf.send_val == [(True, 1), (False, None), (True, 3)]

    def test_not_resolved_initially(self):
        state = WaitAnyHandler.initial_state(["a", "b"])
        wf = MockWf()
        assert not WaitAnyHandler.resolve(state, wf, 0)

    def test_ignores_unrelated(self):
        state = WaitAnyHandler.initial_state(["a", "b"])
        state = WaitAnyHandler.on_event(
            "workflow_finished", "c", WorkflowFinished(result=0), state
        )
        wf = MockWf()
        assert not WaitAnyHandler.resolve(state, wf, 0)


class TestSleepHandler:
    def test_initial_state(self):
        state = SleepHandler.initial_state(1000.0)
        assert state == {"wake_at": 1000.0}

    def test_not_resolved_before_wake(self):
        state = SleepHandler.initial_state(1000.0)
        wf = MockWf()
        assert not SleepHandler.resolve(state, wf, 999.0)
        assert wf.status == "waiting"

    def test_resolves_at_exact_wake_time(self):
        state = SleepHandler.initial_state(1000.0)
        wf = MockWf()
        assert SleepHandler.resolve(state, wf, 1000.0)
        assert wf.status == "running"
        assert wf.send_val is None

    def test_resolves_after_wake_time(self):
        state = SleepHandler.initial_state(1000.0)
        wf = MockWf()
        assert SleepHandler.resolve(state, wf, 1500.0)
        assert wf.status == "running"

    def test_ignores_messages(self):
        state = SleepHandler.initial_state(1000.0)
        state2 = SleepHandler.on_event("tick", None, {}, state)
        assert state2 == state
        state3 = SleepHandler.on_event(
            "workflow_finished", "x", WorkflowFinished(result=1), state
        )
        assert state3 == state
