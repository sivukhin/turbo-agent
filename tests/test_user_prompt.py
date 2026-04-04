"""Tests for user_prompt operation."""

import pytest
from workflows import workflow, wait, Engine, EngineConfig, Store
from workflows.ops import user_prompt
from workflows.events import UserPromptRequest, UserPromptResult


@workflow
def ask_name():
    name = yield user_prompt()
    return f"Hello, {name}!"


@workflow
def two_prompts():
    name = yield user_prompt()
    age = yield user_prompt()
    return f"{name} is {age}"


@workflow
def parent_with_prompts():
    a = child_prompt("A")
    b = child_prompt("B")
    ra = yield wait(a)
    rb = yield wait(b)
    return [ra, rb]


@workflow
def child_prompt(label):
    answer = yield user_prompt()
    return f"{label}={answer}"


REGISTRY = {
    "ask_name": ask_name,
    "two_prompts": two_prompts,
    "parent_with_prompts": parent_with_prompts,
    "child_prompt": child_prompt,
}


def answer_prompts(store, eid, engine, answers: list[str]):
    """Find unanswered prompts and answer them in order."""
    outbox = store.read_outbox(eid)
    inbox = store.read_inbox(eid)
    answered_ids = {
        e.payload.request_id for e in inbox if isinstance(e.payload, UserPromptResult)
    }
    unanswered = [
        e
        for e in outbox
        if isinstance(e.payload, UserPromptRequest)
        and e.payload.request_id not in answered_ids
    ]

    for event, answer in zip(unanswered, answers):
        store.append_event(
            eid,
            event.workflow_id,
            "inbox",
            UserPromptResult(request_id=event.payload.request_id, response=answer),
        )
        engine.step(store, eid)


def run_to_completion(engine, store, eid, answer_sequence: list[str], max_steps=50):
    answers = list(answer_sequence)
    for _ in range(max_steps):
        state, _ = store.load_state(eid)
        if state.finished:
            break

        # Check for unanswered prompts
        outbox = store.read_outbox(eid)
        inbox = store.read_inbox(eid)
        answered_ids = {
            e.payload.request_id
            for e in inbox
            if isinstance(e.payload, UserPromptResult)
        }
        unanswered = [
            e
            for e in outbox
            if isinstance(e.payload, UserPromptRequest)
            and e.payload.request_id not in answered_ids
        ]

        if unanswered and answers:
            event = unanswered[0]
            store.append_event(
                eid,
                event.workflow_id,
                "inbox",
                UserPromptResult(
                    request_id=event.payload.request_id, response=answers.pop(0)
                ),
            )

        engine.step(store, eid)
    state, _ = store.load_state(eid)
    return state


class TestUserPrompt:
    def test_single_prompt(self):
        store = Store(":memory:")
        engine = Engine(EngineConfig(workflows_registry=REGISTRY))
        eid = engine.start(store, "ask_name", [])
        state = run_to_completion(engine, store, eid, ["Alice"])
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == "Hello, Alice!"

    def test_prompt_request_in_outbox(self):
        store = Store(":memory:")
        engine = Engine(EngineConfig(workflows_registry=REGISTRY))
        eid = engine.start(store, "ask_name", [])
        outbox = store.read_outbox(eid)
        prompts = [e for e in outbox if isinstance(e.payload, UserPromptRequest)]
        assert len(prompts) == 1

    def test_workflow_waits_for_answer(self):
        store = Store(":memory:")
        engine = Engine(EngineConfig(workflows_registry=REGISTRY))
        eid = engine.start(store, "ask_name", [])
        engine.step(store, eid)
        state, _ = store.load_state(eid)
        assert not state.finished
        root = state.workflows[state.root_workflow_id]
        assert root.status == "waiting"

    def test_two_sequential_prompts(self):
        store = Store(":memory:")
        engine = Engine(EngineConfig(workflows_registry=REGISTRY))
        eid = engine.start(store, "two_prompts", [])
        state = run_to_completion(engine, store, eid, ["Bob", "30"])
        assert state.finished
        assert state.workflows[state.root_workflow_id].result == "Bob is 30"

    def test_concurrent_prompts_from_children(self):
        store = Store(":memory:")
        engine = Engine(EngineConfig(workflows_registry=REGISTRY))
        eid = engine.start(store, "parent_with_prompts", [])
        state = run_to_completion(engine, store, eid, ["answer-a", "answer-b"])
        assert state.finished
        result = state.workflows[state.root_workflow_id].result
        assert "A=answer-a" in result
        assert "B=answer-b" in result

    def test_answer_in_event_log(self):
        store = Store(":memory:")
        engine = Engine(EngineConfig(workflows_registry=REGISTRY))
        eid = engine.start(store, "ask_name", [])
        run_to_completion(engine, store, eid, ["Eve"])
        inbox = store.read_inbox(eid)
        results = [e for e in inbox if isinstance(e.payload, UserPromptResult)]
        assert len(results) == 1
        assert results[0].payload.response == "Eve"
