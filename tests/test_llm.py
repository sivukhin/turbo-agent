"""LLM integration tests. Skipped if API keys are not set."""

import os
import pytest
from workflows import (
    workflow,
    conv_append,
    conv_read,
    Latest,
    Engine,
    EngineConfig,
    Store,
)
from workflows.ops import ai

has_anthropic = bool(os.environ.get("ANTHROPIC_API_KEY"))
has_openai = bool(os.environ.get("OPENAI_API_KEY"))

requires_anthropic = pytest.mark.skipif(
    not has_anthropic, reason="ANTHROPIC_API_KEY not set"
)
requires_openai = pytest.mark.skipif(not has_openai, reason="OPENAI_API_KEY not set")


@workflow
def ask(question):
    yield conv_append(role="user", content=question)
    response = yield ai(conversation=Latest)
    yield conv_append(role="assistant", content=response.text)
    return response.text


@workflow
def ask_openai(question):
    yield conv_append(role="user", content=question)
    response = yield ai(conversation=Latest, model="openai/gpt-4o-mini")
    yield conv_append(role="assistant", content=response.text)
    return response.text


@workflow
def ask_with_system(question):
    yield conv_append(role="user", content=question)
    response = yield ai(
        conversation=Latest,
        system="You are a calculator. Only respond with numbers.",
    )
    yield conv_append(role="assistant", content=response.text)
    return response.text


@workflow
def ask_with_tools():
    yield conv_append(role="user", content="What is 2+2?")
    response = yield ai(
        conversation=Latest,
        tools=[
            {
                "name": "calculate",
                "description": "Calculate a math expression",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "expression": {"type": "string"},
                    },
                    "required": ["expression"],
                },
            }
        ],
    )
    return {
        "text": response.text,
        "tool_calls": [(tc.name, tc.input) for tc in response.tool_calls],
        "stop_reason": response.stop_reason,
    }


@workflow
def multi_turn():
    yield conv_append(role="user", content="My name is Alice.")
    r1 = yield ai(conversation=Latest)
    yield conv_append(role="assistant", content=r1.text)

    yield conv_append(role="user", content="What is my name?")
    r2 = yield ai(conversation=Latest)
    yield conv_append(role="assistant", content=r2.text)
    return r2.text


REGISTRY = {
    "ask": ask,
    "ask_openai": ask_openai,
    "ask_with_system": ask_with_system,
    "ask_with_tools": ask_with_tools,
    "multi_turn": multi_turn,
}


def run_to_completion(engine, store, eid, max_steps=30):
    for _ in range(max_steps):
        state, _ = store.load_state(eid)
        if state.finished:
            break
        engine.step(store, eid)
    state, _ = store.load_state(eid)
    return state


@pytest.mark.llm
@requires_anthropic
class TestAnthropicLlm:
    def test_simple_question(self):
        store = Store(":memory:")
        engine = Engine(EngineConfig(workflows_registry=REGISTRY))
        eid = engine.start(store, "ask", ["What is 2+2? Reply with just the number."])
        state = run_to_completion(engine, store, eid)
        assert state.finished
        result = state.workflows[state.root_workflow_id].result
        assert "4" in result
        conv_id = state.workflows[state.root_workflow_id].conversation_id
        refs = store.conv_list_messages(conv_id)
        messages = store.conv_read_messages(refs)
        assert len(messages) == 2
        assert messages[0].role == "user"
        assert messages[1].role == "assistant"

    def test_system_prompt(self):
        store = Store(":memory:")
        engine = Engine(EngineConfig(workflows_registry=REGISTRY))
        eid = engine.start(store, "ask_with_system", ["What is 3+3?"])
        state = run_to_completion(engine, store, eid)
        assert state.finished
        assert "6" in state.workflows[state.root_workflow_id].result

    def test_tool_use(self):
        store = Store(":memory:")
        engine = Engine(EngineConfig(workflows_registry=REGISTRY))
        eid = engine.start(store, "ask_with_tools", [])
        state = run_to_completion(engine, store, eid)
        assert state.finished
        result = state.workflows[state.root_workflow_id].result
        assert result["stop_reason"] in ("tool_use", "end_turn")
        if result["tool_calls"]:
            assert result["tool_calls"][0][0] == "calculate"

    def test_multi_turn(self):
        store = Store(":memory:")
        engine = Engine(EngineConfig(workflows_registry=REGISTRY))
        eid = engine.start(store, "multi_turn", [])
        state = run_to_completion(engine, store, eid)
        assert state.finished
        assert "Alice" in state.workflows[state.root_workflow_id].result
        conv_id = state.workflows[state.root_workflow_id].conversation_id
        refs = store.conv_list_messages(conv_id)
        messages = store.conv_read_messages(refs)
        assert len(messages) == 4
        assert [m.role for m in messages] == ["user", "assistant", "user", "assistant"]

    def test_usage_in_events(self):
        store = Store(":memory:")
        engine = Engine(EngineConfig(workflows_registry=REGISTRY))
        eid = engine.start(store, "ask", ["Hi"])
        run_to_completion(engine, store, eid)
        inbox = store.read_inbox(eid)
        from workflows.events import LlmResponse

        llm_responses = [e for e in inbox if isinstance(e.payload, LlmResponse)]
        assert len(llm_responses) >= 1
        assert llm_responses[0].payload.usage["input_tokens"] > 0
        assert llm_responses[0].payload.usage["output_tokens"] > 0


@pytest.mark.llm
@requires_openai
class TestOpenAILlm:
    def test_simple_question(self):
        store = Store(":memory:")
        engine = Engine(EngineConfig(workflows_registry=REGISTRY))
        eid = engine.start(
            store, "ask_openai", ["What is 2+2? Reply with just the number."]
        )
        state = run_to_completion(engine, store, eid)
        assert state.finished
        assert "4" in state.workflows[state.root_workflow_id].result
