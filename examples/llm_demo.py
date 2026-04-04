from workflows import workflow, wait, conv_append, conv_list, conv_read, Latest
from workflows.ops import ai


@workflow
def ask(question):
    """Simple question → answer using conversation."""
    yield conv_append(role="user", content=question)
    response = yield ai(conversation=Latest)
    yield conv_append(role="assistant", content=response.text)
    return response.text


@workflow
def multi_turn():
    """Multi-turn conversation with memory."""
    yield conv_append(role="user", content="My name is Alice.")
    r1 = yield ai(conversation=Latest)
    yield conv_append(role="assistant", content=r1.text)

    yield conv_append(role="user", content="What is my name?")
    r2 = yield ai(conversation=Latest)
    yield conv_append(role="assistant", content=r2.text)

    refs = yield conv_list()
    messages = yield conv_read(refs)
    return {
        "answer": r2.text,
        "history": [f"{m.role}: {m.content[:50]}" for m in messages],
    }


@workflow
def tool_use_demo():
    """LLM with tool use."""
    weather_tool = {
        "name": "get_weather",
        "description": "Get current weather for a city",
        "input_schema": {
            "type": "object",
            "properties": {
                "city": {"type": "string", "description": "City name"},
            },
            "required": ["city"],
        },
    }

    yield conv_append(role="user", content="What's the weather in Tokyo?")
    response = yield ai(conversation=Latest, tools=[weather_tool])

    if response.tool_calls:
        call = response.tool_calls[0]
        yield conv_append(role="assistant", content=str(response.content))
        tool_result = f"72°F, sunny in {call.input['city']}"
        yield conv_append(role="user", content=f"[tool_result: {tool_result}]")
        response2 = yield ai(conversation=Latest, tools=[weather_tool])
        yield conv_append(role="assistant", content=response2.text)
        return response2.text

    yield conv_append(role="assistant", content=response.text)
    return response.text
