from workflows import workflow, wait, llm, shell, write_file, read_file
from workflows.isolation import HostIsolation
from workflows.llm import AnthropicProvider


host = HostIsolation()
claude = AnthropicProvider()


@workflow
def ask(question):
    """Simple question → answer."""
    response = yield llm(
        messages=[{"role": "user", "content": question}],
        provider=claude,
    )
    return response.text


@workflow
def code_gen(task):
    """Ask LLM to write code, save to file, run it."""
    response = yield llm(
        messages=[{"role": "user", "content": f"Write a Python script that {task}. "
                   "Output ONLY the code, no explanation."}],
        provider=claude,
    )
    yield write_file('solution.py', response.text)
    result = yield shell('python3 solution.py', isolation=host)
    return {'code': response.text, 'output': result.stdout, 'exit_code': result.exit_code}


@workflow
def tool_use_demo():
    """LLM with tool use — ask about weather, handle tool calls."""
    weather_tool = {
        "name": "get_weather",
        "description": "Get current weather for a city",
        "input_schema": {
            "type": "object",
            "properties": {
                "city": {"type": "string", "description": "City name"}
            },
            "required": ["city"]
        }
    }

    response = yield llm(
        messages=[{"role": "user", "content": "What's the weather in Tokyo?"}],
        tools=[weather_tool],
        provider=claude,
    )

    if response.tool_calls:
        call = response.tool_calls[0]
        tool_result = f"72°F, sunny in {call.input['city']}"

        response2 = yield llm(
            messages=[
                {"role": "user", "content": "What's the weather in Tokyo?"},
                {"role": "assistant", "content": response.content},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": call.id,
                     "content": tool_result}
                ]},
            ],
            tools=[weather_tool],
            provider=claude,
        )
        return response2.text

    return response.text
