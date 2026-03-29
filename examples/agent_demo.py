"""Terminal agent: LLM + conversation + user prompts + shell tool in Docker sandbox.

Run with: uv run main.py run examples/agent_demo.py:chat
"""

from workflows import workflow, conv_append, Latest, shell
from workflows.ops import ai, user_prompt, ai_response
from workflows.isolation import DockerIsolation

@workflow
def chat():
    """Interactive chat agent with shell tool access."""

    SYSTEM_PROMPT = (
        'You are a helpful coding assistant with access to a sandboxed shell. '
        'You can run Python code and shell commands via the run_shell tool. '
        'Be concise. When running code, show the output to the user.'
    )
    
    TOOLS = [{
        'name': 'run_shell',
        'description': 'Execute a shell command in a sandboxed Docker container (python:3.13-slim-bookworm). '
                        'Use this to run Python scripts, install packages, inspect files, etc.',
        'input_schema': {
            'type': 'object',
            'properties': {'command': {'type': 'string', 'description': 'The shell command to execute'}},
            'required': ['command'],
        },
    }]

    while True:
        question = yield user_prompt()
        if question.strip().lower() in ('quit', 'exit', 'bye'): break
        yield conv_append(role='user', content=question)

        while True:
            response = yield ai(conversation=Latest, system=SYSTEM_PROMPT, tools=TOOLS)
            if response.tool_calls:
                for block in response.content:
                    if block['type'] == 'text':
                        yield conv_append(role='assistant', content=block['text'], meta={'labels': 'hidden'})
                    elif block['type'] == 'tool_use':
                        yield conv_append(role='tool_use', content={
                            'id': block['id'], 'name': block['name'], 'input': block['input'],
                        }, meta={'labels': 'hidden'})
                for tool_call in response.tool_calls:
                    result = yield shell(
                        tool_call.input['command'],
                        isolation=DockerIsolation(image='python:3.13-slim-bookworm', network='none')
                    )
                    output = result.stdout.strip() or result.stderr.strip() or '(no output)'
                    if result.exit_code != 0: output = f'exit code {result.exit_code}\n{output}'
                    yield conv_append(role='tool_result', content={
                        'tool_use_id': tool_call.id, 'output': output,
                    }, meta={'labels': 'hidden'})
            else:
                yield conv_append(role='assistant', content=response.text)
                yield ai_response(response.text)
                break

    return 'Goodbye!'