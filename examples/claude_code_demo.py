"""Example: run Claude Code via shell stream with automatic conversation capture.

The ClaudeStreamHandler event handler automatically parses Claude Code's
stream-json output and appends conversation events — no manual parsing needed.

Tag the stream with meta={'claude_code': True} to enable it.

Usage:
    python main.py run examples/claude_code_demo.py:ask_claude '"What is 2+2?"' --workdir /tmp/claude-demo
    python main.py run examples/claude_code_demo.py:math_challenge --workdir /tmp/claude-math
"""

import json
import os
from workflows import (
    workflow,
    wait,
    conv_append,
    shell_stream_start,
    shell_stream_next,
)
from workflows.ops import ai_response
from workflows.isolation import DockerIsolation


CLAUDE_MODEL = "sonnet"
DOCKER_IMAGE = "turbo-review"


def _private_env():
    return {'ANTHROPIC_API_KEY': os.environ.get('ANTHROPIC_API_KEY', '')}


@workflow
def run_claude(prompt, isolation, public_env=None):
    """Run Claude Code, drain stream, return final result text.

    ClaudeStreamHandler handles conversation capture automatically.
    """
    stream = yield shell_stream_start(
        ['claude', '--model', CLAUDE_MODEL,
         '--output-format', 'stream-json',
         '--verbose',
         '--dangerously-skip-permissions',
         '-p', prompt],
        isolation=isolation,
        public_env=public_env or {},
        private_env=_private_env(),
        meta={'claude_code': True},
    )

    final_text = ''
    while True:
        raw = yield shell_stream_next(stream, private_env=_private_env())
        for line in raw.stdout:
            try:
                event = json.loads(line)
                if event.get('type') == 'result' and event.get('result'):
                    final_text = event['result']
            except (json.JSONDecodeError, KeyError):
                pass
        if raw.finished:
            break

    return final_text


@workflow
def ask_claude(prompt):
    """Run Claude Code with a prompt, capturing all events into the conversation."""
    yield conv_append(role='user', content=prompt)

    handle = run_claude(
        prompt,
        isolation=DockerIsolation(image=DOCKER_IMAGE, network='host'),
        public_env={'IS_SANDBOX': '1'},
    )
    result = yield wait(handle)

    if result:
        yield ai_response(result)
    return result or 'no output'


@workflow
def math_challenge():
    """Ask Claude a hard math question that requires Python to solve.

    Claude Code will use its Bash tool to run Python, and the ClaudeStreamHandler
    will capture the full tool_use / tool_result trace in the conversation.
    """
    prompt = (
        "Compute the exact value of the sum: sum_{k=1}^{1000} floor(sqrt(k)). "
        "Use Python to compute it. Show your work and verify the answer."
    )
    yield conv_append(role='user', content=prompt)

    handle = run_claude(
        prompt,
        isolation=DockerIsolation(image=DOCKER_IMAGE, network='host'),
        public_env={'IS_SANDBOX': '1'},
    )
    result = yield wait(handle)

    if result:
        yield ai_response(result)
    return result or 'no output'
