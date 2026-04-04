"""Example: run Claude Code via shell stream with automatic conversation capture.

The ClaudeStreamHandler event handler automatically parses Claude Code's
stream-json output and appends conversation events — no manual parsing needed.

Tag the stream with meta={'claude_code': True} to enable it.

Usage:
    python main.py run examples/claude_code_demo.py:ask_claude '"What is 2+2?"' --workdir /tmp/claude-demo
"""

import os
from workflows import (
    workflow,
    conv_append,
    shell_stream_start,
    shell_stream_next,
)
from workflows.ops import ai_response
from workflows.isolation import DockerIsolation


CLAUDE_MODEL = "sonnet"
DOCKER_IMAGE = "turbo-review"


def _quote(s):
    return "'" + s.replace("'", "'\\''") + "'"


def _private_env():
    return {"ANTHROPIC_API_KEY": os.environ.get("ANTHROPIC_API_KEY", "")}


@workflow
def ask_claude(prompt):
    """Run Claude Code with a prompt, capturing all events into the conversation."""
    yield conv_append(role="user", content=prompt)

    cmd = (
        f"claude --model {CLAUDE_MODEL} "
        f"--output-format stream-json "
        f"--verbose "
        f"--dangerously-skip-permissions "
        f"-p {_quote(prompt)}"
    )
    stream = yield shell_stream_start(
        cmd,
        isolation=DockerIsolation(image=DOCKER_IMAGE, network="host"),
        public_env={"IS_SANDBOX": "1"},
        private_env=_private_env(),
        meta={"claude_code": True},
    )

    # Just drain the stream — ClaudeStreamHandler does the conversation work
    final_text = ""
    while True:
        raw = yield shell_stream_next(stream, private_env=_private_env())

        # Capture the final result text for the return value
        for line in raw.stdout:
            try:
                import json

                event = json.loads(line)
                if event.get("type") == "result" and event.get("result"):
                    final_text = event["result"]
            except (json.JSONDecodeError, KeyError):
                pass

        if raw.finished:
            break

    if final_text:
        yield ai_response(final_text)

    return final_text or "no output"
