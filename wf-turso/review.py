"""Turso PR review workflow.

Clones the repo, checks out the PR, builds tursodb, then runs Claude Code
inside Docker with stream-json output, capturing all events into the conversation.

Run via web UI: create a task, start execution with wf-turso/review.py:review
with args [<pr_number>]
"""

import json
import os
from workflows import (
    workflow,
    conv_append,
    shell,
    shell_stream_start,
    shell_stream_next,
)
from workflows.isolation import HostIsolation, DockerIsolation

TURSO_REPO = "tursodatabase/turso"
DOCKER_IMAGE = "turbo-review"
CLAUDE_MODEL = "sonnet"

REVIEW_PROMPT = """\
You are reviewing PR #{pr_number} for the Turso database (a SQLite-compatible database).
The repository is at /workspace/turso with the PR branch checked out.
The tursodb binary is built at /workspace/turso/target/debug/tursodb.

Do a thorough code review:
1. Understand what the PR changes (run git diff, git log, read files)
2. Identify correctness issues, edge cases, performance concerns
3. Write SQL reproducers using the tursodb binary to test the changes
4. Organize findings by severity (critical > major > minor > nits)

Write the full review to /workspace/REVIEW.md with:
- Summary of changes
- Detailed findings with code references
- SQL reproducers (fenced code blocks)
"""


def _quote(s):
    return "'" + s.replace("'", "'\\''") + "'"


def _private_env():
    return {"ANTHROPIC_API_KEY": os.environ.get("ANTHROPIC_API_KEY", "")}


@workflow
def review(pr_number):
    pr_number = int(pr_number)

    # Clone and build on host
    yield conv_append(
        role="assistant", content=f"Starting review of PR #{pr_number}..."
    )

    clone = yield shell(
        f"gh repo clone {TURSO_REPO} turso -- --depth=50",
        isolation=HostIsolation(),
    )
    if clone.exit_code != 0:
        yield conv_append(
            role="assistant", content=f"Clone failed:\n```\n{clone.stderr}\n```"
        )
        return "clone failed"

    checkout = yield shell(
        f"cd turso && gh pr checkout {pr_number}", isolation=HostIsolation()
    )
    if checkout.exit_code != 0:
        yield conv_append(
            role="assistant", content=f"Checkout failed:\n```\n{checkout.stderr}\n```"
        )
        return "checkout failed"

    yield conv_append(role="assistant", content="Building tursodb...")
    build = yield shell(
        "cd turso && cargo build --bin tursodb", isolation=HostIsolation()
    )
    if build.exit_code != 0:
        yield conv_append(
            role="assistant", content=f"Build failed:\n```\n{build.stderr[:2000]}\n```"
        )
        return "build failed"

    # Run Claude Code with stream-json output
    yield conv_append(role="assistant", content="Running Claude Code review...")
    prompt = REVIEW_PROMPT.format(pr_number=pr_number)
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
    )

    # Parse stream-json lines and build conversation
    text_buf = []
    tool_name = None
    tool_id = None
    tool_input_buf = []
    final_text = ""

    while True:
        raw = yield shell_stream_next(stream, private_env=_private_env())

        for line in raw.stdout:
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            etype = event.get("type")
            subtype = event.get("subtype", "")

            # system events: init, task_started, task_progress
            if etype == "system":
                if subtype == "init":
                    yield conv_append(
                        role="assistant",
                        content=f"Claude Code session started (model: {event.get('model', '?')})",
                        meta={"labels": "hidden"},
                    )
                elif subtype == "task_started":
                    yield conv_append(
                        role="assistant",
                        content=f"Agent task: {event.get('description', '?')}",
                        meta={"labels": "hidden"},
                    )
                elif subtype == "task_progress":
                    desc = event.get("description", "")
                    usage = event.get("usage", {})
                    if desc:
                        yield conv_append(
                            role="assistant",
                            content=f"Progress: {desc} (tokens: {usage.get('total_tokens', 0)}, tools: {usage.get('tool_uses', 0)})",
                            meta={"labels": "hidden"},
                        )

            # assistant messages: contain tool_use or text content
            elif etype == "assistant":
                msg = event.get("message", {})
                content_blocks = msg.get("content", [])
                for block in content_blocks:
                    if block.get("type") == "text":
                        text = block.get("text", "")
                        if text.strip():
                            final_text = text
                            yield conv_append(
                                role="assistant",
                                content=text,
                                meta={"labels": "hidden"},
                            )
                    elif block.get("type") == "tool_use":
                        yield conv_append(
                            role="tool_use",
                            content={
                                "id": block.get("id", ""),
                                "name": block.get("name", "?"),
                                "input": block.get("input", {}),
                            },
                            meta={"labels": "hidden"},
                        )
                    elif block.get("type") == "thinking":
                        thinking = block.get("thinking", "")
                        if thinking.strip():
                            yield conv_append(
                                role="assistant",
                                content=f"*thinking:* {thinking[:500]}",
                                meta={"labels": "hidden"},
                            )

            # user messages: tool results
            elif etype == "user":
                msg = event.get("message", {})
                content_blocks = msg.get("content", [])
                for block in content_blocks:
                    if block.get("type") == "tool_result":
                        output = block.get("content", "")
                        if isinstance(output, list):
                            output = "\n".join(
                                b.get("text", "")
                                for b in output
                                if isinstance(b, dict) and b.get("type") == "text"
                            )
                        is_error = block.get("is_error", False)
                        yield conv_append(
                            role="tool_result",
                            content={
                                "tool_use_id": block.get("tool_use_id", ""),
                                "output": (
                                    f"ERROR: {output}" if is_error else str(output)
                                )[:2000],
                            },
                            meta={"labels": "hidden"},
                        )

            # result: final output
            elif etype == "result":
                result_text = event.get("result", "")
                if result_text:
                    final_text = result_text

        if raw.finished:
            if raw.stderr:
                yield conv_append(
                    role="assistant",
                    content="stderr:\n```\n" + "\n".join(raw.stderr[:20]) + "\n```",
                    meta={"labels": "hidden"},
                )
            break

    # Read REVIEW.md
    read = yield shell("cat REVIEW.md", isolation=HostIsolation())
    if read.exit_code == 0 and read.stdout.strip():
        yield conv_append(role="assistant", content=read.stdout)
        return "review complete"

    # Fallback: use the final text from Claude Code
    if final_text:
        yield conv_append(role="assistant", content=final_text)
        return "review complete"

    yield conv_append(role="assistant", content="Review produced no output.")
    return "review failed"
