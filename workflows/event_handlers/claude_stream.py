"""Event handler that parses Claude Code stream-json lines from ShellStreamLineEvent
and emits conversation events (conv_append) for assistant text, tool_use, tool_result, etc.

Only processes events with meta['claude_code'] == True.
"""

import json

from workflows.event_handlers.base import make_inbox_event, register_event_handler
from workflows.events import ShellStreamLineEvent
import workflows.events as ev


def _parse_claude_line(line):
    """Parse a single Claude Code stream-json line. Returns (role, content, meta_labels) or None."""
    if not line.strip():
        return None
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        return None

    etype = event.get("type")
    subtype = event.get("subtype", "")

    if etype == "system":
        if subtype == "init":
            return (
                "assistant",
                f"Claude Code session started (model: {event.get('model', '?')})",
                "hidden",
            )
        if subtype == "task_started":
            return "assistant", f"Agent task: {event.get('description', '?')}", "hidden"
        if subtype == "task_progress":
            desc = event.get("description", "")
            usage = event.get("usage", {})
            if desc:
                return (
                    "assistant",
                    f"Progress: {desc} (tokens: {usage.get('total_tokens', 0)}, tools: {usage.get('tool_uses', 0)})",
                    "hidden",
                )
        return None

    if etype == "assistant":
        msg = event.get("message", {})
        results = []
        for block in msg.get("content", []):
            btype = block.get("type")
            if btype == "text":
                text = block.get("text", "")
                if text.strip():
                    results.append(("assistant", text, "hidden"))
            elif btype == "tool_use":
                results.append(
                    (
                        "tool_use",
                        {
                            "id": block.get("id", ""),
                            "name": block.get("name", "?"),
                            "input": block.get("input", {}),
                        },
                        "hidden",
                    )
                )
            elif btype == "thinking":
                thinking = block.get("thinking", "")
                if thinking.strip():
                    results.append(
                        ("assistant", f"*thinking:* {thinking[:500]}", "hidden")
                    )
        return results if results else None

    if etype == "user":
        msg = event.get("message", {})
        results = []
        for block in msg.get("content", []):
            if isinstance(block, dict) and block.get("type") == "tool_result":
                output = block.get("content", "")
                if isinstance(output, list):
                    output = "\n".join(
                        b.get("text", "")
                        for b in output
                        if isinstance(b, dict) and b.get("type") == "text"
                    )
                is_error = block.get("is_error", False)
                results.append(
                    (
                        "tool_result",
                        {
                            "tool_use_id": block.get("tool_use_id", ""),
                            "output": (f"ERROR: {output}" if is_error else str(output))[
                                :2000
                            ],
                        },
                        "hidden",
                    )
                )
        return results if results else None

    if etype == "result":
        result_text = event.get("result", "")
        if result_text:
            return "assistant", result_text, ""
        return None

    return None


@register_event_handler(ShellStreamLineEvent)
class ClaudeStreamHandler:
    """Parses Claude Code stream-json lines and emits conversation events."""

    def handle(self, event, store, state):
        payload = event.payload
        if not payload.meta.get("claude_code"):
            return []

        wf = state.workflows.get(event.workflow_id)
        if not wf or not wf.conversation_id:
            return []

        new_events = []
        for line in payload.stdout:
            parsed = _parse_claude_line(line)
            if parsed is None:
                continue
            # Single result or list of results
            items = parsed if isinstance(parsed, list) else [parsed]
            for role, content, labels in items:
                if not isinstance(content, str):
                    content = json.dumps(content)
                meta = {"labels": labels} if labels else {}
                ref = store.conv_append_message(
                    wf.conversation_id,
                    role,
                    content,
                    meta=meta,
                    event_time=event.event_id,
                )
                new_events.append(
                    make_inbox_event(
                        event,
                        ev.ConvAppendResult(
                            conversation_id=ref.conversation_id,
                            message_id=ref.message_id,
                            layer=ref.layer,
                            role=ref.role,
                            meta=meta,
                        ),
                    )
                )

        return new_events
