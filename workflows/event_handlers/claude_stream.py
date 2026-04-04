"""Event handler that parses Claude Code stream-json lines from ShellStreamLineEvent
and emits ConvAppendRequest events for assistant text, tool_use, tool_result, etc.
Also emits UsageEvent when the result event contains usage/cost data.

Only processes events with meta['claude_code'] == True.
Emits outbox events so the normal ConvAppendRequestHandler processes them.
"""

import json

from workflows.cost import compute_cost
from workflows.event_handlers.base import register_event_handler
from workflows.events import ShellStreamLineEvent
from workflows.models.state import Event
import workflows.events as ev


def _parse_claude_line(line):
    """Parse a single Claude Code stream-json line. Returns (role, content, meta_labels) or None."""
    if not line.strip():
        return None
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        return None

    etype = event.get('type')
    subtype = event.get('subtype', '')

    if etype == 'system':
        if subtype == 'init':
            return 'assistant', f"Claude Code session started (model: {event.get('model', '?')})", 'hidden'
        if subtype == 'task_started':
            return 'assistant', f"Agent task: {event.get('description', '?')}", 'hidden'
        if subtype == 'task_progress':
            desc = event.get('description', '')
            usage = event.get('usage', {})
            if desc:
                return 'assistant', f"Progress: {desc} (tokens: {usage.get('total_tokens', 0)}, tools: {usage.get('tool_uses', 0)})", 'hidden'
        return None

    if etype == 'assistant':
        msg = event.get('message', {})
        results = []
        for block in msg.get('content', []):
            btype = block.get('type')
            if btype == 'text':
                text = block.get('text', '')
                if text.strip():
                    results.append(('assistant', text, 'hidden'))
            elif btype == 'tool_use':
                results.append(('tool_use', {
                    'id': block.get('id', ''),
                    'name': block.get('name', '?'),
                    'input': block.get('input', {}),
                }, 'hidden'))
            elif btype == 'thinking':
                thinking = block.get('thinking', '')
                if thinking.strip():
                    results.append(('assistant', f"*thinking:* {thinking[:500]}", 'hidden'))
        return results if results else None

    if etype == 'user':
        msg = event.get('message', {})
        results = []
        for block in msg.get('content', []):
            if isinstance(block, dict) and block.get('type') == 'tool_result':
                output = block.get('content', '')
                if isinstance(output, list):
                    output = '\n'.join(
                        b.get('text', '') for b in output
                        if isinstance(b, dict) and b.get('type') == 'text'
                    )
                is_error = block.get('is_error', False)
                results.append(('tool_result', {
                    'tool_use_id': block.get('tool_use_id', ''),
                    'output': (f"ERROR: {output}" if is_error else str(output))[:2000],
                }, 'hidden'))
        return results if results else None

    if etype == 'result':
        result_text = event.get('result', '')
        if result_text:
            return 'assistant', result_text, ''
        return None

    return None


def _parse_usage(line, default_model='unknown'):
    """Extract usage/cost from a Claude Code result event line. Returns list of UsageEvent or empty list."""
    if not line.strip():
        return []
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        return []

    if event.get('type') != 'result':
        return []

    # Parse per-model usage from modelUsage dict
    model_usage = event.get('modelUsage', {})
    if model_usage:
        results = []
        for model, usage in model_usage.items():
            input_tokens = usage.get('inputTokens', 0)
            output_tokens = usage.get('outputTokens', 0)
            cache_creation = usage.get('cacheCreationInputTokens', 0)
            cache_read = usage.get('cacheReadInputTokens', 0)
            cost_usd = usage.get('costUSD', 0)
            if not cost_usd and (input_tokens or output_tokens):
                cost_usd = compute_cost(
                    model,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    cache_creation_input_tokens=cache_creation,
                    cache_read_input_tokens=cache_read,
                )
            results.append(ev.UsageEvent(
                model=model,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cache_creation_input_tokens=cache_creation,
                cache_read_input_tokens=cache_read,
                cost_usd=float(cost_usd),
                source='claude_code',
            ))
        return results

    # Fallback: top-level usage dict
    usage = event.get('usage', {})
    cost_usd = event.get('cost_usd') or event.get('total_cost_usd') or 0
    model = event.get('model', default_model)
    input_tokens = usage.get('input_tokens', 0) or usage.get('inputTokens', 0)
    output_tokens = usage.get('output_tokens', 0) or usage.get('outputTokens', 0)
    cache_creation = usage.get('cache_creation_input_tokens', 0) or usage.get('cacheCreationInputTokens', 0)
    cache_read = usage.get('cache_read_input_tokens', 0) or usage.get('cacheReadInputTokens', 0)

    if not cost_usd and (input_tokens or output_tokens):
        cost_usd = compute_cost(
            model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_creation_input_tokens=cache_creation,
            cache_read_input_tokens=cache_read,
        )

    if cost_usd or input_tokens or output_tokens:
        return [ev.UsageEvent(
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_creation_input_tokens=cache_creation,
            cache_read_input_tokens=cache_read,
            cost_usd=float(cost_usd),
            source='claude_code',
        )]
    return []


@register_event_handler(ShellStreamLineEvent)
class ClaudeStreamHandler:
    """Parses Claude Code stream-json lines and emits ConvAppendRequest outbox events."""

    def __init__(self):
        # Track model per stream from system/init events
        self._models: dict[str, str] = {}

    def handle(self, event, store, state):
        payload = event.payload
        if not payload.meta.get('claude_code'):
            return []

        wf = state.workflows.get(event.workflow_id)
        if not wf or not wf.conversation_id:
            return []

        # Track model from init events
        stream_id = payload.stream_id
        for line in payload.stdout:
            try:
                parsed_event = json.loads(line)
                if parsed_event.get('type') == 'system' and parsed_event.get('subtype') == 'init':
                    self._models[stream_id] = parsed_event.get('model', 'unknown')
            except (json.JSONDecodeError, KeyError):
                pass

        new_events = []
        for line in payload.stdout:
            parsed = _parse_claude_line(line)
            if parsed is not None:
                items = parsed if isinstance(parsed, list) else [parsed]
                for role, content, labels in items:
                    if not isinstance(content, str):
                        content = json.dumps(content)
                    meta = {'labels': labels} if labels else {}
                    new_events.append(Event(
                        event_id=0,
                        execution_id=event.execution_id,
                        workflow_id=event.workflow_id,
                        category='outbox',
                        payload=ev.ConvAppendRequest(
                            conversation_id=wf.conversation_id,
                            role=role,
                            content=content,
                            meta=meta,
                        ),
                    ))

            for usage in _parse_usage(line, default_model=self._models.get(stream_id, 'unknown')):
                new_events.append(Event(
                    event_id=0,
                    execution_id=event.execution_id,
                    workflow_id=event.workflow_id,
                    category='inbox',
                    payload=usage,
                ))

        return new_events
