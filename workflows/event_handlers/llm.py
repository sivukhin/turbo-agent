import json

from dotenv import load_dotenv

from workflows.cost import compute_cost
from workflows.event_handlers.base import (
    resolve_wf,
    make_inbox_event,
    register_event_handler,
)
from workflows.llm.anthropic import AnthropicProvider
from workflows.llm.openai import OpenAIProvider
from workflows.models.state import Event
import workflows.events as ev

load_dotenv()

DEFAULT_PROVIDERS = {
    "anthropic": AnthropicProvider(),
    "openai": OpenAIProvider(),
}


def _conv_to_llm_messages(conv_msgs):
    """Convert internal conversation messages to LLM API format.

    Internal roles tool_use/tool_result are converted:
      - tool_use → assistant message with tool_use content block
      - tool_result → user message with tool_result content block
    """
    result = []
    for m in conv_msgs:
        if m.role == "tool_use":
            data = json.loads(m.content) if isinstance(m.content, str) else m.content
            result.append(
                {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": data["id"],
                            "name": data["name"],
                            "input": data["input"],
                        },
                    ],
                }
            )
        elif m.role == "tool_result":
            data = json.loads(m.content) if isinstance(m.content, str) else m.content
            result.append(
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": data["tool_use_id"],
                            "content": data["output"],
                        },
                    ],
                }
            )
        else:
            result.append({"role": m.role, "content": m.content})
    return result


@register_event_handler(ev.LlmRequest)
class LlmRequestHandler:
    def __init__(self, providers: dict | None = None):
        self.providers = providers or dict(DEFAULT_PROVIDERS)

    def _get_provider(self, model: str):
        if "/" in model:
            prefix = model.split("/")[0]
            provider = self.providers.get(prefix)
            if provider:
                return provider
        for provider in self.providers.values():
            return provider
        raise RuntimeError(f"No provider found for model: {model}")

    def _get_model_name(self, model: str) -> str:
        if "/" in model:
            return model.split("/", 1)[1]
        return model

    def handle(self, event, store, state):
        payload = event.payload
        provider = self._get_provider(payload.model)
        model_name = self._get_model_name(payload.model)

        # Resolve messages: from conversation_ref or inline
        if payload.conversation_ref and store:
            ref = payload.conversation_ref
            msg_refs = store.conv_list_messages(
                ref.conversation_id,
                ref.message_id,
                ref.layer,
            )
            conv_msgs = store.conv_read_messages(msg_refs)
            messages = _conv_to_llm_messages(conv_msgs)
        else:
            messages = payload.messages or []

        result = provider.complete(
            messages=messages,
            model=model_name,
            max_tokens=payload.max_tokens,
            temperature=payload.temperature,
            system=payload.system,
            tools=payload.tools,
        )
        resolve_wf(state, event.workflow_id, result)

        usage = result.usage or {}
        input_tokens = usage.get('input_tokens', 0)
        output_tokens = usage.get('output_tokens', 0)
        cache_creation = usage.get('cache_creation_input_tokens', 0)
        cache_read = usage.get('cache_read_input_tokens', 0) or usage.get('cached_tokens', 0)
        cost = compute_cost(
            payload.model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_creation_input_tokens=cache_creation,
            cache_read_input_tokens=cache_read,
        )

        return [
            make_inbox_event(
                event,
                ev.LlmResponse(
                    content=result.content,
                    model=result.model,
                    stop_reason=result.stop_reason,
                    usage=result.usage,
                    text=result.text,
                    tool_calls=[
                        {"id": tc.id, "name": tc.name, "input": tc.input}
                        for tc in result.tool_calls
                    ]
                    or None,
                    message_id=result.message_id,
                    meta=payload.meta,
                ),
            ),
            Event(
                event_id=0,
                execution_id=event.execution_id,
                workflow_id=event.workflow_id,
                category='inbox',
                payload=ev.UsageEvent(
                    model=payload.model,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    cache_creation_input_tokens=cache_creation,
                    cache_read_input_tokens=cache_read,
                    cost_usd=cost,
                    source='llm',
                    meta=payload.meta,
                ),
            ),
        ]
