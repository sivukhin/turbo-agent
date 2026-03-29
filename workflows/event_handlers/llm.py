from dotenv import load_dotenv

from workflows.event_handlers.base import resolve_wf, make_inbox_event, register_event_handler
from workflows.llm.anthropic import AnthropicProvider
from workflows.llm.openai import OpenAIProvider
import workflows.events as ev

load_dotenv()

DEFAULT_PROVIDERS = {
    'anthropic': AnthropicProvider(),
    'openai': OpenAIProvider(),
}


@register_event_handler(ev.LlmRequest)
class LlmRequestHandler:
    def __init__(self, providers: dict | None = None):
        self.providers = providers or dict(DEFAULT_PROVIDERS)

    def _get_provider(self, model: str):
        if '/' in model:
            prefix = model.split('/')[0]
            provider = self.providers.get(prefix)
            if provider:
                return provider
        for provider in self.providers.values():
            return provider
        raise RuntimeError(f'No provider found for model: {model}')

    def _get_model_name(self, model: str) -> str:
        if '/' in model:
            return model.split('/', 1)[1]
        return model

    def handle(self, event, store, state):
        payload = event.payload
        provider = self._get_provider(payload.model)
        model_name = self._get_model_name(payload.model)

        # Resolve messages: from conversation_ref or inline
        if payload.conversation_ref and store:
            from workflows.conversation import MessageRef
            ref = payload.conversation_ref
            msg_refs = store.conv_list_messages(
                ref['conversation_id'], ref.get('message_id'), ref.get('layer'),
            )
            conv_msgs = store.conv_read_messages(msg_refs)
            messages = [{'role': m.role, 'content': m.content} for m in conv_msgs]
            # Extract system messages
            system = payload.system
            system_parts = [m['content'] for m in messages if m['role'] == 'system']
            messages = [m for m in messages if m['role'] != 'system']
            if system_parts:
                combined = '\n'.join(system_parts)
                system = f'{system}\n{combined}' if system else combined
        else:
            messages = payload.messages or []
            system = payload.system

        result = provider.complete(
            messages=messages, model=model_name,
            max_tokens=payload.max_tokens, temperature=payload.temperature,
            system=system, tools=payload.tools,
        )
        resolve_wf(state, event.workflow_id, result)
        return [make_inbox_event(event, ev.LlmResponse(
            content=result.content, model=result.model,
            stop_reason=result.stop_reason, usage=result.usage,
            text=result.text,
            tool_calls=[{'id': tc.id, 'name': tc.name, 'input': tc.input}
                        for tc in result.tool_calls] or None,
            message_id=result.message_id,
        ))]
