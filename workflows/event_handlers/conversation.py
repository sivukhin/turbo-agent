from workflows.conversation import MessageRef
from workflows.event_handlers.base import resolve_wf, make_inbox_event, register_event_handler
import workflows.events as ev


@register_event_handler(ev.ConvAppendRequest)
class ConvAppendRequestHandler:
    def handle(self, event, store, state):
        payload = event.payload
        ref = store.conv_append_message(payload.conversation_id, payload.role, payload.content)
        resolve_wf(state, event.workflow_id, MessageRef(
            conversation_id=ref.conversation_id,
            message_id=ref.message_id, layer=ref.layer,
        ))
        return [make_inbox_event(event, ev.ConvAppendResult(
            conversation_id=ref.conversation_id,
            message_id=ref.message_id, layer=ref.layer,
        ))]


@register_event_handler(ev.ConvReadRequest)
class ConvReadRequestHandler:
    def handle(self, event, store, state):
        payload = event.payload
        messages = store.conv_read_messages(
            payload.conversation_id, payload.end_message_id, payload.layer,
        )
        resolve_wf(state, event.workflow_id, messages)
        return [make_inbox_event(event, ev.ConvReadResult(
            count=len(messages),
            message_refs=[{'conversation_id': m.ref.conversation_id,
                           'message_id': m.ref.message_id,
                           'layer': m.ref.layer} for m in messages],
        ))]


@register_event_handler(ev.ConvSearchRequest)
class ConvSearchRequestHandler:
    def handle(self, event, store, state):
        payload = event.payload
        messages = store.conv_search_messages(payload.conversation_id, payload.pattern)
        resolve_wf(state, event.workflow_id, messages)
        return [make_inbox_event(event, ev.ConvSearchResult(
            count=len(messages),
            message_refs=[{'conversation_id': m.ref.conversation_id,
                           'message_id': m.ref.message_id,
                           'layer': m.ref.layer} for m in messages],
        ))]


@register_event_handler(ev.ConvGetRequest)
class ConvGetRequestHandler:
    def handle(self, event, store, state):
        payload = event.payload
        refs = [MessageRef(**r) for r in payload.message_refs]
        messages = store.conv_get_messages(refs)
        resolve_wf(state, event.workflow_id, messages)
        return [make_inbox_event(event, ev.ConvGetResult(count=len(messages)))]


@register_event_handler(ev.ConvReplaceWithRequest)
class ConvReplaceWithRequestHandler:
    def handle(self, event, store, state):
        payload = event.payload
        new_refs = store.conv_replace_with(
            payload.conversation_id, payload.new_messages,
            payload.start_message_id, payload.end_message_id,
        )
        resolve_wf(state, event.workflow_id, [
            MessageRef(r.conversation_id, r.message_id, r.layer) for r in new_refs
        ])
        return [make_inbox_event(event, ev.ConvReplaceWithResult(
            conversation_id=payload.conversation_id,
            new_layer=new_refs[0].layer if new_refs else 0,
            new_message_refs=[{'conversation_id': r.conversation_id,
                               'message_id': r.message_id,
                               'layer': r.layer} for r in new_refs],
        ))]
