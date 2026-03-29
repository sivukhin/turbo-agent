from workflows.conversation import MessageRef, Message
from workflows.event_handlers.base import resolve_wf, make_inbox_event, register_event_handler
import workflows.events as ev


@register_event_handler(ev.ConvAppendRequest)
class ConvAppendRequestHandler:
    def handle(self, event, store, state):
        payload = event.payload
        ref = store.conv_append_message(payload.conversation_id, payload.role, payload.content, meta=payload.meta)
        resolve_wf(state, event.workflow_id, ref)
        return [make_inbox_event(event, ev.ConvAppendResult(
            conversation_id=ref.conversation_id,
            message_id=ref.message_id, layer=ref.layer, role=ref.role, meta=ref.meta,
        ))]


@register_event_handler(ev.ConvListRequest)
class ConvListRequestHandler:
    def handle(self, event, store, state):
        payload = event.payload
        refs = store.conv_list_messages(
            payload.conversation_id, payload.end_message_id, payload.layer,
            start_message_id=payload.start_message_id,
            role_filter=payload.role_filter, pattern=payload.pattern,
        )
        resolve_wf(state, event.workflow_id, refs)
        return [make_inbox_event(event, ev.ConvListResult(
            count=len(refs),
            message_refs=[{'conversation_id': r.conversation_id,
                           'message_id': r.message_id,
                           'layer': r.layer, 'role': r.role,
                           'meta': r.meta} for r in refs],
        ))]


@register_event_handler(ev.ConvReadRequest)
class ConvReadRequestHandler:
    def handle(self, event, store, state):
        payload = event.payload
        refs = [MessageRef(
            conversation_id=r['conversation_id'], message_id=r['message_id'],
            layer=r['layer'], role=r['role'], meta=r.get('meta', {}),
        ) for r in payload.message_refs]
        messages = store.conv_read_messages(refs)
        resolve_wf(state, event.workflow_id, messages)
        return [make_inbox_event(event, ev.ConvReadResult(count=len(messages)))]


@register_event_handler(ev.ConvReplaceWithRequest)
class ConvReplaceWithRequestHandler:
    def handle(self, event, store, state):
        payload = event.payload
        new_refs = store.conv_replace_with(
            payload.conversation_id, payload.new_messages,
            payload.start_message_id, payload.end_message_id,
        )
        resolve_wf(state, event.workflow_id, new_refs)
        return [make_inbox_event(event, ev.ConvReplaceWithResult(
            conversation_id=payload.conversation_id,
            new_layer=new_refs[0].layer if new_refs else 0,
            new_message_refs=[{'conversation_id': r.conversation_id,
                               'message_id': r.message_id,
                               'layer': r.layer, 'role': r.role,
                               'meta': r.meta} for r in new_refs],
        ))]
