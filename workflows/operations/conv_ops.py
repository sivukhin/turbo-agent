from workflows.operations.base import OpContext, register_handler
from workflows.ops import Event
from workflows.conversation import ConvAppendOp, ConvListOp, ConvReadOp, ConvReplaceWithOp
import workflows.events as ev


@register_handler(ConvAppendOp)
class ConvAppendOpHandler:
    @staticmethod
    def handle(val: ConvAppendOp, ctx: OpContext) -> None:
        if not ctx.store or not ctx.wf.conversation_id:
            return
        ctx.wf.status = 'waiting'
        ctx.new_events.append(Event(
            event_id=0, execution_id=ctx.execution_id,
            workflow_id=ctx.workflow_id, category='outbox',
            payload=ev.ConvAppendRequest(
                conversation_id=ctx.wf.conversation_id,
                role=val.role, content=val.content, meta=val.meta),
        ))


@register_handler(ConvListOp)
class ConvListOpHandler:
    @staticmethod
    def handle(val: ConvListOp, ctx: OpContext) -> None:
        if not ctx.store or not ctx.wf.conversation_id:
            return
        conv_id = ctx.wf.conversation_id
        if val.conversation and hasattr(val.conversation, 'conversation_id'):
            conv_id = val.conversation.conversation_id
        resolved = ctx.store.conv_resolve_ref(conv_id)
        ctx.wf.status = 'waiting'
        ctx.new_events.append(Event(
            event_id=0, execution_id=ctx.execution_id,
            workflow_id=ctx.workflow_id, category='outbox',
            payload=ev.ConvListRequest(
                conversation_id=resolved.conversation_id,
                end_message_id=resolved.message_id,
                layer=resolved.layer,
                start_message_id=val.start_message_id,
                role_filter=val.role_filter,
                pattern=val.pattern),
        ))


@register_handler(ConvReadOp)
class ConvReadOpHandler:
    @staticmethod
    def handle(val: ConvReadOp, ctx: OpContext) -> None:
        if not ctx.store:
            return
        ctx.wf.status = 'waiting'
        ctx.new_events.append(Event(
            event_id=0, execution_id=ctx.execution_id,
            workflow_id=ctx.workflow_id, category='outbox',
            payload=ev.ConvReadRequest(
                message_refs=[{'conversation_id': r.conversation_id,
                               'message_id': r.message_id,
                               'layer': r.layer, 'role': r.role,
                               'meta': r.meta}
                              for r in val.refs]),
        ))


@register_handler(ConvReplaceWithOp)
class ConvReplaceWithOpHandler:
    @staticmethod
    def handle(val: ConvReplaceWithOp, ctx: OpContext) -> None:
        if not ctx.store or not ctx.wf.conversation_id:
            return
        start_id = val.start_ref.message_id if val.start_ref else None
        end_id = val.end_ref.message_id if val.end_ref else None
        ctx.wf.status = 'waiting'
        ctx.new_events.append(Event(
            event_id=0, execution_id=ctx.execution_id,
            workflow_id=ctx.workflow_id, category='outbox',
            payload=ev.ConvReplaceWithRequest(
                conversation_id=ctx.wf.conversation_id,
                new_messages=val.new_messages,
                start_message_id=start_id,
                end_message_id=end_id),
        ))
