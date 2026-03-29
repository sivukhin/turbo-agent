from workflows.operations.base import OpContext, register_handler
from workflows.ops import Event
from workflows.conversation import (
    ConvAppendOp, ConvReadOp, ConvSearchOp, ConvGetOp, ConvReplaceWithOp,
)
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
                role=val.role, content=val.content),
        ))


@register_handler(ConvReadOp)
class ConvReadOpHandler:
    @staticmethod
    def handle(val: ConvReadOp, ctx: OpContext) -> None:
        if not ctx.store or not ctx.wf.conversation_id:
            return
        resolved = ctx.store.conv_resolve_ref(ctx.wf.conversation_id)
        ctx.wf.status = 'waiting'
        ctx.new_events.append(Event(
            event_id=0, execution_id=ctx.execution_id,
            workflow_id=ctx.workflow_id, category='outbox',
            payload=ev.ConvReadRequest(
                conversation_id=resolved.conversation_id,
                end_message_id=resolved.message_id,
                layer=resolved.layer),
        ))


@register_handler(ConvSearchOp)
class ConvSearchOpHandler:
    @staticmethod
    def handle(val: ConvSearchOp, ctx: OpContext) -> None:
        if not ctx.store or not ctx.wf.conversation_id:
            return
        ctx.wf.status = 'waiting'
        ctx.new_events.append(Event(
            event_id=0, execution_id=ctx.execution_id,
            workflow_id=ctx.workflow_id, category='outbox',
            payload=ev.ConvSearchRequest(
                conversation_id=ctx.wf.conversation_id,
                pattern=val.pattern),
        ))


@register_handler(ConvGetOp)
class ConvGetOpHandler:
    @staticmethod
    def handle(val: ConvGetOp, ctx: OpContext) -> None:
        if not ctx.store:
            return
        ctx.wf.status = 'waiting'
        ctx.new_events.append(Event(
            event_id=0, execution_id=ctx.execution_id,
            workflow_id=ctx.workflow_id, category='outbox',
            payload=ev.ConvGetRequest(
                message_refs=[{'conversation_id': r.conversation_id,
                               'message_id': r.message_id,
                               'layer': r.layer} for r in val.refs]),
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
