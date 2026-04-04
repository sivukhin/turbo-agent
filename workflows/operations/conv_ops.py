from workflows.operations.base import OpContext, op_handler
from workflows.ops import Event
from workflows.conversation import ConvAppendOp, ConvListOp, ConvReadOp, ConvReplaceWithOp
import workflows.events as ev


@op_handler(ConvAppendOp)
class ConvAppendOpHandler:
    @staticmethod
    def handle(val: ConvAppendOp, ctx: OpContext) -> None:
        if not ctx.store or not ctx.wf.conversation_id:
            return
        ctx.wf.status = 'waiting'
        ctx.new_events.append(Event(
            event_id=0, 
            execution_id=ctx.execution_id,
            workflow_id=ctx.workflow_id, 
            category='outbox',
            payload=ev.ConvAppendRequest(
                conversation_id=ctx.wf.conversation_id,
                role=val.role, 
                content=val.content, 
                meta=val.meta
            ),
        ))


@op_handler(ConvListOp)
class ConvListOpHandler:
    @staticmethod
    def handle(val: ConvListOp, ctx: OpContext) -> None:
        if not ctx.store or not ctx.wf.conversation_id:
            return
        ref = val.conversation if val.conversation else ctx.store.conv_resolve_ref(ctx.wf.conversation_id)
        ctx.wf.status = 'waiting'
        ctx.new_events.append(Event(
            event_id=0, 
            execution_id=ctx.execution_id,
            workflow_id=ctx.workflow_id, 
            category='outbox',
            payload=ev.ConvListRequest(
                conversation_id=ref.conversation_id,
                end_message_id=ref.message_id,
                layer=ref.layer,
                start_message_id=val.start_message_id,
                role_filter=val.role_filter,
                pattern=val.pattern
            ),
        ))


@op_handler(ConvReadOp)
class ConvReadOpHandler:
    @staticmethod
    def handle(val: ConvReadOp, ctx: OpContext) -> None:
        if not ctx.store:
            return
        ctx.wf.status = 'waiting'
        ctx.new_events.append(Event(
            event_id=0, 
            execution_id=ctx.execution_id,
            workflow_id=ctx.workflow_id, 
            category='outbox',
            payload=ev.ConvReadRequest(message_refs=val.refs),
        ))


@op_handler(ConvReplaceWithOp)
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
                end_message_id=end_id
            ),
        ))
