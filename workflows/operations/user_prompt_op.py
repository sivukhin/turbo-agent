import uuid
from workflows.operations.base import OpContext, register_handler
from workflows.ops import UserPromptOp, AiResponseOp, Event
import workflows.events as ev


@register_handler(UserPromptOp)
class UserPromptOpHandler:
    @staticmethod
    def handle(val: UserPromptOp, ctx: OpContext) -> None:
        request_id = uuid.uuid4().hex[:12]
        ctx.wf.status = 'waiting'
        ctx.new_events.append(Event(
            event_id=0, execution_id=ctx.execution_id,
            workflow_id=ctx.workflow_id, category='outbox',
            payload=ev.UserPromptRequest(request_id=request_id),
        ))


@register_handler(AiResponseOp)
class AiResponseOpHandler:
    @staticmethod
    def handle(val: AiResponseOp, ctx: OpContext) -> None:
        ctx.wf.send_val = None
        ctx.new_events.append(Event(
            event_id=0, execution_id=ctx.execution_id,
            workflow_id=ctx.workflow_id, category='outbox',
            payload=ev.AiResponseEvent(text=val.text),
        ))
