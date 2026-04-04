from workflows.ids import new_id
from workflows.operations.base import OpContext, op_handler
from workflows.ops import UserPromptOp, AiResponseOp, Event
import workflows.events as ev


@op_handler(UserPromptOp)
class UserPromptOpHandler:
    @staticmethod
    def handle(val: UserPromptOp, ctx: OpContext) -> None:
        request_id = new_id()
        ctx.wf.status = 'waiting'
        ctx.new_events.append(Event(
            event_id=0, 
            execution_id=ctx.execution_id,
            workflow_id=ctx.workflow_id, 
            category='outbox',
            payload=ev.UserPromptRequest(request_id=request_id),
        ))


@op_handler(AiResponseOp)
class AiResponseOpHandler:
    @staticmethod
    def handle(val: AiResponseOp, ctx: OpContext) -> None:
        ctx.wf.send_val = None
        ctx.new_events.append(Event(
            event_id=0, 
            execution_id=ctx.execution_id,
            workflow_id=ctx.workflow_id, 
            category='outbox',
            payload=ev.AiResponseEvent(text=val.text),
        ))
