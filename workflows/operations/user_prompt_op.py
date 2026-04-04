from workflows.ids import new_id
from workflows.operations.base import OpContext, op_handler
from workflows.ops import UserPromptOp, Event
import workflows.events as ev


@op_handler(UserPromptOp)
def handle_user_prompt(val: UserPromptOp, ctx: OpContext) -> None:
    request_id = new_id()
    ctx.wf.status = 'waiting'
    ctx.new_events.append(Event(
        event_id=0,
        execution_id=ctx.execution_id,
        workflow_id=ctx.workflow_id,
        category='outbox',
        payload=ev.UserPromptRequest(request_id=request_id, meta=val.meta),
    ))
