from workflows.operations.base import OpContext, op_handler
from workflows.ops import WaitOp, HandlerState, Event
import workflows.events as ev


@op_handler(WaitOp)
def handle_wait(val: WaitOp, ctx: OpContext) -> None:
    handler_cls = ctx.workflow_event_handlers[val.mode]
    ctx.wf.status = 'waiting'
    ctx.state.handlers[ctx.workflow_id] = HandlerState(
        handler_type=val.mode,
        state=handler_cls.initial_state(val.deps),
    )
    ctx.new_events.append(Event(
        event_id=0,
        execution_id=ctx.execution_id,
        workflow_id=ctx.workflow_id,
        category='outbox',
        payload=ev.WaitStarted(mode=val.mode, deps=val.deps),
    ))
