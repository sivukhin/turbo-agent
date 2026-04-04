from workflows.operations.base import OpContext, op_handler
from workflows.ops import SleepOp, HandlerState, Event
import workflows.events as ev


@op_handler(SleepOp)
def handle_sleep(val: SleepOp, ctx: OpContext) -> None:
    handler_cls = ctx.workflow_event_handlers['sleep']
    wake_at = ctx.now + val.seconds
    ctx.wf.status = 'waiting'
    ctx.state.handlers[ctx.workflow_id] = HandlerState(
        handler_type='sleep',
        state=handler_cls.initial_state(wake_at),
    )
    ctx.new_events.append(Event(
        event_id=0,
        execution_id=ctx.execution_id,
        workflow_id=ctx.workflow_id,
        category='outbox',
        payload=ev.SleepStarted(seconds=val.seconds, wake_at=wake_at),
    ))
