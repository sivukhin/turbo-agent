from workflows.operations.base import OpContext, register_handler
from workflows.ops import WaitOp, HandlerState, Event
import workflows.events as ev


@register_handler(WaitOp)
class WaitOpHandler:
    @staticmethod
    def handle(val: WaitOp, ctx: OpContext) -> None:
        handler_cls = ctx.workflow_event_handlers[val.mode]
        ctx.wf.status = 'waiting'
        hs = HandlerState(
            handler_type=val.mode,
            state=handler_cls.initial_state(val.deps),
        )
        # Catch up: scan past inbox events for already-finished deps
        if ctx.store:
            for past_event in ctx.store.read_inbox(ctx.execution_id):
                if (isinstance(past_event.payload, ev.WorkflowFinished)
                        and past_event.workflow_id in val.deps):
                    hs.state = handler_cls.on_event(
                        'workflow_finished', past_event.workflow_id,
                        past_event.payload, hs.state,
                    )
        # Catch up: scan current tick batch
        for finished_event in ctx.new_events:
            if (finished_event.category == 'inbox'
                    and isinstance(finished_event.payload, ev.WorkflowFinished)
                    and finished_event.workflow_id in val.deps):
                hs.state = handler_cls.on_event(
                    'workflow_finished', finished_event.workflow_id,
                    finished_event.payload, hs.state,
                )
        ctx.state.handlers[ctx.workflow_id] = hs
        ctx.new_events.append(Event(
            event_id=0, execution_id=ctx.execution_id,
            workflow_id=ctx.workflow_id, category='outbox',
            payload=ev.WaitStarted(mode=val.mode, deps=val.deps),
        ))
