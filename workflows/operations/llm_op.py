from workflows.operations.base import OpContext, op_handler
from workflows.ops import AiOp, Event
import workflows.events as ev


@op_handler(AiOp)
class AiOpHandler:
    @staticmethod
    def handle(val: AiOp, ctx: OpContext) -> None:
        if val.conversation is not None and ctx.store and ctx.wf.conversation_id:
            conv_ref = ctx.store.conv_resolve_ref(ctx.wf.conversation_id)
            payload = ev.LlmRequest(
                model=val.model, 
                max_tokens=val.max_tokens,
                temperature=val.temperature, 
                system=val.system, 
                tools=val.tools,
                conversation_ref=conv_ref,
            )
        elif val.messages:
            payload = ev.LlmRequest(
                model=val.model, 
                max_tokens=val.max_tokens,
                temperature=val.temperature, 
                system=val.system, 
                tools=val.tools,
                messages=val.messages,
            )
        else:
            raise RuntimeError('AiOp requires messages or conversation')

        ctx.wf.status = 'waiting'
        ctx.new_events.append(Event(
            event_id=0, 
            execution_id=ctx.execution_id,
            workflow_id=ctx.workflow_id, 
            category='outbox',
            payload=payload,
        ))
