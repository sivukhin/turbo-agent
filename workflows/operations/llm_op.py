from workflows.operations.base import OpContext, register_handler
from workflows.ops import LlmOp, Event
import workflows.events as ev


@register_handler(LlmOp)
class LlmOpHandler:
    @staticmethod
    def handle(val: LlmOp, ctx: OpContext) -> None:
        messages = val.messages
        if val.conversation is not None and ctx.store and ctx.wf.conversation_id:
            conv_msgs = ctx.store.conv_read_messages(ctx.wf.conversation_id)
            messages = [{'role': m.role, 'content': m.content} for m in conv_msgs]
        if not messages:
            raise RuntimeError('LlmOp requires messages or conversation')
        ctx.wf.status = 'waiting'
        ctx.new_events.append(Event(
            event_id=0, execution_id=ctx.execution_id,
            workflow_id=ctx.workflow_id, category='outbox',
            payload=ev.LlmRequest(
                messages=messages, model=val.model,
                max_tokens=val.max_tokens, temperature=val.temperature,
                system=val.system, tools=val.tools,
            ),
        ))
