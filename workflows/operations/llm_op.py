from workflows.operations.base import OpContext, op_handler
from workflows.ops import AiOp, Event
import workflows.events as ev


@op_handler(AiOp)
class AiOpHandler:
    @staticmethod
    def handle(val: AiOp, ctx: OpContext) -> None:
        using_conversation = (val.conversation is not None
                              and ctx.store and ctx.wf.conversation_id)

        if using_conversation:
            refs = ctx.store.conv_list_messages(ctx.wf.conversation_id)
            conv_msgs = ctx.store.conv_read_messages(refs)
            messages = [{'role': m.role, 'content': m.content} for m in conv_msgs]
        else:
            messages = val.messages

        if not messages:
            raise RuntimeError('AiOp requires messages or conversation')

        # Extract system messages — they go to the system parameter, not in messages
        system = val.system
        system_parts = [m['content'] for m in messages if m['role'] == 'system']
        messages = [m for m in messages if m['role'] != 'system']
        if system_parts:
            combined = '\n'.join(system_parts)
            system = f'{system}\n{combined}' if system else combined

        # Build event payload — lightweight when using conversation
        if using_conversation:
            conv_ref = ctx.store.conv_resolve_ref(ctx.wf.conversation_id)
            payload = ev.LlmRequest(
                model=val.model, max_tokens=val.max_tokens,
                temperature=val.temperature, system=system, tools=val.tools,
                conversation_ref={
                    'conversation_id': conv_ref.conversation_id,
                    'message_id': conv_ref.message_id,
                    'layer': conv_ref.layer,
                },
                message_count=len(messages),
            )
        else:
            payload = ev.LlmRequest(
                model=val.model, max_tokens=val.max_tokens,
                temperature=val.temperature, system=system, tools=val.tools,
                messages=messages,
            )

        ctx.wf.status = 'waiting'
        ctx.new_events.append(Event(
            event_id=0, execution_id=ctx.execution_id,
            workflow_id=ctx.workflow_id, category='outbox',
            payload=payload,
        ))
