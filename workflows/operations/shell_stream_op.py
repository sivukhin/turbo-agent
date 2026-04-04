from workflows.ids import new_id
from workflows.operations.base import OpContext, op_handler
from workflows.operations.shell_op import _serialize_isolation, _private_envs
from workflows.ops import ShellStreamStartOp, ShellStreamNextOp, StreamDef, Event
import workflows.events as ev

# In-memory private env for streams: stream_id → private_env dict
_stream_private_envs: dict[str, dict] = {}


@op_handler(ShellStreamStartOp)
def handle_shell_stream_start(val: ShellStreamStartOp, ctx: OpContext) -> None:
    if not ctx.wf.workdir:
        raise RuntimeError(f'Workflow {ctx.workflow_id} has no workdir configured')
    iso_type, iso_config = _serialize_isolation(val.isolation)
    stream_id = new_id()

    ctx.state.streams[stream_id] = StreamDef(
        stream_id=stream_id,
        command=val.command,
        isolation_type=iso_type,
        isolation_config=iso_config,
        public_env=val.public_env,
        workflow_id=ctx.workflow_id,
        meta=val.meta,
    )

    if val.private_env:
        _stream_private_envs[stream_id] = val.private_env

    ctx.wf.status = 'waiting'
    ctx.new_events.append(Event(
        event_id=0,
        execution_id=ctx.execution_id,
        workflow_id=ctx.workflow_id,
        category='outbox',
        payload=ev.ShellStreamStartRequest(
            stream_id=stream_id,
            command=val.command,
            isolation_type=iso_type,
            isolation_config=iso_config,
            public_env=val.public_env,
            meta=val.meta,
        ),
    ))


@op_handler(ShellStreamNextOp)
def handle_shell_stream_next(val: ShellStreamNextOp, ctx: OpContext) -> None:
    if val.private_env:
        _stream_private_envs[val.stream_id] = val.private_env
    # Inherit meta from the stream's start operation
    stream_def = ctx.state.streams.get(val.stream_id)
    meta = {**(stream_def.meta if stream_def else {}), **val.meta}
    ctx.wf.status = 'waiting'
    ctx.new_events.append(Event(
        event_id=0,
        execution_id=ctx.execution_id,
        workflow_id=ctx.workflow_id,
        category='outbox',
        payload=ev.ShellStreamNextRequest(stream_id=val.stream_id, meta=meta),
    ))
