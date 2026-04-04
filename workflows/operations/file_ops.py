from workflows.operations.base import OpContext, op_handler
from workflows.ops import ReadFileOp, WriteFileOp, Event
import workflows.events as ev


@op_handler(ReadFileOp)
class ReadFileOpHandler:
    @staticmethod
    def handle(val: ReadFileOp, ctx: OpContext) -> None:
        if not ctx.wf.workdir:
            raise RuntimeError(f'Workflow {ctx.workflow_id} has no workdir configured')
        ctx.wf.status = 'waiting'
        ctx.new_events.append(Event(
            event_id=0, execution_id=ctx.execution_id,
            workflow_id=ctx.workflow_id, category='outbox',
            payload=ev.FileReadRequest(path=val.path),
        ))


@op_handler(WriteFileOp)
class WriteFileOpHandler:
    @staticmethod
    def handle(val: WriteFileOp, ctx: OpContext) -> None:
        if not ctx.wf.workdir:
            raise RuntimeError(f'Workflow {ctx.workflow_id} has no workdir configured')
        ctx.wf.status = 'waiting'
        ctx.new_events.append(Event(
            event_id=0, execution_id=ctx.execution_id,
            workflow_id=ctx.workflow_id, category='outbox',
            payload=ev.FileWriteRequest(path=val.path, content=val.content),
        ))
