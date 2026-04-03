from workflows.operations.base import OpContext, register_handler
from workflows.ops import ShellOp, Event
from workflows.isolation.host import HostIsolation
from workflows.isolation.docker import DockerIsolation
import workflows.events as ev

# In-memory private env store: workflow_id → private_env dict
# Not persisted — only lives for the duration of the process
_private_envs: dict[str, dict] = {}


def _serialize_isolation(isolation):
    if isinstance(isolation, DockerIsolation):
        return 'docker', {'image': isolation.image, 'network': isolation.network}
    return 'host', None


@register_handler(ShellOp)
class ShellOpHandler:
    @staticmethod
    def handle(val: ShellOp, ctx: OpContext) -> None:
        if not ctx.wf.workdir:
            raise RuntimeError(f'Workflow {ctx.workflow_id} has no workdir configured')
        if val.isolation is None:
            raise RuntimeError('ShellOp requires an isolation instance')
        iso_type, iso_config = _serialize_isolation(val.isolation)
        if val.private_env:
            _private_envs[ctx.workflow_id] = val.private_env
        ctx.wf.status = 'waiting'
        ctx.new_events.append(Event(
            event_id=0, execution_id=ctx.execution_id,
            workflow_id=ctx.workflow_id, category='outbox',
            payload=ev.ShellRequest(
                command=val.command,
                isolation_type=iso_type,
                isolation_config=iso_config,
                public_env=val.public_env,
            ),
        ))
