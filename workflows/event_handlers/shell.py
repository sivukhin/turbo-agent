from pathlib import Path
from workflows.isolation.host import HostIsolation
from workflows.isolation.docker import DockerIsolation
from workflows.isolation.base import scan_git_branches
from workflows.event_handlers.base import resolve_wf, make_inbox_event, register_event_handler
import workflows.events as ev


def _make_isolation(iso_type, iso_config):
    if iso_type == 'docker':
        return DockerIsolation(**(iso_config or {}))
    return HostIsolation()


@register_event_handler(ev.ShellRequest)
class ShellRequestHandler:
    def handle(self, event, store, state):
        payload = event.payload
        wf = state.workflows.get(event.workflow_id)
        if not wf or not wf.workdir:
            return []
        workdir = Path(wf.workdir)
        isolation = _make_isolation(payload.isolation_type, payload.isolation_config)
        result = isolation.run_shell(workdir, payload.command)
        wf.branches = scan_git_branches(workdir)
        resolve_wf(state, event.workflow_id, result)
        return [make_inbox_event(event, ev.ShellResult(
            command=payload.command, exit_code=result.exit_code,
            stdout=result.stdout, stderr=result.stderr,
        ))]
