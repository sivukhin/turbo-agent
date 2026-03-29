"""Global event handlers that process outbox request events,
execute the work, and write results back to inbox.

Each handler.handle(event, store, state) can:
- Emit new events (returned as list)
- Modify workflow state directly (set send_val, status, etc.)
"""

from pathlib import Path
from workflows.ops import Event
from workflows.isolation.host import HostIsolation
from workflows.isolation.docker import DockerIsolation
from workflows.isolation.base import ShellResult as IsoShellResult
import workflows.events as ev


def _make_isolation(iso_type, iso_config):
    if iso_type == 'docker':
        return DockerIsolation(**(iso_config or {}))
    return HostIsolation()


def _get_wf(state, workflow_id):
    return state.workflows.get(workflow_id)


def _resolve_wf(state, workflow_id, result):
    """Set workflow back to running with a result."""
    wf = state.workflows.get(workflow_id)
    if wf and wf.status == 'waiting':
        wf.status = 'running'
        wf.send_val = result
        # Remove handler if present
        state.handlers.pop(workflow_id, None)


class ShellRequestHandler:
    @staticmethod
    def event_type():
        return 'shell_request'

    @staticmethod
    def handle(event, store, state):
        payload = event.payload
        wf = _get_wf(state, event.workflow_id)
        if not wf or not wf.workdir:
            return []
        workdir = Path(wf.workdir)
        isolation = _make_isolation(payload.isolation_type, payload.isolation_config)
        result = isolation.run_shell(workdir, payload.command)
        from workflows.isolation.base import scan_git_branches
        wf.branches = scan_git_branches(workdir)
        _resolve_wf(state, event.workflow_id, result)
        return [Event(
            event_id=0, execution_id=event.execution_id,
            workflow_id=event.workflow_id, category='inbox',
            payload=ev.ShellResult(
                command=payload.command, exit_code=result.exit_code,
                stdout=result.stdout, stderr=result.stderr,
            ),
        )]


class FileReadRequestHandler:
    @staticmethod
    def event_type():
        return 'file_read_request'

    @staticmethod
    def handle(event, store, state):
        payload = event.payload
        wf = _get_wf(state, event.workflow_id)
        if not wf or not wf.workdir:
            return []
        content = (Path(wf.workdir) / payload.path).read_text()
        _resolve_wf(state, event.workflow_id, content)
        return [Event(
            event_id=0, execution_id=event.execution_id,
            workflow_id=event.workflow_id, category='inbox',
            payload=ev.FileReadResult(path=payload.path, content=content),
        )]


class FileWriteRequestHandler:
    @staticmethod
    def event_type():
        return 'file_write_request'

    @staticmethod
    def handle(event, store, state):
        payload = event.payload
        wf = _get_wf(state, event.workflow_id)
        if not wf or not wf.workdir:
            return []
        file_path = Path(wf.workdir) / payload.path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(payload.content)
        _resolve_wf(state, event.workflow_id, None)
        return [Event(
            event_id=0, execution_id=event.execution_id,
            workflow_id=event.workflow_id, category='inbox',
            payload=ev.FileWriteResult(path=payload.path, size=len(payload.content)),
        )]


class LlmRequestHandler:
    @staticmethod
    def event_type():
        return 'llm_request'

    @staticmethod
    def handle(event, store, state):
        raise NotImplementedError(
            'LlmRequestHandler must be created with a provider. '
            'Use LlmRequestHandler.with_provider(provider).'
        )

    @classmethod
    def with_provider(cls, provider):
        class BoundLlmHandler:
            @staticmethod
            def event_type():
                return 'llm_request'

            @staticmethod
            def handle(event, store, state):
                payload = event.payload
                result = provider.complete(
                    messages=payload.messages, model=payload.model,
                    max_tokens=payload.max_tokens, temperature=payload.temperature,
                    system=payload.system, tools=payload.tools,
                )
                _resolve_wf(state, event.workflow_id, result)
                return [Event(
                    event_id=0, execution_id=event.execution_id,
                    workflow_id=event.workflow_id, category='inbox',
                    payload=ev.LlmResponse(
                        content=result.content, model=result.model,
                        stop_reason=result.stop_reason, usage=result.usage,
                        text=result.text,
                        tool_calls=[{'id': tc.id, 'name': tc.name, 'input': tc.input}
                                    for tc in result.tool_calls] or None,
                        message_id=result.message_id,
                    ),
                )]
        return BoundLlmHandler()


class ConvAppendRequestHandler:
    @staticmethod
    def event_type():
        return 'conv_append_request'

    @staticmethod
    def handle(event, store, state):
        payload = event.payload
        ref = store.conv_append_message(payload.conversation_id, payload.role, payload.content)
        from workflows.conversation import MessageRef
        _resolve_wf(state, event.workflow_id, MessageRef(
            conversation_id=ref.conversation_id,
            message_id=ref.message_id, layer=ref.layer,
        ))
        return [Event(
            event_id=0, execution_id=event.execution_id,
            workflow_id=event.workflow_id, category='inbox',
            payload=ev.ConvAppendResult(
                conversation_id=ref.conversation_id,
                message_id=ref.message_id, layer=ref.layer,
            ),
        )]


class ConvReadRequestHandler:
    @staticmethod
    def event_type():
        return 'conv_read_request'

    @staticmethod
    def handle(event, store, state):
        payload = event.payload
        messages = store.conv_read_messages(
            payload.conversation_id, payload.end_message_id, payload.layer,
        )
        _resolve_wf(state, event.workflow_id, messages)
        return [Event(
            event_id=0, execution_id=event.execution_id,
            workflow_id=event.workflow_id, category='inbox',
            payload=ev.ConvReadResult(
                count=len(messages),
                message_refs=[{'conversation_id': m.ref.conversation_id,
                               'message_id': m.ref.message_id,
                               'layer': m.ref.layer} for m in messages],
            ),
        )]


class ConvSearchRequestHandler:
    @staticmethod
    def event_type():
        return 'conv_search_request'

    @staticmethod
    def handle(event, store, state):
        payload = event.payload
        messages = store.conv_search_messages(payload.conversation_id, payload.pattern)
        _resolve_wf(state, event.workflow_id, messages)
        return [Event(
            event_id=0, execution_id=event.execution_id,
            workflow_id=event.workflow_id, category='inbox',
            payload=ev.ConvSearchResult(
                count=len(messages),
                message_refs=[{'conversation_id': m.ref.conversation_id,
                               'message_id': m.ref.message_id,
                               'layer': m.ref.layer} for m in messages],
            ),
        )]


class ConvGetRequestHandler:
    @staticmethod
    def event_type():
        return 'conv_get_request'

    @staticmethod
    def handle(event, store, state):
        from workflows.conversation import MessageRef
        payload = event.payload
        refs = [MessageRef(**r) for r in payload.message_refs]
        messages = store.conv_get_messages(refs)
        _resolve_wf(state, event.workflow_id, messages)
        return [Event(
            event_id=0, execution_id=event.execution_id,
            workflow_id=event.workflow_id, category='inbox',
            payload=ev.ConvGetResult(count=len(messages)),
        )]


class ConvReplaceWithRequestHandler:
    @staticmethod
    def event_type():
        return 'conv_replace_with_request'

    @staticmethod
    def handle(event, store, state):
        from workflows.conversation import MessageRef
        payload = event.payload
        new_refs = store.conv_replace_with(
            payload.conversation_id, payload.new_messages,
            payload.start_message_id, payload.end_message_id,
        )
        _resolve_wf(state, event.workflow_id, [
            MessageRef(r.conversation_id, r.message_id, r.layer) for r in new_refs
        ])
        return [Event(
            event_id=0, execution_id=event.execution_id,
            workflow_id=event.workflow_id, category='inbox',
            payload=ev.ConvReplaceWithResult(
                conversation_id=payload.conversation_id,
                new_layer=new_refs[0].layer if new_refs else 0,
                new_message_refs=[{'conversation_id': r.conversation_id,
                                   'message_id': r.message_id,
                                   'layer': r.layer} for r in new_refs],
            ),
        )]


DEFAULT_EVENT_HANDLERS = [
    ShellRequestHandler(),
    FileReadRequestHandler(),
    FileWriteRequestHandler(),
    ConvAppendRequestHandler(),
    ConvReadRequestHandler(),
    ConvSearchRequestHandler(),
    ConvGetRequestHandler(),
    ConvReplaceWithRequestHandler(),
]
