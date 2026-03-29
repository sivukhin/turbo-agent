import pickle
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from workflows.decorator import _TickContext, _current_ctx
from workflows.handlers import HANDLER_REGISTRY
from workflows.events import payload_type_name
from workflows.isolation.base import StorageConfig, setup_child_workspace, scan_git_branches
from workflows.conversation import (
    ConvAppendOp, ConvReadOp, ConvSearchOp, ConvGetOp, ConvReplaceWithOp,
    Latest, _sortable_uuid,
)
import workflows.events as ev


def _uuid():
    return uuid.uuid4().hex[:12]


@dataclass
class WorkflowHandle:
    """Serializable reference to a child workflow instance."""
    id: str
    workflow_name: str
    args: list
    storage: object = None  # StorageConfig, None means 'same'

    def __repr__(self):
        return f'<{self.workflow_name}#{self.id}>'


# ---- operations (yielded by workflows) ----

@dataclass
class WaitOp:
    deps: list[str]
    mode: str  # 'wait' | 'wait_all' | 'wait_any'


@dataclass
class SleepOp:
    seconds: float


def wait(handle):
    return WaitOp(deps=[handle.id], mode='wait')


def wait_all(handles):
    return WaitOp(deps=[h.id for h in handles], mode='wait_all')


def wait_any(handles):
    return WaitOp(deps=[h.id for h in handles], mode='wait_any')


def sleep(seconds):
    """Pause the workflow for `seconds`. Resumes when engine time >= wake_at."""
    return SleepOp(seconds=seconds)


@dataclass
class ShellOp:
    command: str
    isolation: object = None  # HostIsolation or DockerIsolation


@dataclass
class ReadFileOp:
    path: str


@dataclass
class WriteFileOp:
    path: str
    content: str


def shell(command, isolation=None):
    """Run a shell command in the workflow's workspace.
    isolation: HostIsolation or DockerIsolation instance."""
    return ShellOp(command=command, isolation=isolation)


def read_file(path):
    """Read a file from the workflow's workspace."""
    return ReadFileOp(path=path)


def write_file(path, content):
    """Write a file to the workflow's workspace."""
    return WriteFileOp(path=path, content=content)


@dataclass
class LlmOp:
    """Yield this to make an LLM call."""
    messages: list | None = None
    conversation: object = None  # ConversationRef or Latest
    provider: object = None      # LlmProvider instance
    model: str = 'claude-sonnet-4-20250514'
    max_tokens: int | None = None
    temperature: float = 0.0
    system: str | None = None
    tools: list | None = None


def llm(messages=None, *, conversation=None, provider=None,
        model='claude-sonnet-4-20250514', max_tokens=None,
        temperature=0.0, system=None, tools=None):
    """Make an LLM call. Returns LlmResult with .text, .tool_calls, .usage.

        # With explicit messages:
        response = yield llm(messages=[{"role": "user", "content": "Hello"}], provider=claude)

        # With conversation (reads from workflow's conversation):
        response = yield llm(conversation=Latest, provider=claude)
    """
    return LlmOp(
        messages=messages, conversation=conversation, provider=provider,
        model=model, max_tokens=max_tokens, temperature=temperature,
        system=system, tools=tools,
    )


# ---- state ----

@dataclass
class WorkflowState:
    name: str
    args: list
    parent_workflow_id: str | None = None
    workdir: str | None = None           # absolute path to workspace directory
    branches: dict | None = None         # {relative_git_repo_path: branch_name}
    conversation_id: str | None = None   # conversation attached to this workflow
    checkpoint: dict | None = None
    status: str = 'running'
    result: object = None
    send_val: object = field(default=None, repr=False)


@dataclass
class HandlerState:
    handler_type: str
    state: dict


@dataclass
class Event:
    event_id: int
    execution_id: str
    workflow_id: str | None
    category: str   # 'inbox' | 'outbox'
    payload: object  # one of the dataclasses from workflows.events

    @property
    def type(self) -> str:
        return payload_type_name(self.payload)


@dataclass
class ExecutionState:
    workflows: dict[str, WorkflowState]
    handlers: dict[str, HandlerState]  # key = waiting workflow_id
    root_workflow_id: str
    source_file: str | None = None
    finished: bool = False


# ---- engine ----

class Engine:
    """Event-sourced workflow engine.

    step() ticks all running workflows, writes events, then processes
    any new inbox events (workflow_finished) which may unblock handlers.

    Pass `now` to control time (for sleep support and testing).
    """

    def __init__(self, registry: dict):
        self.registry = registry

    def start(self, store, workflow_name, args, now=None, source_file=None,
              workdir=None) -> str:
        """Start a new execution.

        workdir: optional root workspace directory. If provided, the root workflow
        gets this as its working directory and git branches are scanned.
        """
        now = now if now is not None else time.time()
        execution_id = _uuid()
        root_workflow_id = _uuid()

        wf_state = WorkflowState(name=workflow_name, args=list(args))
        if workdir:
            wf_dir = Path(workdir).resolve() / execution_id / root_workflow_id
            wf_dir.mkdir(parents=True, exist_ok=True)
            wf_state.workdir = str(wf_dir)
            wf_state.branches = scan_git_branches(wf_dir)

        # Create conversation for root workflow
        conv_id = _uuid()
        wf_state.conversation_id = conv_id
        store.create_conversation(conv_id)

        state = ExecutionState(
            workflows={root_workflow_id: wf_state},
            handlers={},
            root_workflow_id=root_workflow_id,
            source_file=source_file,
        )
        store.save_state(execution_id, state)
        self._tick_and_process(store, execution_id, now)
        return execution_id

    def step(self, store, execution_id, now=None):
        now = now if now is not None else time.time()
        self._tick_and_process(store, execution_id, now)

    def _tick_and_process(self, store, execution_id, now):
        """Tick each running workflow once, write events, process inbox."""
        state, last_processed = store.load_state(execution_id)

        # Tick all running workflows exactly once
        new_events = self._handle_tick(state, execution_id, now, store)

        # Try to resolve handlers (sleep timers, etc.)
        self._try_resolve_handlers(state, now)
        self._check_finished(state)
        self._prune_finished(state)

        # Write events and save state in one batch
        store.save_state(execution_id, state, last_processed_event_id=last_processed)
        if new_events:
            store.append_events([
                (e.execution_id, e.workflow_id, e.category, e.payload)
                for e in new_events
            ])

        # Process new inbox events — only resolve handlers, don't re-tick.
        # Unblocked workflows will be ticked on the next step().
        self._process_inbox(store, execution_id, now)

    def _process_inbox(self, store, execution_id, now):
        """Process inbox events: feed to handlers and resolve. No re-ticking."""
        while True:
            state, last_processed = store.load_state(execution_id)
            events = store.read_inbox(execution_id, after_event_id=last_processed)
            if not events:
                break

            for event in events:
                for handler_wf_id, hs in list(state.handlers.items()):
                    handler_cls = HANDLER_REGISTRY[hs.handler_type]
                    hs.state = handler_cls.on_event(
                        event.type, event.workflow_id, event.payload, hs.state,
                    )

            self._try_resolve_handlers(state, now)
            self._check_finished(state)
            self._prune_finished(state)

            last_event_id = events[-1].event_id
            store.save_state(execution_id, state, last_processed_event_id=last_event_id)

    def _try_resolve_handlers(self, state, now):
        for handler_wf_id in list(state.handlers):
            hs = state.handlers[handler_wf_id]
            handler_cls = HANDLER_REGISTRY[hs.handler_type]
            resolved, result = handler_cls.try_resolve(hs.state, now)
            if resolved:
                wf = state.workflows[handler_wf_id]
                wf.status = 'running'
                wf.send_val = result
                del state.handlers[handler_wf_id]

    def _check_finished(self, state):
        root = state.workflows.get(state.root_workflow_id)
        if root and root.status == 'finished':
            state.finished = True

    def _prune_finished(self, state):
        """Remove finished workflows from state. Keep root (for final result)."""
        to_remove = [
            wf_id for wf_id, wf in state.workflows.items()
            if wf.status == 'finished' and wf_id != state.root_workflow_id
        ]
        for wf_id in to_remove:
            del state.workflows[wf_id]

    def _handle_tick(self, state, execution_id, now, store=None):
        new_events = []

        for workflow_id, wf in list(state.workflows.items()):
            if wf.status != 'running':
                continue

            send_val = wf.send_val
            wf.send_val = None

            ctx = _TickContext(alloc_id=lambda: _uuid())
            token = _current_ctx.set(ctx)

            try:
                if wf.checkpoint is None:
                    wf_func = self.registry[wf.name]
                    g = wf_func.create(*wf.args)
                    val = next(g)
                else:
                    wf_func = self.registry[wf.name]
                    g = wf_func.resume(wf.checkpoint)
                    val = g.send(send_val)
            except StopIteration as e:
                wf.status = 'finished'
                wf.result = e.value
                wf.checkpoint = None
                self._register_children(state, ctx, new_events, execution_id, workflow_id, store)
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='inbox',
                    payload=ev.WorkflowFinished(result=e.value),
                ))
                continue
            finally:
                _current_ctx.reset(token)

            self._register_children(state, ctx, new_events, execution_id, workflow_id, store)
            wf.checkpoint = pickle.loads(g.save())

            if isinstance(val, WaitOp):
                handler_cls = HANDLER_REGISTRY[val.mode]
                wf.status = 'waiting'
                hs = HandlerState(
                    handler_type=val.mode,
                    state=handler_cls.initial_state(val.deps),
                )
                # Catch up: scan all inbox events (past + current batch)
                # for workflow_finished events matching our deps
                if store:
                    for past_event in store.read_inbox(execution_id):
                        if (isinstance(past_event.payload, ev.WorkflowFinished)
                                and past_event.workflow_id in val.deps):
                            hs.state = handler_cls.on_event(
                                'workflow_finished', past_event.workflow_id,
                                past_event.payload, hs.state,
                            )
                for finished_event in new_events:
                    if (finished_event.category == 'inbox'
                            and isinstance(finished_event.payload, ev.WorkflowFinished)
                            and finished_event.workflow_id in val.deps):
                        hs.state = handler_cls.on_event(
                            'workflow_finished', finished_event.workflow_id,
                            finished_event.payload, hs.state,
                        )
                state.handlers[workflow_id] = hs
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='outbox',
                    payload=ev.WaitStarted(mode=val.mode, deps=val.deps),
                ))
            elif isinstance(val, SleepOp):
                handler_cls = HANDLER_REGISTRY['sleep']
                wake_at = now + val.seconds
                wf.status = 'waiting'
                state.handlers[workflow_id] = HandlerState(
                    handler_type='sleep',
                    state=handler_cls.initial_state(wake_at),
                )
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='outbox',
                    payload=ev.SleepStarted(seconds=val.seconds, wake_at=wake_at),
                ))
            elif isinstance(val, ShellOp):
                if not wf.workdir:
                    raise RuntimeError(f'Workflow {workflow_id} has no workdir configured')
                isolation = val.isolation
                if isolation is None:
                    raise RuntimeError('ShellOp requires an isolation instance')
                workdir = Path(wf.workdir)
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='outbox',
                    payload=ev.ShellRequest(command=val.command),
                ))
                result = isolation.run_shell(workdir, val.command)
                wf.branches = scan_git_branches(workdir)
                wf.send_val = result
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='inbox',
                    payload=ev.ShellResult(
                        command=val.command, exit_code=result.exit_code,
                        stdout=result.stdout, stderr=result.stderr),
                ))
            elif isinstance(val, ReadFileOp):
                if not wf.workdir:
                    raise RuntimeError(f'Workflow {workflow_id} has no workdir configured')
                file_path = Path(wf.workdir) / val.path
                content = file_path.read_text()
                wf.send_val = content
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='outbox',
                    payload=ev.FileReadRequest(path=val.path),
                ))
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='inbox',
                    payload=ev.FileReadResult(path=val.path, content=content),
                ))
            elif isinstance(val, WriteFileOp):
                if not wf.workdir:
                    raise RuntimeError(f'Workflow {workflow_id} has no workdir configured')
                file_path = Path(wf.workdir) / val.path
                file_path.parent.mkdir(parents=True, exist_ok=True)
                file_path.write_text(val.content)
                wf.send_val = None
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='outbox',
                    payload=ev.FileWriteRequest(path=val.path, content=val.content),
                ))
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='inbox',
                    payload=ev.FileWriteResult(path=val.path, size=len(val.content)),
                ))
            elif isinstance(val, LlmOp):
                provider = val.provider
                if provider is None:
                    raise RuntimeError('LlmOp requires a provider instance')
                # Resolve messages: conversation wins over explicit messages
                messages = val.messages
                if val.conversation is not None and store and wf.conversation_id:
                    conv_id = wf.conversation_id
                    conv_msgs = store.conv_read_messages(conv_id)
                    messages = [{'role': m.role, 'content': m.content} for m in conv_msgs]
                if not messages:
                    raise RuntimeError('LlmOp requires messages or conversation')
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='outbox',
                    payload=ev.LlmRequest(
                        messages=messages, model=val.model,
                        max_tokens=val.max_tokens, temperature=val.temperature,
                        system=val.system, tools=val.tools,
                    ),
                ))
                result = provider.complete(
                    messages=messages, model=val.model,
                    max_tokens=val.max_tokens, temperature=val.temperature,
                    system=val.system, tools=val.tools,
                )
                wf.send_val = result
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='inbox',
                    payload=ev.LlmResponse(
                        content=result.content, model=result.model,
                        stop_reason=result.stop_reason, usage=result.usage,
                        text=result.text,
                        tool_calls=[{'id': tc.id, 'name': tc.name, 'input': tc.input}
                                    for tc in result.tool_calls] or None,
                        message_id=result.message_id,
                    ),
                ))
            elif isinstance(val, ConvAppendOp) and store and wf.conversation_id:
                ref = store.conv_append_message(wf.conversation_id, val.role, val.content)
                wf.send_val = ref
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='outbox',
                    payload=ev.ConvAppendRequest(
                        conversation_id=wf.conversation_id, role=val.role, content=val.content),
                ))
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='inbox',
                    payload=ev.ConvAppendResult(
                        conversation_id=ref.conversation_id,
                        message_id=ref.message_id, layer=ref.layer),
                ))
            elif isinstance(val, ConvReadOp) and store and wf.conversation_id:
                messages = store.conv_read_messages(wf.conversation_id)
                wf.send_val = messages
                resolved = store.conv_resolve_ref(wf.conversation_id)
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='outbox',
                    payload=ev.ConvReadRequest(
                        conversation_id=resolved.conversation_id,
                        end_message_id=resolved.message_id,
                        layer=resolved.layer),
                ))
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='inbox',
                    payload=ev.ConvReadResult(
                        count=len(messages),
                        message_refs=[{'conversation_id': m.ref.conversation_id,
                                       'message_id': m.ref.message_id,
                                       'layer': m.ref.layer} for m in messages]),
                ))
            elif isinstance(val, ConvSearchOp) and store and wf.conversation_id:
                messages = store.conv_search_messages(wf.conversation_id, val.pattern)
                wf.send_val = messages
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='outbox',
                    payload=ev.ConvSearchRequest(
                        conversation_id=wf.conversation_id, pattern=val.pattern),
                ))
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='inbox',
                    payload=ev.ConvSearchResult(
                        count=len(messages),
                        message_refs=[{'conversation_id': m.ref.conversation_id,
                                       'message_id': m.ref.message_id,
                                       'layer': m.ref.layer} for m in messages]),
                ))
            elif isinstance(val, ConvGetOp) and store:
                messages = store.conv_get_messages(val.refs)
                wf.send_val = messages
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='outbox',
                    payload=ev.ConvGetRequest(
                        message_refs=[{'conversation_id': r.conversation_id,
                                       'message_id': r.message_id,
                                       'layer': r.layer} for r in val.refs]),
                ))
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='inbox',
                    payload=ev.ConvGetResult(count=len(messages)),
                ))
            elif isinstance(val, ConvReplaceWithOp) and store and wf.conversation_id:
                start_id = val.start_ref.message_id if val.start_ref else None
                end_id = val.end_ref.message_id if val.end_ref else None
                new_refs = store.conv_replace_with(
                    wf.conversation_id, val.new_messages, start_id, end_id,
                )
                wf.send_val = new_refs
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='outbox',
                    payload=ev.ConvReplaceWithRequest(
                        conversation_id=wf.conversation_id,
                        new_messages=val.new_messages,
                        start_message_id=start_id,
                        end_message_id=end_id),
                ))
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='inbox',
                    payload=ev.ConvReplaceWithResult(
                        conversation_id=wf.conversation_id,
                        new_layer=new_refs[0].layer if new_refs else 0,
                        new_message_refs=[{'conversation_id': r.conversation_id,
                                           'message_id': r.message_id,
                                           'layer': r.layer} for r in new_refs]),
                ))
            else:
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='outbox',
                    payload=ev.WorkflowYielded(value=val),
                ))

        return new_events

    def _register_children(self, state, ctx, new_events, execution_id, parent_workflow_id, store=None):
        for handle in ctx.new_children:
            self._register_child(state, handle, new_events, execution_id, parent_workflow_id, store)

    def _register_child(self, state, handle, new_events, execution_id, parent_workflow_id, store=None):
        parent_wf = state.workflows.get(parent_workflow_id) if parent_workflow_id else None

        config = handle.storage or StorageConfig(mode='same')
        child_wf = WorkflowState(
            name=handle.workflow_name,
            args=list(handle.args),
            parent_workflow_id=parent_workflow_id,
        )

        if parent_wf and parent_wf.workdir:
            parent_dir = Path(parent_wf.workdir)
            child_dir = parent_dir.parent / handle.id

            child_workdir, child_branches = setup_child_workspace(
                parent_dir, child_dir,
                parent_wf.branches, config,
            )
            child_wf.workdir = str(child_workdir)
            child_wf.branches = child_branches

        # Fork conversation from parent
        if store and parent_wf and parent_wf.conversation_id:
            child_conv_id = _uuid()
            parent_ref = store.conv_resolve_ref(parent_wf.conversation_id)
            store.create_conversation(
                child_conv_id,
                parent_conversation_id=parent_ref.conversation_id,
                parent_message_id=parent_ref.message_id,
                parent_layer=parent_ref.layer,
            )
            child_wf.conversation_id = child_conv_id

        state.workflows[handle.id] = child_wf

        new_events.append(Event(
            event_id=0, execution_id=execution_id,
            workflow_id=handle.id, category='outbox',
            payload=ev.WorkflowSpawned(
                child_workflow_id=handle.id,
                name=handle.workflow_name,
                args=list(handle.args),
                parent_workflow_id=parent_workflow_id,
                storage_mode=config.mode,
            ),
        ))
