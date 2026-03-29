import pickle
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from workflows.decorator import _TickContext, _current_ctx
from workflows.handlers import HANDLER_REGISTRY
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


# ---- state ----

@dataclass
class WorkflowState:
    name: str
    args: list
    parent_workflow_id: str | None = None
    workdir: str | None = None           # absolute path to workspace directory
    branches: dict | None = None         # {relative_git_repo_path: branch_name}
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
        from workflows.events import payload_type_name
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
            from workflows.isolation.base import scan_git_branches
            wf_dir = Path(workdir).resolve() / execution_id / root_workflow_id
            wf_dir.mkdir(parents=True, exist_ok=True)
            wf_state.workdir = str(wf_dir)
            wf_state.branches = scan_git_branches(wf_dir)

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
        new_events = self._handle_tick(state, execution_id, now)

        # Try to resolve handlers (sleep timers, etc.)
        self._try_resolve_handlers(state, now)
        self._check_finished(state)

        # Write events and save state
        store.save_state(execution_id, state, last_processed_event_id=last_processed)
        for e in new_events:
            store.append_event(e.execution_id, e.workflow_id, e.category, e.payload)

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
        root = state.workflows[state.root_workflow_id]
        if root.status == 'finished':
            state.finished = True

    def _handle_tick(self, state, execution_id, now):
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
                self._register_children(state, ctx, new_events, execution_id, workflow_id)
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='inbox',
                    payload=ev.WorkflowFinished(result=e.value),
                ))
                continue
            finally:
                _current_ctx.reset(token)

            self._register_children(state, ctx, new_events, execution_id, workflow_id)
            wf.checkpoint = pickle.loads(g.save())

            if isinstance(val, WaitOp):
                handler_cls = HANDLER_REGISTRY[val.mode]
                wf.status = 'waiting'
                hs = HandlerState(
                    handler_type=val.mode,
                    state=handler_cls.initial_state(val.deps),
                )
                for dep_id in val.deps:
                    dep_wf = state.workflows.get(dep_id)
                    if dep_wf and dep_wf.status == 'finished':
                        hs.state = handler_cls.on_event(
                            'workflow_finished', dep_id,
                            ev.WorkflowFinished(result=dep_wf.result), hs.state,
                        )
                state.handlers[workflow_id] = hs
            elif isinstance(val, SleepOp):
                handler_cls = HANDLER_REGISTRY['sleep']
                wf.status = 'waiting'
                state.handlers[workflow_id] = HandlerState(
                    handler_type='sleep',
                    state=handler_cls.initial_state(now + val.seconds),
                )
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
                from workflows.isolation.base import scan_git_branches
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
            else:
                new_events.append(Event(
                    event_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='outbox',
                    payload=ev.WorkflowYielded(value=val),
                ))

        return new_events

    def _register_children(self, state, ctx, new_events, execution_id, parent_workflow_id):
        for handle in ctx.new_children:
            self._register_child(state, handle, execution_id, parent_workflow_id)

    def _register_child(self, state, handle, execution_id, parent_workflow_id):
        from workflows.isolation.base import StorageConfig, setup_child_workspace
        parent_wf = state.workflows.get(parent_workflow_id) if parent_workflow_id else None

        child_wf = WorkflowState(
            name=handle.workflow_name,
            args=list(handle.args),
            parent_workflow_id=parent_workflow_id,
        )

        if parent_wf and parent_wf.workdir:
            config = handle.storage or StorageConfig(mode='same')
            parent_dir = Path(parent_wf.workdir)
            child_dir = parent_dir.parent / handle.id

            child_workdir, child_branches = setup_child_workspace(
                parent_dir, child_dir,
                parent_wf.branches, config,
            )
            child_wf.workdir = str(child_workdir)
            child_wf.branches = child_branches

        state.workflows[handle.id] = child_wf
