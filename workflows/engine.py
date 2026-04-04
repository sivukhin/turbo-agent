import copy
import pickle
import time
from collections.abc import Callable
from pathlib import Path
from workflows.decorator import _TickContext, _current_ctx
from workflows.handlers import HANDLER_REGISTRY
from workflows.ids import new_id
from workflows.isolation.base import (
    StorageConfig,
    setup_child_workspace,
    scan_git_branches,
)
from workflows.ops import (
    Event,
    WorkflowHandle,
    WorkflowState,
    HandlerState,
    ExecutionState,
)
from workflows.operations import DEFAULT_OP_HANDLERS
from workflows.operations.base import OpContext
import workflows.events as ev


def _strip_secrets(val):
    """Remove private_env from an op before persisting."""
    if hasattr(val, "private_env") and val.private_env:
        val = copy.copy(val)
        val.private_env = None
    return val


from dataclasses import dataclass as _dataclass, field as _field
from workflows.event_handlers import DEFAULT_EVENT_HANDLERS
from workflows.event_handlers.base import get_event_type_name


@_dataclass
class EngineConfig:
    """Configuration for the workflow engine."""

    workflows_registry: dict = _field(default_factory=dict)
    workflow_event_handlers: dict = _field(
        default_factory=lambda: dict(HANDLER_REGISTRY)
    )
    event_handlers: list = _field(default_factory=lambda: list(DEFAULT_EVENT_HANDLERS))
    op_handlers: list = _field(default_factory=lambda: list(DEFAULT_OP_HANDLERS))
    on_events: Callable[[list[Event]], None] | None = None


class Engine:
    """Event-sourced workflow engine.

    All heavy operations (shell, file, llm) are async:
    - Op handler emits a request event to outbox, sets workflow to waiting
    - Global EventHandler processes the request, does the work, emits result to inbox
    - WorkflowEventHandler on the result unblocks the workflow
    """

    def __init__(self, config: EngineConfig):
        self.config = config
        self._op_handlers = {cls.op_type(): cls for cls in config.op_handlers}

    def start(
        self,
        store,
        workflow_name,
        args,
        now=None,
        source_file=None,
        workdir=None,
        parent_conversation_id=None,
    ) -> str:
        now = now if now is not None else time.time()
        execution_id = new_id()
        root_workflow_id = new_id()

        wf_state = WorkflowState(name=workflow_name, args=list(args))
        if workdir:
            wf_dir = Path(workdir).resolve() / execution_id / root_workflow_id
            wf_dir.mkdir(parents=True, exist_ok=True)
            wf_state.workdir = str(wf_dir)
            wf_state.branches = scan_git_branches(wf_dir)

        conv_id = new_id()
        wf_state.conversation_id = conv_id
        if parent_conversation_id:
            parent_ref = store.conv_resolve_ref(parent_conversation_id)
            store.create_conversation(
                conv_id,
                parent_conversation_id=parent_ref.conversation_id,
                parent_message_id=parent_ref.message_id,
                parent_layer=parent_ref.layer,
            )
        else:
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

    def step(self, store, execution_id, now=None) -> bool:
        """Run one tick. Returns True if progress was made, False if idle."""
        now = now if now is not None else time.time()
        return self._tick_and_process(store, execution_id, now)

    def _emit_events(self, store, new_events):
        if new_events:
            store.append_events(
                [
                    (e.execution_id, e.workflow_id, e.category, e.payload)
                    for e in new_events
                ]
            )
            if self.config.on_events:
                self.config.on_events(new_events)

    def _tick_and_process(self, store, execution_id, now) -> bool:
        state, last_processed = store.load_state(execution_id)
        handlers_before = set(state.handlers)

        new_events = self._handle_tick(state, execution_id, now, store)

        # Catch up newly registered handlers with past events
        new_handler_ids = set(state.handlers) - handlers_before
        if new_handler_ids:
            self._catchup_handlers(
                store, state, execution_id, new_handler_ids, new_events
            )

        new_events.extend(self._resolve_workflow_handlers(state, execution_id, now))
        self._check_finished(state)
        self._prune_finished(state)

        store.save_state(execution_id, state, last_processed_event_id=last_processed)
        self._emit_events(store, new_events)

        made_progress = bool(new_events)
        while self._process_events(store, execution_id, now):
            made_progress = True

        return made_progress

    def _process_events(self, store, execution_id, now):
        """Process one batch of unprocessed events. Returns True if any were processed."""
        state, last_processed = store.load_state(execution_id)
        all_events = store.read_all_events(execution_id, after_event_id=last_processed)
        if not all_events:
            return False

        new_events = self._dispatch_events(all_events, state, store)

        new_events.extend(self._resolve_workflow_handlers(state, execution_id, now))
        self._check_finished(state)
        self._prune_finished(state)

        last_event_id = all_events[-1].event_id
        store.save_state(execution_id, state, last_processed_event_id=last_event_id)
        self._emit_events(store, new_events)

        return True

    def _catchup_handlers(self, store, state, execution_id, handler_ids, new_events):
        """Replay past inbox events to newly registered handlers."""
        # Past persisted events
        if store:
            for past_event in store.read_inbox(execution_id):
                for handler_wf_id in handler_ids:
                    hs = state.handlers.get(handler_wf_id)
                    if not hs:
                        continue
                    handler_cls = self.config.workflow_event_handlers.get(
                        hs.handler_type
                    )
                    if handler_cls:
                        hs.state = handler_cls.on_event(
                            past_event.type,
                            past_event.workflow_id,
                            past_event.payload,
                            hs.state,
                        )
        # Current tick batch (not yet persisted)
        for event in new_events:
            if event.category != "inbox":
                continue
            for handler_wf_id in handler_ids:
                hs = state.handlers.get(handler_wf_id)
                if not hs:
                    continue
                handler_cls = self.config.workflow_event_handlers.get(hs.handler_type)
                if handler_cls:
                    hs.state = handler_cls.on_event(
                        event.type,
                        event.workflow_id,
                        event.payload,
                        hs.state,
                    )

    def _dispatch_events(self, events, state, store):
        """Dispatch events to global and workflow handlers. Returns new events."""
        new_events = []
        for event in events:
            # Global event handlers see all events and can modify state
            for handler in self.config.event_handlers:
                if get_event_type_name(handler) == event.type:
                    emitted = handler.handle(event, store, state)
                    if emitted:
                        new_events.extend(emitted)

            # Workflow event handlers only see inbox events
            if event.category == "inbox":
                for handler_wf_id, hs in list(state.handlers.items()):
                    handler_cls = self.config.workflow_event_handlers.get(
                        hs.handler_type
                    )
                    if handler_cls:
                        hs.state = handler_cls.on_event(
                            event.type,
                            event.workflow_id,
                            event.payload,
                            hs.state,
                        )
        return new_events

    def _resolve_workflow_handlers(self, state, execution_id, now):
        new_events = []
        for handler_wf_id in list(state.handlers):
            hs = state.handlers[handler_wf_id]
            handler_cls = self.config.workflow_event_handlers.get(hs.handler_type)
            if not handler_cls:
                continue
            wf = state.workflows.get(handler_wf_id)
            if not wf:
                del state.handlers[handler_wf_id]
                continue
            if handler_cls.resolve(hs.state, wf, now):
                del state.handlers[handler_wf_id]
            # Collect any events the handler wants to emit
            emit_events = getattr(hs.state, "emit_events", None)
            if emit_events:
                for evt_payload in emit_events:
                    new_events.append(
                        Event(
                            event_id=0,
                            execution_id=execution_id,
                            workflow_id=handler_wf_id,
                            category="inbox",
                            payload=evt_payload,
                        )
                    )
                emit_events.clear()
        return new_events

    def _check_finished(self, state):
        root = state.workflows.get(state.root_workflow_id)
        if root and root.status == "finished":
            state.finished = True

    def _prune_finished(self, state):
        to_remove = [
            wf_id
            for wf_id, wf in state.workflows.items()
            if wf.status == "finished" and wf_id != state.root_workflow_id
        ]
        for wf_id in to_remove:
            del state.workflows[wf_id]

    def _handle_tick(self, state, execution_id, now, store=None):
        new_events = []

        for workflow_id, wf in list(state.workflows.items()):
            if wf.status != "running":
                continue

            send_val = wf.send_val
            wf.send_val = None

            ctx = _TickContext()
            token = _current_ctx.set(ctx)

            try:
                if wf.checkpoint is None:
                    wf_func = self.config.workflows_registry[wf.name]
                    g = wf_func.create(*wf.args, **wf.kwargs)
                    val = next(g)
                else:
                    wf_func = self.config.workflows_registry[wf.name]
                    g = wf_func.resume(wf.checkpoint)
                    val = g.send(send_val)
            except StopIteration as e:
                wf.status = "finished"
                wf.result = e.value
                wf.checkpoint = None
                self._register_children(
                    state, ctx, new_events, execution_id, workflow_id, store
                )
                new_events.append(
                    Event(
                        event_id=0,
                        execution_id=execution_id,
                        workflow_id=workflow_id,
                        category="inbox",
                        payload=ev.WorkflowFinished(result=e.value),
                    )
                )
                continue
            finally:
                _current_ctx.reset(token)

            self._register_children(
                state, ctx, new_events, execution_id, workflow_id, store
            )
            wf.checkpoint = pickle.loads(g.save())

            op_ctx = OpContext(
                execution_id=execution_id,
                workflow_id=workflow_id,
                wf=wf,
                state=state,
                store=store,
                new_events=new_events,
                now=now,
                workflow_event_handlers=self.config.workflow_event_handlers,
            )
            handler = self._op_handlers.get(type(val))
            if handler:
                handler.handle(val, op_ctx)
            new_events.append(
                Event(
                    event_id=0,
                    execution_id=execution_id,
                    workflow_id=workflow_id,
                    category="outbox",
                    payload=ev.WorkflowYielded(
                        value=_strip_secrets(val), meta=getattr(val, "meta", {})
                    ),
                )
            )

        return new_events

    def _register_children(
        self, state, ctx, new_events, execution_id, parent_workflow_id, store=None
    ):
        for handle in ctx.new_children:
            self._register_child(
                state, handle, new_events, execution_id, parent_workflow_id, store
            )

    def _register_child(
        self, state, handle, new_events, execution_id, parent_workflow_id, store=None
    ):
        parent_wf = (
            state.workflows.get(parent_workflow_id) if parent_workflow_id else None
        )

        config = handle.storage or StorageConfig(mode="same")
        child_wf = WorkflowState(
            name=handle.workflow_name,
            args=list(handle.args),
            kwargs=dict(handle.kwargs),
            parent_workflow_id=parent_workflow_id,
            description=getattr(handle, "description", ""),
        )

        if parent_wf and parent_wf.workdir:
            parent_dir = Path(parent_wf.workdir)
            child_dir = parent_dir.parent / handle.id
            child_workdir, child_branches = setup_child_workspace(
                parent_dir,
                child_dir,
                parent_wf.branches,
                config,
            )
            child_wf.workdir = str(child_workdir)
            child_wf.branches = child_branches

        if store and parent_wf and parent_wf.conversation_id:
            child_conv_id = new_id()
            parent_ref = store.conv_resolve_ref(parent_wf.conversation_id)
            store.create_conversation(
                child_conv_id,
                parent_conversation_id=parent_ref.conversation_id,
                parent_message_id=parent_ref.message_id,
                parent_layer=parent_ref.layer,
            )
            child_wf.conversation_id = child_conv_id

        state.workflows[handle.id] = child_wf

        new_events.append(
            Event(
                event_id=0,
                execution_id=execution_id,
                workflow_id=handle.id,
                category="outbox",
                payload=ev.WorkflowSpawned(
                    child_workflow_id=handle.id,
                    name=handle.workflow_name,
                    args=list(handle.args),
                    parent_workflow_id=parent_workflow_id,
                    storage_mode=config.mode,
                ),
            )
        )
