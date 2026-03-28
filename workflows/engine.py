import pickle
import uuid
from dataclasses import dataclass, field
from workflows.decorator import _TickContext, _current_ctx
from workflows.handlers import HANDLER_REGISTRY


def _uuid():
    return uuid.uuid4().hex[:12]


@dataclass
class WorkflowHandle:
    """Serializable reference to a child workflow instance."""
    id: str
    workflow_name: str
    args: list

    def __repr__(self):
        return f'<{self.workflow_name}#{self.id}>'


# ---- operations (yielded by workflows) ----

@dataclass
class WaitOp:
    deps: list[str]
    mode: str  # 'wait' | 'wait_all' | 'wait_any'


def wait(handle):
    return WaitOp(deps=[handle.id], mode='wait')


def wait_all(handles):
    return WaitOp(deps=[h.id for h in handles], mode='wait_all')


def wait_any(handles):
    return WaitOp(deps=[h.id for h in handles], mode='wait_any')


# ---- state ----

@dataclass
class WorkflowState:
    name: str
    args: list
    checkpoint: dict | None = None
    status: str = 'running'
    result: object = None
    send_val: object = field(default=None, repr=False)


@dataclass
class HandlerState:
    handler_type: str
    state: dict


@dataclass
class Message:
    msg_id: int
    execution_id: str
    workflow_id: str | None
    category: str
    type: str
    payload: dict


@dataclass
class ExecutionState:
    workflows: dict[str, WorkflowState]
    handlers: dict[str, HandlerState]  # key = waiting workflow_id
    root_workflow_id: str
    finished: bool = False


# ---- engine ----

class Engine:
    """Event-sourced workflow engine. All state transitions are driven by messages."""

    def __init__(self, registry: dict):
        self.registry = registry

    def start(self, store, workflow_name, args) -> str:
        """Create a new execution, write initial tick, process it."""
        execution_id = _uuid()
        root_workflow_id = _uuid()
        state = ExecutionState(
            workflows={root_workflow_id: WorkflowState(name=workflow_name, args=list(args))},
            handlers={},
            root_workflow_id=root_workflow_id,
        )
        store.save_state(execution_id, state)
        store.append_message(execution_id, None, 'inbox', 'tick', {})
        self.process(store, execution_id)
        return execution_id

    def step(self, store, execution_id):
        """Send a tick and process all resulting messages."""
        store.append_message(execution_id, None, 'inbox', 'tick', {})
        self.process(store, execution_id)

    def process(self, store, execution_id):
        """Process all unprocessed inbox messages for an execution.
        Loops until no new inbox messages remain."""
        while True:
            state, last_processed = store.load_state(execution_id)
            messages = store.read_inbox(execution_id, after_msg_id=last_processed)
            if not messages:
                break

            new_messages = []

            for msg in messages:
                # Feed to all active handlers
                for handler_wf_id, hs in list(state.handlers.items()):
                    handler_cls = HANDLER_REGISTRY[hs.handler_type]
                    hs.state = handler_cls.on_message(
                        msg.type, msg.workflow_id, msg.payload, hs.state,
                    )

                # Process tick messages
                if msg.type == 'tick':
                    tick_msgs = self._handle_tick(state, execution_id)
                    new_messages.extend(tick_msgs)

            # Try to resolve all handlers
            for handler_wf_id in list(state.handlers):
                hs = state.handlers[handler_wf_id]
                handler_cls = HANDLER_REGISTRY[hs.handler_type]
                resolved, result = handler_cls.try_resolve(hs.state)
                if resolved:
                    wf = state.workflows[handler_wf_id]
                    wf.status = 'running'
                    wf.send_val = result
                    del state.handlers[handler_wf_id]

            # Check root
            root = state.workflows[state.root_workflow_id]
            if root.status == 'finished':
                state.finished = True

            # Persist
            last_msg_id = messages[-1].msg_id
            store.save_state(execution_id, state, last_processed_msg_id=last_msg_id)
            for m in new_messages:
                store.append_message(m.execution_id, m.workflow_id, m.category, m.type, m.payload)

    def _handle_tick(self, state, execution_id):
        """Tick all running workflows. Returns list of new messages to write."""
        new_messages = []

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
                self._register_children(state, ctx, new_messages, execution_id)
                new_messages.append(Message(
                    msg_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='inbox',
                    type='workflow_finished',
                    payload={'result': e.value},
                ))
                continue
            finally:
                _current_ctx.reset(token)

            self._register_children(state, ctx, new_messages, execution_id)
            wf.checkpoint = pickle.loads(g.save())

            if isinstance(val, WaitOp):
                handler_cls = HANDLER_REGISTRY[val.mode]
                wf.status = 'waiting'
                hs = HandlerState(
                    handler_type=val.mode,
                    state=handler_cls.initial_state(val.deps),
                )
                # Catch up: feed handler with already-finished deps
                for dep_id in val.deps:
                    dep_wf = state.workflows.get(dep_id)
                    if dep_wf and dep_wf.status == 'finished':
                        hs.state = handler_cls.on_message(
                            'workflow_finished', dep_id,
                            {'result': dep_wf.result}, hs.state,
                        )
                state.handlers[workflow_id] = hs
            else:
                new_messages.append(Message(
                    msg_id=0, execution_id=execution_id,
                    workflow_id=workflow_id, category='outbox',
                    type='workflow_yielded',
                    payload={'value': val},
                ))

        return new_messages

    def _register_children(self, state, ctx, new_messages, execution_id):
        for handle in ctx.new_children:
            state.workflows[handle.id] = WorkflowState(
                name=handle.workflow_name,
                args=list(handle.args),
            )
