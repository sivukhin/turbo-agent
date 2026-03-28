import pickle
from dataclasses import dataclass, field
from workflows.decorator import _TickContext, _current_ctx


@dataclass
class WorkflowHandle:
    """Serializable reference to a child workflow instance."""
    id: str
    workflow_name: str
    args: list

    def __repr__(self):
        return f'<{self.workflow_name}#{self.id}>'


# ---- operations ----

@dataclass
class WaitOp:
    """Yielded by workflows to block on child dependencies."""
    deps: list[str]
    mode: str  # 'all' | 'any'


def wait(handle):
    """Block until a single child finishes, return its result."""
    return WaitOp(deps=[handle.id], mode='all')


def wait_all(handles):
    """Block until ALL children finish, return list of results (in order)."""
    return WaitOp(deps=[h.id for h in handles], mode='all')


def wait_any(handles):
    """Block until ANY child finishes, return (winner_id, result)."""
    return WaitOp(deps=[h.id for h in handles], mode='any')


# ---- state ----

@dataclass
class WorkflowState:
    name: str
    args: list
    checkpoint: dict | None = None
    status: str = 'running'  # running | waiting | finished
    wait_deps: list[str] = field(default_factory=list)
    wait_mode: str | None = None
    result: object = None
    _send_val: object = field(default=None, repr=False)


@dataclass
class ExecutionState:
    workflows: dict[str, WorkflowState]
    root: str
    next_id: int = 1
    step: int = 0
    finished: bool = False


# ---- engine ----

class Engine:
    """Concurrent durable workflow executor.

    Each step ticks all running workflows once. Workflows can spawn children
    (auto-registered via contextvars) and wait on them.
    """

    def __init__(self, state: ExecutionState, registry: dict):
        self.state = state
        self.registry = registry

    @classmethod
    def start(cls, registry, workflow_name, args):
        state = ExecutionState(
            workflows={'0': WorkflowState(name=workflow_name, args=list(args))},
            root='0',
        )
        engine = cls(state, registry)
        outputs = engine._tick_all()
        return engine, outputs

    def step(self, send_val=None):
        self.state.step += 1
        root = self.state.workflows[self.state.root]
        if root.status == 'running' and send_val is not None:
            root._send_val = send_val
        outputs = self._tick_all()
        return outputs, self.state.finished

    def _alloc_id(self):
        wf_id = str(self.state.next_id)
        self.state.next_id += 1
        return wf_id

    def _tick_all(self):
        outputs = []

        for wf_id, wf in list(self.state.workflows.items()):
            if wf.status != 'running':
                continue
            send_val = wf._send_val
            wf._send_val = None
            result = self._tick_one(wf_id, wf, send_val)
            if result is not None:
                outputs.extend(result)

        self._resolve_waits()

        if self.state.workflows[self.state.root].status == 'finished':
            self.state.finished = True

        return outputs

    def _resolve_waits(self):
        wfs = self.state.workflows
        for wf in wfs.values():
            if wf.status != 'waiting':
                continue

            if wf.wait_mode == 'all':
                if all(wfs[d].status == 'finished' for d in wf.wait_deps):
                    results = [wfs[d].result for d in wf.wait_deps]
                    wf._send_val = results[0] if len(wf.wait_deps) == 1 else results
                    wf.status = 'running'
                    wf.wait_deps = []
                    wf.wait_mode = None

            elif wf.wait_mode == 'any':
                for d in wf.wait_deps:
                    if wfs[d].status == 'finished':
                        wf._send_val = (d, wfs[d].result)
                        wf.status = 'running'
                        wf.wait_deps = []
                        wf.wait_mode = None
                        break

    def _tick_one(self, wf_id, wf, send_val):
        wf_func = self.registry[wf.name]

        ctx = _TickContext(alloc_id=self._alloc_id)
        token = _current_ctx.set(ctx)

        try:
            if wf.checkpoint is None:
                g = wf_func.create(*wf.args)
                val = next(g)
            else:
                g = wf_func.resume(wf.checkpoint)
                val = g.send(send_val)
        except StopIteration as e:
            wf.status = 'finished'
            wf.result = e.value
            wf.checkpoint = None
            self._register_children(ctx)
            return []
        finally:
            _current_ctx.reset(token)

        self._register_children(ctx)
        wf.checkpoint = pickle.loads(g.save())

        if isinstance(val, WaitOp):
            wf.wait_deps = val.deps
            wf.wait_mode = val.mode
            wf.status = 'waiting'
            self._resolve_waits()
            return []

        return [(wf_id, wf.name, val)]

    def _register_children(self, ctx):
        for handle in ctx.new_children:
            self.state.workflows[handle.id] = WorkflowState(
                name=handle.workflow_name,
                args=list(handle.args),
            )
