import pickle
from workflows.decorator import _TickContext, _current_ctx


class WorkflowHandle:
    """Serializable reference to a child workflow instance."""
    def __init__(self, wf_id, workflow_name, args):
        self.id = wf_id
        self.workflow_name = workflow_name
        self.args = args

    def __repr__(self):
        return f'<{self.workflow_name}#{self.id}>'


# ---- operations: yielded by workflows, interpreted by engine ----

def wait(handle):
    """Block until a single child finishes, return its result.

        result = yield wait(child)
    """
    return {'op': 'wait', 'deps': [handle.id], 'mode': 'all'}


def wait_all(handles):
    """Block until ALL children finish, return list of results (in order).

        results = yield wait_all([a, b, c])
    """
    return {'op': 'wait', 'deps': [h.id for h in handles], 'mode': 'all'}


def wait_any(handles):
    """Block until ANY child finishes, return (winner_id, result).

        wf_id, result = yield wait_any([a, b, c])
    """
    return {'op': 'wait', 'deps': [h.id for h in handles], 'mode': 'any'}


# ---- engine ----

def _new_wf(name, args):
    return {
        'name': name,
        'args': list(args),
        'checkpoint': None,
        'status': 'running',
        'wait_deps': [],
        'wait_mode': None,
        'result': None,
    }


class Engine:
    """Concurrent durable workflow executor.

    Each step ticks all running workflows once. Workflows can spawn children
    (auto-registered via contextvars) and wait on them.

    State schema:
        workflows: {id: {name, args, checkpoint, status, wait_deps, wait_mode, result}}
        root: id
        next_id: int
        step: int
        finished: bool
    """

    def __init__(self, state, registry):
        self.state = state
        self.registry = registry

    @classmethod
    def start(cls, registry, workflow_name, args):
        state = {
            'workflows': {
                '0': _new_wf(workflow_name, args),
            },
            'root': '0',
            'next_id': 1,
            'step': 0,
            'finished': False,
        }
        engine = cls(state, registry)
        outputs = engine._tick_all()
        return engine, outputs

    def step(self, send_val=None):
        self.state['step'] += 1
        root = self.state['workflows'][self.state['root']]
        if root['status'] == 'running' and send_val is not None:
            root['_send_val'] = send_val
        outputs = self._tick_all()
        return outputs, self.state['finished']

    def _alloc_id(self):
        wf_id = str(self.state['next_id'])
        self.state['next_id'] += 1
        return wf_id

    def _tick_all(self):
        outputs = []

        for wf_id, wf in list(self.state['workflows'].items()):
            if wf['status'] != 'running':
                continue
            send_val = wf.pop('_send_val', None)
            result = self._tick_one(wf_id, wf, send_val)
            if result is not None:
                outputs.extend(result.get('outputs', []))

        self._resolve_waits()

        root = self.state['workflows'][self.state['root']]
        if root['status'] == 'finished':
            self.state['finished'] = True

        return outputs

    def _resolve_waits(self):
        wfs = self.state['workflows']
        for wf_id, wf in list(wfs.items()):
            if wf['status'] != 'waiting':
                continue

            deps = wf['wait_deps']
            mode = wf['wait_mode']

            if mode == 'all':
                if all(wfs[d]['status'] == 'finished' for d in deps):
                    results = [wfs[d]['result'] for d in deps]
                    wf['_send_val'] = results[0] if len(deps) == 1 else results
                    wf['status'] = 'running'
                    wf['wait_deps'] = []
                    wf['wait_mode'] = None

            elif mode == 'any':
                for d in deps:
                    if wfs[d]['status'] == 'finished':
                        wf['_send_val'] = (d, wfs[d]['result'])
                        wf['status'] = 'running'
                        wf['wait_deps'] = []
                        wf['wait_mode'] = None
                        break

    def _tick_one(self, wf_id, wf, send_val):
        wf_func = self.registry[wf['name']]

        ctx = _TickContext(alloc_id=self._alloc_id)
        token = _current_ctx.set(ctx)

        try:
            if wf['checkpoint'] is None:
                g = wf_func.create(*wf['args'])
                val = next(g)
            else:
                g = wf_func.resume(wf['checkpoint'])
                val = g.send(send_val)
        except StopIteration as e:
            wf['status'] = 'finished'
            wf['result'] = e.value
            wf['checkpoint'] = None
            self._register_children(ctx)
            return {'outputs': []}
        finally:
            _current_ctx.reset(token)

        self._register_children(ctx)
        wf['checkpoint'] = pickle.loads(g.save())

        if isinstance(val, dict) and val.get('op') == 'wait':
            wf['wait_deps'] = val['deps']
            wf['wait_mode'] = val['mode']
            wf['status'] = 'waiting'
            self._resolve_waits()
            return {'outputs': []}

        return {'outputs': [(wf_id, wf['name'], val)]}

    def _register_children(self, ctx):
        for handle in ctx.new_children:
            self.state['workflows'][handle.id] = _new_wf(
                handle.workflow_name, handle.args,
            )
