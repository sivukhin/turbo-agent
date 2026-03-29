"""Web server for turbo-agent UI."""

import os
import threading
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from workflows import Engine, EngineConfig, Store
from workflows.loader import load_workflows_from_file
from workflows.tasks import TaskStore
from workflows.events import (
    UserPromptRequest, UserPromptResult, LlmRequest, LlmResponse,
    serialize_payload,
)

TASKS_DB_PATH = os.environ.get('TURBO_TASKS_DB', os.path.join(os.path.dirname(__file__), '..', 'tasks.db'))

# Pricing per 1M tokens (USD). Keys are prefixes matched against model IDs.
MODEL_PRICING = {
    # Anthropic
    'claude-opus-4-6':   {'input': 5.00, 'output': 25.00, 'cache_write': 6.25, 'cache_read': 0.50},
    'claude-opus-4-5':   {'input': 5.00, 'output': 25.00, 'cache_write': 6.25, 'cache_read': 0.50},
    'claude-opus-4-1':   {'input': 15.00, 'output': 75.00, 'cache_write': 18.75, 'cache_read': 1.50},
    'claude-opus-4-':    {'input': 15.00, 'output': 75.00, 'cache_write': 18.75, 'cache_read': 1.50},
    'claude-sonnet-4':   {'input': 3.00, 'output': 15.00, 'cache_write': 3.75, 'cache_read': 0.30},
    'claude-sonnet-3':   {'input': 3.00, 'output': 15.00, 'cache_write': 3.75, 'cache_read': 0.30},
    'claude-haiku-4-5':  {'input': 1.00, 'output': 5.00, 'cache_write': 1.25, 'cache_read': 0.10},
    'claude-haiku-3-5':  {'input': 0.80, 'output': 4.00, 'cache_write': 1.00, 'cache_read': 0.08},
    'claude-haiku-3':    {'input': 0.25, 'output': 1.25, 'cache_write': 0.30, 'cache_read': 0.03},
    # OpenAI
    'gpt-5.4-pro':       {'input': 30.00, 'output': 180.00, 'cache_read': 0.0},
    'gpt-5.4-nano':      {'input': 0.20, 'output': 1.25, 'cache_read': 0.02},
    'gpt-5.4-mini':      {'input': 0.75, 'output': 4.50, 'cache_read': 0.075},
    'gpt-5.4':           {'input': 2.50, 'output': 15.00, 'cache_read': 0.25},
    'gpt-4.1-nano':      {'input': 0.10, 'output': 0.40, 'cache_read': 0.025},
    'gpt-4.1-mini':      {'input': 0.40, 'output': 1.60, 'cache_read': 0.10},
    'gpt-4.1':           {'input': 2.00, 'output': 8.00, 'cache_read': 0.50},
    'gpt-4o-mini':       {'input': 0.15, 'output': 0.60, 'cache_read': 0.075},
    'gpt-4o':            {'input': 2.50, 'output': 10.00, 'cache_read': 0.625},
    'o3':                {'input': 2.00, 'output': 8.00, 'cache_read': 0.50},
    'o4-mini':           {'input': 1.10, 'output': 4.40, 'cache_read': 0.275},
}


def _get_pricing(model: str) -> dict | None:
    """Find pricing by longest prefix match on model ID."""
    best = None
    best_len = 0
    for prefix, pricing in MODEL_PRICING.items():
        if model.startswith(prefix) and len(prefix) > best_len:
            best = pricing
            best_len = len(prefix)
    return best


def _compute_step_cost(usage: dict, model: str) -> float:
    """Compute cost in USD for a single LLM call."""
    pricing = _get_pricing(model)
    if not pricing:
        return 0.0
    cost = 0.0
    cost += usage.get('input_tokens', 0) * pricing.get('input', 0) / 1_000_000
    cost += usage.get('output_tokens', 0) * pricing.get('output', 0) / 1_000_000
    cost += usage.get('cache_creation_input_tokens', 0) * pricing.get('cache_write', 0) / 1_000_000
    cost += usage.get('cache_read_input_tokens', 0) * pricing.get('cache_read', 0) / 1_000_000
    return cost

app = FastAPI(title='turbo-agent')

# Background workers: one thread per execution, sleeps until triggered
_workers: dict[str, threading.Event] = {}
_workers_lock = threading.Lock()


def _engine_for_execution(store, execution_id):
    state, _ = store.load_state(execution_id)
    if state.source_file:
        registry = load_workflows_from_file(state.source_file)
    else:
        registry = {}
    return Engine(EngineConfig(workflows_registry=registry)), state


def _has_pending_prompts(store, execution_id):
    return len(_pending_prompt_workflow_ids(store, execution_id)) > 0


def _pending_prompt_workflow_ids(store, execution_id):
    outbox = store.read_outbox(execution_id)
    inbox = store.read_inbox(execution_id)
    answered = {e.payload.request_id for e in inbox if isinstance(e.payload, UserPromptResult)}
    wf_ids = set()
    for e in outbox:
        if isinstance(e.payload, UserPromptRequest) and e.payload.request_id not in answered:
            wf_ids.add(e.workflow_id)
    return wf_ids


def _worker_loop(execution_id: str, trigger: threading.Event, store_factory=None):
    """Background worker: step until blocked, then sleep until triggered."""
    make_store = store_factory
    while True:
        made_progress = True
        while made_progress:
            made_progress = False
            store = make_store()
            try:
                state, _ = store.load_state(execution_id)
                if state.finished:
                    return
                if _has_pending_prompts(store, execution_id):
                    break
                engine, _ = _engine_for_execution(store, execution_id)
                engine.step(store, execution_id)
                made_progress = True
            finally:
                store.close()

        trigger.clear()
        trigger.wait(timeout=30)

        store = make_store()
        try:
            state, _ = store.load_state(execution_id)
            if state.finished:
                return
        finally:
            store.close()


def _ensure_worker(execution_id: str, store_factory=None):
    """Ensure a background worker exists for this execution. Wake it if sleeping."""
    with _workers_lock:
        if execution_id not in _workers:
            trigger = threading.Event()
            _workers[execution_id] = trigger
            threading.Thread(
                target=_worker_loop, args=(execution_id, trigger, store_factory),
                daemon=True, name=f'worker-{execution_id[:8]}',
            ).start()
        else:
            _workers[execution_id].set()


def _wake_worker(execution_id: str):
    """Wake the background worker for an execution."""
    with _workers_lock:
        trigger = _workers.get(execution_id)
        if trigger:
            trigger.set()


# ---- API ----

def _execution_total_cost(store, execution_id):
    events = store.read_all_events(execution_id)
    cost = 0.0
    for e in events:
        if isinstance(e.payload, LlmResponse) and e.payload.usage:
            cost += _compute_step_cost(e.payload.usage, e.payload.model)
    return round(cost, 6)


@app.get('/api/tasks/{task_id}/executions/{execution_id}')
def get_execution(task_id: str, execution_id: str):
    store = _task_exec_store(task_id)
    try:
        state, last_event = store.load_state(execution_id)
    except KeyError:
        store.close()
        raise HTTPException(404, 'Execution not found')

    has_prompts = _has_pending_prompts(store, execution_id)
    prompt_wf_ids = _pending_prompt_workflow_ids(store, execution_id)
    created_at = store.get_created_at(execution_id)
    all_events = store.read_all_events(execution_id)
    total_cost = 0.0
    total_tokens = {'input_tokens': 0, 'output_tokens': 0, 'cache_creation_input_tokens': 0, 'cache_read_input_tokens': 0}
    for e in all_events:
        if isinstance(e.payload, LlmResponse) and e.payload.usage:
            u = e.payload.usage
            total_cost += _compute_step_cost(u, e.payload.model)
            for k in total_tokens:
                total_tokens[k] += u.get(k, 0)
    store.close()

    workflows = {}
    for wf_id, wf in state.workflows.items():
        workflows[wf_id] = {
            'name': wf.name,
            'status': wf.status,
            'parent': wf.parent_workflow_id,
            'result': repr(wf.result) if wf.result is not None else None,
            'conversation_id': wf.conversation_id,
            'description': getattr(wf, 'description', ''),
        }
    return {
        'execution_id': execution_id,
        'root_workflow_id': state.root_workflow_id,
        'finished': state.finished,
        'last_event': last_event,
        'source_file': state.source_file,
        'description': getattr(state, 'description', ''),
        'workflows': workflows,
        'handlers': {k: {'type': v.handler_type} for k, v in state.handlers.items()},
        'has_pending_prompts': has_prompts,
        'prompt_workflow_ids': list(prompt_wf_ids),
        'running_bg': execution_id in _workers,
        'created_at': created_at,
        'total_cost': round(total_cost, 6),
        'total_tokens': total_tokens,
    }


@app.get('/api/tasks/{task_id}/executions/{execution_id}/events')
def get_events(task_id: str, execution_id: str, after: int = 0):
    store = _task_exec_store(task_id)
    events = store.read_all_events(execution_id, after_event_id=after)
    store.close()
    return [{
        'event_id': e.event_id,
        'workflow_id': e.workflow_id,
        'category': e.category,
        'type': e.type,
        'payload': serialize_payload(e.payload),
        'created_at': e.created_at,
    } for e in events]


def _compute_usage_stats(store, execution_id, messages):
    """Compute per-turn and per-step LLM usage stats for conversation messages.

    A "turn" spans from one visible assistant message to the next.
    Each LLM call within a turn is a "step".
    Stats are attached to the final (visible) assistant message of each turn.
    """
    if not messages:
        return {}

    all_events = store.read_all_events(execution_id)
    llm_requests = [e for e in all_events if isinstance(e.payload, LlmRequest)]
    llm_responses = [e for e in all_events if isinstance(e.payload, LlmResponse) and e.payload.usage]

    # Build request_id → request event map for timing
    # Match requests to responses by workflow_id ordering
    req_by_wf = {}
    for e in llm_requests:
        req_by_wf.setdefault(e.workflow_id, []).append(e)
    resp_by_wf = {}
    for e in llm_responses:
        resp_by_wf.setdefault(e.workflow_id, []).append(e)

    # For each response, find corresponding request (matched by position within workflow)
    response_timing = {}  # response event_id → duration_s
    for wf_id in resp_by_wf:
        reqs = req_by_wf.get(wf_id, [])
        resps = resp_by_wf[wf_id]
        for i, resp in enumerate(resps):
            if i < len(reqs) and reqs[i].created_at and resp.created_at:
                response_timing[resp.event_id] = resp.created_at - reqs[i].created_at

    # Find boundaries: visible assistant messages (non-hidden)
    boundaries = []
    for i, m in enumerate(messages):
        labels = (m.ref.meta.get('labels', '') if m.ref.meta else '').split(',')
        if m.role == 'assistant' and 'hidden' not in labels:
            boundaries.append((i, m.ref.event_time, getattr(m, 'created_at', 0.0)))

    # Find user message timestamps for turn duration
    user_times = []
    for m in messages:
        if m.role == 'user':
            labels = (m.ref.meta.get('labels', '') if m.ref.meta else '').split(',')
            if 'hidden' not in labels:
                user_times.append((m.ref.event_time, getattr(m, 'created_at', 0.0)))

    stats = {}
    prev_event_time = 0
    for idx, (msg_idx, event_time, msg_created_at) in enumerate(boundaries):
        steps = []
        total = {'input_tokens': 0, 'output_tokens': 0, 'cache_creation_input_tokens': 0, 'cache_read_input_tokens': 0}
        total_cost = 0.0
        total_llm_time = 0.0
        for e in llm_responses:
            if e.event_id > prev_event_time and e.event_id <= event_time:
                u = e.payload.usage
                step_cost = _compute_step_cost(u, e.payload.model)
                duration = response_timing.get(e.event_id, 0.0)
                step = {
                    'input_tokens': u.get('input_tokens', 0),
                    'output_tokens': u.get('output_tokens', 0),
                    'cache_creation_input_tokens': u.get('cache_creation_input_tokens', 0),
                    'cache_read_input_tokens': u.get('cache_read_input_tokens', 0),
                    'model': e.payload.model,
                    'cost': round(step_cost, 6),
                    'duration': round(duration, 2),
                }
                steps.append(step)
                for k in total:
                    total[k] += step.get(k, 0)
                total_cost += step_cost
                total_llm_time += duration

        # Turn duration: from last user message before this boundary to this message
        turn_duration = 0.0
        if msg_created_at:
            for ut_event_time, ut_created_at in reversed(user_times):
                if ut_event_time < event_time and ut_created_at:
                    turn_duration = msg_created_at - ut_created_at
                    break

        stats[msg_idx] = {
            'total': total,
            'steps': steps,
            'step_count': len(steps),
            'cost': round(total_cost, 6),
            'llm_time': round(total_llm_time, 2),
            'turn_time': round(turn_duration, 2),
        }
        prev_event_time = event_time

    return stats


@app.get('/api/tasks/{task_id}/executions/{execution_id}/conversation/{conversation_id}')
def get_conversation(task_id: str, execution_id: str, conversation_id: str):
    store = _task_exec_store(task_id)
    refs = store.conv_list_messages(conversation_id)
    messages = store.conv_read_messages(refs)
    usage_stats = _compute_usage_stats(store, execution_id, messages)
    store.close()
    result = []
    for i, m in enumerate(messages):
        entry = {
            'message_id': m.ref.message_id,
            'conversation_id': m.ref.conversation_id,
            'layer': m.ref.layer,
            'role': m.role,
            'content': m.content,
            'meta': m.ref.meta,
            'event_time': m.ref.event_time,
            'created_at': getattr(m, 'created_at', 0.0),
        }
        if i in usage_stats:
            entry['usage'] = usage_stats[i]
        result.append(entry)
    return result


class PromptAnswer(BaseModel):
    request_id: str
    response: str


@app.post('/api/tasks/{task_id}/executions/{execution_id}/answer')
def answer_prompt(task_id: str, execution_id: str, answer: PromptAnswer):
    store = _task_exec_store(task_id)
    try:
        state, _ = store.load_state(execution_id)
    except KeyError:
        store.close()
        raise HTTPException(404, 'Execution not found')

    outbox = store.read_outbox(execution_id)
    prompt_event = None
    for e in outbox:
        if isinstance(e.payload, UserPromptRequest) and e.payload.request_id == answer.request_id:
            prompt_event = e
            break
    if not prompt_event:
        store.close()
        raise HTTPException(404, 'Prompt not found')

    store.append_event(
        execution_id, prompt_event.workflow_id, 'inbox',
        UserPromptResult(request_id=answer.request_id, response=answer.response),
    )
    store.close()

    # Resume background execution
    _ensure_worker(execution_id, store_factory=lambda: _task_exec_store(task_id))
    return {'ok': True}


class UpdateDescription(BaseModel):
    description: str


@app.patch('/api/tasks/{task_id}/executions/{execution_id}')
def update_execution(task_id: str, execution_id: str, body: UpdateDescription):
    store = _task_exec_store(task_id)
    try:
        state, _ = store.load_state(execution_id)
    except KeyError:
        store.close()
        raise HTTPException(404, 'Execution not found')
    state.description = body.description
    store.save_state(execution_id, state)
    store.close()
    return {'ok': True}


@app.get('/api/tasks/{task_id}/executions/{execution_id}/prompts')
def get_pending_prompts(task_id: str, execution_id: str):
    store = _task_exec_store(task_id)
    outbox = store.read_outbox(execution_id)
    inbox = store.read_inbox(execution_id)
    store.close()

    answered = {e.payload.request_id for e in inbox if isinstance(e.payload, UserPromptResult)}
    pending = []
    for e in outbox:
        if isinstance(e.payload, UserPromptRequest) and e.payload.request_id not in answered:
            pending.append({
                'request_id': e.payload.request_id,
                'workflow_id': e.workflow_id,
                'event_id': e.event_id,
            })
    return pending



# ---- Tasks API ----

TASKS_DIR = os.environ.get('TURBO_TASKS_DIR', os.path.join(os.path.dirname(__file__), '..', '.tasks'))


def _task_store():
    return TaskStore(TASKS_DB_PATH, tasks_dir=TASKS_DIR)


def _task_exec_store(task_id: str):
    from workflows.tasks import task_db_path
    db = task_db_path(TASKS_DIR, task_id)
    os.makedirs(os.path.dirname(db), exist_ok=True)
    return Store(db)


@app.get('/api/projects')
def list_projects():
    ts = _task_store()
    projects = ts.list_projects()
    ts.close()
    return projects


@app.post('/api/projects')
def create_project(body: dict):
    ts = _task_store()
    result = ts.create_project(body.get('name', ''))
    ts.close()
    return result


@app.get('/api/tasks')
def list_tasks():
    ts = _task_store()
    tasks = ts.list()
    ts.close()
    return tasks


@app.post('/api/tasks')
def create_task(body: dict):
    ts = _task_store()
    task = ts.create(
        name=body.get('name', 'Untitled'),
        description=body.get('description', ''),
        labels=body.get('labels'),
        color=body.get('color', ''),
    )
    ts.close()
    return task


@app.get('/api/tasks/{task_id}')
def get_task(task_id: str):
    ts = _task_store()
    try:
        task = ts.get(task_id)
    except KeyError:
        ts.close()
        raise HTTPException(404, 'Task not found')
    ts.close()
    return task


@app.patch('/api/tasks/{task_id}')
def update_task(task_id: str, body: dict):
    ts = _task_store()
    try:
        task = ts.update(task_id, **body)
    except KeyError:
        ts.close()
        raise HTTPException(404, 'Task not found')
    ts.close()
    return task


@app.delete('/api/tasks/{task_id}')
def delete_task(task_id: str):
    ts = _task_store()
    try:
        ts.delete(task_id)
    except KeyError:
        ts.close()
        raise HTTPException(404, 'Task not found')
    ts.close()
    return {'ok': True}


# ---- Task-scoped executions ----

@app.get('/api/tasks/{task_id}/executions')
def list_task_executions(task_id: str):
    ts = _task_store()
    try:
        task = ts.get(task_id)
    except KeyError:
        ts.close()
        raise HTTPException(404, 'Task not found')
    ts.close()

    store = _task_exec_store(task_id)
    execs = store.list_executions()
    result = []
    for eid, state, created_at in execs:
        cost = _execution_total_cost(store, eid)
        has_prompts = _has_pending_prompts(store, eid)
        result.append({
            'execution_id': eid,
            'workflow': state.workflows[state.root_workflow_id].name,
            'workflows_count': len(state.workflows),
            'finished': state.finished,
            'created_at': created_at,
            'total_cost': cost,
            'has_pending_prompts': has_prompts,
            'description': getattr(state, 'description', ''),
        })
    store.close()
    return result


class TaskStartRequest(BaseModel):
    target: str
    args: list = []


@app.post('/api/tasks/{task_id}/executions')
def start_task_execution(task_id: str, req: TaskStartRequest):
    ts = _task_store()
    try:
        task = ts.get(task_id)
    except KeyError:
        ts.close()
        raise HTTPException(404, 'Task not found')
    ts.close()

    if ':' not in req.target:
        raise HTTPException(400, 'target must be file.py:workflow_name')
    file_path, wf_name = req.target.rsplit(':', 1)
    registry = load_workflows_from_file(file_path)
    if wf_name not in registry:
        raise HTTPException(400, f'Unknown workflow: {wf_name}')

    from workflows.tasks import task_workdir as _task_workdir
    store = _task_exec_store(task_id)
    engine = Engine(EngineConfig(workflows_registry=registry))
    workdir = _task_workdir(TASKS_DIR, task_id)
    os.makedirs(workdir, exist_ok=True)
    execution_id = engine.start(
        store, wf_name, req.args,
        source_file=file_path,
        workdir=workdir,
        parent_conversation_id=task.get('context_conversation_id'),
    )
    store.close()

    _ensure_worker(execution_id, store_factory=lambda: _task_exec_store(task_id))
    return {'execution_id': execution_id, 'task_id': task_id}


# ---- Static files ----

_dist_dir = Path(__file__).parent / 'dist'


@app.get('/{path:path}')
def static_files(path: str):
    if not path or path == '/':
        return FileResponse(_dist_dir / 'index.html')
    file = _dist_dir / path
    if file.is_file():
        return FileResponse(file)
    return FileResponse(_dist_dir / 'index.html')


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8080)
