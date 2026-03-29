"""Web server for turbo-agent UI."""

import os
import threading
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from workflows import Engine, EngineConfig, Store
from workflows.loader import load_workflows_from_file
from workflows.events import (
    UserPromptRequest, UserPromptResult, LlmRequest, LlmResponse,
    serialize_payload,
)

DB_PATH = os.environ.get('TURBO_DB', os.path.join(os.path.dirname(__file__), '..', 'executions.db'))

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


def _store():
    return Store(DB_PATH)


def _engine_for_execution(store, execution_id):
    state, _ = store.load_state(execution_id)
    if state.source_file:
        registry = load_workflows_from_file(state.source_file)
    else:
        registry = {}
    return Engine(EngineConfig(workflows_registry=registry)), state


def _has_pending_prompts(store, execution_id):
    outbox = store.read_outbox(execution_id)
    inbox = store.read_inbox(execution_id)
    answered = {e.payload.request_id for e in inbox if isinstance(e.payload, UserPromptResult)}
    for e in outbox:
        if isinstance(e.payload, UserPromptRequest) and e.payload.request_id not in answered:
            return True
    return False


def _worker_loop(execution_id: str, trigger: threading.Event):
    """Background worker: step until blocked, then sleep until triggered."""
    while True:
        # Step as much as possible
        made_progress = True
        while made_progress:
            made_progress = False
            store = _store()
            try:
                state, _ = store.load_state(execution_id)
                if state.finished:
                    return  # done, exit thread
                if _has_pending_prompts(store, execution_id):
                    break  # blocked on user, go to sleep
                engine, _ = _engine_for_execution(store, execution_id)
                engine.step(store, execution_id)
                made_progress = True
            finally:
                store.close()

        # Sleep until triggered (prompt answered, etc.)
        trigger.clear()
        trigger.wait(timeout=30)  # wake periodically to check

        # Check if execution is finished
        store = _store()
        try:
            state, _ = store.load_state(execution_id)
            if state.finished:
                return
        finally:
            store.close()


def _ensure_worker(execution_id: str):
    """Ensure a background worker exists for this execution. Wake it if sleeping."""
    with _workers_lock:
        if execution_id not in _workers:
            trigger = threading.Event()
            _workers[execution_id] = trigger
            threading.Thread(
                target=_worker_loop, args=(execution_id, trigger),
                daemon=True, name=f'worker-{execution_id[:8]}',
            ).start()
        else:
            _workers[execution_id].set()  # wake up


def _wake_worker(execution_id: str):
    """Wake the background worker for an execution."""
    with _workers_lock:
        trigger = _workers.get(execution_id)
        if trigger:
            trigger.set()


# ---- API ----

@app.get('/api/executions')
def list_executions():
    store = _store()
    execs = store.list_executions()
    store.close()
    return [{
        'execution_id': eid,
        'workflow': state.workflows[state.root_workflow_id].name,
        'workflows_count': len(state.workflows),
        'finished': state.finished,
        'running_bg': eid in _workers,
        'created_at': created_at,
    } for eid, state, created_at in execs]


@app.get('/api/executions/{execution_id}')
def get_execution(execution_id: str):
    store = _store()
    try:
        state, last_event = store.load_state(execution_id)
    except KeyError:
        store.close()
        raise HTTPException(404, 'Execution not found')

    has_prompts = _has_pending_prompts(store, execution_id)
    created_at = store.get_created_at(execution_id)
    store.close()

    workflows = {}
    for wf_id, wf in state.workflows.items():
        workflows[wf_id] = {
            'name': wf.name,
            'status': wf.status,
            'parent': wf.parent_workflow_id,
            'result': repr(wf.result) if wf.result is not None else None,
            'conversation_id': wf.conversation_id,
        }
    return {
        'execution_id': execution_id,
        'root_workflow_id': state.root_workflow_id,
        'finished': state.finished,
        'last_event': last_event,
        'source_file': state.source_file,
        'workflows': workflows,
        'handlers': {k: {'type': v.handler_type} for k, v in state.handlers.items()},
        'has_pending_prompts': has_prompts,
        'running_bg': execution_id in _workers,
        'created_at': created_at,
    }


@app.get('/api/executions/{execution_id}/events')
def get_events(execution_id: str, after: int = 0):
    store = _store()
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


@app.get('/api/executions/{execution_id}/conversation/{conversation_id}')
def get_conversation(execution_id: str, conversation_id: str):
    store = _store()
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


@app.post('/api/executions/{execution_id}/answer')
def answer_prompt(execution_id: str, answer: PromptAnswer):
    store = _store()
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
    _ensure_worker(execution_id)
    return {'ok': True}


@app.get('/api/executions/{execution_id}/prompts')
def get_pending_prompts(execution_id: str):
    store = _store()
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


class StartRequest(BaseModel):
    target: str
    args: list = []
    workdir: str = '.workspace'


@app.post('/api/executions')
def start_execution(req: StartRequest):
    if ':' not in req.target:
        raise HTTPException(400, 'target must be file.py:workflow_name')
    file_path, wf_name = req.target.rsplit(':', 1)
    registry = load_workflows_from_file(file_path)
    if wf_name not in registry:
        raise HTTPException(400, f'Unknown workflow: {wf_name}')

    store = _store()
    engine = Engine(EngineConfig(workflows_registry=registry))
    workdir = os.path.abspath(req.workdir)
    os.makedirs(workdir, exist_ok=True)
    execution_id = engine.start(store, wf_name, req.args, source_file=file_path, workdir=workdir)
    store.close()

    # Run in background
    _ensure_worker(execution_id)
    return {'execution_id': execution_id}


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
