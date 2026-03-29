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
    UserPromptRequest, UserPromptResult,
    serialize_payload,
)

DB_PATH = os.environ.get('TURBO_DB', os.path.join(os.path.dirname(__file__), '..', 'executions.db'))

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
    } for eid, state in execs]


@app.get('/api/executions/{execution_id}')
def get_execution(execution_id: str):
    store = _store()
    try:
        state, last_event = store.load_state(execution_id)
    except KeyError:
        store.close()
        raise HTTPException(404, 'Execution not found')

    has_prompts = _has_pending_prompts(store, execution_id)
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
    } for e in events]


@app.get('/api/executions/{execution_id}/conversation/{conversation_id}')
def get_conversation(execution_id: str, conversation_id: str):
    store = _store()
    refs = store.conv_list_messages(conversation_id)
    messages = store.conv_read_messages(refs)
    store.close()
    return [{
        'message_id': m.ref.message_id,
        'conversation_id': m.ref.conversation_id,
        'layer': m.ref.layer,
        'role': m.role,
        'content': m.content,
        'meta': m.ref.meta,
    } for m in messages]


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
