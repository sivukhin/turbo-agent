# turbo-agent

Durable workflow execution engine with event sourcing, persistent conversations, LLM integration, and a web UI.

Built on Python 3.14 with bytecode-level generator checkpointing -- workflows survive restarts and can be inspected/replayed from the event log.

## Quick start

```bash
uv sync
make web
```

Open `http://localhost:8080`. Create a task, start an execution with `examples/agent_demo.py:chat`.

## Architecture

**Workflows** are Python generators decorated with `@workflow`. Every `yield` is a checkpoint -- the engine can save and restore execution at any yield point.

```python
from workflows import workflow, conv_append, Latest, shell
from workflows.ops import ai, user_prompt, ai_response

@workflow
def chat():
    question = yield user_prompt()
    yield conv_append(role='user', content=question)
    response = yield ai(conversation=Latest, system='You are helpful.')
    yield conv_append(role='assistant', content=response.text, meta={'model': response.model})
    yield ai_response(response.text)
    return 'done'
```

**Operations** (yielded values) are handled by the engine:

| Operation | Description |
|-----------|-------------|
| `ai()` | LLM call (Anthropic/OpenAI) |
| `conv_append()` | Append message to conversation |
| `conv_list()` / `conv_read()` | Read conversation messages |
| `shell()` | Run shell command (host or Docker) |
| `read_file()` / `write_file()` | File I/O |
| `user_prompt()` | Wait for user input |
| `ai_response()` | Emit response for display |
| `sleep()` | Sleep for N seconds |
| `wait()` / `wait_all()` / `wait_any()` | Wait for child workflows |

**Child workflows** are started by calling a `@workflow` function inside another:

```python
@workflow
def parent():
    child = other_workflow(arg1, arg2, description='do stuff')
    result = yield wait(child)
```

## How the bytecode hack works

The `@workflow` decorator rewrites the generator's bytecode at import time to make it checkpointable. This is the core trick that makes durable execution possible with plain Python syntax -- no DSL, no code generation, just regular generators.

### The problem

Python generators have internal state (local variables, instruction pointer, evaluation stack) that isn't normally accessible or serializable. When a generator yields, CPython suspends it with all state on the C stack. You can't pickle a generator.

### The solution: sentinel + drain/restore

The decorator rewrites every `YIELD_VALUE` instruction to:

1. **Drain the stack**: Before yielding, pop everything off the evaluation stack into a list (`__drain__`). A sentinel value pushed at generator start marks the stack bottom -- the drain loop pops until it hits the sentinel.

2. **Save state**: Store the yield index (`__yield_idx__`), drained stack (`__drain__`), yielded value (`__yv__`), and all local variables. This is the checkpoint -- a plain dict that can be pickled.

3. **Restore on resume**: When resuming from a checkpoint, the decorator injects a jump table at the top of the function. It restores locals from the checkpoint, pushes the saved stack values back (using `FOR_ITER` + `SWAP` to grow the stack dynamically), and jumps to the right yield point.

```
Normal execution:              Checkpoint/Resume:

  push sentinel                  push sentinel
  ... normal code ...            restore locals from checkpoint
  [at yield N]:                  jump to yield N label
    drain stack to list          [at yield N]:
    save yield_idx, drain,         restore stack from drain list
      locals                       resume with send value
    restore stack from list
    yield value
```

The `FOR_ITER` + `SWAP` trick is key -- it lets us push an arbitrary number of values onto the stack at resume time, which CPython doesn't normally allow (the stack depth is fixed at compile time). We set `co_stacksize=100` to give enough headroom.

### Dual behavior of `@workflow`

A `@workflow` function behaves differently depending on context:

- **Called at top level**: Returns a `DurableGenerator` that can be iterated, checkpointed, and resumed.
- **Called inside another `@workflow`** (during an engine tick): Doesn't execute -- instead registers a child workflow via `contextvars` and returns a `WorkflowHandle`. The engine manages the child's lifecycle.

This means the same function call syntax (`child_workflow(args)`) either starts a concurrent child or creates a local generator, depending on whether the engine is driving execution.

### What the rewritten bytecode looks like

For a simple workflow:

```python
@workflow
def example(x):
    a = yield x + 1
    b = yield a + 2
    return b
```

The decorator produces (conceptually):

```python
def example(__checkpoint__, x):
    if __checkpoint__ is not None:
        __resume_idx__, __drain__, __yv__, locals = unpack(__checkpoint__)
        x, a, b = locals['x'], locals['a'], locals['b']
        # restore stack, jump to yield point
        if __resume_idx__ == 0: goto yield_0
        if __resume_idx__ == 1: goto yield_1

    SENTINEL  # mark stack bottom
    # ... original code with yield points wrapped ...

yield_0:
    __drain__ = drain_stack_until(SENTINEL)
    __yield_idx__ = 0
    restore_stack(__drain__)
    a = yield (x + 1)

yield_1:
    __drain__ = drain_stack_until(SENTINEL)
    __yield_idx__ = 1
    restore_stack(__drain__)
    b = yield (a + 2)

    return b
```

The actual implementation operates on raw CPython bytecode instructions via the `bytecode` library, handling `LOAD_FAST`, `STORE_FAST`, `YIELD_VALUE`, control flow labels, and stack manipulation opcodes directly.

## Event sourcing

All operations go through the event log: the engine emits an outbox event (request), an event handler does the work and emits an inbox event (result), and the workflow resumes.

Events are stored in Turso (SQLite-compatible) with typed JSON payloads.

## Tasks and executions

Tasks are the top-level organizational unit. Each task has:
- Name, description (markdown), labels (key-value), status (pending/finished)
- Its own Turso database for executions
- A context conversation with task title and description -- all executions fork from it
- Shared workspace directory

Executions run workflows within a task. Multiple executions can run concurrently.

## Conversations

Persistent, forkable, layered message storage:
- Messages have `role`, `content`, `meta` (arbitrary JSON), `event_time`
- Conversations can fork (child workflows get a fork of the parent's conversation)
- Layer compaction via `conv_replace_with()` -- original messages preserved
- Special roles: `tool_use`, `tool_result` (rendered nicely in UI, converted to API format for LLM calls)
- `meta.labels` -- comma-separated tags; `hidden` hides messages from default view
- `meta.model` -- LLM model name, shown in UI

## LLM providers

Configure via environment variables:
- `ANTHROPIC_API_KEY` -- Anthropic (Claude)
- `OPENAI_API_KEY` -- OpenAI (GPT)

Model string format: `provider/model-name` (e.g. `anthropic/claude-sonnet-4-20250514`).

## Isolation

Shell commands can run with different isolation strategies:
- `HostIsolation()` -- subprocess on host
- `DockerIsolation(image='python:3.13-slim-bookworm')` -- Docker container

Child workflows support storage modes: `same`, `copy-full`, `copy-git`, `branch`.

## Web UI

React + Tailwind, built with Vite. Features:
- Task board grouped by project
- Execution view with chat, events, status tabs
- Workflow tree navigation
- Interactive user prompts
- Token usage, cost tracking (Anthropic/OpenAI pricing), timing per turn
- Markdown rendering for assistant messages
- Expandable JSON tree for events

```bash
make build  # build frontend
make web    # build + start server
make test   # run tests
```

## CLI

```bash
uv run main.py task create "My task" -l project=demo
uv run main.py task list
uv run main.py task show <id>
uv run main.py task update <id> -d "description" -s finished
uv run main.py task delete <id>

uv run main.py start examples/agent_demo.py:chat
uv run main.py run examples/agent_demo.py:chat
uv run main.py status <execution_id>
uv run main.py events <execution_id>
uv run main.py conv <execution_id>
uv run main.py web
```

## Project structure

```
workflows/
  __init__.py          # public API
  decorator.py         # @workflow bytecode rewriting
  engine.py            # event-sourced execution engine
  ops.py               # operation dataclasses, state
  events.py            # typed event payloads
  store.py             # Turso DB persistence
  conversation.py      # conversation system
  handlers.py          # workflow event handlers (wait/sleep)
  tasks.py             # task management
  loader.py            # load workflows from files
  operations/          # operation handlers (emit outbox events)
  event_handlers/      # event handlers (do work, emit inbox events)
  llm/                 # LLM providers (Anthropic, OpenAI)
  isolation/           # shell isolation (host, Docker)
web/
  server.py            # FastAPI backend
  src/App.jsx          # React frontend
examples/              # demo workflows
tests/                 # 200+ tests
```

## Requirements

- Python 3.14
- Node.js (for frontend build)
- Docker (optional, for sandboxed shell)
