import argparse
import json
import os
import sys
import time

from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

from workflows import Engine, EngineConfig, Store, load_workflows_from_file
from workflows.tasks import TaskStore
import workflows.events as ev


DB_PATH = os.path.join(os.path.dirname(__file__), "executions.db")
TASKS_DB_PATH = os.path.join(os.path.dirname(__file__), "tasks.db")
console = Console()


def _parse_cli_arg(a):
    """Parse a CLI argument: try JSON first, fall back to plain string."""
    try:
        return json.loads(a)
    except json.JSONDecodeError:
        return a


def _parse_target(target: str) -> tuple[str, str]:
    """Parse 'file.py:function' into (file_path, function_name)."""
    if ":" not in target:
        console.print(f"[red]Invalid target:[/] {target}")
        console.print("Expected format: [bold]path/to/file.py:workflow_name[/]")
        sys.exit(1)
    file_path, func_name = target.rsplit(":", 1)
    return file_path, func_name


def _load_registry(file_path: str) -> dict:
    return load_workflows_from_file(file_path)


def _load_registry_for_execution(store, execution_id):
    """Load the registry from the file stored in execution metadata."""
    state, _ = store.load_state(execution_id)
    file_path = state.source_file
    if not file_path:
        console.print(f"[red]Execution {execution_id} has no source file[/]")
        sys.exit(1)
    return load_workflows_from_file(file_path), state


def _status_style(status):
    return {"running": "green", "waiting": "yellow", "finished": "dim"}[status]


def _category_style(category):
    return {"inbox": "cyan", "outbox": "magenta"}[category]


def cmd_start(args):
    file_path, wf_name = _parse_target(args.target)
    registry = _load_registry(file_path)
    if wf_name not in registry:
        console.print(f"[red]Unknown workflow:[/] {wf_name}")
        console.print(f"Available in {file_path}: {', '.join(registry)}")
        sys.exit(1)

    parsed_args = [_parse_cli_arg(a) for a in args.args]
    store = Store(DB_PATH)
    engine = Engine(
        EngineConfig(
            workflows_registry=registry,
            on_events=lambda events: print_events(events, trace=True),
        )
    )
    workdir = os.path.abspath(args.workdir)
    os.makedirs(workdir, exist_ok=True)
    execution_id = engine.start(
        store, wf_name, parsed_args, source_file=file_path, workdir=workdir
    )
    store.close()

    console.print(f"[bold]Started execution[/] [cyan]{execution_id}[/]")
    console.print(f"  workflow: [bold]{wf_name}[/]({', '.join(args.args)})")


def _has_active_streams(state):
    """Check if any streams are active in-memory (non-durable)."""
    from workflows.event_handlers.shell_stream import _active_streams, _streams_lock

    with _streams_lock:
        return any(sid in _active_streams for sid in state.streams)


def cmd_step(args):
    store = Store(DB_PATH)
    registry, state = _load_registry_for_execution(store, args.id)
    if state.finished:
        store.close()
        console.print(f"[yellow]Execution {args.id} already finished[/]")
        sys.exit(1)

    trace = getattr(args, "trace", False)
    engine = Engine(
        EngineConfig(
            workflows_registry=registry,
            on_events=lambda events: print_events(events, trace=trace),
        )
    )

    # Run until safe pause point: no active in-memory streams
    try:
        while True:
            state, _ = store.load_state(args.id)
            if state.finished:
                break
            progress = engine.step(store, args.id)
            state, _ = store.load_state(args.id)
            if state.finished:
                break
            if not _has_active_streams(state):
                break
            if not progress:
                time.sleep(0.01)
    except KeyboardInterrupt:
        pass

    state, _ = store.load_state(args.id)
    if state.finished:
        root = state.workflows[state.root_workflow_id]
        console.print(f"\n  [bold green]returned:[/] {root.result!r}")
    store.close()


def _handle_user_prompts(store, execution_id, engine):
    """Check for unanswered UserPromptRequest events, prompt user, send result."""
    from workflows.events import UserPromptRequest, UserPromptResult

    outbox = store.read_outbox(execution_id)
    inbox = store.read_inbox(execution_id)

    # Find prompt requests
    requests = {
        e.payload.request_id: e
        for e in outbox
        if isinstance(e.payload, UserPromptRequest)
    }
    # Find already-answered
    answered = {
        e.payload.request_id for e in inbox if isinstance(e.payload, UserPromptResult)
    }

    for request_id, event in requests.items():
        if request_id in answered:
            continue
        # Unanswered prompt — ask the user
        response = console.input("[bold green]You>[/] ")

        # Write the result to inbox
        store.append_event(
            execution_id,
            event.workflow_id,
            "inbox",
            UserPromptResult(request_id=request_id, response=response),
        )

        engine.step(store, execution_id)


def _format_conv_content(role, content):
    """Format conversation content for CLI display."""
    if role == "tool_use":
        try:
            data = json.loads(content) if isinstance(content, str) else content
            name = data.get("name", "?")
            inp = data.get("input", {})
            if isinstance(inp, dict) and "command" in inp:
                return f"{name}: {inp['command']}"
            return f"{name}({json.dumps(inp, ensure_ascii=False)})"
        except (json.JSONDecodeError, TypeError):
            pass
    if role == "tool_result":
        try:
            data = json.loads(content) if isinstance(content, str) else content
            return data.get("output", str(content))
        except (json.JSONDecodeError, TypeError):
            pass
    return str(content)


def print_events(events, trace=False):
    """Print events to the CLI console."""
    all_events = sorted(events, key=lambda e: e.event_id)
    for event in all_events:
        payload = event.payload
        if not trace:
            if isinstance(payload, ev.ConvAppendRequest):
                labels = (payload.meta or {}).get("labels", "")
                hidden = "hidden" in labels.split(",")
                role_style = {
                    "user": "green",
                    "assistant": "blue",
                    "tool_use": "magenta",
                    "tool_result": "cyan",
                }.get(payload.role, "white")
                content = _format_conv_content(payload.role, payload.content)
                if hidden:
                    console.print(f"[dim]{payload.role}> {content}[/]")
                else:
                    console.print(f"[bold {role_style}]{payload.role}>[/] {content}")
                continue
            continue
        wf_id = (event.workflow_id or "-")[:8]
        cat_style = _category_style(event.category)
        payload_str = _format_payload(payload)
        console.print(
            f"  [{cat_style}]{event.category:<6}[/] [dim]{wf_id}[/] "
            f"[bold]{event.type}[/] {payload_str}"
        )


def cmd_status(args):
    store = Store(DB_PATH)
    state, last_event = store.load_state(args.id)
    store.close()

    finished_text = "[green]yes[/]" if state.finished else "[yellow]no[/]"
    root_tree = Tree(
        f"[bold]Execution[/] [cyan]{args.id}[/]  "
        f"finished={finished_text}  [dim]{state.source_file}[/]"
    )

    children_of = {}
    for wf_id, wf in state.workflows.items():
        children_of.setdefault(wf.parent_workflow_id, []).append(wf_id)

    def _render(tree_node, wf_id):
        wf = state.workflows[wf_id]
        style = _status_style(wf.status)
        root_tag = " [bold cyan][root][/]" if wf_id == state.root_workflow_id else ""
        extra = ""
        if wf.status == "finished":
            extra = f"  result={wf.result!r}"
        if wf_id in state.handlers:
            extra += f"  [yellow]({state.handlers[wf_id].handler_type})[/]"
        node = tree_node.add(
            f"[bold]{wf.name}[/]{root_tag}  [dim]{wf_id[:8]}[/]  [{style}]{wf.status}[/]{extra}"
        )
        for child_id in children_of.get(wf_id, []):
            _render(node, child_id)

    _render(root_tree, state.root_workflow_id)
    console.print(root_tree)


def cmd_events(args):
    store = Store(DB_PATH)
    inbox = store.read_inbox(args.id)
    outbox = store.read_outbox(args.id)
    store.close()

    all_events = sorted(inbox + outbox, key=lambda e: e.event_id)

    if not all_events:
        console.print("[dim]No events.[/]")
        return

    table = Table(title=f"Events for {args.id}", show_lines=False)
    table.add_column("#", style="dim", width=5)
    table.add_column("category", width=7)
    table.add_column("type", width=20)
    table.add_column("workflow", style="dim", width=14)
    table.add_column("payload")

    for event in all_events:
        cat_style = _category_style(event.category)
        wf_id = (event.workflow_id or "-")[:12]
        from workflows.events import serialize_payload

        payload_json = serialize_payload(event.payload)
        table.add_row(
            str(event.event_id),
            Text(event.category, style=cat_style),
            event.type,
            wf_id,
            payload_json,
        )

    console.print(table)


def _format_payload(payload):
    from workflows import events as ev

    if isinstance(payload, ev.WorkflowYielded):
        return repr(payload.value)
    if isinstance(payload, ev.WorkflowFinished):
        return f"result={payload.result!r}"
    if isinstance(payload, ev.ShellRequest):
        return f"$ {payload.command}"
    if isinstance(payload, ev.ShellResult):
        parts = [f"exit={payload.exit_code}"]
        out = payload.stdout.strip()
        err = payload.stderr.strip()
        if out:
            parts.append(f"stdout={out!r}")
        if err:
            parts.append(f"stderr={err!r}")
        return ", ".join(parts)
    if isinstance(payload, ev.FileReadRequest):
        return f"read {payload.path}"
    if isinstance(payload, ev.FileReadResult):
        content = payload.content
        if len(content) > 80:
            content = content[:77] + "..."
        return f"{payload.path}: {content!r}"
    if isinstance(payload, ev.FileWriteRequest):
        content = payload.content
        if len(content) > 80:
            content = content[:77] + "..."
        return f"write {payload.path}: {content!r}"
    if isinstance(payload, ev.FileWriteResult):
        return f"{payload.path} ({payload.size} bytes)"
    if isinstance(payload, ev.WaitStarted):
        deps = ", ".join(d[:8] for d in payload.deps)
        return f"{payload.mode}({deps})"
    if isinstance(payload, ev.SleepStarted):
        return f"{payload.seconds}s (wake_at={payload.wake_at})"
    if isinstance(payload, ev.UserPromptRequest):
        return f"[{payload.request_id[:8]}] waiting for input"
    if isinstance(payload, ev.UserPromptResult):
        return f"[{payload.request_id[:8]}] {payload.response!r}"
    if isinstance(payload, ev.WorkflowSpawned):
        parent = payload.parent_workflow_id[:8] if payload.parent_workflow_id else "-"
        return f"{payload.name}({payload.args}) parent={parent} storage={payload.storage_mode}"
    if isinstance(payload, ev.ConvAppendRequest):
        content = str(payload.content)
        if len(content) > 60:
            content = content[:57] + "..."
        return f"{payload.role}: {content!r}"
    if isinstance(payload, ev.ConvAppendResult):
        return f"msg={payload.message_id[:12]} layer={payload.layer}"
    if isinstance(payload, ev.ConvReadRequest):
        return f"conv={payload.conversation_id[:8]}"
    if isinstance(payload, ev.ConvReadResult):
        return f"{payload.count} messages"
    if isinstance(payload, ev.ConvListRequest):
        return f"conv={payload.conversation_id[:8]}"
    if isinstance(payload, ev.ConvListResult):
        return f"{payload.count} messages"
    if isinstance(payload, ev.ConvReplaceWithRequest):
        return f"{len(payload.new_messages)} new msgs"
    if isinstance(payload, ev.ConvReplaceWithResult):
        return f"layer={payload.new_layer} {len(payload.new_message_refs)} msgs"
    if isinstance(payload, ev.LlmRequest):
        if payload.conversation_ref:
            src = f"conv={payload.conversation_ref.conversation_id[:8]}"
        else:
            src = f"{len(payload.messages)} msgs" if payload.messages else "0 msgs"
        tools = f", {len(payload.tools)} tools" if payload.tools else ""
        return f"{payload.model} ({src}{tools}, T={payload.temperature})"
    if isinstance(payload, ev.LlmResponse):
        texts = [b["text"][:60] for b in payload.content if b.get("type") == "text"]
        tool_calls = [b["name"] for b in payload.content if b.get("type") == "tool_use"]
        parts = []
        if texts:
            parts.append(f"text={texts[0]!r}{'...' if len(texts[0]) >= 60 else ''}")
        if tool_calls:
            parts.append(f"tools=[{', '.join(tool_calls)}]")
        if payload.stop_reason:
            parts.append(f"stop={payload.stop_reason}")
        if payload.usage:
            parts.append(
                f"tokens={payload.usage.get('input_tokens', 0)}+{payload.usage.get('output_tokens', 0)}"
            )
        return ", ".join(parts)
    if isinstance(payload, ev.UsageEvent):
        return (
            f"{payload.source} {payload.model} "
            f"in={payload.input_tokens} out={payload.output_tokens} "
            f"${payload.cost_usd:.4f}"
        )
    return repr(payload)


def cmd_inspect(args):
    store = Store(DB_PATH)
    state, last_event = store.load_state(args.id)
    store.close()

    finished_text = "[green]yes[/]" if state.finished else "[yellow]no[/]"
    root_tree = Tree(
        f"[bold]Execution[/] [cyan]{args.id}[/]  "
        f"finished={finished_text}  last_event={last_event}  "
        f"[dim]{state.source_file}[/]"
    )

    # Build parent→children map
    children_of = {}
    for wf_id, wf in state.workflows.items():
        parent = wf.parent_workflow_id
        children_of.setdefault(parent, []).append(wf_id)

    # Render tree recursively
    def _render_wf(tree_node, wf_id):
        wf = state.workflows[wf_id]
        style = _status_style(wf.status)
        root_tag = " [bold cyan][root][/]" if wf_id == state.root_workflow_id else ""
        label = (
            f"[bold]{wf.name}[/]{root_tag}  [dim]{wf_id}[/]  [{style}]{wf.status}[/]"
        )
        wf_node = tree_node.add(label)

        wf_node.add(f"[dim]args:[/] {wf.args!r}")

        if wf.status == "finished":
            wf_node.add(f"[dim]result:[/] [green]{wf.result!r}[/]")

        if wf.checkpoint:
            cp = wf.checkpoint
            if cp.get("locals"):
                locals_node = wf_node.add("[dim]locals:[/]")
                for k, v in sorted(cp["locals"].items()):
                    locals_node.add(f"[bold]{k}[/] = {v!r}")
            if cp.get("drain"):
                wf_node.add(f"[dim]stack:[/] {cp['drain']!r}")
            if cp.get("yield_idx") is not None:
                wf_node.add(f"[dim]yield_idx:[/] {cp['yield_idx']}")
            if cp.get("yv") is not None:
                wf_node.add(f"[dim]yielded:[/] {cp['yv']!r}")

        handler = state.handlers.get(wf_id)
        if handler:
            h_node = wf_node.add(f"[dim]handler:[/] [yellow]{handler.handler_type}[/]")
            fields = vars(handler.state) if hasattr(handler.state, '__dict__') else handler.state
            if isinstance(fields, dict):
                for k, v in sorted(fields.items()):
                    h_node.add(f"[bold]{k}[/] = {v!r}")

        for child_id in children_of.get(wf_id, []):
            _render_wf(wf_node, child_id)

    # Start from root
    _render_wf(root_tree, state.root_workflow_id)

    # Render orphans (shouldn't happen, but just in case)
    rendered = set()

    def _collect(wf_id):
        rendered.add(wf_id)
        for child_id in children_of.get(wf_id, []):
            _collect(child_id)

    _collect(state.root_workflow_id)
    for wf_id in state.workflows:
        if wf_id not in rendered:
            _render_wf(root_tree, wf_id)

    console.print(root_tree)


def cmd_conv(args):
    store = Store(DB_PATH)
    state, _ = store.load_state(args.id)

    if args.conversation_id:
        # Show specific conversation by ID (prefix match)
        cur = store.conn.cursor()
        cur.execute(
            "SELECT conversation_id FROM conversation_refs WHERE conversation_id LIKE ?",
            (f"{args.conversation_id}%",),
        )
        rows = cur.fetchall()
        if not rows:
            console.print(f"[red]No conversation matching {args.conversation_id}[/]")
            store.close()
            sys.exit(1)
        for (conv_id,) in rows:
            # Find workflow name from active state or events
            wf_name, wf_id = _find_conv_owner(state, store, args.id, conv_id)
            _print_conversation(store, conv_id, wf_name, wf_id)
            console.print()
    else:
        # Show conversations belonging to this execution's workflows
        conv_ids = set()
        for wf in state.workflows.values():
            if wf.conversation_id:
                conv_ids.add(wf.conversation_id)
        # Also find conversations from pruned workflows via events
        from workflows.events import ConvAppendRequest

        outbox = store.read_outbox(args.id)
        for event in outbox:
            if isinstance(event.payload, ConvAppendRequest):
                conv_ids.add(event.payload.conversation_id)
        for conv_id in sorted(conv_ids):
            wf_name, wf_id = _find_conv_owner(state, store, args.id, conv_id)
            _print_conversation(store, conv_id, wf_name, wf_id)
            console.print()

    store.close()


def _find_conv_owner(state, store, execution_id, conversation_id):
    """Find the workflow that owns a conversation."""
    for wf_id, wf in state.workflows.items():
        if wf.conversation_id == conversation_id:
            return wf.name, wf_id
    # Search events for pruned workflows
    outbox = store.read_outbox(execution_id)
    from workflows.events import WorkflowSpawned, ConvAppendRequest

    for event in outbox:
        if isinstance(event.payload, ConvAppendRequest):
            if event.payload.conversation_id == conversation_id:
                return "(pruned)", event.workflow_id or "?"
    return "(unknown)", "?"


def _print_conversation(store, conversation_id, wf_name, wf_id):
    refs = store.conv_list_messages(conversation_id)
    messages = store.conv_read_messages(refs)
    ref = store.conv_resolve_ref(conversation_id)

    # Check for parent
    cur = store.conn.cursor()
    cur.execute(
        "SELECT parent_conversation_id FROM conversation_refs WHERE conversation_id = ?",
        (conversation_id,),
    )
    row = cur.fetchone()
    parent = row[0] if row and row[0] else None

    console.print(
        f"[bold]{wf_name}[/] [dim]{wf_id[:8]}[/]  "
        f"conv=[cyan]{conversation_id[:8]}[/]  "
        f"{len(messages)} messages"
        + (f"  [dim]parent={parent[:8]}[/]" if parent else "")
    )

    if not messages:
        console.print("  [dim](empty)[/]")
        return

    role_style = {
        "user": "green",
        "assistant": "blue",
        "system": "yellow",
        "tool_use": "purple",
        "tool_result": "magenta",
    }

    table = Table(show_lines=False, show_header=True, padding=(0, 1))
    table.add_column("#", style="dim", width=4)
    table.add_column("role", width=12)
    table.add_column("content")
    table.add_column("ref", style="dim", width=12)

    for i, msg in enumerate(messages):
        labels = msg.ref.meta.get("labels", "")
        hidden = "hidden" in labels.split(",")
        style = "dim" if hidden else role_style.get(msg.role, "white")
        content = msg.content
        if msg.role == "tool_use":
            try:
                data = json.loads(content)
                content = f"[bold purple]{data.get('name', '?')}[/] {json.dumps(data.get('input', {}))}"
            except Exception:
                pass
        elif msg.role == "tool_result":
            try:
                data = json.loads(content)
                content = f"[dim]{data.get('tool_use_id', '')}[/] {data.get('output', content)}"
            except Exception:
                pass
        if len(content) > 120:
            content = content[:117] + "..."
        from_parent = msg.ref.conversation_id != conversation_id
        parent_tag = " [dim](parent)[/]" if from_parent else ""
        label_tag = f" [dim][{labels}][/]" if labels else ""
        row_content = content + parent_tag + label_tag
        if hidden:
            row_content = f"[dim]{row_content}[/]"
        table.add_row(
            str(i),
            Text(msg.role, style=style),
            row_content,
            msg.ref.message_id[:10],
        )

    console.print(table)


def _run_loop(store, engine, execution_id, trace=False):
    """Run an execution to completion, handling user prompts interactively.
    Handles Ctrl+C gracefully — state is always persisted."""
    interrupted = False
    try:
        while True:
            state, _ = store.load_state(execution_id)
            if state.finished:
                break

            _handle_user_prompts(store, execution_id, engine)

            state, _ = store.load_state(execution_id)
            if state.finished:
                break

            if not engine.step(store, execution_id):
                time.sleep(0.01)

    except KeyboardInterrupt:
        interrupted = True

    state, _ = store.load_state(execution_id)
    if interrupted:
        console.print(
            f"\n  [yellow]Paused[/] [cyan]{execution_id}[/] — resume with: "
            f"[bold]main.py continue {execution_id}[/]"
        )
    elif state.finished:
        root = state.workflows[state.root_workflow_id]
        console.print(f"\n  [bold green]returned:[/] {root.result!r}")


def cmd_run(args):
    """Start a workflow and run it to completion."""
    file_path, wf_name = _parse_target(args.target)
    registry = _load_registry(file_path)
    if wf_name not in registry:
        console.print(f"[red]Unknown workflow:[/] {wf_name}")
        console.print(f"Available in {file_path}: {', '.join(registry)}")
        sys.exit(1)

    parsed_args = [_parse_cli_arg(a) for a in args.args]
    trace = getattr(args, "trace", False)
    store = Store(DB_PATH)
    engine = Engine(
        EngineConfig(
            workflows_registry=registry,
            on_events=lambda events: print_events(events, trace=trace),
        )
    )
    workdir = os.path.abspath(args.workdir)
    os.makedirs(workdir, exist_ok=True)
    execution_id = engine.start(
        store, wf_name, parsed_args, source_file=file_path, workdir=workdir
    )

    console.print(
        f"[bold]Running[/] [cyan]{execution_id}[/] {wf_name}({', '.join(args.args)})"
    )
    _run_loop(store, engine, execution_id, trace=trace)
    store.close()


def cmd_continue(args):
    """Continue a paused execution to completion."""
    store = Store(DB_PATH)
    registry, state = _load_registry_for_execution(store, args.id)
    if state.finished:
        store.close()
        console.print(f"[yellow]Execution {args.id} already finished[/]")
        sys.exit(1)

    trace = getattr(args, "trace", False)
    engine = Engine(
        EngineConfig(
            workflows_registry=registry,
            on_events=lambda events: print_events(events, trace=trace),
        )
    )
    console.print(f"[bold]Continuing[/] [cyan]{args.id}[/]")
    _run_loop(store, engine, args.id, trace=trace)
    store.close()


def cmd_list(args):
    store = Store(DB_PATH)
    all_execs = store.list_executions()
    store.close()

    if not all_execs:
        console.print("[dim]No executions yet.[/]")
        return

    table = Table(show_lines=False)
    table.add_column("execution_id", style="cyan")
    table.add_column("workflow")
    table.add_column("workflows", justify="right")
    table.add_column("status")

    for exec_id, state, _created_at in all_execs:
        n_wf = len(state.workflows)
        root_name = state.workflows[state.root_workflow_id].name
        if state.finished:
            status = Text("finished", style="dim")
        else:
            status = Text("running", style="green")
        table.add_row(exec_id, root_name, str(n_wf), status)

    console.print(table)


def cmd_task(args):
    ts = TaskStore(TASKS_DB_PATH)
    action = args.task_action

    if action == "create":
        labels = {}
        if args.label:
            for l in args.label:
                k, _, v = l.partition("=")
                labels[k] = v
        task = ts.create(
            name=args.name,
            description=args.description or "",
            labels=labels,
            color=args.color or "",
        )
        console.print(
            f"[bold green]Created[/] [cyan]{task['task_id']}[/] {task['name']}"
        )

    elif action == "list":
        tasks = ts.list()
        if not tasks:
            console.print("[dim]No tasks yet.[/]")
            ts.close()
            return
        table = Table(show_lines=False)
        table.add_column("task_id", style="cyan")
        table.add_column("name")
        table.add_column("status")
        table.add_column("description", style="dim")
        table.add_column("labels", style="dim")
        for t in tasks:
            labels_str = (
                ", ".join(f"{k}={v}" for k, v in t["labels"].items())
                if t["labels"]
                else ""
            )
            status_style = "green" if t["status"] == "pending" else "dim"
            status_text = t["status"]
            if t["needs_input"]:
                status_text += " [amber]input[/]"
            table.add_row(
                t["task_id"][:12],
                t["name"],
                Text(status_text, style=status_style),
                t["description"][:40] if t["description"] else "",
                labels_str,
            )
        console.print(table)

    elif action == "show":
        task = ts.find_by_prefix(args.id)
        for k, v in task.items():
            console.print(f"  [bold]{k}[/]: {v}")

    elif action == "update":
        kwargs = {}
        if args.name is not None:
            kwargs["name"] = args.name
        if args.description is not None:
            kwargs["description"] = args.description
        if args.color is not None:
            kwargs["color"] = args.color
        if getattr(args, "status", None) is not None:
            kwargs["status"] = args.status
        if args.label:
            task = ts.find_by_prefix(args.id)
            labels = dict(task["labels"])
            for l in args.label:
                k, _, v = l.partition("=")
                if v == "":
                    labels.pop(k, None)
                else:
                    labels[k] = v
            kwargs["labels"] = labels
        task = ts.update(ts.find_by_prefix(args.id)["task_id"], **kwargs)
        console.print(
            f"[bold green]Updated[/] [cyan]{task['task_id'][:12]}[/] {task['name']}"
        )

    elif action == "delete":
        task = ts.find_by_prefix(args.id)
        ts.delete(task["task_id"])
        console.print(
            f"[bold red]Deleted[/] [cyan]{task['task_id'][:12]}[/] {task['name']}"
        )

    ts.close()


def main():
    parser = argparse.ArgumentParser(description="Durable workflow executor")
    sub = parser.add_subparsers(dest="command", required=True)

    p_start = sub.add_parser("start", help="Start a new workflow execution")
    p_start.add_argument("target", help="file.py:workflow_name")
    p_start.add_argument("args", nargs="*", help="JSON-encoded arguments")
    p_start.add_argument(
        "-w",
        "--workdir",
        default=".workspace",
        help="Working directory for the root workflow (default: .workspace)",
    )

    p_step = sub.add_parser("step", help="Advance all active workflows one tick")
    p_step.add_argument("id", help="Execution ID")
    p_step.add_argument("--trace", action="store_true", help="Show all events")

    p_status = sub.add_parser("status", help="Show execution status")
    p_status.add_argument("id", help="Execution ID")

    p_events = sub.add_parser("events", help="Show inbox/outbox events")
    p_events.add_argument("id", help="Execution ID")

    p_inspect = sub.add_parser("inspect", help="Inspect full execution state")
    p_inspect.add_argument("id", help="Execution ID")

    p_conv = sub.add_parser("conv", help="Show conversations for an execution")
    p_conv.add_argument("id", help="Execution ID")
    p_conv.add_argument(
        "conversation_id", nargs="?", help="Conversation ID prefix (default: all)"
    )

    p_run = sub.add_parser(
        "run", help="Start and run a workflow to completion (interactive)"
    )
    p_run.add_argument("target", help="file.py:workflow_name")
    p_run.add_argument("args", nargs="*", help="JSON-encoded arguments")
    p_run.add_argument(
        "-w",
        "--workdir",
        default=".workspace",
        help="Working directory (default: .workspace)",
    )
    p_run.add_argument(
        "--trace", action="store_true", help="Show all events (not just user-facing)"
    )

    p_continue = sub.add_parser(
        "continue", help="Continue a paused execution to completion"
    )
    p_continue.add_argument("id", help="Execution ID")
    p_continue.add_argument("--trace", action="store_true", help="Show all events")

    sub.add_parser("list", help="List all executions")

    p_task = sub.add_parser("task", help="Manage tasks")
    task_sub = p_task.add_subparsers(dest="task_action", required=True)

    p_tc = task_sub.add_parser("create", help="Create a task")
    p_tc.add_argument("name", help="Task name")
    p_tc.add_argument("-d", "--description", help="Description")
    p_tc.add_argument("-l", "--label", action="append", help="Label as key=value")
    p_tc.add_argument("--color", help="Color")

    task_sub.add_parser("list", help="List all tasks")

    p_ts = task_sub.add_parser("show", help="Show task details")
    p_ts.add_argument("id", help="Task ID or prefix")

    p_tu = task_sub.add_parser("update", help="Update a task")
    p_tu.add_argument("id", help="Task ID or prefix")
    p_tu.add_argument("-n", "--name", help="New name")
    p_tu.add_argument("-d", "--description", help="New description")
    p_tu.add_argument("-s", "--status", choices=["pending", "finished"], help="Status")
    p_tu.add_argument(
        "-l",
        "--label",
        action="append",
        help="Set label key=value (empty value removes)",
    )
    p_tu.add_argument("--color", help="Color")

    p_td = task_sub.add_parser("delete", help="Delete a task")
    p_td.add_argument("id", help="Task ID or prefix")

    p_web = sub.add_parser("web", help="Start the web UI")
    p_web.add_argument("--port", type=int, default=8080, help="Port (default: 8080)")
    p_web.add_argument("--host", default="0.0.0.0", help="Host (default: 0.0.0.0)")

    args = parser.parse_args()
    cmds = {
        "start": cmd_start,
        "step": cmd_step,
        "status": cmd_status,
        "events": cmd_events,
        "inspect": cmd_inspect,
        "conv": cmd_conv,
        "run": cmd_run,
        "continue": cmd_continue,
        "list": cmd_list,
        "task": cmd_task,
        "web": cmd_web,
    }
    cmds[args.command](args)


def cmd_web(args):
    """Start the web UI."""
    import uvicorn

    os.environ["TURBO_DB"] = DB_PATH
    console.print(f"[bold]Starting web UI[/] on http://{args.host}:{args.port}")
    uvicorn.run("web.server:app", host=args.host, port=args.port, reload=False)


if __name__ == "__main__":
    main()
