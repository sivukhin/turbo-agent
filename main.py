import argparse
import json
import os
import sys

from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

from workflows import Engine, Store, load_workflows_from_file


DB_PATH = os.path.join(os.path.dirname(__file__), 'executions.db')
console = Console()


def _parse_target(target: str) -> tuple[str, str]:
    """Parse 'file.py:function' into (file_path, function_name)."""
    if ':' not in target:
        console.print(f'[red]Invalid target:[/] {target}')
        console.print('Expected format: [bold]path/to/file.py:workflow_name[/]')
        sys.exit(1)
    file_path, func_name = target.rsplit(':', 1)
    return file_path, func_name


def _load_registry(file_path: str) -> dict:
    return load_workflows_from_file(file_path)


def _load_registry_for_execution(store, execution_id):
    """Load the registry from the file stored in execution metadata."""
    state, _ = store.load_state(execution_id)
    file_path = state.source_file
    if not file_path:
        console.print(f'[red]Execution {execution_id} has no source file[/]')
        sys.exit(1)
    return load_workflows_from_file(file_path), state


def _status_style(status):
    return {'running': 'green', 'waiting': 'yellow', 'finished': 'dim'}[status]


def _category_style(category):
    return {'inbox': 'cyan', 'outbox': 'magenta'}[category]


def cmd_start(args):
    file_path, wf_name = _parse_target(args.target)
    registry = _load_registry(file_path)
    if wf_name not in registry:
        console.print(f'[red]Unknown workflow:[/] {wf_name}')
        console.print(f'Available in {file_path}: {", ".join(registry)}')
        sys.exit(1)

    parsed_args = [json.loads(a) for a in args.args]
    store = Store(DB_PATH)
    engine = Engine.from_registry(registry)
    workdir = os.path.abspath(args.workdir)
    os.makedirs(workdir, exist_ok=True)
    execution_id = engine.start(store, wf_name, parsed_args, source_file=file_path,
                                workdir=workdir)

    inbox = store.read_inbox(execution_id)
    outbox = store.read_outbox(execution_id)
    store.close()

    console.print(f'[bold]Started execution[/] [cyan]{execution_id}[/]')
    console.print(f'  workflow: [bold]{wf_name}[/]({", ".join(args.args)})')
    _print_step_events(inbox, outbox)


def cmd_step(args):
    store = Store(DB_PATH)
    registry, state = _load_registry_for_execution(store, args.id)
    if state.finished:
        store.close()
        console.print(f'[yellow]Execution {args.id} already finished[/]')
        sys.exit(1)

    inbox_before = store.read_inbox(args.id)
    outbox_before = store.read_outbox(args.id)
    last_inbox = inbox_before[-1].event_id if inbox_before else 0
    last_outbox = outbox_before[-1].event_id if outbox_before else 0

    engine = Engine.from_registry(registry)
    try:
        engine.step(store, args.id)
    except Exception as e:
        store.close()
        console.print(f'[red]error:[/] {e}', highlight=False)
        sys.exit(1)

    state, _ = store.load_state(args.id)
    new_inbox = store.read_inbox(args.id, after_event_id=last_inbox)
    new_outbox = store.read_outbox(args.id, after_event_id=last_outbox)
    store.close()

    _print_step_events(new_inbox, new_outbox)
    if state.finished:
        root = state.workflows[state.root_workflow_id]
        console.print(f'  [bold green]returned:[/] {root.result!r}')


def _print_step_events(inbox, outbox):
    all_events = sorted(inbox + outbox, key=lambda e: e.event_id)
    for event in all_events:
        wf_id = (event.workflow_id or '-')[:8]
        cat_style = _category_style(event.category)
        payload_str = _format_payload(event.payload)
        console.print(
            f'  [{cat_style}]{event.category:<6}[/] [dim]{wf_id}[/] '
            f'[bold]{event.type}[/] {payload_str}'
        )


def cmd_status(args):
    store = Store(DB_PATH)
    state, last_event = store.load_state(args.id)
    store.close()

    finished_text = '[green]yes[/]' if state.finished else '[yellow]no[/]'
    root_tree = Tree(
        f'[bold]Execution[/] [cyan]{args.id}[/]  '
        f'finished={finished_text}  [dim]{state.source_file}[/]'
    )

    children_of = {}
    for wf_id, wf in state.workflows.items():
        children_of.setdefault(wf.parent_workflow_id, []).append(wf_id)

    def _render(tree_node, wf_id):
        wf = state.workflows[wf_id]
        style = _status_style(wf.status)
        root_tag = ' [bold cyan][root][/]' if wf_id == state.root_workflow_id else ''
        extra = ''
        if wf.status == 'finished':
            extra = f'  result={wf.result!r}'
        if wf_id in state.handlers:
            extra += f'  [yellow]({state.handlers[wf_id].handler_type})[/]'
        node = tree_node.add(
            f'[bold]{wf.name}[/]{root_tag}  [dim]{wf_id[:8]}[/]  [{style}]{wf.status}[/]{extra}'
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
        console.print('[dim]No events.[/]')
        return

    table = Table(title=f'Events for {args.id}', show_lines=False)
    table.add_column('#', style='dim', width=5)
    table.add_column('category', width=7)
    table.add_column('type', width=20)
    table.add_column('workflow', style='dim', width=14)
    table.add_column('payload')

    for event in all_events:
        cat_style = _category_style(event.category)
        wf_id = (event.workflow_id or '-')[:12]
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
        return f'result={payload.result!r}'
    if isinstance(payload, ev.ShellRequest):
        return f'$ {payload.command}'
    if isinstance(payload, ev.ShellResult):
        parts = [f'exit={payload.exit_code}']
        out = payload.stdout.strip()
        err = payload.stderr.strip()
        if out:
            parts.append(f'stdout={out!r}')
        if err:
            parts.append(f'stderr={err!r}')
        return ', '.join(parts)
    if isinstance(payload, ev.FileReadRequest):
        return f'read {payload.path}'
    if isinstance(payload, ev.FileReadResult):
        content = payload.content
        if len(content) > 80:
            content = content[:77] + '...'
        return f'{payload.path}: {content!r}'
    if isinstance(payload, ev.FileWriteRequest):
        content = payload.content
        if len(content) > 80:
            content = content[:77] + '...'
        return f'write {payload.path}: {content!r}'
    if isinstance(payload, ev.FileWriteResult):
        return f'{payload.path} ({payload.size} bytes)'
    if isinstance(payload, ev.WaitStarted):
        deps = ', '.join(d[:8] for d in payload.deps)
        return f'{payload.mode}({deps})'
    if isinstance(payload, ev.SleepStarted):
        return f'{payload.seconds}s (wake_at={payload.wake_at})'
    if isinstance(payload, ev.WorkflowSpawned):
        parent = payload.parent_workflow_id[:8] if payload.parent_workflow_id else '-'
        return f'{payload.name}({payload.args}) parent={parent} storage={payload.storage_mode}'
    if isinstance(payload, ev.ConvAppendRequest):
        content = payload.content[:60] + ('...' if len(payload.content) > 60 else '')
        return f'{payload.role}: {content!r}'
    if isinstance(payload, ev.ConvAppendResult):
        return f'msg={payload.message_id[:12]} layer={payload.layer}'
    if isinstance(payload, ev.ConvReadRequest):
        return f'conv={payload.conversation_id[:8]}'
    if isinstance(payload, ev.ConvReadResult):
        return f'{payload.count} messages'
    if isinstance(payload, ev.ConvSearchRequest):
        return f'pattern={payload.pattern!r}'
    if isinstance(payload, ev.ConvSearchResult):
        return f'{payload.count} matches'
    if isinstance(payload, ev.ConvGetRequest):
        return f'{len(payload.message_refs)} refs'
    if isinstance(payload, ev.ConvGetResult):
        return f'{payload.count} messages'
    if isinstance(payload, ev.ConvReplaceWithRequest):
        return f'{len(payload.new_messages)} new msgs'
    if isinstance(payload, ev.ConvReplaceWithResult):
        return f'layer={payload.new_layer} {len(payload.new_message_refs)} msgs'
    if isinstance(payload, ev.LlmRequest):
        n_msgs = len(payload.messages)
        tools = f', {len(payload.tools)} tools' if payload.tools else ''
        return f'{payload.model} ({n_msgs} msgs{tools}, T={payload.temperature})'
    if isinstance(payload, ev.LlmResponse):
        texts = [b['text'][:60] for b in payload.content if b.get('type') == 'text']
        tool_calls = [b['name'] for b in payload.content if b.get('type') == 'tool_use']
        parts = []
        if texts:
            parts.append(f'text={texts[0]!r}{"..." if len(texts[0]) >= 60 else ""}')
        if tool_calls:
            parts.append(f'tools=[{", ".join(tool_calls)}]')
        if payload.stop_reason:
            parts.append(f'stop={payload.stop_reason}')
        if payload.usage:
            parts.append(f'tokens={payload.usage.get("input_tokens",0)}+{payload.usage.get("output_tokens",0)}')
        return ', '.join(parts)
    return repr(payload)


def cmd_inspect(args):
    store = Store(DB_PATH)
    state, last_event = store.load_state(args.id)
    store.close()

    finished_text = '[green]yes[/]' if state.finished else '[yellow]no[/]'
    root_tree = Tree(
        f'[bold]Execution[/] [cyan]{args.id}[/]  '
        f'finished={finished_text}  last_event={last_event}  '
        f'[dim]{state.source_file}[/]'
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
        root_tag = ' [bold cyan][root][/]' if wf_id == state.root_workflow_id else ''
        label = f'[bold]{wf.name}[/]{root_tag}  [dim]{wf_id}[/]  [{style}]{wf.status}[/]'
        wf_node = tree_node.add(label)

        wf_node.add(f'[dim]args:[/] {wf.args!r}')

        if wf.status == 'finished':
            wf_node.add(f'[dim]result:[/] [green]{wf.result!r}[/]')

        if wf.checkpoint:
            cp = wf.checkpoint
            if cp.get('locals'):
                locals_node = wf_node.add('[dim]locals:[/]')
                for k, v in sorted(cp['locals'].items()):
                    locals_node.add(f'[bold]{k}[/] = {v!r}')
            if cp.get('drain'):
                wf_node.add(f'[dim]stack:[/] {cp["drain"]!r}')
            if cp.get('yield_idx') is not None:
                wf_node.add(f'[dim]yield_idx:[/] {cp["yield_idx"]}')
            if cp.get('yv') is not None:
                wf_node.add(f'[dim]yielded:[/] {cp["yv"]!r}')

        handler = state.handlers.get(wf_id)
        if handler:
            h_node = wf_node.add(f'[dim]handler:[/] [yellow]{handler.handler_type}[/]')
            for k, v in sorted(handler.state.items()):
                h_node.add(f'[bold]{k}[/] = {v!r}')

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
            (f'{args.conversation_id}%',),
        )
        rows = cur.fetchall()
        if not rows:
            console.print(f'[red]No conversation matching {args.conversation_id}[/]')
            store.close()
            sys.exit(1)
        for (conv_id,) in rows:
            # Find workflow name from active state or events
            wf_name, wf_id = _find_conv_owner(state, store, args.id, conv_id)
            _print_conversation(store, conv_id, wf_name, wf_id)
            console.print()
    else:
        # Show all conversations: active workflows + all from conversation_refs
        cur = store.conn.cursor()
        cur.execute("SELECT conversation_id FROM conversation_refs ORDER BY conversation_id")
        all_convs = [row[0] for row in cur.fetchall()]
        for conv_id in all_convs:
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
                return '(pruned)', event.workflow_id or '?'
    return '(unknown)', '?'


def _print_conversation(store, conversation_id, wf_name, wf_id):
    messages = store.conv_read_messages(conversation_id)
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
        f'[bold]{wf_name}[/] [dim]{wf_id[:8]}[/]  '
        f'conv=[cyan]{conversation_id[:8]}[/]  '
        f'{len(messages)} messages'
        + (f'  [dim]parent={parent[:8]}[/]' if parent else '')
    )

    if not messages:
        console.print('  [dim](empty)[/]')
        return

    role_style = {
        'user': 'green',
        'assistant': 'blue',
        'system': 'yellow',
        'tool_result': 'magenta',
    }

    table = Table(show_lines=False, show_header=True, padding=(0, 1))
    table.add_column('#', style='dim', width=4)
    table.add_column('role', width=10)
    table.add_column('content')
    table.add_column('ref', style='dim', width=12)

    for i, msg in enumerate(messages):
        style = role_style.get(msg.role, 'white')
        content = msg.content
        if len(content) > 120:
            content = content[:117] + '...'
        # Show if message came from parent conversation
        from_parent = msg.ref.conversation_id != conversation_id
        parent_tag = ' [dim](parent)[/]' if from_parent else ''
        table.add_row(
            str(i),
            Text(msg.role, style=style),
            content + parent_tag,
            msg.ref.message_id[:10],
        )

    console.print(table)


def cmd_list(args):
    store = Store(DB_PATH)
    all_execs = store.list_executions()
    store.close()

    if not all_execs:
        console.print('[dim]No executions yet.[/]')
        return

    table = Table(show_lines=False)
    table.add_column('execution_id', style='cyan')
    table.add_column('workflow')
    table.add_column('workflows', justify='right')
    table.add_column('status')

    for exec_id, state in all_execs:
        n_wf = len(state.workflows)
        root_name = state.workflows[state.root_workflow_id].name
        if state.finished:
            status = Text('finished', style='dim')
        else:
            status = Text('running', style='green')
        table.add_row(exec_id, root_name, str(n_wf), status)

    console.print(table)


def main():
    parser = argparse.ArgumentParser(description='Durable workflow executor')
    sub = parser.add_subparsers(dest='command', required=True)

    p_start = sub.add_parser('start', help='Start a new workflow execution')
    p_start.add_argument('target', help='file.py:workflow_name')
    p_start.add_argument('args', nargs='*', help='JSON-encoded arguments')
    p_start.add_argument('-w', '--workdir', default='.workspace',
                         help='Working directory for the root workflow (default: .workspace)')

    p_step = sub.add_parser('step', help='Advance all active workflows one tick')
    p_step.add_argument('id', help='Execution ID')

    p_status = sub.add_parser('status', help='Show execution status')
    p_status.add_argument('id', help='Execution ID')

    p_events = sub.add_parser('events', help='Show inbox/outbox events')
    p_events.add_argument('id', help='Execution ID')

    p_inspect = sub.add_parser('inspect', help='Inspect full execution state')
    p_inspect.add_argument('id', help='Execution ID')

    p_conv = sub.add_parser('conv', help='Show conversations for an execution')
    p_conv.add_argument('id', help='Execution ID')
    p_conv.add_argument('conversation_id', nargs='?', help='Conversation ID prefix (default: all)')

    sub.add_parser('list', help='List all executions')

    args = parser.parse_args()
    cmds = {
        'start': cmd_start, 'step': cmd_step, 'status': cmd_status,
        'events': cmd_events, 'inspect': cmd_inspect, 'conv': cmd_conv,
        'list': cmd_list,
    }
    cmds[args.command](args)


if __name__ == '__main__':
    main()
