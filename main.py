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
    engine = Engine(registry)
    workdir = os.path.abspath(args.workdir)
    os.makedirs(workdir, exist_ok=True)
    execution_id = engine.start(store, wf_name, parsed_args, source_file=file_path,
                                workdir=workdir)

    outbox = store.read_outbox(execution_id)
    store.close()

    console.print(f'[bold]Started execution[/] [cyan]{execution_id}[/]')
    console.print(f'  workflow: [bold]{wf_name}[/]({", ".join(args.args)})')
    _print_events(outbox)


def cmd_step(args):
    store = Store(DB_PATH)
    registry, state = _load_registry_for_execution(store, args.id)
    if state.finished:
        store.close()
        console.print(f'[yellow]Execution {args.id} already finished[/]')
        sys.exit(1)

    outbox_before = store.read_outbox(args.id)
    last_before = outbox_before[-1].event_id if outbox_before else 0

    engine = Engine(registry)
    try:
        engine.step(store, args.id)
    except Exception as e:
        store.close()
        console.print(f'[red]error:[/] {e}', highlight=False)
        sys.exit(1)

    state, _ = store.load_state(args.id)
    new_outbox = store.read_outbox(args.id, after_event_id=last_before)
    store.close()

    _print_events(new_outbox)
    if state.finished:
        root = state.workflows[state.root_workflow_id]
        console.print(f'  [bold green]returned:[/] {root.result!r}')


def _print_events(events):
    from workflows.events import WorkflowYielded
    for event in events:
        if isinstance(event.payload, WorkflowYielded):
            wf_id = (event.workflow_id or '?')[:8]
            console.print(f'  [dim]{wf_id}[/] [bold]→[/] {event.payload.value!r}')


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

    sub.add_parser('list', help='List all executions')

    args = parser.parse_args()
    cmds = {
        'start': cmd_start, 'step': cmd_step, 'status': cmd_status,
        'events': cmd_events, 'inspect': cmd_inspect, 'list': cmd_list,
    }
    cmds[args.command](args)


if __name__ == '__main__':
    main()
