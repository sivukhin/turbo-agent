import argparse
import json
import os
import sys

from rich.console import Console
from rich.table import Table
from rich.text import Text

from workflows import workflow, wait, wait_all, wait_any, sleep, Engine, Store


DB_PATH = os.path.join(os.path.dirname(__file__), 'executions.db')
console = Console()


# ---- example workflows ----

@workflow
def accumulator(n):
    total = 0
    for i in range(n):
        for s in range(n):
            yield total
            total = total + i + s
    yield total
    return total


@workflow
def double_accumulate(n):
    a = accumulator(n)
    b = accumulator(n)
    yield 'both started'
    first, second = yield wait_all([a, b])
    yield f'results: {first}, {second}'
    return first + second


@workflow
def race(n):
    children = [accumulator(i + 1) for i in range(n)]
    yield f'racing {n} children'
    results = yield wait_any(children)
    finished = [(i, r) for i, (done, r) in enumerate(results) if done]
    yield f'finished: {finished}'
    return results


@workflow
def pipeline(steps):
    children = []
    for i in range(steps):
        children.append(accumulator(i + 1))
    yield f'launched {steps} children'
    results = []
    for i, child in enumerate(children):
        result = yield wait(child)
        results.append(result)
        yield f'stage {i} done: {result}'
    return sum(results)


WORKFLOWS = {
    'accumulator': accumulator,
    'double_accumulate': double_accumulate,
    'race': race,
    'pipeline': pipeline,
}


def _status_style(status):
    return {'running': 'green', 'waiting': 'yellow', 'finished': 'dim'}[status]


def _category_style(category):
    return {'inbox': 'cyan', 'outbox': 'magenta'}[category]


def cmd_start(args):
    wf_name = args.workflow
    if wf_name not in WORKFLOWS:
        console.print(f'[red]Unknown workflow:[/] {wf_name}')
        console.print(f'Available: {", ".join(WORKFLOWS)}')
        sys.exit(1)

    parsed_args = [json.loads(a) for a in args.args]
    store = Store(DB_PATH)
    engine = Engine(WORKFLOWS)
    execution_id = engine.start(store, wf_name, parsed_args)

    state, _ = store.load_state(execution_id)
    outbox = store.read_outbox(execution_id)
    store.close()

    console.print(f'[bold]Started execution[/] [cyan]{execution_id}[/]')
    console.print(f'  workflow: [bold]{wf_name}[/]({", ".join(args.args)})')
    _print_events(outbox)


def cmd_step(args):
    store = Store(DB_PATH)
    state, _ = store.load_state(args.id)
    if state.finished:
        store.close()
        console.print(f'[yellow]Execution {args.id} already finished[/]')
        sys.exit(1)

    outbox_before = store.read_outbox(args.id)
    last_before = outbox_before[-1].event_id if outbox_before else 0

    engine = Engine(WORKFLOWS)
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
    for event in events:
        if event.type == 'workflow_yielded':
            wf_id = (event.workflow_id or '?')[:8]
            val = event.payload.get('value')
            console.print(f'  [dim]{wf_id}[/] [bold]→[/] {val!r}')


def cmd_status(args):
    store = Store(DB_PATH)
    state, last_event = store.load_state(args.id)
    store.close()

    console.print(f'[bold]Execution[/] [cyan]{args.id}[/]')
    finished_text = '[green]yes[/]' if state.finished else '[yellow]no[/]'
    console.print(f'  finished:     {finished_text}')
    console.print(f'  last_event:   {last_event}')

    console.print(f'  [bold]workflows[/] ({len(state.workflows)}):')
    for wf_id, wf in sorted(state.workflows.items()):
        style = _status_style(wf.status)
        root = ' [bold][root][/]' if wf_id == state.root_workflow_id else ''
        extra = ''
        if wf.status == 'finished':
            extra = f' result={wf.result!r}'
        console.print(f'    [dim]{wf_id}[/] {wf.name}{root}  [{style}]{wf.status}[/]{extra}')

    if state.handlers:
        console.print(f'  [bold]handlers[/] ({len(state.handlers)}):')
        for wf_id, hs in state.handlers.items():
            console.print(f'    [dim]{wf_id}[/] [yellow]{hs.handler_type}[/]  {hs.state}')


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
        payload_str = _format_payload(event.type, event.payload)
        table.add_row(
            str(event.event_id),
            Text(event.category, style=cat_style),
            event.type,
            wf_id,
            payload_str,
        )

    console.print(table)


def _format_payload(event_type, payload):
    if event_type == 'workflow_yielded':
        return repr(payload.get('value', ''))
    if event_type == 'workflow_finished':
        return f'result={payload.get("result")!r}'
    if event_type == 'tick':
        return ''
    return repr(payload)


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
    p_start.add_argument('workflow', help='Workflow name')
    p_start.add_argument('args', nargs='*', help='JSON-encoded arguments')

    p_step = sub.add_parser('step', help='Advance all active workflows one tick')
    p_step.add_argument('id', help='Execution ID')

    p_status = sub.add_parser('status', help='Show execution status')
    p_status.add_argument('id', help='Execution ID')

    p_events = sub.add_parser('events', help='Show inbox/outbox events for an execution')
    p_events.add_argument('id', help='Execution ID')

    sub.add_parser('list', help='List all executions')

    args = parser.parse_args()
    cmds = {
        'start': cmd_start, 'step': cmd_step, 'status': cmd_status,
        'events': cmd_events, 'list': cmd_list,
    }
    cmds[args.command](args)


if __name__ == '__main__':
    main()
