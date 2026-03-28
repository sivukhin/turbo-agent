import argparse
import json
import os
import sys

from workflows import workflow, wait, wait_all, wait_any, Engine, Store


DB_PATH = os.path.join(os.path.dirname(__file__), 'executions.db')


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


def cmd_start(args):
    wf_name = args.workflow
    if wf_name not in WORKFLOWS:
        print(f'Unknown workflow: {wf_name}')
        print(f'Available: {", ".join(WORKFLOWS)}')
        sys.exit(1)

    parsed_args = [json.loads(a) for a in args.args]
    store = Store(DB_PATH)
    engine = Engine(WORKFLOWS)
    execution_id = engine.start(store, wf_name, parsed_args)

    state, _ = store.load_state(execution_id)
    outbox = store.read_outbox(execution_id)
    store.close()

    print(f'Started execution {execution_id}')
    print(f'  workflow: {wf_name}({", ".join(args.args)})')
    _print_outbox(outbox)


def cmd_step(args):
    store = Store(DB_PATH)
    state, _ = store.load_state(args.id)
    if state.finished:
        store.close()
        print(f'Execution {args.id} already finished')
        sys.exit(1)

    outbox_before = store.read_outbox(args.id)
    last_before = outbox_before[-1].event_id if outbox_before else 0

    engine = Engine(WORKFLOWS)
    try:
        engine.step(store, args.id)
    except Exception as e:
        store.close()
        print(f'  error: {e}', file=sys.stderr)
        sys.exit(1)

    state, _ = store.load_state(args.id)
    new_outbox = store.read_outbox(args.id, after_event_id=last_before)
    store.close()

    _print_outbox(new_outbox)
    if state.finished:
        root = state.workflows[state.root_workflow_id]
        print(f'  → returned: {root.result!r}')


def _print_outbox(events):
    for event in events:
        if event.type == 'workflow_yielded':
            wf_id = event.workflow_id or '?'
            val = event.payload.get('value')
            print(f'  [{wf_id}] → {val!r}')


def cmd_status(args):
    store = Store(DB_PATH)
    state, last_event = store.load_state(args.id)
    store.close()

    print(f'Execution {args.id}')
    print(f'  finished:       {state.finished}')
    print(f'  last_event_id:    {last_event}')
    print(f'  workflows ({len(state.workflows)}):')
    for wf_id, wf in sorted(state.workflows.items()):
        extra = ''
        if wf.status == 'finished':
            extra = f' result={wf.result!r}'
        root = ' [root]' if wf_id == state.root_workflow_id else ''
        print(f'    {wf_id} {wf.name}{root}  {wf.status}{extra}')
    if state.handlers:
        print(f'  handlers ({len(state.handlers)}):')
        for wf_id, hs in state.handlers.items():
            print(f'    {wf_id} → {hs.handler_type}  state={hs.state}')


def cmd_list(args):
    store = Store(DB_PATH)
    all_execs = store.list_executions()
    store.close()

    if not all_execs:
        print('No executions yet.')
        return
    for exec_id, state in all_execs:
        status = 'finished' if state.finished else 'running'
        n_wf = len(state.workflows)
        root_name = state.workflows[state.root_workflow_id].name
        print(f'  {exec_id}  {root_name} ({n_wf} workflows)  [{status}]')


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

    sub.add_parser('list', help='List all executions')

    args = parser.parse_args()
    {'start': cmd_start, 'step': cmd_step, 'status': cmd_status, 'list': cmd_list}[args.command](args)


if __name__ == '__main__':
    main()
