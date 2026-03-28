import argparse
import json
import os
import uuid
import sys

from workflows import workflow, wait, wait_all, wait_any, Engine, Store


DB_PATH = os.path.join(os.path.dirname(__file__), 'executions.db')


# ---- example workflows ----

@workflow
def accumulator(n):
    """Accumulate i+s for nested loops."""
    total = 0
    for i in range(n):
        for s in range(n):
            yield total
            total = total + i + s
    yield total
    return total


@workflow
def double_accumulate(n):
    """Run two accumulators concurrently, wait for both with wait_all."""
    a = accumulator(n)
    b = accumulator(n)
    yield 'both started'
    first, second = yield wait_all([a, b])
    yield f'results: {first}, {second}'
    return first + second


@workflow
def race(n):
    """Launch accumulators of different sizes, return first to finish."""
    children = [accumulator(i + 1) for i in range(n)]
    yield f'racing {n} children'
    winner_id, result = yield wait_any(children)
    yield f'winner: #{winner_id} with result {result}'
    return result


@workflow
def pipeline(steps):
    """Launch accumulators concurrently, wait for each in order."""
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
    exec_id = str(uuid.uuid4())[:8]

    engine, outputs = Engine.start(WORKFLOWS, wf_name, parsed_args)

    store = Store(DB_PATH)
    store.save(exec_id, engine.state)
    store.close()

    print(f'Started execution {exec_id}')
    print(f'  workflow: {wf_name}({", ".join(args.args)})')
    _print_outputs(engine.state, outputs)


def cmd_step(args):
    store = Store(DB_PATH)
    state = store.load(args.id)

    if state.finished:
        store.close()
        print(f'Execution {args.id} already finished')
        sys.exit(1)

    engine = Engine(state, WORKFLOWS)
    send_val = json.loads(args.value) if args.value is not None else None

    try:
        outputs, finished = engine.step(send_val)
    except Exception as e:
        store.close()
        print(f'  step {state.step} → error: {e}', file=sys.stderr)
        sys.exit(1)

    store.save(args.id, engine.state)
    store.close()

    _print_outputs(engine.state, outputs)

    if finished:
        root = state.workflows[state.root]
        print(f'  → returned: {root.result!r}')


def _print_outputs(state, outputs):
    for wf_id, wf_name, val in outputs:
        print(f'  step {state.step} [{wf_name}#{wf_id}] → {val!r}')


def cmd_status(args):
    store = Store(DB_PATH)
    state = store.load(args.id)
    store.close()

    print(f'Execution {args.id}')
    print(f'  step:     {state.step}')
    print(f'  finished: {state.finished}')
    print(f'  workflows ({len(state.workflows)}):')
    for wf_id, wf in sorted(state.workflows.items(), key=lambda x: x[0]):
        extra = ''
        if wf.status == 'waiting':
            extra = f' ({wf.wait_mode} of {", ".join("#"+d for d in wf.wait_deps)})'
        if wf.status == 'finished':
            extra = f' result={wf.result!r}'
        root = ' [root]' if wf_id == state.root else ''
        print(f'    #{wf_id} {wf.name}{root}  {wf.status}{extra}')


def cmd_list(args):
    store = Store(DB_PATH)
    all_execs = store.list_all()
    store.close()

    if not all_execs:
        print('No executions yet.')
        return
    for exec_id, state in all_execs:
        status = 'finished' if state.finished else f'step {state.step}'
        n_wf = len(state.workflows)
        root_name = state.workflows[state.root].name
        print(f'  {exec_id}  {root_name} ({n_wf} workflows)  [{status}]')


def main():
    parser = argparse.ArgumentParser(description='Durable workflow executor')
    sub = parser.add_subparsers(dest='command', required=True)

    p_start = sub.add_parser('start', help='Start a new workflow execution')
    p_start.add_argument('workflow', help='Workflow name')
    p_start.add_argument('args', nargs='*', help='JSON-encoded arguments')

    p_step = sub.add_parser('step', help='Advance all active workflows one tick')
    p_step.add_argument('id', help='Execution ID')
    p_step.add_argument('value', nargs='?', help='JSON-encoded value to send to root')

    p_status = sub.add_parser('status', help='Show execution status')
    p_status.add_argument('id', help='Execution ID')

    sub.add_parser('list', help='List all executions')

    args = parser.parse_args()
    {'start': cmd_start, 'step': cmd_step, 'status': cmd_status, 'list': cmd_list}[args.command](args)


if __name__ == '__main__':
    main()
