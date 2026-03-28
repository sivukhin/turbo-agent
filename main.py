import argparse
import json
import os
import pickle
import uuid
import sys

from workflows import workflow, wait, wait_all, wait_any, Engine


EXECUTIONS_DIR = os.path.join(os.path.dirname(__file__), '.executions')


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


def _exec_path(exec_id):
    return os.path.join(EXECUTIONS_DIR, f'{exec_id}.pkl')


def _load_state(exec_id):
    with open(_exec_path(exec_id), 'rb') as f:
        return pickle.load(f)


def _save_state(exec_id, state):
    os.makedirs(EXECUTIONS_DIR, exist_ok=True)
    with open(_exec_path(exec_id), 'wb') as f:
        pickle.dump(state, f)


def cmd_start(args):
    wf_name = args.workflow
    if wf_name not in WORKFLOWS:
        print(f'Unknown workflow: {wf_name}')
        print(f'Available: {", ".join(WORKFLOWS)}')
        sys.exit(1)

    parsed_args = [json.loads(a) for a in args.args]
    exec_id = str(uuid.uuid4())[:8]

    engine, outputs = Engine.start(WORKFLOWS, wf_name, parsed_args)
    _save_state(exec_id, engine.state)

    print(f'Started execution {exec_id}')
    print(f'  workflow: {wf_name}({", ".join(args.args)})')
    _print_outputs(engine.state, outputs)


def cmd_step(args):
    state = _load_state(args.id)
    if state['finished']:
        print(f'Execution {args.id} already finished')
        sys.exit(1)

    engine = Engine(state, WORKFLOWS)
    send_val = json.loads(args.value) if args.value is not None else None

    try:
        outputs, finished = engine.step(send_val)
    except Exception as e:
        print(f'  step {state["step"]} → error: {e}', file=sys.stderr)
        sys.exit(1)

    _save_state(args.id, engine.state)
    _print_outputs(engine.state, outputs)

    if finished:
        root = state['workflows'][state['root']]
        print(f'  → returned: {root["result"]!r}')


def _print_outputs(state, outputs):
    step = state['step']
    for wf_id, wf_name, val in outputs:
        print(f'  step {step} [{wf_name}#{wf_id}] → {val!r}')


def cmd_status(args):
    state = _load_state(args.id)
    print(f'Execution {args.id}')
    print(f'  step:     {state["step"]}')
    print(f'  finished: {state["finished"]}')
    print(f'  workflows ({len(state["workflows"])}):')
    for wf_id, wf in sorted(state['workflows'].items(), key=lambda x: x[0]):
        status = wf['status']
        extra = ''
        if status == 'waiting':
            deps = wf.get('wait_deps', [])
            mode = wf.get('wait_mode', 'all')
            extra = f' ({mode} of {", ".join("#"+d for d in deps)})'
        if status == 'finished':
            extra = f' result={wf["result"]!r}'
        root = ' [root]' if wf_id == state['root'] else ''
        print(f'    #{wf_id} {wf["name"]}{root}  {status}{extra}')


def cmd_list(args):
    if not os.path.isdir(EXECUTIONS_DIR):
        print('No executions yet.')
        return
    for fname in sorted(os.listdir(EXECUTIONS_DIR)):
        if not fname.endswith('.pkl'):
            continue
        exec_id = fname[:-4]
        state = _load_state(exec_id)
        status = 'finished' if state['finished'] else f'step {state["step"]}'
        n_wf = len(state['workflows'])
        print(f'  {exec_id}  {state["workflows"][state["root"]]["name"]} ({n_wf} workflows)  [{status}]')


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
