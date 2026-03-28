import argparse
import json
import os
import pickle
import uuid
import sys

from workflow import workflow


EXECUTIONS_DIR = os.path.join(os.path.dirname(__file__), '.executions')


# ---- example workflow ----

@workflow
def accumulator(n):
    """Sum values sent at each step, adding the loop index as bonus."""
    total = 0
    for i in range(n):
        for s in range(n):
            yield total
            total = total + i + s
    yield total


# Registry of available workflows (extend as needed)
WORKFLOWS = {
    'accumulator': accumulator,
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
    wf = WORKFLOWS.get(args.workflow)
    if wf is None:
        print(f'Unknown workflow: {args.workflow}')
        print(f'Available: {", ".join(WORKFLOWS)}')
        sys.exit(1)

    parsed_args = [json.loads(a) for a in args.args]
    exec_id = str(uuid.uuid4())[:8]

    g = wf(*parsed_args)
    val = next(g)

    state = {
        'workflow': args.workflow,
        'checkpoint': g.save(),
        'step': 0,
        'finished': g.finished,
    }
    _save_state(exec_id, state)

    print(f'Started execution {exec_id}')
    print(f'  workflow: {args.workflow}({", ".join(args.args)})')
    print(f'  step 0 → yielded: {val}')


def cmd_step(args):
    state = _load_state(args.id)
    if state['finished']:
        print(f'Execution {args.id} already finished')
        sys.exit(1)

    wf = WORKFLOWS[state['workflow']]
    g = wf.resume(state['checkpoint'])

    send_val = json.loads(args.value) if args.value is not None else None
    step = state['step'] + 1

    try:
        val = g.send(send_val)
        state['checkpoint'] = g.save()
        state['step'] = step
        state['finished'] = g.finished
        _save_state(args.id, state)
        print(f'  step {step} → yielded: {val}')
    except StopIteration:
        state['finished'] = True
        state['checkpoint'] = None
        state['step'] = step
        _save_state(args.id, state)
        print(f'  step {step} → finished')
    except Exception as e:
        print(f'  step {step} → error: {e}', file=sys.stderr)
        sys.exit(1)


def cmd_status(args):
    state = _load_state(args.id)
    print(f'Execution {args.id}')
    print(f'  workflow: {state["workflow"]}')
    print(f'  step:     {state["step"]}')
    print(f'  finished: {state["finished"]}')
    if state['checkpoint']:
        cp = pickle.loads(state['checkpoint'])
        print(f'  yield_idx: {cp["yield_idx"]}')
        print(f'  locals:    {cp["locals"]}')
        print(f'  yielded:   {cp["yv"]}')
        print(f'  stack:     {cp["drain"]}')


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
        print(f'  {exec_id}  {state["workflow"]}  [{status}]')


def main():
    parser = argparse.ArgumentParser(description='Durable workflow executor')
    sub = parser.add_subparsers(dest='command', required=True)

    p_start = sub.add_parser('start', help='Start a new workflow execution')
    p_start.add_argument('workflow', help='Workflow name')
    p_start.add_argument('args', nargs='*', help='JSON-encoded arguments')

    p_step = sub.add_parser('step', help='Send a value and advance one step')
    p_step.add_argument('id', help='Execution ID')
    p_step.add_argument('value', nargs='?', help='JSON-encoded value to send (default: null)')

    p_status = sub.add_parser('status', help='Show execution status')
    p_status.add_argument('id', help='Execution ID')

    sub.add_parser('list', help='List all executions')

    args = parser.parse_args()
    {'start': cmd_start, 'step': cmd_step, 'status': cmd_status, 'list': cmd_list}[args.command](args)


if __name__ == '__main__':
    main()
