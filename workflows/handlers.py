"""Handler registry for wait operations.

Each handler has:
  initial_state(args) -> state
  on_message(msg_type, msg_workflow_id, payload, state) -> state
  try_resolve(state, now) -> (resolved, result)
"""


class WaitHandler:
    """Wait for a single workflow to finish."""

    @staticmethod
    def initial_state(deps):
        return {'dep': deps[0], 'result': None, 'resolved': False}

    @staticmethod
    def on_message(msg_type, msg_workflow_id, payload, state):
        if msg_type == 'workflow_finished' and msg_workflow_id == state['dep']:
            return {**state, 'result': payload['result'], 'resolved': True}
        return state

    @staticmethod
    def try_resolve(state, now):
        if state['resolved']:
            return True, state['result']
        return False, None


class WaitAllHandler:
    """Wait for all workflows in a list to finish."""

    @staticmethod
    def initial_state(deps):
        return {'deps': list(deps), 'results': {}}

    @staticmethod
    def on_message(msg_type, msg_workflow_id, payload, state):
        if msg_type == 'workflow_finished' and msg_workflow_id in state['deps']:
            results = {**state['results'], msg_workflow_id: payload['result']}
            return {**state, 'results': results}
        return state

    @staticmethod
    def try_resolve(state, now):
        if all(d in state['results'] for d in state['deps']):
            ordered = [state['results'][d] for d in state['deps']]
            if len(state['deps']) == 1:
                return True, ordered[0]
            return True, ordered
        return False, None


class WaitAnyHandler:
    """Wait for at least one workflow to finish. Returns list of (bool, result|None)."""

    @staticmethod
    def initial_state(deps):
        return {'deps': list(deps), 'results': {}}

    @staticmethod
    def on_message(msg_type, msg_workflow_id, payload, state):
        if msg_type == 'workflow_finished' and msg_workflow_id in state['deps']:
            results = {**state['results'], msg_workflow_id: payload['result']}
            return {**state, 'results': results}
        return state

    @staticmethod
    def try_resolve(state, now):
        if state['results']:
            result = [
                (True, state['results'][d]) if d in state['results'] else (False, None)
                for d in state['deps']
            ]
            return True, result
        return False, None


class SleepHandler:
    """Sleep until a given timestamp. Resolves when now >= wake_at."""

    @staticmethod
    def initial_state(wake_at):
        return {'wake_at': wake_at}

    @staticmethod
    def on_message(msg_type, msg_workflow_id, payload, state):
        return state

    @staticmethod
    def try_resolve(state, now):
        if now >= state['wake_at']:
            return True, None
        return False, None


HANDLER_REGISTRY = {
    'wait': WaitHandler,
    'wait_all': WaitAllHandler,
    'wait_any': WaitAnyHandler,
    'sleep': SleepHandler,
}
