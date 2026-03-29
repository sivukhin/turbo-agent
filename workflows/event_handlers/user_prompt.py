from workflows.event_handlers.base import resolve_wf, register_event_handler
import workflows.events as ev


@register_event_handler(ev.UserPromptResult)
class UserPromptResultHandler:
    def handle(self, event, store, state):
        """When user responds, unblock the waiting workflow."""
        resolve_wf(state, event.workflow_id, event.payload.response)
        return []
