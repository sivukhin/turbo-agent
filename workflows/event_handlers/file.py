from pathlib import Path
from workflows.event_handlers.base import (
    resolve_wf,
    make_inbox_event,
    register_event_handler,
)
import workflows.events as ev


@register_event_handler(ev.FileReadRequest)
class FileReadRequestHandler:
    def handle(self, event, store, state):
        payload = event.payload
        wf = state.workflows.get(event.workflow_id)
        if not wf or not wf.workdir:
            return []
        content = (Path(wf.workdir) / payload.path).read_text()
        resolve_wf(state, event.workflow_id, content)
        return [
            make_inbox_event(
                event,
                ev.FileReadResult(
                    path=payload.path,
                    content=content,
                ),
            )
        ]


@register_event_handler(ev.FileWriteRequest)
class FileWriteRequestHandler:
    def handle(self, event, store, state):
        payload = event.payload
        wf = state.workflows.get(event.workflow_id)
        if not wf or not wf.workdir:
            return []
        file_path = Path(wf.workdir) / payload.path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(payload.content)
        resolve_wf(state, event.workflow_id, None)
        return [
            make_inbox_event(
                event,
                ev.FileWriteResult(
                    path=payload.path,
                    size=len(payload.content),
                ),
            )
        ]
