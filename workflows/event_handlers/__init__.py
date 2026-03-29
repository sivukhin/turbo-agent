from workflows.event_handlers.shell import ShellRequestHandler
from workflows.event_handlers.file import FileReadRequestHandler, FileWriteRequestHandler
from workflows.event_handlers.llm import LlmRequestHandler
from workflows.event_handlers.conversation import (
    ConvAppendRequestHandler, ConvReadRequestHandler, ConvSearchRequestHandler,
    ConvGetRequestHandler, ConvReplaceWithRequestHandler,
)

DEFAULT_EVENT_HANDLERS = [
    ShellRequestHandler(),
    FileReadRequestHandler(),
    FileWriteRequestHandler(),
    LlmRequestHandler(),
    ConvAppendRequestHandler(),
    ConvReadRequestHandler(),
    ConvSearchRequestHandler(),
    ConvGetRequestHandler(),
    ConvReplaceWithRequestHandler(),
]

__all__ = [
    'ShellRequestHandler', 'FileReadRequestHandler', 'FileWriteRequestHandler',
    'LlmRequestHandler',
    'ConvAppendRequestHandler', 'ConvReadRequestHandler', 'ConvSearchRequestHandler',
    'ConvGetRequestHandler', 'ConvReplaceWithRequestHandler',
    'DEFAULT_EVENT_HANDLERS',
]
