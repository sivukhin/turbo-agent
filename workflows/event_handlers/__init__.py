from workflows.event_handlers.shell import ShellRequestHandler
from workflows.event_handlers.shell_stream import (
    ShellStreamStartRequestHandler,
    ShellStreamNextRequestHandler,
)
from workflows.event_handlers.file import (
    FileReadRequestHandler,
    FileWriteRequestHandler,
)
from workflows.event_handlers.llm import LlmRequestHandler
from workflows.event_handlers.conversation import (
    ConvAppendRequestHandler,
    ConvListRequestHandler,
    ConvReadRequestHandler,
    ConvReplaceWithRequestHandler,
)
from workflows.event_handlers.user_prompt import UserPromptResultHandler

DEFAULT_EVENT_HANDLERS = [
    ShellRequestHandler(),
    FileReadRequestHandler(),
    FileWriteRequestHandler(),
    LlmRequestHandler(),
    ConvAppendRequestHandler(),
    ConvListRequestHandler(),
    ConvReadRequestHandler(),
    ConvReplaceWithRequestHandler(),
    UserPromptResultHandler(),
    ShellStreamStartRequestHandler(),
    ShellStreamNextRequestHandler(),
]

__all__ = [
    "ShellRequestHandler",
    "FileReadRequestHandler",
    "FileWriteRequestHandler",
    "LlmRequestHandler",
    "ConvAppendRequestHandler",
    "ConvListRequestHandler",
    "ConvReadRequestHandler",
    "ConvReplaceWithRequestHandler",
    "DEFAULT_EVENT_HANDLERS",
]
