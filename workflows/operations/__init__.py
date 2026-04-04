"""Operation handlers. Each handler processes a yielded Op and returns events."""

from workflows.operations.base import OpContext, OpHandler
from workflows.operations.wait_op import WaitOpHandler
from workflows.operations.sleep_op import SleepOpHandler
from workflows.operations.shell_op import ShellOpHandler
from workflows.operations.shell_stream_op import (
    ShellStreamStartOpHandler,
    ShellStreamNextOpHandler,
)
from workflows.operations.file_ops import ReadFileOpHandler, WriteFileOpHandler
from workflows.operations.llm_op import AiOpHandler
from workflows.operations.user_prompt_op import UserPromptOpHandler, AiResponseOpHandler
from workflows.operations.conv_ops import (
    ConvAppendOpHandler,
    ConvListOpHandler,
    ConvReadOpHandler,
    ConvReplaceWithOpHandler,
)

DEFAULT_OP_HANDLERS = [
    ShellOpHandler,
    ShellStreamStartOpHandler,
    ShellStreamNextOpHandler,
    ReadFileOpHandler,
    WriteFileOpHandler,
    AiOpHandler,
    UserPromptOpHandler,
    AiResponseOpHandler,
    WaitOpHandler,
    SleepOpHandler,
    ConvAppendOpHandler,
    ConvListOpHandler,
    ConvReadOpHandler,
    ConvReplaceWithOpHandler,
]

__all__ = [
    "OpContext",
    "OpHandler",
    "DEFAULT_OP_HANDLERS",
]
