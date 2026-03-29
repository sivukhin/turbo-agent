"""Operation handlers. Each handler processes a yielded Op and returns events."""

from workflows.operations.base import OpContext, OpHandler, handle_op
from workflows.operations.wait_op import WaitOpHandler
from workflows.operations.sleep_op import SleepOpHandler
from workflows.operations.shell_op import ShellOpHandler
from workflows.operations.file_ops import ReadFileOpHandler, WriteFileOpHandler
from workflows.operations.llm_op import AiOpHandler
from workflows.operations.user_prompt_op import UserPromptOpHandler, AiResponseOpHandler
from workflows.operations.conv_ops import (
    ConvAppendOpHandler, ConvListOpHandler, ConvReadOpHandler,
    ConvReplaceWithOpHandler,
)

__all__ = [
    'OpContext', 'OpHandler', 'handle_op',
]
