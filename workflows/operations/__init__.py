"""Operation handlers. Each handler processes a yielded Op and returns events."""

from workflows.operations.base import OpContext, OpHandler, handle_op
from workflows.operations.wait_op import WaitOpHandler
from workflows.operations.sleep_op import SleepOpHandler
from workflows.operations.shell_op import ShellOpHandler
from workflows.operations.file_ops import ReadFileOpHandler, WriteFileOpHandler
from workflows.operations.llm_op import LlmOpHandler
from workflows.operations.conv_ops import (
    ConvAppendOpHandler, ConvReadOpHandler, ConvSearchOpHandler,
    ConvGetOpHandler, ConvReplaceWithOpHandler,
)

__all__ = [
    'OpContext', 'OpHandler', 'handle_op',
]
