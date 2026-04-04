"""Operation handlers. Each handler processes a yielded Op and returns events."""

from workflows.operations.base import OpContext, OpHandler
from workflows.operations.wait_op import handle_wait
from workflows.operations.sleep_op import handle_sleep
from workflows.operations.shell_op import handle_shell
from workflows.operations.shell_stream_op import handle_shell_stream_start, handle_shell_stream_next
from workflows.operations.file_ops import handle_read_file, handle_write_file
from workflows.operations.llm_op import handle_ai
from workflows.operations.user_prompt_op import handle_user_prompt, handle_ai_response
from workflows.operations.conv_ops import (
    handle_conv_append,
    handle_conv_list,
    handle_conv_read,
    handle_conv_replace_with,
)

DEFAULT_OP_HANDLERS = [
    handle_shell,
    handle_shell_stream_start,
    handle_shell_stream_next,
    handle_read_file,
    handle_write_file,
    handle_ai,
    handle_user_prompt,
    handle_ai_response,
    handle_wait,
    handle_sleep,
    handle_conv_append,
    handle_conv_list,
    handle_conv_read,
    handle_conv_replace_with,
]

__all__ = [
    'OpContext',
    'OpHandler',
    'DEFAULT_OP_HANDLERS',
]
