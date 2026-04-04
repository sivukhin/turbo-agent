"""Op dataclasses and Event — shared by engine and operation handlers."""

from workflows.models.operations import (  # noqa: F401
    WaitOp,
    SleepOp,
    ShellOp,
    ShellStreamStartOp,
    ShellStreamNextOp,
    ReadFileOp,
    WriteFileOp,
    UserPromptOp,
    AiResponseOp,
    AiOp,
)
from workflows.models.state import (  # noqa: F401
    ShellStreamLine,
    Event,
    WorkflowHandle,
    WorkflowState,
    HandlerState,
    StreamDef,
    ExecutionState,
)


# ---- Yield functions ----


def wait(handle):
    return WaitOp(deps=[handle.id], mode="wait")


def wait_all(handles):
    return WaitOp(deps=[h.id for h in handles], mode="wait_all")


def wait_any(handles):
    return WaitOp(deps=[h.id for h in handles], mode="wait_any")


def sleep(seconds):
    return SleepOp(seconds=seconds)


def shell(command, isolation=None, public_env=None, private_env=None):
    return ShellOp(
        command=command,
        isolation=isolation,
        public_env=public_env,
        private_env=private_env,
    )


def shell_stream_start(command, isolation=None, public_env=None, private_env=None):
    return ShellStreamStartOp(
        command=command,
        isolation=isolation,
        public_env=public_env,
        private_env=private_env,
    )


def shell_stream_next(stream_id, private_env=None):
    return ShellStreamNextOp(stream_id=stream_id, private_env=private_env)


def read_file(path):
    return ReadFileOp(path=path)


def write_file(path, content):
    return WriteFileOp(path=path, content=content)


def user_prompt():
    """Wait for user input. Blocks until answered externally. Returns the response string."""
    return UserPromptOp()


def ai_response(text):
    """Emit an AI response for display. Does not block."""
    return AiResponseOp(text=text)


def ai(
    messages=None,
    *,
    conversation=None,
    model="anthropic/claude-sonnet-4-20250514",
    max_tokens=None,
    temperature=0.0,
    system=None,
    tools=None,
):
    return AiOp(
        messages=messages,
        conversation=conversation,
        model=model,
        max_tokens=max_tokens,
        temperature=temperature,
        system=system,
        tools=tools,
    )
