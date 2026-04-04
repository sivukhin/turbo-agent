"""Op dataclasses and Event — shared by engine and operation handlers."""

from workflows.isolation.base import Isolation, ShellResult
from workflows.llm.base import LlmResult
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


def wait(handle) -> WaitOp:
    return WaitOp(deps=[handle.id], mode='wait')


def wait_all(handles) -> WaitOp:
    return WaitOp(deps=[h.id for h in handles], mode='wait_all')


def wait_any(handles) -> WaitOp:
    return WaitOp(deps=[h.id for h in handles], mode='wait_any')


def sleep(seconds: float) -> SleepOp:
    return SleepOp(seconds=seconds)


def shell(command: str, isolation: Isolation, public_env=None, private_env=None) -> ShellOp:
    return ShellOp(
        command=command,
        isolation=isolation,
        public_env=public_env,
        private_env=private_env,
    )


def shell_stream_start(command: str, isolation: Isolation, public_env=None, private_env=None) -> ShellStreamStartOp:
    return ShellStreamStartOp(
        command=command,
        isolation=isolation,
        public_env=public_env,
        private_env=private_env,
    )


def shell_stream_next(stream_id: str, private_env=None) -> ShellStreamNextOp:
    return ShellStreamNextOp(stream_id=stream_id, private_env=private_env)


def read_file(path: str) -> ReadFileOp:
    return ReadFileOp(path=path)


def write_file(path: str, content: str) -> WriteFileOp:
    return WriteFileOp(path=path, content=content)


def user_prompt() -> UserPromptOp:
    return UserPromptOp()


def ai_response(text: str) -> AiResponseOp:
    return AiResponseOp(text=text)


def ai(
    messages=None,
    *,
    conversation=None,
    model='anthropic/claude-sonnet-4-20250514',
    max_tokens=None,
    temperature=0.0,
    system=None,
    tools=None,
) -> AiOp:
    return AiOp(
        messages=messages,
        conversation=conversation,
        model=model,
        max_tokens=max_tokens,
        temperature=temperature,
        system=system,
        tools=tools,
    )
