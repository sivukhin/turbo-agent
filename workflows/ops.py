"""Op dataclasses and Event — shared by engine and operation handlers."""

import shlex

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


def _to_command(command: str | list[str]) -> str:
    if isinstance(command, list):
        return shlex.join(command)
    return command


def wait(handle, meta=None) -> WaitOp:
    return WaitOp(deps=[handle.id], mode="wait", meta=meta or {})


def wait_all(handles, meta=None) -> WaitOp:
    return WaitOp(deps=[h.id for h in handles], mode="wait_all", meta=meta or {})


def wait_any(handles, meta=None) -> WaitOp:
    return WaitOp(deps=[h.id for h in handles], mode="wait_any", meta=meta or {})


def sleep(seconds: float, meta=None) -> SleepOp:
    return SleepOp(seconds=seconds, meta=meta or {})


def shell(
    command: str | list[str],
    isolation: Isolation,
    public_env=None,
    private_env=None,
    meta=None,
) -> ShellOp:
    return ShellOp(
        command=_to_command(command),
        isolation=isolation,
        public_env=public_env,
        private_env=private_env,
        meta=meta or {},
    )


def shell_stream_start(
    command: str | list[str],
    isolation: Isolation,
    public_env=None,
    private_env=None,
    meta=None,
) -> ShellStreamStartOp:
    return ShellStreamStartOp(
        command=_to_command(command),
        isolation=isolation,
        public_env=public_env,
        private_env=private_env,
        meta=meta or {},
    )


def shell_stream_next(stream_id: str, private_env=None, meta=None) -> ShellStreamNextOp:
    return ShellStreamNextOp(
        stream_id=stream_id, private_env=private_env, meta=meta or {}
    )


def read_file(path: str, meta=None) -> ReadFileOp:
    return ReadFileOp(path=path, meta=meta or {})


def write_file(path: str, content: str, meta=None) -> WriteFileOp:
    return WriteFileOp(path=path, content=content, meta=meta or {})


def user_prompt(meta=None) -> UserPromptOp:
    return UserPromptOp(meta=meta or {})


def ai(
    messages=None,
    *,
    conversation=None,
    model="anthropic/claude-sonnet-4-20250514",
    max_tokens=None,
    temperature=0.0,
    system=None,
    tools=None,
    meta=None,
) -> AiOp:
    return AiOp(
        messages=messages,
        conversation=conversation,
        model=model,
        max_tokens=max_tokens,
        temperature=temperature,
        system=system,
        tools=tools,
        meta=meta or {},
    )
