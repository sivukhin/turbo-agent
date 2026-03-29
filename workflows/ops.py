"""Op dataclasses and Event — shared by engine and operation handlers."""

from dataclasses import dataclass, field


# ---- Operations (yielded by workflows) ----

@dataclass
class WaitOp:
    deps: list[str]
    mode: str  # 'wait' | 'wait_all' | 'wait_any'

@dataclass
class SleepOp:
    seconds: float

@dataclass
class ShellOp:
    command: str
    isolation: object = None

@dataclass
class ReadFileOp:
    path: str

@dataclass
class WriteFileOp:
    path: str
    content: str

@dataclass
class LlmOp:
    messages: list | None = None
    conversation: object = None
    provider: object = None
    model: str = 'claude-sonnet-4-20250514'
    max_tokens: int | None = None
    temperature: float = 0.0
    system: str | None = None
    tools: list | None = None


# ---- Yield functions ----

def wait(handle):
    return WaitOp(deps=[handle.id], mode='wait')

def wait_all(handles):
    return WaitOp(deps=[h.id for h in handles], mode='wait_all')

def wait_any(handles):
    return WaitOp(deps=[h.id for h in handles], mode='wait_any')

def sleep(seconds):
    return SleepOp(seconds=seconds)

def shell(command, isolation=None):
    return ShellOp(command=command, isolation=isolation)

def read_file(path):
    return ReadFileOp(path=path)

def write_file(path, content):
    return WriteFileOp(path=path, content=content)

def llm(messages=None, *, conversation=None, provider=None,
        model='claude-sonnet-4-20250514', max_tokens=None,
        temperature=0.0, system=None, tools=None):
    return LlmOp(
        messages=messages, conversation=conversation, provider=provider,
        model=model, max_tokens=max_tokens, temperature=temperature,
        system=system, tools=tools,
    )


# ---- Event ----

@dataclass
class Event:
    event_id: int
    execution_id: str
    workflow_id: str | None
    category: str
    payload: object

    @property
    def type(self) -> str:
        from workflows.events import payload_type_name
        return payload_type_name(self.payload)


# ---- State ----

@dataclass
class WorkflowHandle:
    id: str
    workflow_name: str
    args: list
    storage: object = None

    def __repr__(self):
        return f'<{self.workflow_name}#{self.id}>'

@dataclass
class WorkflowState:
    name: str
    args: list
    parent_workflow_id: str | None = None
    workdir: str | None = None
    branches: dict | None = None
    conversation_id: str | None = None
    checkpoint: dict | None = None
    status: str = 'running'
    result: object = None
    send_val: object = field(default=None, repr=False)

@dataclass
class HandlerState:
    handler_type: str
    state: dict

@dataclass
class ExecutionState:
    workflows: dict[str, WorkflowState]
    handlers: dict[str, HandlerState]
    root_workflow_id: str
    source_file: str | None = None
    finished: bool = False
