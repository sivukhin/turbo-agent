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
    public_env: dict | None = None
    private_env: dict | None = None

@dataclass
class ShellStreamStartOp:
    command: str
    isolation: object = None
    public_env: dict | None = None
    private_env: dict | None = None

@dataclass
class ShellStreamNextOp:
    stream_id: str
    private_env: dict | None = None

@dataclass
class ShellStreamLine:
    stdout: list[str]
    stderr: list[str]
    finished: bool = False
    exit_code: int | None = None

@dataclass
class ReadFileOp:
    path: str

@dataclass
class WriteFileOp:
    path: str
    content: str

@dataclass
class UserPromptOp:
    pass

@dataclass
class AiResponseOp:
    text: str

@dataclass
class AiOp:
    messages: list | None = None
    conversation: object = None
    model: str = 'anthropic/claude-sonnet-4-20250514'
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

def shell(command, isolation=None, public_env=None, private_env=None):
    return ShellOp(command=command, isolation=isolation, public_env=public_env, private_env=private_env)

def shell_stream_start(command, isolation=None, public_env=None, private_env=None):
    return ShellStreamStartOp(command=command, isolation=isolation, public_env=public_env, private_env=private_env)

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

def ai(messages=None, *, conversation=None,
        model='anthropic/claude-sonnet-4-20250514', max_tokens=None,
        temperature=0.0, system=None, tools=None):
    return AiOp(
        messages=messages, conversation=conversation,
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
    created_at: float = 0.0

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
    description: str = ''

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
    description: str = ''

@dataclass
class HandlerState:
    handler_type: str
    state: dict

@dataclass
class StreamDef:
    """Persisted stream definition — enough info to restart a stream after crash.
    Only public env is stored. Private env (secrets) must be re-supplied."""
    stream_id: str
    command: str
    isolation_type: str
    isolation_config: dict | None
    public_env: dict | None
    workflow_id: str

@dataclass
class ExecutionState:
    workflows: dict[str, WorkflowState]
    handlers: dict[str, HandlerState]
    root_workflow_id: str
    source_file: str | None = None
    finished: bool = False
    description: str = ''
    streams: dict[str, StreamDef] = field(default_factory=dict)
