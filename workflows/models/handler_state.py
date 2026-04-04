"""Typed state dataclasses for workflow event handlers."""

from dataclasses import dataclass, field


@dataclass
class WaitState:
    dep: str
    result: object = None
    resolved: bool = False


@dataclass
class WaitAllState:
    deps: list[str]
    results: dict[str, object] = field(default_factory=dict)


@dataclass
class WaitAnyState:
    deps: list[str]
    results: dict[str, object] = field(default_factory=dict)


@dataclass
class SleepState:
    wake_at: float


@dataclass
class StreamNextState:
    stream_id: str
    emit_events: list = field(default_factory=list)
