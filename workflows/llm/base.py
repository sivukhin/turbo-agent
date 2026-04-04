from dataclasses import dataclass, field
from typing import Protocol


@dataclass
class ToolCall:
    """A parsed tool call from the LLM response."""

    id: str
    name: str
    input: dict


@dataclass
class LlmResult:
    """Provider-agnostic LLM result."""

    content: list  # raw content blocks
    model: str
    stop_reason: str | None
    usage: dict | None  # {"input_tokens": int, "output_tokens": int, ...}
    tool_calls: list[ToolCall] = field(default_factory=list)
    text: str = ""  # concatenated text from all text blocks
    message_id: str | None = None


class LlmProvider(Protocol):
    """Protocol for LLM providers."""

    def complete(
        self,
        messages: list,
        model: str,
        max_tokens: int | None,
        temperature: float,
        system: str | None = None,
        tools: list | None = None,
    ) -> LlmResult: ...
