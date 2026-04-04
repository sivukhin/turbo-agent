from dataclasses import dataclass

import anthropic

from workflows.llm.base import LlmResult, ToolCall


@dataclass
class AnthropicProvider:
    """LLM provider using the Anthropic API."""

    api_key: str | None = None  # uses ANTHROPIC_API_KEY env var if None

    def complete(
        self,
        messages: list,
        model: str,
        max_tokens: int | None,
        temperature: float,
        system: str | None = None,
        tools: list | None = None,
    ) -> LlmResult:
        client = anthropic.Anthropic(api_key=self.api_key)

        kwargs = {
            "model": model,
            "max_tokens": max_tokens or 16384,
            "messages": messages,
            "temperature": temperature,
        }

        if system is not None:
            kwargs["system"] = system
        if tools:
            kwargs["tools"] = tools

        response = client.messages.create(**kwargs)

        content = []
        tool_calls = []
        text_parts = []

        for block in response.content:
            if block.type == "text":
                content.append({"type": "text", "text": block.text})
                text_parts.append(block.text)
            elif block.type == "tool_use":
                content.append(
                    {
                        "type": "tool_use",
                        "id": block.id,
                        "name": block.name,
                        "input": block.input,
                    }
                )
                tool_calls.append(
                    ToolCall(
                        id=block.id,
                        name=block.name,
                        input=block.input,
                    )
                )

        return LlmResult(
            content=content,
            model=response.model,
            stop_reason=response.stop_reason,
            usage={
                "input_tokens": response.usage.input_tokens,
                "output_tokens": response.usage.output_tokens,
                "cache_creation_input_tokens": getattr(
                    response.usage, "cache_creation_input_tokens", 0
                )
                or 0,
                "cache_read_input_tokens": getattr(
                    response.usage, "cache_read_input_tokens", 0
                )
                or 0,
            },
            tool_calls=tool_calls,
            text="\n".join(text_parts),
            message_id=response.id,
        )
