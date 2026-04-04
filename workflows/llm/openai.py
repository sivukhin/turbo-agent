import json
from dataclasses import dataclass

import openai

from workflows.llm.base import LlmResult, ToolCall


@dataclass
class OpenAIProvider:
    """LLM provider using the OpenAI API."""

    api_key: str | None = None  # uses OPENAI_API_KEY env var if None
    base_url: str | None = None

    def complete(
        self,
        messages: list,
        model: str,
        max_tokens: int | None,
        temperature: float,
        system: str | None = None,
        tools: list | None = None,
    ) -> LlmResult:
        client = openai.OpenAI(api_key=self.api_key, base_url=self.base_url)

        msgs = list(messages)
        if system:
            msgs = [{"role": "system", "content": system}] + msgs

        kwargs = {
            "model": model,
            "messages": msgs,
            "temperature": temperature,
        }

        if max_tokens is not None:
            kwargs["max_tokens"] = max_tokens

        if tools:
            kwargs["tools"] = [
                {
                    "type": "function",
                    "function": {
                        "name": t["name"],
                        "description": t.get("description", ""),
                        "parameters": t.get("input_schema", t.get("parameters", {})),
                    },
                }
                for t in tools
            ]

        response = client.chat.completions.create(**kwargs)
        choice = response.choices[0]
        msg = choice.message

        content = []
        tool_calls = []
        text_parts = []

        if msg.content:
            content.append({"type": "text", "text": msg.content})
            text_parts.append(msg.content)

        if msg.tool_calls:
            for tc in msg.tool_calls:
                args = (
                    json.loads(tc.function.arguments) if tc.function.arguments else {}
                )
                content.append(
                    {
                        "type": "tool_use",
                        "id": tc.id,
                        "name": tc.function.name,
                        "input": args,
                    }
                )
                tool_calls.append(
                    ToolCall(
                        id=tc.id,
                        name=tc.function.name,
                        input=args,
                    )
                )

        usage = None
        if response.usage:
            usage = {
                "input_tokens": response.usage.prompt_tokens,
                "output_tokens": response.usage.completion_tokens,
                "cached_tokens": getattr(response.usage, "prompt_tokens_details", None)
                and getattr(response.usage.prompt_tokens_details, "cached_tokens", 0)
                or 0,
            }

        return LlmResult(
            content=content,
            model=response.model,
            stop_reason=choice.finish_reason,
            usage=usage,
            tool_calls=tool_calls,
            text="\n".join(text_parts),
            message_id=response.id,
        )
