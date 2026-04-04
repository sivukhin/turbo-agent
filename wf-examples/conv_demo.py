from workflows import (
    workflow,
    wait,
    conv_append,
    conv_read,
    conv_replace_with,
    Latest,
)


@workflow
def chat():
    """Build a conversation, read it back."""
    yield conv_append(role="system", content="You are a helpful assistant.")
    yield conv_append(role="user", content="What is 2+2?")
    yield conv_append(role="assistant", content="2+2 equals 4.")
    yield conv_append(role="user", content="And 3+3?")
    yield conv_append(role="assistant", content="3+3 equals 6.")

    messages = yield conv_read()
    return [f"{m.role}: {m.content}" for m in messages]


@workflow
def summarize():
    """Build conversation, then replace early messages with a summary."""
    r1 = yield conv_append(role="user", content="Tell me about Python.")
    yield conv_append(role="assistant", content="Python is a programming language...")
    r3 = yield conv_append(role="user", content="What about its history?")
    yield conv_append(
        role="assistant", content="Python was created by Guido van Rossum..."
    )
    yield conv_append(role="user", content="What about version 3?")

    # Summarize the first 3 messages
    yield conv_replace_with(
        [
            {
                "role": "system",
                "content": "Summary: User asked about Python and its history.",
            }
        ],
        start_ref=r1,
        end_ref=r3,
    )

    messages = yield conv_read()
    return [f"{m.role}: {m.content[:50]}" for m in messages]


@workflow
def parent_child_chat():
    """Parent builds conversation, child forks and continues."""
    yield conv_append(role="system", content="You are a helpful assistant.")
    yield conv_append(role="user", content="Hello!")
    yield conv_append(role="assistant", content="Hi! How can I help?")

    child = child_chat()
    result = yield wait(child)

    # Parent conversation unchanged
    parent_msgs = yield conv_read()
    return {
        "parent_count": len(parent_msgs),
        "child_result": result,
    }


@workflow
def child_chat():
    """Child continues from parent's conversation."""
    yield conv_append(role="user", content="Tell me a joke.")
    yield conv_append(role="assistant", content="Why did the chicken cross the road?")

    messages = yield conv_read()
    return [f"{m.role}: {m.content[:40]}" for m in messages]
