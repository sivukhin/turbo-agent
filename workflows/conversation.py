"""Conversation system: persistent, forkable, layered message storage.

Conversations live in the DB. Each workflow has one. Child workflows fork
from parent's conversation. Layers enable compaction without mutation.
"""

import uuid
import time
from dataclasses import dataclass, field


_seq_counter = 0

def _sortable_uuid():
    """Generate a time-sortable UUID (UUIDv7-like) with sequence counter."""
    global _seq_counter
    _seq_counter += 1
    ts = int(time.time() * 1_000_000)  # microseconds
    return f'{ts:016x}-{_seq_counter:08x}-{uuid.uuid4().hex[:8]}'


@dataclass
class MessageRef:
    """Concrete reference to a message in a conversation."""
    conversation_id: str
    message_id: str
    layer: int


@dataclass
class ConversationRef:
    """Concrete reference to a conversation at a specific point."""
    conversation_id: str
    message_id: str | None = None   # None = latest
    layer: int | None = None        # None = latest


@dataclass
class ConversationMessage:
    """A message read from a conversation."""
    ref: MessageRef
    role: str
    content: str


# Sentinel: resolves to the workflow's current conversation at latest point
class _LatestType:
    _instance = None
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    def __repr__(self):
        return 'Latest'
    def __reduce__(self):
        return (_LatestType, ())

Latest = _LatestType()


# ---- Operations (yielded by workflows) ----

@dataclass
class ConvAppendOp:
    role: str
    content: str


@dataclass
class ConvReadOp:
    pass


@dataclass
class ConvSearchOp:
    pattern: str


@dataclass
class ConvGetOp:
    refs: list  # list of MessageRef


@dataclass
class ConvReplaceWithOp:
    new_messages: list  # [{"role": str, "content": str}]
    start_ref: MessageRef | None = None
    end_ref: MessageRef | None = None


# ---- Yield functions ----

def conv_append(role, content):
    """Append a message to the workflow's conversation. Returns MessageRef."""
    return ConvAppendOp(role=role, content=content)


def conv_read():
    """Read all messages in the workflow's conversation. Returns [ConversationMessage]."""
    return ConvReadOp()


def conv_search(pattern):
    """Search messages by LIKE pattern. Returns [ConversationMessage]."""
    return ConvSearchOp(pattern=pattern)


def conv_get(refs):
    """Batch read messages by refs. Returns [ConversationMessage]."""
    return ConvGetOp(refs=refs)


def conv_replace_with(new_messages, start_ref=None, end_ref=None):
    """Replace a range of messages with new ones (compaction via layers)."""
    return ConvReplaceWithOp(
        new_messages=new_messages,
        start_ref=start_ref,
        end_ref=end_ref,
    )
