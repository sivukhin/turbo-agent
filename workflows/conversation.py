"""Conversation system: persistent, forkable, layered message storage.

Two reference types:
  - ConversationRef: (conversation_id, message_id, layer) — points to a conversation snapshot
  - MessageRef: (conversation_id, message_id, layer, role) — points to a specific message with meta

Two read operations:
  - list_messages: returns [MessageRef] — refs with role, no content
  - read_messages: accepts [MessageRef], returns [Message] — refs with content
"""

import uuid
import time
from dataclasses import dataclass, field


_seq_counter = 0

def _sortable_uuid():
    global _seq_counter
    _seq_counter += 1
    ts = int(time.time() * 1_000_000)
    return f'{ts:016x}-{_seq_counter:08x}-{uuid.uuid4().hex[:8]}'


@dataclass
class ConversationRef:
    """Reference to a conversation at a specific point."""
    conversation_id: str
    message_id: str | None = None   # None = latest
    layer: int | None = None        # None = latest


@dataclass
class MessageRef:
    """Reference to a specific message, includes role as metadata."""
    conversation_id: str
    message_id: str
    layer: int
    role: str


@dataclass
class Message:
    """A message with content, returned by read_messages."""
    ref: MessageRef
    content: str

    @property
    def role(self):
        return self.ref.role


# Sentinel: resolves to the workflow's current conversation
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
class ConvListOp:
    """List message refs in range. Returns [MessageRef]."""
    conversation: object = None  # ConversationRef or None (= workflow's current)
    start_message_id: str | None = None
    end_message_id: str | None = None
    role_filter: str | None = None
    pattern: str | None = None  # LIKE pattern on content


@dataclass
class ConvReadOp:
    """Read messages by refs. Returns [Message]."""
    refs: list  # [MessageRef]


@dataclass
class ConvReplaceWithOp:
    new_messages: list  # [{"role": str, "content": str}]
    start_ref: MessageRef | None = None
    end_ref: MessageRef | None = None


# ---- Yield functions ----

def conv_append(role, content):
    """Append a message. Returns MessageRef."""
    return ConvAppendOp(role=role, content=content)


def conv_list(conversation=None, start_message_id=None, end_message_id=None,
              role_filter=None, pattern=None):
    """List message refs. Returns [MessageRef]."""
    return ConvListOp(
        conversation=conversation,
        start_message_id=start_message_id,
        end_message_id=end_message_id,
        role_filter=role_filter,
        pattern=pattern,
    )


def conv_read(refs):
    """Read messages by refs. Returns [Message]."""
    return ConvReadOp(refs=refs)


def conv_replace_with(new_messages, start_ref=None, end_ref=None):
    """Replace a range with new messages (compaction)."""
    return ConvReplaceWithOp(
        new_messages=new_messages,
        start_ref=start_ref,
        end_ref=end_ref,
    )
