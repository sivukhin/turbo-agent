"""Conversation system: persistent, forkable, layered message storage.

Two reference types:
  - ConversationRef: (conversation_id, message_id, layer) — points to a conversation snapshot
  - MessageRef: (conversation_id, message_id, layer, role) — points to a specific message with meta

Two read operations:
  - list_messages: returns [MessageRef] — refs with role, no content
  - read_messages: accepts [MessageRef], returns [Message] — refs with content
"""

from workflows.ids import new_id  # noqa: F401
from workflows.models.conversation import ConversationRef, MessageRef, Message  # noqa: F401
from workflows.models.operations import (  # noqa: F401
    ConvAppendOp, ConvListOp, ConvReadOp, ConvReplaceWithOp,
)


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


# ---- Yield functions ----

def conv_append(role, content, meta=None):
    """Append a message. Returns MessageRef. Content can be str or dict/list (auto-serialized to JSON)."""
    return ConvAppendOp(role=role, content=content, meta=meta or {})


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
