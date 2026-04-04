"""Sortable ID generation. All IDs are hex strings, time-sortable, human-distinguishable."""

import os
import time

_seq_counter = 0


def new_id() -> str:
    """Generate a sortable hex ID: 12-char timestamp + 4-char random suffix."""
    global _seq_counter
    _seq_counter += 1
    ts = int(time.time() * 1000)
    rand = os.urandom(2).hex()
    return f"{ts:012x}{_seq_counter:04x}{rand}"
