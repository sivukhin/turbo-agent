"""Base operation handler interface."""

from typing import Protocol
from workflows.models.context import OpContext  # noqa: F401


class OpHandler(Protocol):
    """Protocol for operation handlers."""

    @staticmethod
    def handle(val, ctx: OpContext) -> None: ...


def op_handler(op_type):
    """Decorator that tags a handler class with the op type it handles.

    Usage:
        @op_handler(ShellOp)
        class ShellOpHandler:
            @staticmethod
            def handle(val, ctx): ...
    """

    def decorator(cls):
        cls._op_type = op_type
        return cls

    return decorator
