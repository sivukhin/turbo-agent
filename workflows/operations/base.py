"""Base operation handler interface."""

from typing import Protocol
from workflows.models.context import OpContext  # noqa: F401


class OpHandler(Protocol):
    """Protocol for operation handlers."""

    @staticmethod
    def op_type() -> type: ...

    @staticmethod
    def handle(val, ctx: OpContext) -> None: ...


def op_handler(op_type):
    """Decorate a function to create an OpHandler class.

    Usage:
        @op_handler(ShellOp)
        def handle_shell(val: ShellOp, ctx: OpContext) -> None: ...
    """
    def decorator(func):
        return type(func.__name__, (), {
            'op_type': staticmethod(lambda: op_type),
            'handle': staticmethod(func),
        })
    return decorator
