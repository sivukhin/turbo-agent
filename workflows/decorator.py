import pickle
import types
import functools
import contextvars
from workflows.ids import new_id
from workflows.models.state import WorkflowHandle
from bytecode import (
    Bytecode,
    Instr,
    Label,
    ControlFlowGraph,
    TryBegin,
    TryEnd,
    Compare,
    BinaryOp,
)


_SENTINEL = object()
_SUBSCR = BinaryOp.SUBSCR

# Tick context: set by Engine during a tick so that calling a @workflow
# inside another workflow auto-registers the child.
_current_ctx = contextvars.ContextVar("workflow_ctx", default=None)


class _TickContext:
    def __init__(self):
        self.new_children = []


def _unpack_checkpoint(cp, varnames):
    locals_dict = cp.get("locals", {})
    restored = {v: locals_dict.get(v) for v in varnames}
    return (cp["yield_idx"], cp["drain"], cp["yv"], restored)


# ---- bytecode snippets ----


def _emit_drain(yield_idx):
    loop = Label()
    done = Label()
    return [
        Instr("STORE_FAST", "__yv__"),
        Instr("LOAD_SMALL_INT", yield_idx),
        Instr("STORE_FAST", "__yield_idx__"),
        Instr("BUILD_LIST", 0),
        Instr("STORE_FAST", "__drain__"),
        loop,
        Instr("COPY", 1),
        Instr("LOAD_CONST", _SENTINEL),
        Instr("IS_OP", 0),
        Instr("POP_JUMP_IF_TRUE", done),
        Instr("NOT_TAKEN"),
        Instr("LOAD_FAST", "__drain__"),
        Instr("SWAP", 2),
        Instr("LIST_APPEND", 1),
        Instr("POP_TOP"),
        Instr("JUMP_BACKWARD", loop),
        done,
        Instr("POP_TOP"),
    ]


def _emit_restore():
    loop = Label()
    done = Label()
    return [
        Instr("LOAD_CONST", _SENTINEL),
        Instr("LOAD_GLOBAL", (True, "reversed")),
        Instr("LOAD_FAST", "__drain__"),
        Instr("CALL", 1),
        loop,
        Instr("FOR_ITER", done),
        Instr("SWAP", 2),
        Instr("JUMP_BACKWARD", loop),
        done,
        Instr("END_FOR"),
        Instr("POP_ITER"),
        Instr("LOAD_FAST", "__yv__"),
    ]


# ---- DurableGenerator ----


class DurableGenerator:
    def __init__(self, gen, workflow_name=None, args=None):
        self._gen = gen
        self._finished = False
        self.workflow_name = workflow_name
        self.workflow_args = args or ()

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self._gen)
        except StopIteration:
            self._finished = True
            raise

    def send(self, value):
        try:
            return self._gen.send(value)
        except StopIteration:
            self._finished = True
            raise

    def checkpoint(self):
        if self._finished or self._gen.gi_frame is None:
            return None
        loc = self._gen.gi_frame.f_locals
        return {
            "yield_idx": loc.get("__yield_idx__"),
            "drain": loc.get("__drain__", []),
            "yv": loc.get("__yv__"),
            "locals": {k: v for k, v in loc.items() if not k.startswith("__")},
        }

    def save(self):
        cp = self.checkpoint()
        return pickle.dumps(cp) if cp else None

    def throw(self, *args):
        return self._gen.throw(*args)

    def close(self):
        return self._gen.close()

    @property
    def finished(self):
        return self._finished


# ---- @workflow decorator ----


def workflow(func):
    """Makes a generator function durable and checkpointable.

    When called inside another @workflow (during an engine tick), auto-registers
    as a concurrent child and returns a WorkflowHandle.
    When called at top level, returns a DurableGenerator.
    """
    bc = Bytecode.from_code(func.__code__)
    orig_argnames = list(bc.argnames)
    orig_varnames = [
        v for v in func.__code__.co_varnames if v not in ("__checkpoint__",)
    ]

    yield_count = sum(1 for i in bc if isinstance(i, Instr) and i.name == "YIELD_VALUE")
    resume_labels = [Label() for _ in range(yield_count)]

    new_bc = Bytecode()
    new_bc.argcount = bc.argcount + 1
    new_bc.argnames = ["__checkpoint__"] + orig_argnames
    new_bc.name = bc.name
    new_bc.filename = bc.filename
    new_bc.flags = bc.flags
    new_bc.posonlyargcount = bc.posonlyargcount
    new_bc.kwonlyargcount = bc.kwonlyargcount

    yield_idx = 0

    for item in bc:
        if isinstance(item, (TryBegin, TryEnd)):
            continue

        if isinstance(item, Instr) and item.name == "RESUME" and item.arg == 0:
            new_bc.append(item)
            normal_start = Label()

            new_bc.extend(
                [
                    Instr("LOAD_FAST", "__checkpoint__"),
                    Instr("LOAD_CONST", None),
                    Instr("IS_OP", 0),
                    Instr("POP_JUMP_IF_TRUE", normal_start),
                    Instr("NOT_TAKEN"),
                ]
            )

            new_bc.extend(
                [
                    Instr("LOAD_GLOBAL", (True, "_unpack_checkpoint")),
                    Instr("LOAD_FAST", "__checkpoint__"),
                    Instr("LOAD_CONST", tuple(orig_varnames)),
                    Instr("CALL", 2),
                    Instr("UNPACK_SEQUENCE", 4),
                    Instr("STORE_FAST", "__resume_idx__"),
                    Instr("STORE_FAST", "__drain__"),
                    Instr("STORE_FAST", "__yv__"),
                    Instr("STORE_FAST", "__restored_locals__"),
                ]
            )

            for varname in orig_varnames:
                new_bc.extend(
                    [
                        Instr("LOAD_FAST", "__restored_locals__"),
                        Instr("LOAD_CONST", varname),
                        Instr("BINARY_OP", _SUBSCR),
                        Instr("STORE_FAST", varname),
                    ]
                )

            for idx, label in enumerate(resume_labels):
                skip = Label()
                new_bc.extend(
                    [
                        Instr("LOAD_FAST", "__resume_idx__"),
                        Instr("LOAD_SMALL_INT", idx),
                        Instr("COMPARE_OP", Compare.EQ),
                        Instr("POP_JUMP_IF_FALSE", skip),
                        Instr("NOT_TAKEN"),
                    ]
                )
                new_bc.extend(_emit_restore())
                new_bc.append(Instr("JUMP_FORWARD", label))
                new_bc.append(skip)

            new_bc.append(normal_start)
            new_bc.append(Instr("LOAD_CONST", _SENTINEL))
            continue

        if isinstance(item, Instr) and item.name == "YIELD_VALUE":
            new_bc.extend(_emit_drain(yield_idx))
            new_bc.extend(_emit_restore())
            new_bc.append(resume_labels[yield_idx])
            new_bc.append(item)
            yield_idx += 1
            continue

        new_bc.append(item)

    cfg = ControlFlowGraph.from_bytecode(new_bc)
    new_code = cfg.to_code(stacksize=100)

    n_orig_args = len(orig_argnames)

    def _make_gen(*args, checkpoint=None, **kwargs):
        raw = types.FunctionType(
            new_code,
            {
                **func.__globals__,
                "_unpack_checkpoint": _unpack_checkpoint,
            },
        )
        if checkpoint is not None:
            dummy_args = (None,) * n_orig_args
            gen = raw(checkpoint, *dummy_args)
            next(gen)
            return DurableGenerator(gen, workflow_name=func.__name__, args=args)
        return DurableGenerator(
            raw(None, *args, **kwargs), workflow_name=func.__name__, args=args
        )

    @functools.wraps(func)
    def wrapper(*args, storage=None, description="", **kwargs):
        ctx = _current_ctx.get()
        if ctx is not None:
            handle = WorkflowHandle(
                id=new_id(),
                workflow_name=func.__name__,
                args=list(args),
                kwargs=kwargs,
                storage=storage,
                description=description,
            )
            ctx.new_children.append(handle)
            return handle
        return _make_gen(*args, **kwargs)

    def resume(data):
        cp = pickle.loads(data) if isinstance(data, bytes) else data
        return _make_gen(checkpoint=cp)

    def create(*args, **kwargs):
        return _make_gen(*args, **kwargs)

    wrapper.resume = resume
    wrapper.create = create
    wrapper._code = new_code
    return wrapper
