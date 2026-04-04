from workflows import workflow, wait, wait_all, shell, read_file, write_file, shell_stream_start, shell_stream_next, user_prompt
from workflows.events import ShellStreamLineEvent
from workflows.isolation import HostIsolation, StorageConfig


host = HostIsolation()

@workflow
def stream_test():
    stream = yield shell_stream_start(
        'echo 1; sleep 1; echo 2; sleep 1; echo 3',
        isolation=HostIsolation(),
        meta={'shell': 'demo'},
    )
    while True:
        result: ShellStreamLineEvent = yield shell_stream_next(stream)
        if result.finished:
            break
        yield f'stdout: {result.stdout}'

@workflow
def shell_prompt():
    yield 'hello'
    input = yield user_prompt()
    stream = yield shell_stream_start(input, isolation=HostIsolation())
    while True:
        result: ShellStreamLineEvent = yield shell_stream_next(stream)
        if result.finished:
            break
        yield f'stdout: {result.stdout}'

@workflow
def build_and_test():
    """Write source code, run it, read output."""
    yield write_file("main.sh", '#!/bin/sh\necho "hello from $(hostname)"')
    result = yield shell("sh main.sh", isolation=host)
    yield write_file("data.txt", "line1\nline2\nline3")
    result = yield shell("wc -l data.txt", isolation=host)
    content = yield read_file("data.txt")
    return "done"


@workflow
def parent_child_files():
    """Parent writes a file, child reads it (same workdir by default)."""
    yield write_file("config.json", '{"key": "value"}')
    child = builder()
    result = yield wait(child)
    return result


@workflow
def builder():
    """Child workflow that reads parent's file and creates its own."""
    content = yield read_file("config.json")
    yield write_file("output.txt", f"processed: {content}")
    result = yield shell("cat output.txt", isolation=host)
    return result.stdout.strip()


# ---- storage mode examples ----


@workflow
def isolated_children():
    """Parent writes a shared file, spawns two children with copy-full.

    Each child gets its own copy of the workspace.
    Child writes don't affect parent or each other.
    """
    yield write_file("shared.txt", "from parent")

    # Each child gets a full copy of parent's workspace
    a = modifier("A", storage=StorageConfig(mode="copy-full"))
    b = modifier("B", storage=StorageConfig(mode="copy-full"))

    ra, rb = yield wait_all([a, b])

    # Parent's file is untouched
    content = yield read_file("shared.txt")
    return {"parent": content, "a": ra, "b": rb}


@workflow
def modifier(name):
    """Read shared file, append to it, return the result."""
    content = yield read_file("shared.txt")
    new_content = f"{content} + {name}"
    yield write_file("shared.txt", new_content)
    final = yield read_file("shared.txt")
    return final


@workflow
def same_dir_children():
    """Parent and children share the same directory (default mode).

    Children can see each other's writes.
    """
    yield write_file("counter.txt", "0")

    a = incrementer("first")
    b = incrementer("second")

    yield wait(a)
    yield wait(b)

    content = yield read_file("counter.txt")
    return int(content)


@workflow
def incrementer(name):
    """Read counter, increment, write back."""
    val = yield read_file("counter.txt")
    yield write_file("counter.txt", str(int(val) + 1))
    return f"{name} done"
