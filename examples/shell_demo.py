from workflows import workflow, wait, shell, read_file, write_file
from workflows.isolation import HostIsolation


host = HostIsolation()


@workflow
def build_and_test():
    """Write source code, run it, read output."""
    yield write_file('main.sh', '#!/bin/sh\necho "hello from $(hostname)"')
    result = yield shell('sh main.sh', isolation=host)
    yield write_file('data.txt', 'line1\nline2\nline3')
    result = yield shell('wc -l data.txt', isolation=host)
    content = yield read_file('data.txt')
    return 'done'


@workflow
def parent_child_files():
    """Parent writes a file, child reads it (same workdir by default)."""
    yield write_file('config.json', '{"key": "value"}')
    child = builder()
    result = yield wait(child)
    return result


@workflow
def builder():
    """Child workflow that reads parent's file and creates its own."""
    content = yield read_file('config.json')
    yield write_file('output.txt', f'processed: {content}')
    result = yield shell('cat output.txt', isolation=host)
    return result.stdout.strip()
