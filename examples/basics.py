from workflows import workflow, wait, wait_all, wait_any, sleep


@workflow
def accumulator(n):
    total = 0
    for i in range(n):
        for s in range(n):
            yield total
            total = total + i + s
    yield total
    return total


@workflow
def double_accumulate(n):
    a = accumulator(n)
    b = accumulator(n)
    yield "both started"
    first, second = yield wait_all([a, b])
    yield f"results: {first}, {second}"
    return first + second


@workflow
def race(n):
    children = [accumulator(i + 1) for i in range(n)]
    yield f"racing {n} children"
    results = yield wait_any(children)
    finished = [(i, r) for i, (done, r) in enumerate(results) if done]
    yield f"finished: {finished}"
    return results


@workflow
def pipeline(steps):
    children = []
    for i in range(steps):
        children.append(accumulator(i + 1))
    yield f"launched {steps} children"
    results = []
    for i, child in enumerate(children):
        result = yield wait(child)
        results.append(result)
        yield f"stage {i} done: {result}"
    return sum(results)


@workflow
def sleepy():
    yield "starting"
    yield sleep(5)
    yield "woke up"
    child = accumulator(2)
    result = yield wait(child)
    yield f"done: {result}"
    return result


@workflow
def worker(name, n):
    total = 0
    for i in range(n):
        total += i
        yield f"{name}: step {i}, total={total}"
    return total


@workflow
def supervisor(n):
    """Spawns workers, each supervised by a sub-supervisor."""
    workers = [worker(f"w{i}", i + 1) for i in range(n)]
    yield f"supervisor: launched {n} workers"
    results = yield wait_all(workers)
    yield f"supervisor: all done, results={results}"
    return sum(results)


@workflow
def deep_chain(depth):
    """Recursive chain: each level spawns a supervisor with increasing workers.

    deep_chain(3) creates:
      deep_chain → supervisor(1) → worker("w0", 1)
                 → supervisor(2) → worker("w0", 1), worker("w1", 2)
                 → supervisor(3) → worker("w0", 1), worker("w1", 2), worker("w2", 3)
    """
    children = []
    for i in range(depth):
        children.append(supervisor(i + 1))
    yield f"deep_chain: launched {depth} supervisors"
    results = yield wait_all(children)
    yield f"deep_chain: supervisor results={results}"
    return sum(results)
