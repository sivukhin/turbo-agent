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
    yield 'both started'
    first, second = yield wait_all([a, b])
    yield f'results: {first}, {second}'
    return first + second


@workflow
def race(n):
    children = [accumulator(i + 1) for i in range(n)]
    yield f'racing {n} children'
    results = yield wait_any(children)
    finished = [(i, r) for i, (done, r) in enumerate(results) if done]
    yield f'finished: {finished}'
    return results


@workflow
def pipeline(steps):
    children = []
    for i in range(steps):
        children.append(accumulator(i + 1))
    yield f'launched {steps} children'
    results = []
    for i, child in enumerate(children):
        result = yield wait(child)
        results.append(result)
        yield f'stage {i} done: {result}'
    return sum(results)


@workflow
def sleepy():
    yield 'starting'
    yield sleep(5)
    yield 'woke up'
    child = accumulator(2)
    result = yield wait(child)
    yield f'done: {result}'
    return result
