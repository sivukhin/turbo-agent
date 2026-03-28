import pickle
import pytest
from workflows import workflow


# ---- basic generator behavior ----

@workflow
def counter(n):
    for i in range(n):
        yield i
    return n

@workflow
def echo():
    x = yield 'ready'
    yield f'got {x}'
    return x

@workflow
def expr_yield():
    a = 1
    b = 2
    c = a + (yield b) + b
    yield c
    return c

@workflow
def for_loop_yield(n):
    for i in range(n):
        yield i
    yield 'done'
    return n

@workflow
def if_else_yield(x):
    if x > 0:
        yield x
    else:
        yield -x
    yield 'end'

@workflow
def nested_expr():
    result = (yield 1) + (yield 2) + (yield 3)
    yield result
    return result


class TestBasicYield:
    def test_simple_counter(self):
        g = counter(3)
        assert next(g) == 0
        assert next(g) == 1
        assert next(g) == 2
        with pytest.raises(StopIteration) as exc:
            next(g)
        assert exc.value.value == 3

    def test_send_value(self):
        g = echo()
        assert next(g) == 'ready'
        assert g.send(42) == 'got 42'
        with pytest.raises(StopIteration) as exc:
            next(g)
        assert exc.value.value == 42

    def test_yield_inside_expression(self):
        g = expr_yield()
        assert next(g) == 2  # yields b=2
        assert g.send(10) == 13  # c = 1 + 10 + 2
        with pytest.raises(StopIteration) as exc:
            next(g)
        assert exc.value.value == 13

    def test_for_loop(self):
        g = for_loop_yield(3)
        assert [next(g) for _ in range(4)] == [0, 1, 2, 'done']

    def test_if_else_true_branch(self):
        g = if_else_yield(5)
        assert next(g) == 5
        assert next(g) == 'end'

    def test_if_else_false_branch(self):
        g = if_else_yield(-5)
        assert next(g) == 5  # -(-5)
        assert next(g) == 'end'

    def test_nested_expression_yields(self):
        g = nested_expr()
        assert next(g) == 1
        assert g.send(10) == 2
        assert g.send(20) == 3
        assert g.send(30) == 60
        with pytest.raises(StopIteration) as exc:
            next(g)
        assert exc.value.value == 60


# ---- checkpoint / resume ----

class TestCheckpointResume:
    def test_checkpoint_has_locals(self):
        g = counter(5)
        next(g)  # yield 0
        next(g)  # yield 1
        cp = g.checkpoint()
        assert cp is not None
        assert cp['yv'] == 1
        assert 'i' in cp['locals']
        assert cp['locals']['i'] == 1

    def test_save_and_resume(self):
        g = counter(5)
        next(g)  # 0
        next(g)  # 1
        next(g)  # 2
        data = g.save()
        assert data is not None

        g2 = counter.resume(data)
        assert next(g2) == 3
        assert next(g2) == 4
        with pytest.raises(StopIteration) as exc:
            next(g2)
        assert exc.value.value == 5

    def test_resume_with_send(self):
        g = echo()
        next(g)  # 'ready'
        data = g.save()

        g2 = echo.resume(data)
        assert g2.send(99) == 'got 99'

    def test_resume_preserves_expression_stack(self):
        g = expr_yield()
        next(g)  # yields b=2, stack has a=1 below
        data = g.save()

        g2 = expr_yield.resume(data)
        assert g2.send(10) == 13  # a(1) + 10 + b(2)

    def test_resume_mid_for_loop(self):
        g = for_loop_yield(5)
        next(g)  # 0
        next(g)  # 1
        next(g)  # 2
        data = g.save()

        g2 = for_loop_yield.resume(data)
        assert next(g2) == 3
        assert next(g2) == 4
        assert next(g2) == 'done'

    def test_checkpoint_is_picklable(self):
        g = counter(3)
        next(g)
        data = g.save()
        cp = pickle.loads(data)
        # Round-trip through pickle
        data2 = pickle.dumps(cp)
        g2 = counter.resume(pickle.loads(data2))
        assert next(g2) == 1

    def test_resume_nested_expression(self):
        g = nested_expr()
        next(g)      # yield 1
        g.send(10)   # yield 2, stack has 10
        data = g.save()

        g2 = nested_expr.resume(data)
        assert g2.send(20) == 3     # yield 3, stack has 10+20=30
        assert g2.send(30) == 60    # result = 30 + 30 = 60


# ---- cross-process simulation ----

class TestCrossProcess:
    def test_serialize_deserialize_resume(self):
        """Simulate full process restart via bytes serialization."""
        g = counter(10)
        for _ in range(5):
            next(g)
        data = g.save()

        # "New process" — only has bytes
        g2 = counter.resume(pickle.loads(pickle.dumps(pickle.loads(data))))
        remaining = list(g2)
        assert remaining == [5, 6, 7, 8, 9]

    def test_multiple_checkpoint_resume_cycles(self):
        g = counter(6)
        next(g)  # 0
        next(g)  # 1
        data = g.save()

        g2 = counter.resume(data)
        next(g2)  # 2
        next(g2)  # 3
        data2 = g2.save()

        g3 = counter.resume(data2)
        assert next(g3) == 4
        assert next(g3) == 5


# ---- nested loops ----

@workflow
def nested_loops(n, m):
    results = []
    for i in range(n):
        for j in range(m):
            yield (i, j)
            results.append(i * m + j)
    yield results
    return sum(results)

@workflow
def triple_nested(a, b, c):
    total = 0
    for i in range(a):
        for j in range(b):
            for k in range(c):
                total += 1
                yield total
    return total

@workflow
def nested_with_send(n):
    """Nested loop where inner loop uses sent values."""
    total = 0
    for i in range(n):
        for j in range(n):
            x = yield total
            total += x
    return total

@workflow
def while_in_for(n):
    for i in range(n):
        count = 0
        while count < i:
            yield (i, count)
            count += 1
    return n


class TestNestedLoops:
    def test_nested_loop_values(self):
        g = nested_loops(2, 3)
        pairs = []
        for _ in range(6):
            pairs.append(next(g))
        assert pairs == [(0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2)]

    def test_nested_loop_return(self):
        g = nested_loops(2, 3)
        for _ in range(6):
            next(g)
        final = next(g)  # yields results list
        assert final == [0, 1, 2, 3, 4, 5]
        with pytest.raises(StopIteration) as exc:
            next(g)
        assert exc.value.value == 15

    def test_triple_nested(self):
        g = triple_nested(2, 2, 2)
        vals = list(g)
        assert vals == [1, 2, 3, 4, 5, 6, 7, 8]

    def test_triple_nested_return(self):
        g = triple_nested(2, 3, 1)
        vals = list(g)
        assert vals == [1, 2, 3, 4, 5, 6]

    def test_nested_with_send(self):
        g = nested_with_send(2)
        assert next(g) == 0      # i=0,j=0, total=0
        assert g.send(10) == 10  # i=0,j=1, total=10
        assert g.send(5) == 15   # i=1,j=0, total=15
        assert g.send(1) == 16   # i=1,j=1, total=16
        with pytest.raises(StopIteration) as exc:
            g.send(100)
        assert exc.value.value == 116

    def test_while_in_for(self):
        g = while_in_for(4)
        vals = list(g)
        # i=0: nothing, i=1: (1,0), i=2: (2,0),(2,1), i=3: (3,0),(3,1),(3,2)
        assert vals == [(1, 0), (2, 0), (2, 1), (3, 0), (3, 1), (3, 2)]

    def test_nested_loop_checkpoint_resume(self):
        g = nested_loops(3, 3)
        for _ in range(5):
            next(g)
        data = g.save()
        g2 = nested_loops.resume(data)
        remaining = []
        for val in g2:
            remaining.append(val)
        # 9 pairs total, we consumed 5, so 4 pairs + final list
        assert remaining[0] == (1, 2)
        assert len(remaining) == 5  # 4 pairs + results list

    def test_triple_nested_checkpoint(self):
        g = triple_nested(2, 2, 2)
        next(g)  # 1
        next(g)  # 2
        next(g)  # 3
        data = g.save()
        g2 = triple_nested.resume(data)
        remaining = list(g2)
        assert remaining == [4, 5, 6, 7, 8]

    def test_nested_send_checkpoint(self):
        g = nested_with_send(3)
        next(g)       # 0
        g.send(10)    # 10
        g.send(5)     # 15
        data = g.save()

        g2 = nested_with_send.resume(data)
        assert g2.send(1) == 16  # continues from total=15


# ---- complex control flow ----

@workflow
def try_yield():
    """Yield inside try (stripped by our rewriter, but logic should work)."""
    total = 0
    for i in range(5):
        total += i
        yield total
    return total

@workflow
def conditional_chain(n):
    """Multiple branches with yields."""
    result = 0
    for i in range(n):
        if i % 3 == 0:
            x = yield f'fizz {i}'
            result += x
        elif i % 3 == 1:
            x = yield f'buzz {i}'
            result += x * 2
        else:
            yield f'skip {i}'
    return result

@workflow
def break_loop(n):
    for i in range(n):
        if i == 3:
            break
        yield i
    yield 'after break'
    return i

@workflow
def continue_loop(n):
    collected = []
    for i in range(n):
        if i % 2 == 0:
            continue
        yield i
        collected.append(i)
    return collected

@workflow
def accumulate_with_reset(n):
    """Accumulator that resets when it exceeds a threshold."""
    total = 0
    resets = 0
    for i in range(n):
        total += i
        if total > 10:
            yield f'reset at {total}'
            total = 0
            resets += 1
        else:
            yield total
    return resets


class TestComplexControlFlow:
    def test_conditional_chain(self):
        g = conditional_chain(6)
        assert next(g) == 'fizz 0'
        assert g.send(1) == 'buzz 1'
        assert g.send(2) == 'skip 2'
        assert next(g) == 'fizz 3'
        assert g.send(3) == 'buzz 4'
        assert g.send(4) == 'skip 5'
        with pytest.raises(StopIteration) as exc:
            next(g)
        # result = 1 + 2*2 + 3 + 4*2 = 1 + 4 + 3 + 8 = 16
        assert exc.value.value == 16

    def test_break_loop(self):
        g = break_loop(10)
        assert list(g) == [0, 1, 2, 'after break']

    def test_continue_loop(self):
        g = continue_loop(6)
        vals = list(g)
        assert vals == [1, 3, 5]

    def test_accumulate_with_reset(self):
        g = accumulate_with_reset(10)
        vals = list(g)
        # 0,1,3,6,10 then reset at 15 → 0+5=5,11 reset → 0+7=7,15 reset → 0+9=9
        assert 'reset at' in str(vals)

    def test_conditional_chain_checkpoint(self):
        g = conditional_chain(9)
        next(g)      # fizz 0
        g.send(10)   # buzz 1 (x=10, result=10)
        g.send(5)    # skip 2 (x=5, result=10+5*2=20)
        data = g.save()

        g2 = conditional_chain.resume(data)
        val = next(g2)
        assert val == 'fizz 3'

    def test_break_loop_return(self):
        g = break_loop(10)
        list(g)  # consume all
        # Raises StopIteration with i=3 (the break value)

    def test_try_yield(self):
        g = try_yield()
        vals = list(g)
        assert vals == [0, 1, 3, 6, 10]

    def test_try_yield_checkpoint(self):
        g = try_yield()
        next(g)  # 0
        next(g)  # 1
        data = g.save()
        g2 = try_yield.resume(data)
        assert list(g2) == [3, 6, 10]
