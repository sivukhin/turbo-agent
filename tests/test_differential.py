"""Differential tests: verify that checkpoint/resume at every yield point
produces the same sequence of values as a straight-through run."""

import pickle
import pytest
from workflows import workflow


# ---- test workflows ----

@workflow
def counter(n):
    for i in range(n):
        yield i
    return n

@workflow
def nested_loops(n, m):
    for i in range(n):
        for j in range(m):
            yield i * m + j
    return n * m

@workflow
def accumulator_send(n):
    total = 0
    for i in range(n):
        x = yield total
        total += x + i
    return total

@workflow
def expr_yield():
    a = 1
    b = 2
    c = a + (yield b) + b
    yield c
    return c

@workflow
def conditional(n):
    for i in range(n):
        if i % 2 == 0:
            yield ('even', i)
        else:
            yield ('odd', i)
    return n

@workflow
def nested_expr():
    result = (yield 10) + (yield 20) + (yield 30)
    yield result
    return result

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
def break_continue(n):
    collected = []
    for i in range(n):
        if i == 7:
            break
        if i % 3 == 0:
            continue
        yield i
        collected.append(i)
    return collected

@workflow
def while_loop(n):
    i = 0
    while i < n:
        yield i * i
        i += 1
    return i


def collect_values(gen, send_values=None):
    """Run generator to completion, return list of yielded values and the return value.
    send_values: list of values to send (index-aligned with yields). None → next()."""
    values = []
    send_values = send_values or []
    idx = 0
    try:
        val = next(gen)
        values.append(val)
        idx += 1
        while True:
            sv = send_values[idx - 1] if idx - 1 < len(send_values) else None
            if sv is not None:
                val = gen.send(sv)
            else:
                val = next(gen)
            values.append(val)
            idx += 1
    except StopIteration as e:
        return values, e.value


def collect_with_resume_at(wf_func, args, resume_at, send_values=None):
    """Run generator, checkpoint at yield index `resume_at`, resume, collect remaining."""
    send_values = send_values or []
    gen = wf_func(*args)

    # Collect up to resume_at
    before = []
    for i in range(resume_at + 1):
        if i == 0:
            val = next(gen)
        else:
            sv = send_values[i - 1] if i - 1 < len(send_values) else None
            val = gen.send(sv) if sv is not None else next(gen)
        before.append(val)

    # Checkpoint and resume
    data = gen.save()
    assert data is not None, f"save() returned None at yield {resume_at}"
    gen2 = wf_func.resume(data)

    # Collect remaining
    after = []
    idx = resume_at + 1
    try:
        while True:
            sv = send_values[idx - 1] if idx - 1 < len(send_values) else None
            val = gen2.send(sv) if sv is not None else next(gen2)
            after.append(val)
            idx += 1
    except StopIteration as e:
        return_val = e.value

    return before + after, return_val


# ---- parametrized differential tests ----

class TestDifferentialCounter:
    @pytest.mark.parametrize("n", [1, 2, 5, 10, 20])
    def test_counter(self, n):
        baseline_vals, baseline_ret = collect_values(counter(n))
        for resume_at in range(len(baseline_vals)):
            vals, ret = collect_with_resume_at(counter, [n], resume_at)
            assert vals == baseline_vals, f"Mismatch resuming at yield {resume_at}"
            assert ret == baseline_ret, f"Return mismatch resuming at yield {resume_at}"


class TestDifferentialNestedLoops:
    @pytest.mark.parametrize("n,m", [(2, 2), (3, 3), (1, 5), (4, 2)])
    def test_nested_loops(self, n, m):
        baseline_vals, baseline_ret = collect_values(nested_loops(n, m))
        for resume_at in range(len(baseline_vals)):
            vals, ret = collect_with_resume_at(nested_loops, [n, m], resume_at)
            assert vals == baseline_vals
            assert ret == baseline_ret


class TestDifferentialTripleNested:
    @pytest.mark.parametrize("a,b,c", [(2, 2, 2), (3, 1, 2), (1, 3, 3)])
    def test_triple_nested(self, a, b, c):
        baseline_vals, baseline_ret = collect_values(triple_nested(a, b, c))
        for resume_at in range(len(baseline_vals)):
            vals, ret = collect_with_resume_at(triple_nested, [a, b, c], resume_at)
            assert vals == baseline_vals
            assert ret == baseline_ret


class TestDifferentialConditional:
    @pytest.mark.parametrize("n", [1, 4, 7, 10])
    def test_conditional(self, n):
        baseline_vals, baseline_ret = collect_values(conditional(n))
        for resume_at in range(len(baseline_vals)):
            vals, ret = collect_with_resume_at(conditional, [n], resume_at)
            assert vals == baseline_vals
            assert ret == baseline_ret


class TestDifferentialBreakContinue:
    @pytest.mark.parametrize("n", [5, 8, 10, 15])
    def test_break_continue(self, n):
        baseline_vals, baseline_ret = collect_values(break_continue(n))
        for resume_at in range(len(baseline_vals)):
            vals, ret = collect_with_resume_at(break_continue, [n], resume_at)
            assert vals == baseline_vals
            assert ret == baseline_ret


class TestDifferentialWhileLoop:
    @pytest.mark.parametrize("n", [1, 3, 5, 8])
    def test_while_loop(self, n):
        baseline_vals, baseline_ret = collect_values(while_loop(n))
        for resume_at in range(len(baseline_vals)):
            vals, ret = collect_with_resume_at(while_loop, [n], resume_at)
            assert vals == baseline_vals
            assert ret == baseline_ret


class TestDifferentialWithSend:
    @pytest.mark.parametrize("n", [2, 3, 5])
    def test_accumulator_send(self, n):
        send_vals = [10] * n  # send 10 at each yield
        baseline_vals, baseline_ret = collect_values(accumulator_send(n), send_vals)
        for resume_at in range(len(baseline_vals)):
            vals, ret = collect_with_resume_at(
                accumulator_send, [n], resume_at, send_vals
            )
            assert vals == baseline_vals, f"Mismatch at resume_at={resume_at}"
            assert ret == baseline_ret

    def test_expr_yield(self):
        send_vals = [100, None]  # send 100 to first yield, None to second
        baseline_vals, baseline_ret = collect_values(expr_yield(), send_vals)
        for resume_at in range(len(baseline_vals)):
            vals, ret = collect_with_resume_at(expr_yield, [], resume_at, send_vals)
            assert vals == baseline_vals
            assert ret == baseline_ret

    def test_nested_expr(self):
        send_vals = [10, 20, 30, None]
        baseline_vals, baseline_ret = collect_values(nested_expr(), send_vals)
        for resume_at in range(len(baseline_vals)):
            vals, ret = collect_with_resume_at(nested_expr, [], resume_at, send_vals)
            assert vals == baseline_vals
            assert ret == baseline_ret


class TestDifferentialMultipleResumes:
    """Resume at every point, then resume the resumed generator again."""

    @pytest.mark.parametrize("n", [5, 10])
    def test_double_resume(self, n):
        """Checkpoint, resume, advance, checkpoint again, resume — same output."""
        baseline_vals, baseline_ret = collect_values(counter(n))

        for first_break in range(len(baseline_vals)):
            for second_break in range(first_break + 1, len(baseline_vals)):
                g = counter(n)
                before = []
                for i in range(first_break + 1):
                    before.append(next(g))
                data1 = g.save()

                g2 = counter.resume(data1)
                middle = []
                for i in range(second_break - first_break):
                    middle.append(next(g2))
                data2 = g2.save()

                g3 = counter.resume(data2)
                after = list(g3)

                combined = before + middle + after
                assert combined == baseline_vals, (
                    f"Mismatch: first_break={first_break}, second_break={second_break}"
                )


class TestDifferentialPickleRoundtrip:
    """Verify that pickle serialization doesn't corrupt checkpoint data."""

    @pytest.mark.parametrize("n,m", [(3, 3), (4, 2)])
    def test_pickle_roundtrip(self, n, m):
        baseline_vals, baseline_ret = collect_values(nested_loops(n, m))

        for resume_at in range(len(baseline_vals)):
            g = nested_loops(n, m)
            for i in range(resume_at + 1):
                next(g)
            data = g.save()

            # Triple pickle roundtrip
            cp = pickle.loads(pickle.dumps(pickle.loads(pickle.dumps(
                pickle.loads(data)
            ))))
            g2 = nested_loops.resume(cp)
            after = list(g2)
            combined = baseline_vals[:resume_at + 1] + after
            assert combined == baseline_vals
