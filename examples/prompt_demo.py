from workflows import workflow, conv_append
from workflows.ops import user_prompt


@workflow
def greeter():
    """Ask user's name, greet them."""
    name = yield user_prompt()
    yield conv_append(role='user', content=f'My name is {name}')
    yield conv_append(role='assistant', content=f'Hello, {name}!')
    return f'Hello, {name}!'


@workflow
def quiz():
    """Simple interactive quiz."""
    score = 0

    answer = yield user_prompt()
    if answer.strip() == '4':
        score += 1
        yield conv_append(role='assistant', content='Correct!')
    else:
        yield conv_append(role='assistant', content=f'Wrong, the answer is 4. You said: {answer}')

    answer = yield user_prompt()
    if 'paris' in answer.strip().lower():
        score += 1
        yield conv_append(role='assistant', content='Correct!')
    else:
        yield conv_append(role='assistant', content=f'Wrong, the answer is Paris. You said: {answer}')

    return f'Score: {score}/2'
