"""Token cost calculation for LLM models.

Prices per million tokens (USD).
"""

PRICING = {
    # Anthropic — https://docs.anthropic.com/en/docs/about-claude/pricing
    'claude-sonnet-4-20250514': {'input': 3.0, 'output': 15.0, 'cache_write': 3.75, 'cache_read': 0.30},
    'claude-opus-4-20250514': {'input': 15.0, 'output': 75.0, 'cache_write': 18.75, 'cache_read': 1.50},
    'claude-haiku-4-20250514': {'input': 0.80, 'output': 4.0, 'cache_write': 1.0, 'cache_read': 0.08},
    'claude-3-5-sonnet-20241022': {'input': 3.0, 'output': 15.0, 'cache_write': 3.75, 'cache_read': 0.30},
    'claude-3-5-haiku-20241022': {'input': 0.80, 'output': 4.0, 'cache_write': 1.0, 'cache_read': 0.08},
    'sonnet': {'input': 3.0, 'output': 15.0, 'cache_write': 3.75, 'cache_read': 0.30},
    'opus': {'input': 15.0, 'output': 75.0, 'cache_write': 18.75, 'cache_read': 1.50},
    'haiku': {'input': 0.80, 'output': 4.0, 'cache_write': 1.0, 'cache_read': 0.08},
    # OpenAI — https://developers.openai.com/api/docs/pricing
    'gpt-5.4': {'input': 2.50, 'output': 15.0, 'cache_write': 0, 'cache_read': 0.25},
    'gpt-5.4-mini': {'input': 0.75, 'output': 4.50, 'cache_write': 0, 'cache_read': 0.075},
    'gpt-5.4-nano': {'input': 0.20, 'output': 1.25, 'cache_write': 0, 'cache_read': 0.02},
    'gpt-5.2': {'input': 1.75, 'output': 14.0, 'cache_write': 0, 'cache_read': 0.175},
    'gpt-5.1': {'input': 1.25, 'output': 10.0, 'cache_write': 0, 'cache_read': 0.125},
    'gpt-5': {'input': 1.25, 'output': 10.0, 'cache_write': 0, 'cache_read': 0.125},
    'gpt-5-mini': {'input': 0.25, 'output': 2.0, 'cache_write': 0, 'cache_read': 0.025},
    'gpt-5-nano': {'input': 0.05, 'output': 0.40, 'cache_write': 0, 'cache_read': 0.005},
    'gpt-4.1': {'input': 2.0, 'output': 8.0, 'cache_write': 0, 'cache_read': 0.50},
    'gpt-4.1-mini': {'input': 0.40, 'output': 1.60, 'cache_write': 0, 'cache_read': 0.10},
    'gpt-4.1-nano': {'input': 0.10, 'output': 0.40, 'cache_write': 0, 'cache_read': 0.025},
    'gpt-4o': {'input': 2.50, 'output': 10.0, 'cache_write': 0, 'cache_read': 1.25},
    'gpt-4o-mini': {'input': 0.15, 'output': 0.60, 'cache_write': 0, 'cache_read': 0.075},
    'o3': {'input': 2.0, 'output': 8.0, 'cache_write': 0, 'cache_read': 0.50},
    'o4-mini': {'input': 1.10, 'output': 4.40, 'cache_write': 0, 'cache_read': 0.275},
    'o3-mini': {'input': 1.10, 'output': 4.40, 'cache_write': 0, 'cache_read': 0.55},
    'o1': {'input': 15.0, 'output': 60.0, 'cache_write': 0, 'cache_read': 7.50},
    'o1-mini': {'input': 1.10, 'output': 4.40, 'cache_write': 0, 'cache_read': 0.55},
}

DEFAULT_PRICING = {'input': 3.0, 'output': 15.0, 'cache_write': 3.75, 'cache_read': 0.30}


def compute_cost(
    model: str,
    input_tokens: int = 0,
    output_tokens: int = 0,
    cache_creation_input_tokens: int = 0,
    cache_read_input_tokens: int = 0,
) -> float:
    """Compute cost in USD for a given model and token counts."""
    if '/' in model:
        model = model.split('/', 1)[1]
    prices = PRICING.get(model, DEFAULT_PRICING)
    cost = (
        input_tokens * prices['input']
        + output_tokens * prices['output']
        + cache_creation_input_tokens * prices['cache_write']
        + cache_read_input_tokens * prices['cache_read']
    ) / 1_000_000
    return cost
