"""Token cost calculation for LLM models."""

# Prices per million tokens (USD)
# https://docs.anthropic.com/en/docs/about-claude/pricing
PRICING = {
    'claude-sonnet-4-20250514': {'input': 3.0, 'output': 15.0, 'cache_write': 3.75, 'cache_read': 0.30},
    'claude-opus-4-20250514': {'input': 15.0, 'output': 75.0, 'cache_write': 18.75, 'cache_read': 1.50},
    'claude-haiku-4-20250514': {'input': 0.80, 'output': 4.0, 'cache_write': 1.0, 'cache_read': 0.08},
    'claude-3-5-sonnet-20241022': {'input': 3.0, 'output': 15.0, 'cache_write': 3.75, 'cache_read': 0.30},
    'claude-3-5-haiku-20241022': {'input': 0.80, 'output': 4.0, 'cache_write': 1.0, 'cache_read': 0.08},
    # Short aliases used by Claude Code
    'sonnet': {'input': 3.0, 'output': 15.0, 'cache_write': 3.75, 'cache_read': 0.30},
    'opus': {'input': 15.0, 'output': 75.0, 'cache_write': 18.75, 'cache_read': 1.50},
    'haiku': {'input': 0.80, 'output': 4.0, 'cache_write': 1.0, 'cache_read': 0.08},
}

# Default pricing for unknown models
DEFAULT_PRICING = {'input': 3.0, 'output': 15.0, 'cache_write': 3.75, 'cache_read': 0.30}


def compute_cost(
    model: str,
    input_tokens: int = 0,
    output_tokens: int = 0,
    cache_creation_input_tokens: int = 0,
    cache_read_input_tokens: int = 0,
) -> float:
    """Compute cost in USD for a given model and token counts."""
    # Strip provider prefix (e.g. 'anthropic/claude-sonnet-4-20250514')
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
