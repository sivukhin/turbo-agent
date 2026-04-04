FROM node:22-bookworm

# System tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl git ca-certificates python3 \
    && rm -rf /var/lib/apt/lists/*

# Bun
RUN curl -fsSL https://bun.sh/install | bash
ENV PATH="/root/.bun/bin:${PATH}"

# Claude Code + Turso agent skills
RUN npm install -g @anthropic-ai/claude-code
RUN npx skills add tursodatabase/agent-skills -g -a claude-code -s '*' -y

# Copy skills to /etc/claude-skills so they're available regardless of HOME
RUN cp -r /root/.claude/skills /etc/claude-skills 2>/dev/null || true

# Entrypoint script that ensures skills are available in current HOME
RUN echo '#!/bin/sh\nmkdir -p "$HOME/.claude"\nln -sfn /etc/claude-skills "$HOME/.claude/skills"\nexec "$@"' > /entrypoint.sh \
    && chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
WORKDIR /workspace
