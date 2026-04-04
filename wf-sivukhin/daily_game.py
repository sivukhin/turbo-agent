"""Daily browser puzzle game generator.

Searches for interesting combinatorial facts (OEIS, Wikipedia, etc.),
then uses Claude Code to create an original puzzle game inspired by them.

Games are saved to wf-sivukhin/games/{date}-{nn}/index.html

Usage:
    uv run main.py run wf-sivukhin/daily_game.py:daily_game --workdir .workspace
    uv run main.py run wf-sivukhin/daily_game.py:daily_game '"graph coloring"' --workdir .workspace
"""

import json
import os
import glob
from datetime import date

from workflows import (
    workflow,
    wait,
    conv_append,
    shell,
    shell_stream_start,
    shell_stream_next,
)
from workflows.isolation import HostIsolation, DockerIsolation

DOCKER_IMAGE = "turbo-game"
CLAUDE_MODEL = "sonnet"

GAMES_DIR = os.path.join(os.path.dirname(__file__), "games")

RESEARCH_PROMPT = """\
I need a quick idea for an original combinatorial puzzle game for the browser.

Spend at most 2-3 minutes. Look up ONE interesting concept from OEIS or Wikipedia — \
something like permutations, tilings, graph coloring, peg solitaire variants, \
partition puzzles, or constraint problems.

Do NOT exhaustively search. Pick the FIRST good idea that is:
- Not a well-known game (no sudoku, 2048, minesweeper, wordle)
- Has simple rules (explainable in 2-3 sentences)
- Has a clear win condition and scoring

Write your findings and final choice to /workspace/research.md.
Then STOP. Do not iterate or refine — just pick one and write it.
"""

GAME_PROMPT = """\
Read /workspace/research.md for the puzzle concept you researched.

Now implement it as a browser puzzle game. Requirements:

FORMAT:
- Single index.html file, all CSS and JS inline
- Use Turso (libSQL WASM) for persistence via OPFS:
  <script src="https://unpkg.com/@libsql/client@0.14/web/libsql-experimental.js"></script>
- Store game state in SQLite. Implement undo/redo via a history table:
  - history(id INTEGER PRIMARY KEY, state_json TEXT, created_at TEXT)
  - On each move: INSERT into history
  - Undo/Redo: navigate history entries
  - Show undo/redo buttons

GAMEPLAY:
- Clear, simple rules — explainable in 2-3 sentences shown in the UI
- A well-defined SCORE function (moves, time, or puzzle-specific metric)
- Display current score prominently
- "New Game" button generates a fresh random instance
- "Restart" button resets current instance to initial state
- Multiple difficulty levels (easy/medium/hard) that affect board size or constraints
- Win detection — show congratulations + final score when solved

DESIGN:
- Modern, clean UI — nice colors, rounded corners, subtle shadows
- Responsive — works on mobile and desktop
- Smooth CSS transitions on state changes
- Touch-friendly tap targets (min 44px)
- Dark/light theme toggle

Write the complete game to /workspace/game/index.html.
After writing, verify the file exists and has reasonable content (>5KB).
"""


def _private_env():
    return {"ANTHROPIC_API_KEY": os.environ.get("ANTHROPIC_API_KEY", "")}


def _next_game_dir():
    today = date.today().isoformat()
    existing = sorted(glob.glob(os.path.join(GAMES_DIR, f"{today}-*")))
    nn = len(existing) + 1
    return os.path.join(GAMES_DIR, f"{today}-{nn:02d}")


@workflow
def run_claude_code(prompt):
    """Run Claude Code in Docker, return final result text."""
    stream = yield shell_stream_start(
        [
            "claude",
            "--model", CLAUDE_MODEL,
            "--output-format", "stream-json",
            "--verbose",
            "--dangerously-skip-permissions",
            "-p", prompt,
        ],
        isolation=DockerIsolation(image=DOCKER_IMAGE, network="host"),
        public_env={"IS_SANDBOX": "1"},
        private_env=_private_env(),
        meta={"claude_code": True},
    )

    final_text = ""
    while True:
        raw = yield shell_stream_next(stream, private_env=_private_env())
        for line in raw.stdout:
            try:
                event = json.loads(line)
                if event.get("type") == "result" and event.get("result"):
                    final_text = event["result"]
            except (json.JSONDecodeError, KeyError):
                pass
        if raw.finished:
            break

    return final_text


@workflow
def daily_game():
    """Generate a daily browser puzzle game.

    Args:
        theme: optional hint for the combinatorial concept to explore
               (e.g. "graph coloring", "tilings", "permutation groups").
               If empty, Claude researches freely.
    """
    
    # Phase 1: Research combinatorial concepts
    yield conv_append(role="user", content=f"Research interesting combinatorial puzzles")
    yield wait(run_claude_code(RESEARCH_PROMPT))

    # Check research output
    check = yield shell("test -f research.md && wc -c research.md", isolation=HostIsolation())
    if check.exit_code != 0:
        yield conv_append(role="assistant", content="Research phase failed — no research.md produced")
        return "research failed"

    yield conv_append(role="assistant", content=f"Research complete. Building game...")

    # Phase 2: Build the game
    yield wait(run_claude_code(GAME_PROMPT))

    # Check if game was created
    check = yield shell("test -f game/index.html && wc -c game/index.html", isolation=HostIsolation())
    if check.exit_code != 0:
        yield conv_append(role="assistant", content="Game generation failed — no index.html produced")
        return "game failed"

    size = check.stdout.strip().split()[0]
    yield conv_append(role="assistant", content=f"Game created ({size} bytes)")

    # Copy to games directory
    game_dir = _next_game_dir()
    copy = yield shell(
        f"mkdir -p {game_dir} && cp game/index.html {game_dir}/ && cp research.md {game_dir}/",
        isolation=HostIsolation(),
    )
    if copy.exit_code != 0:
        yield conv_append(role="assistant", content=f"Failed to copy game: {copy.stderr}")
        return "copy failed"

    yield conv_append(role="assistant", content=f"Game saved to {game_dir}/index.html")
    return game_dir
