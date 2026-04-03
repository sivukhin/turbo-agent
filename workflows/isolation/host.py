import subprocess
from dataclasses import dataclass
from pathlib import Path
from workflows.isolation.base import ShellResult


@dataclass
class HostIsolation:
    """Runs commands directly on the host in the given workdir."""

    def run_shell(self, workdir: Path, command: str, env: dict | None = None) -> ShellResult:
        result = subprocess.run(
            ['sh', '-c', command],
            cwd=str(workdir),
            capture_output=True,
            text=True,
            env=env or None,
        )
        return ShellResult(
            exit_code=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
        )
