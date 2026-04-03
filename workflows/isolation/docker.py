import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from workflows.isolation.base import ShellResult


@dataclass
class DockerIsolation:
    """Runs commands in Docker containers with the workdir bind-mounted."""

    image: str = 'alpine:latest'
    network: str = 'none'  # 'none' | 'host'

    def run_shell(self, workdir: Path, command: str, env: dict | None = None) -> ShellResult:
        uid, gid = os.getuid(), os.getgid()
        cmd = [
            'docker', 'run', '--rm',
            f'--network={self.network}',
            f'--user={uid}:{gid}',
            '-v', f'{workdir.resolve()}:/workspace',
            '-w', '/workspace',
        ]
        for k, v in (env or {}).items():
            cmd.extend(['-e', f'{k}={v}'])
        cmd.extend([self.image, 'sh', '-c', command])
        result = subprocess.run(cmd, capture_output=True, text=True)
        return ShellResult(
            exit_code=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
        )
