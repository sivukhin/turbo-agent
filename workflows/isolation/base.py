import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol


@dataclass
class ShellResult:
    exit_code: int
    stdout: str
    stderr: str


class Isolation(Protocol):
    """Protocol for running shell commands in a workspace directory."""
    def run_shell(self, workdir: Path, command: str) -> ShellResult: ...


@dataclass
class StorageConfig:
    """Configures how a child workflow's workspace relates to its parent's.

    Modes:
      'same'      — child uses the exact same directory as parent
      'copy-full' — child gets a full copy of parent's directory
      'copy-git'  — child gets a copy of only git-tracked files (including .git)
      'branch'    — child uses same directory but creates a new git branch
                    (branch_suffix is appended to parent's current branch name)
    """
    mode: str = 'same'
    branch_suffix: str | None = None  # required for 'branch' mode


def scan_git_branches(workdir: Path) -> dict[str, str]:
    """Scan workdir for .git directories and record current branch for each.

    Returns {relative_path: branch_name} where relative_path is '.' for the
    root repo or 'sub/dir' for nested repos.
    """
    branches = {}
    for git_dir in workdir.rglob('.git'):
        if not git_dir.is_dir():
            continue
        repo_dir = git_dir.parent
        rel = str(repo_dir.relative_to(workdir))
        result = subprocess.run(
            ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
            cwd=str(repo_dir),
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            branches[rel] = result.stdout.strip()
    return branches


def setup_child_workspace(
    parent_workdir: Path,
    child_workdir: Path,
    parent_branches: dict[str, str] | None,
    config: StorageConfig,
) -> tuple[Path, dict[str, str]]:
    """Create a child workspace from a parent directory.

    Returns (child_workdir, child_branches).
    """
    parent_branches = parent_branches or {}

    if config.mode == 'same':
        return parent_workdir, dict(parent_branches)

    elif config.mode == 'copy-full':
        if not child_workdir.exists():
            child_workdir.mkdir(parents=True, exist_ok=True)
            subprocess.run(
                ['cp', '-a', f'{parent_workdir}/.', str(child_workdir)],
                check=True, capture_output=True,
            )
        return child_workdir, scan_git_branches(child_workdir)

    elif config.mode == 'copy-git':
        if not child_workdir.exists():
            child_workdir.mkdir(parents=True, exist_ok=True)
            # Copy .git directory
            git_dir = parent_workdir / '.git'
            if git_dir.exists():
                subprocess.run(
                    ['cp', '-a', str(git_dir), str(child_workdir / '.git')],
                    check=True, capture_output=True,
                )
            # Copy only tracked files
            result = subprocess.run(
                ['git', 'ls-files', '-z'],
                cwd=str(parent_workdir),
                capture_output=True,
            )
            if result.returncode == 0 and result.stdout:
                files = result.stdout.rstrip(b'\0').split(b'\0')
                for f in files:
                    fname = f.decode()
                    src = parent_workdir / fname
                    dst = child_workdir / fname
                    if src.exists():
                        dst.parent.mkdir(parents=True, exist_ok=True)
                        subprocess.run(
                            ['cp', '-a', str(src), str(dst)],
                            check=True, capture_output=True,
                        )
        return child_workdir, scan_git_branches(child_workdir)

    elif config.mode == 'branch':
        if config.branch_suffix is None:
            raise ValueError("StorageConfig mode='branch' requires branch_suffix")
        new_branches = {}
        for rel_path, parent_branch in parent_branches.items():
            repo_dir = parent_workdir / rel_path if rel_path != '.' else parent_workdir
            child_branch = f'{parent_branch}-{config.branch_suffix}'
            subprocess.run(
                ['git', 'checkout', '-b', child_branch],
                cwd=str(repo_dir),
                capture_output=True, check=True,
            )
            new_branches[rel_path] = child_branch
        return parent_workdir, new_branches

    else:
        raise ValueError(f"Unknown storage mode: {config.mode}")
