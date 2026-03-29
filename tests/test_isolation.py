"""Tests for isolation implementations and storage modes."""

import subprocess
import pytest
from pathlib import Path
from workflows.isolation import (
    HostIsolation, DockerIsolation, ShellResult,
    StorageConfig, setup_child_workspace, scan_git_branches,
)


def _has_docker():
    try:
        subprocess.run(['docker', 'info'], capture_output=True, check=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


requires_docker = pytest.mark.skipif(not _has_docker(), reason='Docker not available')


def _init_git_repo(path: Path, branch='main'):
    """Helper: create a git repo with an initial commit."""
    subprocess.run(['git', 'init', '-b', branch], cwd=str(path), capture_output=True, check=True)
    subprocess.run(['git', 'config', 'user.email', 'test@test.com'], cwd=str(path), capture_output=True, check=True)
    subprocess.run(['git', 'config', 'user.name', 'Test'], cwd=str(path), capture_output=True, check=True)
    (path / '.gitignore').write_text('untracked/\n')
    subprocess.run(['git', 'add', '.'], cwd=str(path), capture_output=True, check=True)
    subprocess.run(['git', 'commit', '-m', 'init'], cwd=str(path), capture_output=True, check=True)


# ---- HostIsolation ----

class TestHostIsolation:
    def test_echo(self, tmp_path):
        host = HostIsolation()
        result = host.run_shell(tmp_path, 'echo hello')
        assert isinstance(result, ShellResult)
        assert result.exit_code == 0
        assert result.stdout.strip() == 'hello'

    def test_exit_code(self, tmp_path):
        host = HostIsolation()
        result = host.run_shell(tmp_path, 'exit 42')
        assert result.exit_code == 42

    def test_writes_to_workdir(self, tmp_path):
        host = HostIsolation()
        host.run_shell(tmp_path, 'echo data > output.txt')
        assert (tmp_path / 'output.txt').read_text().strip() == 'data'

    def test_reads_from_workdir(self, tmp_path):
        (tmp_path / 'input.txt').write_text('hello')
        host = HostIsolation()
        result = host.run_shell(tmp_path, 'cat input.txt')
        assert result.stdout.strip() == 'hello'

    def test_stderr(self, tmp_path):
        host = HostIsolation()
        result = host.run_shell(tmp_path, 'echo err >&2')
        assert result.stderr.strip() == 'err'


# ---- DockerIsolation ----

@requires_docker
class TestDockerIsolation:
    def test_echo(self, tmp_path):
        docker = DockerIsolation()
        result = docker.run_shell(tmp_path, 'echo hello')
        assert result.exit_code == 0
        assert result.stdout.strip() == 'hello'

    def test_writes_to_workdir(self, tmp_path):
        docker = DockerIsolation()
        docker.run_shell(tmp_path, 'echo data > output.txt')
        assert (tmp_path / 'output.txt').read_text().strip() == 'data'

    def test_reads_from_workdir(self, tmp_path):
        (tmp_path / 'input.txt').write_text('from host')
        docker = DockerIsolation()
        result = docker.run_shell(tmp_path, 'cat input.txt')
        assert result.stdout.strip() == 'from host'

    def test_exit_code(self, tmp_path):
        docker = DockerIsolation()
        result = docker.run_shell(tmp_path, 'exit 7')
        assert result.exit_code == 7

    def test_network_none(self, tmp_path):
        docker = DockerIsolation(network='none')
        result = docker.run_shell(tmp_path, 'wget -q -O- http://example.com 2>&1 || echo FAIL')
        assert 'FAIL' in result.stdout or result.exit_code != 0

    def test_custom_image(self, tmp_path):
        docker = DockerIsolation(image='alpine:latest')
        result = docker.run_shell(tmp_path, 'cat /etc/os-release')
        assert 'Alpine' in result.stdout


# ---- StorageConfig: same ----

class TestStorageSame:
    def test_same_returns_parent_dir(self, tmp_path):
        parent = tmp_path / 'parent'
        parent.mkdir()
        (parent / 'file.txt').write_text('hello')
        config = StorageConfig(mode='same')
        child_dir, branches = setup_child_workspace(parent, tmp_path / 'child', None, config)
        assert child_dir == parent
        assert (child_dir / 'file.txt').read_text() == 'hello'

    def test_same_shares_writes(self, tmp_path):
        parent = tmp_path / 'parent'
        parent.mkdir()
        config = StorageConfig(mode='same')
        child_dir, _ = setup_child_workspace(parent, tmp_path / 'child', None, config)
        (child_dir / 'new.txt').write_text('from child')
        assert (parent / 'new.txt').read_text() == 'from child'


# ---- StorageConfig: copy-full ----

class TestStorageCopyFull:
    def test_copies_all_files(self, tmp_path):
        parent = tmp_path / 'parent'
        parent.mkdir()
        (parent / 'a.txt').write_text('aaa')
        (parent / 'sub').mkdir()
        (parent / 'sub' / 'b.txt').write_text('bbb')
        config = StorageConfig(mode='copy-full')
        child_dir, _ = setup_child_workspace(parent, tmp_path / 'child', None, config)
        assert child_dir == tmp_path / 'child'
        assert (child_dir / 'a.txt').read_text() == 'aaa'
        assert (child_dir / 'sub' / 'b.txt').read_text() == 'bbb'

    def test_copy_full_isolates_writes(self, tmp_path):
        parent = tmp_path / 'parent'
        parent.mkdir()
        (parent / 'file.txt').write_text('original')
        config = StorageConfig(mode='copy-full')
        child_dir, _ = setup_child_workspace(parent, tmp_path / 'child', None, config)
        (child_dir / 'file.txt').write_text('modified')
        assert (parent / 'file.txt').read_text() == 'original'
        assert (child_dir / 'file.txt').read_text() == 'modified'

    def test_copy_full_idempotent(self, tmp_path):
        parent = tmp_path / 'parent'
        parent.mkdir()
        (parent / 'file.txt').write_text('hello')
        config = StorageConfig(mode='copy-full')
        child_dir, _ = setup_child_workspace(parent, tmp_path / 'child', None, config)
        (child_dir / 'extra.txt').write_text('extra')
        # Second call should not overwrite
        child_dir2, _ = setup_child_workspace(parent, tmp_path / 'child', None, config)
        assert (child_dir2 / 'extra.txt').read_text() == 'extra'


# ---- StorageConfig: copy-git ----

class TestStorageCopyGit:
    def test_copies_tracked_files_only(self, tmp_path):
        parent = tmp_path / 'parent'
        parent.mkdir()
        _init_git_repo(parent)
        (parent / 'tracked.txt').write_text('tracked')
        subprocess.run(['git', 'add', 'tracked.txt'], cwd=str(parent), capture_output=True, check=True)
        subprocess.run(['git', 'commit', '-m', 'add tracked'], cwd=str(parent), capture_output=True, check=True)
        (parent / 'untracked').mkdir()
        (parent / 'untracked' / 'secret.txt').write_text('secret')

        config = StorageConfig(mode='copy-git')
        child_dir, branches = setup_child_workspace(parent, tmp_path / 'child', None, config)
        assert (child_dir / 'tracked.txt').read_text() == 'tracked'
        assert (child_dir / '.git').is_dir()
        assert not (child_dir / 'untracked' / 'secret.txt').exists()

    def test_copies_gitignore(self, tmp_path):
        parent = tmp_path / 'parent'
        parent.mkdir()
        _init_git_repo(parent)
        config = StorageConfig(mode='copy-git')
        child_dir, _ = setup_child_workspace(parent, tmp_path / 'child', None, config)
        assert (child_dir / '.gitignore').exists()

    def test_copy_git_scans_branches(self, tmp_path):
        parent = tmp_path / 'parent'
        parent.mkdir()
        _init_git_repo(parent, branch='main')
        config = StorageConfig(mode='copy-git')
        _, branches = setup_child_workspace(parent, tmp_path / 'child', None, config)
        assert branches.get('.') == 'main'


# ---- StorageConfig: branch ----

class TestStorageBranch:
    def test_creates_new_branch(self, tmp_path):
        parent = tmp_path / 'repo'
        parent.mkdir()
        _init_git_repo(parent, branch='main')
        parent_branches = {'.': 'main'}
        config = StorageConfig(mode='branch', branch_suffix='child-work')
        child_dir, child_branches = setup_child_workspace(
            parent, tmp_path / 'unused', parent_branches, config,
        )
        assert child_dir == parent  # same dir
        assert child_branches['.'] == 'main-child-work'
        # Verify git is on the new branch
        result = subprocess.run(
            ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
            cwd=str(parent), capture_output=True, text=True,
        )
        assert result.stdout.strip() == 'main-child-work'

    def test_branch_shares_directory(self, tmp_path):
        parent = tmp_path / 'repo'
        parent.mkdir()
        _init_git_repo(parent, branch='main')
        (parent / 'file.txt').write_text('hello')
        subprocess.run(['git', 'add', '.'], cwd=str(parent), capture_output=True, check=True)
        subprocess.run(['git', 'commit', '-m', 'add file'], cwd=str(parent), capture_output=True, check=True)

        config = StorageConfig(mode='branch', branch_suffix='feature')
        child_dir, _ = setup_child_workspace(parent, tmp_path / 'unused', {'.': 'main'}, config)
        assert (child_dir / 'file.txt').read_text() == 'hello'

    def test_branch_requires_suffix(self, tmp_path):
        parent = tmp_path / 'repo'
        parent.mkdir()
        _init_git_repo(parent)
        config = StorageConfig(mode='branch')
        with pytest.raises(ValueError, match='branch_suffix'):
            setup_child_workspace(parent, tmp_path / 'unused', {'.': 'main'}, config)


# ---- scan_git_branches ----

class TestScanGitBranches:
    def test_single_repo(self, tmp_path):
        _init_git_repo(tmp_path, branch='develop')
        branches = scan_git_branches(tmp_path)
        assert branches == {'.': 'develop'}

    def test_nested_repos(self, tmp_path):
        _init_git_repo(tmp_path, branch='main')
        sub = tmp_path / 'sub' / 'repo'
        sub.mkdir(parents=True)
        _init_git_repo(sub, branch='feature')
        branches = scan_git_branches(tmp_path)
        assert branches['.'] == 'main'
        assert branches['sub/repo'] == 'feature'

    def test_no_git_repos(self, tmp_path):
        branches = scan_git_branches(tmp_path)
        assert branches == {}
