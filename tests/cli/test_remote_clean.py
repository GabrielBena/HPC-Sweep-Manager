"""CLI tests for `hsm remote clean`."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import pytest
import yaml

from hpc_sweep_manager.cli import remote as remote_cli


class FakeResult:
    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = ""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class FakeConn:
    """Async context manager that records run() calls."""

    def __init__(self):
        self.run_calls: list[str] = []
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.closed = True
        return False

    async def run(self, cmd: str, *, check: bool = False, input: Optional[str] = None):
        self.run_calls.append(cmd)
        return FakeResult(returncode=0)


@pytest.fixture
def fake_ssh(monkeypatch):
    """Replace create_ssh_connection with a fake-conn factory that records the host it would dial."""
    state = {"last_host": None, "last_key": None, "last_port": None, "conn": None}

    async def _fake_connect(host, ssh_key=None, ssh_port=None):
        state["last_host"] = host
        state["last_key"] = ssh_key
        state["last_port"] = ssh_port
        conn = FakeConn()
        state["conn"] = conn
        return conn

    # Patch the symbol imported at function-resolution time in cli/remote.py.
    monkeypatch.setattr(
        "hpc_sweep_manager.core.remote.discovery.create_ssh_connection",
        _fake_connect,
    )
    return state


def _write_hsm_config(cwd: Path, payload: dict) -> None:
    (cwd / ".hsm").mkdir(exist_ok=True)
    (cwd / ".hsm" / "config.yaml").write_text(yaml.safe_dump(payload))


class TestRemoteClean:
    def test_default_removes_current_project_dir(self, tmp_path, monkeypatch, fake_ssh):
        monkeypatch.chdir(tmp_path)
        # No hsm_config — bare alias path.
        from click.testing import CliRunner

        runner = CliRunner()
        result = runner.invoke(remote_cli.remote, ["clean", "anahita", "-y"])
        assert result.exit_code == 0, result.output
        assert fake_ssh["last_host"] == "anahita"
        # rm -rf <remote_root>/<cwd_basename>
        cmd = fake_ssh["conn"].run_calls[0]
        assert cmd.startswith("rm -rf ")
        assert cmd.endswith(f"~/.hsm/runs/{tmp_path.name}")

    def test_all_projects_removes_remote_root(self, tmp_path, monkeypatch, fake_ssh):
        monkeypatch.chdir(tmp_path)
        from click.testing import CliRunner

        runner = CliRunner()
        result = runner.invoke(
            remote_cli.remote, ["clean", "anahita", "-y", "--all-projects"]
        )
        assert result.exit_code == 0
        cmd = fake_ssh["conn"].run_calls[0]
        assert cmd == "rm -rf ~/.hsm/runs"

    def test_registered_remote_uses_overrides(self, tmp_path, monkeypatch, fake_ssh):
        monkeypatch.chdir(tmp_path)
        _write_hsm_config(
            tmp_path,
            {
                "distributed": {
                    "remote_root": "/scratch/hsm",
                    "remotes": {
                        "anahita": {
                            "host": "anahita.lab",
                            "ssh_key": "~/.ssh/foo",
                            "ssh_port": 2222,
                            "remote_root": "/scratch/private",
                        }
                    },
                }
            },
        )

        from click.testing import CliRunner

        runner = CliRunner()
        result = runner.invoke(remote_cli.remote, ["clean", "anahita", "-y"])
        assert result.exit_code == 0
        # Per-remote remote_root overrides global.
        cmd = fake_ssh["conn"].run_calls[0]
        assert cmd.startswith("rm -rf /scratch/private/")
        # Explicit host / key / port make it through.
        assert fake_ssh["last_host"] == "anahita.lab"
        assert fake_ssh["last_key"] == "~/.ssh/foo"
        assert fake_ssh["last_port"] == 2222

    def test_confirmation_declined_does_not_invoke_ssh(self, tmp_path, monkeypatch, fake_ssh):
        monkeypatch.chdir(tmp_path)
        from click.testing import CliRunner

        runner = CliRunner()
        # No -y flag → prompt; "n" declines.
        result = runner.invoke(remote_cli.remote, ["clean", "anahita"], input="n\n")
        assert result.exit_code == 0
        # Cancelled before SSH.
        assert fake_ssh["last_host"] is None
