"""CLI tests for `hsm remote clean`."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from hpc_sweep_manager.cli import remote as remote_cli


class FakeResult:
    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = ""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class FakeConn:
    """Async context manager that records run() calls.

    Answers clean's `echo <root>; echo "$HOME"` the way a remote shell would.
    """

    def __init__(self, home: str = "/home/u"):
        self.run_calls: list[str] = []
        self.closed = False
        self.home = home

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.closed = True
        return False

    async def run(self, cmd: str, *, check: bool = False, input: str | None = None):
        self.run_calls.append(cmd)
        if cmd.startswith("echo "):
            root = cmd[len("echo ") :].split(";")[0].replace("~", self.home)
            return FakeResult(stdout=f"{root.replace('$HOME', self.home)}\n{self.home}\n")
        return FakeResult(returncode=0)


@pytest.fixture
def fake_ssh(monkeypatch):
    """Swap create_ssh_connection for a fake-conn factory recording the host it would dial."""
    state = {"last_host": None, "last_key": None, "last_port": None, "conn": None}
    state["home"] = "/home/u"  # what the remote shell expands ~ / $HOME to

    async def _fake_connect(host, ssh_key=None, ssh_port=None):
        state["last_host"] = host
        state["last_key"] = ssh_key
        state["last_port"] = ssh_port
        conn = FakeConn(home=state["home"])
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


def _rm_calls(fake_ssh) -> list[str]:
    conn = fake_ssh["conn"]
    return [c for c in conn.run_calls if c.startswith("rm ")] if conn else []


def _clean(*args):
    from click.testing import CliRunner

    return CliRunner().invoke(remote_cli.remote, ["clean", *args])


class TestRemoteClean:
    def test_default_removes_current_project_dir(self, tmp_path, monkeypatch, fake_ssh):
        monkeypatch.chdir(tmp_path)
        # No hsm_config — bare alias path.
        from click.testing import CliRunner

        runner = CliRunner()
        result = runner.invoke(remote_cli.remote, ["clean", "anahita", "-y"])
        assert result.exit_code == 0, result.output
        assert fake_ssh["last_host"] == "anahita"
        # rm -rf <remote_root, expanded on the remote>/<project root dir name = cwd here>
        assert _rm_calls(fake_ssh) == [f"rm -rf /home/u/.hsm/runs/{tmp_path.name}"]

    def test_all_projects_removes_remote_root(self, tmp_path, monkeypatch, fake_ssh):
        monkeypatch.chdir(tmp_path)
        from click.testing import CliRunner

        runner = CliRunner()
        result = runner.invoke(remote_cli.remote, ["clean", "anahita", "-y", "--all-projects"])
        assert result.exit_code == 0
        assert _rm_calls(fake_ssh) == ["rm -rf /home/u/.hsm/runs"]

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
        (cmd,) = _rm_calls(fake_ssh)
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

    def test_project_name_from_project_root_not_cwd(self, tmp_path, monkeypatch, fake_ssh):
        # C8: the sources push to <root>/<project-root dir name>; clean must match.
        monkeypatch.chdir(tmp_path)
        _write_hsm_config(tmp_path, {"project": {"root": "/code/my proj $x"}})
        result = _clean("anahita", "-y")
        assert result.exit_code == 0, result.output
        assert _rm_calls(fake_ssh) == ["rm -rf '/home/u/.hsm/runs/my proj $x'"]  # quoted

    def test_slurm_workdir_wins_over_remote_root(self, tmp_path, monkeypatch, fake_ssh):
        monkeypatch.chdir(tmp_path)
        uzh = {"backend": "slurm", "workdir": "/scratch/u/hsm-runs", "remote_root": "/other/x"}
        _write_hsm_config(tmp_path, {"distributed": {"remotes": {"uzh": uzh}}})
        result = _clean("uzh", "-y")
        assert result.exit_code == 0, result.output
        assert _rm_calls(fake_ssh) == [f"rm -rf /scratch/u/hsm-runs/{tmp_path.name}"]

    @pytest.mark.parametrize("all_projects", [True, False])
    @pytest.mark.parametrize(
        "root,home",
        [
            ("~", "/home/u"),  # $HOME itself
            ("$HOME/", "/home/u"),
            ("/", "/home/u"),
            ("/scratch", "/home/u"),  # one-level
            ("~/..", "/home/u"),  # resolves to /home
            ("/home/users", "/home/users/u"),  # an ancestor of $HOME
            ("hsm-runs", "/home/u"),  # relative
        ],
    )
    def test_unsafe_root_refused(self, tmp_path, monkeypatch, fake_ssh, root, home, all_projects):
        monkeypatch.chdir(tmp_path)
        fake_ssh["home"] = home
        _write_hsm_config(tmp_path, {"distributed": {"remote_root": root}})
        result = _clean("anahita", "-y", *(["--all-projects"] if all_projects else []))
        assert result.exit_code != 0
        assert "Refusing to clean" in result.output
        assert _rm_calls(fake_ssh) == []

    @pytest.mark.parametrize("project_root", ["/", "/code/.."])
    def test_unsafe_project_name_refused(self, tmp_path, monkeypatch, fake_ssh, project_root):
        monkeypatch.chdir(tmp_path)
        _write_hsm_config(tmp_path, {"project": {"root": project_root}})
        result = _clean("anahita", "-y")
        assert result.exit_code != 0
        assert "unsafe project name" in result.output
        assert fake_ssh["last_host"] is None  # refused before connecting
