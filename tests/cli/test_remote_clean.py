"""CLI tests for `hsm remote clean`.

The fake SSH connection runs every command through a REAL local bash, with a
temp dir as the "remote" ``$HOME`` and cwd, so the probe's ``realpath``/``find``
and the final ``rm`` act on real files under ``tmp_path``. As a backstop, the
fake refuses to run any ``rm`` outside ``tmp_path``.
"""

from __future__ import annotations

import os
import shlex
import subprocess
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from hpc_sweep_manager.cli import remote as remote_cli
from hpc_sweep_manager.cli.remote import _clean_verdict


class FakeResult:
    def __init__(self, returncode: int | None = 0, stdout: str = "", stderr: str = ""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class BashConn:
    """Async context manager whose run() executes in local bash as the 'remote'."""

    def __init__(self, home: Path, sandbox: Path):
        self.home, self.sandbox = home, sandbox
        self.run_calls: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def run(self, cmd: str, *, check: bool = False, input: str | None = None):
        self.run_calls.append(cmd)
        if cmd.startswith("rm "):
            target = shlex.split(cmd)[-1]
            assert target.startswith(str(self.sandbox.resolve()) + "/"), f"unsafe rm: {cmd}"
        env = {"PATH": os.environ["PATH"], "HOME": str(self.home)}
        proc = subprocess.run(
            ["bash", "-c", cmd], env=env, cwd=self.home, capture_output=True, text=True
        )
        return FakeResult(proc.returncode, proc.stdout, proc.stderr)


@pytest.fixture
def remote(tmp_path, monkeypatch):
    """The 'remote' filesystem (under tmp_path) + a recording SSH-connect fake."""
    monkeypatch.setattr(
        "hpc_sweep_manager.core.common.config.MACHINE_CONFIG_PATH", tmp_path / "no-machine.yaml"
    )
    home = tmp_path / "remote" / "home" / "u"
    home.mkdir(parents=True)
    (home / "precious.txt").write_text("keep me")
    state = {"home": home, "last_host": None, "last_key": None, "last_port": None, "conn": None}

    async def _fake_connect(host, ssh_key=None, ssh_port=None):
        state.update(last_host=host, last_key=ssh_key, last_port=ssh_port)
        state["conn"] = BashConn(state["home"], tmp_path)
        return state["conn"]

    monkeypatch.setattr(
        "hpc_sweep_manager.core.remote.discovery.create_ssh_connection", _fake_connect
    )
    return state


def _project(tmp_path: Path, monkeypatch, payload: dict, name: str = "proj") -> Path:
    """A local project dir (cwd) with a .hsm/config.yaml."""
    proj = tmp_path / "local" / name
    (proj / ".hsm").mkdir(parents=True)
    (proj / ".hsm" / "config.yaml").write_text(yaml.safe_dump(payload))
    monkeypatch.chdir(proj)
    return proj


def _hsm_tree(root: Path, *projects: str) -> None:
    """What HSM leaves on a remote: <root>/<project>/{code,sweeps,snapshots}/..."""
    for p in projects:
        for sub in ("code", "sweeps", "snapshots"):
            (root / p / sub).mkdir(parents=True)
            (root / p / sub / "f").write_text("x")


def _rm_calls(remote) -> list[str]:
    conn = remote["conn"]
    return [c for c in conn.run_calls if c.startswith("rm ")] if conn else []


def _clean(*args, input: str | None = None):
    return CliRunner().invoke(remote_cli.remote, ["clean", *args], input=input)


def _root_cfg(root) -> dict:
    return {"distributed": {"remote_root": str(root)}}


class TestCleanTargets:
    def test_project_dir_cleaned(self, tmp_path, monkeypatch, remote):
        runs = tmp_path / "remote" / "scratch" / "u" / "hsm-runs"
        _hsm_tree(runs, "proj", "other")
        _project(tmp_path, monkeypatch, _root_cfg(runs))
        result = _clean("box", "-y")
        assert result.exit_code == 0, result.output
        assert not (runs / "proj").exists() and (runs / "other").exists()
        assert _rm_calls(remote) == [f"rm -rf -- {runs / 'proj'}"]
        assert f"Cleaned {runs / 'proj'} on box" in result.output.replace("\n", "")  # rich wraps

    def test_all_projects_cleans_root_of_hsm_projects(self, tmp_path, monkeypatch, remote):
        runs = tmp_path / "remote" / "scratch" / "u" / "hsm-runs"
        _hsm_tree(runs, "p1", "p2")
        _project(tmp_path, monkeypatch, _root_cfg(runs))
        result = _clean("box", "-y", "--all-projects")
        assert result.exit_code == 0, result.output
        assert not runs.exists() and runs.parent.exists()

    def test_project_name_from_project_root_not_cwd(self, tmp_path, monkeypatch, remote):
        runs = tmp_path / "remote" / "runs"
        _hsm_tree(runs, "my proj $x")
        _project(tmp_path, monkeypatch, {**_root_cfg(runs), "project": {"root": "/x/my proj $x"}})
        result = _clean("box", "-y")
        assert result.exit_code == 0, result.output
        assert _rm_calls(remote) == [f"rm -rf -- {shlex.quote(str(runs / 'my proj $x'))}"]
        assert not (runs / "my proj $x").exists()

    def test_slurm_workdir_wins_over_remote_root(self, tmp_path, monkeypatch, remote):
        workdir = tmp_path / "remote" / "scratch" / "hsm-runs"
        _hsm_tree(workdir, "proj")
        uzh = {"backend": "SLURM", "workdir": str(workdir), "remote_root": "/elsewhere/x"}
        _project(tmp_path, monkeypatch, {"distributed": {"remotes": {"uzh": uzh}}})
        result = _clean("uzh", "-y")
        assert result.exit_code == 0, result.output
        assert not (workdir / "proj").exists()

    def test_registered_remote_uses_overrides(self, tmp_path, monkeypatch, remote):
        box = {"host": "box.lab", "ssh_key": "~/.ssh/k", "ssh_port": 2222}
        box["remote_root"] = str(tmp_path / "remote" / "private")
        _project(tmp_path, monkeypatch, {"distributed": {"remotes": {"box": box}}})
        result = _clean("box", "-y")
        assert result.exit_code == 0, result.output
        assert (remote["last_host"], remote["last_key"], remote["last_port"]) == (
            "box.lab",
            "~/.ssh/k",
            2222,
        )
        assert "Nothing to clean" in result.output and _rm_calls(remote) == []

    def test_prompt_shows_canonical_target_and_decline_keeps_it(
        self, tmp_path, monkeypatch, remote
    ):
        real = tmp_path / "remote" / "real-runs"
        _hsm_tree(real, "proj")
        link = tmp_path / "remote" / "runs-link"
        link.symlink_to(real)
        _project(tmp_path, monkeypatch, _root_cfg(link))
        result = _clean("box", input="n\n")
        assert f"Remove {real / 'proj'} on box?" in result.output  # canonical, not the link
        assert (real / "proj" / "code").exists() and _rm_calls(remote) == []


class TestCleanRefusals:
    @staticmethod
    def _assert_refused(result, remote):
        assert result.exit_code != 0
        assert "Refusing to clean" in result.output
        assert _rm_calls(remote) == []
        assert (remote["home"] / "precious.txt").exists()

    def test_tilde_root_is_home(self, tmp_path, monkeypatch, remote):
        _project(tmp_path, monkeypatch, _root_cfg("~"))
        self._assert_refused(_clean("box", "-y", "--all-projects"), remote)

    def test_symlinked_home_refused(self, tmp_path, monkeypatch, remote):
        # $HOME is a symlink (/home/u → /nfs/home/u); the root spells the physical path.
        physical = remote["home"]
        remote["home"] = tmp_path / "remote" / "home-link"
        remote["home"].symlink_to(physical)
        _project(tmp_path, monkeypatch, _root_cfg(physical))
        self._assert_refused(_clean("box", "-y", "--all-projects"), remote)

    def test_root_symlinked_to_home_refused(self, tmp_path, monkeypatch, remote):
        link = tmp_path / "remote" / "runs"
        link.symlink_to(remote["home"])
        _project(tmp_path, monkeypatch, _root_cfg(link))
        self._assert_refused(_clean("box", "-y", "--all-projects"), remote)

    def test_ancestor_of_home_refused(self, tmp_path, monkeypatch, remote):
        _project(tmp_path, monkeypatch, _root_cfg(remote["home"].parent))
        self._assert_refused(_clean("box", "-y", "--all-projects"), remote)

    def test_project_target_resolving_to_home_refused(self, tmp_path, monkeypatch, remote):
        # <root>/<project> with root = home's parent and project named like the home dir.
        _project(tmp_path, monkeypatch, _root_cfg(remote["home"].parent), name="u")
        self._assert_refused(_clean("box", "-y"), remote)

    def test_scratch_like_root_with_foreign_children_refused(self, tmp_path, monkeypatch, remote):
        scratch = tmp_path / "remote" / "scratch" / "u"
        _hsm_tree(scratch / "hsm-runs", "proj")
        (scratch / "data").mkdir()
        (scratch / "data" / "x.npy").write_text("results")
        _project(tmp_path, monkeypatch, _root_cfg(scratch))
        self._assert_refused(_clean("box", "-y", "--all-projects"), remote)
        assert (scratch / "data" / "x.npy").exists()

    def test_loose_file_under_all_projects_root_refused(self, tmp_path, monkeypatch, remote):
        runs = tmp_path / "remote" / "runs"
        _hsm_tree(runs, "proj")
        (runs / "notes.txt").write_text("mine")
        _project(tmp_path, monkeypatch, _root_cfg(runs))
        self._assert_refused(_clean("box", "-y", "--all-projects"), remote)

    def test_project_dir_with_foreign_child_refused(self, tmp_path, monkeypatch, remote):
        runs = tmp_path / "remote" / "runs"
        _hsm_tree(runs, "proj")
        (runs / "proj" / "results").mkdir()
        _project(tmp_path, monkeypatch, _root_cfg(runs))
        self._assert_refused(_clean("box", "-y"), remote)
        assert (runs / "proj" / "code").exists()

    @pytest.mark.parametrize("root", ["/tmp/x; rm -rf ~", "$(id)/x", "/tmp/`id`", "/a b"])
    def test_unsafe_root_characters_refused_before_connecting(
        self, tmp_path, monkeypatch, remote, root
    ):
        _project(tmp_path, monkeypatch, _root_cfg(root))
        result = _clean("box", "-y")
        assert result.exit_code != 0 and "unexpected characters" in result.output
        assert remote["last_host"] is None

    def test_default_mode_without_project_config_refused(self, tmp_path, monkeypatch, remote):
        sub = tmp_path / "local" / "proj" / "subdir"
        sub.mkdir(parents=True)
        monkeypatch.chdir(sub)
        result = _clean("box", "-y")
        assert result.exit_code != 0 and "No project config" in result.output
        assert remote["last_host"] is None


class TestCleanVerdict:
    """The pure guard on probe output (cases a local bash can't stage safely)."""

    def test_rc_file_noise_is_ignored(self):
        out = "Welcome!\n@@target /s/u/runs/p\n@@home /home/u\n@@entry d \n@@entry d code\n"
        assert _clean_verdict(out, all_projects=False) == ("/s/u/runs/p", True, None)

    @pytest.mark.parametrize("target", ["/", "/home", "/home/u"])
    def test_root_home_and_ancestors_refused(self, target):
        out = f"@@target {target}\n@@home /home/u\n@@entry d \n"
        assert _clean_verdict(out, all_projects=True)[2] is not None

    @pytest.mark.parametrize("home", ["", "relative"])
    def test_unresolved_home_is_unsafe(self, home):
        out = f"@@target /s/u/runs\n@@home {home}\n"
        assert _clean_verdict(out, all_projects=True)[2] is not None

    def test_missing_target_is_a_noop(self):
        assert _clean_verdict("@@target /s/u/runs/p\n@@home /home/u\n", False) == (
            "/s/u/runs/p",
            False,
            None,
        )
