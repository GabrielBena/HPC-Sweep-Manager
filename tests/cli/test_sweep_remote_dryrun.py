"""Dry-run tests for remote sweeps.

Dry-run returns before any SSH connection or sweep-dir resolution, so these
run fully offline. Covers:
  #2 — dry-run displays the MERGED per-remote ResourceSpec (gres/qos/...).
  #3 — `--remote <alias> --mode array` reconciliation + submission=array.

We drive ``run_sweep`` / ``build_compute_source`` directly rather than through
``CliRunner``: HSM's ``setup_logging`` installs a ``StreamHandler(sys.stdout)``
that goes stale under CliRunner+pytest's stdout capture.
"""

from __future__ import annotations

import io
import logging
from pathlib import Path

import pytest
import yaml
from rich.console import Console

from hpc_sweep_manager.cli import sweep as sweep_cli
from hpc_sweep_manager.cli.sweep import run_sweep
from hpc_sweep_manager.core.common.config import HSMConfig


def _make_project(tmp_path, *, backend="slurm", spec=None):
    """Minimal project: train script, sweep config, one registered remote."""
    (tmp_path / "train.py").write_text("print('hi')\n")
    sweeps = tmp_path / "sweeps"
    sweeps.mkdir()
    (sweeps / "sweep.yaml").write_text(
        yaml.safe_dump({"sweep": {"grid": {"lr": [0.1, 0.2, 0.3]}}})
    )
    remote = {"host": "uzh", "backend": backend}
    if spec is not None:
        remote["spec"] = spec
    (tmp_path / ".hsm").mkdir()
    (tmp_path / ".hsm" / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "paths": {"train_script": str(tmp_path / "train.py")},
                "distributed": {"remotes": {"uzh": remote}},
            }
        )
    )


def _dry_run(*, remote_alias="uzh", mode="remote", remote_submission=None):
    """Run a dry-run and return the rendered console text."""
    buf = io.StringIO()
    run_sweep(
        config_path=Path("sweeps/sweep.yaml"),
        mode=mode,
        dry_run=True,
        count_only=False,
        max_runs=None,
        walltime=None,
        resources=None,
        group=None,
        parallel_jobs=None,
        no_progress=True,
        console=Console(file=buf, width=200),
        logger=logging.getLogger("test"),
        hsm_config=HSMConfig.load(),
        remote_alias=remote_alias,
        remote_submission=remote_submission,
    )
    return buf.getvalue()


@pytest.fixture(autouse=True)
def _isolate_home(tmp_path, monkeypatch):
    # Don't read the real machine ~/.hsm/config.yaml during these tests.
    home = tmp_path / "_home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))


class TestRemoteDryRun:
    def test_dry_run_shows_merged_per_remote_spec(self, tmp_path, monkeypatch):
        _make_project(
            tmp_path,
            spec={
                "walltime": "04:00:00",
                "gpus": 1,
                "gpu_type": "A100",
                "qos": "normal",
            },
        )
        monkeypatch.chdir(tmp_path)
        out = _dry_run()
        # The per-remote spec is merged + displayed (pre-fix this showed empty
        # / "GPUs: none requested (spec.gpus=0)").
        assert "A100" in out
        assert "normal" in out
        assert "04:00:00" in out
        assert "gpu:A100:1" in out  # placement shows the resulting gres

    def test_dry_run_array_submission_reported(self, tmp_path, monkeypatch):
        _make_project(tmp_path, spec={"walltime": "01:00:00"})
        monkeypatch.chdir(tmp_path)
        out = _dry_run(remote_submission="array")
        assert "submission=array" in out

    def test_dry_run_default_submission_individual(self, tmp_path, monkeypatch):
        _make_project(tmp_path, spec={"walltime": "01:00:00"})
        monkeypatch.chdir(tmp_path)
        out = _dry_run()
        assert "submission=individual" in out


class TestRemoteModeReconciliation:
    """run_cmd reconciles `--remote <alias> --mode array|individual` into
    remote execution + a submission style (and still rejects local/distributed)."""

    @staticmethod
    def _run_cmd(monkeypatch, args):
        """Invoke run_cmd via CliRunner with run_sweep mocked to a recorder."""
        from click.testing import CliRunner

        calls = {}

        def fake_run_sweep(**kwargs):
            calls.update(kwargs)

        monkeypatch.setattr(sweep_cli, "run_sweep", fake_run_sweep)
        monkeypatch.setattr(sweep_cli.HSMConfig, "load", staticmethod(lambda: None))
        buf = io.StringIO()
        obj = {"console": Console(file=buf, width=200), "logger": logging.getLogger("t")}
        result = CliRunner().invoke(
            sweep_cli.sweep_cmd, ["run", *args], obj=obj, catch_exceptions=False
        )
        return result, buf.getvalue(), calls

    def test_remote_mode_array_accepted(self, tmp_path, monkeypatch):
        _make_project(tmp_path, spec={"walltime": "01:00:00"})
        monkeypatch.chdir(tmp_path)
        result, out, calls = self._run_cmd(
            monkeypatch,
            ["-c", "sweeps/sweep.yaml", "--remote", "uzh", "--mode", "array"],
        )
        assert result.exit_code == 0, out
        assert "only valid" not in out and "can't be combined" not in out
        assert calls.get("mode") == "remote"
        assert calls.get("remote_submission") == "array"

    def test_remote_mode_individual_accepted(self, tmp_path, monkeypatch):
        _make_project(tmp_path, spec={"walltime": "01:00:00"})
        monkeypatch.chdir(tmp_path)
        _result, _out, calls = self._run_cmd(
            monkeypatch,
            ["-c", "sweeps/sweep.yaml", "--remote", "uzh", "--mode", "individual"],
        )
        assert calls.get("mode") == "remote"
        assert calls.get("remote_submission") == "individual"

    def test_remote_mode_local_rejected(self, tmp_path, monkeypatch):
        _make_project(tmp_path, spec={"walltime": "01:00:00"})
        monkeypatch.chdir(tmp_path)
        _result, out, calls = self._run_cmd(
            monkeypatch,
            ["-c", "sweeps/sweep.yaml", "--remote", "uzh", "--mode", "local"],
        )
        # Rejected before reaching run_sweep.
        assert calls == {}
        assert "can't be combined" in out
