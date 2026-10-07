"""`hsm sweep run` pre-flight: unknown keys warn (R4), stale paths stop the run first (R6).

Drives ``run_sweep`` directly, as test_sweep_remote_dryrun.py does (CliRunner and HSM's stdout
log handler don't mix under pytest's capture).
"""

from __future__ import annotations

import io
import logging
from pathlib import Path

import pytest
import yaml
from rich.console import Console

from hpc_sweep_manager.cli.sweep import run_sweep
from hpc_sweep_manager.core.common.config import HSMConfig


def _project(tmp_path, monkeypatch, *, train_script, sweep=None):
    """A project whose sweeps land under its own `_sweeps_root` (never the machine's)."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "train.py").write_text("print('hi')\n")
    (tmp_path / "_sweeps_root").mkdir()
    (tmp_path / "sweep.yaml").write_text(yaml.safe_dump(sweep or {"sweep": {"grid": {"lr": [1]}}}))
    cfg = {
        "project": {"name": "p", "root": str(tmp_path)},
        "paths": {"train_script": train_script},
        "local": {"sweeps_root": str(tmp_path / "_sweeps_root")},
        "distributed": {"remotes": {"uzh": {"backend": "slurm", "pre_script": ["x"]}}},
    }
    (tmp_path / "config.yaml").write_text(yaml.safe_dump(cfg))
    return HSMConfig.load(tmp_path / "config.yaml", machine_config_path=tmp_path / "no-machine")


def _run(hsm_config, *, dry_run, out=None):
    """Run the sweep into ``out`` (a StringIO the caller keeps when the run raises)."""
    out = out or io.StringIO()
    run_sweep(
        config_path=Path("sweep.yaml"),
        mode="local",
        dry_run=dry_run,
        count_only=False,
        max_runs=None,
        walltime=None,
        resources=None,
        group=None,
        parallel_jobs=None,
        no_progress=True,
        console=Console(file=out, width=300),
        logger=logging.getLogger("test"),
        hsm_config=hsm_config,
    )
    return out.getvalue()


class TestPreflight:
    def test_a_stale_script_stops_a_real_run_before_any_sweep_dir(self, tmp_path, monkeypatch):
        cfg = _project(tmp_path, monkeypatch, train_script=str(tmp_path / "moved" / "train.py"))
        out = io.StringIO()
        with pytest.raises(SystemExit) as exc:
            _run(cfg, dry_run=False, out=out)
        assert exc.value.code == 2
        assert "`paths.train_script`" in out.getvalue() and "moved/train.py" in out.getvalue()
        assert not (tmp_path / "sweeps").exists()  # no discovery symlink dir
        assert list((tmp_path / "_sweeps_root").iterdir()) == []  # no ghost sweep dir

    def test_a_stale_root_stops_the_dry_run_before_its_output(self, tmp_path, monkeypatch):
        cfg = _project(tmp_path, monkeypatch, train_script="train.py")
        cfg.config_data["project"]["root"] = str(tmp_path / "moved")
        out = io.StringIO()
        with pytest.raises(SystemExit):
            _run(cfg, dry_run=True, out=out)
        assert "`project.root`" in out.getvalue() and "DRY RUN" not in out.getvalue()

    def test_unknown_keys_warn_and_the_dry_run_goes_on(self, tmp_path, monkeypatch):
        sweep = {"sweep": {"grid": {"lr": [1]}, "gird": {"wd": [0]}}}
        cfg = _project(tmp_path, monkeypatch, train_script="train.py", sweep=sweep)
        out = _run(cfg, dry_run=True)
        assert "did you mean `sweep.grid`?" in out
        assert "did you mean `distributed.remotes.uzh.spec.pre_script`?" in out
        assert "DRY RUN" in out
