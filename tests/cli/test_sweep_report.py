"""`hsm sweep report` on a plain local sweep dir, and the inspect commands' exit codes (R8)."""

from __future__ import annotations

import io
import logging
from pathlib import Path

import pytest
from click.testing import CliRunner
from rich.console import Console

from hpc_sweep_manager.cli.sweep import sweep_cmd


def _invoke(*args):
    buf = io.StringIO()
    obj = {"console": Console(file=buf, width=200), "logger": logging.getLogger("t")}
    res = CliRunner().invoke(sweep_cmd, list(args), obj=obj, catch_exceptions=False)
    return res, buf.getvalue()


def _local_sweep(*statuses):
    """A sweep dir as a local run leaves it: no source_mapping.yaml, a task_info.txt per task."""
    d = Path("sweeps/outputs/sw")
    (d / "tasks").mkdir(parents=True)
    (d / "sweep_config.yaml").write_text(f"sweep:\n  grid:\n    lr: {list(range(len(statuses)))}\n")
    for i, status in enumerate(statuses, 1):
        (d / "tasks" / f"task_{i:03d}").mkdir()
        (d / "tasks" / f"task_{i:03d}" / "task_info.txt").write_text(f"Status: {status}\n")
    return d


def test_report_reads_a_plain_local_sweep(tmp_path, monkeypatch):
    # It crashed on the per-task dicts of the task-directory scan (every non-distributed sweep).
    monkeypatch.chdir(tmp_path)
    _local_sweep("COMPLETED", "FAILED")
    res, out = _invoke("report", "sw")
    assert res.exit_code == 0, out
    assert "COMPLETED: 1 tasks" in out and "FAILED: 1 tasks" in out


@pytest.mark.parametrize(
    "args, message",
    [
        (["status", "nope"], "Sweep directory not found"),
        (["status"], "Please specify a sweep ID"),
        (["report", "nope"], "Sweep directory not found"),
        (["watch", "nope"], "Sweep directory not found"),
        (["cancel", "nope"], "Sweep directory not found"),
    ],
)
def test_a_missing_sweep_exits_1(tmp_path, monkeypatch, args, message):
    monkeypatch.chdir(tmp_path)
    res, _ = _invoke(*args)
    assert res.exit_code == 1
    assert message in res.output
