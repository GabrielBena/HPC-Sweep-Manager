"""Tests for `hsm sweep advance` (resumable chains, issue #12).

Command-level manifest guards via CliRunner, plus the pre-reattach branches of
the async helper (which don't need a live SSH connection).
"""

from __future__ import annotations

import json
import logging

import pytest
from rich.console import Console

from hpc_sweep_manager.cli.sweep import _advance_via_manifest, sweep_cmd


def _obj():
    return {"console": Console(), "logger": logging.getLogger("test")}


def _chain_manifest(**chain_over):
    chain = {
        "state": {"chunk_index": 1, "consecutive_no_progress": 0, "done": False, "failed": False},
        "chunks": [
            {"index": 0, "job_ids": ["100"], "terminal_states": ["COMPLETED"]},
            {"index": 1, "job_ids": ["101"], "terminal_states": []},
        ],
        "num_tasks": 2,
    }
    chain.update(chain_over)
    return {
        "sweep_id": "sw1",
        "backend": "slurm",
        "host": "uzh",
        "resumable": {"enabled": True, "chunk_walltime": "23:00:00"},
        "chain": chain,
    }


class TestAdvanceCommandGuards:
    def test_no_manifest(self, tmp_path, monkeypatch):
        from click.testing import CliRunner

        monkeypatch.chdir(tmp_path)
        res = CliRunner().invoke(sweep_cmd, ["advance", "nope"], obj=_obj())
        assert "No manifest" in res.output

    def test_not_a_resumable_chain(self, tmp_path, monkeypatch):
        from click.testing import CliRunner

        monkeypatch.chdir(tmp_path)
        d = tmp_path / "sweeps/outputs/sw1"
        d.mkdir(parents=True)
        (d / ".hsm_manifest.json").write_text(json.dumps({"sweep_id": "sw1", "backend": "slurm"}))
        res = CliRunner().invoke(sweep_cmd, ["advance", "sw1"], obj=_obj())
        assert "not a resumable chain" in res.output

    def test_collect_refuses_resumable_chain(self, tmp_path, monkeypatch):
        from click.testing import CliRunner

        monkeypatch.chdir(tmp_path)
        d = tmp_path / "sweeps/outputs/sw1"
        d.mkdir(parents=True)
        (d / ".hsm_manifest.json").write_text(json.dumps(_chain_manifest()))
        res = CliRunner().invoke(sweep_cmd, ["collect", "sw1"], obj=_obj())
        # collect must redirect to advance, never run the destructive pull/clean.
        assert "resumable chain" in res.output
        assert "advance" in res.output

    def test_collect_takes_a_done_chain(self, tmp_path, monkeypatch):
        from click.testing import CliRunner

        manifest = _chain_manifest()
        manifest["chain"]["state"]["done"] = True  # e.g. its final archive was cut short
        monkeypatch.chdir(tmp_path)
        (tmp_path / "sweeps/outputs/sw1").mkdir(parents=True)
        (tmp_path / "sweeps/outputs/sw1/.hsm_manifest.json").write_text(json.dumps(manifest))
        res = CliRunner().invoke(sweep_cmd, ["collect", "sw1"], obj=_obj())
        assert "resumable chain" not in res.output
        assert "missing required field" in res.output  # past the guard (a bare test manifest)


class TestAdvanceHelperGuards:
    @pytest.mark.asyncio
    async def test_no_chunks(self, tmp_path):
        out = Console(file=__import__("io").StringIO(), force_terminal=False, width=200)
        m = _chain_manifest(chunks=[])
        await _advance_via_manifest(tmp_path, m, out, block=False)
        assert "no chunks" in out.file.getvalue().lower()

    @pytest.mark.asyncio
    async def test_already_done(self, tmp_path):
        import io

        out = Console(file=io.StringIO(), force_terminal=False, width=200)
        m = _chain_manifest()
        m["chain"]["state"]["done"] = True
        await _advance_via_manifest(tmp_path, m, out, block=False)
        assert "already completed" in out.file.getvalue().lower()

    @pytest.mark.asyncio
    async def test_missing_sweep_config(self, tmp_path):
        import io

        out = Console(file=io.StringIO(), force_terminal=False, width=200)
        # tmp_path has no sweep_config.yaml -> can't re-derive the param set.
        await _advance_via_manifest(tmp_path, _chain_manifest(), out, block=False)
        assert "sweep_config.yaml" in out.file.getvalue()


class TestOneDriverAtATime:
    """One process drives a chain at a time; a cron `advance` steps aside (tracker R10)."""

    def test_a_second_driver_steps_aside(self, tmp_path, monkeypatch):
        from click.testing import CliRunner

        from hpc_sweep_manager.cli import sweep as sweep_mod
        from hpc_sweep_manager.core.remote.ssh_compute_source import launcher_lock

        monkeypatch.chdir(tmp_path)
        d = tmp_path / "sweeps/outputs/sw1"
        d.mkdir(parents=True)
        (d / ".hsm_manifest.json").write_text(json.dumps(_chain_manifest()))
        driven = []

        async def drive(sweep_dir, manifest, console, *, block):
            driven.append(sweep_dir)

        monkeypatch.setattr(sweep_mod, "_advance_via_manifest", drive)
        live = launcher_lock(d)  # a live launcher drives the chain
        res = CliRunner().invoke(sweep_cmd, ["advance", "sw1"], obj=_obj())
        assert res.exit_code == 0 and "Another process drives chain sw1" in res.output
        assert driven == []
        live.close()
        assert CliRunner().invoke(sweep_cmd, ["advance", "sw1"], obj=_obj()).exit_code == 0
        assert len(driven) == 1

    @pytest.mark.asyncio
    async def test_advance_resubmits_with_the_saved_costs(self, tmp_path, monkeypatch):
        import io

        from hpc_sweep_manager.core.common import sweep_orchestrator
        from hpc_sweep_manager.core.common.sweep_orchestrator import SweepResult
        from hpc_sweep_manager.core.remote.ssh_slurm_compute_source import SSHSlurmComputeSource

        class Reattached:
            _remote_sweep_dir, conda_env, python_path, poll_interval = "/r/sw1", None, "python", 0

            async def reattach(self, *args):
                return True

            async def adopt(self, job_ids, jobs=()):
                return dict.fromkeys(job_ids, "COMPLETED")

            async def cleanup(self):
                pass

        calls = []

        async def run(**kwargs):
            calls.append(kwargs)
            return SweepResult(sweep_id="sw1", sweep_dir=tmp_path, chain_decision="advance")

        src = Reattached()
        monkeypatch.setattr(SSHSlurmComputeSource, "from_manifest", lambda m: src)
        monkeypatch.setattr(sweep_orchestrator, "run_resumable_sweep_async", run)
        (tmp_path / "sweep_config.yaml").write_text("sweep:\n  grid:\n    lr: [1, 2]\n")
        out = Console(file=io.StringIO(), force_terminal=False, width=200)
        await _advance_via_manifest(tmp_path, _chain_manifest(costs=[1.0, 3.0]), out, block=False)
        assert calls[0]["costs"] == [1.0, 3.0]
        assert src.hydra_overrides == ("wandb.group", "output.dir")  # launched before R3
        saved = _chain_manifest(hydra_overrides=["output.dir"])
        await _advance_via_manifest(tmp_path, saved, out, block=False)
        assert src.hydra_overrides == ("output.dir",)  # the chain keeps the project's pick
        odd = _chain_manifest(hydra_overrides=["output.dir", "from.a.newer.hsm"])
        await _advance_via_manifest(tmp_path, odd, out, block=False)
        assert src.hydra_overrides == ("output.dir",)  # an unknown name can't break rendering
