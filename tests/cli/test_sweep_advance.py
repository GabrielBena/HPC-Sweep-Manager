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

        runner = CliRunner()
        with runner.isolated_filesystem():
            res = runner.invoke(sweep_cmd, ["advance", "nope"], obj=_obj())
        assert "No manifest" in res.output

    def test_not_a_resumable_chain(self, tmp_path):
        from click.testing import CliRunner
        from pathlib import Path

        runner = CliRunner()
        with runner.isolated_filesystem():
            d = Path("sweeps/outputs/sw1")
            d.mkdir(parents=True)
            (d / ".hsm_manifest.json").write_text(
                json.dumps({"sweep_id": "sw1", "backend": "slurm"})
            )
            res = runner.invoke(sweep_cmd, ["advance", "sw1"], obj=_obj())
        assert "not a resumable chain" in res.output

    def test_collect_refuses_resumable_chain(self, tmp_path):
        from click.testing import CliRunner
        from pathlib import Path

        runner = CliRunner()
        with runner.isolated_filesystem():
            d = Path("sweeps/outputs/sw1")
            d.mkdir(parents=True)
            (d / ".hsm_manifest.json").write_text(json.dumps(_chain_manifest()))
            res = runner.invoke(sweep_cmd, ["collect", "sw1"], obj=_obj())
        # collect must redirect to advance, never run the destructive pull/clean.
        assert "resumable chain" in res.output
        assert "advance" in res.output


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
