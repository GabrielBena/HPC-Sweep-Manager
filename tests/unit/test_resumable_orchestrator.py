"""Unit tests for the resumable chain DRIVER (issue #12).

Drives ``run_resumable_sweep_async`` with a scripted fake ComputeSource so the
chain state machine + advance/stop/fail + deferred cleanup + dependency wiring
are tested without any cluster.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from hpc_sweep_manager.core.common.resumable import ChunkProgress, ResumableConfig
from hpc_sweep_manager.core.common.sweep_orchestrator import run_resumable_sweep_async


class FakeSource:
    """Minimal ComputeSource stand-in scripted by a list of ChunkProgress."""

    name = "fake"
    source_type = "fake_slurm"

    def __init__(self, progress_script: List[ChunkProgress]):
        self._script = list(progress_script)
        self.active_jobs: Dict[str, Any] = {}
        self.completed_jobs: Dict[str, Any] = {}
        self._pull_excludes: tuple = ()
        self._next = 0
        self.submit_calls: List[Dict[str, Any]] = []
        self.collect_calls: List[bool] = []
        self.persist_calls: List[Dict[str, Any]] = []
        self.cleaned = False

    async def setup(self, sweep_dir: Path, sweep_id: str) -> bool:
        return True

    async def submit_batch(self, *, params_list, sweep_id, mode, spec, wandb_group,
                           job_name_prefix, costs, dependency=None, resumable=None):
        self._next += 1
        jid = f"job{self._next}"
        self.submit_calls.append(
            {"dependency": dependency, "chunk_index": resumable.chunk_index}
        )
        self.active_jobs[jid] = object()
        return [jid]

    async def wait_for_all(self, poll_interval: float = 10.0, on_progress=None):
        # All submitted jobs reach a terminal state immediately.
        out = {j: "COMPLETED" for j in self.active_jobs}
        for j in list(self.active_jobs):
            self.completed_jobs[j] = object()
        return out

    async def chunk_progress(self, num_tasks, *, done_sentinel, checkpoint_subdir):
        if self._script:
            return self._script.pop(0)
        return ChunkProgress(done_indices=frozenset(), checkpoint_mtime=None)

    async def collect_results(self, job_ids=None, *, defer_cleanup: bool = False) -> bool:
        self.collect_calls.append(defer_cleanup)
        return True

    async def persist_chain_manifest(self, *, resumable, chain, job_ids, num_tasks):
        self.persist_calls.append({"state": dict(chain["state"]), "job_ids": list(job_ids)})

    async def cleanup(self) -> None:
        self.cleaned = True


def _cfg(**over):
    base = dict(enabled=True, chunk_walltime="23:00:00", max_chunks=10,
                max_consecutive_failures=2)
    base.update(over)
    return ResumableConfig(**base)


async def _run(source, cfg, *, params=2, **kw):
    return await run_resumable_sweep_async(
        source=source,
        sweep_dir=Path("/tmp/sw"),
        sweep_id="sw",
        params_list=[{"seed": i} for i in range(params)],
        spec=None,
        resumable=cfg,
        **kw,
    )


class TestDrive:
    @pytest.mark.asyncio
    async def test_advance_then_done(self):
        # chunk0: nothing done but a checkpoint appears -> progressed -> ADVANCE.
        # chunk1: all 3 tasks done -> DONE.
        src = FakeSource([
            ChunkProgress(frozenset(), 100.0),
            ChunkProgress(frozenset({1, 2, 3}), 200.0),
        ])
        res = await _run(src, _cfg(), params=3)
        assert res.chain_decision == "done"
        assert res.chunks_run == 2
        # collect_results called exactly once, at the end, with cleanup ENABLED.
        assert src.collect_calls == [False]
        assert src.cleaned is True
        # Dependency wiring: chunk0 has no dep; chunk1 afterany the chunk0 job.
        assert src.submit_calls[0]["dependency"] is None
        assert src.submit_calls[0]["chunk_index"] == 0
        assert src.submit_calls[1]["dependency"] == "afterany:job1"
        assert src.submit_calls[1]["chunk_index"] == 1
        # Pull-excludes were set so wait_for_all's pulls skip the checkpoint dir.
        assert src._pull_excludes == ("*/resume/",)

    @pytest.mark.asyncio
    async def test_done_in_one_chunk(self):
        src = FakeSource([ChunkProgress(frozenset({1, 2}), 100.0)])
        res = await _run(src, _cfg(), params=2)
        assert res.chain_decision == "done"
        assert res.chunks_run == 1
        assert src.collect_calls == [False]

    @pytest.mark.asyncio
    async def test_fail_on_no_progress(self):
        # Two chunks, neither progresses -> consecutive_no_progress hits cap 2.
        src = FakeSource([
            ChunkProgress(frozenset(), None),
            ChunkProgress(frozenset(), None),
        ])
        res = await _run(src, _cfg(max_consecutive_failures=2), params=3)
        assert res.chain_decision == "failed"
        # FAILED keeps the remote: collect with defer_cleanup=True (no rm -rf).
        assert src.collect_calls == [True]

    @pytest.mark.asyncio
    async def test_fail_out_of_budget(self):
        # Always progressing, never done, max_chunks=2 -> out-of-budget FAILED.
        src = FakeSource([
            ChunkProgress(frozenset(), 100.0),
            ChunkProgress(frozenset({1}), 200.0),
        ])
        res = await _run(src, _cfg(max_chunks=2, max_consecutive_failures=9), params=3)
        assert res.chain_decision == "failed"
        assert src.collect_calls == [True]
        assert res.chunks_run == 2

    @pytest.mark.asyncio
    async def test_non_blocking_advance_submits_one_and_returns(self):
        # block=False: submit chunk 0 and return without waiting (cron launch).
        src = FakeSource([])
        res = await _run(src, _cfg(), params=2, block=False)
        assert res.chain_decision == "advance"
        assert res.chunks_run == 1
        assert src.collect_calls == []  # no terminal collect — chunk left running
        assert len(src.submit_calls) == 1

    @pytest.mark.asyncio
    async def test_deferred_cleanup_only_terminal(self):
        # Three-chunk run: collect must NOT be called between chunks, only once
        # at the terminal DONE.
        src = FakeSource([
            ChunkProgress(frozenset(), 100.0),
            ChunkProgress(frozenset({1}), 200.0),
            ChunkProgress(frozenset({1, 2}), 300.0),
        ])
        res = await _run(src, _cfg(), params=2)
        assert res.chain_decision == "done"
        assert src.collect_calls == [False]  # exactly one, terminal
        assert res.chunks_run == 3
