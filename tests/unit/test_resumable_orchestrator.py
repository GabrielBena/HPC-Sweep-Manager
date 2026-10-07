"""Unit tests for the resumable chain DRIVER (issue #12).

Drives ``run_resumable_sweep_async`` with a scripted fake ComputeSource so the
chain state machine + advance/stop/fail + deferred cleanup + dependency wiring
are tested without any cluster.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from hpc_sweep_manager.core.common.resumable import ChunkProgress, ResumableConfig
from hpc_sweep_manager.core.common.sweep_orchestrator import (
    PROBE_TRIES,
    run_resumable_sweep_async,
)


class FakeSource:
    """Minimal ComputeSource stand-in scripted by a list of ChunkProgress."""

    name = "fake"
    source_type = "fake_slurm"
    hydra_overrides = ("wandb.group", "output.dir", "hydra.run.dir")

    def __init__(self, progress_script: list[ChunkProgress], *, archives: bool | None = None):
        self._script = list(progress_script)
        self.active_jobs: dict[str, Any] = {}
        self.completed_jobs: dict[str, Any] = {}
        self._pull_excludes: tuple = ()
        self._next = 0
        self.submit_calls: list[dict[str, Any]] = []
        self.collect_calls: list[bool] = []
        self.persist_calls: list[dict[str, Any]] = []
        self.cleaned = False
        # archives None → no _should_archive method (like the native source);
        # True/False → emulate an SSH-Slurm source whose archive will/won't run.
        if archives is not None:
            self._archives = archives
            self._should_archive = lambda any_failed: self._archives
            self.archive_calls: list[tuple[bool, int]] = []  # (any_failed, collects before it)

    async def _archive_remote(self, any_failed: bool) -> bool:
        self.archive_calls.append((any_failed, len(self.collect_calls)))
        return True

    async def setup(self, sweep_dir: Path, sweep_id: str) -> bool:
        return True

    async def submit_batch(
        self,
        *,
        params_list,
        sweep_id,
        mode,
        spec,
        wandb_group,
        job_name_prefix,
        costs,
        dependency=None,
        resumable=None,
    ):
        self._next += 1
        jid = f"job{self._next}"
        self.submit_calls.append({"dependency": dependency, "chunk_index": resumable.chunk_index})
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
        self.persist_calls.append({"chain": dict(chain), "job_ids": list(job_ids)})

    async def cleanup(self) -> None:
        self.cleaned = True


def _cfg(**over):
    base = dict(enabled=True, chunk_walltime="23:00:00", max_chunks=10, max_consecutive_failures=2)
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
        src = FakeSource(
            [
                ChunkProgress(frozenset(), 100.0),
                ChunkProgress(frozenset({1, 2, 3}), 200.0),
            ]
        )
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
    async def test_a_failed_probe_is_asked_again_not_a_strike(self):
        # Tracker S11 review: an empty probe after a blip counted as a chunk without progress.
        src = FakeSource([None, None, ChunkProgress(frozenset({1, 2}), 100.0)])
        res = await _run(src, _cfg(max_consecutive_failures=1), params=2, poll_interval=0)
        assert res.chain_decision == "done" and res.chunks_run == 1 and not src._script

    @pytest.mark.asyncio
    async def test_a_probe_that_keeps_failing_ends_a_live_launcher(self):
        src = FakeSource([None] * PROBE_TRIES + [ChunkProgress(frozenset({1}), 1.0)])
        with pytest.raises(RuntimeError, match="hsm sweep advance sw"):
            await _run(src, _cfg(), params=1, poll_interval=0)
        assert len(src._script) == 1 and src.collect_calls == []

    @pytest.mark.asyncio
    async def test_a_failed_probe_leaves_a_detached_advance_undecided(self):
        # Two overlapping cron runs could both ADVANCE once the link is back: this one stops.
        src = FakeSource([None, ChunkProgress(frozenset(), 100.0)])
        res = await _run(src, _cfg(), params=2, do_setup=False, initial_job_ids=["j0"], block=False)
        assert res.chain_decision == "" and src.submit_calls == [] and len(src._script) == 1

    @pytest.mark.asyncio
    async def test_a_chain_marked_cancelled_submits_no_more(self, tmp_path):
        # `hsm sweep cancel` marks the manifest; the live driver must not write over it and go on.
        src = FakeSource([ChunkProgress(frozenset(), 100.0)] * 2)
        src.sweep_dir = tmp_path
        mark = {"chain": {"state": {"failed": True}}}
        (tmp_path / ".hsm_manifest.json").write_text(json.dumps(mark))
        with pytest.raises(RuntimeError, match="stopped by `hsm sweep cancel`"):
            await _run(src, _cfg(), params=3)
        assert len(src.submit_calls) == 1 and len(src.persist_calls) == 1  # none after the wait

    @pytest.mark.asyncio
    async def test_an_advance_after_an_unsubmitted_chunk_judges_nothing_twice(self):
        # Review of #59: ADVANCE was saved for chunk 0 (one strike), then the submit of chunk 1
        # stopped. The advance must submit chunk 1 after chunk 0, not judge chunk 0 again.
        from hpc_sweep_manager.core.common.chain import ChainState

        src = FakeSource([ChunkProgress(frozenset({1, 2}), 300.0)])
        state = ChainState(chunk_index=1, consecutive_no_progress=1)
        res = await _run(
            src, _cfg(), params=2, chain_state=state, initial_job_ids=["job0"], initial_decided=True
        )
        assert res.chain_decision == "done"
        first = src.submit_calls[0]
        assert (first["chunk_index"], first["dependency"]) == (1, "afterany:job0")

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
        src = FakeSource(
            [
                ChunkProgress(frozenset(), None),
                ChunkProgress(frozenset(), None),
            ]
        )
        res = await _run(src, _cfg(max_consecutive_failures=2), params=3)
        assert res.chain_decision == "failed"
        # FAILED keeps the remote: collect with defer_cleanup=True (no rm -rf).
        assert src.collect_calls == [True]

    @pytest.mark.asyncio
    async def test_fail_out_of_budget(self):
        # Always progressing, never done, max_chunks=2 -> out-of-budget FAILED.
        src = FakeSource(
            [
                ChunkProgress(frozenset(), 100.0),
                ChunkProgress(frozenset({1}), 200.0),
            ]
        )
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
        src = FakeSource(
            [
                ChunkProgress(frozenset(), 100.0),
                ChunkProgress(frozenset({1}), 200.0),
                ChunkProgress(frozenset({1, 2}), 300.0),
            ]
        )
        res = await _run(src, _cfg(), params=2)
        assert res.chain_decision == "done"
        assert src.collect_calls == [False]  # exactly one, terminal
        assert res.chunks_run == 3

    @pytest.mark.asyncio
    async def test_a_task_out_of_retries_fails_the_chain_once_the_rest_is_done(self):
        # Issue #15: task 2 crashes (one line in .hsm_failed), then again -> out of retries.
        # Task 1 progressing keeps the chain alive in between; a crash below the cap is retried.
        src = FakeSource(
            [
                ChunkProgress(frozenset(), 100.0, {2: 1}),
                ChunkProgress(frozenset({1}), 200.0, {2: 2}),
            ]
        )
        res = await _run(src, _cfg(max_task_crashes=2), params=2)
        assert res.chain_decision == "failed" and res.chunks_run == 2
        assert src.collect_calls == [True]  # the remote is kept

    @pytest.mark.asyncio
    @pytest.mark.parametrize("archives", [True, False])
    async def test_a_failed_chain_archives_unless_opted_out(self, archives):
        # Issue #15: a FAILED chain left finished checkpoints on purgeable /scratch.
        src = FakeSource([ChunkProgress(frozenset(), None)] * 2, archives=archives)
        res = await _run(src, _cfg(max_consecutive_failures=2), params=2)
        assert res.chain_decision == "failed"
        assert src.archive_calls == ([(True, 0)] if archives else [])  # before the pull (4b)
        assert src.collect_calls == [True]  # the pull; never an rm -rf


class TestReviewFixes:
    """Cold-review hardenings: advance re-attach, baseline restore/persist,
    data-loss guard, zero-task guard (issue #12)."""

    @pytest.mark.asyncio
    async def test_advance_reattach_submits_next_with_dependency(self):
        from hpc_sweep_manager.core.common.chain import ChainState

        # Seeded terminal chunk j0 that progressed but isn't done -> ADVANCE ->
        # submit the next chunk depending on j0, then return (block=False).
        src = FakeSource([ChunkProgress(frozenset(), 100.0)])
        res = await run_resumable_sweep_async(
            source=src,
            sweep_dir=Path("/tmp/sw"),
            sweep_id="sw",
            params_list=[{"seed": 0}, {"seed": 1}],
            spec=None,
            resumable=_cfg(),
            chain_state=ChainState(chunk_index=0),
            do_setup=False,
            initial_job_ids=["j0"],
            block=False,
        )
        assert res.chain_decision == "advance"
        assert res.chunks_run == 1
        assert len(src.submit_calls) == 1
        assert src.submit_calls[0]["dependency"] == "afterany:j0"
        assert src.submit_calls[0]["chunk_index"] == 1
        assert src.collect_calls == []  # left running, no terminal collect

    @pytest.mark.asyncio
    async def test_restored_baseline_lets_no_progress_strike_count(self):
        from hpc_sweep_manager.core.common.chain import ChainState

        # Re-attach with baseline done=2/mtime=100 already seen, AND a prior
        # no-progress strike. A chunk that shows the SAME done/mtime made no new
        # progress -> with cap=2 the chain FAILS (instead of resetting).
        src = FakeSource([ChunkProgress(frozenset({1, 2}), 100.0)])
        res = await run_resumable_sweep_async(
            source=src,
            sweep_dir=Path("/tmp/sw"),
            sweep_id="sw",
            params_list=[{"seed": i} for i in range(3)],
            spec=None,
            resumable=_cfg(max_consecutive_failures=2),
            chain_state=ChainState(chunk_index=1, consecutive_no_progress=1),
            do_setup=False,
            initial_job_ids=["j1"],
            initial_prev_done=2,
            initial_prev_mtime=100.0,
            block=True,
        )
        assert res.chain_decision == "failed"

    @pytest.mark.asyncio
    async def test_baseline_and_group_persisted_in_chain(self):
        src = FakeSource([ChunkProgress(frozenset({1, 2}), 250.0)])
        await run_resumable_sweep_async(
            source=src,
            sweep_dir=Path("/tmp/sw"),
            sweep_id="sw",
            params_list=[{"seed": 0}, {"seed": 1}],
            spec=None,
            resumable=_cfg(),
            wandb_group="grp",
        )
        last = src.persist_calls[-1]["chain"]
        assert last["wandb_group"] == "grp"
        assert last["hydra_overrides"] == ["wandb.group", "output.dir", "hydra.run.dir"]
        assert last["last_done_count"] == 2
        assert last["last_checkpoint_mtime"] == 250.0

    @pytest.mark.asyncio
    async def test_done_without_archive_pulls_checkpoint(self):
        # No server-side archive -> the final pull must include the checkpoint
        # (clear _pull_excludes) so the trained model isn't rm -rf'd.
        src = FakeSource([ChunkProgress(frozenset({1, 2}), 100.0)], archives=False)
        await _run(src, _cfg(), params=2)
        assert src._pull_excludes == ()  # cleared for the final pull

    @pytest.mark.asyncio
    async def test_done_with_archive_keeps_excludes(self):
        src = FakeSource([ChunkProgress(frozenset({1, 2}), 100.0)], archives=True)
        await _run(src, _cfg(), params=2)
        # Archive captures the checkpoint server-side; the WAN pull stays light.
        assert src._pull_excludes == ("*/resume/",)

    @pytest.mark.asyncio
    async def test_zero_task_chain_rejected(self):
        src = FakeSource([])
        with pytest.raises(ValueError, match="0 tasks"):
            await _run(src, _cfg(), params=0)


def test_task_crashes_have_their_own_cap():
    # Review of #51: two transient crashes must not end a multi-day task by default.
    cfg = ResumableConfig.from_dict({"enabled": True, "chunk_walltime": "23:00:00"})
    assert (cfg.max_consecutive_failures, cfg.max_task_crashes) == (2, 3)
    assert ResumableConfig.from_manifest(cfg.to_manifest()).max_task_crashes == 3
    assert "max_task_crashes" in " ".join(ResumableConfig(max_task_crashes=0).validate())


@pytest.mark.asyncio
async def test_costs_persisted_in_chain():
    # `hsm sweep advance` re-submits with them: the same GPU-type split every chunk (R10).
    src = FakeSource([ChunkProgress(frozenset({1, 2}), 250.0)])
    await _run(src, _cfg(), costs=[1.0, 3.0])
    assert src.persist_calls[-1]["chain"]["costs"] == [1.0, 3.0]
