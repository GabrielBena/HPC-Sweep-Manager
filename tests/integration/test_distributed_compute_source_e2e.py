"""End-to-end runs of ``--mode distributed`` through ``run_sweep_async``, with fake children.

The fakes behave like the real children: a job runs for ``secs`` seconds; a blocking child
(local, ssh) waits for a free slot inside ``submit_job``; a non-blocking child (Slurm) takes
the job at once and a poll reveals when it is done. Every run finishes in well under a second.
"""

from __future__ import annotations

import asyncio
import time

import pytest
import yaml

from hpc_sweep_manager.core.common.compute_source import ComputeSource, JobInfo
from hpc_sweep_manager.core.common.sweep_orchestrator import run_sweep_async
from hpc_sweep_manager.core.distributed.distributed_compute_source import (
    DistributedComputeSource,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class FakeChild(ComputeSource):
    def __init__(self, name, slots=2, secs=0.01, *, blocking=True, fail_submit=False):
        super().__init__(name, "fake", slots)
        self.secs, self.blocking, self.fail_submit = secs, blocking, fail_submit
        self.ends: dict[str, float] = {}  # job id -> when it finishes
        self.peak = 0  # most jobs active at once
        self.collects: list[tuple[float, int, set[str]]] = []  # (when, active in sweep, done)
        self.world: list[FakeChild] = [self]  # every child of the sweep
        self.cleaned = False

    async def setup(self, sweep_dir, sweep_id):
        return True

    async def submit_job(self, params, job_name, sweep_id, wandb_group=None, spec=None):
        if self.fail_submit:
            raise ConnectionError("ChannelOpenError: open failed")
        while self.blocking and len(self.active_jobs) >= self.max_parallel_jobs:
            await asyncio.sleep(0.001)
            await self.update_all_job_statuses()
        job_id = f"{self.name}-{len(self.ends) + 1}"
        self.ends[job_id] = time.monotonic() + self.secs
        self.active_jobs[job_id] = JobInfo(job_id, job_name, params, self.name, "RUNNING")
        self.peak = max(self.peak, len(self.active_jobs))
        return job_id

    async def get_job_status(self, job_id):
        if job_id in self.active_jobs and time.monotonic() >= self.ends[job_id]:
            self.update_job_status(job_id, "COMPLETED")
        return (self.active_jobs.get(job_id) or self.completed_jobs[job_id]).status

    async def cancel_job(self, job_id):
        return job_id in self.active_jobs

    async def collect_results(self, job_ids=None, *, defer_cleanup=False):
        active = sum(len(c.active_jobs) for c in self.world)
        self.collects.append((time.monotonic(), active, set(self.completed_jobs)))
        return True

    async def cleanup(self):
        self.cleaned = True

    async def health_check(self):
        return {"status": "healthy"}


async def _run(tmp_path, children, n):
    for child in children:
        child.world = children
    src = DistributedComputeSource(child_sources=children, poll_interval=0.005)
    result = await asyncio.wait_for(
        run_sweep_async(
            source=src,
            sweep_dir=tmp_path / "sw",
            sweep_id="sw",
            params_list=[{"seed": i} for i in range(n)],
            poll_interval=0.005,
        ),
        timeout=10,
    )
    return result, src


async def test_all_tasks_complete_and_the_faster_child_takes_more(tmp_path):
    fast, slow = FakeChild("fast", slots=2, secs=0.002), FakeChild("slow", slots=1, secs=0.1)
    result, _ = await _run(tmp_path, [fast, slow], 12)

    assert len(result.job_ids) == 12
    assert list(result.final_statuses.values()) == ["COMPLETED"] * 12
    assert len(fast.ends) > len(slow.ends) >= 1
    assert fast.cleaned and slow.cleaned
    mapping = yaml.safe_load((tmp_path / "sw" / "source_mapping.yaml").read_text())
    tasks = mapping["task_assignments"]
    assert list(tasks) == [f"task_{i:03d}" for i in range(1, 13)]
    assert {t["compute_source"] for t in tasks.values()} == {"fast", "slow"}
    assert {t["status"] for t in tasks.values()} == {"COMPLETED"}


async def test_the_last_task_to_finish_is_pulled(tmp_path):
    quick, last = FakeChild("quick", secs=0.002), FakeChild("last", slots=1, secs=0.15)
    await _run(tmp_path, [quick, last], 4)

    for child in (quick, last):
        assert len(child.collects) == 1
        when, _, done = child.collects[0]
        assert done == set(child.ends)  # every job of the child, the last one included
        assert when >= max(child.ends.values())


async def test_no_collect_while_any_task_is_active(tmp_path):
    quick, slow = FakeChild("quick", secs=0.002), FakeChild("slow", secs=0.1)
    for child in (quick, slow):
        child.world = [quick, slow]
    src = DistributedComputeSource(child_sources=[quick, slow], poll_interval=0.005)
    assert await src.setup(tmp_path / "sw", "sw")
    await src.submit_batch([{"seed": i} for i in range(4)], "sw")

    assert await src.collect_results() is False  # slow still runs: nobody pulls or cleans
    assert quick.collects == [] and slow.collects == []

    await src.wait_for_all(poll_interval=0.005)
    assert await src.collect_results() is True
    assert [c[1] for c in quick.collects + slow.collects] == [0, 0]


async def test_a_failed_submit_ends_failed_instead_of_hanging(tmp_path):
    broken, ok = FakeChild("broken", fail_submit=True), FakeChild("ok", secs=0.002)
    result, _ = await _run(tmp_path, [broken, ok], 6)

    statuses = sorted(result.final_statuses.values())
    assert statuses == ["COMPLETED"] * 5 + ["FAILED"]  # broken took one task, then no more
    assert broken.ends == {} and len(ok.ends) == 5


async def test_tasks_no_child_can_take_end_failed(tmp_path):
    result, _ = await _run(tmp_path, [FakeChild("a", fail_submit=True)], 3)

    assert result.job_ids == []
    assert sorted(result.final_statuses.values()) == ["FAILED"] * 3
    mapping = yaml.safe_load((tmp_path / "sw" / "source_mapping.yaml").read_text())
    assert sorted(mapping["task_assignments"]) == ["task_001", "task_002", "task_003"]


async def test_a_slurm_child_is_capped_and_collected(tmp_path):
    local = FakeChild("local", slots=1, secs=0.05)
    uzh = FakeChild("uzh", slots=2, secs=0.01, blocking=False)  # queued jobs count as active
    result, _ = await _run(tmp_path, [local, uzh], 8)

    assert set(result.final_statuses.values()) == {"COMPLETED"}
    assert uzh.peak == 2 and len(uzh.ends) >= 2
    assert len(uzh.collects) == 1 and uzh.collects[0][2] == set(uzh.ends)


async def test_a_child_lost_mid_run_fails_its_jobs_and_the_others_are_collected(tmp_path):
    class Lost(FakeChild):
        async def update_all_job_statuses(self):
            raise ConnectionError("connection lost")

    lost, ok = Lost("lost", slots=4, secs=60), FakeChild("ok", slots=4, secs=0.002)
    result, _ = await _run(tmp_path, [lost, ok], 6)

    by_child = {c.name: {result.final_statuses[j] for j in c.ends} for c in (lost, ok)}
    assert by_child == {"lost": {"FAILED"}, "ok": {"COMPLETED"}}
    assert len(ok.collects) == 1 and len(lost.collects) == 1


async def test_cancellation_is_not_swallowed(tmp_path):
    src = DistributedComputeSource(child_sources=[FakeChild("a", secs=60)], poll_interval=0.005)
    run = run_sweep_async(
        source=src, sweep_dir=tmp_path / "sw", sweep_id="sw", params_list=[{}], poll_interval=0.005
    )
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(run, timeout=0.2)
