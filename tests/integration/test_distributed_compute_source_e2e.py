"""End-to-end runs of ``--mode distributed`` through ``run_sweep_async``, with fake children.

The fakes behave like the real children: a job runs for ``secs`` seconds and then ends
``outcome``; a blocking child (local, ssh) waits for a free slot inside ``submit_job``; a Slurm
child takes the job at once and a poll reveals when it is done. Every run finishes in well
under a second.
"""

from __future__ import annotations

import asyncio
import logging
import time

import pytest
import yaml

from hpc_sweep_manager.core.common.compute_source import ComputeSource, JobInfo
from hpc_sweep_manager.core.common.sweep_orchestrator import run_sweep_async
from hpc_sweep_manager.core.distributed.distributed_compute_source import (
    DistributedComputeSource,
)
from hpc_sweep_manager.core.hpc.slurm_base import SlurmBase

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class FakeChild(ComputeSource):
    def __init__(
        self, name, slots=2, secs=0.01, *, blocking=True, fail_submit=False, outcome="COMPLETED"
    ):
        super().__init__(name, "fake", slots)
        self.secs, self.blocking, self.fail_submit = secs, blocking, fail_submit
        self.outcome = outcome
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
            self.update_job_status(job_id, self.outcome)
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


class FakeSlurm(FakeChild, SlurmBase):
    """A ``backend: slurm`` child: takes a job at once (queued jobs count as active), names its
    task dir after the job, and pulls finished tasks after each poll."""

    update_all_job_statuses = ComputeSource.update_all_job_statuses  # no scheduler to ask

    def __init__(self, name, slots=2, secs=0.01, **kw):
        super().__init__(name, slots, secs, blocking=False, **kw)
        self.host, self._remote_sweep_dir, self.keep_remote_on_success = name, "/scratch/sw", False
        self.pulls: list[int] = []  # newly_done of each _after_poll

    async def _sh(self, argv):
        raise AssertionError("no scheduler here")

    async def submit_job(self, params, job_name, sweep_id, wandb_group=None, spec=None):
        job_id = await super().submit_job(params, job_name, sweep_id, wandb_group, spec)
        self.active_jobs[job_id].task_dir = f"/hq/sw/tasks/{job_name}"
        return job_id

    async def _after_poll(self, newly_done):
        self.pulls.append(newly_done)


async def _run(tmp_path, children, n, params=None, **kw):
    for child in children:
        child.world = children
    src = DistributedComputeSource(child_sources=children, poll_interval=0.005)
    result = await asyncio.wait_for(
        run_sweep_async(
            source=src,
            sweep_dir=tmp_path / "sw",
            sweep_id="sw",
            params_list=params or [{"seed": i} for i in range(n)],
            poll_interval=0.005,
            **kw,
        ),
        timeout=10,
    )
    return result, src


def _mapping(tmp_path):
    return yaml.safe_load((tmp_path / "sw" / "source_mapping.yaml").read_text())["task_assignments"]


async def test_all_tasks_complete_and_the_faster_child_takes_more(tmp_path):
    fast, slow = FakeChild("fast", slots=2, secs=0.002), FakeChild("slow", slots=1, secs=0.1)
    result, _ = await _run(tmp_path, [fast, slow], 12)

    assert len(result.job_ids) == 12
    assert list(result.final_statuses.values()) == ["COMPLETED"] * 12
    assert len(fast.ends) > len(slow.ends) >= 1
    assert fast.cleaned and slow.cleaned
    tasks = _mapping(tmp_path)
    assert list(tasks) == [f"task_{i:03d}" for i in range(1, 13)]
    assert {t["compute_source"] for t in tasks.values()} == {"fast", "slow"}
    assert {t["status"] for t in tasks.values()} == {"COMPLETED"}
    assert {t["host"] for t in tasks.values()} == {"localhost"}
    ids = {f"{t['compute_source']}:{t['job_id']}" for t in tasks.values()}
    assert ids == set(result.job_ids)


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


async def test_a_failed_submit_hands_the_task_to_another_child(tmp_path):
    broken, ok = FakeChild("broken", fail_submit=True), FakeChild("ok", secs=0.002)
    result, _ = await _run(tmp_path, [broken, ok], 6)

    assert list(result.final_statuses.values()) == ["COMPLETED"] * 6
    assert broken.ends == {} and len(ok.ends) == 6  # broken retired after its one try


async def test_a_task_whose_submit_keeps_failing_ends_failed_after_three_tries(tmp_path):
    class Poisoned(FakeChild):
        async def submit_job(self, params, job_name, sweep_id, wandb_group=None, spec=None):
            if params.get("poison"):
                raise ValueError("bad params")
            return await super().submit_job(params, job_name, sweep_id, wandb_group, spec)

    children = [Poisoned(n, secs=0.002) for n in ("a", "b", "c", "d")]
    params = [{"poison": True}] + [{"seed": i} for i in range(5)]
    result, _ = await _run(tmp_path, children, 6, params=params)

    assert sorted(result.final_statuses.values()) == ["COMPLETED"] * 5 + ["FAILED"]
    assert [len(c.ends) for c in children] == [0, 0, 0, 5]  # three tries retired three children
    assert _mapping(tmp_path)["task_001"] == {
        "compute_source": "c",
        "host": "localhost",
        "job_id": None,
        "status": "FAILED",
        "complete_time": _mapping(tmp_path)["task_001"]["complete_time"],
    }


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

    by_child = {
        c.name: {result.final_statuses[f"{c.name}:{j}"] for j in c.ends} for c in (lost, ok)
    }
    assert by_child == {"lost": {"FAILED"}, "ok": {"COMPLETED"}}
    assert len(ok.collects) == 1 and len(lost.collects) == 1


async def test_a_full_slurm_child_whose_connection_dies_retires_instead_of_hanging(tmp_path):
    class Dead(FakeSlurm):
        async def update_all_job_statuses(self):  # SlurmBase._sh on a dead connection
            raise ConnectionError("Connection lost")

    uzh = Dead("uzh", slots=2, secs=60)
    result, _ = await _run(tmp_path, [uzh], 5)  # used to poll the dead child forever

    assert len(uzh.ends) == 2  # its jobs may still run: FAILED, and the dir stays
    assert {result.final_statuses[f"uzh:{j}"] for j in uzh.ends} == {"FAILED"}
    assert uzh.keep_remote_on_success
    assert sorted(result.final_statuses.values()) == ["FAILED"] * 5  # 3 never submitted


async def test_a_child_whose_jobs_keep_failing_retires(tmp_path):
    bad = FakeChild("bad", slots=2, secs=0.001, outcome="FAILED")  # e.g. a wrong python_path
    good = FakeChild("good", slots=4, secs=0.02)
    result, _ = await _run(tmp_path, [bad, good], 30)

    assert 5 <= len(bad.ends) <= 7  # 5 finished FAILED retire it; 2 more may be in flight
    assert sorted(result.final_statuses.values()).count("FAILED") == len(bad.ends)
    assert len(result.final_statuses) == 30


@pytest.mark.parametrize(
    ("hosts", "kept"), [(("athena", "athena"), True), (("athena", "box"), False)]
)
async def test_remote_dirs_are_kept_when_two_remotes_share_one(tmp_path, hosts, kept):
    children = [FakeChild(f"r{i}", secs=0.002) for i in range(2)]
    for child, host in zip(children, hosts, strict=True):
        child.host, child._remote_sweep_dir, child.keep_remote_on_success = host, "/r/sw", False
    result, _ = await _run(tmp_path, children, 4)

    assert set(result.final_statuses.values()) == {"COMPLETED"}
    assert [c.keep_remote_on_success for c in children] == [kept, kept]


async def test_one_failed_task_keeps_every_remote_dir(tmp_path):
    ok, bad = FakeChild("ok", secs=0.002), FakeChild("bad", slots=1, secs=0.002, outcome="FAILED")
    for child in (ok, bad):
        child.host, child._remote_sweep_dir, child.keep_remote_on_success = child.name, "/r", False
    result, _ = await _run(tmp_path, [ok, bad], 4)

    assert "FAILED" in result.final_statuses.values()
    assert ok.keep_remote_on_success and bad.keep_remote_on_success


async def test_job_ids_from_two_clusters_never_collide(tmp_path):
    class Numbered(FakeSlurm):  # two clusters number their jobs independently
        async def submit_job(self, params, job_name, sweep_id, wandb_group=None, spec=None):
            job_id = await super().submit_job(params, job_name, sweep_id, wandb_group, spec)
            self.active_jobs["1000"] = self.active_jobs.pop(job_id)
            self.ends["1000"] = self.ends.pop(job_id)
            return "1000"

    a, b = Numbered("a", slots=1, secs=0.002), Numbered("b", slots=1, outcome="FAILED")
    result, _ = await _run(tmp_path, [a, b], 2)

    assert result.final_statuses == {"a:1000": "COMPLETED", "b:1000": "FAILED"}


async def test_progress_counts_the_sweep_s_tasks_only(tmp_path):
    seen = []
    busy, idle = FakeChild("busy", slots=4, secs=0.002), FakeChild("idle", fail_submit=True)
    await _run(tmp_path, [busy, idle], 4, on_progress=lambda d, t: seen.append((d, t)))

    assert {t for _, t in seen} == {4} and seen[-1] == (4, 4)  # the idle child adds nothing


async def test_a_slurm_child_pulls_while_tasks_are_handed_out(tmp_path):
    uzh = FakeSlurm("uzh", slots=1, secs=0.002)
    src = DistributedComputeSource(child_sources=[uzh], poll_interval=0.005)
    assert await src.setup(tmp_path / "sw", "sw")
    await src.submit_batch([{"seed": i} for i in range(3)], "sw")

    assert sum(uzh.pulls) == 2  # each finished job was pulled before the last was submitted
    tasks = _mapping(tmp_path)  # keyed by the dirs the Slurm child writes
    assert list(tasks) == ["sw_task_001", "sw_task_002", "sw_task_003"]
    assert {t["host"] for t in tasks.values()} == {"uzh"}


async def test_an_interrupted_dispatch_records_its_jobs_and_how_to_cancel_them(tmp_path, caplog):
    uzh = FakeSlurm("uzh", slots=2, secs=60)
    src = DistributedComputeSource(child_sources=[uzh], poll_interval=0.005)
    assert await src.setup(tmp_path / "sw", "sw")
    dispatch = asyncio.create_task(src.submit_batch([{"seed": i} for i in range(5)], "sw"))
    while len(uzh.ends) < 2:
        await asyncio.sleep(0.001)
    with caplog.at_level(logging.ERROR):
        dispatch.cancel()
        with pytest.raises(asyncio.CancelledError):
            await dispatch

    assert [t["job_id"] for t in _mapping(tmp_path).values()] == ["uzh-1", "uzh-2"]
    assert any("ssh uzh scancel uzh-1 uzh-2" in r.message for r in caplog.records)


async def test_cancellation_is_not_swallowed(tmp_path):
    src = DistributedComputeSource(child_sources=[FakeChild("a", secs=60)], poll_interval=0.005)
    run = run_sweep_async(
        source=src, sweep_dir=tmp_path / "sw", sweep_id="sw", params_list=[{}], poll_interval=0.005
    )
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(run, timeout=0.2)
