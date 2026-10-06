"""SlurmBase: one squeue + one sacct per poll, and a failed call is never a verdict.

Tracker S1 (an outage read as COMPLETED cleaned a live sweep dir) and S2 (per-job polling).
"""

from __future__ import annotations

import pytest

from hpc_sweep_manager.core.common.compute_source import JobInfo
from hpc_sweep_manager.core.hpc.slurm_base import (
    SACCT_GRACE,
    SlurmBase,
    queued_states,
    sacct_verdicts,
)


class TestQueuedStates:
    def test_array_rows_count_for_their_parent(self):
        out = "100_[5-9%2] PENDING\n100_3 RUNNING\n200 PENDING\n"
        assert queued_states(["100", "200", "300"], out) == {"100": "RUNNING", "200": "PENDING"}

    def test_a_terminal_row_does_not_keep_a_job_queued(self):
        assert queued_states(["100"], "100 CANCELLED\n") == {}

    def test_other_users_jobs_are_ignored(self):
        assert queued_states(["1"], "2 RUNNING\n") == {}


class TestSacctVerdicts:
    @pytest.mark.parametrize(
        ("rows", "verdict"),
        [
            ("1|COMPLETED", "COMPLETED"),
            ("1|FAILED", "FAILED"),
            ("1|TIMEOUT", "FAILED"),
            ("1|OUT_OF_MEMORY", "FAILED"),
            ("1|NODE_FAIL", "FAILED"),
            ("1|CANCELLED by 12345", "CANCELLED"),
            ("1|CANCELLED+", "CANCELLED"),
            # sacct settles after squeue forgets: keep waiting, never assume.
            ("1|RUNNING", "RUNNING"),
            ("1|PENDING", "RUNNING"),
            ("1|WEIRD_NEW_STATE", "RUNNING"),
            # An array is done only when every task is; FAILED beats CANCELLED beats COMPLETED.
            ("1_1|COMPLETED\n1_2|FAILED\n1_3|COMPLETED", "FAILED"),
            ("1_1|COMPLETED\n1_2|CANCELLED", "CANCELLED"),
            ("1_1|FAILED\n1_2|RUNNING", "RUNNING"),
            ("1_[4-9]|PENDING\n1_1|COMPLETED", "RUNNING"),
        ],
    )
    def test_verdict(self, rows, verdict):
        assert sacct_verdicts(["1"], rows) == {"1": verdict}

    def test_no_rows_is_no_verdict(self):
        assert sacct_verdicts(["1"], "") == {}


class ScriptedSlurm(SlurmBase):
    """A SlurmBase whose ``_sh`` replays scripted ``(rc, stdout, stderr)`` replies."""

    def __init__(self, replies, jobs=("1",)):
        super().__init__("fake", "slurm", 10_000)
        self.replies, self.calls = list(replies), []
        for job in jobs:
            self.active_jobs[job] = JobInfo(job, job, {}, self.name)

    async def _sh(self, argv):
        self.calls.append(argv[0])
        return self.replies.pop(0)

    async def setup(self, sweep_dir, sweep_id):
        return True

    async def submit_job(self, params, job_name, sweep_id, wandb_group=None, spec=None):
        raise NotImplementedError

    async def cancel_job(self, job_id):
        return False

    async def collect_results(self, job_ids=None, *, defer_cleanup=False):
        return True

    async def health_check(self):
        return {}

    async def cleanup(self):
        pass


OUTAGE = (1, "", "slurm_load_jobs error: Unable to contact slurm controller")
GONE = (0, "", "")


@pytest.mark.asyncio
class TestRefresh:
    async def test_squeue_outage_changes_nothing(self):
        src = ScriptedSlurm([OUTAGE] * 5)
        for _ in range(5):
            await src.update_all_job_statuses()
        assert src.calls == ["squeue"] * 5  # sacct is never asked to guess
        assert src.active_jobs["1"].status == "PENDING"

    @pytest.mark.parametrize("sacct_reply", [(1, "", "DB connection refused"), (0, "", "")])
    async def test_no_sacct_verdict_waits_out_the_grace(self, sacct_reply):
        src = ScriptedSlurm([GONE, sacct_reply] * SACCT_GRACE)
        for _ in range(SACCT_GRACE - 1):
            await src.update_all_job_statuses()
            assert "1" in src.active_jobs
        await src.update_all_job_statuses()
        assert src.completed_jobs["1"].status == "COMPLETED"

    async def test_a_verdict_resets_the_grace(self):
        src = ScriptedSlurm([GONE, GONE, GONE, (0, "1|RUNNING", ""), GONE, GONE, GONE, GONE])
        for _ in range(4):
            await src.update_all_job_statuses()
        assert "1" in src.active_jobs  # 1 miss, a RUNNING verdict, then 1 miss again

    async def test_two_calls_per_poll_whatever_the_job_count(self):
        jobs = [str(i) for i in range(600)]
        queued = "".join(f"{j} RUNNING\n" for j in jobs[:300])
        done = "".join(f"{j}|COMPLETED\n" for j in jobs[300:599]) + "599|TIMEOUT\n"
        src = ScriptedSlurm([(0, queued, ""), (0, done, "")], jobs=jobs)
        await src.update_all_job_statuses()
        assert src.calls == ["squeue", "sacct"]
        assert len(src.active_jobs) == 300
        assert src.completed_jobs["599"].status == "FAILED"

    async def test_adopt_settles_reattached_jobs(self):
        src = ScriptedSlurm([GONE, GONE, GONE, (0, "7|FAILED", "")], jobs=())
        assert await src.adopt(["7"], pause=0) == {"7": "FAILED"}
        assert src.calls == ["squeue", "sacct", "squeue", "sacct"]

    async def test_adopt_keeps_a_queued_job_running(self):
        src = ScriptedSlurm([(0, "7_[1-9] PENDING", "")], jobs=())
        assert await src.adopt(["7"], pause=0) == {"7": "PENDING"}
