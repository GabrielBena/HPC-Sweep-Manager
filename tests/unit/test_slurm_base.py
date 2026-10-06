"""SlurmBase: one squeue + one sacct per poll, and a failed call is never a verdict.

Tracker S1 (an outage read as COMPLETED cleaned a live sweep dir) and S2 (per-job polling).
"""

from __future__ import annotations

from datetime import datetime

import pytest

from hpc_sweep_manager.core.common.compute_source import JobInfo
from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
from hpc_sweep_manager.core.hpc.scheduler_queue import Reservation
from hpc_sweep_manager.core.hpc.slurm_base import (
    SACCT_GRACE,
    SlurmBase,
    blocking_reservations,
    queued_states,
    sacct_verdicts,
)
from hpc_sweep_manager.core.hpc.slurm_protocol import render_sbatch_directives


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

    async def test_a_failing_sacct_changes_nothing(self):
        # slurmdbd down while slurmctld is up: a failed sacct is not "no record".
        src = ScriptedSlurm([GONE, (1, "", "Problem talking to the database")] * 5)
        for _ in range(5):
            await src.update_all_job_statuses()
        assert src.active_jobs["1"].status == "PENDING"

    @pytest.mark.parametrize(
        "sacct_reply",
        [
            (0, "", ""),
            (127, "", "sacct: not found"),
            (1, "", "Slurm accounting storage is disabled"),
        ],
    )
    async def test_no_accounting_record_waits_out_the_grace(self, sacct_reply):
        src = ScriptedSlurm([GONE, sacct_reply] * SACCT_GRACE)
        for _ in range(SACCT_GRACE - 1):
            await src.update_all_job_statuses()
            assert "1" in src.active_jobs
        await src.update_all_job_statuses()
        assert src.completed_jobs["1"].status == "COMPLETED"

    async def test_the_grace_counts_polls_in_a_row(self):
        # Two misses, seen queued again, then two misses: never three in a row.
        replies = [GONE, (0, "", "")] * 2 + [(0, "1 RUNNING", "")] + [GONE, (0, "", "")] * 2
        src = ScriptedSlurm(replies)
        for _ in range(5):
            await src.update_all_job_statuses()
        assert "1" in src.active_jobs

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

    async def test_adopt_during_an_outage_says_unknown(self):
        src = ScriptedSlurm([OUTAGE], jobs=())
        assert await src.adopt(["7"], pause=0) == {"7": "UNKNOWN"}

    async def test_get_job_status_never_polls(self):
        # A per-job loop must not burn through the grace (one refresh per call used to).
        src = ScriptedSlurm([])
        assert await src.get_job_status("1") == "PENDING"
        assert await src.get_job_status("2") == "UNKNOWN"
        assert src.calls == []


@pytest.mark.asyncio
class TestCpuOnlyJobsKeepOffGpuNodes:
    """Tracker S6 (Gabriel, 2026-10-06): 24 of ~300 CPU tasks once sat on GPU nodes."""

    SINFO = (
        0,
        "cpu-1 (null)\ncpu-2 tmpdisk:1000\ngpu-1 gpu:A100:4\ngpu-1 gpu:A100:4\n"
        "gpu-2 gpu:H100:8(S:0-1)\n",
        "",
    )

    async def test_gpu_nodes_join_every_exclude_given(self):
        # Only a GRES naming a GPU marks a GPU node (cpu-2's tmpdisk doesn't).
        src = ScriptedSlurm([self.SINFO])
        given = (("exclude", "old-[1-2]"), ("--exclude", "old-9"))
        spec = ResourceSpec(partition="standard", extra_directives=given)
        got = await src._off_gpu_nodes(spec)
        assert dict(got.extra_directives) == {"--exclude": "old-[1-2],old-9,gpu-1,gpu-2"}
        assert await src._off_gpu_nodes(spec) == got and src.calls == ["sinfo"]  # once

    @pytest.mark.parametrize(
        "spec",
        [
            ResourceSpec(partition="standard", gpus=1),
            ResourceSpec(partition="standard", extra_directives=(("gres", "gpu:1"),)),
            ResourceSpec(partition="standard", extra_directives=(("--gpus-per-task", "1"),)),
            ResourceSpec(partition="standard", extra_directives=(("tres-per-task", "gres/gpu:1"),)),
            ResourceSpec(partition="standard", extra_directives=(("nodelist", "gpu-1"),)),
            ResourceSpec(partition="standard", extra_directives=(("-w", "gpu-1"),)),
            ResourceSpec(partition="standard", extra_directives=(("constraint", "GPUMEM80GB"),)),
            ResourceSpec(partition="standard", cpu_only_nodes=False),
            ResourceSpec(),  # the default partition isn't known here
        ],
        ids=[
            "gpus",
            "gres-directive",
            "gpus-directive",
            "tres-directive",
            "nodelist",
            "short-nodelist",
            "constraint",
            "opt-out",
            "no-partition",
        ],
    )
    async def test_gpu_jobs_opt_outs_and_unknown_partitions_are_left_alone(self, spec):
        src = ScriptedSlurm([])
        assert await src._off_gpu_nodes(spec) == spec and src.calls == []

    @pytest.mark.parametrize(
        "sinfo", [(0, "gpu-1 gpu:A100:4\ngpu-2 gpu:H100:8\n", ""), (1, "", "sinfo: error")]
    )
    async def test_never_excludes_every_node(self, sinfo):
        # An all-GPU partition (or a failed sinfo): excluding would leave the job nowhere to run.
        spec = ResourceSpec(partition="gpu")
        assert await ScriptedSlurm([sinfo])._off_gpu_nodes(spec) == spec

    async def test_a_failed_sinfo_is_asked_again(self):
        # Cached, it sent every later CPU-only job out without the --exclude (rc 255: a blip).
        src, spec = ScriptedSlurm([(255, "", "link down"), self.SINFO]), ResourceSpec(partition="p")
        assert await src._off_gpu_nodes(spec) == spec
        assert dict((await src._off_gpu_nodes(spec)).extra_directives)["--exclude"] == "gpu-1,gpu-2"


def test_a_directive_key_without_dashes_still_renders():
    spec = ResourceSpec(extra_directives=(("exclude", "n1"), ("--nice", "100")))
    assert render_sbatch_directives(spec).splitlines() == [
        "#SBATCH --exclude=n1",
        "#SBATCH --nice=100",
    ]


@pytest.mark.parametrize(
    ("start", "end", "flags", "nodes", "walltime_h", "blocks"),
    [
        ("2026-10-07T06:00:00", "2026-10-07T18:00:00", "MAINT", "n[1-9]", 48, True),
        ("2026-10-07T06:00:00", "2026-10-07T18:00:00", "", "ALL", 48, True),
        ("2026-10-07T06:00:00", "2026-10-07T18:00:00", "", "n[1-2]", 48, False),  # not maint
        ("2026-10-07T06:00:00", "2026-10-07T18:00:00", "MAINT", "ALL", 4, False),  # ends first
        ("2026-10-05T06:00:00", "2026-10-05T18:00:00", "MAINT", "ALL", 48, False),  # over
        ("2026-10-06T06:00:00", "2026-10-06T18:00:00", "MAINT", "ALL", 1, True),  # running now
        ("2026-10-07T06:00:00", "2026-10-07T18:00:00", "ALL_NODES", "n[1-900]", 48, True),
    ],
)
def test_blocking_reservations(start, end, flags, nodes, walltime_h, blocks):
    res = Reservation("r", start, end, "12:00:00", nodes, 1, flags)
    now = datetime(2026, 10, 6, 10, 0, 0)
    assert bool(blocking_reservations([res], now, walltime_h * 3600)) is blocks


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reply",
    [
        (0, "2026-10-06T10:00:00\nNo reservations in the system\n", ""),
        (0, "not a clock\n", ""),
        (255, "", "ssh: connection reset"),
        OSError("transport gone"),
    ],
)
@pytest.mark.parametrize("walltime", ["2-00:00:00", "90", "UNLIMITED", None])
async def test_the_reservation_check_never_breaks_a_submission(reply, walltime):
    class Raising(ScriptedSlurm):
        async def _sh(self, argv):
            if isinstance(reply, Exception):
                raise reply
            return reply

    await Raising([])._warn_reservations(walltime)  # no exception, whatever happens
