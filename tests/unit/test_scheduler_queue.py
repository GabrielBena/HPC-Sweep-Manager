"""Unit tests for :mod:`core.hpc.scheduler_queue`.

Strategy: mock ``subprocess.run`` directly (no shell, no fixture binaries).
Each test feeds canned ``squeue`` / ``scontrol`` stdout and asserts the
parsed dataclasses come out right.
"""

from __future__ import annotations

import asyncio
import shlex
import subprocess
from typing import List, Optional
from unittest.mock import MagicMock, patch

import pytest

from hpc_sweep_manager.core.hpc.scheduler_queue import (
    SQUEUE_FORMAT,
    JobGroup,
    QueueCommandError,
    QueueJob,
    Reservation,
    SlurmQueue,
    SSHSlurmQueue,
    _parse_gpu_from_tres,
    _parse_priority,
    _split_gres_entries,
    enrich_groups_with_accounting,
    group_jobs_by_array,
    parse_array_task_count,
    parse_reservations_output,
    parse_sacct_job_states,
    parse_sinfo_gpu_capacity,
    parse_squeue_output,
    positions_by_base,
    strip_array_suffix,
    summarize_gpu_jobs,
)


def _fake_completed(stdout: str = "", stderr: str = "", returncode: int = 0):
    r = MagicMock(spec=subprocess.CompletedProcess)
    r.stdout = stdout
    r.stderr = stderr
    r.returncode = returncode
    return r


# --------------------------------------------------------- _parse_gpu_from_tres


class TestParseGpuFromTres:
    def test_no_gpu(self):
        assert _parse_gpu_from_tres("cpu=1,mem=4G") == (0, None)

    def test_untyped_gpu(self):
        assert _parse_gpu_from_tres("cpu=1,gres/gpu=2") == (2, None)

    def test_typed_gpu_uppercase(self):
        assert _parse_gpu_from_tres("cpu=1,gres/gpu:H100=1") == (1, "H100")

    def test_typed_gpu_lowercase(self):
        assert _parse_gpu_from_tres("cpu=1,gres/gpu:l4=2") == (2, "l4")

    def test_typed_wins_over_untyped(self):
        # If both forms appear, the typed entry is what the job asked for.
        assert _parse_gpu_from_tres("gres/gpu=1,gres/gpu:H100=1") == (1, "H100")

    def test_empty_string(self):
        assert _parse_gpu_from_tres("") == (0, None)


class TestParsePriority:
    def test_integer(self):
        assert _parse_priority("12345") == 12345

    def test_scientific_notation(self):
        # Slurm sometimes emits priorities in this form.
        assert _parse_priority("1.234e5") == 123400

    def test_garbage_returns_zero(self):
        assert _parse_priority("not-a-number") == 0
        assert _parse_priority("") == 0


# ----------------------------------------------------------------- SlurmQueue


# Format mirrors SlurmQueue._SQUEUE_FORMAT exactly.
def _row(*fields: str) -> str:
    return "\t".join(fields)


_SAMPLE_PENDING_H100 = _row(
    "1001", "sweep_a_1", "alice", "PENDING", "(Priority)",
    "standard", "cpu=1,gres/gpu:H100=1", "N/A", "5000",
)
_SAMPLE_PENDING_L4 = _row(
    "1002", "sweep_b_1", "bob", "PENDING", "(Resources)",
    "standard", "cpu=2,gres/gpu:L4=1", "2026-05-28T16:00:00", "6000",
)
_SAMPLE_RUNNING_H100 = _row(
    "1003", "sweep_c_1", "alice", "RUNNING", "u24-chiihm0-621",
    "standard", "cpu=4,gres/gpu:H100=2", "N/A", "0",
)
_SAMPLE_PENDING_CPU = _row(
    "1004", "datacrunch", "carol", "PENDING", "(Priority)",
    "standard", "cpu=8,mem=32G", "N/A", "4000",
)
_SAMPLE_MALFORMED = "1005\tbad\trow"  # too few fields


class TestSlurmQueueParse:
    def test_empty_output_yields_no_jobs(self):
        with patch("subprocess.run", return_value=_fake_completed("")):
            jobs = SlurmQueue()._run_squeue([])
        assert jobs == []

    def test_single_row_parsed(self):
        with patch("subprocess.run", return_value=_fake_completed(_SAMPLE_PENDING_H100 + "\n")):
            jobs = SlurmQueue()._run_squeue([])
        assert len(jobs) == 1
        j = jobs[0]
        assert j.job_id == "1001"
        assert j.name == "sweep_a_1"
        assert j.user == "alice"
        assert j.state == "PENDING"
        assert j.reason == "(Priority)"
        assert j.partition == "standard"
        assert j.gpu_count == 1
        assert j.gpu_type == "H100"
        assert j.priority == 5000

    def test_malformed_rows_skipped(self, caplog):
        stdout = "\n".join([_SAMPLE_PENDING_H100, _SAMPLE_MALFORMED, _SAMPLE_PENDING_L4])
        with patch("subprocess.run", return_value=_fake_completed(stdout)):
            with caplog.at_level("WARNING"):
                jobs = SlurmQueue()._run_squeue([])
        assert len(jobs) == 2
        assert {j.job_id for j in jobs} == {"1001", "1002"}
        assert any("expected 9" in r.message for r in caplog.records)

    def test_nonzero_returncode_yields_empty(self):
        with patch(
            "subprocess.run",
            return_value=_fake_completed("", stderr="boom", returncode=1),
        ):
            assert SlurmQueue()._run_squeue([]) == []

    def test_filenotfound_yields_empty(self):
        # squeue binary missing on PATH — degrade gracefully, don't crash.
        with patch("subprocess.run", side_effect=FileNotFoundError):
            assert SlurmQueue()._run_squeue([]) == []


class TestPendingGpuSorted:
    def _all_rows(self):
        return "\n".join(
            [
                _SAMPLE_PENDING_H100,
                _SAMPLE_PENDING_L4,
                _SAMPLE_RUNNING_H100,
                _SAMPLE_PENDING_CPU,
            ]
        )

    def test_filters_to_pending_gpu_only(self):
        # The CPU-only pending job MUST be filtered out — it's not in the GPU queue.
        with patch("subprocess.run", return_value=_fake_completed(self._all_rows())):
            jobs = SlurmQueue().pending_gpu_jobs_sorted()
        assert {j.job_id for j in jobs} == {"1001", "1002"}
        assert all(j.state == "PENDING" and j.gpu_count > 0 for j in jobs)


class TestGpuSummary:
    def test_aggregates_by_type_and_state(self):
        stdout = "\n".join(
            [
                _SAMPLE_PENDING_H100,    # H100, pending, count 1
                _SAMPLE_PENDING_L4,      # L4, pending, count 1
                _SAMPLE_RUNNING_H100,    # H100, running, count 2
                _SAMPLE_PENDING_CPU,     # ignored (no GPU)
            ]
        )
        with patch("subprocess.run", return_value=_fake_completed(stdout)):
            summary = SlurmQueue().gpu_summary()
        assert summary == {
            "H100": {"PENDING": 1, "RUNNING": 2},
            "L4": {"PENDING": 1},
        }


class TestPositionInQueue:
    def _two_pending(self):
        return _fake_completed("\n".join([_SAMPLE_PENDING_H100, _SAMPLE_PENDING_L4]))

    def test_finds_first(self):
        with patch("subprocess.run", return_value=self._two_pending()):
            pos = SlurmQueue().position_in_gpu_queue("1001")
        assert pos == (1, 2)

    def test_finds_second(self):
        with patch("subprocess.run", return_value=self._two_pending()):
            pos = SlurmQueue().position_in_gpu_queue("1002")
        assert pos == (2, 2)

    def test_unknown_returns_none(self):
        with patch("subprocess.run", return_value=self._two_pending()):
            assert SlurmQueue().position_in_gpu_queue("9999") is None


class TestReservations:
    def test_no_reservations(self):
        with patch("subprocess.run", return_value=_fake_completed("No reservations in the system\n")):
            assert SlurmQueue().reservations() == []

    def test_single_reservation_parsed(self):
        stdout = (
            "ReservationName=maint_2026_06 StartTime=2026-06-01T06:00:00 "
            "EndTime=2026-06-01T18:00:00 Duration=12:00:00 "
            "Nodes=u24-chiihm0-[621-622] NodeCnt=2 Features=(null)\n"
        )
        with patch("subprocess.run", return_value=_fake_completed(stdout)):
            reservations = SlurmQueue().reservations()
        assert len(reservations) == 1
        r = reservations[0]
        assert r.name == "maint_2026_06"
        assert r.start_time == "2026-06-01T06:00:00"
        assert r.end_time == "2026-06-01T18:00:00"
        assert r.duration == "12:00:00"
        assert r.nodes == "u24-chiihm0-[621-622]"
        assert r.node_count == 2

    def test_multiple_reservations(self):
        stdout = (
            "ReservationName=a StartTime=2026-06-01T06:00:00 EndTime=2026-06-01T18:00:00 "
            "Duration=12:00:00 Nodes=node[1-2] NodeCnt=2\n"
            "\n"
            "ReservationName=b StartTime=2026-07-01T06:00:00 EndTime=2026-07-01T18:00:00 "
            "Duration=12:00:00 Nodes=node3 NodeCnt=1\n"
        )
        with patch("subprocess.run", return_value=_fake_completed(stdout)):
            reservations = SlurmQueue().reservations()
        assert [r.name for r in reservations] == ["a", "b"]
        assert reservations[0].node_count == 2
        assert reservations[1].node_count == 1


class TestParseReservationsOutput:
    """The pure parser SSH-driven sources reuse over their own transport."""

    def test_empty_and_none(self):
        assert parse_reservations_output("") == []
        assert parse_reservations_output("No reservations in the system\n") == []

    def test_parses_window(self):
        out = parse_reservations_output(
            "ReservationName=maint StartTime=2026-06-04T06:00:00 "
            "EndTime=2026-06-04T18:00:00 Duration=12:00:00 Nodes=n[1-2] NodeCnt=2\n"
        )
        assert len(out) == 1
        assert out[0].name == "maint"
        assert out[0].start_time == "2026-06-04T06:00:00"
        assert out[0].node_count == 2


# ------------------------------------------------- colon-count GRES (live S3IT)


class TestParseGpuFromTresColonGrammar:
    """The grammar ``%b`` actually emits on Slurm 25.05 (S3IT live census,
    2026-06-04). The original parser only understood ``=``-count and reported
    (0, None) for ALL of these — the field-audit bug that blanked every view.
    """

    # The complete census over 1092 live queue rows:
    def test_census_na(self):
        assert _parse_gpu_from_tres("N/A") == (0, None)

    def test_census_untyped_one(self):
        assert _parse_gpu_from_tres("gres/gpu:1") == (1, None)

    def test_census_typed_a100(self):
        assert _parse_gpu_from_tres("gres/gpu:A100:1") == (1, "A100")

    def test_census_typed_h100(self):
        assert _parse_gpu_from_tres("gres/gpu:H100:1") == (1, "H100")

    def test_census_untyped_three(self):
        assert _parse_gpu_from_tres("gres/gpu:3") == (3, None)

    def test_census_typed_l4(self):
        assert _parse_gpu_from_tres("gres/gpu:L4:1") == (1, "L4")

    # Tolerated variants beyond the census:
    def test_multi_gpu_typed(self):
        assert _parse_gpu_from_tres("gres/gpu:H200:8") == (8, "H200")

    def test_bare_type_defaults_to_one(self):
        assert _parse_gpu_from_tres("gres/gpu:a100") == (1, "a100")

    def test_bare_gpu_defaults_to_one(self):
        assert _parse_gpu_from_tres("gres/gpu") == (1, None)

    def test_index_decoration_stripped(self):
        assert _parse_gpu_from_tres("gres/gpu:A100:2(IDX:0-1)") == (2, "A100")

    def test_typed_colon_wins_over_untyped(self):
        assert _parse_gpu_from_tres("gres/gpu:1,gres/gpu:A100:1") == (1, "A100")

    def test_mixed_with_other_tres(self):
        assert _parse_gpu_from_tres("cpu=4,mem=32G,gres/gpu:H100:2") == (2, "H100")

    def test_non_gpu_gres_ignored(self):
        assert _parse_gpu_from_tres("gres/shard:4") == (0, None)


# ----------------------------------------------------------- array-id helpers


class TestArrayIdHelpers:
    def test_strip_plain(self):
        assert strip_array_suffix("3713695") == "3713695"

    def test_strip_task(self):
        assert strip_array_suffix("3703585_14") == "3703585"

    def test_strip_collapsed_range(self):
        assert strip_array_suffix("3710878_[690-1920]") == "3710878"

    def test_strip_throttled_range(self):
        assert strip_array_suffix("3713285_[6-44%4]") == "3713285"

    def test_count_plain_and_single_task(self):
        assert parse_array_task_count("3713695") == 1
        assert parse_array_task_count("3703585_14") == 1

    def test_count_range(self):
        assert parse_array_task_count("3703585_[19-22]") == 4

    def test_count_big_live_range(self):
        # The live row that motivated this: ONE squeue line, 1231 queued tasks.
        assert parse_array_task_count("3710878_[690-1920]") == 1231

    def test_count_throttle_is_not_a_divisor(self):
        # %N limits concurrency, it does not change how many tasks are queued.
        assert parse_array_task_count("3713285_[6-44%4]") == 39

    def test_count_comma_list_with_ranges(self):
        assert parse_array_task_count("123_[1,3,7-9]") == 5

    def test_count_step_range(self):
        # Slurm reconstructs `lo-hi:step` for evenly-spaced pending indices
        # (sbatch --array=0-100:10). 0,10,...,100 → 11 tasks, not 1.
        assert parse_array_task_count("123_[0-100:10]") == 11
        assert parse_array_task_count("123_[1-9:2]") == 5

    def test_count_step_range_with_throttle(self):
        assert parse_array_task_count("123_[0-15:4%2]") == 4

    def test_count_zero_step_is_tolerated(self):
        # Malformed step → treated as step 1, not a ZeroDivisionError.
        assert parse_array_task_count("123_[1-5:0]") == 5

    def test_count_unparseable_part_counts_one(self):
        assert parse_array_task_count("123_[x]") == 1


# ------------------------------------------------------------ pure aggregators


def _job(job_id: str, state: str = "PENDING", gpu_count: int = 1,
         gpu_type: Optional[str] = None, task_count: int = 1,
         reason: str = "(Priority)") -> QueueJob:
    return QueueJob(
        job_id=job_id, name="n", user="u", state=state, reason=reason,
        partition="standard", tres_per_node="", expected_start="N/A",
        priority=0, gpu_count=gpu_count, gpu_type=gpu_type, task_count=task_count,
    )


class TestSummarizeTaskWeighted:
    def test_collapsed_pending_array_counts_all_tasks(self):
        # One collapsed row, 10 tasks × 1 A100 each → 10 pending A100 GPUs.
        summary = summarize_gpu_jobs([_job("2001_[1-10]", gpu_type="A100", task_count=10)])
        assert summary == {"A100": {"PENDING": 10}}

    def test_expanded_rows_unchanged(self):
        summary = summarize_gpu_jobs(
            [_job("2001_1", gpu_type="A100"), _job("2001_2", gpu_type="A100")]
        )
        assert summary == {"A100": {"PENDING": 2}}


class TestPositionsByBase:
    def test_groups_expanded_array_tasks(self):
        pending = [
            _job("9_1"),           # someone else's task at position 1
            _job("3703585_19"),    # mine
            _job("9_2"),
            _job("3703585_20"),    # mine
        ]
        by_base = positions_by_base(pending)
        assert by_base["3703585"] == [2, 4]
        assert by_base["9"] == [1, 3]


class TestCountingPathsExpandArrays:
    """The counting paths must pass -r so Slurm expands pending arrays
    per-task; the display path (list_user_jobs) must NOT, to stay compact."""

    def _capture_cmd(self, call):
        with patch("subprocess.run", return_value=_fake_completed("")) as mock_run:
            call()
        return mock_run.call_args[0][0]

    def test_pending_gpu_sorted_uses_r(self):
        cmd = self._capture_cmd(lambda: SlurmQueue().pending_gpu_jobs_sorted())
        assert "-r" in cmd

    def test_gpu_summary_uses_r(self):
        cmd = self._capture_cmd(lambda: SlurmQueue().gpu_summary())
        assert "-r" in cmd

    def test_list_user_jobs_stays_collapsed(self):
        cmd = self._capture_cmd(lambda: SlurmQueue().list_user_jobs("alice"))
        assert "-r" not in cmd
        assert "-u" in cmd and "alice" in cmd


# ------------------------------------------------------- grouped mine helpers


def _group(base_id: str, **kw) -> JobGroup:
    defaults = dict(
        name="n", user="u", partition="standard",
        gpu_count=0, gpu_type=None, is_array=True,
    )
    defaults.update(kw)
    return JobGroup(base_id=base_id, **defaults)


class TestGroupJobsByArray:
    def test_mixed_array_aggregation(self):
        jobs = [
            _job("9001_1", state="RUNNING", gpu_type="A100", reason="node-a"),
            _job("9001_2", state="RUNNING", gpu_type="A100", reason="node-a"),
            _job("9001_3", state="RUNNING", gpu_type="A100", reason="node-b"),
            _job("9001_[5-10]", state="PENDING", gpu_type="A100", task_count=6),
            _job("7777", state="PENDING", gpu_count=0, reason="(Resources)"),
        ]
        groups = group_jobs_by_array(jobs)
        assert [g.base_id for g in groups] == ["9001", "7777"]  # first-seen order
        arr, single = groups
        assert arr.is_array and not single.is_array
        assert arr.running == 3
        assert arr.pending == 6  # task-weighted from the collapsed row
        assert arr.in_queue == 9
        assert arr.nodes == ("node-a", "node-b")  # deduped
        assert arr.reason == "(Priority)"
        assert arr.gpu_count == 1 and arr.gpu_type == "A100"
        assert single.pending == 1 and single.gpu_count == 0
        assert single.reason == "(Resources)"

    def test_gpu_spec_from_first_gpu_bearing_row(self):
        jobs = [
            _job("1_1", state="COMPLETING", gpu_count=0),
            _job("1_2", state="RUNNING", gpu_count=1, gpu_type="L4", reason="n1"),
        ]
        g = group_jobs_by_array(jobs)[0]
        assert g.gpu_count == 1 and g.gpu_type == "L4"
        assert g.other == 1 and g.running == 1

    def test_unenriched_groups_have_unknown_accounting(self):
        g = group_jobs_by_array([_job("5_1", state="RUNNING", reason="n")])[0]
        assert g.completed is None and g.failed is None and g.total is None


class TestParseSacctJobStates:
    # Shaped like the live S3IT output (sacct -j a,b -n -X -P -o JobID,State).
    _LIVE_SHAPED = "\n".join(
        [
            "3703585_1|COMPLETED",
            "3703585_2|COMPLETED",
            "3703585_11|FAILED",
            "3703585_9|RUNNING",
            "3703585_[19-22]|PENDING",
            "3710878_5|CANCELLED by 123456",
            "3710878_6|TIMEOUT+",
            "3710878_[859-1920]|PENDING",
        ]
    )

    def test_live_shaped_fixture(self):
        states = parse_sacct_job_states(self._LIVE_SHAPED)
        assert states["3703585"] == {
            "COMPLETED": 2, "FAILED": 1, "RUNNING": 1, "PENDING": 4,
        }
        # CANCELLED-by long form parsed; TIMEOUT folds into FAILED; the
        # collapsed pending range is task-counted.
        assert states["3710878"] == {"CANCELLED": 1, "FAILED": 1, "PENDING": 1062}

    def test_empty_and_garbage(self):
        assert parse_sacct_job_states("") == {}
        assert parse_sacct_job_states("no pipes here\n\n") == {}

    def test_unknown_state_counts_as_running(self):
        # Same default the sweep path uses: unknown → non-terminal.
        assert parse_sacct_job_states("1|REQUEUED\n") == {"1": {"RUNNING": 1}}


class TestEnrichGroupsWithAccounting:
    def test_none_states_is_noop(self):
        out = enrich_groups_with_accounting([_group("1")], None)
        assert out[0].completed is None and out[0].total is None

    def test_merges_counts_and_total(self):
        states = {"3703585": {"COMPLETED": 8, "FAILED": 1, "RUNNING": 9, "PENDING": 4}}
        e = enrich_groups_with_accounting([_group("3703585")], states)[0]
        assert e.completed == 8
        assert e.failed == 1
        assert e.total == 22  # sum over every state — the array's true size

    def test_cancelled_folds_into_failed(self):
        states = {"1": {"COMPLETED": 1, "CANCELLED": 2, "FAILED": 1}}
        e = enrich_groups_with_accounting([_group("1")], states)[0]
        assert e.failed == 3 and e.total == 4

    def test_base_missing_from_states_left_unenriched(self):
        e = enrich_groups_with_accounting([_group("42")], {"other": {"COMPLETED": 1}})[0]
        assert e.completed is None


class TestSplitGresEntries:
    def test_comma_inside_parens_is_one_entry(self):
        # THE live trap: GresUsed index lists contain commas.
        assert _split_gres_entries("gpu:A100:6(IDX:0-1,4-7)") == [
            "gpu:A100:6(IDX:0-1,4-7)"
        ]

    def test_top_level_commas_split(self):
        assert _split_gres_entries("gpu:A100:8,shard:a100:32") == [
            "gpu:A100:8",
            "shard:a100:32",
        ]

    def test_mixed(self):
        assert _split_gres_entries("gpu:H100:2(IDX:0,1),gpu:L4:1") == [
            "gpu:H100:2(IDX:0,1)",
            "gpu:L4:1",
        ]


class TestParseSinfoGpuCapacity:
    # Shaped like live S3IT `sinfo -h -N -O NodeHost,StateCompact,Gres,GresUsed`
    # (incl. the duplicate rows -N emits for multi-partition nodes and the
    # comma-bearing IDX decorations).
    _LIVE_SHAPED = "\n".join(
        [
            "node-611   mix    gpu:A100:8   gpu:A100:4(IDX:0-3)",
            "node-612   mix-   gpu:A100:8   gpu:A100:6(IDX:0-1,4-7)",
            "node-612   mix-   gpu:A100:8   gpu:A100:6(IDX:0-1,4-7)",  # dup partition row
            "node-613   alloc  gpu:A100:8   gpu:A100:8(IDX:0-7)",
            "node-700   idle   gpu:L4:1     gpu:0",
            "node-701   comp   gpu:H100:2   gpu:H100:2(IDX:0-1)",
            "node-800   down*  gpu:H100:8   gpu:0",
            "node-801   drain  gpu:L4:1     gpu:0",
            "node-802   drng   gpu:L4:1     gpu:L4:1(IDX:0)",  # draining BUT busy → counts
            "node-900   idle   (null)       (null)",
            "cpu-node   mix    (null)",
        ]
    )

    def test_live_shaped_fixture(self):
        capacity, excluded = parse_sinfo_gpu_capacity(self._LIVE_SHAPED)
        # node-612 counted ONCE despite the duplicate partition row; the
        # IDX-comma entry parses as 6 used (a naive comma split would say 1).
        assert capacity["A100"] == {"total": 24, "used": 18}
        assert capacity["H100"] == {"total": 2, "used": 2}  # down node excluded
        # idle L4 contributes total only; drng node is present AND in use.
        assert capacity["L4"] == {"total": 2, "used": 1}
        assert excluded == 9  # 8×H100 down + 1×L4 drained

    def test_untyped_used_zero_ignored(self):
        capacity, _ = parse_sinfo_gpu_capacity("n1 idle gpu:A100:2 gpu:0\n")
        assert capacity["A100"] == {"total": 2, "used": 0}
        assert "<untyped>" not in capacity

    def test_state_suffix_flags_stripped(self):
        capacity, excluded = parse_sinfo_gpu_capacity(
            "n1 down~ gpu:H100:4 gpu:0\nn2 mix* gpu:H100:4 gpu:H100:1(IDX:0)\n"
        )
        assert capacity["H100"] == {"total": 4, "used": 1}
        assert excluded == 4

    def test_empty(self):
        assert parse_sinfo_gpu_capacity("") == ({}, 0)


class TestGpuCapacityTransports:
    def test_local_missing_binary_returns_none(self):
        with patch("subprocess.run", side_effect=FileNotFoundError):
            assert SlurmQueue().gpu_capacity() is None

    def test_local_rc_nonzero_returns_none(self):
        with patch("subprocess.run", return_value=_fake_completed("", "err", 1)):
            assert SlurmQueue().gpu_capacity() is None

    def test_local_happy_path(self):
        with patch(
            "subprocess.run",
            return_value=_fake_completed("n1 idle gpu:L4:2 gpu:L4:1(IDX:0)\n"),
        ):
            assert SlurmQueue().gpu_capacity() == ({"L4": {"total": 2, "used": 1}}, 0)


class TestSacctTransports:
    def test_local_missing_binary_returns_none(self):
        with patch("subprocess.run", side_effect=FileNotFoundError):
            assert SlurmQueue().sacct_job_states(["1"]) is None

    def test_local_rc_nonzero_returns_none(self):
        with patch("subprocess.run", return_value=_fake_completed("", "disabled", 1)):
            assert SlurmQueue().sacct_job_states(["1"]) is None

    def test_local_happy_path(self):
        with patch("subprocess.run", return_value=_fake_completed("1_1|COMPLETED\n")):
            assert SlurmQueue().sacct_job_states(["1"]) == {"1": {"COMPLETED": 1}}

    def test_local_empty_ids_short_circuits(self):
        with patch("subprocess.run", side_effect=AssertionError("must not run")):
            assert SlurmQueue().sacct_job_states([]) == {}


# --------------------------------------------------------------- SSHSlurmQueue


class _Result:
    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = ""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class FakeConn:
    """asyncssh stand-in: substring responder, records every command."""

    def __init__(self):
        self.run_calls: List[str] = []
        self._responder: List[tuple] = []
        self.run_delay_s: float = 0.0

    def add(self, sub: str, res: _Result) -> None:
        self._responder.append((sub, res))

    async def run(self, cmd: str, *, input: Optional[str] = None, check: bool = False):
        self.run_calls.append(cmd)
        if self.run_delay_s:
            await asyncio.sleep(self.run_delay_s)
        for i, (sub, res) in enumerate(self._responder):
            if sub in cmd:
                del self._responder[i]
                return res
        return _Result(0, "")


# A live-grammar row (colon-count GRES) as the remote would emit it.
_REMOTE_ROW_A100 = "\t".join(
    ["3703585_14", "sweep_x_array", "gbena", "RUNNING", "u24-chaiam0-615",
     "standard", "gres/gpu:A100:1", "2026-06-04T09:05:27", "106515"]
)


class TestSSHSlurmQueue:
    @pytest.mark.asyncio
    async def test_format_string_survives_shell_quoting(self):
        """THE wire-level gotcha: the format string contains literal tabs.

        Unquoted, a remote shell word-splits them into separate argv entries
        and squeue gets a broken --format. shlex.join must keep it ONE token
        with the real tabs intact (squeue treats a textual ``\\t`` as two
        characters — only real tabs delimit).
        """
        conn = FakeConn()
        await SSHSlurmQueue(conn).list_user_jobs("gbena")
        cmd = conn.run_calls[0]
        # Re-split the way the remote shell would: format must be one token.
        tokens = shlex.split(cmd)
        fmt_tokens = [t for t in tokens if t.startswith("--format=")]
        assert fmt_tokens == [f"--format={SQUEUE_FORMAT}"]
        assert "\t" in fmt_tokens[0]  # real tabs, not backslash-t text

    @pytest.mark.asyncio
    async def test_parses_live_grammar_rows(self):
        conn = FakeConn()
        conn.add("squeue", _Result(0, _REMOTE_ROW_A100 + "\n"))
        jobs = await SSHSlurmQueue(conn).list_user_jobs("gbena")
        assert len(jobs) == 1
        assert jobs[0].gpu_count == 1
        assert jobs[0].gpu_type == "A100"

    @pytest.mark.asyncio
    async def test_pending_path_passes_r_flag(self):
        conn = FakeConn()
        await SSHSlurmQueue(conn).pending_gpu_jobs_sorted()
        assert "-r" in shlex.split(conn.run_calls[0])

    @pytest.mark.asyncio
    async def test_nonzero_rc_raises_not_empty(self):
        """Over SSH an empty table must mean 'no jobs', never 'squeue broke'."""
        conn = FakeConn()
        conn.add("squeue", _Result(127, "", "bash: squeue: command not found"))
        with pytest.raises(QueueCommandError, match="rc=127"):
            await SSHSlurmQueue(conn).list_user_jobs("gbena")

    @pytest.mark.asyncio
    async def test_timeout_raises(self):
        conn = FakeConn()
        conn.run_delay_s = 0.2
        q = SSHSlurmQueue(conn, timeout_s=0.01)
        with pytest.raises(QueueCommandError, match="timed out"):
            await q.list_user_jobs("gbena")

    @pytest.mark.asyncio
    async def test_whoami(self):
        conn = FakeConn()
        conn.add("whoami", _Result(0, "gbena\n"))
        assert await SSHSlurmQueue(conn).whoami() == "gbena"

    @pytest.mark.asyncio
    async def test_whoami_empty_raises(self):
        conn = FakeConn()
        conn.add("whoami", _Result(0, "\n"))
        with pytest.raises(QueueCommandError):
            await SSHSlurmQueue(conn).whoami()

    @pytest.mark.asyncio
    async def test_reservations_roundtrip(self):
        conn = FakeConn()
        conn.add(
            "scontrol",
            _Result(0, "ReservationName=maint StartTime=s EndTime=e "
                       "Duration=d Nodes=n NodeCnt=3\n"),
        )
        res = await SSHSlurmQueue(conn).reservations()
        assert len(res) == 1 and res[0].node_count == 3

    @pytest.mark.asyncio
    async def test_sacct_failure_returns_none_not_raise(self):
        """Deliberate asymmetry: sacct is optional enrichment — a cluster
        without accounting must degrade, not error (unlike squeue)."""
        conn = FakeConn()
        conn.add("sacct", _Result(1, "", "Slurm accounting storage is disabled"))
        assert await SSHSlurmQueue(conn).sacct_job_states(["1"]) is None

    @pytest.mark.asyncio
    async def test_sacct_happy_path(self):
        conn = FakeConn()
        conn.add("sacct", _Result(0, "1_1|COMPLETED\n1_2|FAILED\n"))
        states = await SSHSlurmQueue(conn).sacct_job_states(["1"])
        assert states == {"1": {"COMPLETED": 1, "FAILED": 1}}

    @pytest.mark.asyncio
    async def test_sacct_empty_ids_short_circuits(self):
        conn = FakeConn()
        assert await SSHSlurmQueue(conn).sacct_job_states([]) == {}
        assert conn.run_calls == []  # "nothing to ask" ≠ a remote round-trip

    @pytest.mark.asyncio
    async def test_sinfo_failure_returns_none_not_raise(self):
        conn = FakeConn()
        conn.add("sinfo", _Result(127, "", "bash: sinfo: command not found"))
        assert await SSHSlurmQueue(conn).gpu_capacity() is None

    @pytest.mark.asyncio
    async def test_sinfo_happy_path(self):
        conn = FakeConn()
        conn.add("sinfo", _Result(0, "n1 mix gpu:A100:8 gpu:A100:4(IDX:0-3)\n"))
        capacity = await SSHSlurmQueue(conn).gpu_capacity()
        assert capacity == ({"A100": {"total": 8, "used": 4}}, 0)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "method,args",
        [
            ("list_user_jobs", ("alice",)),
            ("pending_gpu_jobs_sorted", ()),
            ("gpu_summary", ()),
            ("reservations", ()),
            ("sacct_job_states", (["1", "2"],)),
            ("gpu_capacity", ()),
        ],
    )
    async def test_same_commands_as_local_transport(self, method, args):
        """Anti-drift: the SSH twin must run exactly the local argv, joined —
        for every query path, not just one exemplar."""
        conn = FakeConn()
        await getattr(SSHSlurmQueue(conn), method)(*args)
        with patch("subprocess.run", return_value=_fake_completed("")) as mock_run:
            getattr(SlurmQueue(), method)(*args)
        assert shlex.split(conn.run_calls[0]) == mock_run.call_args[0][0]

    def test_bare_trailing_colon_type_is_none_not_empty(self):
        # "gres/gpu:" must not yield gpu_type="" (falsy-but-not-None trap).
        assert _parse_gpu_from_tres("gres/gpu:") == (1, None)
