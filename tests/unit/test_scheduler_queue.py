"""Unit tests for :mod:`core.hpc.scheduler_queue`.

Strategy: mock ``subprocess.run`` directly (no shell, no fixture binaries).
Each test feeds canned ``squeue`` / ``scontrol`` stdout and asserts the
parsed dataclasses come out right.
"""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from hpc_sweep_manager.core.hpc.scheduler_queue import (
    QueueJob,
    Reservation,
    SlurmQueue,
    _parse_gpu_from_tres,
    _parse_priority,
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
