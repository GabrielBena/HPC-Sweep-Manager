"""Unit tests for the pure Slurm protocol helpers (no I/O, no shell).

Covers ``parse_sacct_state`` — the terminal-state classifier that backs the
fix for "FAILED jobs reported as COMPLETED" (queue-absence is not a completion
signal; once a job leaves ``squeue`` we ask ``sacct``).
"""

from __future__ import annotations

from hpc_sweep_manager.core.hpc.slurm_protocol import parse_sacct_state


class TestParseSacctState:
    def test_empty_returns_none(self):
        assert parse_sacct_state("") is None
        assert parse_sacct_state("\n   \n") is None

    def test_single_completed(self):
        assert parse_sacct_state("COMPLETED\n") == "COMPLETED"

    def test_single_failed(self):
        assert parse_sacct_state("FAILED\n") == "FAILED"

    def test_timeout_maps_to_failed(self):
        assert parse_sacct_state("TIMEOUT\n") == "FAILED"

    def test_oom_maps_to_failed(self):
        assert parse_sacct_state("OUT_OF_MEMORY\n") == "FAILED"

    def test_node_fail_maps_to_failed(self):
        assert parse_sacct_state("NODE_FAIL\n") == "FAILED"

    def test_cancelled_long_form(self):
        # sacct's State column for a scancel'd job: "CANCELLED by <uid>".
        assert parse_sacct_state("CANCELLED by 12345\n") == "CANCELLED"

    def test_trailing_plus_truncation(self):
        # Narrow -o widths truncate with a trailing '+'.
        assert parse_sacct_state("CANCELLED+\n") == "CANCELLED"
        assert parse_sacct_state("COMPLETED+\n") == "COMPLETED"

    def test_running_and_pending_are_nonterminal(self):
        assert parse_sacct_state("RUNNING\n") == "RUNNING"
        assert parse_sacct_state("PENDING\n") == "RUNNING"

    def test_unknown_state_treated_as_running(self):
        # Unknown → RUNNING (safer than treating as terminal).
        assert parse_sacct_state("WEIRD_NEW_STATE\n") == "RUNNING"

    # --- array aggregation (one row per task via -X) ---

    def test_array_all_completed(self):
        assert parse_sacct_state("COMPLETED\nCOMPLETED\nCOMPLETED\n") == "COMPLETED"

    def test_array_any_failed_is_failed(self):
        assert parse_sacct_state("COMPLETED\nFAILED\nCOMPLETED\n") == "FAILED"

    def test_array_cancelled_when_no_failed(self):
        assert parse_sacct_state("COMPLETED\nCANCELLED\n") == "CANCELLED"

    def test_array_running_takes_precedence_over_failed(self):
        # Not done yet — keep waiting even though one task already failed.
        # (We'll see the FAILED once every task is terminal.)
        assert parse_sacct_state("FAILED\nRUNNING\n") == "RUNNING"

    def test_array_failed_beats_cancelled(self):
        assert parse_sacct_state("FAILED\nCANCELLED\nCOMPLETED\n") == "FAILED"
