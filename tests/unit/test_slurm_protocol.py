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


class TestRenderRejectsMultiType:
    """A multi-type spec reaching the renderer means some path skipped the
    planner — refuse loudly instead of emitting a broken --gres."""

    def test_tuple_gpu_type_raises(self):
        import pytest

        from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
        from hpc_sweep_manager.core.hpc.slurm_protocol import (
            render_sbatch_directives,
        )

        spec = ResourceSpec(gpus=1, gpu_type=("A100", "H200"))
        with pytest.raises(ValueError, match="scalarize via gpu_planner"):
            render_sbatch_directives(spec)

    def test_scalar_gpu_type_still_renders(self):
        from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
        from hpc_sweep_manager.core.hpc.slurm_protocol import (
            render_sbatch_directives,
        )

        out = render_sbatch_directives(ResourceSpec(gpus=2, gpu_type="H100"))
        assert "#SBATCH --gres=gpu:H100:2" in out


class TestRenderChainDirectives:
    """Resumable-chain additions (issue #12): --dependency / --signal."""

    def _spec(self):
        from hpc_sweep_manager.core.common.resource_spec import ResourceSpec

        return ResourceSpec(walltime="23:00:00", cpus_per_task=4)

    def test_omitted_is_byte_identical(self):
        from hpc_sweep_manager.core.hpc.slurm_protocol import render_sbatch_directives

        spec = self._spec()
        assert render_sbatch_directives(spec) == render_sbatch_directives(
            spec, dependency=None, signal=None
        )
        assert "--dependency" not in render_sbatch_directives(spec)
        assert "--signal" not in render_sbatch_directives(spec)

    def test_dependency_line(self):
        from hpc_sweep_manager.core.hpc.slurm_protocol import render_sbatch_directives

        out = render_sbatch_directives(self._spec(), dependency="afterany:12345")
        assert "#SBATCH --dependency=afterany:12345" in out

    def test_signal_line(self):
        from hpc_sweep_manager.core.hpc.slurm_protocol import render_sbatch_directives

        out = render_sbatch_directives(self._spec(), signal="B:TERM@120")
        assert "#SBATCH --signal=B:TERM@120" in out

    def test_both_appended_after_extra_directives(self):
        from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
        from hpc_sweep_manager.core.hpc.slurm_protocol import render_sbatch_directives

        spec = ResourceSpec(
            walltime="23:00:00", extra_directives=(("--exclusive", ""),)
        )
        out = render_sbatch_directives(
            spec, dependency="afterany:9:10", signal="B:TERM@120"
        )
        lines = out.splitlines()
        assert lines.index("#SBATCH --exclusive") < lines.index(
            "#SBATCH --dependency=afterany:9:10"
        )
        assert lines.index("#SBATCH --dependency=afterany:9:10") < lines.index(
            "#SBATCH --signal=B:TERM@120"
        )

    def test_format_signal(self):
        from hpc_sweep_manager.core.hpc.slurm_protocol import format_signal

        assert format_signal(120) == "B:TERM@120"
        assert format_signal(60) == "B:TERM@60"
