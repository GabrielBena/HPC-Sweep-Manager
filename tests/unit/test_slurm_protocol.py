"""Unit tests for the pure Slurm protocol helpers (no I/O, no shell).

Directive rendering; the terminal-state classification moved to ``slurm_base``
(``tests/unit/test_slurm_base.py``).
"""

from __future__ import annotations

import pytest

from hpc_sweep_manager.core.hpc.slurm_protocol import parse_sbatch_job_id


class TestParseSbatchJobId:
    def test_the_submitted_line(self):
        assert parse_sbatch_job_id("Submitted batch job 6734161\n") == "6734161"

    def test_a_notice_after_it_is_ignored(self):
        out = "Submitted batch job 42\nsbatch: note: job goes to qos medium\n"
        assert parse_sbatch_job_id(out) == "42"

    @pytest.mark.parametrize("out", ["", "sbatch: error: Batch job submission failed\n"])
    def test_no_id_raises_instead_of_guessing(self, out):
        # A guessed id is unknown to sacct, reads as finished, and gets its sweep dir cleaned.
        with pytest.raises(ValueError, match="no job id"):
            parse_sbatch_job_id(out)


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

        spec = ResourceSpec(walltime="23:00:00", extra_directives=(("--exclusive", ""),))
        out = render_sbatch_directives(spec, dependency="afterany:9:10", signal="B:TERM@120")
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
