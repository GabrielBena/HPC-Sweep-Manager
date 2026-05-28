"""Unit tests for SlurmComputeSource's pure-Python logic.

Subprocess-touching code (submit_job, get_job_status, sbatch/squeue calls)
is exercised in Phase 2 via the fake_slurm PATH-stub fixture. These tests
focus on directive rendering, spec resolution, and QOS validation.
"""

from __future__ import annotations

import pytest

from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
from hpc_sweep_manager.core.hpc.slurm_compute_source import SlurmComputeSource
from hpc_sweep_manager.core.hpc.slurm_protocol import render_sbatch_directives


class TestConstruction:
    def test_default_source_type(self):
        src = SlurmComputeSource()
        assert src.source_type == "slurm"
        assert src.name == "slurm"

    def test_default_max_parallel_jobs_unlimited(self):
        # 0 → unlimited (we use a high sentinel so the abstract slot-count logic still works)
        src = SlurmComputeSource()
        assert src.max_parallel_jobs >= 1_000

    def test_explicit_max_parallel_jobs(self):
        src = SlurmComputeSource(max_parallel_jobs=20)
        assert src.max_parallel_jobs == 20

    def test_default_spec_is_empty_when_unset(self):
        src = SlurmComputeSource()
        assert src.default_spec == ResourceSpec()


class TestDirectiveRendering:
    def test_empty_spec_emits_nothing(self):
        src = SlurmComputeSource()
        assert render_sbatch_directives(ResourceSpec()) == ""

    def test_walltime(self):
        src = SlurmComputeSource()
        out = render_sbatch_directives(ResourceSpec(walltime="04:00:00"))
        assert "#SBATCH --time=04:00:00" in out.splitlines()

    def test_cpus_per_task(self):
        src = SlurmComputeSource()
        out = render_sbatch_directives(ResourceSpec(cpus_per_task=8))
        assert "#SBATCH --cpus-per-task=8" in out.splitlines()

    def test_mem_only(self):
        src = SlurmComputeSource()
        out = render_sbatch_directives(ResourceSpec(mem="32G"))
        assert "#SBATCH --mem=32G" in out.splitlines()
        assert "--mem-per-cpu" not in out

    def test_mem_per_cpu_only(self):
        src = SlurmComputeSource()
        out = render_sbatch_directives(ResourceSpec(mem_per_cpu="4G"))
        assert "#SBATCH --mem-per-cpu=4G" in out.splitlines()
        assert not any(l == "#SBATCH --mem=" for l in out.splitlines())

    def test_gpus_without_type_uses_gpus_flag(self):
        src = SlurmComputeSource()
        out = render_sbatch_directives(ResourceSpec(gpus=2))
        assert "#SBATCH --gpus=2" in out.splitlines()
        assert "--gres" not in out

    def test_gpu_type_uses_gres(self):
        src = SlurmComputeSource()
        out = render_sbatch_directives(ResourceSpec(gpus=1, gpu_type="h100"))
        assert "#SBATCH --gres=gpu:h100:1" in out.splitlines()
        assert "--gpus=" not in out

    def test_zero_gpus_emits_nothing_gpu_related(self):
        src = SlurmComputeSource()
        out = render_sbatch_directives(ResourceSpec(gpus=0))
        assert "--gpus" not in out
        assert "--gres" not in out

    def test_s3it_qos(self):
        src = SlurmComputeSource()
        for q in ("normal", "medium", "long"):
            out = render_sbatch_directives(ResourceSpec(qos=q))
            assert f"#SBATCH --qos={q}" in out.splitlines()

    def test_partition_and_account(self):
        src = SlurmComputeSource()
        out = render_sbatch_directives(ResourceSpec(partition="gpu", account="proj-x"))
        assert "#SBATCH --partition=gpu" in out.splitlines()
        assert "#SBATCH --account=proj-x" in out.splitlines()

    def test_extra_directives_with_value(self):
        src = SlurmComputeSource()
        spec = ResourceSpec(extra_directives=(("--mail-type", "BEGIN"),))
        out = render_sbatch_directives(spec)
        assert "#SBATCH --mail-type=BEGIN" in out.splitlines()

    def test_extra_directive_without_value(self):
        src = SlurmComputeSource()
        spec = ResourceSpec(extra_directives=(("--exclusive", ""),))
        out = render_sbatch_directives(spec)
        assert "#SBATCH --exclusive" in out.splitlines()

    def test_full_s3it_spec(self):
        src = SlurmComputeSource()
        spec = ResourceSpec(
            walltime="04:00:00",
            cpus_per_task=4,
            mem_per_cpu="4G",
            gpus=1,
            gpu_type="h100",
            qos="normal",
        )
        out = render_sbatch_directives(spec)
        assert "--time=04:00:00" in out
        assert "--cpus-per-task=4" in out
        assert "--mem-per-cpu=4G" in out
        assert "--gres=gpu:h100:1" in out
        assert "--qos=normal" in out


class TestEffectiveSpec:
    def test_default_only(self):
        default = ResourceSpec(walltime="04:00:00", cpus_per_task=4)
        src = SlurmComputeSource(default_spec=default)
        assert src._effective_spec(None) == default

    def test_override_merges(self):
        default = ResourceSpec(walltime="04:00:00", cpus_per_task=4)
        override = ResourceSpec(walltime="24:00:00", qos="long")
        src = SlurmComputeSource(default_spec=default)
        eff = src._effective_spec(override)
        assert eff.walltime == "24:00:00"  # overridden
        assert eff.cpus_per_task == 4  # preserved
        assert eff.qos == "long"  # added

    def test_qos_whitelist_accepts_valid(self):
        src = SlurmComputeSource(qos_whitelist=frozenset({"normal", "medium", "long"}))
        eff = src._effective_spec(ResourceSpec(qos="normal"))
        assert eff.qos == "normal"

    def test_qos_whitelist_rejects_invalid(self):
        src = SlurmComputeSource(qos_whitelist=frozenset({"normal", "medium", "long"}))
        with pytest.raises(ValueError, match="qos='special'"):
            src._effective_spec(ResourceSpec(qos="special"))

    def test_qos_whitelist_skipped_when_qos_unset(self):
        # Empty qos shouldn't trigger validation even with a whitelist set.
        src = SlurmComputeSource(qos_whitelist=frozenset({"normal"}))
        eff = src._effective_spec(ResourceSpec(walltime="01:00:00"))
        assert eff.qos is None

    def test_no_whitelist_accepts_any_qos(self):
        src = SlurmComputeSource()
        eff = src._effective_spec(ResourceSpec(qos="custom"))
        assert eff.qos == "custom"


class TestTemplatesExist:
    """Sanity check: the templates SlurmComputeSource references actually exist."""

    def test_slurm_single_template_exists(self):
        from pathlib import Path
        import hpc_sweep_manager

        template_dir = Path(hpc_sweep_manager.__file__).parent / "templates"
        assert (template_dir / "slurm_single.sh.j2").is_file()

    def test_slurm_array_template_exists(self):
        from pathlib import Path
        import hpc_sweep_manager

        template_dir = Path(hpc_sweep_manager.__file__).parent / "templates"
        assert (template_dir / "slurm_array.sh.j2").is_file()


class TestTemplateRendering:
    """Render the templates with a realistic context and check the output is well-formed."""

    def test_single_template_renders(self):
        from hpc_sweep_manager.core.common.templating import render_template

        src = SlurmComputeSource()
        spec = ResourceSpec(walltime="01:00:00", cpus_per_task=2, qos="normal")
        rendered = render_template(
            "slurm_single.sh.j2",
            job_name="test_job",
            sweep_id="sweep_42",
            logs_dir="/tmp/logs",
            task_dir="/tmp/tasks/test_job",
            sbatch_directives=render_sbatch_directives(spec),
            modules=["h100"],
            pre_script=["source ~/venv/bin/activate"],
            project_dir="/home/user/project",
            python_path="python",
            script_path="train.py",
            params_hydra='"lr=0.001"',
            wandb_group=None,
        )
        assert rendered.startswith("#!/bin/bash")
        assert "#SBATCH --job-name=test_job" in rendered
        assert "#SBATCH --time=01:00:00" in rendered
        assert "#SBATCH --qos=normal" in rendered
        assert "module load h100" in rendered
        assert "source ~/venv/bin/activate" in rendered
        assert "cd /home/user/project" in rendered
        assert "python train.py" in rendered

    def test_array_template_renders(self):
        from hpc_sweep_manager.core.common.templating import render_template

        src = SlurmComputeSource()
        spec = ResourceSpec(walltime="04:00:00", cpus_per_task=2, gpus=1, gpu_type="l4")
        rendered = render_template(
            "slurm_array.sh.j2",
            job_name="sweep_42_array",
            sweep_id="sweep_42",
            num_jobs=4,
            logs_dir="/tmp/logs",
            tasks_dir="/tmp/tasks",
            params_file="/tmp/params.json",
            sbatch_directives=render_sbatch_directives(spec),
            modules=["l4"],
            pre_script=[],
            project_dir="/home/user/project",
            python_path="python",
            script_path="train.py",
            wandb_group="my_group",
        )
        assert "#SBATCH --array=1-4" in rendered
        assert "#SBATCH --gres=gpu:l4:1" in rendered
        assert "module load l4" in rendered
        assert "$SLURM_ARRAY_TASK_ID" in rendered
        assert "WANDB_GROUP=\"my_group\"" in rendered
