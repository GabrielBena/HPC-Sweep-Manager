"""Unit tests for templating utilities."""

from __future__ import annotations

import pytest

from hpc_sweep_manager.core.common.templating import (
    params_to_hydra_args,
    render_template,
)
from hpc_sweep_manager.core.hpc.slurm_compute_source import _python_needs_conda_init


class TestParamsToHydraArgs:
    def test_empty_dict(self):
        assert params_to_hydra_args({}) == ""

    def test_simple_string(self):
        assert params_to_hydra_args({"name": "alice"}) == '"name=alice"'

    def test_integer(self):
        assert params_to_hydra_args({"seed": 42}) == '"seed=42"'

    def test_float(self):
        assert params_to_hydra_args({"lr": 0.001}) == '"lr=0.001"'

    def test_none_becomes_null(self):
        assert params_to_hydra_args({"checkpoint": None}) == '"checkpoint=null"'

    def test_bool_lowercased(self):
        assert params_to_hydra_args({"shuffle": True}) == '"shuffle=true"'
        assert params_to_hydra_args({"shuffle": False}) == '"shuffle=false"'

    def test_list_becomes_bracket_form(self):
        result = params_to_hydra_args({"layers": [128, 256, 512]})
        assert result == '"layers=[128, 256, 512]"'

    def test_tuple_treated_as_list(self):
        result = params_to_hydra_args({"layers": (128, 256)})
        assert result == '"layers=[128, 256]"'

    def test_string_with_space_quoted(self):
        # Already quoted by the outer "" — Hydra parses inner spaces
        assert params_to_hydra_args({"msg": "hello world"}) == '"msg=hello world"'

    def test_string_with_comma_quoted(self):
        assert params_to_hydra_args({"tags": "a,b,c"}) == '"tags=a,b,c"'

    def test_multiple_params(self):
        result = params_to_hydra_args({"lr": 0.01, "batch_size": 16})
        # Dict iteration order in Python 3.7+ is insertion order
        assert result == '"lr=0.01" "batch_size=16"'

    def test_dotted_keys_for_nested_hydra_configs(self):
        result = params_to_hydra_args({"model.hidden_size": 128})
        assert result == '"model.hidden_size=128"'

    @pytest.mark.parametrize(
        "value,expected_suffix",
        [
            (0, "=0"),
            (-1, "=-1"),
            (1e10, "=10000000000.0"),
            ("", "="),
        ],
    )
    def test_edge_value_types(self, value, expected_suffix):
        result = params_to_hydra_args({"x": value})
        assert result == f'"x{expected_suffix}"'


class TestPythonNeedsCondaInit:
    """Heuristic the native Slurm source uses to decide whether to source the
    shared `_conda_init.sh.j2` partial in the rendered sbatch script."""

    def test_qualified_python_path_returns_false(self):
        # Plain interpreter path — env activation isn't needed; the binary
        # already knows its env.
        assert _python_needs_conda_init("/home/u/miniconda3/bin/python") is False
        assert _python_needs_conda_init("python") is False
        assert _python_needs_conda_init("/usr/bin/python3") is False

    def test_conda_run_returns_true(self):
        # `conda run` REQUIRES `conda` to be on PATH; non-interactive sbatch
        # shells lose .bashrc, so the init block must run.
        assert _python_needs_conda_init("conda run -n env python") is True

    def test_mamba_run_returns_true(self):
        assert _python_needs_conda_init("mamba run -n env python") is True

    def test_micromamba_run_returns_true(self):
        assert _python_needs_conda_init("micromamba run -n env python") is True

    def test_case_insensitive(self):
        assert _python_needs_conda_init("CONDA run -n env python") is True

    def test_whitespace_tolerated(self):
        assert _python_needs_conda_init("  conda run -n env python  ") is True


class TestCondaInitPartialRenders:
    """The shared `_conda_init.sh.j2` partial should:

    1. Emit the probe block when `uses_conda=True` (in slurm + ssh templates).
    2. Emit NOTHING substantive when `uses_conda=False`.
    3. Include both conda paths AND micromamba paths in the probe.
    """

    _BASE_KWARGS = {
        "job_name": "j",
        "sweep_id": "s",
        "logs_dir": "/tmp/logs",
        "tasks_dir": "/tmp/tasks",
        "task_dir": "/tmp/task",
        "num_jobs": 1,
        "params_file": "/tmp/params.json",
        "sbatch_directives": "#SBATCH --time=01:00:00",
        "modules": [],
        "pre_script": [],
        "project_dir": "/tmp/project",
        "python_path": "conda run -n env python",
        "script_path": "train.py",
        "params_hydra": '"seed=1"',
        "wandb_group": "g",
    }

    def test_slurm_array_emits_init_block_when_uses_conda(self):
        rendered = render_template(
            "slurm_array.sh.j2", uses_conda=True, **self._BASE_KWARGS
        )
        # Conda paths
        assert "miniconda3/etc/profile.d/conda.sh" in rendered
        assert "miniforge3/etc/profile.d/conda.sh" in rendered
        # Micromamba paths
        assert "MAMBA_EXE" in rendered
        assert "micromamba" in rendered
        # The bridge function that lets `conda run` route to micromamba
        assert "conda() { micromamba" in rendered

    def test_slurm_array_skips_init_block_when_not_uses_conda(self):
        rendered = render_template(
            "slurm_array.sh.j2", uses_conda=False, **self._BASE_KWARGS
        )
        assert "MAMBA_EXE" not in rendered
        assert "miniconda3/etc/profile.d/conda.sh" not in rendered

    def test_slurm_single_emits_init_block_when_uses_conda(self):
        rendered = render_template(
            "slurm_single.sh.j2", uses_conda=True, **self._BASE_KWARGS
        )
        assert "MAMBA_EXE" in rendered
        assert "conda() { micromamba" in rendered

    def test_ssh_compute_source_emits_init_block_when_uses_conda(self):
        rendered = render_template(
            "ssh_compute_source.sh.j2",
            uses_conda=True,
            job_name="j",
            job_id="abc123",
            cuda_visible_devices=None,
            modules=[],
            pre_script=[],
            remote_code_dir="/remote/code",
            remote_task_dir="/remote/tasks/j",
            run_prefix="conda run -n env python",
            script_path="train.py",
            params_hydra='"seed=1"',
            wandb_group="g",
        )
        assert "MAMBA_EXE" in rendered
        assert "miniconda3/etc/profile.d/conda.sh" in rendered

    def test_micromamba_probe_includes_hsm_clone_path(self):
        # The user's S3IT layout has micromamba INSIDE the HSM clone's bin/,
        # not in any standard location. The probe must include this path.
        rendered = render_template(
            "slurm_array.sh.j2", uses_conda=True, **self._BASE_KWARGS
        )
        assert "HPC-Sweep-Manager/bin/micromamba" in rendered
