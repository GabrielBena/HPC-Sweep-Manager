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


class TestNativeJobStatus:
    """get_job_status: squeue-absence triggers a sacct terminal-state query
    instead of optimistically assuming COMPLETED."""

    @staticmethod
    def _patch_run(monkeypatch, *, squeue_out: str, sacct_out: str):
        from hpc_sweep_manager.core.hpc import slurm_compute_source as mod

        def fake_run(argv, capture_output=True, text=True):
            r = type("R", (), {})()
            r.returncode = 0
            r.stderr = ""
            if "squeue" in argv:
                r.stdout = squeue_out
            elif "sacct" in argv:
                r.stdout = sacct_out
            else:
                r.stdout = ""
            return r

        monkeypatch.setattr(mod.subprocess, "run", fake_run)

    @pytest.mark.asyncio
    async def test_absent_failed_via_sacct(self, monkeypatch):
        self._patch_run(monkeypatch, squeue_out="", sacct_out="FAILED\n")
        assert await SlurmComputeSource().get_job_status("123") == "FAILED"

    @pytest.mark.asyncio
    async def test_absent_completed_via_sacct(self, monkeypatch):
        self._patch_run(monkeypatch, squeue_out="", sacct_out="COMPLETED\n")
        assert await SlurmComputeSource().get_job_status("123") == "COMPLETED"

    @pytest.mark.asyncio
    async def test_absent_empty_sacct_falls_back_completed(self, monkeypatch):
        self._patch_run(monkeypatch, squeue_out="", sacct_out="")
        assert await SlurmComputeSource().get_job_status("123") == "COMPLETED"

    @pytest.mark.asyncio
    async def test_running_in_squeue_unchanged(self, monkeypatch):
        self._patch_run(monkeypatch, squeue_out="RUNNING\n", sacct_out="FAILED\n")
        # Still in the queue → trust squeue, never consult sacct.
        assert await SlurmComputeSource().get_job_status("123") == "RUNNING"


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


class TestCondaInitOrdering:
    """The conda-init partial must render AFTER modules + pre_script, and must
    guard the micromamba bridge behind `command -v conda` so a module-provided
    conda (loaded by pre_script) is never shadowed → no silent CPU fallback."""

    GUARD = "command -v conda >/dev/null 2>&1"
    BRIDGE = "conda() { micromamba"

    def _render(self, name, **extra):
        from hpc_sweep_manager.core.common.templating import render_template

        ctx = dict(
            job_name="j",
            job_id="1",
            sweep_id="sw",
            logs_dir="/tmp/logs",
            tasks_dir="/tmp/tasks",
            task_dir="/tmp/tasks/j",
            remote_task_dir="/tmp/tasks/j",
            remote_code_dir="/remote/code",
            num_jobs=1,
            params_file="/tmp/p.json",
            sbatch_directives="",
            project_dir="/proj",
            python_path="python",
            run_prefix="conda run -n env python",
            script_path="train.py",
            params_hydra='"lr=1"',
            wandb_group=None,
            cuda_visible_devices=None,
        )
        ctx.update(extra)
        return render_template(name, **ctx)

    def test_single_pre_script_before_conda_init(self):
        r = self._render(
            "slurm_single.sh.j2",
            modules=["gcc"],
            pre_script=["module load miniforge3"],
            uses_conda=True,
        )
        assert self.GUARD in r  # guard present
        assert r.index("module load miniforge3") < r.index(self.GUARD)
        assert r.index("module load gcc") < r.index(self.GUARD)
        # The micromamba bridge is gated behind the guard.
        assert r.index(self.GUARD) < r.index(self.BRIDGE)

    def test_array_pre_script_before_conda_init(self):
        r = self._render(
            "slurm_array.sh.j2",
            modules=["miniforge3"],
            pre_script=["export FOO=bar"],
            uses_conda=True,
        )
        assert self.GUARD in r
        assert r.index("module load miniforge3") < r.index(self.GUARD)
        assert r.index("export FOO=bar") < r.index(self.GUARD)

    def test_ssh_pre_script_before_conda_init(self):
        r = self._render(
            "ssh_compute_source.sh.j2",
            modules=["miniforge3"],
            pre_script=["module load cuda"],
            uses_conda=True,
        )
        assert self.GUARD in r
        assert r.index("module load miniforge3") < r.index(self.GUARD)
        assert r.index("module load cuda") < r.index(self.GUARD)

    def test_no_conda_block_when_uses_conda_false(self):
        r = self._render(
            "slurm_single.sh.j2",
            modules=[],
            pre_script=[],
            uses_conda=False,
        )
        assert self.GUARD not in r
        assert "HSM conda" not in r


class TestSelfDescribingParams:
    """Each task dir gets a params.yaml with the exact overrides (Tier 2), so a
    synced checkpoint is never orphaned from its parameters."""

    def test_params_to_yaml_roundtrips(self):
        import yaml

        from hpc_sweep_manager.core.common.templating import params_to_yaml

        params = {"lr": 0.1, "seed": 42, "layers": [2, 4], "name": "a b"}
        loaded = yaml.safe_load(params_to_yaml(params))
        assert loaded == params

    @staticmethod
    def _heredoc_body(rendered, path_marker="params.yaml"):
        """Extract the body of the params.yaml heredoc."""
        lines = rendered.splitlines()
        start = next(
            i
            for i, ln in enumerate(lines)
            if path_marker in ln and "HSM_PARAMS_EOF" in ln
        )
        end = next(
            i for i in range(start + 1, len(lines)) if lines[i] == "HSM_PARAMS_EOF"
        )
        return "\n".join(lines[start + 1 : end])

    @pytest.mark.parametrize(
        "template,dir_kw",
        [
            ("slurm_single.sh.j2", "task_dir"),
            ("ssh_compute_source.sh.j2", "remote_task_dir"),
            ("local_compute_source.sh.j2", "task_dir"),
        ],
    )
    def test_single_job_templates_write_params_yaml(self, template, dir_kw):
        import yaml

        from hpc_sweep_manager.core.common.templating import (
            params_to_yaml,
            render_template,
        )

        params = {"lr": 0.1, "seed": 42}
        ctx = dict(
            job_name="j",
            job_id="1",
            sweep_id="sw",
            logs_dir="/l",
            sbatch_directives="",
            modules=[],
            pre_script=[],
            project_dir="/p",
            remote_code_dir="/p",
            python_path="python",
            run_prefix="conda run -n e python",
            script_path="train.py",
            params_hydra='"lr=0.1" "seed=42"',
            params_yaml=params_to_yaml(params),
            wandb_group=None,
            cuda_visible_devices=None,
            uses_conda=False,
            **{dir_kw: "/t"},
        )
        r = render_template(template, **ctx)
        assert "cat > /t/params.yaml" in r
        assert yaml.safe_load(self._heredoc_body(r)) == params

    def test_array_template_python_writes_params_yaml(self):
        from hpc_sweep_manager.core.common.templating import render_template

        r = render_template(
            "slurm_array.sh.j2",
            job_name="a",
            sweep_id="sw",
            num_jobs=2,
            logs_dir="/l",
            tasks_dir="/t",
            params_file="/p.json",
            sbatch_directives="",
            modules=[],
            pre_script=[],
            project_dir="/p",
            python_path="python",
            script_path="train.py",
            wandb_group=None,
            uses_conda=False,
        )
        # The inline python creates the task dir + dumps params.yaml (JSON is
        # valid YAML, so no PyYAML needed on the compute node).
        assert "os.makedirs(_task_dir" in r
        assert 'params.yaml' in r
        assert "json.dump(params" in r


def _render_array_script(**extra):
    from hpc_sweep_manager.core.common.templating import render_template

    ctx = dict(
        job_name="a",
        sweep_id="sw",
        num_jobs=1,
        logs_dir="/l",
        tasks_dir="/t",
        params_file="/p.json",
        sbatch_directives="",
        modules=[],
        pre_script=[],
        project_dir="/p",
        python_path="conda run -n env python",
        script_path="train.py",
        wandb_group=None,
        uses_conda=False,
    )
    ctx.update(extra)
    return render_template("slurm_array.sh.j2", **ctx)


class TestArrayParamsExtraction:
    """B3 (field report 2026-06-03): per-task params extraction must run python
    BY FILE PATH, never via stdin heredoc — `conda run -n env python -` does
    not forward heredoc stdin inside $() on some clusters, so the snippet reads
    empty stdin, prints nothing, exits 0, and the task silently trains the
    project's DEFAULT config while reporting SUCCESS."""

    def test_no_stdin_heredoc_into_interpreter(self):
        r = _render_array_script()
        # The fatal shape: `<python_path> - <<'EOF'` (program fed over stdin).
        assert " - <<'PYTHON_EOF'" not in r
        # The snippet is written to a tempfile and run by path instead.
        assert "mktemp" in r
        assert '"$_HSM_PARAMS_SCRIPT"' in r

    def test_empty_params_fail_fast_guard(self):
        # Defense-in-depth: zero overrides is never intended for a sweep task.
        r = _render_array_script()
        assert '-z "$PARAMS_JSON"' in r
        assert "refusing to run the default config" in r


class TestArrayParamsExtractionFunctional:
    """Execute the rendered array script under bash with interpreter wrappers
    that reproduce the cluster failure modes — no Slurm needed."""

    def _setup_sweep(self, tmp_path):
        import json

        proj = tmp_path / "proj"
        proj.mkdir()
        tasks_dir = tmp_path / "tasks"
        params_file = tmp_path / "parameter_combinations.json"
        # "note" carries a literal '|' — the tokens|index protocol must split
        # on the LAST pipe, not the first, or values with pipes corrupt both
        # the overrides and the parsed global index.
        params_file.write_text(
            json.dumps(
                [
                    {
                        "index": 1,
                        "global_index": 1,
                        "params": {"lr": 0.01, "seed": 7, "note": "a|b"},
                    }
                ]
            )
        )
        # Training stub: records its argv so we can assert the overrides
        # actually arrived.
        (proj / "train.py").write_text(
            "import os, sys\n"
            "open(os.environ['HSM_TEST_ARGS_OUT'], 'w').write('\\n'.join(sys.argv[1:]))\n"
        )
        return proj, tasks_dir, params_file

    def _run(self, tmp_path, script_text, args_out):
        import os
        import subprocess

        script = tmp_path / "array_job.sh"
        script.write_text(script_text)
        env = dict(os.environ)
        env["SLURM_ARRAY_TASK_ID"] = "1"
        env["HSM_TEST_ARGS_OUT"] = str(args_out)
        return subprocess.run(
            ["bash", str(script)], env=env, capture_output=True, text=True
        )

    def test_overrides_survive_a_stdin_swallowing_wrapper(self, tmp_path):
        # Wrapper that mimics `conda run` on S3IT: argv passes through, but
        # the child's stdin is severed. The legacy `python - <<heredoc` form
        # yields empty params + exit 0 under this wrapper; run-by-path is
        # immune.
        import sys

        wrapper = tmp_path / "swallowpy"
        wrapper.write_text(f'#!/bin/bash\nexec "{sys.executable}" "$@" < /dev/null\n')
        wrapper.chmod(0o755)

        proj, tasks_dir, params_file = self._setup_sweep(tmp_path)
        rendered = _render_array_script(
            tasks_dir=str(tasks_dir),
            params_file=str(params_file),
            project_dir=str(proj),
            python_path=str(wrapper),
        )
        args_out = tmp_path / "argv.txt"
        result = self._run(tmp_path, rendered, args_out)

        assert result.returncode == 0, result.stdout + result.stderr
        # eval consumes the protective quotes; the script sees bare overrides.
        argv = args_out.read_text().splitlines()
        assert "lr=0.01" in argv
        assert "seed=7" in argv
        assert "note=a|b" in argv  # pipe in a VALUE survives the index split
        # The tempfile snippet also dropped the self-describing params.yaml.
        assert (tasks_dir / "task_1" / "params.yaml").is_file()

    def test_empty_extraction_fails_fast_not_default_run(self, tmp_path):
        # Wrapper reproducing the OBSERVED legacy failure: extraction produces
        # nothing yet exits 0. The guard must kill the task, not let it run
        # the default config.
        wrapper = tmp_path / "mutepy"
        wrapper.write_text("#!/bin/bash\nexit 0\n")
        wrapper.chmod(0o755)

        proj, tasks_dir, params_file = self._setup_sweep(tmp_path)
        rendered = _render_array_script(
            tasks_dir=str(tasks_dir),
            params_file=str(params_file),
            project_dir=str(proj),
            python_path=str(wrapper),
        )
        args_out = tmp_path / "argv.txt"
        result = self._run(tmp_path, rendered, args_out)

        assert result.returncode != 0
        assert "refusing to run the default config" in result.stdout
        # The task never got as far as creating its dir or running anything.
        assert not (tasks_dir / "task_1").exists()
        assert not args_out.exists()
