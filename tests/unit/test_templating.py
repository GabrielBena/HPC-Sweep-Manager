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

    def test_array_throttle_renders_as_percent_n(self):
        base = {**self._BASE_KWARGS, "num_jobs": 600}
        assert "#SBATCH --array=1-600%50\n" in render_template(
            "slurm_array.sh.j2", uses_conda=False, array_throttle=50, **base
        )
        assert "#SBATCH --array=1-600\n" in render_template(
            "slurm_array.sh.j2", uses_conda=False, **base
        )

    @pytest.mark.parametrize("template", ["slurm_array.sh.j2", "slurm_single.sh.j2"])
    def test_hsm_code_dir_is_exported_before_pre_script(self, template):
        kwargs = {**self._BASE_KWARGS, "pre_script": ["echo $HSM_CODE_DIR"]}
        rendered = render_template(template, uses_conda=False, **kwargs)
        export = rendered.index("export HSM_CODE_DIR=/tmp/project")
        assert export < rendered.index("echo $HSM_CODE_DIR")

    def test_slurm_array_emits_init_block_when_uses_conda(self):
        rendered = render_template("slurm_array.sh.j2", uses_conda=True, **self._BASE_KWARGS)
        # Conda paths
        assert "$HOME/miniconda3" in rendered and "/etc/profile.d/conda.sh" in rendered
        assert "$HOME/miniforge3" in rendered
        # Micromamba paths
        assert "MAMBA_EXE" in rendered
        assert "micromamba" in rendered
        # The bridge function that lets `conda run` route to micromamba
        assert "conda() { micromamba" in rendered

    def test_slurm_array_skips_init_block_when_not_uses_conda(self):
        rendered = render_template("slurm_array.sh.j2", uses_conda=False, **self._BASE_KWARGS)
        assert "MAMBA_EXE" not in rendered
        assert "etc/profile.d/conda.sh" not in rendered

    def test_slurm_single_emits_init_block_when_uses_conda(self):
        rendered = render_template("slurm_single.sh.j2", uses_conda=True, **self._BASE_KWARGS)
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
        assert "$HOME/miniconda3" in rendered and "/etc/profile.d/conda.sh" in rendered

    def test_micromamba_probe_includes_hsm_clone_path(self):
        # The user's S3IT layout has micromamba INSIDE the HSM clone's bin/,
        # not in any standard location. The probe must include this path.
        rendered = render_template("slurm_array.sh.j2", uses_conda=True, **self._BASE_KWARGS)
        assert "HPC-Sweep-Manager/bin/micromamba" in rendered

    def test_local_template_emits_init_block_when_uses_conda(self):
        # LocalComputeSource template must also support the partial so
        # `paths.conda_env` is honored end-to-end for --mode local.
        rendered = render_template(
            "local_compute_source.sh.j2",
            uses_conda=True,
            job_name="j",
            job_id="abc",
            task_dir="/tmp/task",
            project_dir="/tmp/project",
            python_path="conda run -n env python",
            script_path="train.py",
            params_hydra='"seed=1"',
            wandb_group="g",
            cuda_visible_devices=None,
            modules=[],
            pre_script=[],
        )
        assert "MAMBA_EXE" in rendered
        assert "$HOME/miniconda3" in rendered and "/etc/profile.d/conda.sh" in rendered
        assert "conda() { micromamba" in rendered

    def test_local_template_skips_init_block_when_not_uses_conda(self):
        rendered = render_template(
            "local_compute_source.sh.j2",
            uses_conda=False,
            job_name="j",
            job_id="abc",
            task_dir="/tmp/task",
            project_dir="/tmp/project",
            python_path="/abs/python",
            script_path="train.py",
            params_hydra='"seed=1"',
            wandb_group="g",
            cuda_visible_devices=None,
            modules=[],
            pre_script=[],
        )
        assert "MAMBA_EXE" not in rendered
        assert "etc/profile.d/conda.sh" not in rendered


class TestComputeSourceCondaEnvWrap:
    """End-to-end at the compute-source construction boundary: setting
    `conda_env=` flips `python_path` to `conda run -n <env> python` AND
    makes the rendered script include the init block.

    Doesn't actually submit anything — checks the rendered text only.
    """

    def test_local_conda_env_wraps_python_path(self):
        from hpc_sweep_manager.core.local.local_compute_source import LocalComputeSource

        src = LocalComputeSource(
            python_path="/abs/python",
            conda_env="my-project",
        )
        # conda_env wins over explicit python_path — env is the intent.
        assert src.python_path == "conda run -n my-project python"

    def test_local_no_conda_env_keeps_explicit_python(self):
        from hpc_sweep_manager.core.local.local_compute_source import LocalComputeSource

        src = LocalComputeSource(python_path="/abs/python")
        assert src.python_path == "/abs/python"

    def test_slurm_conda_env_wraps_python_path(self):
        from hpc_sweep_manager.core.hpc.slurm_compute_source import SlurmComputeSource

        src = SlurmComputeSource(
            python_path="/abs/python",
            conda_env="my-project",
        )
        assert src.python_path == "conda run -n my-project python"

    def test_slurm_no_conda_env_keeps_explicit_python(self):
        from hpc_sweep_manager.core.hpc.slurm_compute_source import SlurmComputeSource

        src = SlurmComputeSource(python_path="/abs/python")
        assert src.python_path == "/abs/python"


class TestGpuPinning:
    """HSM's slot indices are nvidia-smi's (PCI order): every script that pins
    ``CUDA_VISIBLE_DEVICES`` says so, or CUDA would read them fastest-first."""

    _KWARGS = dict(
        job_name="j",
        job_id="abc",
        params_hydra='"seed=1"',
        wandb_group="g",
        modules=[],
        pre_script=[],
        uses_conda=False,
        # local
        task_dir="/tmp/task",
        project_dir="/tmp/project",
        python_path="python",
        script_path="train.py",
        # ssh
        remote_code_dir="/remote/code",
        remote_task_dir="/remote/tasks/j",
        run_prefix="python",
    )

    @pytest.mark.parametrize("template", ["local_compute_source.sh.j2", "ssh_compute_source.sh.j2"])
    @pytest.mark.parametrize("cvd", ["1,2", ""])  # a GPU slot; a CPU slot (no GPU visible)
    def test_pinned_slots_are_in_nvidia_smi_order(self, template, cvd):
        rendered = render_template(template, cuda_visible_devices=cvd, **self._KWARGS)
        assert (
            f"export CUDA_DEVICE_ORDER=PCI_BUS_ID\nexport CUDA_VISIBLE_DEVICES={cvd}\n" in rendered
        )

    @pytest.mark.parametrize("template", ["local_compute_source.sh.j2", "ssh_compute_source.sh.j2"])
    def test_an_unpinned_slot_leaves_the_environment_alone(self, template):
        rendered = render_template(template, cuda_visible_devices=None, **self._KWARGS)
        assert "export CUDA_VISIBLE_DEVICES" not in rendered
        assert "CUDA_DEVICE_ORDER" not in rendered


@pytest.mark.parametrize(
    ("installs", "conda_exe", "conda_env", "picked"),
    [
        # FR#15a: a leftover ~/miniconda3 shadowed the ~/miniforge3 that has the env.
        ({"miniconda3": [], "miniforge3": ["lab"]}, None, "lab", "miniforge3"),
        ({"miniconda3": [], "mambaforge": ["lab"]}, None, "lab", "mambaforge"),
        ({"miniconda3": [], "miniforge3": []}, None, "lab", "miniconda3"),  # none has it: first
        ({"miniconda3": ["lab"]}, None, None, "miniconda3"),  # no env name: first, as before
        ({"miniconda3": [], "opt/c": ["lab"]}, "opt/c/bin/conda", "lab", "opt/c"),  # $CONDA_EXE
        ({"miniconda3": [], "micromamba": ["lab"]}, None, "lab", None),  # micromamba's own env
    ],
)
def test_conda_init_sources_the_install_that_has_the_env(
    tmp_path, installs, conda_exe, conda_env, picked
):
    """The rendered partial, run by a real bash with no conda on PATH."""
    import shutil
    import subprocess

    for name, envs in installs.items():
        (tmp_path / name / "etc" / "profile.d").mkdir(parents=True)
        (tmp_path / name / "etc" / "profile.d" / "conda.sh").write_text(f"echo sourced {name}\n")
        for env in envs:
            (tmp_path / name / "envs" / env / "conda-meta").mkdir(parents=True)
    (tmp_path / "miniconda3" / "envs" / "lab").mkdir(parents=True, exist_ok=True)  # leftover
    script = render_template("_conda_init.sh.j2", uses_conda=True, conda_env=conda_env)
    env = {"HOME": str(tmp_path), "PATH": str(tmp_path / "no-bin")}  # no conda on PATH (CI has one)
    if conda_exe:
        env["CONDA_EXE"] = str(tmp_path / conda_exe)
    bash = shutil.which("bash")
    out = subprocess.run([bash, "-c", script], env=env, capture_output=True, text=True)
    assert out.stdout.split() == (["sourced", picked] if picked else []), out.stderr


def test_every_render_with_the_conda_init_names_the_env():
    """A render site that can emit the conda init also passes ``conda_env``: without it the probe
    falls back to the first install found, the bug R11 fixed."""
    import ast
    from pathlib import Path

    import hpc_sweep_manager

    sites = []
    for path in Path(hpc_sweep_manager.__file__).parent.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            keys = {kw.arg for kw in getattr(node, "keywords", ())}
            if isinstance(node, ast.Call) and "uses_conda" in keys:
                sites.append((path.name, node.lineno, "conda_env" in keys))
    assert len(sites) >= 6
    assert all(named for *_, named in sites), [s for s in sites if not s[2]]


class TestModuleInit:
    """Issue #15: `module` is undefined in a job submitted from a non-login shell, so every
    template that loads modules first sources a module-system init script."""

    GUARD = "if ! command -v module >/dev/null 2>&1; then"
    TEMPLATES = [
        "slurm_array.sh.j2",
        "slurm_single.sh.j2",
        "ssh_compute_source.sh.j2",
        "local_compute_source.sh.j2",
    ]

    def _render(self, template, modules, pre_script):
        kw = {**TestCondaInitPartialRenders._BASE_KWARGS, "modules": modules}
        kw |= {"pre_script": pre_script, "job_id": "1", "cuda_visible_devices": None}
        return render_template(template, uses_conda=False, **kw)

    @pytest.mark.parametrize("template", TEMPLATES)
    def test_init_precedes_modules_and_only_when_used(self, template):
        r = self._render(template, ["cuda"], [])
        assert r.index(self.GUARD) < r.index("module load cuda")
        r = self._render(template, [], ["module load miniforge3/25.3.0-3"])
        assert r.index(self.GUARD) < r.index("module load miniforge3")
        assert self.GUARD not in self._render(template, [], ["export X=1"])

    def test_sources_an_init_script_that_defines_module(self, tmp_path):
        import subprocess

        # A fake Lmod whose init also runs a failing command: under `set -e` it must not
        # end the task (profile scripts are not written for it).
        (tmp_path / "init").mkdir()
        (tmp_path / "init" / "bash").write_text('false\nmodule() { echo "loaded $*"; }\n')
        init = render_template("_module_init.sh.j2", modules=["cuda"], pre_script=[])
        script = tmp_path / "job.sh"
        script.write_text(f"set -e\n{init}module load cuda\n")
        env = {"PATH": "/usr/bin:/bin", "LMOD_PKG": str(tmp_path)}  # no exported `module`
        r = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True)
        assert (r.returncode, r.stdout) == (0, "loaded load cuda\n"), r.stderr
