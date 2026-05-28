"""End-to-end tests for the sweep orchestrator against real ComputeSources.

Covers the same lifecycle (setup → submit_batch → wait_for_all) that
``examples/smoke_slurm.py`` proves on real S3IT, but here against the
fake_slurm PATH-stub fixture and the in-process LocalComputeSource.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
from hpc_sweep_manager.core.common.sweep_orchestrator import (
    build_compute_source,
    run_sweep_async,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# Trivial trainer: writes args + CVD to a sentinel file shared across jobs.
TRAIN_SCRIPT_TEMPLATE = """#!/usr/bin/env python3
import json
import os
import sys

with open({sentinel_path!r}, "a") as f:
    f.write(json.dumps({{
        "cvd": os.environ.get("CUDA_VISIBLE_DEVICES", "<unset>"),
        "argv": sys.argv[1:],
    }}) + "\\n")
"""


async def test_local_orchestrator_runs_four_jobs(tmp_path, no_gpus):
    sentinel = tmp_path / "completed.jsonl"
    train = tmp_path / "train.py"
    train.write_text(TRAIN_SCRIPT_TEMPLATE.format(sentinel_path=str(sentinel)))
    train.chmod(0o755)

    source, resolved_mode, sub_mode = build_compute_source(
        mode="local",
        python_path=sys.executable,
        script_path=str(train),
        project_dir=str(tmp_path),
        parallel_jobs=2,
    )
    assert resolved_mode == "local"
    assert sub_mode == "individual"

    params_list = [{"seed": i} for i in range(4)]
    progress: list[tuple[int, int]] = []
    result = await run_sweep_async(
        source=source,
        sweep_dir=tmp_path / "sweep",
        sweep_id="orch_test",
        params_list=params_list,
        submission_mode=sub_mode,
        poll_interval=0.05,
        on_progress=lambda done, total: progress.append((done, total)),
    )
    assert result.sweep_id == "orch_test"
    assert result.source_type == "local"
    assert len(result.job_ids) == 4
    assert all(s == "COMPLETED" for s in result.final_statuses.values())
    rows = [json.loads(line) for line in sentinel.read_text().splitlines() if line.strip()]
    assert len(rows) == 4
    # Last progress tick should report 4 completed.
    assert progress[-1][0] == 4


async def test_slurm_orchestrator_array_submission(tmp_path, fake_slurm):
    # The fake_slurm fixture transitions states in 0.2+0.3s; tighten that.
    fake_slurm.set_pending_seconds(0.05)
    fake_slurm.set_running_seconds(0.05)

    train = tmp_path / "train.py"
    train.write_text("#!/usr/bin/env python3\nprint('hi')\n")
    train.chmod(0o755)

    source, resolved_mode, sub_mode = build_compute_source(
        mode="array",
        python_path=sys.executable,
        script_path=str(train),
        project_dir=str(tmp_path),
        default_spec=ResourceSpec(walltime="00:05:00", cpus_per_task=1),
    )
    assert resolved_mode == "array"
    assert sub_mode == "array"

    params_list = [{"seed": i} for i in range(4)]
    result = await run_sweep_async(
        source=source,
        sweep_dir=tmp_path / "sweep",
        sweep_id="orch_array",
        params_list=params_list,
        submission_mode=sub_mode,
        poll_interval=0.05,
    )
    # Array mode collapses to one job id.
    assert len(result.job_ids) == 1
    assert result.submission_mode == "array"
    assert result.source_type == "slurm"
    final = list(result.final_statuses.values())
    assert final == ["COMPLETED"]
    # Script + parameter file should exist on disk.
    sweep_dir = tmp_path / "sweep"
    assert (sweep_dir / "scripts").exists()
    assert (sweep_dir / "parameter_combinations.json").exists()


async def test_slurm_orchestrator_individual_submission(tmp_path, fake_slurm):
    fake_slurm.set_pending_seconds(0.05)
    fake_slurm.set_running_seconds(0.05)

    train = tmp_path / "train.py"
    train.write_text("#!/usr/bin/env python3\nprint('hi')\n")
    train.chmod(0o755)

    source, _, sub_mode = build_compute_source(
        mode="individual",
        python_path=sys.executable,
        script_path=str(train),
        project_dir=str(tmp_path),
        default_spec=ResourceSpec(walltime="00:05:00", cpus_per_task=1),
    )

    params_list = [{"seed": i} for i in range(3)]
    result = await run_sweep_async(
        source=source,
        sweep_dir=tmp_path / "sweep",
        sweep_id="orch_indiv",
        params_list=params_list,
        submission_mode=sub_mode,
        poll_interval=0.05,
    )
    assert len(result.job_ids) == 3
    assert all(s == "COMPLETED" for s in result.final_statuses.values())


async def test_no_wait_returns_empty_statuses(tmp_path, no_gpus):
    train = tmp_path / "train.py"
    train.write_text("#!/usr/bin/env python3\nimport time; time.sleep(0.1)\n")
    train.chmod(0o755)

    source, _, sub_mode = build_compute_source(
        mode="local",
        python_path=sys.executable,
        script_path=str(train),
        project_dir=str(tmp_path),
        parallel_jobs=1,
    )

    result = await run_sweep_async(
        source=source,
        sweep_dir=tmp_path / "sweep",
        sweep_id="orch_no_wait",
        params_list=[{"x": 1}],
        submission_mode=sub_mode,
        wait=False,
    )
    assert len(result.job_ids) == 1
    assert result.final_statuses == {}
    # Even though we didn't wait, clean up so the monitor task finishes.
    await source.cleanup()


async def test_setup_failure_raises_runtimeerror(tmp_path, monkeypatch):
    """A source whose setup() returns False should surface a clear error."""
    from hpc_sweep_manager.core.common.compute_source import ComputeSource

    class FailingSource(ComputeSource):
        def __init__(self):
            super().__init__("bad", "broken", 1)

        async def setup(self, sweep_dir, sweep_id):
            return False

        async def submit_job(self, *a, **kw):  # pragma: no cover
            raise NotImplementedError

        async def get_job_status(self, job_id):  # pragma: no cover
            raise NotImplementedError

        async def cancel_job(self, job_id):  # pragma: no cover
            raise NotImplementedError

        async def collect_results(self, job_ids=None):  # pragma: no cover
            raise NotImplementedError

        async def health_check(self):  # pragma: no cover
            raise NotImplementedError

        async def cleanup(self):  # pragma: no cover
            return None

    with pytest.raises(RuntimeError, match="setup\\(\\) failed"):
        await run_sweep_async(
            source=FailingSource(),
            sweep_dir=tmp_path / "sweep",
            sweep_id="bad",
            params_list=[{"x": 1}],
            submission_mode="individual",
        )


async def test_slurm_block_in_hsm_config_drives_sbatch_directives(tmp_path, fake_slurm):
    """The typed `slurm:` block reaches fields the --resources string can't.

    Verifies the full Phase 3.4 wiring: feed a slurm: block with gpu_type +
    modules + qos_whitelist via HSMConfig, build the source through the
    orchestrator's spec_from_cli + build_compute_source, submit a job, then
    read back the rendered sbatch script and confirm the right #SBATCH
    directives + module-load lines are present.
    """
    from hpc_sweep_manager.core.common.config import HSMConfig
    from hpc_sweep_manager.core.common.sweep_orchestrator import spec_from_cli

    fake_slurm.set_pending_seconds(0.05)
    fake_slurm.set_running_seconds(0.05)

    train = tmp_path / "train.py"
    train.write_text("#!/usr/bin/env python3\nprint('hi')\n")
    train.chmod(0o755)

    cfg = HSMConfig(
        {
            "slurm": {
                "walltime": "00:30:00",
                "cpus_per_task": 4,
                "gpus": 1,
                "gpu_type": "h100",
                "modules": ["h100"],
                "qos": "normal",
                "qos_whitelist": ["normal", "medium", "long"],
            }
        }
    )

    spec = spec_from_cli(walltime=None, resources=None, scheduler="slurm", hsm_config=cfg)
    assert spec.gpu_type == "h100"
    assert spec.modules == ("h100",)
    assert spec.qos == "normal"

    source, _, sub_mode = build_compute_source(
        mode="individual",
        python_path=sys.executable,
        script_path=str(train),
        project_dir=str(tmp_path),
        default_spec=spec,
        hsm_config=cfg,
    )
    # qos_whitelist should have come from the hsm_config slurm: block.
    assert source.qos_whitelist == frozenset({"normal", "medium", "long"})

    result = await run_sweep_async(
        source=source,
        sweep_dir=tmp_path / "sweep",
        sweep_id="orch_slurm_block",
        params_list=[{"seed": 1}],
        submission_mode=sub_mode,
        spec=spec,
        poll_interval=0.05,
    )
    assert result.final_statuses == {result.job_ids[0]: "COMPLETED"}

    # Read the rendered sbatch script and verify the key directives.
    submitted = fake_slurm.jobs()
    assert len(submitted) == 1
    script_path = Path(submitted[0]["script_path"])
    rendered = script_path.read_text()

    # GPU type → --gres=gpu:h100:1 (NOT --gpus=1, which doesn't allocate on S3IT).
    assert "--gres=gpu:h100:1" in rendered, rendered
    # Modules → module load line.
    assert "module load h100" in rendered, rendered
    # Walltime + cpus + qos from the slurm: block.
    assert "--time=00:30:00" in rendered
    assert "--cpus-per-task=4" in rendered
    assert "--qos=normal" in rendered
