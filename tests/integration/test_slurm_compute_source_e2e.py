"""End-to-end integration tests for SlurmComputeSource against the fake cluster.

The ``fake_slurm`` fixture (in tests/conftest.py) prepends a directory of
Python-stubbed ``sbatch``/``squeue``/``scancel``/``sinfo`` scripts to PATH.
This lets us exercise the real SlurmComputeSource code path — script
generation, sbatch invocation, status polling, wait_for_all loop — without
a real Slurm cluster.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
from hpc_sweep_manager.core.hpc.slurm_compute_source import SlurmComputeSource


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
def configured_source(tmp_path, fake_slurm):
    """SlurmComputeSource bound to a temp sweep directory and the fake cluster."""
    src = SlurmComputeSource(
        name="fake",
        python_path="python",
        script_path="/tmp/fake_train.py",
        project_dir=str(tmp_path),
        default_spec=ResourceSpec(walltime="01:00:00", cpus_per_task=2),
    )
    return src


async def test_setup_succeeds_against_fake_cluster(configured_source, tmp_path):
    sweep_dir = tmp_path / "sweep_test"
    sweep_dir.mkdir()
    assert await configured_source.setup(sweep_dir, "sweep_test")
    assert configured_source.stats.health_status == "healthy"


async def test_submit_single_job_records_submission(configured_source, tmp_path, fake_slurm):
    sweep_dir = tmp_path / "sweep_test"
    sweep_dir.mkdir()
    await configured_source.setup(sweep_dir, "sweep_test")

    job_id = await configured_source.submit_job({"lr": 0.001}, "task_001", "sweep_test")

    # Job ID returned should be the monotonic counter from fake sbatch.
    assert job_id.isdigit()

    jobs = fake_slurm.jobs()
    assert len(jobs) == 1
    assert jobs[0]["name"] == "task_001"
    assert jobs[0]["id"] == job_id
    assert jobs[0]["state"] == "PENDING"


async def test_submitted_script_has_expected_directives(configured_source, tmp_path):
    sweep_dir = tmp_path / "sweep_test"
    sweep_dir.mkdir()
    await configured_source.setup(sweep_dir, "sweep_test")
    await configured_source.submit_job({"lr": 0.001}, "task_001", "sweep_test")

    script_path = sweep_dir / "scripts" / "task_001.slurm"
    assert script_path.exists()
    content = script_path.read_text()

    assert "#SBATCH --job-name=task_001" in content
    assert "#SBATCH --time=01:00:00" in content
    assert "#SBATCH --cpus-per-task=2" in content
    assert '"lr=0.001"' in content


async def test_wait_for_all_observes_completion(configured_source, tmp_path, fake_slurm):
    sweep_dir = tmp_path / "sweep_test"
    sweep_dir.mkdir()
    await configured_source.setup(sweep_dir, "sweep_test")
    fake_slurm.set_pending_seconds(0.05)
    fake_slurm.set_running_seconds(0.1)

    job_id = await configured_source.submit_job({"lr": 0.01}, "task_001", "sweep_test")
    final = await configured_source.wait_for_all(poll_interval=0.05)
    assert final == {job_id: "COMPLETED"}
    assert job_id not in configured_source.active_jobs
    assert job_id in configured_source.completed_jobs


async def test_cancel_job_marks_state(configured_source, tmp_path, fake_slurm):
    sweep_dir = tmp_path / "sweep_test"
    sweep_dir.mkdir()
    await configured_source.setup(sweep_dir, "sweep_test")
    # Make sure jobs sit in PENDING long enough to cancel.
    fake_slurm.set_pending_seconds(10.0)
    fake_slurm.set_running_seconds(10.0)

    job_id = await configured_source.submit_job({"lr": 0.01}, "task_001", "sweep_test")
    assert await configured_source.cancel_job(job_id)

    jobs_after = fake_slurm.jobs()
    assert jobs_after[0]["state"] == "CANCELLED"
    assert job_id in configured_source.completed_jobs
    assert configured_source.completed_jobs[job_id].status == "CANCELLED"


async def test_submit_batch_array_mode(configured_source, tmp_path, fake_slurm):
    sweep_dir = tmp_path / "sweep_test"
    sweep_dir.mkdir()
    await configured_source.setup(sweep_dir, "sweep_test")

    params_list = [{"lr": 0.001}, {"lr": 0.01}, {"lr": 0.1}, {"lr": 1.0}]
    job_ids = await configured_source.submit_batch(
        params_list, sweep_id="sweep_test", mode="array"
    )

    assert len(job_ids) == 1
    array_id = job_ids[0]

    jobs = fake_slurm.jobs()
    assert len(jobs) == 1
    assert jobs[0]["is_array"] is True
    assert jobs[0]["array_size"] == 4

    # Parameter combinations file is written for the array script to consume.
    params_file = sweep_dir / "parameter_combinations.json"
    assert params_file.exists()
    combos = json.loads(params_file.read_text())
    assert len(combos) == 4
    assert combos[0] == {"index": 1, "global_index": 1, "params": {"lr": 0.001}}

    # Array script has --array=1-4 and uses $SLURM_ARRAY_TASK_ID.
    script_path = sweep_dir / "scripts" / "sweep_test_array.slurm"
    content = script_path.read_text()
    assert "#SBATCH --array=1-4" in content
    assert "$SLURM_ARRAY_TASK_ID" in content


async def test_individual_mode_falls_back_to_default_submit_batch(configured_source, tmp_path, fake_slurm):
    sweep_dir = tmp_path / "sweep_test"
    sweep_dir.mkdir()
    await configured_source.setup(sweep_dir, "sweep_test")

    job_ids = await configured_source.submit_batch(
        [{"a": 1}, {"a": 2}, {"a": 3}], sweep_id="sweep_test", mode="individual"
    )
    assert len(job_ids) == 3
    assert len(fake_slurm.jobs()) == 3


async def test_qos_whitelist_enforced(tmp_path, fake_slurm):
    sweep_dir = tmp_path / "sweep_test"
    sweep_dir.mkdir()

    s3it_source = SlurmComputeSource(
        name="s3it",
        python_path="python",
        script_path="/tmp/fake_train.py",
        project_dir=str(tmp_path),
        qos_whitelist=frozenset({"normal", "medium", "long"}),
    )
    await s3it_source.setup(sweep_dir, "sweep_test")

    # Bad QOS is rejected before sbatch is ever invoked.
    with pytest.raises(ValueError, match="qos='special'"):
        await s3it_source.submit_job(
            {"x": 1}, "task_001", "sweep_test",
            spec=ResourceSpec(qos="special"),
        )
    assert fake_slurm.jobs() == []

    # Good QOS works.
    await s3it_source.submit_job(
        {"x": 1}, "task_001", "sweep_test",
        spec=ResourceSpec(qos="normal"),
    )
    jobs = fake_slurm.jobs()
    assert len(jobs) == 1


async def test_modules_and_pre_script_render_into_array(tmp_path, fake_slurm):
    sweep_dir = tmp_path / "sweep_test"
    sweep_dir.mkdir()

    src = SlurmComputeSource(
        name="s3it",
        python_path="python",
        script_path="/tmp/fake_train.py",
        project_dir=str(tmp_path),
        default_spec=ResourceSpec(
            walltime="04:00:00",
            cpus_per_task=4,
            gpus=1,
            gpu_type="h100",
            qos="normal",
            modules=("h100", "cuda/12.1"),
            pre_script=("source ~/venv/bin/activate", "export FOO=bar"),
        ),
    )
    await src.setup(sweep_dir, "sweep_test")
    await src.submit_batch(
        [{"lr": 0.001}, {"lr": 0.01}], sweep_id="sweep_test", mode="array"
    )

    script_path = sweep_dir / "scripts" / "sweep_test_array.slurm"
    content = script_path.read_text()
    assert "#SBATCH --gres=gpu:h100:1" in content
    assert "#SBATCH --qos=normal" in content
    assert "module load h100" in content
    assert "module load cuda/12.1" in content
    assert "source ~/venv/bin/activate" in content
    assert "export FOO=bar" in content
