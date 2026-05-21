"""End-to-end integration tests for the GPU-aware LocalComputeSource.

These tests actually spawn subprocesses (real bash + python). They're fast
(<10s total) because the training scripts only sleep briefly.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import sys

import pytest

from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
from hpc_sweep_manager.core.local.local_compute_source import LocalComputeSource


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# Minimal "training" script: prints CUDA_VISIBLE_DEVICES, writes a sentinel.
TRAIN_SCRIPT_TEMPLATE = """#!/usr/bin/env python3
import json
import os
import sys

result = {{
    "cuda_visible_devices": os.environ.get("CUDA_VISIBLE_DEVICES", "<unset>"),
    "argv": sys.argv[1:],
    "pid": os.getpid(),
}}
out_dir = "<unknown>"
for arg in sys.argv[1:]:
    if arg.startswith("output.dir="):
        out_dir = arg.split("=", 1)[1].strip('"')
# Append our result to a shared sentinel file so multi-job tests can read.
with open({sentinel_path!r}, "a") as f:
    f.write(json.dumps(result) + "\\n")
"""


@pytest.fixture
def configured_source(tmp_path, no_gpus):
    """LocalComputeSource pointing at a tmp trainer; default = no GPUs."""
    sentinel = tmp_path / "completed_jobs.jsonl"
    train = tmp_path / "train.py"
    train.write_text(TRAIN_SCRIPT_TEMPLATE.format(sentinel_path=str(sentinel)))
    train.chmod(0o755)
    src = LocalComputeSource(
        name="local_test",
        max_parallel_jobs=2,
        python_path=sys.executable,
        script_path=str(train),
        project_dir=str(tmp_path),
    )
    return src, train, sentinel


async def test_single_job_runs_and_writes_task_info(configured_source, tmp_path):
    src, train, sentinel = configured_source
    sweep_dir = tmp_path / "sweep"
    assert await src.setup(sweep_dir, "test_sweep")

    job_id = await src.submit_job({"seed": 42}, "task_001", "test_sweep")
    final = await src.wait_for_all(poll_interval=0.05)

    assert final == {job_id: "COMPLETED"}
    info = (sweep_dir / "tasks" / "task_001" / "task_info.txt").read_text()
    assert "Status: SUCCESS" in info
    assert "seed=42" in info

    # The trainer wrote one line to the sentinel — verify our env arrived.
    assert sentinel.exists()
    rows = [json.loads(line) for line in sentinel.read_text().splitlines() if line.strip()]
    assert len(rows) == 1
    # No GPUs => CUDA_VISIBLE_DEVICES not exported by us; the inherited env may
    # have it set externally, but in CI it shouldn't.
    assert "seed=42" in rows[0]["argv"][0]


async def test_failing_script_marks_status_failed(tmp_path, no_gpus):
    train = tmp_path / "train_fail.py"
    train.write_text("#!/usr/bin/env python3\nimport sys\nsys.exit(7)\n")
    train.chmod(0o755)

    src = LocalComputeSource(
        max_parallel_jobs=1,
        python_path=sys.executable,
        script_path=str(train),
        project_dir=str(tmp_path),
    )
    sweep_dir = tmp_path / "sweep"
    assert await src.setup(sweep_dir, "test_sweep")
    job_id = await src.submit_job({"x": 1}, "task_001", "test_sweep")
    final = await src.wait_for_all(poll_interval=0.05)
    assert final == {job_id: "FAILED"}
    info = (sweep_dir / "tasks" / "task_001" / "task_info.txt").read_text()
    assert "Status: FAILED" in info
    assert "Exit Code: 7" in info


async def test_cancel_running_job(tmp_path, no_gpus):
    # Long-sleeping trainer so we can cancel it mid-flight.
    train = tmp_path / "train_long.py"
    train.write_text("#!/usr/bin/env python3\nimport time; time.sleep(30)\n")
    train.chmod(0o755)

    src = LocalComputeSource(
        max_parallel_jobs=1,
        python_path=sys.executable,
        script_path=str(train),
        project_dir=str(tmp_path),
    )
    sweep_dir = tmp_path / "sweep"
    assert await src.setup(sweep_dir, "test_sweep")
    job_id = await src.submit_job({"x": 1}, "task_001", "test_sweep")

    # Give it a moment to actually start the python process.
    await asyncio.sleep(0.2)
    assert await src.cancel_job(job_id)
    await src.cleanup()  # wait for monitor to finalize

    # update_job_status moves it into completed_jobs with status CANCELLED.
    assert src.completed_jobs[job_id].status == "CANCELLED"


async def test_gpu_isolation_per_job(tmp_path, fake_gpus):
    """With 4 fake GPUs and gpus_per_job=1, four jobs should each see a different
    CUDA_VISIBLE_DEVICES index."""
    fake_gpus.set_count(4)
    sentinel = tmp_path / "completed_jobs.jsonl"
    train = tmp_path / "train.py"
    train.write_text(TRAIN_SCRIPT_TEMPLATE.format(sentinel_path=str(sentinel)))
    train.chmod(0o755)

    src = LocalComputeSource(
        name="local_gpu",
        max_parallel_jobs=10,
        python_path=sys.executable,
        script_path=str(train),
        project_dir=str(tmp_path),
        default_spec=ResourceSpec(gpus=1),
    )
    sweep_dir = tmp_path / "sweep"
    assert await src.setup(sweep_dir, "test_sweep")
    assert src._slot_count == 4

    job_ids = []
    for i in range(4):
        job_ids.append(await src.submit_job({"i": i}, f"task_{i + 1:03d}", "test_sweep"))
    final = await src.wait_for_all(poll_interval=0.05)
    assert set(final.values()) == {"COMPLETED"}

    rows = [json.loads(line) for line in sentinel.read_text().splitlines() if line.strip()]
    cvds = sorted(r["cuda_visible_devices"] for r in rows)
    assert cvds == ["0", "1", "2", "3"], cvds


async def test_gpu_isolation_two_per_job(tmp_path, fake_gpus):
    fake_gpus.set_count(4)
    sentinel = tmp_path / "completed_jobs.jsonl"
    train = tmp_path / "train.py"
    train.write_text(TRAIN_SCRIPT_TEMPLATE.format(sentinel_path=str(sentinel)))
    train.chmod(0o755)

    src = LocalComputeSource(
        name="local_gpu",
        max_parallel_jobs=10,
        python_path=sys.executable,
        script_path=str(train),
        project_dir=str(tmp_path),
        default_spec=ResourceSpec(gpus=2),
    )
    sweep_dir = tmp_path / "sweep"
    assert await src.setup(sweep_dir, "test_sweep")
    assert src._slot_count == 2

    for i in range(2):
        await src.submit_job({"i": i}, f"task_{i + 1:03d}", "test_sweep")
    final = await src.wait_for_all(poll_interval=0.05)
    assert set(final.values()) == {"COMPLETED"}

    rows = [json.loads(line) for line in sentinel.read_text().splitlines() if line.strip()]
    cvds = sorted(r["cuda_visible_devices"] for r in rows)
    assert cvds == ["0,1", "2,3"], cvds


async def test_more_jobs_than_slots_serializes(tmp_path, fake_gpus):
    """8 jobs with 4 GPUs and gpus_per_job=2 => 2 slots; jobs run in 4 waves."""
    fake_gpus.set_count(4)
    sentinel = tmp_path / "completed_jobs.jsonl"
    train = tmp_path / "train.py"
    train.write_text(TRAIN_SCRIPT_TEMPLATE.format(sentinel_path=str(sentinel)))
    train.chmod(0o755)

    src = LocalComputeSource(
        max_parallel_jobs=10,
        python_path=sys.executable,
        script_path=str(train),
        project_dir=str(tmp_path),
        default_spec=ResourceSpec(gpus=2),
    )
    sweep_dir = tmp_path / "sweep"
    assert await src.setup(sweep_dir, "test_sweep")
    assert src._slot_count == 2

    for i in range(8):
        await src.submit_job({"i": i}, f"task_{i + 1:03d}", "test_sweep")
    final = await src.wait_for_all(poll_interval=0.05)
    assert len(final) == 8
    assert all(s == "COMPLETED" for s in final.values())

    rows = [json.loads(line) for line in sentinel.read_text().splitlines() if line.strip()]
    cvds = [r["cuda_visible_devices"] for r in rows]
    # Each job saw a valid 2-GPU slot.
    assert all(c in ("0,1", "2,3") for c in cvds)


async def test_pre_script_lines_execute(tmp_path, no_gpus):
    sentinel = tmp_path / "completed_jobs.jsonl"
    side = tmp_path / "side_effect.txt"
    train = tmp_path / "train.py"
    train.write_text(TRAIN_SCRIPT_TEMPLATE.format(sentinel_path=str(sentinel)))
    train.chmod(0o755)

    src = LocalComputeSource(
        max_parallel_jobs=1,
        python_path=sys.executable,
        script_path=str(train),
        project_dir=str(tmp_path),
        default_spec=ResourceSpec(pre_script=(f"echo hello > {side}",)),
    )
    sweep_dir = tmp_path / "sweep"
    assert await src.setup(sweep_dir, "test_sweep")
    job_id = await src.submit_job({"x": 1}, "task_001", "test_sweep")
    final = await src.wait_for_all(poll_interval=0.05)
    assert final == {job_id: "COMPLETED"}
    assert side.read_text().strip() == "hello"
