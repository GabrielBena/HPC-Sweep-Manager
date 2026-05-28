"""Unit tests for the rewritten, GPU-aware LocalComputeSource."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
from hpc_sweep_manager.core.local.local_compute_source import (
    LocalComputeSource,
    _detect_gpus,
)


class TestGPUDetection:
    pytestmark = pytest.mark.asyncio

    async def test_no_nvidia_smi(self, no_gpus):
        assert await _detect_gpus() == []

    async def test_reports_configured_count(self, fake_gpus):
        fake_gpus.set_count(2)
        assert await _detect_gpus() == [0, 1]

    async def test_reports_eight(self, fake_gpus):
        fake_gpus.set_count(8)
        assert await _detect_gpus() == [0, 1, 2, 3, 4, 5, 6, 7]

    async def test_disable(self, fake_gpus):
        fake_gpus.disable()
        assert await _detect_gpus() == []


class TestSlotAllocation:
    pytestmark = pytest.mark.asyncio

    async def test_no_gpus_no_spec_uses_cpu_slots(self, tmp_path, no_gpus):
        src = LocalComputeSource(max_parallel_jobs=3)
        assert await src.setup(tmp_path / "sweep", "test_sweep")
        assert src._slot_count == 3
        # All slots should be None (CPU).
        slots = []
        for _ in range(3):
            slots.append(src._slot_queue.get_nowait())
        assert slots == [None, None, None]

    async def test_gpus_detected_no_request_uses_cpu_slots(self, tmp_path, fake_gpus):
        fake_gpus.set_count(4)
        src = LocalComputeSource(max_parallel_jobs=2)
        assert await src.setup(tmp_path / "sweep", "test_sweep")
        # default_spec has gpus=None so no GPU partitioning even with GPUs detected.
        assert src._slot_count == 2
        assert src._gpu_indices == [0, 1, 2, 3]
        slots = [src._slot_queue.get_nowait() for _ in range(2)]
        assert slots == [None, None]

    async def test_one_gpu_per_job_with_four_gpus(self, tmp_path, fake_gpus):
        fake_gpus.set_count(4)
        src = LocalComputeSource(
            max_parallel_jobs=10,
            default_spec=ResourceSpec(gpus=1),
        )
        assert await src.setup(tmp_path / "sweep", "test_sweep")
        # 4 GPUs / 1 per job = 4 slots
        assert src._slot_count == 4
        slots = [src._slot_queue.get_nowait() for _ in range(4)]
        assert slots == [[0], [1], [2], [3]]

    async def test_two_gpus_per_job_with_four_gpus(self, tmp_path, fake_gpus):
        fake_gpus.set_count(4)
        src = LocalComputeSource(
            max_parallel_jobs=10,
            default_spec=ResourceSpec(gpus=2),
        )
        assert await src.setup(tmp_path / "sweep", "test_sweep")
        assert src._slot_count == 2
        slots = [src._slot_queue.get_nowait() for _ in range(2)]
        assert slots == [[0, 1], [2, 3]]

    async def test_gpus_per_job_doesnt_divide_evenly_drops_remainder(self, tmp_path, fake_gpus):
        fake_gpus.set_count(3)
        src = LocalComputeSource(
            max_parallel_jobs=10,
            default_spec=ResourceSpec(gpus=2),
        )
        assert await src.setup(tmp_path / "sweep", "test_sweep")
        # 3 GPUs / 2 per job = 1 slot (GPU 2 is unused)
        assert src._slot_count == 1
        assert src._slot_queue.get_nowait() == [0, 1]

    async def test_more_gpus_per_job_than_available_falls_back_to_cpu(self, tmp_path, fake_gpus):
        fake_gpus.set_count(2)
        src = LocalComputeSource(
            max_parallel_jobs=4,
            default_spec=ResourceSpec(gpus=4),  # asked for 4, have 2
        )
        assert await src.setup(tmp_path / "sweep", "test_sweep")
        # Falls back to CPU mode with max_parallel_jobs slots
        assert src._slot_count == 4
        slots = [src._slot_queue.get_nowait() for _ in range(4)]
        assert slots == [None, None, None, None]


class TestVisibleGpusAllowlist:
    """``visible_gpus`` filters detected GPUs before slot partitioning.

    Use case: shared GPU boxes where (e.g.) ``GPU:0`` is reserved for
    interactive work. The slot queue must never schedule on excluded
    indices, regardless of `gpus_per_job`.
    """

    pytestmark = pytest.mark.asyncio

    async def test_explicit_allowlist_excludes_gpu_zero(self, tmp_path, fake_gpus):
        fake_gpus.set_count(4)
        src = LocalComputeSource(
            max_parallel_jobs=10,
            default_spec=ResourceSpec(gpus=1),
            visible_gpus=[1, 2, 3],  # GPU:0 reserved
        )
        assert await src.setup(tmp_path / "sweep", "test_sweep")
        # 3 visible / 1 per job = 3 slots — none contain GPU:0.
        assert src._slot_count == 3
        assert src._gpu_indices == [1, 2, 3]
        slots = [src._slot_queue.get_nowait() for _ in range(3)]
        assert slots == [[1], [2], [3]]
        assert all(0 not in slot for slot in slots)

    async def test_int_n_takes_first_n_of_detected(self, tmp_path, fake_gpus):
        fake_gpus.set_count(4)
        src = LocalComputeSource(
            max_parallel_jobs=10,
            default_spec=ResourceSpec(gpus=1),
            visible_gpus=2,  # first 2 detected
        )
        assert await src.setup(tmp_path / "sweep", "test_sweep")
        assert src._gpu_indices == [0, 1]
        slots = [src._slot_queue.get_nowait() for _ in range(2)]
        assert slots == [[0], [1]]

    async def test_zero_means_cpu_only(self, tmp_path, fake_gpus):
        fake_gpus.set_count(4)
        src = LocalComputeSource(
            max_parallel_jobs=3,
            default_spec=ResourceSpec(gpus=1),
            visible_gpus=0,  # CPU-only
        )
        assert await src.setup(tmp_path / "sweep", "test_sweep")
        assert src._gpu_indices == []
        # No GPUs visible AND gpus_per_job=1 — falls back to CPU slots.
        slots = [src._slot_queue.get_nowait() for _ in range(3)]
        assert slots == [None, None, None]

    async def test_none_means_use_all_detected(self, tmp_path, fake_gpus):
        # Sanity: visible_gpus=None preserves the pre-feature behavior.
        fake_gpus.set_count(4)
        src = LocalComputeSource(
            max_parallel_jobs=10,
            default_spec=ResourceSpec(gpus=1),
            visible_gpus=None,
        )
        assert await src.setup(tmp_path / "sweep", "test_sweep")
        assert src._gpu_indices == [0, 1, 2, 3]

    async def test_stale_indices_warned_and_dropped(self, tmp_path, fake_gpus, caplog):
        # User says [1, 2, 7] but the box has GPUs 0..3 — index 7 is stale.
        fake_gpus.set_count(4)
        src = LocalComputeSource(
            max_parallel_jobs=10,
            default_spec=ResourceSpec(gpus=1),
            visible_gpus=[1, 2, 7],
        )
        with caplog.at_level("WARNING"):
            assert await src.setup(tmp_path / "sweep", "test_sweep")
        assert src._gpu_indices == [1, 2]  # 7 dropped, no crash
        assert any("not present in nvidia-smi" in r.message for r in caplog.records)


class TestSubmitJobBackPressure:
    pytestmark = pytest.mark.asyncio

    async def test_submit_without_setup_raises(self, tmp_path):
        src = LocalComputeSource()
        with pytest.raises(RuntimeError, match="not set up"):
            await src.submit_job({"x": 1}, "task_001", "sweep_test")

    async def test_slot_count_caps_concurrent_submissions(self, tmp_path, no_gpus):
        # Use a sleeping trivial script so we can observe the slot back-pressure.
        train = tmp_path / "train.py"
        train.write_text(
            "#!/usr/bin/env python3\n"
            "import sys, time\n"
            "time.sleep(1.0)\n"
        )
        train.chmod(0o755)

        src = LocalComputeSource(
            max_parallel_jobs=2,
            python_path="python3",
            script_path=str(train),
            project_dir=str(tmp_path),
        )
        sweep_dir = tmp_path / "sweep"
        assert await src.setup(sweep_dir, "test_sweep")

        # Submit 2 jobs immediately — these should not block.
        async def submit(i):
            return await src.submit_job({"i": i}, f"task_{i:03d}", "test_sweep")

        ids = await asyncio.gather(submit(1), submit(2))
        assert len(ids) == 2
        # A third submission should block (slot queue is empty until one finishes).
        third = asyncio.create_task(submit(3))
        # Allow a beat; the third should still be waiting.
        await asyncio.sleep(0.1)
        assert not third.done(), "third submission should be waiting on a slot"
        # When the first job finishes (~1s later) the third can proceed.
        await third
        await src.cleanup()


class TestConstruction:
    def test_default_source_type(self):
        src = LocalComputeSource()
        assert src.source_type == "local"

    def test_minimum_parallel_jobs_is_one(self):
        # The base class respects available_slots which uses max_parallel_jobs.
        src = LocalComputeSource(max_parallel_jobs=0)
        assert src.max_parallel_jobs == 1
