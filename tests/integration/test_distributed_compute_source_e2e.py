"""End-to-end test for DistributedComputeSource through a full blocking run.

Uses auto-completing mock child sources (no real subprocesses / SSH). Costs a
few seconds because the manager's _wait_for_completion polls on a 5s cadence.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pytest

from hpc_sweep_manager.core.common.compute_source import ComputeSource, JobInfo
from hpc_sweep_manager.core.distributed.distributed_compute_source import (
    DistributedComputeSource,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class AutoCompleteChild(ComputeSource):
    """Child whose jobs land directly in completed_jobs as COMPLETED."""

    def __init__(self, name: str, max_parallel_jobs: int = 2):
        super().__init__(name, "mock", max_parallel_jobs)
        self._counter = 0

    async def setup(self, sweep_dir: Path, sweep_id: str) -> bool:
        return True

    async def submit_job(self, params, job_name, sweep_id, wandb_group=None, spec=None) -> str:
        self._counter += 1
        job_id = f"{self.name}_{self._counter}"
        self.completed_jobs[job_id] = JobInfo(
            job_id=job_id,
            job_name=job_name,
            params=params,
            source_name=self.name,
            status="COMPLETED",
            submit_time=datetime.now(),
            complete_time=datetime.now(),
        )
        return job_id

    async def get_job_status(self, job_id: str) -> str:
        return "COMPLETED"

    async def cancel_job(self, job_id: str) -> bool:
        return True

    async def collect_results(self, job_ids=None) -> bool:
        return True

    async def cleanup(self) -> None:
        return None

    async def health_check(self) -> Dict[str, Any]:
        return {"status": "healthy", "timestamp": datetime.now().isoformat()}


async def test_full_distributed_run_completes_all_jobs(tmp_path):
    a = AutoCompleteChild("host_a", max_parallel_jobs=2)
    b = AutoCompleteChild("host_b", max_parallel_jobs=2)
    src = DistributedComputeSource(child_sources=[a, b], show_progress=False)

    sweep_dir = tmp_path / "sweep"
    assert await src.setup(sweep_dir, "dist_e2e")

    params_list = [{"seed": i} for i in range(4)]
    job_ids = await src.submit_batch(params_list, "dist_e2e")
    assert len(job_ids) == 4

    final = await src.wait_for_all(poll_interval=0.05)
    assert len(final) == 4
    assert all(status == "COMPLETED" for status in final.values())

    # Jobs were spread across both hosts (round-robin default).
    used_sources = {info.source_name for info in src.completed_jobs.values()}
    assert used_sources == {"host_a", "host_b"}

    await src.cleanup()
