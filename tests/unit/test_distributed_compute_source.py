"""Fast unit tests for the DistributedComputeSource ABC wrapper surface.

The full blocking submit→complete lifecycle is exercised in
tests/integration/test_distributed_compute_source_e2e.py (it costs ~5s due to
the manager's poll loop). These tests cover construction, capacity
aggregation, setup delegation, status/cancel delegation, health aggregation,
and the wait_for_all short-circuit — all without a real run.
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


class MockChild(ComputeSource):
    """Minimal child source; jobs complete instantly on submit."""

    def __init__(self, name: str, max_parallel_jobs: int = 2, health: str = "healthy"):
        super().__init__(name, "mock", max_parallel_jobs)
        self._health = health
        self.setup_called = False
        self.cleanup_called = False
        self._counter = 0

    async def setup(self, sweep_dir: Path, sweep_id: str) -> bool:
        self.setup_called = True
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
        self.cleanup_called = True

    async def health_check(self) -> Dict[str, Any]:
        return {"status": self._health, "timestamp": datetime.now().isoformat()}


class TestConstruction:
    def test_source_type_is_distributed(self):
        src = DistributedComputeSource()
        assert src.source_type == "distributed"

    def test_capacity_aggregates_children(self):
        src = DistributedComputeSource(
            child_sources=[MockChild("a", 3), MockChild("b", 5)]
        )
        assert src.max_parallel_jobs == 8

    def test_no_children_defaults_to_one_slot(self):
        src = DistributedComputeSource()
        assert src.max_parallel_jobs == 1

    def test_add_source_grows_capacity(self):
        src = DistributedComputeSource(child_sources=[MockChild("a", 2)])
        src.add_source(MockChild("b", 4))
        assert src.max_parallel_jobs == 6
        assert len(src._child_sources) == 2


class TestSetup:
    pytestmark = pytest.mark.asyncio

    async def test_setup_without_children_fails(self, tmp_path):
        src = DistributedComputeSource()
        assert await src.setup(tmp_path / "sweep", "sweep_test") is False
        assert src.stats.health_status == "unhealthy"

    async def test_setup_delegates_to_children(self, tmp_path):
        a, b = MockChild("a"), MockChild("b")
        src = DistributedComputeSource(child_sources=[a, b])
        ok = await src.setup(tmp_path / "sweep", "sweep_test")
        assert ok is True
        assert a.setup_called and b.setup_called
        assert src.stats.health_status == "healthy"
        assert src.sweep_id == "sweep_test"


class TestStatusAndCancel:
    pytestmark = pytest.mark.asyncio

    async def test_submit_without_setup_raises(self):
        src = DistributedComputeSource(child_sources=[MockChild("a")])
        with pytest.raises(RuntimeError, match="not set up"):
            await src.submit_batch([{"x": 1}], "sweep_test")

    async def test_get_job_status_unknown(self):
        src = DistributedComputeSource(child_sources=[MockChild("a")])
        assert await src.get_job_status("nope") == "UNKNOWN"

    async def test_get_job_status_from_completed(self):
        src = DistributedComputeSource(child_sources=[MockChild("a")])
        src.completed_jobs["j1"] = JobInfo("j1", "n", {}, "a", status="FAILED")
        assert await src.get_job_status("j1") == "FAILED"

    async def test_cancel_unknown_job_returns_false(self, tmp_path):
        src = DistributedComputeSource(child_sources=[MockChild("a")])
        await src.setup(tmp_path / "sweep", "sweep_test")
        assert await src.cancel_job("nope") is False

    async def test_cancel_delegates_to_child(self, tmp_path):
        child = MockChild("a")
        src = DistributedComputeSource(child_sources=[child])
        await src.setup(tmp_path / "sweep", "sweep_test")
        # Wire the manager's job→source mapping by hand.
        src._manager.job_to_source["j1"] = "a"
        assert await src.cancel_job("j1") is True


class TestWaitAndHealth:
    pytestmark = pytest.mark.asyncio

    async def test_wait_for_all_returns_completed_statuses(self):
        src = DistributedComputeSource(child_sources=[MockChild("a")])
        src.completed_jobs["j1"] = JobInfo("j1", "n", {}, "a", status="COMPLETED")
        src.completed_jobs["j2"] = JobInfo("j2", "n", {}, "a", status="FAILED")
        final = await src.wait_for_all(poll_interval=0.01)
        assert final == {"j1": "COMPLETED", "j2": "FAILED"}

    async def test_wait_for_all_reports_progress(self):
        src = DistributedComputeSource(child_sources=[MockChild("a")])
        src.completed_jobs["j1"] = JobInfo("j1", "n", {}, "a", status="COMPLETED")
        seen = []
        await src.wait_for_all(on_progress=lambda d, t: seen.append((d, t)))
        assert seen == [(1, 1)]

    async def test_health_check_aggregates_children(self):
        src = DistributedComputeSource(
            child_sources=[MockChild("a", health="healthy"), MockChild("b", health="unhealthy")]
        )
        result = await src.health_check()
        assert result["status"] == "healthy"  # at least one healthy
        assert result["healthy_sources"] == 1
        assert result["total_sources"] == 2

    async def test_health_check_all_unhealthy(self):
        src = DistributedComputeSource(
            child_sources=[MockChild("a", health="unhealthy")]
        )
        result = await src.health_check()
        assert result["status"] == "unhealthy"

    async def test_cleanup_without_setup_is_noop(self):
        src = DistributedComputeSource(child_sources=[MockChild("a")])
        await src.cleanup()  # should not raise
