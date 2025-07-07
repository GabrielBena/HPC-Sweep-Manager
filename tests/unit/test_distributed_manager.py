"""Unit tests for the distributed job manager."""

from pathlib import Path
from typing import Any, Dict

import pytest

from hpc_sweep_manager.core.common.compute_source import ComputeSource, JobInfo
from hpc_sweep_manager.core.distributed.base_distributed_manager import (
    DistributedSweepConfig,
    DistributionStrategy,
)
from hpc_sweep_manager.core.distributed.distributed_manager import DistributedJobManager


class MockComputeSource(ComputeSource):
    """Mock compute source for testing."""

    def __init__(self, name: str, max_parallel_jobs: int = 2, fail_setup: bool = False):
        super().__init__(name, "mock", max_parallel_jobs)
        self.fail_setup = fail_setup
        self.submitted_jobs = []
        self.job_statuses = {}  # job_id -> status
        self.setup_called = False
        self.cleanup_called = False

    async def setup(self, sweep_dir: Path, sweep_id: str) -> bool:
        """Mock setup method."""
        self.setup_called = True
        if self.fail_setup:
            return False
        return True

    async def submit_job(
        self, params: dict, job_name: str, sweep_id: str, wandb_group: str = None
    ) -> str:
        """Mock job submission."""
        job_id = f"{self.name}_{len(self.submitted_jobs) + 1}"
        self.submitted_jobs.append(
            {
                "job_id": job_id,
                "params": params,
                "job_name": job_name,
                "sweep_id": sweep_id,
                "wandb_group": wandb_group,
            }
        )
        self.job_statuses[job_id] = "RUNNING"

        # Add to active_jobs to make utilization calculation work
        from datetime import datetime

        job_info = JobInfo(
            job_id=job_id,
            job_name=job_name,
            params=params,
            source_name=self.name,
            status="RUNNING",
            submit_time=datetime.now(),
            start_time=datetime.now(),
        )
        self.active_jobs[job_id] = job_info

        return job_id

    async def get_job_status(self, job_id: str) -> str:
        """Mock job status check."""
        return self.job_statuses.get(job_id, "UNKNOWN")

    async def cancel_job(self, job_id: str) -> bool:
        """Mock job cancellation."""
        if job_id in self.job_statuses:
            self.job_statuses[job_id] = "CANCELLED"
            return True
        return False

    async def collect_results(self, job_ids: list = None) -> bool:
        """Mock result collection."""
        return True

    async def cleanup(self) -> bool:
        """Mock cleanup."""
        self.cleanup_called = True
        return True

    async def health_check(self) -> Dict[str, Any]:
        """Mock health check."""
        return {"status": "healthy", "timestamp": "2023-01-01T00:00:00", "connection": "ok"}

    def get_current_load(self) -> int:
        """Get current job load."""
        running_jobs = sum(1 for status in self.job_statuses.values() if status == "RUNNING")
        return running_jobs

    def can_accept_job(self) -> bool:
        """Check if source can accept a new job."""
        return self.get_current_load() < self.max_parallel_jobs


class TestDistributedJobManager:
    """Test cases for DistributedJobManager."""

    @pytest.fixture
    def temp_dir(self, tmp_path):
        """Create a temporary directory for tests."""
        return tmp_path

    @pytest.fixture
    def sweep_dir(self, temp_dir):
        """Create a sweep directory."""
        sweep_dir = temp_dir / "test_sweep"
        sweep_dir.mkdir()
        return sweep_dir

    @pytest.fixture
    def distributed_manager(self, sweep_dir):
        """Create a distributed job manager for testing."""
        config = DistributedSweepConfig(
            strategy=DistributionStrategy.ROUND_ROBIN,
            collect_interval=10,  # Short interval for testing
            health_check_interval=5,
        )
        return DistributedJobManager(sweep_dir, config, show_progress=False)

    @pytest.fixture
    def mock_sources(self):
        """Create mock compute sources."""
        return [
            MockComputeSource("source1", max_parallel_jobs=2),
            MockComputeSource("source2", max_parallel_jobs=1),
            MockComputeSource("source3", max_parallel_jobs=3),
        ]

    @pytest.fixture
    def sample_params(self):
        """Sample parameter combinations for testing."""
        return [
            {"lr": 0.001, "batch_size": 16},
            {"lr": 0.001, "batch_size": 32},
            {"lr": 0.01, "batch_size": 16},
            {"lr": 0.01, "batch_size": 32},
            {"lr": 0.1, "batch_size": 16},
        ]

    @pytest.mark.unit
    def test_init(self, sweep_dir):
        """Test distributed manager initialization."""
        config = DistributedSweepConfig(strategy=DistributionStrategy.LEAST_LOADED)
        manager = DistributedJobManager(sweep_dir, config)

        assert manager.sweep_dir == sweep_dir
        assert manager.config.strategy == DistributionStrategy.LEAST_LOADED
        assert len(manager.sources) == 0
        assert len(manager.source_by_name) == 0
        assert manager.total_jobs_planned == 0
        assert not manager._running

    @pytest.mark.unit
    def test_add_compute_source(self, distributed_manager, mock_sources):
        """Test adding compute sources."""
        for source in mock_sources:
            distributed_manager.add_compute_source(source)

        assert len(distributed_manager.sources) == 3
        assert "source1" in distributed_manager.source_by_name
        assert "source2" in distributed_manager.source_by_name
        assert "source3" in distributed_manager.source_by_name

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_setup_all_sources_success(self, distributed_manager, mock_sources):
        """Test successful setup of all sources."""
        for source in mock_sources:
            distributed_manager.add_compute_source(source)

        result = await distributed_manager.setup_all_sources("test_sweep")

        assert result is True
        assert len(distributed_manager.sources) == 3
        for source in mock_sources:
            assert source.setup_called

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_setup_all_sources_with_failures(self, distributed_manager):
        """Test setup with some sources failing."""
        sources = [
            MockComputeSource("good1", fail_setup=False),
            MockComputeSource("bad1", fail_setup=True),
            MockComputeSource("good2", fail_setup=False),
            MockComputeSource("bad2", fail_setup=True),
        ]

        for source in sources:
            distributed_manager.add_compute_source(source)

        result = await distributed_manager.setup_all_sources("test_sweep")

        assert result is True  # Should succeed if at least one source works
        assert len(distributed_manager.sources) == 2  # Only good sources remain
        assert "good1" in distributed_manager.source_by_name
        assert "good2" in distributed_manager.source_by_name
        assert "bad1" not in distributed_manager.source_by_name
        assert "bad2" not in distributed_manager.source_by_name

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_setup_all_sources_all_fail(self, distributed_manager):
        """Test setup when all sources fail."""
        sources = [
            MockComputeSource("bad1", fail_setup=True),
            MockComputeSource("bad2", fail_setup=True),
        ]

        for source in sources:
            distributed_manager.add_compute_source(source)

        result = await distributed_manager.setup_all_sources("test_sweep")

        assert result is False
        assert len(distributed_manager.sources) == 0

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_select_compute_source_round_robin(self, distributed_manager, mock_sources):
        """Test round-robin source selection."""
        distributed_manager.config.strategy = DistributionStrategy.ROUND_ROBIN

        for source in mock_sources:
            distributed_manager.add_compute_source(source)

        # Test round-robin selection
        selected1 = await distributed_manager._select_compute_source()
        selected2 = await distributed_manager._select_compute_source()
        selected3 = await distributed_manager._select_compute_source()
        selected4 = await distributed_manager._select_compute_source()  # Should wrap around

        assert selected1.name == "source1"
        assert selected2.name == "source2"
        assert selected3.name == "source3"
        assert selected4.name == "source1"  # Wrapped around

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_select_compute_source_least_loaded(self, distributed_manager, mock_sources):
        """Test least-loaded source selection."""
        distributed_manager.config.strategy = DistributionStrategy.LEAST_LOADED

        for source in mock_sources:
            distributed_manager.add_compute_source(source)

        # Simulate different loads by adding jobs to active_jobs
        from datetime import datetime

        # Source 1: 2 active jobs (utilization = 2/2 = 1.0)
        for i in range(2):
            job_id = f"job{i + 1}"
            job_info = JobInfo(job_id, f"task{i + 1}", {}, "source1", "RUNNING", datetime.now())
            mock_sources[0].active_jobs[job_id] = job_info
            mock_sources[0].job_statuses[job_id] = "RUNNING"

        # Source 2: 1 active job (utilization = 1/1 = 1.0)
        job_info = JobInfo("job3", "task3", {}, "source2", "RUNNING", datetime.now())
        mock_sources[1].active_jobs["job3"] = job_info
        mock_sources[1].job_statuses["job3"] = "RUNNING"

        # Source 3: 0 active jobs (utilization = 0/3 = 0.0)
        # No jobs added - should have lowest utilization

        selected = await distributed_manager._select_compute_source()
        assert selected.name == "source3"  # Should select the one with lowest load

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_cancel_all_jobs(self, distributed_manager, mock_sources):
        """Test cancelling all jobs across sources."""
        for source in mock_sources:
            distributed_manager.add_compute_source(source)

        # Simulate some jobs
        distributed_manager.all_jobs = {
            "job1": JobInfo("job1", "task1", {}, "source1", "RUNNING"),
            "job2": JobInfo("job2", "task2", {}, "source2", "RUNNING"),
            "job3": JobInfo("job3", "task3", {}, "source1", "COMPLETED"),
        }
        distributed_manager.job_to_source = {
            "job1": "source1",
            "job2": "source2",
            "job3": "source1",
        }

        # Mock the source status
        mock_sources[0].job_statuses = {"job1": "RUNNING", "job3": "COMPLETED"}
        mock_sources[1].job_statuses = {"job2": "RUNNING"}

        results = await distributed_manager.cancel_all_jobs()

        assert "cancelled" in results
        assert "failed" in results
        assert results["cancelled"] >= 0
        assert results["failed"] >= 0

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_cleanup(self, distributed_manager, mock_sources):
        """Test cleanup of distributed manager."""
        for source in mock_sources:
            distributed_manager.add_compute_source(source)

        await distributed_manager.cleanup()

        for source in mock_sources:
            assert source.cleanup_called

    @pytest.mark.unit
    def test_get_distributed_stats(self, distributed_manager, mock_sources):
        """Test getting distributed statistics."""
        for source in mock_sources:
            distributed_manager.add_compute_source(source)

        distributed_manager.total_jobs_planned = 10
        distributed_manager.jobs_submitted = 8
        distributed_manager.jobs_completed = 5
        distributed_manager.jobs_failed = 1
        distributed_manager.jobs_cancelled = 2

        stats = distributed_manager.get_distributed_stats()

        assert "overall" in stats
        assert "sources" in stats
        assert "strategy" in stats
        assert "timestamp" in stats

        overall = stats["overall"]
        assert overall["total_planned"] == 10
        assert overall["submitted"] == 8
        assert overall["completed"] == 5
        assert overall["failed"] == 1
        assert overall["cancelled"] == 2

        assert len(stats["sources"]) == 3
        assert "source1" in stats["sources"]
        assert "source2" in stats["sources"]
        assert "source3" in stats["sources"]
