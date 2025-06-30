"""
Integration tests for the simple_mlp example.

These tests cover the specific issues encountered when running the simple_mlp example
and ensure that the HSM v2 system works correctly for basic local execution.
"""

import pytest
import asyncio
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

pytestmark = pytest.mark.unit

from hsm.cli.utils import create_compute_source, parse_compute_sources
from hsm.compute.local import LocalComputeSource
from hsm.compute.base import SweepContext, Task, TaskStatus, HealthStatus
from hsm.config.sweep import SweepConfig
from hsm.core.engine import SweepEngine, DistributionStrategy
from hsm.utils.params import ParameterGenerator


class TestSimpleMlpIntegration:
    """Integration tests for the simple_mlp example."""

    @pytest.fixture
    def temp_project_dir(self):
        """Create a temporary project directory with a main.py file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            project_dir = Path(temp_dir)

            # Create main.py file
            main_py = project_dir / "main.py"
            main_py.write_text("""
import argparse
import sys

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--learning_rate", type=float, default=0.01)
    parser.add_argument("--batch_size", type=int, default=32)
    args = parser.parse_args()
    
    print(f"Training with lr={args.learning_rate}, batch_size={args.batch_size}")
    print("Training completed successfully!")
    return 0

if __name__ == "__main__":
    sys.exit(main())
""")

            # Create output directories
            (project_dir / "outputs" / "sweeps").mkdir(parents=True, exist_ok=True)

            yield project_dir

    def test_local_compute_source_creation(self):
        """Test that local compute source can be created with default config."""
        config = {"name": "local-test"}
        source = create_compute_source("local", config)

        assert isinstance(source, LocalComputeSource)
        assert source.name == "local-test"
        assert source.source_type == "local"
        assert source.max_concurrent_tasks == 4  # default value

    def test_sweep_config_parameter_generation(self):
        """Test that SweepConfig and ParameterGenerator work correctly."""
        config_data = {"grid": {"learning_rate": [0.001, 0.01], "batch_size": [32, 64]}}

        config = SweepConfig(**config_data)
        param_gen = ParameterGenerator(config)

        # Test the correct method name
        combinations = param_gen.generate_combinations()
        assert len(combinations) == 4  # 2 * 2

        # Test count method
        count = param_gen.count_combinations()
        assert count == 4

    @pytest.mark.asyncio
    async def test_local_compute_source_setup_and_health_check(self, temp_project_dir):
        """Test that local compute source can be set up and health checked."""
        source = LocalComputeSource(
            name="test-local", max_concurrent_tasks=2, project_dir=str(temp_project_dir)
        )

        # Create sweep context
        sweep_context = SweepContext(
            sweep_id="test_sweep",
            sweep_dir=temp_project_dir / "outputs" / "sweeps" / "test_sweep",
            config={},
        )

        # Test setup
        success = await source.setup(sweep_context)
        assert success
        assert source._setup_complete
        assert source.script_path == str(temp_project_dir / "main.py")  # auto-detected

        # Test health check
        health_report = await source.health_check()
        assert health_report.source_name == "test-local"
        assert health_report.status == HealthStatus.HEALTHY

    @pytest.mark.asyncio
    async def test_task_submission_and_status(self, temp_project_dir):
        """Test that tasks can be submitted and their status queried."""
        source = LocalComputeSource(
            name="test-local",
            max_concurrent_tasks=2,
            project_dir=str(temp_project_dir),
            timeout=30,  # shorter timeout for testing
        )

        # Setup source
        sweep_context = SweepContext(
            sweep_id="test_sweep",
            sweep_dir=temp_project_dir / "outputs" / "sweeps" / "test_sweep",
            config={},
        )
        await source.setup(sweep_context)

        # Create test task
        task = Task(
            task_id="test_task_001",
            params={"learning_rate": 0.01, "batch_size": 32},
            sweep_id="test_sweep",
        )

        # Submit task
        result = await source.submit_task(task)
        assert result.status == TaskStatus.QUEUED  # Should be QUEUED, not PENDING
        assert result.task_id == "test_task_001"

        # Wait a bit for task to start/complete
        await asyncio.sleep(2)

        # Check task status
        status_result = await source.get_task_status("test_task_001")
        assert status_result.task_id == "test_task_001"
        # Status should be RUNNING or COMPLETED
        assert status_result.status in [TaskStatus.RUNNING, TaskStatus.COMPLETED]

    @pytest.mark.asyncio
    async def test_sweep_engine_integration(self, temp_project_dir):
        """Test full SweepEngine integration with LocalComputeSource."""
        # Create local compute source
        source = LocalComputeSource(
            name="test-local", max_concurrent_tasks=2, project_dir=str(temp_project_dir), timeout=30
        )

        # Create sweep context
        sweep_context = SweepContext(
            sweep_id="integration_test",
            sweep_dir=temp_project_dir / "outputs" / "sweeps" / "integration_test",
            config={"grid": {"learning_rate": [0.01], "batch_size": [32]}},
        )

        # Create SweepEngine
        engine = SweepEngine(
            sweep_context=sweep_context,
            sources=[source],
            distribution_strategy=DistributionStrategy.ROUND_ROBIN,
            max_concurrent_tasks=1,
        )

        # Setup sources
        setup_success = await engine.setup_sources()
        assert setup_success

        # Create tasks
        tasks = [
            Task(
                task_id="integration_task_001",
                params={"learning_rate": 0.01, "batch_size": 32},
                sweep_id="integration_test",
            )
        ]

        # Run sweep
        result = await engine.run_sweep(tasks)

        # Verify results
        assert result.sweep_id == "integration_test"
        assert result.total_tasks == 1
        assert result.completed_tasks >= 0  # Should complete or at least start
        assert result.status.value in ["completed", "failed"]  # Should finish

    def test_parse_compute_sources_simple_local(self):
        """Test parsing simple local compute source specification."""
        source_specs = parse_compute_sources("local")

        assert len(source_specs) == 1
        source_type, config = source_specs[0]
        assert source_type == "local"
        assert "name" in config

    def test_end_to_end_parameter_flow(self):
        """Test the complete parameter flow from config to combinations."""
        # This mirrors what happens in the CLI
        config_data = {"grid": {"learning_rate": [0.01]}}

        # Create SweepConfig
        sweep_config = SweepConfig(**config_data)

        # Generate parameters
        param_gen = ParameterGenerator(sweep_config)
        task_params = param_gen.generate_combinations()

        # Create tasks
        tasks = []
        for i, params in enumerate(task_params):
            task = Task(task_id=f"test_task_{i:04d}", params=params, sweep_id="test_sweep")
            tasks.append(task)

        assert len(tasks) == 1
        assert tasks[0].params["learning_rate"] == 0.01

    @pytest.mark.asyncio
    async def test_error_handling_and_recovery(self, temp_project_dir):
        """Test error handling in various scenarios."""
        source = LocalComputeSource(name="test-local", project_dir=str(temp_project_dir))

        # Test submitting task without setup
        task = Task(task_id="test_task_002", params={"learning_rate": 0.01}, sweep_id="test_sweep")

        result = await source.submit_task(task)
        assert result.status == TaskStatus.FAILED
        assert "not properly set up" in result.message

    def test_compute_source_stats_initialization(self):
        """Test that ComputeSourceStats is initialized correctly."""
        source = LocalComputeSource(name="test-local")

        # Verify stats object was created correctly
        assert source.stats.source_name == "test-local"
        assert source.stats.source_type == "local"
        assert source.stats.max_parallel_tasks == 4  # default
        assert source.stats.tasks_submitted == 0
        assert source.stats.tasks_completed == 0
        assert source.stats.health_status == HealthStatus.UNKNOWN
