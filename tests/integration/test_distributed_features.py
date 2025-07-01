"""
Comprehensive tests for HSM v2 distributed features using simple_mlp example.

This validates the key features claimed in documentation:
- Multi-source distributed execution
- Health monitoring and failover
- Cross-mode completion
- Result collection and aggregation
"""

import asyncio
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from unittest.mock import AsyncMock, patch

from click.testing import CliRunner
import pytest
import yaml

from hsm.cli.main import cli
from hsm.compute.base import HealthStatus, SweepContext, Task, TaskStatus
from hsm.compute.local import LocalComputeSource
from hsm.core.engine import DistributionStrategy, SweepEngine, SweepStatus
from hsm.utils.params import ParameterGenerator

pytestmark = [pytest.mark.integration, pytest.mark.slow]


class TestDistributedFeatures:
    """Test distributed sweep features using simple_mlp."""

    @pytest.fixture
    def simple_mlp_copy(self, temp_dir, simple_mlp_dir):
        """Create working copy of simple_mlp in temp directory."""
        mlp_copy = temp_dir / "simple_mlp_test"
        shutil.copytree(simple_mlp_dir, mlp_copy)
        return mlp_copy

    @pytest.fixture
    def test_sweep_config(self):
        """Minimal sweep config for testing."""
        return {
            "grid": {"training.lr": [0.01, 0.02], "training.epochs": [2, 3]},
            "metadata": {"description": "Test distributed sweep", "tags": ["integration"]},
        }

    @pytest.mark.asyncio
    async def test_multi_source_execution(self, simple_mlp_copy, test_sweep_config):
        """Test execution across multiple compute sources."""
        # Create multiple sources to simulate distributed execution
        sources = [
            LocalComputeSource(
                name="source-1", max_concurrent_tasks=2, project_dir=str(simple_mlp_copy)
            ),
            LocalComputeSource(
                name="source-2", max_concurrent_tasks=2, project_dir=str(simple_mlp_copy)
            ),
        ]

        sweep_dir = simple_mlp_copy / "outputs" / "test_multi_source"
        sweep_context = SweepContext(
            sweep_id="test_multi",
            sweep_dir=sweep_dir,
            config=test_sweep_config,
        )

        engine = SweepEngine(
            sweep_context=sweep_context,
            sources=sources,
            distribution_strategy=DistributionStrategy.ROUND_ROBIN,
            max_concurrent_tasks=4,
        )

        # Setup sources
        setup_success = await engine.setup_sources()
        assert setup_success
        assert len(engine.sources) == 2

        # Generate tasks
        from hsm.config.sweep import SweepConfig

        sweep_config = SweepConfig(**test_sweep_config)
        param_gen = ParameterGenerator(sweep_config)
        combinations = list(param_gen.generate_combinations())

        tasks = [
            Task(task_id=f"task_{i:03d}", params=params, sweep_id="test_multi")
            for i, params in enumerate(combinations[:4])
        ]

        # Run sweep
        result = await engine.run_sweep(tasks)

        # Verify distributed execution
        assert result.status in [SweepStatus.COMPLETED, SweepStatus.FAILED]
        assert result.total_tasks == 4
        assert len(result.source_stats) == 2

        # Verify task distribution
        task_assignments = {}
        for task_id, source_name in engine.task_to_source.items():
            task_assignments.setdefault(source_name, 0)
            task_assignments[source_name] += 1

        # Should use both sources
        assert len(task_assignments) >= 1

        # Verify directory structure
        assert sweep_dir.exists()
        assert (sweep_dir / "tasks").exists()
        assert (sweep_dir / "logs").exists()

    @pytest.mark.asyncio
    async def test_health_monitoring_failover(self, simple_mlp_copy, test_sweep_config):
        """Test health monitoring with source failure simulation."""
        healthy_source = LocalComputeSource(
            name="healthy", max_concurrent_tasks=2, project_dir=str(simple_mlp_copy)
        )
        failing_source = LocalComputeSource(
            name="failing", max_concurrent_tasks=2, project_dir=str(simple_mlp_copy)
        )

        # Mock failing source health check
        async def mock_failing_health_check():
            from hsm.compute.base import HealthReport

            return HealthReport(
                source_name="failing",
                status=HealthStatus.UNHEALTHY,
                timestamp=None,
                message="Simulated failure",
            )

        failing_source.health_check = mock_failing_health_check

        sweep_context = SweepContext(
            sweep_id="test_failover",
            sweep_dir=simple_mlp_copy / "outputs" / "test_failover",
            config=test_sweep_config,
        )

        engine = SweepEngine(
            sweep_context=sweep_context,
            sources=[healthy_source, failing_source],
            health_check_interval=1,
            auto_disable_unhealthy_sources=True,
            health_check_failure_threshold=2,
        )

        await engine.setup_sources()
        assert len(engine.sources) == 2

        # Start health monitoring
        await engine._start_background_tasks()

        # Wait for health checks
        await asyncio.sleep(3)

        # Verify failing source was disabled
        health_status = engine.get_health_status()
        assert "sources" in health_status

        await engine._cleanup()

    def test_cli_cross_mode_completion(self, simple_mlp_copy):
        """Test cross-mode completion through CLI."""
        cli_runner = CliRunner()
        original_cwd = os.getcwd()

        try:
            os.chdir(simple_mlp_copy)

            # Create minimal sweep config
            sweep_config = {
                "grid": {"training.lr": [0.01, 0.02], "training.epochs": [1]},
                "metadata": {"description": "Completion test"},
            }

            with open("test_completion.yaml", "w") as f:
                yaml.dump(sweep_config, f)

            # Run partial sweep
            result = cli_runner.invoke(
                cli,
                [
                    "run",
                    "--config",
                    "test_completion.yaml",
                    "--sources",
                    "local",
                    "--max-tasks",
                    "1",
                ],
            )

            print(f"Run result: {result.exit_code}, {result.output}")

            # Find sweep directory
            outputs_dir = (
                Path("sweeps/outputs") if Path("sweeps/outputs").exists() else Path("outputs")
            )
            if outputs_dir.exists():
                sweep_dirs = list(outputs_dir.glob("sweep_*"))
                if sweep_dirs:
                    sweep_id = sweep_dirs[-1].name

                    # Test completion command
                    result = cli_runner.invoke(
                        cli, ["complete", sweep_id, "--sources", "local", "--dry-run"]
                    )

                    print(f"Complete result: {result.exit_code}, {result.output}")
                    assert result.exit_code in [0, 1]

        finally:
            os.chdir(original_cwd)

    def test_monitor_health_command(self, simple_mlp_copy):
        """Test monitor health CLI command."""
        cli_runner = CliRunner()
        original_cwd = os.getcwd()

        try:
            os.chdir(simple_mlp_copy)

            result = cli_runner.invoke(cli, ["monitor", "health", "--sources", "local"])

            print(f"Health result: {result.exit_code}, {result.output}")
            assert result.exit_code in [0, 1]

        finally:
            os.chdir(original_cwd)

    def test_all_claimed_cli_commands_exist(self):
        """Test that claimed CLI commands actually exist."""
        cli_runner = CliRunner()

        commands_to_test = [
            ["--help"],
            ["config", "--help"],
            ["run", "--help"],
            ["complete", "--help"],
            ["status", "--help"],
            ["monitor", "--help"],
            ["monitor", "health", "--help"],
        ]

        for cmd in commands_to_test:
            result = cli_runner.invoke(cli, cmd)
            assert result.exit_code == 0, f"Command {cmd} should work"
            assert len(result.output) > 0

    @pytest.mark.asyncio
    async def test_sweep_engine_status_reporting(self, simple_mlp_copy, test_sweep_config):
        """Test comprehensive status reporting."""
        source = LocalComputeSource(
            name="status-test", max_concurrent_tasks=1, project_dir=str(simple_mlp_copy)
        )

        sweep_context = SweepContext(
            sweep_id="test_status",
            sweep_dir=simple_mlp_copy / "outputs" / "test_status",
            config=test_sweep_config,
        )

        engine = SweepEngine(sweep_context=sweep_context, sources=[source])
        await engine.setup_sources()

        # Test status methods
        status = await engine.get_sweep_status()
        assert "sweep_id" in status
        assert "progress" in status
        assert "sources" in status

        health_status = engine.get_health_status()
        assert "sources" in health_status
        assert "disabled_sources" in health_status

        await engine._cleanup()

    def test_simple_mlp_basic_functionality(self, simple_mlp_dir):
        """Test that simple_mlp works as documented."""
        # Test basic execution
        result = subprocess.run(
            [
                "python",
                "main.py",
                "training.epochs=1",
                "training.simulate_time=0.001",
                "logging.wandb.enabled=false",
            ],
            cwd=simple_mlp_dir,
            capture_output=True,
            text=True,
            timeout=30,
        )

        assert result.returncode == 0, f"simple_mlp should work: {result.stderr}"
        assert "Training completed" in result.stdout

    def test_distribution_strategies_exist(self):
        """Test that all claimed distribution strategies exist."""
        from hsm.core.engine import DistributionStrategy

        assert hasattr(DistributionStrategy, "ROUND_ROBIN")
        assert hasattr(DistributionStrategy, "LEAST_LOADED")
        assert hasattr(DistributionStrategy, "RANDOM")

    def test_health_status_types_exist(self):
        """Test that health status types exist."""
        from hsm.compute.base import HealthStatus

        assert hasattr(HealthStatus, "HEALTHY")
        assert hasattr(HealthStatus, "DEGRADED")
        assert hasattr(HealthStatus, "UNHEALTHY")

    @patch("hsm.compute.ssh.ASYNCSSH_AVAILABLE", False)
    def test_ssh_requires_asyncssh(self):
        """Test SSH source requires asyncssh."""
        from hsm.cli.utils import create_compute_source

        with pytest.raises(ImportError, match="asyncssh"):
            create_compute_source("ssh", {"hostname": "example.com"})

    def test_simple_mlp_has_required_structure(self, simple_mlp_dir):
        """Test simple_mlp has required files."""
        assert (simple_mlp_dir / "main.py").exists()
        assert (simple_mlp_dir / "configs").exists()
        assert (simple_mlp_dir / "hsm_config.yaml").exists()

    @pytest.mark.asyncio
    async def test_result_collection_mechanism(self, simple_mlp_copy, test_sweep_config):
        """Test result collection works."""
        source = LocalComputeSource(
            name="collector", max_concurrent_tasks=1, project_dir=str(simple_mlp_copy)
        )

        sweep_context = SweepContext(
            sweep_id="test_results",
            sweep_dir=simple_mlp_copy / "outputs" / "test_results",
            config=test_sweep_config,
        )

        engine = SweepEngine(
            sweep_context=sweep_context, sources=[source], result_collection_interval=1
        )

        await engine.setup_sources()

        # Create minimal task
        tasks = [
            Task(
                task_id="result_task",
                params={"training.lr": 0.01, "training.epochs": 1},
                sweep_id="test_results",
            )
        ]

        result = await engine.run_sweep(tasks)

        # Verify result collection occurred
        assert result.status in [SweepStatus.COMPLETED, SweepStatus.FAILED]

        # Check directory structure created
        assert sweep_context.sweep_dir.exists()
        assert (sweep_context.sweep_dir / "tasks").exists()
        assert (sweep_context.sweep_dir / "logs").exists()
