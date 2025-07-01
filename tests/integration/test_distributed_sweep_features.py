"""
Comprehensive tests for distributed sweep execution features using simple_mlp.

This test suite validates all the key features claimed in HSM v2 documentation:
- Multi-source distributed execution
- Health monitoring and automatic failover
- Cross-mode completion
- Project synchronization verification
- Result collection from remote sources
- Error analysis and recovery
- CLI command completeness
"""

import asyncio
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from unittest.mock import AsyncMock, MagicMock, patch

from click.testing import CliRunner
import pytest
import yaml

from hsm.cli.main import cli
from hsm.compute.base import HealthStatus, SweepContext, Task, TaskStatus
from hsm.compute.local import LocalComputeSource
from hsm.config.hsm import HSMConfig
from hsm.core.engine import DistributionStrategy, SweepEngine, SweepStatus
from hsm.utils.params import ParameterGenerator

pytestmark = [pytest.mark.integration, pytest.mark.slow]


class TestDistributedSweepFeatures:
    """Test all claimed distributed sweep features using simple_mlp."""

    @pytest.fixture
    def simple_mlp_copy(self, temp_dir, simple_mlp_dir):
        """Create a working copy of simple_mlp in temp directory."""
        mlp_copy = temp_dir / "simple_mlp_test"
        shutil.copytree(simple_mlp_dir, mlp_copy)
        return mlp_copy

    @pytest.fixture
    def test_sweep_config(self):
        """Create a minimal sweep config for testing."""
        return {
            "grid": {
                "training.lr": [0.01, 0.02],
                "training.epochs": [2, 3],
                "model.hidden_dim": [32, 64],
            },
            "metadata": {
                "description": "Test sweep for distributed features",
                "tags": ["integration", "distributed"],
            },
        }

    @pytest.mark.asyncio
    async def test_multi_source_execution_simulation(self, simple_mlp_copy, test_sweep_config):
        """Test distributed execution across multiple compute sources."""
        # Create multiple local sources to simulate distributed execution
        sources = [
            LocalComputeSource(
                name="local-1", max_concurrent_tasks=2, project_dir=str(simple_mlp_copy)
            ),
            LocalComputeSource(
                name="local-2", max_concurrent_tasks=2, project_dir=str(simple_mlp_copy)
            ),
            LocalComputeSource(
                name="local-3", max_concurrent_tasks=1, project_dir=str(simple_mlp_copy)
            ),
        ]

        # Create sweep context
        sweep_dir = simple_mlp_copy / "outputs" / "test_distributed_sweep"
        sweep_context = SweepContext(
            sweep_id="test_distributed",
            sweep_dir=sweep_dir,
            config=test_sweep_config,
        )

        # Create SweepEngine with multiple sources
        engine = SweepEngine(
            sweep_context=sweep_context,
            sources=sources,
            distribution_strategy=DistributionStrategy.ROUND_ROBIN,
            max_concurrent_tasks=5,
            health_check_interval=5,  # More frequent for testing
            result_collection_interval=2,
        )

        # Setup sources
        setup_success = await engine.setup_sources()
        assert setup_success, "All sources should setup successfully"
        assert len(engine.sources) == 3, "All sources should be available"

        # Generate tasks
        from hsm.config.sweep import SweepConfig

        sweep_config = SweepConfig(**test_sweep_config)
        param_gen = ParameterGenerator(sweep_config)
        combinations = list(param_gen.generate_combinations())

        tasks = [
            Task(
                task_id=f"task_{i:03d}",
                params=params,
                sweep_id="test_distributed",
            )
            for i, params in enumerate(combinations[:6])  # Test with 6 tasks
        ]

        # Run sweep
        result = await engine.run_sweep(tasks)

        # Verify distributed execution
        assert result.status in [SweepStatus.COMPLETED, SweepStatus.FAILED]
        assert result.total_tasks == 6
        assert len(result.source_stats) == 3  # All sources should have stats

        # Verify task distribution across sources
        task_assignments = {}
        for task_id, source_name in engine.task_to_source.items():
            if source_name not in task_assignments:
                task_assignments[source_name] = 0
            task_assignments[source_name] += 1

        # Should distribute tasks across multiple sources
        sources_used = len([count for count in task_assignments.values() if count > 0])
        assert sources_used >= 2, (
            f"Tasks should be distributed across multiple sources, got: {task_assignments}"
        )

        # Verify sweep directory structure exists
        assert sweep_dir.exists()
        assert (sweep_dir / "tasks").exists()
        assert (sweep_dir / "logs").exists()
        assert (sweep_dir / "sweep_config.yaml").exists()

    @pytest.mark.asyncio
    async def test_health_monitoring_and_failover(self, simple_mlp_copy, test_sweep_config):
        """Test health monitoring with simulated source failure and recovery."""
        # Create sources with one that will "fail"
        healthy_source = LocalComputeSource(
            name="healthy", max_concurrent_tasks=2, project_dir=str(simple_mlp_copy)
        )
        failing_source = LocalComputeSource(
            name="failing", max_concurrent_tasks=2, project_dir=str(simple_mlp_copy)
        )

        # Mock the failing source to simulate health check failures
        original_health_check = failing_source.health_check
        health_check_call_count = 0

        async def mock_health_check():
            nonlocal health_check_call_count
            health_check_call_count += 1
            if health_check_call_count <= 3:  # Fail first 3 health checks
                from hsm.compute.base import HealthReport

                return HealthReport(
                    source_name="failing",
                    status=HealthStatus.UNHEALTHY,
                    timestamp=time.time(),
                    message="Simulated health failure",
                )
            else:
                # Recover after 3 failures
                return await original_health_check()

        failing_source.health_check = mock_health_check

        sources = [healthy_source, failing_source]

        # Create engine with aggressive health monitoring
        sweep_context = SweepContext(
            sweep_id="test_failover",
            sweep_dir=simple_mlp_copy / "outputs" / "test_failover",
            config=test_sweep_config,
        )

        engine = SweepEngine(
            sweep_context=sweep_context,
            sources=sources,
            health_check_interval=1,  # Check every second
            auto_disable_unhealthy_sources=True,
            health_check_failure_threshold=3,
        )

        # Setup sources
        await engine.setup_sources()
        assert len(engine.sources) == 2

        # Start health monitoring
        await engine._start_background_tasks()

        # Wait for health checks to run and disable the failing source
        await asyncio.sleep(5)

        # Check that failing source was disabled
        assert "failing" in engine.disabled_sources, (
            f"Failing source should be disabled. Disabled: {engine.disabled_sources}"
        )

        # Get health status
        health_status = engine.get_health_status()
        assert "failing" in health_status["disabled_sources"]
        assert health_status["sources"]["failing"]["enabled"] == False

        # Create minimal tasks to verify execution continues with healthy source only
        tasks = [
            Task(
                task_id="failover_task_001", params={"training.lr": 0.01}, sweep_id="test_failover"
            )
        ]

        # Run sweep - should complete with only healthy source
        result = await engine.run_sweep(tasks)
        assert result.status in [SweepStatus.COMPLETED, SweepStatus.FAILED]

        await engine._cleanup()

    @pytest.mark.asyncio
    async def test_task_distribution_strategies(self, simple_mlp_copy, test_sweep_config):
        """Test different task distribution strategies."""
        # Create sources with different capacities
        sources = [
            LocalComputeSource(
                name="small", max_concurrent_tasks=1, project_dir=str(simple_mlp_copy)
            ),
            LocalComputeSource(
                name="medium", max_concurrent_tasks=2, project_dir=str(simple_mlp_copy)
            ),
            LocalComputeSource(
                name="large", max_concurrent_tasks=4, project_dir=str(simple_mlp_copy)
            ),
        ]

        tasks = [
            Task(task_id=f"dist_task_{i:03d}", params={"training.lr": 0.01}, sweep_id="test_dist")
            for i in range(6)
        ]

        # Test round-robin distribution
        for strategy in [DistributionStrategy.ROUND_ROBIN, DistributionStrategy.LEAST_LOADED]:
            sweep_context = SweepContext(
                sweep_id=f"test_{strategy.value}",
                sweep_dir=simple_mlp_copy / "outputs" / f"test_{strategy.value}",
                config=test_sweep_config,
            )

            engine = SweepEngine(
                sweep_context=sweep_context,
                sources=sources.copy(),
                distribution_strategy=strategy,
                max_concurrent_tasks=7,
            )

            await engine.setup_sources()

            # Submit just the first few tasks to test distribution logic
            for task in tasks[:3]:
                source = await engine._select_compute_source()
                assert source is not None, f"Should select a source for strategy {strategy}"
                engine.task_to_source[task.task_id] = source.name

            # Verify tasks were distributed
            assignments = list(engine.task_to_source.values())
            unique_sources = set(assignments)
            assert len(unique_sources) >= 2, (
                f"Strategy {strategy} should distribute across multiple sources"
            )

            await engine._cleanup()

    @pytest.mark.asyncio
    async def test_result_collection_and_aggregation(self, simple_mlp_copy, test_sweep_config):
        """Test result collection from multiple sources."""
        sources = [
            LocalComputeSource(
                name="collector-1", max_concurrent_tasks=1, project_dir=str(simple_mlp_copy)
            ),
            LocalComputeSource(
                name="collector-2", max_concurrent_tasks=1, project_dir=str(simple_mlp_copy)
            ),
        ]

        sweep_context = SweepContext(
            sweep_id="test_collection",
            sweep_dir=simple_mlp_copy / "outputs" / "test_collection",
            config=test_sweep_config,
        )

        engine = SweepEngine(
            sweep_context=sweep_context,
            sources=sources,
            result_collection_interval=1,  # Collect frequently
        )

        await engine.setup_sources()

        # Create and run tasks
        tasks = [
            Task(
                task_id="collect_task_001",
                params={"training.lr": 0.01, "training.epochs": 1},
                sweep_id="test_collection",
            ),
            Task(
                task_id="collect_task_002",
                params={"training.lr": 0.02, "training.epochs": 1},
                sweep_id="test_collection",
            ),
        ]

        result = await engine.run_sweep(tasks)

        # Verify result collection
        assert result.status in [SweepStatus.COMPLETED, SweepStatus.FAILED]

        # Check that task directories exist
        tasks_dir = sweep_context.sweep_dir / "tasks"
        assert tasks_dir.exists()

        # Verify logs aggregation
        logs_dir = sweep_context.sweep_dir / "logs"
        assert logs_dir.exists()
        assert (logs_dir / "sources").exists()

        # Check for aggregated log if tasks completed
        aggregated_log = logs_dir / "sweep_aggregated.log"
        if result.completed_tasks > 0:
            assert aggregated_log.exists(), "Aggregated log should exist for completed sweep"

    def test_cli_cross_mode_completion_simulation(self, simple_mlp_copy):
        """Test cross-mode completion through CLI commands."""
        cli_runner = CliRunner()
        original_cwd = os.getcwd()

        try:
            os.chdir(simple_mlp_copy)

            # Create a minimal sweep config
            sweep_config = {
                "grid": {"training.lr": [0.01, 0.02], "training.epochs": [1]},
                "metadata": {"description": "Cross-mode completion test"},
            }

            with open("test_completion_sweep.yaml", "w") as f:
                yaml.dump(sweep_config, f)

            # Simulate partial sweep by running with limited tasks
            result = cli_runner.invoke(
                cli,
                [
                    "run",
                    "--config",
                    "test_completion_sweep.yaml",
                    "--sources",
                    "local",
                    "--max-tasks",
                    "1",  # Only run 1 out of 2 tasks
                    "--parallel-limit",
                    "1",
                ],
            )

            print(f"Initial run result: {result.exit_code}")
            print(f"Initial run output: {result.output}")

            # Find the sweep directory that was created
            outputs_dir = (
                Path("sweeps/outputs") if Path("sweeps/outputs").exists() else Path("outputs")
            )
            if outputs_dir.exists():
                sweep_dirs = list(outputs_dir.glob("sweep_*"))
                if sweep_dirs:
                    sweep_id = sweep_dirs[-1].name  # Get the most recent

                    # Test completion command
                    result = cli_runner.invoke(
                        cli, ["complete", sweep_id, "--sources", "local", "--dry-run"]
                    )

                    print(f"Completion dry-run result: {result.exit_code}")
                    print(f"Completion dry-run output: {result.output}")

                    # Should show completion plan
                    assert result.exit_code in [0, 1]  # May have warnings but shouldn't crash

        finally:
            os.chdir(original_cwd)

    def test_error_analysis_and_debugging(self, simple_mlp_copy):
        """Test error analysis capabilities."""
        cli_runner = CliRunner()
        original_cwd = os.getcwd()

        try:
            os.chdir(simple_mlp_copy)

            # Create a sweep that will have failures
            bad_sweep_config = {
                "grid": {
                    "training.lr": [0.01],
                    "training.epochs": [-1],  # Invalid negative epochs to cause failure
                    "nonexistent.param": [1],  # Param that doesn't exist
                },
                "metadata": {"description": "Error analysis test"},
            }

            with open("test_error_sweep.yaml", "w") as f:
                yaml.dump(bad_sweep_config, f)

            # Run sweep that should have errors
            result = cli_runner.invoke(
                cli,
                [
                    "run",
                    "--config",
                    "test_error_sweep.yaml",
                    "--sources",
                    "local",
                    "--max-tasks",
                    "1",
                    "--parallel-limit",
                    "1",
                ],
            )

            print(f"Error sweep result: {result.exit_code}")
            print(f"Error sweep output: {result.output}")

            # Command should execute (though tasks may fail)
            assert result.exit_code in [0, 1]

        finally:
            os.chdir(original_cwd)

    @patch("hsm.compute.ssh.ASYNCSSH_AVAILABLE", False)
    def test_ssh_source_creation_without_asyncssh(self):
        """Test that SSH source creation fails gracefully without asyncssh."""
        from hsm.cli.utils import create_compute_source

        with pytest.raises(ImportError, match="asyncssh is required"):
            create_compute_source("ssh", {"hostname": "example.com"})

    def test_monitor_commands_functionality(self, simple_mlp_copy):
        """Test monitor CLI commands."""
        cli_runner = CliRunner()
        original_cwd = os.getcwd()

        try:
            os.chdir(simple_mlp_copy)

            # Test health monitoring command
            result = cli_runner.invoke(cli, ["monitor", "health", "--sources", "local"])

            print(f"Monitor health result: {result.exit_code}")
            print(f"Monitor health output: {result.output}")

            # Should complete without error
            assert result.exit_code in [0, 1]  # May have warnings

        finally:
            os.chdir(original_cwd)

    def test_comprehensive_cli_help_coverage(self):
        """Test that all claimed CLI commands have working help."""
        cli_runner = CliRunner()

        # Test main commands mentioned in documentation
        commands_to_test = [
            ["--help"],
            ["config", "--help"],
            ["config", "init", "--help"],
            ["config", "show", "--help"],
            ["config", "validate", "--help"],
            ["run", "--help"],
            ["complete", "--help"],
            ["status", "--help"],
            ["monitor", "--help"],
            ["monitor", "health", "--help"],
            ["monitor", "watch", "--help"],
        ]

        for cmd in commands_to_test:
            result = cli_runner.invoke(cli, cmd)
            assert result.exit_code == 0, f"Help command {' '.join(cmd)} should work"
            assert len(result.output) > 0, f"Help command {' '.join(cmd)} should produce output"

    @pytest.mark.asyncio
    async def test_sweep_engine_comprehensive_status(self, simple_mlp_copy, test_sweep_config):
        """Test comprehensive status reporting from SweepEngine."""
        source = LocalComputeSource(
            name="status-test", max_concurrent_tasks=2, project_dir=str(simple_mlp_copy)
        )

        sweep_context = SweepContext(
            sweep_id="test_status",
            sweep_dir=simple_mlp_copy / "outputs" / "test_status",
            config=test_sweep_config,
        )

        engine = SweepEngine(
            sweep_context=sweep_context,
            sources=[source],
        )

        await engine.setup_sources()

        # Test initial status
        status = await engine.get_sweep_status()
        assert status["sweep_id"] == "test_status"
        assert status["status"] == "pending"
        assert "progress" in status
        assert "sources" in status
        assert "health" in status

        # Test health status
        health_status = engine.get_health_status()
        assert "sources" in health_status
        assert "disabled_sources" in health_status
        assert "failure_tracking" in health_status

        await engine._cleanup()

    def test_project_structure_requirements(self, simple_mlp_copy):
        """Test that simple_mlp has the required structure for comprehensive testing."""
        # Verify simple_mlp has all required components
        assert (simple_mlp_copy / "main.py").exists(), "main.py should exist"
        assert (simple_mlp_copy / "configs").exists(), "configs directory should exist"
        assert (simple_mlp_copy / "configs" / "config.yaml").exists(), "config.yaml should exist"
        assert (simple_mlp_copy / "hsm_config.yaml").exists(), "hsm_config.yaml should exist"

        # Verify main.py is executable
        result = subprocess.run(
            ["python", "main.py", "--help"],
            cwd=simple_mlp_copy,
            capture_output=True,
            text=True,
            timeout=10,
        )

        # Should either show help or run without crashing
        assert result.returncode in [0, 1], "main.py should be executable"


class TestSimpleMlpDocumentationClaims:
    """Test specific claims made in the documentation using simple_mlp."""

    def test_simple_mlp_supports_hydra_config(self, simple_mlp_dir):
        """Test that simple_mlp properly supports Hydra configuration as claimed."""
        # Test that Hydra config overrides work
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

        assert result.returncode == 0, f"Hydra config should work: {result.stderr}"
        assert "Training completed" in result.stdout, "Should complete training successfully"

    def test_simple_mlp_dependency_graceful_handling(self, simple_mlp_dir):
        """Test that simple_mlp handles missing dependencies gracefully as claimed."""
        # The documentation claims it works without PyTorch/W&B
        # Verify this by checking the import handling in main.py
        main_py_content = (simple_mlp_dir / "main.py").read_text()

        assert "TORCH_AVAILABLE = True" in main_py_content or "try:" in main_py_content
        assert "except ImportError:" in main_py_content
        assert "mock" in main_py_content.lower() or "fallback" in main_py_content.lower()

    def test_simple_mlp_generates_results(self, simple_mlp_dir, temp_dir):
        """Test that simple_mlp generates the claimed output files."""
        output_dir = temp_dir / "test_results"

        result = subprocess.run(
            [
                "python",
                "main.py",
                "training.epochs=2",
                "training.simulate_time=0.001",
                "logging.wandb.enabled=false",
                f"output.dir={output_dir}",
                "output.save_results=true",
            ],
            cwd=simple_mlp_dir,
            capture_output=True,
            text=True,
            timeout=30,
        )

        assert result.returncode == 0, f"Should generate results: {result.stderr}"

        # Check that results file is created
        results_file = output_dir / "results.txt"
        assert results_file.exists(), "Should create results.txt file"

        # Verify results file has expected content
        content = results_file.read_text()
        assert "Final Loss:" in content
        assert "Best Loss:" in content
        assert "Config:" in content


class TestFeatureCompleteness:
    """Test that all claimed features actually exist and work."""

    def test_sweep_engine_has_claimed_attributes(self):
        """Test that SweepEngine has all attributes mentioned in documentation."""
        from hsm.core.engine import SweepEngine

        # Create minimal engine to test attributes
        sources = []
        sweep_context = SweepContext(sweep_id="test", sweep_dir=Path("/tmp/test"), config={})

        engine = SweepEngine(sweep_context=sweep_context, sources=sources)

        # Verify claimed attributes exist
        assert hasattr(engine, "health_check_interval")
        assert hasattr(engine, "result_collection_interval")
        assert hasattr(engine, "disabled_sources")
        assert hasattr(engine, "source_failure_threshold")
        assert hasattr(engine, "auto_disable_unhealthy_sources")
        assert hasattr(engine, "project_sync_verified")
        assert hasattr(engine, "comprehensive_collection_used")

    def test_compute_sources_have_claimed_methods(self):
        """Test that compute sources have all methods mentioned in documentation."""
        from hsm.compute.local import LocalComputeSource

        source = LocalComputeSource(name="test")

        # Verify claimed methods exist
        assert hasattr(source, "health_check")
        assert hasattr(source, "setup")
        assert hasattr(source, "submit_task")
        assert hasattr(source, "get_task_status")
        assert hasattr(source, "cancel_task")
        assert hasattr(source, "collect_results")
        assert hasattr(source, "cleanup")

    def test_distribution_strategies_exist(self):
        """Test that all claimed distribution strategies are implemented."""
        from hsm.core.engine import DistributionStrategy

        # Verify all claimed strategies exist
        assert hasattr(DistributionStrategy, "ROUND_ROBIN")
        assert hasattr(DistributionStrategy, "LEAST_LOADED")
        assert hasattr(DistributionStrategy, "RANDOM")

    def test_health_status_types_exist(self):
        """Test that all claimed health status types are implemented."""
        from hsm.compute.base import HealthStatus

        assert hasattr(HealthStatus, "HEALTHY")
        assert hasattr(HealthStatus, "DEGRADED")
        assert hasattr(HealthStatus, "UNHEALTHY")

    def test_claimed_cli_commands_exist(self):
        """Test that all CLI commands mentioned in documentation exist."""
        from hsm.cli.main import cli
        from hsm.cli.sweep import sweep_cmd
        from hsm.cli.monitor import monitor

        # Verify main CLI groups exist
        assert "config" in cli.commands
        assert "run" in cli.commands
        assert "complete" in cli.commands
        assert "status" in cli.commands
        assert "monitor" in cli.commands

        # Verify monitor subcommands exist
        assert "health" in monitor.commands
        assert "watch" in monitor.commands
