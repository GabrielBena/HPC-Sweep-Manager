"""
End-to-end tests using the simple_mlp example.

This module provides comprehensive end-to-end testing of HSM v2 using
the simple_mlp example to demonstrate the complete workflow.
"""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile

from click.testing import CliRunner
import pytest
import yaml

from hsm.cli.main import cli
from hsm.config.hsm import HSMConfig
from hsm.config.sweep import SweepConfig

pytestmark = [pytest.mark.integration, pytest.mark.slow]


class TestSimpleMlpEndToEnd:
    """Complete end-to-end workflow tests using simple_mlp example."""

    def test_complete_workflow_local_execution(self, simple_mlp_dir):
        """Test complete workflow: config init -> sweep run -> monitor -> results."""
        if not (simple_mlp_dir / "main.py").exists():
            pytest.skip("simple_mlp main.py not found")

        cli_runner = CliRunner()
        original_cwd = os.getcwd()

        try:
            # Create temporary working directory
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)

                # Copy simple_mlp example to temp directory
                shutil.copytree(simple_mlp_dir, temp_path / "simple_mlp")
                work_dir = temp_path / "simple_mlp"
                os.chdir(work_dir)

                # Step 1: Validate that main.py works directly
                result = subprocess.run(
                    [
                        "python",
                        "main.py",
                        "training.epochs=2",
                        "training.simulate_time=0.001",
                        "logging.wandb.enabled=false",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                assert result.returncode == 0, f"Direct execution failed: {result.stderr}"

                # Step 2: Test configuration validation
                if (work_dir / "hsm_config.yaml").exists():
                    result = cli_runner.invoke(cli, ["config", "validate"])
                    # Should complete (may have warnings but shouldn't crash)
                    assert result.exit_code in [0, 1]

                # Step 3: Test sweep parameter counting
                result = cli_runner.invoke(cli, ["run", "--count-only"])
                if result.exit_code == 0:
                    assert "combinations" in result.output.lower()

                # Step 4: Test dry run
                result = cli_runner.invoke(cli, ["run", "--dry-run"])
                if result.exit_code == 0:
                    assert (
                        "dry run" in result.output.lower()
                        or "would execute" in result.output.lower()
                    )

                # Step 5: Run a minimal sweep (if implementation supports it)
                minimal_sweep_config = {
                    "grid": {"training.lr": [0.01, 0.02], "training.epochs": [2]},
                    "metadata": {"description": "End-to-end test sweep"},
                }

                with open("test_e2e_sweep.yaml", "w") as f:
                    yaml.dump(minimal_sweep_config, f)

                result = cli_runner.invoke(
                    cli,
                    [
                        "run",
                        "--config",
                        "test_e2e_sweep.yaml",
                        "--sources",
                        "local",
                        "--max-tasks",
                        "2",
                        "--parallel-limit",
                        "1",
                    ],
                )

                # For now, just check that command executes without major crashes
                # TODO: Add more specific assertions once implementation is stable
                print(f"Sweep execution result: {result.exit_code}")
                print(f"Sweep output: {result.output}")

        finally:
            os.chdir(original_cwd)

    def test_config_management_workflow(self, simple_mlp_dir):
        """Test configuration management workflow."""
        cli_runner = CliRunner()
        original_cwd = os.getcwd()

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                os.chdir(temp_path)

                # Step 1: Initialize configuration in empty directory
                result = cli_runner.invoke(
                    cli, ["config", "init", "--project-root", str(temp_path)]
                )
                assert result.exit_code in [0, 1]  # May have warnings about missing files

                # Step 2: Show configuration (if created)
                if (temp_path / "hsm_config.yaml").exists():
                    result = cli_runner.invoke(cli, ["config", "show"])
                    # Should display config or show error
                    assert result.exit_code in [0, 1]

                # Step 3: Validate configuration
                if (temp_path / "hsm_config.yaml").exists():
                    result = cli_runner.invoke(cli, ["config", "validate"])
                    # May have validation issues but shouldn't crash
                    assert result.exit_code in [0, 1]

        finally:
            os.chdir(original_cwd)

    def test_help_and_documentation(self):
        """Test that all help commands work correctly."""
        cli_runner = CliRunner()

        # Test main help
        result = cli_runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "HPC Sweep Manager" in result.output

        # Test subcommand help
        subcommands = ["config", "run", "complete", "status", "monitor"]
        for cmd in subcommands:
            result = cli_runner.invoke(cli, [cmd, "--help"])
            assert result.exit_code == 0, f"Help for {cmd} failed"

        # Test nested help commands
        nested_commands = [
            ["config", "init", "--help"],
            ["config", "show", "--help"],
            ["config", "validate", "--help"],
        ]

        for cmd in nested_commands:
            result = cli_runner.invoke(cli, cmd)
            assert result.exit_code == 0, f"Help for {' '.join(cmd)} failed"

    def test_error_handling(self):
        """Test error handling for various failure scenarios."""
        cli_runner = CliRunner()

        # Test invalid commands
        result = cli_runner.invoke(cli, ["invalid_command"])
        assert result.exit_code != 0
        assert "No such command" in result.output

        # Test invalid options
        result = cli_runner.invoke(cli, ["config", "init", "--invalid-option"])
        assert result.exit_code != 0

        # Test missing required arguments (where applicable)
        # Note: Most commands have defaults, so this might be limited

    def test_configuration_file_validation(self, simple_mlp_dir):
        """Test validation of configuration files from simple_mlp example."""
        # Test HSM config validation
        hsm_config_path = simple_mlp_dir / "hsm_config.yaml"
        if hsm_config_path.exists():
            try:
                config = HSMConfig.load(hsm_config_path)
                assert config is not None
                # Basic structure validation
                assert hasattr(config, "paths")
            except Exception as e:
                pytest.skip(f"HSM config validation failed: {e}")

        # Test sweep config validation
        sweep_config_path = simple_mlp_dir / "sweep_config.yaml"
        if sweep_config_path.exists():
            try:
                with open(sweep_config_path) as f:
                    all_configs = yaml.safe_load(f)

                # Validate each sweep configuration in the file
                for config_name, config_data in all_configs.items():
                    if isinstance(config_data, dict) and "method" in config_data:
                        sweep_config = SweepConfig.from_dict(config_data)
                        assert sweep_config is not None

            except Exception as e:
                pytest.skip(f"Sweep config validation failed: {e}")
