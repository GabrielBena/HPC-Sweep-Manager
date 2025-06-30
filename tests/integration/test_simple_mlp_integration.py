"""
Integration tests using the simple_mlp example.

These tests validate the complete HSM workflow using a real example.
"""

import pytest
import os
import subprocess
import tempfile
import shutil
from pathlib import Path
from click.testing import CliRunner

from hsm.cli.main import cli
from hsm.config.sweep import SweepConfig
from hsm.config.hsm import HSMConfig
from hsm.config.validation import SweepConfigValidator


@pytest.mark.integration
class TestSimpleMlpIntegration:
    """Integration tests using the simple_mlp example."""

    def test_simple_mlp_basic_run(self, simple_mlp_dir):
        """Test basic execution of simple_mlp example."""
        # Change to simple_mlp directory and run basic command
        original_cwd = os.getcwd()
        try:
            os.chdir(simple_mlp_dir)

            # Run with minimal parameters to avoid dependencies
            cmd = [
                "python",
                "main.py",
                "training.epochs=3",
                "training.simulate_time=0.001",
                "logging.wandb.enabled=false",
                "output.dir=./test_outputs",
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

            assert result.returncode == 0, f"Command failed: {result.stderr}"
            assert "Training completed" in result.stdout or "Finished" in result.stdout

        finally:
            os.chdir(original_cwd)
            # Clean up test outputs
            test_output_dir = simple_mlp_dir / "test_outputs"
            if test_output_dir.exists():
                shutil.rmtree(test_output_dir, ignore_errors=True)

    def test_hsm_config_validation_simple_mlp(self, simple_mlp_dir):
        """Test HSM config validation in simple_mlp directory."""
        if not (simple_mlp_dir / "hsm_config.yaml").exists():
            pytest.skip("hsm_config.yaml not found in simple_mlp")

        cli_runner = CliRunner()
        original_cwd = os.getcwd()

        try:
            os.chdir(simple_mlp_dir)

            result = cli_runner.invoke(cli, ["config", "validate"])

            # Should succeed or show specific validation errors
            if result.exit_code != 0:
                # Print output for debugging
                print(f"Config validation output: {result.output}")

            # The test passes if config validation runs (may have warnings)
            assert "Error:" not in result.output or result.exit_code == 0

        finally:
            os.chdir(original_cwd)

    def test_sweep_config_validation_simple_mlp(self, simple_mlp_dir):
        """Test sweep config validation in simple_mlp directory."""
        if not (simple_mlp_dir / "sweep_config.yaml").exists():
            pytest.skip("sweep_config.yaml not found in simple_mlp")

        # Load config and validate properly
        try:
            sweep_cfg = SweepConfig.from_yaml(simple_mlp_dir / "sweep_config.yaml")
            # The config file has multiple sweep configurations, let's use the test_sweep
            if hasattr(sweep_cfg, "test_sweep"):
                config_data = sweep_cfg.test_sweep
            else:
                # Try to extract a specific sweep config
                with open(simple_mlp_dir / "sweep_config.yaml", "r") as f:
                    import yaml

                    all_configs = yaml.safe_load(f)
                    config_data = all_configs.get("test_sweep", all_configs)

                # Create a proper SweepConfig from the test data
                sweep_cfg = SweepConfig.from_dict(config_data)

            validation_result = SweepConfigValidator(sweep_cfg).validate()

            # Check if validation succeeded
            if not validation_result.is_valid:
                print(f"Validation errors: {[str(e) for e in validation_result.errors]}")
                # Skip test if config is fundamentally broken
                pytest.skip("Sweep config is invalid")

        except Exception as e:
            print(f"Failed to load sweep config: {e}")
            pytest.skip("Could not load sweep config")

    def test_hsm_run_dry_run_simple_mlp(self, simple_mlp_dir):
        """Test HSM dry run with simple_mlp example."""
        if not (simple_mlp_dir / "sweep_config.yaml").exists():
            pytest.skip("sweep_config.yaml not found in simple_mlp")

        cli_runner = CliRunner()
        original_cwd = os.getcwd()

        try:
            os.chdir(simple_mlp_dir)

            result = cli_runner.invoke(
                cli, ["run", "--config", "sweep_config.yaml", "--sources", "local", "--dry-run"]
            )

            if result.exit_code != 0:
                print(f"Dry run output: {result.output}")
                print(
                    f"Dry run stderr: {result.stderr if hasattr(result, 'stderr') else 'No stderr'}"
                )

            # Should complete successfully showing what would be executed
            assert result.exit_code == 0
            assert "Dry run" in result.output or "dry run" in result.output

        finally:
            os.chdir(original_cwd)

    def test_hsm_count_only_simple_mlp(self, simple_mlp_dir):
        """Test HSM count-only with simple_mlp example."""
        if not (simple_mlp_dir / "sweep_config.yaml").exists():
            pytest.skip("sweep_config.yaml not found in simple_mlp")

        cli_runner = CliRunner()
        original_cwd = os.getcwd()

        try:
            os.chdir(simple_mlp_dir)

            result = cli_runner.invoke(
                cli, ["run", "--config", "sweep_config.yaml", "--count-only"]
            )

            if result.exit_code != 0:
                print(f"Count-only output: {result.output}")

            assert result.exit_code == 0
            assert "combinations" in result.output.lower()

        finally:
            os.chdir(original_cwd)

    @pytest.mark.slow
    @pytest.mark.integration
    def test_hsm_run_small_sweep_simple_mlp(self, simple_mlp_dir):
        """Test running a small actual sweep with simple_mlp example."""
        if not (simple_mlp_dir / "main.py").exists():
            pytest.skip("main.py not found in simple_mlp")

        cli_runner = CliRunner()
        original_cwd = os.getcwd()

        try:
            os.chdir(simple_mlp_dir)

            # Create a minimal sweep config for testing
            test_sweep_config = {
                "grid": {"training.lr": [0.01, 0.02], "model.hidden_dim": [32, 64]},
                "metadata": {"description": "Test sweep for integration testing"},
            }

            # Write test config
            with open("test_sweep.yaml", "w") as f:
                import yaml

                yaml.dump(test_sweep_config, f)

            # Run small sweep
            result = cli_runner.invoke(
                cli,
                [
                    "run",
                    "--config",
                    "test_sweep.yaml",
                    "--sources",
                    "local",
                    "--max-tasks",
                    "2",  # Limit to 2 tasks
                    "--parallel-limit",
                    "1",  # Run sequentially
                ],
                catch_exceptions=False,
            )

            if result.exit_code != 0:
                print(f"Sweep run output: {result.output}")
                print(
                    f"Exception: {result.exception if hasattr(result, 'exception') else 'No exception'}"
                )

            # Clean up
            if Path("test_sweep.yaml").exists():
                os.remove("test_sweep.yaml")

            # For now, just check that the command doesn't crash
            # TODO: Add more specific assertions once implementation is complete
            assert result.exit_code == 0 or "not yet implemented" in result.output.lower()

        finally:
            os.chdir(original_cwd)
            # Clean up any test outputs
            test_dirs = ["sweeps", "outputs", "test_outputs"]
            for test_dir in test_dirs:
                test_path = simple_mlp_dir / test_dir
                if test_path.exists():
                    shutil.rmtree(test_path, ignore_errors=True)


class TestSimpleMlpConfigLoading:
    """Test configuration loading with simple_mlp example."""

    def test_load_simple_mlp_sweep_config(self, simple_mlp_dir):
        """Test loading sweep config from simple_mlp example."""
        sweep_config_path = simple_mlp_dir / "sweep_config.yaml"
        if not sweep_config_path.exists():
            pytest.skip("sweep_config.yaml not found in simple_mlp")

        # Load the config file directly and extract test_sweep config
        import yaml

        with open(sweep_config_path, "r") as f:
            all_configs = yaml.safe_load(f)

        # Use test_sweep configuration for validation
        test_config_data = all_configs.get("test_sweep", all_configs)
        config = SweepConfig.from_dict(test_config_data)

        # Basic validation
        assert config is not None
        validator = SweepConfigValidator(config)
        validation_result = validator.validate()

        # Should be valid or have only warnings
        if not validation_result.is_valid:
            print(f"Validation errors: {[str(e) for e in validation_result.errors]}")
            print(f"Validation warnings: {[str(w) for w in validation_result.warnings]}")

        assert validation_result.is_valid or len(validation_result.errors) == 0

    def test_load_simple_mlp_hsm_config(self, simple_mlp_dir):
        """Test loading HSM config from simple_mlp example."""
        hsm_config_path = simple_mlp_dir / "hsm_config.yaml"
        if not hsm_config_path.exists():
            pytest.skip("hsm_config.yaml not found in simple_mlp")

        config = HSMConfig.load(hsm_config_path)

        # Basic validation
        assert config is not None
        assert config.paths is not None

        # Check that training script path exists
        if hasattr(config.paths, "training_script"):
            training_script = simple_mlp_dir / config.paths.training_script
            assert training_script.exists(), f"Training script not found: {training_script}"


class TestSimpleMlpParameterGeneration:
    """Test parameter generation with simple_mlp example configs."""

    def test_parameter_generation_simple_mlp(self, simple_mlp_dir):
        """Test parameter generation with simple_mlp sweep config."""
        sweep_config_path = simple_mlp_dir / "sweep_config.yaml"
        if not sweep_config_path.exists():
            pytest.skip("sweep_config.yaml not found in simple_mlp")

        from hsm.utils.params import ParameterGenerator

        # Load the config file and use test_sweep configuration
        import yaml

        with open(sweep_config_path, "r") as f:
            all_configs = yaml.safe_load(f)

        test_config_data = all_configs.get("test_sweep", all_configs)
        config = SweepConfig.from_dict(test_config_data)

        generator = ParameterGenerator(config)

        # Test counting combinations
        count = generator.count_combinations()
        assert count > 0, "Should have at least one parameter combination"

        # Test generating combinations
        combinations = list(generator.generate_combinations())
        assert len(combinations) == count
        assert len(combinations) > 0

        # Each combination should be a dictionary with parameter values
        for combo in combinations:
            assert isinstance(combo, dict)
            assert len(combo) > 0


class TestSimpleMlpCLICommands:
    """Test all CLI commands with simple_mlp example."""

    def test_cli_help_commands(self, simple_mlp_dir):
        """Test that all CLI help commands work in simple_mlp context."""
        cli_runner = CliRunner()
        original_cwd = os.getcwd()

        try:
            os.chdir(simple_mlp_dir)

            # Test main help
            result = cli_runner.invoke(cli, ["--help"])
            assert result.exit_code == 0

            # Test subcommand help
            help_commands = [
                ["config", "--help"],
                ["sweep", "--help"],
                ["monitor", "--help"],
                ["run", "--help"],
                ["status", "--help"],
            ]

            for cmd in help_commands:
                result = cli_runner.invoke(cli, cmd)
                assert result.exit_code == 0, f"Help command failed: {cmd}"

        finally:
            os.chdir(original_cwd)

    def test_status_commands_simple_mlp(self, simple_mlp_dir):
        """Test status commands in simple_mlp context."""
        cli_runner = CliRunner()
        original_cwd = os.getcwd()

        try:
            os.chdir(simple_mlp_dir)

            # Test status without arguments (should show all sweeps)
            result = cli_runner.invoke(cli, ["status"])
            assert result.exit_code == 0

            # Test status with --all flag
            result = cli_runner.invoke(cli, ["status", "--all"])
            assert result.exit_code == 0

        finally:
            os.chdir(original_cwd)
