"""
Documentation validation tests for simple_mlp example.

This module ensures that the simple_mlp example works as documented
in the getting started guide and README files.
"""

import os
import pytest
import subprocess
from pathlib import Path
from click.testing import CliRunner

pytestmark = pytest.mark.integration


class TestSimpleMlpDocumentation:
    """Validate that simple_mlp works as documented."""

    def test_readme_quick_start(self, simple_mlp_dir):
        """Test the quick start command from the README."""
        original_cwd = os.getcwd()

        try:
            os.chdir(simple_mlp_dir)

            # From README: python main.py
            result = subprocess.run(
                [
                    "python",
                    "main.py",
                    "training.epochs=3",
                    "training.simulate_time=0.001",
                    "logging.wandb.enabled=false",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )

            assert result.returncode == 0, f"Quick start failed: {result.stderr}"
            assert "Training completed" in result.stdout or "Finished" in result.stdout

        finally:
            os.chdir(original_cwd)

    def test_parameter_override_examples(self, simple_mlp_dir):
        """Test parameter override examples from documentation."""
        original_cwd = os.getcwd()

        try:
            os.chdir(simple_mlp_dir)

            # Test various parameter combinations as shown in docs
            test_cases = [
                ["training.lr=0.01", "model.hidden_dim=128"],
                ["seed=123", "training.epochs=5"],
                ["model.input_dim=10", "model.output_dim=1"],
            ]

            for params in test_cases:
                cmd = (
                    ["python", "main.py"]
                    + params
                    + [
                        "training.epochs=2",  # Keep it fast
                        "training.simulate_time=0.001",
                        "logging.wandb.enabled=false",
                    ]
                )

                result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                assert result.returncode == 0, (
                    f"Parameter override failed for {params}: {result.stderr}"
                )

        finally:
            os.chdir(original_cwd)

    def test_hydra_features(self, simple_mlp_dir):
        """Test Hydra features mentioned in documentation."""
        original_cwd = os.getcwd()

        try:
            os.chdir(simple_mlp_dir)

            # Test --help option
            result = subprocess.run(
                ["python", "main.py", "--help"], capture_output=True, text=True, timeout=30
            )
            assert result.returncode == 0
            assert "Hydra" in result.stdout or "Config" in result.stdout

            # Test --cfg job option to show configuration
            result = subprocess.run(
                ["python", "main.py", "--cfg", "job"], capture_output=True, text=True, timeout=30
            )
            assert result.returncode == 0
            assert "seed:" in result.stdout or "training:" in result.stdout

        finally:
            os.chdir(original_cwd)

    def test_requirements_work(self, simple_mlp_dir):
        """Test that requirements.txt is installable and example works."""
        requirements_file = simple_mlp_dir / "requirements.txt"

        if not requirements_file.exists():
            pytest.skip("requirements.txt not found")

        # Read requirements
        with open(requirements_file, "r") as f:
            requirements = f.read().strip()

        # Should contain essential packages
        assert "hydra-core" in requirements or "omegaconf" in requirements

        # Test that the example works with minimal dependencies
        original_cwd = os.getcwd()
        try:
            os.chdir(simple_mlp_dir)

            result = subprocess.run(
                [
                    "python",
                    "main.py",
                    "training.epochs=1",
                    "training.simulate_time=0.001",
                    "logging.wandb.enabled=false",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )

            assert result.returncode == 0, f"Example with requirements failed: {result.stderr}"

        finally:
            os.chdir(original_cwd)

    def test_config_file_structure(self, simple_mlp_dir):
        """Test that config files match documented structure."""
        configs_dir = simple_mlp_dir / "configs"
        assert configs_dir.exists(), "configs directory should exist"

        config_file = configs_dir / "config.yaml"
        assert config_file.exists(), "config.yaml should exist in configs/"

        # Read and validate basic structure
        import yaml

        with open(config_file, "r") as f:
            config = yaml.safe_load(f)

        # Should have basic sections mentioned in docs
        expected_sections = ["seed", "training", "model"]
        for section in expected_sections:
            assert section in config, f"Config should contain {section} section"

    def test_sweep_config_examples(self, simple_mlp_dir):
        """Test that sweep_config.yaml contains documented examples."""
        sweep_config_file = simple_mlp_dir / "sweep_config.yaml"

        if not sweep_config_file.exists():
            pytest.skip("sweep_config.yaml not found")

        import yaml

        with open(sweep_config_file, "r") as f:
            all_configs = yaml.safe_load(f)

        # Should contain example sweep types mentioned in docs
        expected_sweeps = ["grid_sweep", "random_sweep", "test_sweep"]
        for sweep_type in expected_sweeps:
            if sweep_type in all_configs:
                sweep_config = all_configs[sweep_type]
                assert "method" in sweep_config or "parameters" in sweep_config

        # Test that test_sweep is reasonable for testing
        if "test_sweep" in all_configs:
            test_sweep = all_configs["test_sweep"]
            assert "parameters" in test_sweep
            # Should be small for testing
            param_count = 1
            for param, values in test_sweep["parameters"].items():
                param_count *= len(values["values"])
            assert param_count <= 20, "test_sweep should be small for quick testing"

    def test_output_directory_structure(self, simple_mlp_dir):
        """Test that output directory structure matches documentation."""
        original_cwd = os.getcwd()

        try:
            os.chdir(simple_mlp_dir)

            # Run example and check output structure
            result = subprocess.run(
                [
                    "python",
                    "main.py",
                    "training.epochs=2",
                    "training.simulate_time=0.001",
                    "logging.wandb.enabled=false",
                    "output.save_results=true",
                    "output.dir=./test_output",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )

            assert result.returncode == 0

            # Check that output directory was created
            output_dir = Path("test_output")
            if output_dir.exists():
                # Should contain results file
                results_file = output_dir / "results.txt"
                if results_file.exists():
                    content = results_file.read_text()
                    assert "Final Loss:" in content
                    assert "Best Loss:" in content

                # Clean up
                import shutil

                shutil.rmtree(output_dir, ignore_errors=True)

        finally:
            os.chdir(original_cwd)
