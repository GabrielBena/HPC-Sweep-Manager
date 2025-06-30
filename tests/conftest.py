"""
Pytest configuration and shared fixtures for HSM test suite.
"""

import asyncio
from pathlib import Path
import shutil
import tempfile

from click.testing import CliRunner
import pytest
import yaml


@pytest.fixture
def cli_runner():
    """Provide a Click CLI runner for testing CLI commands."""
    from rich.console import Console

    class TestCliRunner(CliRunner):
        def invoke(self, cli, args=None, **kwargs):
            # Provide a console object in the context if not already provided
            if "obj" not in kwargs:
                kwargs["obj"] = {"console": Console(file=None, force_terminal=False)}
            return super().invoke(cli, args, **kwargs)

    return TestCliRunner()


@pytest.fixture
def temp_dir():
    """Provide a temporary directory that's cleaned up after the test."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def simple_mlp_dir():
    """Provide path to the simple_mlp example directory."""
    project_root = Path(__file__).parent.parent
    simple_mlp_path = project_root / "examples" / "simple_mlp"
    if not simple_mlp_path.exists():
        pytest.skip("simple_mlp example not found")
    return simple_mlp_path


@pytest.fixture
def sample_sweep_config():
    """Provide a sample sweep configuration for testing."""
    return {
        "grid": {"learning_rate": [0.001, 0.01], "batch_size": [32, 64], "hidden_dim": [64, 128]},
        "metadata": {"description": "Test sweep", "tags": ["test"]},
    }


@pytest.fixture
def sample_hsm_config():
    """Provide a sample HSM configuration for testing."""
    return {
        "project": {"name": "test_project", "description": "Test project for HSM"},
        "paths": {"training_script": "main.py", "output_dir": "outputs", "conda_env": "test_env"},
        "compute": {"default_source": "local", "local": {"max_parallel": 4, "timeout": 3600}},
        "logging": {"level": "INFO", "file": "hsm.log"},
    }


@pytest.fixture
def sweep_config_file(temp_dir, sample_sweep_config):
    """Create a temporary sweep configuration file."""
    config_file = temp_dir / "sweep.yaml"
    with open(config_file, "w") as f:
        yaml.dump(sample_sweep_config, f)
    return config_file


@pytest.fixture
def hsm_config_file(temp_dir, sample_hsm_config):
    """Create a temporary HSM configuration file."""
    config_file = temp_dir / "hsm_config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(sample_hsm_config, f)
    return config_file


@pytest.fixture
def mock_project_structure(temp_dir):
    """Create a mock project structure for testing."""
    # Create main training script
    main_py = temp_dir / "main.py"
    main_py.write_text("""
import hydra
from omegaconf import DictConfig

@hydra.main(version_base=None, config_path="configs", config_name="config")
def main(cfg: DictConfig) -> None:
    print(f"Running with lr={cfg.training.lr}")
    # Simulate training
    import time
    time.sleep(0.1)
    print("Training completed")

if __name__ == "__main__":
    main()
""")

    # Create configs directory
    configs_dir = temp_dir / "configs"
    configs_dir.mkdir()

    # Create config.yaml
    config_yaml = configs_dir / "config.yaml"
    config_yaml.write_text("""
defaults:
  - _self_

seed: 42
training:
  lr: 0.001
  epochs: 10
model:
  hidden_dim: 64
""")

    # Create outputs directory
    outputs_dir = temp_dir / "outputs"
    outputs_dir.mkdir()

    return temp_dir


@pytest.fixture
def event_loop():
    """Create an event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_compute_source():
    """Provide a mock compute source for testing."""
    from unittest.mock import AsyncMock, MagicMock

    from hsm.compute.base import ComputeSource, HealthStatus, TaskStatus

    mock_source = AsyncMock(spec=ComputeSource)
    mock_source.setup.return_value = True
    mock_source.health_check.return_value = HealthStatus.HEALTHY
    mock_source.submit_task.return_value = MagicMock(
        task_id="test_task_001", status=TaskStatus.COMPLETED
    )
    mock_source.get_task_status.return_value = TaskStatus.COMPLETED
    mock_source.cancel_task.return_value = True
    mock_source.cleanup.return_value = True

    return mock_source


@pytest.fixture
def valid_validation_result():
    """Provide a valid ValidationResult for testing."""
    from hsm.config.validation import ValidationResult

    result = ValidationResult()
    return result


@pytest.fixture
def invalid_validation_result():
    """Provide an invalid ValidationResult for testing."""
    from hsm.config.validation import ValidationResult

    result = ValidationResult()
    result.add_error("Test error")
    result.add_warning("Test warning")
    return result


@pytest.fixture
def validation_result_with_warnings():
    """Provide a ValidationResult with warnings for testing."""
    from hsm.config.validation import ValidationResult

    result = ValidationResult()
    result.add_warning("Test warning")
    return result
