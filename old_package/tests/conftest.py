"""Pytest configuration and fixtures for HSM testing."""

import os
from pathlib import Path
import shutil
import sys
import tempfile
from unittest.mock import MagicMock

import pytest
import yaml

# Add src to Python path for testing
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from hpc_sweep_manager.core.common.config import HSMConfig


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def mock_project_dir(temp_dir):
    """Create a mock project directory structure."""
    project_dir = temp_dir / "test_project"
    project_dir.mkdir()

    # Create basic project structure
    (project_dir / "train.py").write_text("""
import argparse
import time
import random

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("overrides", nargs="*")
    args = parser.parse_args()
    
    print(f"Training with args: {args.overrides}")
    time.sleep(random.uniform(1, 3))  # Simulate training
    print("Training completed successfully")
""")

    # Create sweeps directory
    sweeps_dir = project_dir / "sweeps"
    sweeps_dir.mkdir()

    # Create basic sweep config
    sweep_config = {"sweep": {"grid": {"lr": [0.001, 0.01], "batch_size": [16, 32]}}}
    with open(sweeps_dir / "sweep.yaml", "w") as f:
        yaml.dump(sweep_config, f)

    # Create HSM config
    hsm_config = {
        "paths": {
            "python_interpreter": sys.executable,
            "train_script": str(project_dir / "train.py"),
            "config_dir": str(project_dir / "configs"),
        },
        "local": {"max_parallel_jobs": 2, "timeout": 30},
        "hpc": {
            "default_walltime": "01:00:00",
            "default_resources": "select=1:ncpus=2:mem=4gb",
        },
    }
    with open(sweeps_dir / "hsm_config.yaml", "w") as f:
        yaml.dump(hsm_config, f)

    return project_dir


@pytest.fixture
def sample_params():
    """Sample parameter combinations for testing."""
    return [
        {"lr": 0.001, "batch_size": 16},
        {"lr": 0.001, "batch_size": 32},
        {"lr": 0.01, "batch_size": 16},
        {"lr": 0.01, "batch_size": 32},
    ]


@pytest.fixture
def mock_hsm_config(mock_project_dir):
    """Mock HSM configuration."""
    config_path = mock_project_dir / "sweeps" / "hsm_config.yaml"
    return HSMConfig.from_file(config_path)


@pytest.fixture
def mock_subprocess():
    """Mock subprocess for testing shell commands."""
    mock = MagicMock()
    mock.returncode = 0
    mock.stdout = "mock output"
    mock.stderr = ""
    return mock


@pytest.fixture
def mock_hpc_commands(monkeypatch):
    """Mock HPC commands (qsub, qstat, sbatch, squeue)."""

    def mock_which(cmd):
        if cmd in ["qsub", "qstat", "sbatch", "squeue", "sinfo"]:
            return "/usr/bin/" + cmd
        return None

    def mock_run(*args, **kwargs):
        mock = MagicMock()
        mock.returncode = 0
        cmd_str = str(args[0])

        if "qsub" in cmd_str and "--version" not in cmd_str:
            mock.stdout = "12345.cluster"  # Mock job ID
        elif "qstat" in cmd_str:
            if "--version" in cmd_str:
                mock.stdout = "qstat version 18.1.4"
            else:
                mock.stdout = "12345.cluster Q normal"  # Mock queued status
        elif "sbatch" in cmd_str and "--version" not in cmd_str:
            mock.stdout = "Submitted batch job 67890"
        elif "squeue" in cmd_str:
            mock.stdout = "67890 RUNNING"
        elif "sinfo" in cmd_str and "--version" in cmd_str:
            mock.stdout = "slurm 20.11.7"
        else:
            mock.stdout = "mock command output"
        mock.stderr = ""
        return mock

    monkeypatch.setattr("shutil.which", mock_which)
    monkeypatch.setattr("subprocess.run", mock_run)


@pytest.fixture
def mock_ssh(monkeypatch):
    """Mock SSH connections for remote testing."""
    mock_client = MagicMock()

    def mock_exec_command(command):
        stdout = MagicMock()
        stderr = MagicMock()

        if "python" in command and "--version" in command:
            stdout.read.return_value = b"Python 3.8.5\n"
        elif "which python" in command:
            stdout.read.return_value = b"/usr/bin/python\n"
        elif "ls" in command:
            stdout.read.return_value = b"file1.txt\nfile2.txt\n"
        else:
            stdout.read.return_value = b"mock ssh output\n"

        stderr.read.return_value = b""
        return None, stdout, stderr

    mock_client.exec_command = mock_exec_command

    def mock_connect(*args, **kwargs):
        return mock_client

    monkeypatch.setattr("paramiko.SSHClient.connect", mock_connect)
    monkeypatch.setattr("paramiko.SSHClient", lambda: mock_client)


@pytest.fixture
def cli_runner():
    """Click CLI test runner."""
    from click.testing import CliRunner

    return CliRunner()


@pytest.fixture
def mock_wandb(monkeypatch):
    """Mock W&B for testing."""
    mock_run = MagicMock()
    mock_run.id = "test_run_123"
    mock_run.name = "test_run"
    mock_run.url = "https://wandb.ai/test/test/runs/test_run_123"

    def mock_init(*args, **kwargs):
        return mock_run

    monkeypatch.setattr("wandb.init", mock_init)
    monkeypatch.setattr("wandb.log", MagicMock())
    monkeypatch.setattr("wandb.finish", MagicMock())


@pytest.fixture
def sample_job_outputs(temp_dir):
    """Create sample job output files for testing result collection."""
    sweep_dir = temp_dir / "test_sweep"
    sweep_dir.mkdir()

    # Create task directories with outputs
    for i in range(4):
        task_dir = sweep_dir / "tasks" / f"task_{i + 1:03d}"
        task_dir.mkdir(parents=True)

        # Create task info file
        task_info = {
            "task_id": i + 1,
            "parameters": {"lr": 0.001 * (i + 1), "batch_size": 16 * (i + 1)},
            "start_time": "2024-01-15T10:00:00",
            "end_time": "2024-01-15T10:30:00",
            "status": "completed",
        }
        with open(task_dir / "task_info.json", "w") as f:
            import json

            json.dump(task_info, f)

        # Create sample results
        results = {
            "final_accuracy": 0.85 + i * 0.02,
            "final_loss": 0.5 - i * 0.05,
            "epochs": 10,
            "runtime": 1800 + i * 300,
        }
        with open(task_dir / "results.json", "w") as f:
            import json

            json.dump(results, f)

        # Create log file
        log_content = f"""
Starting training for task {i + 1}
Epoch 1/10: loss=0.8, acc=0.6
Epoch 5/10: loss=0.6, acc=0.75
Epoch 10/10: loss={results["final_loss"]}, acc={results["final_accuracy"]}
Training completed successfully
"""
        (task_dir / "output.log").write_text(log_content)

    return sweep_dir


@pytest.fixture(autouse=True)
def clean_environment():
    """Clean environment variables before each test."""
    # Store original values
    original_env = {}
    hsm_vars = [k for k in os.environ.keys() if k.startswith("HSM_")]
    for var in hsm_vars:
        original_env[var] = os.environ[var]
        del os.environ[var]

    yield

    # Restore original values
    for var, value in original_env.items():
        os.environ[var] = value


@pytest.fixture
def mock_file_system(monkeypatch):
    """Mock file system operations for testing."""

    def mock_exists(path):
        # Mock existence of common files
        path_str = str(path)
        return bool(any(x in path_str for x in ["train.py", "hsm_config.yaml", "sweep.yaml"]))

    def mock_is_file(path):
        return mock_exists(path)

    def mock_is_dir(path):
        path_str = str(path)
        return any(x in path_str for x in ["sweeps", "outputs", "logs", "scripts"])

    monkeypatch.setattr("pathlib.Path.exists", mock_exists)
    monkeypatch.setattr("pathlib.Path.is_file", mock_is_file)
    monkeypatch.setattr("pathlib.Path.is_dir", mock_is_dir)


# Test markers
pytest.mark.unit = pytest.mark.unit
pytest.mark.integration = pytest.mark.integration
pytest.mark.cli = pytest.mark.cli
pytest.mark.slow = pytest.mark.slow


# Force asyncio only - disable trio for all tests
@pytest.fixture
def anyio_backend():
    """Force tests to use asyncio backend only."""
    return "asyncio"
