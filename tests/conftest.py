"""Pytest configuration and fixtures for HSM testing."""

from dataclasses import dataclass
import json
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

    # Create HSM config. No `hpc:` block — its `default_walltime` /
    # `default_resources` fields used to silently override the typed
    # `local:` / `slurm:` blocks; both are now gone.
    hsm_config = {
        "paths": {
            "python_interpreter": sys.executable,
            "train_script": str(project_dir / "train.py"),
            "config_dir": str(project_dir / "configs"),
        },
        "slurm": {
            "walltime": "01:00:00",
            "cpus_per_task": 2,
            "mem": "4gb",
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


# -----------------------------------------------------------------------------
# Fake Slurm cluster fixture (PATH-stub scheduler)
# -----------------------------------------------------------------------------
# Pre-prepares a temp ``bin/`` directory containing executable Python shims for
# ``sbatch``, ``squeue``, ``scancel`` and ``sinfo``, then prepends it to PATH
# for the duration of a test. Tests that exercise any code path which shells
# out to Slurm should request the ``fake_slurm`` fixture.
#
# Each call yields a fresh, isolated cluster — state files live under
# ``tmp_path / "fake_slurm_state"`` so concurrent tests cannot collide.


_FAKE_SLURM_FIXTURES_DIR = Path(__file__).parent / "fixtures" / "fake_slurm"
_FAKE_SLURM_STUBS = ("sbatch", "squeue", "scancel", "sinfo")


@dataclass
class FakeSlurm:
    """Handle into the running fake-Slurm fixture.

    Attributes
    ----------
    state_dir
        Directory where the fake stubs read/write their jobs.jsonl + counter
        files.
    bin_dir
        Directory containing the stub executables that's prepended to PATH.
    """

    state_dir: Path
    bin_dir: Path

    def jobs(self) -> list[dict]:
        """Read every recorded submission (regardless of current state)."""
        path = self.state_dir / "jobs.jsonl"
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]

    def set_pending_seconds(self, seconds: float) -> None:
        """Tune the PENDING duration for state transitions in this test."""
        os.environ["HSM_FAKE_PENDING_S"] = str(seconds)

    def set_running_seconds(self, seconds: float) -> None:
        """Tune the RUNNING duration for state transitions in this test."""
        os.environ["HSM_FAKE_RUNNING_S"] = str(seconds)


@pytest.fixture
def fake_slurm(tmp_path, monkeypatch) -> FakeSlurm:
    """Provide a PATH-stubbed Slurm cluster for the duration of a test.

    Defaults: jobs transition PENDING -> RUNNING after 0.2 s, then
    RUNNING -> COMPLETED after another 0.3 s. Tune via
    ``fake_slurm.set_pending_seconds`` / ``set_running_seconds``.
    """
    bin_dir = tmp_path / "fake_slurm_bin"
    bin_dir.mkdir()
    state_dir = tmp_path / "fake_slurm_state"
    state_dir.mkdir()

    for tool in _FAKE_SLURM_STUBS:
        src_path = _FAKE_SLURM_FIXTURES_DIR / tool
        dst_path = bin_dir / tool
        dst_path.write_text(src_path.read_text())
        dst_path.chmod(0o755)

    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ.get('PATH', '')}")
    monkeypatch.setenv("HSM_FAKE_STATE_DIR", str(state_dir))
    monkeypatch.setenv("HSM_FAKE_PENDING_S", "0.2")
    monkeypatch.setenv("HSM_FAKE_RUNNING_S", "0.3")

    return FakeSlurm(state_dir=state_dir, bin_dir=bin_dir)


# -----------------------------------------------------------------------------
# Fake nvidia-smi fixture (for LocalComputeSource GPU detection tests)
# -----------------------------------------------------------------------------

_FAKE_NVIDIA_SMI_FIXTURE = (
    Path(__file__).parent / "fixtures" / "fake_nvidia_smi" / "nvidia-smi"
)


@dataclass
class FakeGPUs:
    bin_dir: Path
    _monkeypatch: object  # pytest's monkeypatch fixture; intentionally untyped to dodge import

    def set_count(self, n: int) -> None:
        """Configure how many GPUs the stub nvidia-smi will report."""
        self._monkeypatch.setenv("HSM_FAKE_GPU_COUNT", str(n))

    def disable(self) -> None:
        """Make nvidia-smi behave as if there are no GPUs (exits non-zero)."""
        self._monkeypatch.setenv("HSM_FAKE_GPU_COUNT", "0")


@pytest.fixture
def fake_gpus(tmp_path, monkeypatch) -> FakeGPUs:
    """Provide a PATH-stubbed ``nvidia-smi`` returning a configurable GPU count.

    Default: 4 GPUs visible. Call ``fake_gpus.set_count(n)`` to change, or
    ``fake_gpus.disable()`` to simulate a no-GPU host.
    """
    bin_dir = tmp_path / "fake_nvidia_smi_bin"
    bin_dir.mkdir()
    dst = bin_dir / "nvidia-smi"
    dst.write_text(_FAKE_NVIDIA_SMI_FIXTURE.read_text())
    dst.chmod(0o755)

    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ.get('PATH', '')}")
    monkeypatch.setenv("HSM_FAKE_GPU_COUNT", "4")
    return FakeGPUs(bin_dir=bin_dir, _monkeypatch=monkeypatch)


@pytest.fixture
def no_gpus(monkeypatch, tmp_path) -> None:
    """Force GPU detection to fail by shadowing nvidia-smi with a failing stub."""
    bin_dir = tmp_path / "no_nvidia_smi_bin"
    bin_dir.mkdir()
    stub = bin_dir / "nvidia-smi"
    stub.write_text("#!/bin/sh\nexit 1\n")
    stub.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ.get('PATH', '')}")
