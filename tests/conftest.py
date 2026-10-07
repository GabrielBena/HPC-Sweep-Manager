"""Pytest configuration and fixtures for HSM testing."""

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

# Add src to Python path for testing
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


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
_FAKE_SLURM_STUBS = ("sbatch", "squeue", "scancel", "sinfo", "sacct")


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

    def mark_failed(self, job_id: str) -> None:
        """Force a submitted job to a sticky FAILED state.

        Rewrites its jobs.jsonl record so the fake squeue drops it from the
        queue (terminal) and the fake sacct reports FAILED — reproducing the
        field-report scenario where a job leaves the queue having failed.
        """
        path = self.state_dir / "jobs.jsonl"
        if not path.exists():
            return
        records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        for r in records:
            if r["id"] == str(job_id):
                r["state"] = "FAILED"
        path.write_text("".join(json.dumps(r) + "\n" for r in records))


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

_FAKE_NVIDIA_SMI_FIXTURE = Path(__file__).parent / "fixtures" / "fake_nvidia_smi" / "nvidia-smi"


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

    def set_busy(self, *indices: int) -> None:
        """Make these GPUs report memory in use and high utilisation (a co-tenant's)."""
        self._monkeypatch.setenv("HSM_FAKE_GPU_BUSY", ",".join(map(str, indices)))


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
    monkeypatch.delenv("HSM_FAKE_GPU_BUSY", raising=False)
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
