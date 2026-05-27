"""Pure helpers for the push-based remote execution model.

The push model: rsync the local project up to a rolling code dir on the
remote, run ``[conda run -n env] python train.py <params>`` per task with
``CUDA_VISIBLE_DEVICES`` pinning, rsync results back. These functions build the
commands and partition the GPU pool — kept pure (no I/O) so they're unit-tested
without a real remote; :class:`SSHComputeSource` wires them to an SSH channel.
"""

from __future__ import annotations

from typing import List, Optional, Sequence, Union

# Files never worth shipping to a compute node — keeps the rsync payload to
# source only (no history, caches, prior outputs, data, checkpoints).
DEFAULT_RSYNC_EXCLUDES: tuple[str, ...] = (
    ".git",
    "__pycache__",
    "*.pyc",
    ".venv",
    "venv",
    "sweeps/outputs",
    "*.ckpt",
    "*.pt",
    "wandb",
)


def normalize_gpu_allowlist(
    gpus: Union[None, int, Sequence[int]], detected: Sequence[int]
) -> List[int]:
    """Resolve the per-remote ``gpus`` config against the box's detected GPUs.

    - ``None`` → use all detected GPUs.
    - ``0`` (int) → empty list (CPU-only).
    - ``N`` (int>0) → the first N detected GPUs.
    - ``[indices]`` → exactly those indices, intersected with detected (so a
      stale allowlist can't point at a GPU that isn't there). Order preserved.
    """
    detected = list(detected)
    if gpus is None:
        return detected
    if isinstance(gpus, bool):  # guard: bool is an int subclass
        raise TypeError("gpus must be None, an int, or a list of ints")
    if isinstance(gpus, int):
        if gpus <= 0:
            return []
        return detected[:gpus]
    # explicit allowlist
    detected_set = set(detected)
    return [i for i in gpus if i in detected_set]


def partition_gpu_slots(
    allowed: Sequence[int], gpus_per_job: int, cpu_slots: int
) -> List[Optional[List[int]]]:
    """Partition the allowed GPUs into execution slots.

    Returns a list where each element is a list of GPU indices to expose for
    one concurrent worker, or ``None`` for a CPU slot. Mirrors
    LocalComputeSource: ``gpus_per_job`` GPUs per slot, dropping a trailing
    remainder that can't fill a slot. Falls back to ``cpu_slots`` CPU slots when
    there are no GPUs, ``gpus_per_job`` is 0, or the request exceeds supply.
    """
    allowed = list(allowed)
    if allowed and gpus_per_job and gpus_per_job > 0:
        slots: List[Optional[List[int]]] = []
        for i in range(0, len(allowed), gpus_per_job):
            chunk = allowed[i : i + gpus_per_job]
            if len(chunk) == gpus_per_job:
                slots.append(chunk)
        if slots:
            return slots
    return [None] * max(cpu_slots, 1)


def resolve_run_prefix(conda_env: Optional[str], python_path: Optional[str]) -> str:
    """Build the interpreter invocation for a remote task.

    Prefers a conda env *name* (path-independent across boxes); falls back to an
    explicit interpreter path, then bare ``python``.
    """
    if conda_env:
        return f"conda run -n {conda_env} python"
    if python_path:
        return python_path
    return "python"


def build_rsync_push_cmd(
    local_dir: str, host: str, remote_dir: str, excludes: Sequence[str]
) -> List[str]:
    """rsync the local project tree up to the rolling remote code dir.

    ``--delete`` keeps the remote copy an exact mirror (files removed locally
    vanish remotely); trailing slashes put *contents* of ``local_dir`` into
    ``remote_dir``. Relies on the system ssh transport, which reads
    ``~/.ssh/config`` natively, so ``host`` may be an alias.
    """
    cmd = ["rsync", "-az", "--delete"]
    for pattern in excludes:
        cmd.append(f"--exclude={pattern}")
    cmd.append(f"{local_dir.rstrip('/')}/")
    cmd.append(f"{host}:{remote_dir.rstrip('/')}/")
    return cmd


def build_rsync_pull_cmd(host: str, remote_dir: str, local_dir: str) -> List[str]:
    """rsync a remote results dir back down (no ``--delete`` — purely additive)."""
    return [
        "rsync",
        "-az",
        f"{host}:{remote_dir.rstrip('/')}/",
        f"{local_dir.rstrip('/')}/",
    ]
