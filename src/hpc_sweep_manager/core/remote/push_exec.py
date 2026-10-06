"""Pure helpers for the push-based remote execution model.

The push model: rsync the local project up to a rolling code dir on the
remote, run ``[conda run -n env] python train.py <params>`` per task with
``CUDA_VISIBLE_DEVICES`` pinning, rsync results back. These functions build the
commands and partition the GPU pool — kept pure (no I/O) so they're unit-tested
without a real remote; :class:`SSHComputeSource` wires them to an SSH channel.
"""

from __future__ import annotations

import logging
import shlex
from collections.abc import Sequence

logger = logging.getLogger(__name__)

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
    "*.pth",
    "/checkpoints",
    "/multirun",
    ".hydra",
    "/wandb",
)
# Per-remote ``rsync_excludes:`` is layered ON TOP of these (extends, doesn't
# replace — see SSHComputeSource.__init__). Scope notes:
# - Output-DIR names are anchored to the transfer root (leading ``/`` =
#   ``<project>/`` since the push is ``rsync … <local>/ host:<remote>/``).
#   An unanchored ``wandb`` also matches ``configs/wandb/`` — a Hydra config
#   GROUP — and silently strips it from the push; every task then dies with
#   ``MissingConfigException`` (field report 2026-06-03, and *guaranteed*
#   load-bearing for ``wandb`` because the templates inject ``wandb.group=``
#   into each command). The asymmetry decides the default: an unanchored
#   exclude breaking a config group has NO consumer workaround (per-remote
#   excludes only extend), while anchoring merely re-pushes nested junk —
#   the weight globs still catch the heavy *checkpoint* files (nested wandb
#   logs/media are not weights and will ship), and a per-remote
#   ``rsync_excludes: [wandb]`` can re-add the unanchored form on purpose.
#   Same reasoning for ``/checkpoints`` and ``/multirun`` (common
#   config-group names). Dot-dirs (``.hydra``) and env dirs (``venv``)
#   stay unanchored — they legitimately appear nested and are never
#   config-group names.
# - NOT excluding a bare ``outputs`` (too easy to clobber a legit source dir of
#   that name); only ``sweeps/outputs`` is. Add ``outputs/`` per-remote if your
#   project dumps artifacts there.
# - NOT excluding ``*.pkl`` — pickle is as often INPUT data as output, and a
#   default exclude can't be un-set per-remote. The field report's bloat was
#   ``.pkl`` *checkpoints*, already covered by the ``/checkpoints`` dir exclude.
# - Weight globs (``*.pt``/``*.pth``/``*.ckpt``) + artifact dirs assume the
#   output convention; if your repo commits one as a training INPUT it won't
#   ship — rename it (no un-exclude mechanism yet).


# The ssh under every rsync: never prompt (a headless launcher would wait forever),
# bound the connect, and give up on a link that stops answering mid-transfer.
RSYNC_SSH = "ssh -o BatchMode=yes -o ConnectTimeout=30 -o ServerAliveInterval=30"


def normalize_gpu_allowlist(gpus: None | int | Sequence[int], detected: Sequence[int]) -> list[int]:
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
) -> list[list[int] | None]:
    """Partition the allowed GPUs into execution slots.

    Returns a list where each element is a list of GPU indices to expose for
    one concurrent worker, or ``None`` for a CPU slot. Mirrors
    LocalComputeSource: ``gpus_per_job`` GPUs per slot, dropping a trailing
    remainder that can't fill a slot. Falls back to ``cpu_slots`` CPU slots when
    there are no GPUs, ``gpus_per_job`` is 0, or the request exceeds supply.
    """
    allowed = list(allowed)
    if allowed and gpus_per_job and gpus_per_job > 0:
        slots: list[list[int] | None] = []
        for i in range(0, len(allowed), gpus_per_job):
            chunk = allowed[i : i + gpus_per_job]
            if len(chunk) == gpus_per_job:
                slots.append(chunk)
        if slots:
            return slots
    return [None] * max(cpu_slots, 1)


def resolve_run_prefix(conda_env: str | None, python_path: str | None) -> str:
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
    local_dir: str,
    host: str,
    remote_dir: str,
    excludes: Sequence[str],
    agentless: bool = False,
    link_dest: str | None = None,
) -> list[str]:
    """rsync the local project tree up to a remote code dir.

    ``--delete`` keeps the remote copy an exact mirror (files removed locally
    vanish remotely); trailing slashes put *contents* of ``local_dir`` into
    ``remote_dir``. ``link_dest`` (the previous snapshot) hard-links unchanged
    files instead of copying them. Relies on the system ssh transport, which
    reads ``~/.ssh/config`` natively, so ``host`` may be an alias. ``agentless``
    skips the SSH agent, for a host whose agent stalled (``discovery.agent_stalled``).
    """
    ssh = RSYNC_SSH + (" -o IdentityAgent=none" if agentless else "")
    cmd = ["rsync", "-az", "--delete", "-e", ssh]
    if link_dest:
        cmd.append(f"--link-dest={link_dest}")
    for pattern in excludes:
        cmd.append(f"--exclude={pattern}")
    cmd.append(f"{local_dir.rstrip('/')}/")
    cmd.append(f"{host}:{remote_dir.rstrip('/')}/")
    return cmd


def build_rsync_pull_cmd(
    host: str,
    remote_dir: str,
    local_dir: str,
    excludes: Sequence[str] = (),
    agentless: bool = False,
) -> list[str]:
    """rsync a remote results dir back down (no ``--delete`` — purely additive).

    ``excludes`` lets the resumable chain (issue #12) skip the heavy per-task
    ``resume/`` checkpoint dir on intermediate pulls so a multi-GB checkpoint
    isn't dragged over the WAN at every chunk seam — it rides the cheap
    cluster-internal archive instead. ``agentless`` as in :func:`build_rsync_push_cmd`.
    """
    ssh = RSYNC_SSH + (" -o IdentityAgent=none" if agentless else "")
    cmd = ["rsync", "-az", "-e", ssh]
    for pattern in excludes:
        cmd.append(f"--exclude={pattern}")
    cmd.append(f"{host}:{remote_dir.rstrip('/')}/")
    cmd.append(f"{local_dir.rstrip('/')}/")
    return cmd


# Per-sweep code snapshots (tracker S4). Each sweep pushes its code to
# ``<project_root>/snapshots/<sweep_id>/`` and its tasks run from there, so a later push can never
# change the code of tasks still queued. ``code`` stays a symlink to the newest snapshot, so a path
# hard-coded to the old shared dir (a ``pre_script`` PYTHONPATH, a queued pre-snapshot task) keeps
# working as it did; ``$HSM_CODE_DIR`` (exported by every wrapper) names the task's own snapshot.


def snapshot_prepare_cmd(project_root: str, sweep_id: str, sweep_dirs: Sequence[str]) -> str:
    """Remote shell: print the newest existing snapshot (the ``--link-dest`` base, if any), then
    create this sweep's snapshot dir and ``sweep_dirs``."""
    snaps = f"{project_root}/snapshots"
    return f"ls -1d {snaps}/*/ 2>/dev/null | tail -1; mkdir -p {snaps}/{sweep_id} " + " ".join(
        sweep_dirs
    )


def snapshot_publish_cmd(project_root: str, sweep_id: str, keep_days: int = 7) -> str:
    """Remote shell, after the push: point ``code`` at this snapshot, then drop snapshots older
    than ``keep_days`` whose sweep dir is gone.

    A legacy real ``code/`` dir is renamed to ``code.pre-snapshots`` once, never deleted (queued
    pre-snapshot tasks may still ``cd`` there). The age guard covers tasks that entered through the
    ``code`` symlink: no snapshot younger than the longest walltime is removed under them.
    """
    root = shlex.quote(project_root)
    return (
        f"cd {root} && {{ [ -L code ] || [ ! -e code ] || mv code code.pre-snapshots; }} && "
        f"ln -sfn snapshots/{sweep_id} code && "
        f"find snapshots -mindepth 1 -maxdepth 1 -type d -mtime +{keep_days} | "
        'while read -r d; do [ -d "sweeps/${d#snapshots/}" ] || rm -rf "$d"; done'
    )


def warn_shared_code_refs(pre_script: Sequence[str], project: str) -> None:
    """Warn when ``pre_script`` names the old shared code dir instead of ``$HSM_CODE_DIR``."""
    if any(f"/{project}/code" in line for line in pre_script):
        logger.warning(
            f"pre_script mentions .../{project}/code, which now points at the newest sweep's code "
            "snapshot; use $HSM_CODE_DIR for the code of the task's own sweep."
        )
