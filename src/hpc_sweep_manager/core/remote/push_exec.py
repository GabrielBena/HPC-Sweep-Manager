"""Pure helpers for the push-based remote execution model.

The push model: rsync the local project up to the sweep's code snapshot on
the remote, run ``[conda run -n env] python train.py <params>`` per task with
``CUDA_VISIBLE_DEVICES`` pinning, rsync results back. These functions build the
commands and partition the GPU pool — kept pure (no I/O) so they're unit-tested
without a real remote; :class:`SSHComputeSource` wires them to an SSH channel.
"""

from __future__ import annotations

import logging
import re
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


def normalize_gpu_allowlist(
    gpus: None | str | int | Sequence[int], detected: Sequence[int], busy: Sequence[int] = ()
) -> list[int]:
    """Resolve a ``gpus`` allowlist against the box's GPUs (nvidia-smi indices, i.e. PCI order).

    Only ``"all"`` takes a GPU in ``busy``: HSM never joins a co-tenant's GPU otherwise.

    - ``None`` (no allowlist) → every free detected GPU.
    - ``"all"`` → every detected GPU, busy or not.
    - ``0`` (int) → empty list (CPU-only).
    - ``N`` (int>0) → the first N free GPUs.
    - ``[indices]`` → those indices that are detected (so a stale allowlist
      can't point at a GPU that isn't there) and free. Order preserved.
    """
    if gpus == "all":
        return list(detected)
    free = [i for i in detected if i not in busy]
    if gpus is None:
        return free
    if isinstance(gpus, bool):  # guard: bool is an int subclass
        raise TypeError("gpus must be None, 'all', an int, or a list of ints")
    if isinstance(gpus, int):
        return free[: max(gpus, 0)]
    return [i for i in gpus if i in free]


def cpu_only(gpus: None | str | int | Sequence[int]) -> bool:
    """An explicit CPU allowlist (``--gpus cpu``, ``0``, ``[]``): its tasks see no GPU."""
    return gpus == 0 or (isinstance(gpus, (list, tuple)) and not gpus)


def partition_gpu_slots(
    allowed: Sequence[int], gpus_per_job: int, cpu_slots: int, cpu: bool = False
) -> list[list[int] | None]:
    """Partition the allowed GPUs into execution slots.

    Returns one element per concurrent worker: the GPU indices it sees (exported
    as ``CUDA_VISIBLE_DEVICES``), ``gpus_per_job`` GPUs per slot, dropping a
    trailing remainder that can't fill a slot. Without a full slot (no allowed
    GPU, ``gpus_per_job`` 0, or a request above supply) it is ``cpu_slots`` CPU
    slots: ``[]`` (no GPU visible) for an explicit CPU allowlist (``cpu``), else
    ``None`` (the environment left as is).
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
    return [[] if cpu else None] * max(cpu_slots, 1)


def check_gpu_slots(
    where: str, slots: Sequence, gpus_per_job: int, detected: Sequence[int], busy: Sequence[int]
) -> bool:
    """Whether a source can run its tasks, logged when not plainly. A GPU job with no full slot
    of free allowed GPUs on a box that has GPUs is an error (False), never a silent CPU run; on a
    CPU box, or with ``--gpus cpu`` (whose slots are ``[]``), it runs on CPU with a warning."""
    if not gpus_per_job or slots[0]:
        return True
    if slots[0] is None and detected:
        hint = f"; busy: {list(busy)}: wait, or pass --gpus all" if busy else ""
        logger.error(
            f"{where}: {gpus_per_job} GPU(s) per task, but no full slot of free allowed GPUs "
            f"among {list(detected)}{hint}"
        )
        return False
    why = "--gpus cpu" if slots[0] == [] else "no GPU found"
    logger.warning(
        f"{where}: {gpus_per_job} GPU(s) per task, but {why}: "
        f"running {len(slots)} task(s) at a time on CPU"
    )
    return True


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


def remote_interpreter(
    remote_cfg: dict,
    distributed_cfg: dict,
    project_conda_env: str | None = None,
    conda_env_override: str | None = None,
) -> tuple[str | None, str | None]:
    """``(conda_env, python_path)`` for a remote: the narrowest of an override, the remote's
    entry and the ``distributed:`` block that sets either (a key left empty is unset), taken
    whole, else the project's ``paths.conda_env``. So a remote's ``python_path`` is never beaten
    by an env set more widely."""
    for level in ({"conda_env": conda_env_override}, remote_cfg, distributed_cfg):
        if level.get("conda_env") or level.get("python_path"):
            return level.get("conda_env"), level.get("python_path")
    return project_conda_env, None


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


# Per-sweep code snapshots (tracker S4): a sweep's tasks run from ``snapshots/<sweep_id>/``, which
# lives as long as its sweep dir. An older HSM's shared ``code/`` is never written or deleted.


def own_snapshot(code_dir: str | None, sweep_id: str | None) -> str | None:
    """``code_dir`` if it is this sweep's own snapshot, the only code dir a cleanup may delete."""
    if code_dir and sweep_id and code_dir.endswith(f"/snapshots/{sweep_id}"):
        return code_dir
    return None


def snapshot_prepare_cmd(project_root: str, sweep_id: str, sweep_dirs: Sequence[str]) -> str:
    """Remote shell: print the newest existing snapshot, or the legacy ``code/`` dir (the
    ``--link-dest`` base), then create this sweep's snapshot dir and ``sweep_dirs``."""
    snaps = f"{project_root}/snapshots"
    return (
        f"ls -1d {project_root}/code/ {snaps}/*/ 2>/dev/null | tail -1; "
        f"mkdir -p {snaps}/{sweep_id} " + " ".join(sweep_dirs)
    )


def pin_code_refs(pre_script: Sequence[str], project: str) -> tuple[str, ...]:
    """Point ``pre_script``'s references to the old shared ``.../<project>/code`` dir at
    ``$HSM_CODE_DIR``, the task's own snapshot, so a task never mixes two sweeps' code."""
    old = re.compile(rf"[^\s:=\"',;&|<>()]*/{re.escape(project)}/code(?![\w.-])")
    pinned = tuple(old.sub("$HSM_CODE_DIR", line) for line in pre_script)
    if pinned != tuple(pre_script):
        logger.warning(
            f"pre_script names the old shared .../{project}/code dir, which no longer receives "
            "pushes; using $HSM_CODE_DIR (this sweep's code) instead. Write $HSM_CODE_DIR there."
        )
    if any(re.search(rf"/{re.escape(project)}/code\b", line) for line in pinned):
        logger.warning(
            f"pre_script still mentions .../{project}/code in a form HSM can't rewrite; that dir "
            "no longer receives pushes: use $HSM_CODE_DIR."
        )
    return pinned
