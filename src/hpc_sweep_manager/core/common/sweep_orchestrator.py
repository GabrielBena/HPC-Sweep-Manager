"""High-level sweep orchestrator built on the unified :class:`ComputeSource` API.

The CLI ``hsm sweep run`` ultimately wants to express:

    1. choose a backend (Slurm / local / ...),
    2. translate user-provided knobs into a :class:`ResourceSpec`,
    3. ``setup`` → ``submit_batch`` → ``wait_for_all``.

That lifecycle is the canonical pattern proven end-to-end in
``examples/smoke_slurm.py``. This module factors it out so callers don't
hand-roll it in every entry point.

Out of scope here: completion runs, dry-run rendering, sweep-config parsing,
remote/distributed backends. Those stay in ``cli/sweep.py`` for now and will
fold in once the SSH + distributed backends finish their own refactor.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
import shutil
from pathlib import Path
from typing import Any, Callable, Optional, Sequence, Union

from .compute_source import ComputeSource, SubmissionMode
from .resource_spec import ResourceSpec, spec_from_legacy_resources

logger = logging.getLogger(__name__)


# Mode strings the orchestrator accepts. Completion runs still route through
# the legacy path (they need starting-task-number propagation); see cli/sweep.py.
SUPPORTED_MODES = frozenset(
    {"local", "auto", "array", "individual", "distributed", "remote"}
)


@dataclass
class SweepResult:
    """Outcome of one sweep run.

    ``final_statuses`` is empty when the caller opted out of waiting.
    """

    sweep_id: str
    sweep_dir: Path
    job_ids: list[str] = field(default_factory=list)
    final_statuses: dict[str, str] = field(default_factory=dict)
    submission_mode: SubmissionMode = "individual"
    source_type: str = ""


def resolve_auto_mode(mode: str) -> str:
    """Resolve ``mode='auto'`` to ``'array'`` (if ``sbatch`` on PATH) or ``'local'``.

    Pure helper so the CLI can resolve mode *before* asking :func:`spec_from_cli`
    which config block to read. Non-``auto`` modes pass through unchanged.
    """
    if mode != "auto":
        return mode
    resolved = "array" if _slurm_on_path() else "local"
    logger.info(f"Auto-detected execution mode: {resolved}")
    return resolved


def spec_from_cli(
    walltime: str | None,
    resources: str | None,
    scheduler: str | None = None,
    hsm_config: Any = None,
    mode: str | None = None,
) -> ResourceSpec:
    """Build the effective :class:`ResourceSpec` from CLI flags + the *mode-matching* config block.

    Precedence (highest wins):

    1. ``--walltime`` CLI flag.
    2. Fields parsed out of ``--resources`` (legacy opaque string).
    3. The config block matching ``mode``:

       - ``mode='local'`` → ``local:`` block (``walltime``/``cpus_per_task``/``mem``/``gpus``/``pre_script`` only).
       - ``mode='array'`` or ``'individual'`` → ``slurm:`` block (full ResourceSpec including ``gpu_type``/``modules``/``qos``/``account``).
       - ``mode='remote'`` or ``'distributed'`` → *neither*. Per-remote spec
         lives under ``distributed.remotes.<alias>.spec`` and is layered in
         :func:`build_ssh_source`. CLI flags still apply on top.
       - ``mode is None`` (legacy callers) → behaves as before this refactor
         (reads ``slurm:`` block). Avoid in new code.

    4. Hardcoded defaults (all-``None`` ``ResourceSpec``).

    Pass the *resolved* mode (call :func:`resolve_auto_mode` first if you have
    ``'auto'``) so the block lookup matches the backend that will actually run.
    """
    base = ResourceSpec()
    if hsm_config is not None:
        if mode == "local":
            block_spec = hsm_config.get_local_spec()
        elif mode in ("array", "individual"):
            block_spec = hsm_config.get_slurm_spec()
        elif mode in ("remote", "distributed"):
            block_spec = None  # per-remote spec layered in build_ssh_source
        else:
            # Legacy/unknown: preserve historical behavior (read slurm:).
            block_spec = hsm_config.get_slurm_spec()
        if block_spec is not None:
            base = block_spec

    # Layer the legacy --resources string on top.
    spec = base.merge(spec_from_legacy_resources(resources, scheduler)) if resources else base

    if walltime:
        spec = spec.merge(ResourceSpec(walltime=walltime))
    return spec


def _resolve_local_parallel_jobs(
    parallel_jobs: int | None,
    hsm_config: Any,
    default: int = 1,
) -> int:
    if parallel_jobs is not None:
        return max(parallel_jobs, 1)
    if hsm_config is not None:
        cap = hsm_config.get_max_array_size() or default
        return max(1, min(cap, 8))
    return default


def _slurm_on_path() -> bool:
    return shutil.which("sbatch") is not None


def build_compute_source(
    *,
    mode: str,
    python_path: str,
    script_path: str,
    project_dir: str,
    default_spec: ResourceSpec | None = None,
    hsm_config: Any = None,
    parallel_jobs: int | None = None,
    qos_whitelist: frozenset[str] | None = None,
    remote_alias: str | None = None,
    gpus_override: Union[None, int, Sequence[int]] = None,
    conda_env_override: str | None = None,
) -> tuple[ComputeSource, str, SubmissionMode]:
    """Build a :class:`ComputeSource` for the requested mode.

    Returns ``(source, resolved_mode, submission_mode)``:

    - ``resolved_mode`` is the user-facing mode after auto-detect fallback
      (``"auto"`` is never returned — it resolves to ``"array"`` or ``"local"``).
    - ``submission_mode`` is the value to pass to ``submit_batch``.

    Local backend always uses ``submission_mode="individual"``; its slot queue
    delivers the same multi-job parallelism the legacy CLI achieved by setting
    ``local_mode="array"`` on ``LocalJobManager``.
    """
    if mode not in SUPPORTED_MODES:
        raise ValueError(
            f"build_compute_source: unsupported mode {mode!r}; "
            f"expected one of {sorted(SUPPORTED_MODES)}"
        )

    mode = resolve_auto_mode(mode)

    if mode == "distributed":
        from ..distributed.distributed_compute_source import DistributedComputeSource

        if hsm_config is None:
            raise RuntimeError("--mode distributed requires hsm_config.yaml")
        distributed_cfg = hsm_config.config_data.get("distributed", {})
        if not distributed_cfg.get("enabled", False):
            raise RuntimeError(
                "Distributed computing is not enabled in hsm_config.yaml "
                "(set distributed.enabled: true)"
            )
        if not distributed_cfg.get("remotes") and not distributed_cfg.get("local_max_jobs"):
            raise RuntimeError("No compute sources configured under distributed: in hsm_config.yaml")

        source = DistributedComputeSource(hsm_config=hsm_config, show_progress=False)
        # Distributed always fans out individual jobs across child sources.
        return source, "distributed", "individual"

    if mode == "remote":
        from ..remote.ssh_compute_source import build_ssh_source

        if not remote_alias:
            raise RuntimeError("--mode remote requires --remote <alias>")
        # Lookup precedence: registered remote → bare ssh-config alias (empty cfg).
        distributed_cfg = (
            hsm_config.config_data.get("distributed", {}) if hsm_config else {}
        )
        registered = distributed_cfg.get("remotes", {})
        remote_cfg = registered.get(remote_alias, {})
        if remote_alias not in registered:
            logger.info(
                f"Remote {remote_alias!r} not in hsm_config — using bare "
                f"~/.ssh/config alias"
            )
        source = build_ssh_source(
            name=remote_alias,
            remote_cfg=remote_cfg,
            distributed_cfg=distributed_cfg,
            project_dir=project_dir,
            script_path=script_path,
            default_spec=default_spec,
            gpus_override=gpus_override,
            conda_env_override=conda_env_override,
        )
        return source, "remote", "individual"

    if mode == "local":
        from ..local.local_compute_source import LocalComputeSource

        max_parallel = _resolve_local_parallel_jobs(parallel_jobs, hsm_config, default=1)
        source = LocalComputeSource(
            max_parallel_jobs=max_parallel,
            python_path=python_path,
            script_path=script_path,
            project_dir=project_dir,
            default_spec=default_spec,
        )
        return source, "local", "individual"

    if mode in ("array", "individual"):
        if not _slurm_on_path():
            raise RuntimeError(
                f"mode={mode!r} requested but no Slurm tools (sbatch) found on PATH. "
                "Use --mode local or set up Slurm."
            )
        from ..hpc.slurm_compute_source import SlurmComputeSource

        # Fall back to the qos_whitelist from .hsm/config.yaml's slurm: block
        # when the caller didn't supply one. Explicit caller arg always wins.
        effective_qos_whitelist = qos_whitelist
        if effective_qos_whitelist is None and hsm_config is not None:
            effective_qos_whitelist = hsm_config.get_slurm_qos_whitelist()

        source = SlurmComputeSource(
            python_path=python_path,
            script_path=script_path,
            project_dir=project_dir,
            default_spec=default_spec,
            qos_whitelist=effective_qos_whitelist,
        )
        submission_mode: SubmissionMode = "array" if mode == "array" else "individual"
        return source, mode, submission_mode

    raise AssertionError(f"unreachable: mode={mode!r}")


async def run_sweep_async(
    *,
    source: ComputeSource,
    sweep_dir: Path,
    sweep_id: str,
    params_list: list[dict[str, Any]],
    submission_mode: SubmissionMode = "individual",
    spec: ResourceSpec | None = None,
    wandb_group: str | None = None,
    job_name_prefix: str | None = None,
    wait: bool = True,
    poll_interval: float = 10.0,
    on_progress: Optional[Callable[[int, int], None]] = None,
) -> SweepResult:
    """Drive a sweep through setup → submit_batch → wait_for_all.

    This is the canonical lifecycle every ``ComputeSource`` is built around.
    Callers that already have a configured source should prefer this over
    re-implementing the dance.
    """
    if not await source.setup(sweep_dir, sweep_id):
        raise RuntimeError(
            f"setup() failed for source {source.name!r} ({source.source_type})"
        )

    job_ids = await source.submit_batch(
        params_list=params_list,
        sweep_id=sweep_id,
        mode=submission_mode,
        spec=spec,
        wandb_group=wandb_group,
        job_name_prefix=job_name_prefix,
    )

    final_statuses: dict[str, str] = {}
    if wait:
        final_statuses = await source.wait_for_all(
            poll_interval=poll_interval,
            on_progress=on_progress,
        )
        # Pull results back (rsync for SSH; no-op for Local/Slurm) and release
        # backend resources (closes the asyncssh conn, etc.). Failures are
        # warnings — don't lose final_statuses for a flaky cleanup.
        try:
            ok = await source.collect_results()
            if not ok:
                logger.warning(
                    f"collect_results returned False for source {source.name!r}"
                )
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"collect_results raised for source {source.name!r}: {e}"
            )
        try:
            await source.cleanup()
        except Exception as e:  # noqa: BLE001
            logger.warning(f"cleanup raised for source {source.name!r}: {e}")

    return SweepResult(
        sweep_id=sweep_id,
        sweep_dir=sweep_dir,
        job_ids=job_ids,
        final_statuses=final_statuses,
        submission_mode=submission_mode,
        source_type=source.source_type,
    )
