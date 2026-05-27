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
from typing import Any, Callable, Optional

from .compute_source import ComputeSource, SubmissionMode
from .resource_spec import ResourceSpec, spec_from_legacy_resources

logger = logging.getLogger(__name__)


# Mode strings the orchestrator accepts. Completion runs still route through
# the legacy path (they need starting-task-number propagation); see cli/sweep.py.
SUPPORTED_MODES = frozenset({"local", "auto", "array", "individual", "distributed"})


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


def spec_from_cli(
    walltime: str | None,
    resources: str | None,
    scheduler: str | None = None,
) -> ResourceSpec:
    """Translate the legacy CLI surface (``--walltime``, ``--resources``) into a typed spec.

    Bridges the still-textual CLI flags into the typed-spec world used by every
    new ComputeSource. Phase 3 will replace this with a typed ``slurm:`` block
    in ``hsm_config.yaml``.
    """
    spec = spec_from_legacy_resources(resources, scheduler) if resources else ResourceSpec()
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

    if mode == "auto":
        mode = "array" if _slurm_on_path() else "local"
        logger.info(f"Auto-detected execution mode: {mode}")

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

        source = SlurmComputeSource(
            python_path=python_path,
            script_path=script_path,
            project_dir=project_dir,
            default_spec=default_spec,
            qos_whitelist=qos_whitelist,
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

    return SweepResult(
        sweep_id=sweep_id,
        sweep_dir=sweep_dir,
        job_ids=job_ids,
        final_statuses=final_statuses,
        submission_mode=submission_mode,
        source_type=source.source_type,
    )
