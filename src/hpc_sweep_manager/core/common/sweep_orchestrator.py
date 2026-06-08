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

from .chain import ChainConfig, ChainDecision, ChainState, ChunkOutcome, decide_next
from .compute_source import ComputeSource, SubmissionMode
from .resource_spec import ResourceSpec, spec_from_legacy_resources
from .resumable import ResumableConfig, ResumableContext

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
    # Resumable chains (issue #12): the terminal decision ("done"/"failed"/
    # "advance" — "advance" means a non-blocking advance left a chunk running)
    # and how many chunks this invocation submitted. Empty for normal sweeps.
    chain_decision: str = ""
    chunks_run: int = 0


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
    hsm_config: Any,  # kept for signature compat; no longer consulted
    default: int = 1,
) -> int:
    """Resolve the local slot-queue width.

    CLI ``--parallel-jobs`` is the only knob. Pre-cleanup this used to
    piggyback on ``slurm.max_array_size`` for a default cap, but that was
    a leftover from the v0.1 opaque-block era — local mode and Slurm
    array size have nothing to do with each other. If you need a higher
    local default, pass ``--parallel-jobs N``.
    """
    if parallel_jobs is not None:
        return max(parallel_jobs, 1)
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
    remote_submission: SubmissionMode | None = None,
    resumable: bool = False,
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

    # Resumable chains (issue #12) need Slurm dependencies + the pre-walltime
    # signal — reject backends that have neither. The remote+ssh case is caught
    # inside the remote branch (it depends on the per-remote backend field).
    _RESUMABLE_HINT = (
        "--resumable requires a Slurm backend: native `--mode array` on a login "
        "node, or `--remote <alias>` where the remote has `backend: slurm`. The "
        "local and ssh-bash backends have no scheduler dependency/signal mechanism."
    )
    if resumable and mode in ("local", "distributed", "individual"):
        raise RuntimeError(_RESUMABLE_HINT)

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
        if not remote_alias:
            raise RuntimeError("--mode remote requires --remote <alias>")
        # Lookup precedence: registered remote → bare ssh-config alias (empty cfg).
        distributed_cfg = dict(
            hsm_config.config_data.get("distributed", {}) if hsm_config else {}
        )
        # paths.conda_env is the lowest-priority fallback for the SSH
        # factories. Per-remote / distributed.conda_env still win because
        # we only inject when absent.
        if hsm_config is not None:
            _proj_env = getattr(hsm_config, "get_conda_env", lambda: None)()
            if _proj_env and "conda_env" not in distributed_cfg:
                distributed_cfg["conda_env"] = _proj_env
        registered = distributed_cfg.get("remotes", {})
        remote_cfg = registered.get(remote_alias, {})
        if remote_alias not in registered:
            logger.info(
                f"Remote {remote_alias!r} not in hsm_config — using bare "
                f"~/.ssh/config alias"
            )

        # Dispatch on the per-remote `backend:` field (default `ssh`).
        # `slurm` routes through SSHSlurmComputeSource which drives sbatch
        # on the remote login node. Stays at the remote level (not nested
        # under `spec:`) because the backend choice is per-source, not
        # per-job; keeping it out of ResourceSpec preserves that block's
        # purity.
        backend = (remote_cfg.get("backend") or "ssh").lower()
        if backend == "slurm":
            from ..remote.ssh_slurm_compute_source import build_ssh_slurm_source

            if gpus_override is not None:
                logger.warning(
                    f"--gpus is ignored for backend=slurm on {remote_alias!r} "
                    f"(Slurm allocates GPUs via the `--gres`/`--gpus` "
                    f"#SBATCH directive; set `spec.gpus` and `spec.gpu_type`)"
                )
            source = build_ssh_slurm_source(
                name=remote_alias,
                remote_cfg=remote_cfg,
                distributed_cfg=distributed_cfg,
                project_dir=project_dir,
                script_path=script_path,
                default_spec=default_spec,
                conda_env_override=conda_env_override,
            )
            # `--mode array` over SSH-Slurm packs one `sbatch --array`; default
            # is one sbatch per combo (individual). SSHSlurmComputeSource
            # already implements both via submit_batch(mode=...).
            return source, "remote", (remote_submission or "individual")
        elif backend == "ssh":
            from ..remote.ssh_compute_source import build_ssh_source

            if resumable:
                raise RuntimeError(
                    f"--resumable needs `backend: slurm` on remote {remote_alias!r} "
                    f"(it drives Slurm dependencies + the pre-walltime signal); "
                    f"this remote is bash-over-SSH (`backend: ssh`)."
                )
            if remote_submission == "array":
                logger.warning(
                    f"--mode array is ignored for backend=ssh on {remote_alias!r} "
                    f"(array submission needs Slurm; using individual). Set "
                    f"`backend: slurm` on the remote to pack one sbatch --array."
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
        else:
            raise ValueError(
                f"Unknown backend {backend!r} for remote {remote_alias!r}; "
                f"expected 'ssh' or 'slurm'"
            )

    # Project-level conda env (from paths.conda_env). Single source of truth
    # for local + native Slurm. Overridable per-source for SSH/SSH-Slurm via
    # distributed.remotes.<alias>.conda_env (handled in build_ssh* factories).
    # CLI --conda-env (conda_env_override) wins for --remote runs; for
    # local/array/individual it also wins if passed.
    project_conda_env: str | None = None
    if hsm_config is not None:
        # Defensive getattr — older config objects + FakeConfig in tests
        # may not have the accessor.
        project_conda_env = getattr(hsm_config, "get_conda_env", lambda: None)()
    if conda_env_override is not None:
        project_conda_env = conda_env_override

    if mode == "local":
        from ..local.local_compute_source import LocalComputeSource

        max_parallel = _resolve_local_parallel_jobs(parallel_jobs, hsm_config, default=1)
        # GPU allowlist precedence: CLI --gpus > local.visible_gpus > all detected.
        # CLI sentinel: gpus_override is None when --gpus was not passed.
        if gpus_override is not None:
            visible_gpus = gpus_override
        elif hsm_config is not None:
            visible_gpus = hsm_config.get_local_visible_gpus()
        else:
            visible_gpus = None
        source = LocalComputeSource(
            max_parallel_jobs=max_parallel,
            python_path=python_path,
            script_path=script_path,
            project_dir=project_dir,
            default_spec=default_spec,
            visible_gpus=visible_gpus,
            conda_env=project_conda_env,
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

        speed_factors = (
            hsm_config.get_slurm_speed_factors() if hsm_config is not None else None
        )
        source = SlurmComputeSource(
            python_path=python_path,
            script_path=script_path,
            project_dir=project_dir,
            default_spec=default_spec,
            qos_whitelist=effective_qos_whitelist,
            conda_env=project_conda_env,
            speed_factors=speed_factors,
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
    costs: Optional[list[float]] = None,
) -> SweepResult:
    """Drive a sweep through setup → submit_batch → wait_for_all.

    This is the canonical lifecycle every ``ComputeSource`` is built around.
    Callers that already have a configured source should prefer this over
    re-implementing the dance. ``costs`` (optional, parallel to
    ``params_list``) are per-task relative cost hints for heterogeneous
    placement — see ``core/hpc/gpu_planner``.
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
        costs=costs,
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


async def run_resumable_sweep_async(
    *,
    source: ComputeSource,
    sweep_dir: Path,
    sweep_id: str,
    params_list: list[dict[str, Any]],
    spec: ResourceSpec | None,
    resumable: ResumableConfig,
    submission_mode: SubmissionMode = "array",
    wandb_group: str | None = None,
    job_name_prefix: str | None = None,
    poll_interval: float = 10.0,
    on_progress: Optional[Callable[[int, int], None]] = None,
    costs: Optional[list[float]] = None,
    chain_state: ChainState | None = None,
    do_setup: bool = True,
    initial_job_ids: list[str] | None = None,
    block: bool = True,
) -> SweepResult:
    """Drive a checkpoint-chained run (issue #12, option B = advance-on-poll).

    Each chunk re-submits the FULL param set as one Slurm array (or K typed
    sub-arrays); already-done tasks no-op via the ``.hsm_done`` sentinel, so the
    chunk advances heterogeneous per-task lengths automatically. After a chunk
    reaches a terminal Slurm state the driver probes per-task sentinels +
    checkpoint mtime (:meth:`ComputeSource.chunk_progress`), turns successive
    observations into the ``progressed`` bool, and consults the pure
    :func:`chain.decide_next` — DONE / ADVANCE / FAILED.

    ``block=True`` (foreground ``hsm sweep run --resumable``) drives to a
    terminal decision. ``block=False`` (cron ``hsm sweep advance``) performs a
    single transition: evaluate the current terminal chunk and, on ADVANCE,
    submit the next chunk WITHOUT waiting (leaving it running). ``do_setup`` /
    ``chain_state`` / ``initial_job_ids`` let ``advance`` re-attach mid-flight.
    """
    if submission_mode != "array":
        raise ValueError("resumable chains require submission_mode='array'")

    num_tasks = len(params_list)
    ckpt_subdir = resumable.checkpoint_subdir
    # Make every tasks/ pull (the incremental ones in wait_for_all AND the final
    # collect) skip the heavy checkpoint dir — it rides the cheap server-side
    # archive, not the WAN. (No-op attr on the native source.)
    if hasattr(source, "_pull_excludes"):
        source._pull_excludes = (f"*/{ckpt_subdir}/",)

    if do_setup:
        if not await source.setup(sweep_dir, sweep_id):
            raise RuntimeError(
                f"setup() failed for source {source.name!r} ({source.source_type})"
            )

    state = chain_state or ChainState()
    chain_cfg = ChainConfig(
        max_chunks=resumable.max_chunks,
        max_consecutive_failures=resumable.max_consecutive_failures,
    )
    rcfg_manifest = resumable.to_manifest()
    chunks_meta: list[dict[str, Any]] = []
    # Re-attach (advance): the seeded chunk is already submitted — record it so
    # the manifest never persists an empty `chunks` list (a crash between the
    # post-evaluate persist and the next submit would otherwise strand the
    # chain: the next advance would see "no chunks recorded").
    if initial_job_ids:
        chunks_meta.append(
            {
                "index": state.chunk_index,
                "job_ids": list(initial_job_ids),
                "terminal_states": [],
            }
        )
    prev_job_ids: list[str] = []
    # The no-progress baseline. On a fresh foreground run it accumulates across
    # chunks, so the consecutive-failure cap fires promptly. On a detached
    # `advance` re-attach it resets here (the prior chunk's done-count/mtime
    # aren't carried in the manifest), so the FIRST chunk after re-attach is
    # biased toward "progressed". `state.consecutive_no_progress` is still
    # restored, and `max_chunks` is the hard bound regardless — so a
    # cron-driven chain is always bounded, just slower to flag a deterministic
    # crash than the live launcher. (Carrying these in the manifest is a clean
    # follow-up.)
    prev_done = 0
    prev_mtime: float | None = None
    current: list[str] | None = initial_job_ids
    decision: ChainDecision | None = None
    submitted_this_call = 0
    last_job_ids: list[str] = list(initial_job_ids or [])

    async def _persist() -> None:
        try:
            await source.persist_chain_manifest(
                resumable=rcfg_manifest,
                chain={
                    "state": state.to_dict(),
                    "chunks": chunks_meta,
                    "num_tasks": num_tasks,
                },
                job_ids=last_job_ids,
                num_tasks=num_tasks,
            )
        except Exception as e:  # noqa: BLE001 — manifest is a convenience, not load-bearing mid-run
            logger.warning(f"could not persist chain manifest: {e}")

    while True:
        if current is None:
            dependency = (
                f"afterany:{':'.join(prev_job_ids)}" if prev_job_ids else None
            )
            ctx = ResumableContext(chunk_index=state.chunk_index, config=resumable)
            logger.info(
                f"chain {sweep_id}: submitting chunk {state.chunk_index + 1} "
                f"(walltime cap {resumable.chunk_walltime}"
                f"{', dep ' + dependency if dependency else ''})"
            )
            current = await source.submit_batch(
                params_list=params_list,
                sweep_id=sweep_id,
                mode="array",
                spec=spec,
                wandb_group=wandb_group,
                job_name_prefix=job_name_prefix,
                costs=costs,
                dependency=dependency,
                resumable=ctx,
            )
            submitted_this_call += 1
            last_job_ids = list(current)
            chunks_meta.append(
                {
                    "index": state.chunk_index,
                    "job_ids": list(current),
                    "terminal_states": [],
                }
            )
            await _persist()
            if not block:
                # Non-blocking advance: the next chunk is queued (afterany the
                # prior); leave it running for the cluster / a later advance.
                decision = ChainDecision.ADVANCE
                break

        # Wait for the current chunk to reach a terminal Slurm state. For a
        # re-attached terminal chunk the caller seeded completed_jobs, so this
        # returns at once.
        last_statuses = await source.wait_for_all(poll_interval=poll_interval)
        if chunks_meta:
            chunks_meta[-1]["terminal_states"] = list(last_statuses.values())

        progress = await source.chunk_progress(
            num_tasks,
            done_sentinel=resumable.done_sentinel,
            checkpoint_subdir=ckpt_subdir,
        )
        done_count = len(progress.done_indices)
        progressed = (done_count > prev_done) or (
            progress.checkpoint_mtime is not None
            and (prev_mtime is None or progress.checkpoint_mtime > prev_mtime)
        )
        outcome = ChunkOutcome(
            chunk_index=state.chunk_index,
            done_count=done_count,
            num_tasks=num_tasks,
            progressed=progressed,
            terminal_states=tuple(last_statuses.values()),
        )
        step = decide_next(outcome, state, chain_cfg)
        decision = step.decision
        state = step.next_state
        logger.info(f"chain {sweep_id}: {step.reason}")
        if on_progress is not None:
            on_progress(done_count, max(num_tasks, 1))
        await _persist()

        prev_job_ids = list(current)
        prev_done = done_count
        if progress.checkpoint_mtime is not None:
            prev_mtime = progress.checkpoint_mtime

        if decision is ChainDecision.DONE:
            await _safe_collect(source, defer_cleanup=False)
            break
        if decision is ChainDecision.FAILED:
            # Keep the remote for inspection (don't archive/clean a failed chain).
            await _safe_collect(source, defer_cleanup=True)
            break

        # ADVANCE: clear per-chunk tracking so the next wait_for_all sees only
        # the next chunk, then loop to submit it.
        source.active_jobs.clear()
        source.completed_jobs.clear()
        current = None

    try:
        await source.cleanup()
    except Exception as e:  # noqa: BLE001
        logger.warning(f"cleanup raised for source {source.name!r}: {e}")

    return SweepResult(
        sweep_id=sweep_id,
        sweep_dir=sweep_dir,
        job_ids=last_job_ids,
        final_statuses={},
        submission_mode="array",
        source_type=source.source_type,
        chain_decision=(decision.value if decision is not None else ""),
        chunks_run=submitted_this_call,
    )


async def _safe_collect(source: ComputeSource, *, defer_cleanup: bool) -> None:
    try:
        ok = await source.collect_results(defer_cleanup=defer_cleanup)
        if not ok:
            logger.warning(
                f"collect_results returned False for source {source.name!r}"
            )
    except Exception as e:  # noqa: BLE001
        logger.warning(f"collect_results raised for source {source.name!r}: {e}")
