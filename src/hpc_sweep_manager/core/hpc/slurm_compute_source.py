"""Native Slurm compute source built on the unified ComputeSource ABC.

Replaces the legacy :class:`SlurmJobManager` (``slurm_manager.py``) and the
parallel :class:`HPCJobManager` hierarchy. Submissions accept a typed
:class:`ResourceSpec`; the source translates it into ``#SBATCH`` directives
and merges with the cluster-default spec stored on the source.

QOS whitelist (``qos_whitelist``) is opt-in. For S3IT, pass
``frozenset({"normal", "medium", "long"})``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import shutil
import subprocess
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from ..common.compute_source import JobInfo, SubmissionMode
from ..common.resource_spec import ResourceSpec
from ..common.resumable import ChunkProgress, ResumableContext
from ..common.templating import params_to_hydra_args, params_to_yaml, render_template
from ..remote.push_exec import resolve_run_prefix
from .gpu_planner import (
    SubArraySubmission,
    build_array_submissions,
    replace_sub_walltime,
)
from .slurm_base import SlurmBase
from .slurm_protocol import (
    format_signal,
    parse_sbatch_job_id,
    render_sbatch_directives,
)

logger = logging.getLogger(__name__)


def _python_needs_conda_init(python_path: str) -> bool:
    """Heuristic: does this `python_path` need the conda/mamba init block?

    Returns ``True`` when ``python_path`` looks like a conda/mamba invocation
    (e.g. ``conda run -n env python``, ``micromamba run -n env python``) — in
    that case the rendered sbatch script needs to source a conda/mamba init
    file so the command actually resolves on the compute node, where
    non-interactive shells skip ``~/.bashrc``.

    Returns ``False`` for fully-qualified python paths (e.g.
    ``/home/user/miniconda3/bin/python``) — those don't need shell-level
    activation; the python binary already knows its own env.
    """
    p = str(python_path).lower().strip()
    return p.startswith("conda ") or p.startswith("mamba ") or p.startswith("micromamba ")


class SlurmComputeSource(SlurmBase):
    def __init__(
        self,
        name: str = "slurm",
        max_parallel_jobs: int = 0,
        python_path: str = "python",
        script_path: str = "",
        project_dir: str = ".",
        default_spec: ResourceSpec | None = None,
        qos_whitelist: frozenset[str] | None = None,
        conda_env: str | None = None,
        speed_factors: dict[str, float] | None = None,
    ):
        # 0 means "no client-side cap" — the cluster's own scheduler decides.
        super().__init__(name, "slurm", max_parallel_jobs or 10_000)
        # When conda_env is set, wrap the supplied python_path in
        # `conda run -n <env> python` and emit the shared conda init
        # partial. Matches SSHSlurmComputeSource's behavior, so a single
        # `paths.conda_env: <name>` in the project config gives both
        # local and remote Slurm runs the same env without per-source
        # duplication.
        self.conda_env = conda_env
        if conda_env:
            self.python_path = resolve_run_prefix(conda_env, None)
        else:
            self.python_path = python_path
        self.script_path = script_path
        self.project_dir = project_dir
        self.default_spec = default_spec or ResourceSpec()
        self.qos_whitelist = qos_whitelist
        # GPU type → relative runtime multiplier; parameterizes the
        # multi-gpu_type planner (core/hpc/gpu_planner). None = all 1.0.
        self.speed_factors = dict(speed_factors) if speed_factors else None
        self.sweep_dir: Path | None = None
        self.sweep_id: str | None = None

    async def setup(self, sweep_dir: Path, sweep_id: str) -> bool:
        for tool in ("sbatch", "squeue", "scancel"):
            if not shutil.which(tool):
                logger.error(f"Slurm tool '{tool}' not found on PATH")
                self.stats.health_status = "unhealthy"
                return False
        try:
            result = await asyncio.to_thread(
                subprocess.run,
                ["sinfo", "--version"],
                capture_output=True,
                check=True,
                timeout=5,
                text=True,
            )
            logger.debug(f"Slurm detected: {result.stdout.strip()}")
        except (
            subprocess.CalledProcessError,
            FileNotFoundError,
            subprocess.TimeoutExpired,
        ) as e:
            logger.error(f"sinfo --version failed: {e}")
            self.stats.health_status = "unhealthy"
            return False

        self.sweep_dir = sweep_dir
        self.sweep_id = sweep_id
        self.stats.health_status = "healthy"
        self.stats.last_health_check = datetime.now()
        return True

    def _effective_spec(self, spec: ResourceSpec | None) -> ResourceSpec:
        merged = self.default_spec.merge(spec)
        if (
            merged.qos is not None
            and self.qos_whitelist is not None
            and merged.qos not in self.qos_whitelist
        ):
            raise ValueError(
                f"qos={merged.qos!r} is not in the whitelist {sorted(self.qos_whitelist)}"
            )
        return merged

    def _ensure_dirs(self) -> tuple[Path, Path, Path]:
        if self.sweep_dir is None:
            raise RuntimeError(f"SlurmComputeSource {self.name!r} not set up; call setup() first")
        scripts_dir = self.sweep_dir / "scripts"
        logs_dir = self.sweep_dir / "logs"
        tasks_dir = self.sweep_dir / "tasks"
        for d in (scripts_dir, logs_dir, tasks_dir):
            d.mkdir(parents=True, exist_ok=True)
        return scripts_dir, logs_dir, tasks_dir

    async def submit_job(
        self,
        params: dict[str, Any],
        job_name: str,
        sweep_id: str,
        wandb_group: str | None = None,
        spec: ResourceSpec | None = None,
    ) -> str:
        effective = await self._off_gpu_nodes(self._effective_spec(spec))
        directives = render_sbatch_directives(effective)
        scripts_dir, logs_dir, tasks_dir = self._ensure_dirs()
        task_dir = tasks_dir / job_name
        task_dir.mkdir(parents=True, exist_ok=True)

        script_content = render_template(
            "slurm_single.sh.j2",
            job_name=job_name,
            sweep_id=sweep_id,
            logs_dir=str(logs_dir),
            task_dir=str(task_dir),
            sbatch_directives=directives,
            modules=list(effective.modules),
            pre_script=list(effective.pre_script),
            project_dir=self.project_dir,
            python_path=self.python_path,
            script_path=self.script_path,
            params_hydra=params_to_hydra_args(params),
            params_yaml=params_to_yaml(params),
            wandb_group=wandb_group,
            uses_conda=_python_needs_conda_init(self.python_path),
            conda_env=self.conda_env,
        )
        script_path = scripts_dir / f"{job_name}.slurm"
        script_path.write_text(script_content)

        result = await asyncio.to_thread(
            subprocess.run,
            ["sbatch", str(script_path)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"sbatch failed for {job_name}: {result.stderr.strip() or 'no stderr'}"
            )
        job_id = parse_sbatch_job_id(result.stdout)

        self.active_jobs[job_id] = JobInfo(
            job_id=job_id,
            job_name=job_name,
            params=params,
            source_name=self.name,
            status="PENDING",
            submit_time=datetime.now(),
            task_dir=str(task_dir),
        )
        self.stats.total_submitted += 1
        logger.info(f"Submitted Slurm job {job_id} ({job_name})")
        return job_id

    async def submit_batch(
        self,
        params_list: list[dict[str, Any]],
        sweep_id: str,
        mode: SubmissionMode = "individual",
        *args: Any,
        **kwargs: Any,
    ) -> list[str]:
        """Submit, then name the live jobs in ``.hsm_manifest.json``, also when submission stops
        partway, so that ``hsm sweep cancel`` finds them. A chain's driver writes its own."""
        try:
            return await self._submit_batch(params_list, sweep_id, mode, *args, **kwargs)
        finally:
            if self.active_jobs and kwargs.get("resumable") is None:
                await self._write_manifest(list(self.active_jobs), mode, len(params_list))

    async def _submit_batch(
        self,
        params_list: list[dict[str, Any]],
        sweep_id: str,
        mode: SubmissionMode = "individual",
        spec: ResourceSpec | None = None,
        wandb_group: str | None = None,
        job_name_prefix: str | None = None,
        costs: Sequence[float] | None = None,
        *,
        dependency: str | None = None,
        resumable: ResumableContext | None = None,
    ) -> list[str]:
        cap = resumable.config.chunk_walltime if resumable else None
        await self._warn_reservations(cap or self._effective_spec(spec).walltime)
        if resumable is not None and mode != "array":
            raise ValueError(
                f"resumable chains use array mode (one chunk = one Slurm array); got mode={mode!r}"
            )
        if mode == "array":
            return await self._submit_array(
                params_list,
                sweep_id,
                spec,
                wandb_group,
                job_name_prefix,
                costs,
                dependency=dependency,
                resumable=resumable,
            )
        effective = self._effective_spec(spec)
        if isinstance(effective.gpu_type, tuple):
            raise ValueError(
                "Multi-type gpu_type lists are supported in array mode only "
                "— use `--mode array` (one Slurm array per GPU type)."
            )
        return await super().submit_batch(
            params_list, sweep_id, mode, spec, wandb_group, job_name_prefix
        )

    async def _submit_array(
        self,
        params_list: list[dict[str, Any]],
        sweep_id: str,
        spec: ResourceSpec | None,
        wandb_group: str | None,
        job_name_prefix: str | None,
        costs: Sequence[float] | None = None,
        *,
        dependency: str | None = None,
        resumable: ResumableContext | None = None,
    ) -> list[str]:
        """Submit the sweep as 1..K Slurm arrays (K > 1 for multi-type specs).

        Mirrors ``SSHSlurmComputeSource._submit_array`` — both build their
        sub-array descriptors from the same pure
        :func:`gpu_planner.build_array_submissions`, so the partitioning
        cannot drift between transports. Resumable chunks (issue #12) cap every
        sub-array's walltime at ``chunk_walltime`` and carry the dependency.
        """
        if not params_list:
            raise ValueError("Cannot submit an empty array")
        effective = await self._off_gpu_nodes(self._effective_spec(spec))
        submissions = build_array_submissions(
            params_list=params_list,
            effective_spec=effective,
            prefix=job_name_prefix or sweep_id,
            speed_factors=self.speed_factors,
            costs=costs,
        )
        if resumable is not None:
            cap = resumable.config.chunk_walltime
            submissions = [replace_sub_walltime(sub, cap) for sub in submissions]
        job_ids: list[str] = []
        try:
            for sub in submissions:
                job_ids.append(
                    await self._submit_one_array(
                        sub,
                        sweep_id,
                        wandb_group,
                        dependency=dependency,
                        resumable=resumable,
                    )
                )
        except Exception:
            if job_ids:
                # Earlier sub-arrays are LIVE — name them so the user can
                # decide (outside a chain, the manifest names them too).
                logger.error(
                    f"array submission failed partway — {len(job_ids)} "
                    f"sub-array(s) already live: {', '.join(job_ids)}. "
                    f"Cancel with: scancel {' '.join(job_ids)}"
                )
            raise
        return job_ids

    async def _submit_one_array(
        self,
        sub: SubArraySubmission,
        sweep_id: str,
        wandb_group: str | None,
        *,
        dependency: str | None = None,
        resumable: ResumableContext | None = None,
    ) -> str:
        signal = format_signal(resumable.config.signal_grace) if resumable else None
        directives = render_sbatch_directives(sub.spec, dependency=dependency, signal=signal)
        scripts_dir, logs_dir, tasks_dir = self._ensure_dirs()

        # "index" is array-local (matched against $SLURM_ARRAY_TASK_ID);
        # "global_index" keeps the task's original 1..N position so
        # tasks/task_%04d stays globally numbered across sub-arrays.
        params_file = self.sweep_dir / sub.params_filename  # type: ignore[union-attr]
        params_file.write_text(json.dumps(list(sub.entries), indent=2))

        rcfg = resumable.config if resumable else None
        script_content = render_template(
            "slurm_array.sh.j2",
            job_name=sub.job_name,
            sweep_id=sweep_id,
            num_jobs=len(sub.entries),
            array_throttle=sub.spec.array_throttle,
            logs_dir=str(logs_dir),
            tasks_dir=str(tasks_dir),
            params_file=str(params_file),
            sbatch_directives=directives,
            modules=list(sub.spec.modules),
            pre_script=list(sub.spec.pre_script),
            project_dir=self.project_dir,
            python_path=self.python_path,
            script_path=self.script_path,
            wandb_group=wandb_group,
            uses_conda=_python_needs_conda_init(self.python_path),
            conda_env=self.conda_env,
            gpu_type=sub.gpu_type,
            resumable=resumable is not None,
            resume_from_present=(resumable.resume_from_present if resumable else False),
            resume_arg=(rcfg.resume_arg if rcfg else None),
            done_sentinel=(rcfg.done_sentinel if rcfg else ".hsm_done"),
            checkpoint_subdir=(rcfg.checkpoint_subdir if rcfg else "resume"),
        )
        script_path = scripts_dir / f"{sub.job_name}.slurm"
        script_path.write_text(script_content)

        result = await asyncio.to_thread(
            subprocess.run,
            ["sbatch", str(script_path)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"sbatch (array) failed: {result.stderr.strip() or 'no stderr'}")
        job_id = parse_sbatch_job_id(result.stdout)

        params: dict[str, Any] = {"_array_size": len(sub.entries)}
        if sub.gpu_type:
            params["_gpu_type"] = sub.gpu_type
        self.active_jobs[job_id] = JobInfo(
            job_id=job_id,
            job_name=sub.job_name,
            params=params,
            source_name=self.name,
            status="PENDING",
            submit_time=datetime.now(),
            task_dir=str(tasks_dir),
        )
        self.stats.total_submitted += len(sub.entries)
        gpu_note = f", gpu_type={sub.gpu_type}" if sub.gpu_type else ""
        logger.info(
            f"Submitted Slurm array job {job_id} ({sub.job_name}, "
            f"{len(sub.entries)} tasks{gpu_note})"
        )
        return job_id

    async def _sh(self, argv: Sequence[str]) -> tuple[int, str, str]:
        try:
            r = await asyncio.to_thread(subprocess.run, list(argv), capture_output=True, text=True)
        except OSError as e:  # the binary is missing: what a shell would report as rc 127
            return 127, "", str(e)
        return r.returncode, r.stdout or "", r.stderr or ""

    async def cancel_job(self, job_id: str) -> bool:
        result = await asyncio.to_thread(
            subprocess.run, ["scancel", job_id], capture_output=True, text=True
        )
        success = result.returncode == 0
        if success and job_id in self.active_jobs:
            self.update_job_status(job_id, "CANCELLED")
        return success

    async def collect_results(
        self, job_ids: list[str] | None = None, *, defer_cleanup: bool = False
    ) -> bool:
        # Slurm outputs land directly in the shared filesystem under tasks_dir.
        # (defer_cleanup is a resumable-chain no-op: nothing to pull or tear down
        # on the shared FS — checkpoints already persist in place across chunks.)
        return True

    async def chunk_progress(
        self, num_tasks: int, *, done_sentinel: str, checkpoint_subdir: str
    ) -> ChunkProgress:
        """Local-FS twin of the SSH probe (issue #12): scan the shared tasks dir
        for done-sentinels + the newest checkpoint mtime. No pull needed — the
        files are already on the filesystem the driver runs on."""
        _, _, tasks_dir = self._ensure_dirs()
        done: set[int] = set()
        newest: float | None = None
        for task_dir in Path(tasks_dir).glob("task_*"):
            if not task_dir.is_dir():
                continue
            m = re.match(r"task_(\d+)$", task_dir.name)
            if m and (task_dir / done_sentinel).exists():
                done.add(int(m.group(1)))
            ckpt = task_dir / checkpoint_subdir
            if ckpt.is_dir():
                for f in ckpt.rglob("*"):
                    if f.is_file():
                        mt = f.stat().st_mtime
                        if newest is None or mt > newest:
                            newest = mt
        return ChunkProgress(done_indices=frozenset(done), checkpoint_mtime=newest)

    async def health_check(self) -> dict[str, Any]:
        try:
            result = await asyncio.to_thread(
                subprocess.run,
                ["sinfo", "-h", "-o", "%P %a %D"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode != 0:
                self.stats.health_status = "unhealthy"
                return {"status": "unhealthy", "error": result.stderr.strip()}
            self.stats.health_status = "healthy"
            self.stats.last_health_check = datetime.now()
            return {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "active_jobs": len(self.active_jobs),
                "partitions": result.stdout.strip(),
            }
        except Exception as e:
            self.stats.health_status = "unhealthy"
            return {"status": "unhealthy", "error": str(e)}

    async def cleanup(self) -> None:
        for job_id in list(self.active_jobs.keys()):
            await self.cancel_job(job_id)
