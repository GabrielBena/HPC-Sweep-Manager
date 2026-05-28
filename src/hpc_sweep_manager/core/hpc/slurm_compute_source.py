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
from datetime import datetime
import json
import logging
from pathlib import Path
import shutil
import subprocess
from typing import Any, Dict, List, Optional

from ..common.compute_source import ComputeSource, JobInfo, SubmissionMode
from ..common.resource_spec import ResourceSpec
from ..common.templating import params_to_hydra_args, render_template
from .slurm_protocol import SLURM_STATE_MAP, parse_sbatch_job_id, render_sbatch_directives

logger = logging.getLogger(__name__)


class SlurmComputeSource(ComputeSource):
    def __init__(
        self,
        name: str = "slurm",
        max_parallel_jobs: int = 0,
        python_path: str = "python",
        script_path: str = "",
        project_dir: str = ".",
        default_spec: Optional[ResourceSpec] = None,
        qos_whitelist: Optional[frozenset[str]] = None,
    ):
        # 0 means "no client-side cap" — the cluster's own scheduler decides.
        super().__init__(name, "slurm", max_parallel_jobs or 10_000)
        self.python_path = python_path
        self.script_path = script_path
        self.project_dir = project_dir
        self.default_spec = default_spec or ResourceSpec()
        self.qos_whitelist = qos_whitelist
        self.sweep_dir: Optional[Path] = None
        self.sweep_id: Optional[str] = None

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

    def _effective_spec(self, spec: Optional[ResourceSpec]) -> ResourceSpec:
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
            raise RuntimeError(
                f"SlurmComputeSource {self.name!r} not set up; call setup() first"
            )
        scripts_dir = self.sweep_dir / "scripts"
        logs_dir = self.sweep_dir / "logs"
        tasks_dir = self.sweep_dir / "tasks"
        for d in (scripts_dir, logs_dir, tasks_dir):
            d.mkdir(parents=True, exist_ok=True)
        return scripts_dir, logs_dir, tasks_dir

    async def submit_job(
        self,
        params: Dict[str, Any],
        job_name: str,
        sweep_id: str,
        wandb_group: Optional[str] = None,
        spec: Optional[ResourceSpec] = None,
    ) -> str:
        effective = self._effective_spec(spec)
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
            wandb_group=wandb_group,
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
        params_list: List[Dict[str, Any]],
        sweep_id: str,
        mode: SubmissionMode = "individual",
        spec: Optional[ResourceSpec] = None,
        wandb_group: Optional[str] = None,
        job_name_prefix: Optional[str] = None,
    ) -> List[str]:
        if mode == "array":
            return [
                await self._submit_array(
                    params_list, sweep_id, spec, wandb_group, job_name_prefix
                )
            ]
        return await super().submit_batch(
            params_list, sweep_id, mode, spec, wandb_group, job_name_prefix
        )

    async def _submit_array(
        self,
        params_list: List[Dict[str, Any]],
        sweep_id: str,
        spec: Optional[ResourceSpec],
        wandb_group: Optional[str],
        job_name_prefix: Optional[str],
    ) -> str:
        if not params_list:
            raise ValueError("Cannot submit an empty array")
        effective = self._effective_spec(spec)
        directives = render_sbatch_directives(effective)
        scripts_dir, logs_dir, tasks_dir = self._ensure_dirs()

        prefix = job_name_prefix or sweep_id
        job_name = f"{prefix}_array"

        params_file = self.sweep_dir / "parameter_combinations.json"  # type: ignore[union-attr]
        indexed = [
            {"index": i + 1, "global_index": i + 1, "params": p}
            for i, p in enumerate(params_list)
        ]
        params_file.write_text(json.dumps(indexed, indent=2))

        script_content = render_template(
            "slurm_array.sh.j2",
            job_name=job_name,
            sweep_id=sweep_id,
            num_jobs=len(params_list),
            logs_dir=str(logs_dir),
            tasks_dir=str(tasks_dir),
            params_file=str(params_file),
            sbatch_directives=directives,
            modules=list(effective.modules),
            pre_script=list(effective.pre_script),
            project_dir=self.project_dir,
            python_path=self.python_path,
            script_path=self.script_path,
            wandb_group=wandb_group,
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
                f"sbatch (array) failed: {result.stderr.strip() or 'no stderr'}"
            )
        job_id = parse_sbatch_job_id(result.stdout)

        self.active_jobs[job_id] = JobInfo(
            job_id=job_id,
            job_name=job_name,
            params={"_array_size": len(params_list)},
            source_name=self.name,
            status="PENDING",
            submit_time=datetime.now(),
            task_dir=str(tasks_dir),
        )
        self.stats.total_submitted += len(params_list)
        logger.info(
            f"Submitted Slurm array job {job_id} ({job_name}, {len(params_list)} tasks)"
        )
        return job_id

    async def get_job_status(self, job_id: str) -> str:
        result = await asyncio.to_thread(
            subprocess.run,
            ["squeue", "-j", job_id, "-h", "-o", "%T"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0 or not result.stdout.strip():
            # Job no longer in queue → assume it finished.
            status = "COMPLETED"
        else:
            raw = result.stdout.strip().splitlines()[0].strip()
            status = SLURM_STATE_MAP.get(raw, "RUNNING")
        if job_id in self.active_jobs:
            self.update_job_status(job_id, status)
        return status

    async def cancel_job(self, job_id: str) -> bool:
        result = await asyncio.to_thread(
            subprocess.run, ["scancel", job_id], capture_output=True, text=True
        )
        success = result.returncode == 0
        if success and job_id in self.active_jobs:
            self.update_job_status(job_id, "CANCELLED")
        return success

    async def collect_results(self, job_ids: Optional[List[str]] = None) -> bool:
        # Slurm outputs land directly in the shared filesystem under tasks_dir.
        return True

    async def health_check(self) -> Dict[str, Any]:
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
