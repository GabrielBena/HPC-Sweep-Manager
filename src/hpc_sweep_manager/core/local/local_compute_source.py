"""Self-contained local compute source.

Probes ``nvidia-smi -L`` at setup to enumerate GPUs, partitions them into
slots of size ``spec.gpus`` (the per-job GPU count), and injects
``CUDA_VISIBLE_DEVICES`` per worker via a rendered wrapper script. Falls back
to CPU-only execution when no GPUs are present or ``spec.gpus`` is 0.

This module is self-contained: no dependency on the legacy
:class:`LocalJobManager`. The wrapper script template
(``templates/local_compute_source.sh.j2``) handles task_info.txt /
command.txt / module loads / pre_script in the same shape the Slurm template
uses, so a task directory looks the same regardless of backend.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
import logging
import os
from pathlib import Path
import re
import signal
from typing import Any, Dict, List, Optional, Sequence, Union

from ..common.compute_source import ComputeSource, JobInfo
from ..common.resource_spec import ResourceSpec
from ..common.templating import params_to_hydra_args, render_template

logger = logging.getLogger(__name__)


async def _detect_gpus() -> List[int]:
    """Return GPU indices reported by ``nvidia-smi -L``, or [] if unavailable."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "nvidia-smi",
            "-L",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except (FileNotFoundError, OSError):
        return []
    stdout, _ = await proc.communicate()
    if proc.returncode != 0:
        return []
    indices: List[int] = []
    for line in stdout.decode("utf-8", errors="replace").splitlines():
        m = re.match(r"GPU\s+(\d+):", line)
        if m:
            indices.append(int(m.group(1)))
    return indices


class LocalComputeSource(ComputeSource):
    def __init__(
        self,
        name: str = "local",
        max_parallel_jobs: int = 1,
        python_path: str = "python",
        script_path: str = "",
        project_dir: str = ".",
        default_spec: Optional[ResourceSpec] = None,
        visible_gpus: Union[None, int, Sequence[int]] = None,
    ):
        """Build a local slot-queue compute source.

        ``visible_gpus`` is the GPU allowlist (same shape as the ``--gpus``
        CLI flag and ``normalize_gpu_allowlist``): ``None`` = every GPU
        ``nvidia-smi -L`` reports, ``0`` = CPU-only, ``N`` = first N
        detected, ``[i, j, k]`` = exactly those indices. Filter is applied
        in :meth:`setup` after detection; indices in the allowlist that
        aren't actually present are warned-and-dropped.

        Use case: shared GPU boxes where (e.g.) ``GPU:0`` is reserved for
        interactive work — set ``visible_gpus=[1, 2, 3]`` and the slot
        queue never schedules a task on GPU:0.
        """
        super().__init__(name, "local", max(max_parallel_jobs, 1))
        self.python_path = python_path
        self.script_path = script_path
        self.project_dir = project_dir
        self.default_spec = default_spec or ResourceSpec()
        self._visible_gpus = visible_gpus
        self.sweep_dir: Optional[Path] = None
        self.sweep_id: Optional[str] = None
        # GPU bookkeeping (populated in setup)
        self._gpu_indices: List[int] = []
        # Each slot is either a list of GPU indices to expose, or None for CPU.
        self._slot_queue: Optional[asyncio.Queue] = None
        self._slot_count: int = max_parallel_jobs
        # Job bookkeeping
        self._processes: Dict[str, asyncio.subprocess.Process] = {}
        self._monitors: Dict[str, asyncio.Task] = {}
        self._job_counter: int = 0
        self._counter_lock: Optional[asyncio.Lock] = None

    # ------------------------------------------------------------------ setup

    async def setup(self, sweep_dir: Path, sweep_id: str) -> bool:
        sweep_dir.mkdir(parents=True, exist_ok=True)
        for sub in ("logs", "scripts", "tasks"):
            (sweep_dir / sub).mkdir(parents=True, exist_ok=True)
        self.sweep_dir = sweep_dir
        self.sweep_id = sweep_id
        self._counter_lock = asyncio.Lock()

        detected = await _detect_gpus()
        if self._visible_gpus is not None and detected:
            from ..remote.push_exec import normalize_gpu_allowlist

            if isinstance(self._visible_gpus, (list, tuple)) and not isinstance(
                self._visible_gpus, bool
            ):
                missing = sorted(set(self._visible_gpus) - set(detected))
                if missing:
                    logger.warning(
                        f"LocalComputeSource: visible_gpus references indices "
                        f"not present in nvidia-smi -L output: {missing}. "
                        f"Detected: {detected}. Dropping the missing ones."
                    )
            self._gpu_indices = normalize_gpu_allowlist(self._visible_gpus, detected)
            if detected != self._gpu_indices:
                logger.info(
                    f"LocalComputeSource: GPU allowlist applied "
                    f"({len(self._gpu_indices)}/{len(detected)} visible: {self._gpu_indices})"
                )
        else:
            self._gpu_indices = detected
        gpus_per_job = self.default_spec.gpus or 0

        self._slot_queue = asyncio.Queue()
        if self._gpu_indices and gpus_per_job > 0:
            slots: List[List[int]] = []
            for i in range(0, len(self._gpu_indices), gpus_per_job):
                chunk = self._gpu_indices[i : i + gpus_per_job]
                if len(chunk) == gpus_per_job:
                    slots.append(chunk)
            if not slots:
                logger.warning(
                    f"Detected {len(self._gpu_indices)} GPU(s) but gpus_per_job={gpus_per_job} > "
                    f"available; falling back to {self.max_parallel_jobs} CPU worker(s)"
                )
                self._slot_count = self.max_parallel_jobs
                for _ in range(self._slot_count):
                    self._slot_queue.put_nowait(None)
            else:
                self._slot_count = len(slots)
                logger.info(
                    f"LocalComputeSource: {self._slot_count} GPU slot(s), "
                    f"{gpus_per_job} GPU(s) each, from {len(self._gpu_indices)} detected"
                )
                for slot in slots:
                    self._slot_queue.put_nowait(slot)
        else:
            self._slot_count = self.max_parallel_jobs
            if self._gpu_indices and gpus_per_job == 0:
                logger.info(
                    f"LocalComputeSource: {len(self._gpu_indices)} GPU(s) detected but "
                    f"gpus_per_job=0 — running CPU-only"
                )
            for _ in range(self._slot_count):
                self._slot_queue.put_nowait(None)

        self.stats.health_status = "healthy"
        self.stats.last_health_check = datetime.now()
        return True

    # ----------------------------------------------------------------- submit

    async def _next_job_id(self) -> str:
        assert self._counter_lock is not None
        async with self._counter_lock:
            self._job_counter += 1
            return f"local_{os.getpid()}_{self._job_counter}"

    def _task_dir_for(self, job_name: str) -> Path:
        # Match the legacy task-naming convention so distributed runs keep
        # producing tasks/task_NNN/ directories.
        assert self.sweep_dir is not None
        m = re.search(r"task_(\d+)", job_name)
        task_name = f"task_{m.group(1)}" if m else job_name
        return self.sweep_dir / "tasks" / task_name

    async def submit_job(
        self,
        params: Dict[str, Any],
        job_name: str,
        sweep_id: str,
        wandb_group: Optional[str] = None,
        spec: Optional[ResourceSpec] = None,
    ) -> str:
        if self.sweep_dir is None or self._slot_queue is None:
            raise RuntimeError(
                f"LocalComputeSource {self.name!r} not set up; call setup() first"
            )
        # Local execution doesn't enforce walltime / memory / partition. The
        # spec contributes modules + pre_script + (implicitly) gpus via the
        # slot pool wired in setup().
        effective_spec = self.default_spec.merge(spec)

        job_id = await self._next_job_id()
        task_dir = self._task_dir_for(job_name)
        task_dir.mkdir(parents=True, exist_ok=True)
        scripts_dir = self.sweep_dir / "scripts"
        logs_dir = self.sweep_dir / "logs"

        # Block here when all slots are busy — natural back-pressure.
        slot = await self._slot_queue.get()
        cuda_visible = ",".join(str(i) for i in slot) if slot else None

        script_content = render_template(
            "local_compute_source.sh.j2",
            job_name=job_name,
            job_id=job_id,
            task_dir=str(task_dir),
            project_dir=self.project_dir,
            python_path=self.python_path,
            script_path=self.script_path,
            params_hydra=params_to_hydra_args(params),
            wandb_group=wandb_group or sweep_id,
            cuda_visible_devices=cuda_visible,
            modules=list(effective_spec.modules),
            pre_script=list(effective_spec.pre_script),
        )
        script_path = scripts_dir / f"{job_name}.sh"
        script_path.write_text(script_content)
        script_path.chmod(0o755)

        log_path = logs_dir / f"{job_name}.out"
        err_path = logs_dir / f"{job_name}.err"
        log_f = open(log_path, "w")
        err_f = open(err_path, "w")

        try:
            proc = await asyncio.create_subprocess_exec(
                "/bin/bash",
                str(script_path),
                stdout=log_f,
                stderr=err_f,
                cwd=self.project_dir,
                start_new_session=True,
            )
        except BaseException:
            log_f.close()
            err_f.close()
            self._slot_queue.put_nowait(slot)
            raise

        self._processes[job_id] = proc
        self.active_jobs[job_id] = JobInfo(
            job_id=job_id,
            job_name=job_name,
            params=params,
            source_name=self.name,
            status="RUNNING",
            submit_time=datetime.now(),
            start_time=datetime.now(),
            task_dir=str(task_dir),
        )
        self.stats.total_submitted += 1
        self._monitors[job_id] = asyncio.create_task(
            self._monitor(job_id, proc, slot, log_f, err_f)
        )
        gpu_msg = f" on GPU(s) {cuda_visible}" if cuda_visible else ""
        logger.info(f"Started local job {job_id} ({job_name}){gpu_msg}")
        return job_id

    async def _monitor(
        self,
        job_id: str,
        proc: asyncio.subprocess.Process,
        slot,
        log_f,
        err_f,
    ) -> None:
        try:
            rc = await proc.wait()
            status = "COMPLETED" if rc == 0 else "FAILED"
            # If cancel_job marked it CANCELLED first, don't clobber.
            existing = self.active_jobs.get(job_id) or self.completed_jobs.get(job_id)
            if existing and existing.status == "CANCELLED":
                status = "CANCELLED"
            if job_id in self.active_jobs:
                self.update_job_status(job_id, status)
            logger.info(f"Local job {job_id} finished status={status} rc={rc}")
        finally:
            try:
                log_f.close()
            except Exception:  # pragma: no cover
                pass
            try:
                err_f.close()
            except Exception:  # pragma: no cover
                pass
            self._processes.pop(job_id, None)
            assert self._slot_queue is not None
            self._slot_queue.put_nowait(slot)

    # ----------------------------------------------------------------- status

    async def get_job_status(self, job_id: str) -> str:
        if job_id in self.active_jobs:
            proc = self._processes.get(job_id)
            if proc is None:
                # Monitor finished between get and now — fall through.
                pass
            elif proc.returncode is None:
                return "RUNNING"
            else:
                status = "COMPLETED" if proc.returncode == 0 else "FAILED"
                self.update_job_status(job_id, status)
                return status
        if job_id in self.completed_jobs:
            return self.completed_jobs[job_id].status
        return "UNKNOWN"

    async def cancel_job(self, job_id: str) -> bool:
        proc = self._processes.get(job_id)
        if proc is None or proc.returncode is not None:
            return False
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            return False

        # Mark cancelled now so the monitor's status assignment doesn't race.
        if job_id in self.active_jobs:
            self.update_job_status(job_id, "CANCELLED")

        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except asyncio.TimeoutError:  # pragma: no cover
                logger.warning(f"Local job {job_id} did not exit after SIGKILL")
        return True

    async def wait_for_all(
        self,
        poll_interval: float = 5.0,
        on_progress=None,
    ) -> Dict[str, str]:
        """Wait by directly awaiting the monitor tasks of all active jobs.

        Overrides the base class's polling loop because we have a stronger
        signal — the per-job monitor coroutine — that avoids the race where
        a job moves from ``active_jobs`` to ``completed_jobs`` between two
        ``get_job_status`` calls.
        """
        pending_ids = list(self.active_jobs.keys())
        # Always return statuses for every job this source has touched, not
        # just the ones that were still active at call time. Matches the
        # contract used by the base ComputeSource.wait_for_all.
        if not pending_ids:
            final = {jid: info.status for jid, info in self.completed_jobs.items()}
            if on_progress is not None:
                on_progress(len(final), max(len(final), 1))
            return final

        monitors = [self._monitors[jid] for jid in pending_ids if jid in self._monitors]
        total = len(pending_ids) + len(self.completed_jobs)
        if on_progress is not None:
            on_progress(len(self.completed_jobs), max(total, 1))

        for fut in asyncio.as_completed(monitors):
            await fut
            if on_progress is not None:
                on_progress(len(self.completed_jobs), max(total, 1))
        return {jid: info.status for jid, info in self.completed_jobs.items()}

    async def collect_results(self, job_ids: Optional[List[str]] = None) -> bool:
        # Local jobs write outputs into self.sweep_dir/tasks/* directly.
        return True

    async def health_check(self) -> Dict[str, Any]:
        info: Dict[str, Any] = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "active_jobs": len(self.active_jobs),
            "max_jobs": self.max_parallel_jobs,
            "utilization": f"{self.utilization:.1%}",
            "gpus_detected": list(self._gpu_indices),
            "slot_count": self._slot_count,
            "python_path": self.python_path,
            "project_dir": self.project_dir,
        }
        try:
            import psutil

            cpu = psutil.cpu_percent(interval=0.1)
            mem = psutil.virtual_memory()
            info["cpu_percent"] = cpu
            info["memory_percent"] = mem.percent
            if cpu > 90 or mem.percent > 95:
                info["status"] = "overloaded"
        except Exception as e:  # pragma: no cover
            logger.debug(f"psutil health check failed: {e}")
        self.stats.health_status = info["status"]
        self.stats.last_health_check = datetime.now()
        return info

    async def cleanup(self) -> None:
        for job_id in list(self._processes.keys()):
            await self.cancel_job(job_id)
        if self._monitors:
            await asyncio.gather(*self._monitors.values(), return_exceptions=True)
        self._monitors.clear()

    async def update_all_job_statuses(self) -> None:
        for job_id in list(self.active_jobs.keys()):
            try:
                await self.get_job_status(job_id)
            except Exception as e:  # pragma: no cover
                logger.warning(f"Failed to update status for {job_id}: {e}")

    def __str__(self) -> str:
        return f"Local:{self.name}: {self.current_job_count}/{self._slot_count} jobs (gpus={self._gpu_indices or 'none'})"
