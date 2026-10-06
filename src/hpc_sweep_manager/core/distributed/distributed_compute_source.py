"""Distributed compute source: one sweep fanned across several child sources.

Each child (the local box, a ``backend: ssh`` remote, a ``backend: slurm`` remote) is a
full :class:`ComputeSource`. This class only hands out the tasks and then defers to them:

- :meth:`submit_batch` runs one worker per child over a shared task queue. A worker takes
  the next task whenever its child has room, so faster children take more tasks. Room means
  fewer active jobs than the child's ``max_parallel_jobs`` (50 for a Slurm child that sets
  none); for a Slurm child, queued jobs count. A local or ssh child also waits for a free
  slot of its own inside ``submit_job``.
- A child retires (takes no more tasks) when a submit or a status refresh fails, or when
  40% of its finished jobs FAILED (from 5 on). A task whose submit failed goes back to the
  queue, up to 3 tries; tasks no child could take end FAILED. Nothing hangs, and
  cancellation propagates.
- :meth:`wait_for_all` waits on every child; :meth:`collect_results` then has each child
  pull (and clean up) its own results. Collection starts only once every task of the sweep
  is terminal, and every remote dir is kept when a task did not complete or two children
  share one, so no child deletes another's tasks.
- ``source_mapping.yaml`` names each task's child, host and job id. It is written while
  tasks are handed out and when the waiting ends, Ctrl-C included.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import deque
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from ..common.compute_source import ComputeSource, JobInfo, SubmissionMode
from ..common.resource_spec import ResourceSpec
from ..hpc.fair_share import DEFAULT_THROTTLE
from ..hpc.slurm_base import SlurmBase

logger = logging.getLogger(__name__)

SUBMIT_TRIES = 3  # a task whose submit fails is handed to another child, up to this many tries
FAILSAFE_MIN_JOBS, FAILSAFE_RATIO = 5, 0.4  # retire a child once 40% of its 5+ finished jobs FAILED

# `distributed:` keys of the old dispatcher. They no longer change anything.
_RETIRED_KEYS = (
    "strategy",
    "collect_interval",
    "health_check_interval",
    "max_retries",
    "sync_method",
    "enable_auto_sync",
    "enable_interactive_sync",
    "enable_source_failsafe",
    "source_failure_threshold",
    "min_jobs_for_failsafe",
    "auto_disable_unhealthy_sources",
    "health_check_failure_threshold",
)


def _build_local_child(hsm_config, distributed_cfg: dict) -> ComputeSource | None:
    """Construct the local child source from config, or None on failure."""
    from ..common.path_detector import PathDetector
    from ..local.local_compute_source import LocalComputeSource

    try:
        detector = PathDetector()
        python_path = hsm_config.get_default_python_path() or detector.detect_python_path()
        script_path = hsm_config.get_default_script_path() or detector.detect_train_script()
        project_dir = hsm_config.get_project_root() or str(Path.cwd())

        local_max_jobs = distributed_cfg.get("local_max_jobs", 1)

        return LocalComputeSource(
            name="local",
            max_parallel_jobs=local_max_jobs,
            python_path=python_path,
            script_path=script_path,
            project_dir=project_dir,
            # Like `--mode local`: the `local:` block's per-task spec and GPU allowlist
            # (on a shared box the allowlist is what keeps a reserved GPU out).
            default_spec=hsm_config.get_local_spec(),
            visible_gpus=hsm_config.get_local_visible_gpus(),
            conda_env=getattr(hsm_config, "get_conda_env", lambda: None)(),
        )
    except ValueError:
        raise  # a config error fails the run; never drop the local child silently
    except Exception as e:  # noqa: BLE001 - local source is optional
        logger.warning(f"Could not build local compute source: {e}")
        return None


async def _build_ssh_children(hsm_config, remotes: dict) -> list[ComputeSource]:
    """Construct push-model SSH/SSH-Slurm child sources from local hsm_config.

    Dispatches per-remote on the optional ``backend:`` field
    (``ssh`` default, ``slurm`` routes through
    :class:`SSHSlurmComputeSource`). Bare ssh-config aliases work because
    ``host`` defaults to ``name``. No discovery step — every field comes
    from ``distributed.remotes[name]`` (per-remote) or ``distributed:``
    (global).
    """
    from ..common.path_detector import PathDetector
    from ..remote.ssh_compute_source import build_ssh_source
    from ..remote.ssh_slurm_compute_source import build_ssh_slurm_source

    detector = PathDetector()
    project_dir = hsm_config.get_project_root() or str(Path.cwd())
    script_path = hsm_config.get_default_script_path() or detector.detect_train_script()
    distributed_cfg = dict(hsm_config.config_data.get("distributed", {}))
    # Project-level conda_env (from paths.conda_env) is the lowest-priority
    # fallback for SSH/SSH-Slurm children. Per-remote and `distributed.conda_env`
    # still win because we only set it when absent. Defensive getattr handles
    # FakeConfig in tests + older config objects without the accessor.
    project_conda_env = getattr(hsm_config, "get_conda_env", lambda: None)()
    if project_conda_env and "conda_env" not in distributed_cfg:
        distributed_cfg["conda_env"] = project_conda_env

    sources: list[ComputeSource] = []
    for remote_name, remote_config in remotes.items():
        backend = (remote_config.get("backend") or "ssh").lower()
        try:
            if backend == "slurm":
                source = build_ssh_slurm_source(
                    name=remote_name,
                    remote_cfg=remote_config,
                    distributed_cfg=distributed_cfg,
                    project_dir=project_dir,
                    script_path=script_path,
                )
                if not remote_config.get("max_parallel_jobs"):  # the fair-share rule: <= 50 at once
                    source.max_parallel_jobs = DEFAULT_THROTTLE
                logger.info(f"Remote source ready: {remote_name} (backend=slurm)")
            elif backend == "ssh":
                source = build_ssh_source(
                    name=remote_name,
                    remote_cfg=remote_config,
                    distributed_cfg=distributed_cfg,
                    project_dir=project_dir,
                    script_path=script_path,
                )
                logger.info(f"Remote source ready: {remote_name} (backend=ssh)")
            else:
                logger.warning(
                    f"Unknown backend {backend!r} for remote {remote_name!r}; "
                    f"expected 'ssh' or 'slurm'. Skipping."
                )
                continue
            sources.append(source)
        except ValueError:
            raise  # a config error (e.g. a bad spec value) fails the run; never drop the remote
        except Exception as e:  # noqa: BLE001 - a bad remote shouldn't kill the run
            logger.warning(f"Failed to add {backend} source {remote_name!r}: {e}")
    return sources


class DistributedComputeSource(ComputeSource):
    """Fan a sweep across child :class:`ComputeSource` instances (see the module docstring).

    Pass the children directly (tests), or an ``hsm_config`` to build them from its
    ``distributed:`` block in :meth:`setup`. ``poll_interval`` is how often a child that is
    full re-checks its jobs before taking the next task.
    """

    def __init__(
        self,
        name: str = "distributed",
        child_sources: list[ComputeSource] | None = None,
        hsm_config: Any = None,
        poll_interval: float = 10.0,
    ):
        self._child_sources: list[ComputeSource] = list(child_sources or [])
        super().__init__(
            name, "distributed", sum(s.max_parallel_jobs for s in self._child_sources) or 1
        )
        self._hsm_config = hsm_config
        self.poll_interval = poll_interval
        self._owner: dict[str, tuple[ComputeSource, str]] = {}  # "child:job id" -> (child, job id)
        self._unplaced: dict[str, JobInfo] = {}  # FAILED records of tasks never submitted
        self.sweep_dir: Path | None = None
        self.sweep_id: str | None = None

    def add_source(self, source: ComputeSource) -> None:
        """Register a child compute source (before :meth:`setup`)."""
        self._child_sources.append(source)
        self.max_parallel_jobs += source.max_parallel_jobs

    async def _build_children_from_config(self) -> None:
        """Populate child sources (local + SSH remotes) from hsm_config."""
        cfg = self._hsm_config.config_data.get("distributed", {})
        if retired := [k for k in _RETIRED_KEYS if k in cfg]:
            logger.warning(
                f"distributed: {', '.join(retired)} no longer change anything (each child "
                f"takes the next task when it has room); remove them."
            )
        children = []
        if cfg.get("local_max_jobs", 1) > 0 and (
            local := _build_local_child(self._hsm_config, cfg)
        ):
            children.append(local)
        remotes = {n: c for n, c in (cfg.get("remotes") or {}).items() if c.get("enabled", True)}
        children += await _build_ssh_children(self._hsm_config, remotes)
        self._child_sources = children
        self.max_parallel_jobs = sum(s.max_parallel_jobs for s in children) or 1

    async def setup(self, sweep_dir: Path, sweep_id: str) -> bool:
        """Set every child up; one that fails gets no tasks (and is cleaned up)."""
        if not self._child_sources and self._hsm_config is not None:
            await self._build_children_from_config()
        sweep_dir.mkdir(parents=True, exist_ok=True)
        results = await asyncio.gather(
            *(c.setup(sweep_dir, sweep_id) for c in self._child_sources), return_exceptions=True
        )
        ready = []
        for child, ok in zip(self._child_sources, results, strict=True):
            if ok is True:
                ready.append(child)
                continue
            logger.error(f"{child.name}: setup failed ({ok or 'returned False'}); it gets no tasks")
            with contextlib.suppress(Exception):
                await child.cleanup()
        if not ready:
            logger.error("DistributedComputeSource: no child source is ready")
        self._child_sources = ready
        self.max_parallel_jobs = sum(c.max_parallel_jobs for c in ready) or 1  # set up: slot counts
        self.sweep_dir, self.sweep_id = sweep_dir, sweep_id
        self.stats.health_status = "healthy" if ready else "unhealthy"
        self.stats.last_health_check = datetime.now()
        return bool(ready)

    async def submit_batch(
        self,
        params_list: list[dict[str, Any]],
        sweep_id: str,
        mode: SubmissionMode = "individual",
        spec: ResourceSpec | None = None,
        wandb_group: str | None = None,
        job_name_prefix: str | None = None,
        costs: Sequence[float] | None = None,
    ) -> list[str]:
        """Place every task on a child; return the submitted job ids.

        ``mode``, ``spec`` and ``costs`` are ignored: each child submits individual jobs with
        its own ``default_spec``. Returns once every task is submitted or FAILED.
        """
        if self.sweep_dir is None:
            raise RuntimeError(
                f"DistributedComputeSource {self.name!r} not set up; call setup() first"
            )
        prefix = job_name_prefix or sweep_id
        queue = deque((f"{prefix}_task_{i:03d}", p, 1) for i, p in enumerate(params_list, 1))
        retired: set[str] = set()

        def retire(child: ComputeSource, why: str) -> None:
            retired.add(child.name)
            logger.error(f"{child.name}: {why}; it takes no more tasks")

        async def feed(child: ComputeSource) -> None:
            while queue:
                n = len(child.completed_jobs)
                failed = sum(j.status == "FAILED" for j in child.completed_jobs.values())
                if n >= FAILSAFE_MIN_JOBS and failed >= FAILSAFE_RATIO * n:
                    return retire(child, f"{failed} of its {n} finished jobs FAILED")
                if len(child.active_jobs) >= max(child.max_parallel_jobs, 1):
                    await asyncio.sleep(self.poll_interval)
                    try:
                        await child.poll()
                    except Exception as e:  # noqa: BLE001 — e.g. its ssh connection died
                        return retire(child, f"its status refresh failed ({e!r})")
                    self._write_mapping()
                    continue
                name, params, tries = queue.popleft()
                try:
                    job_id = await child.submit_job(
                        params=params, job_name=name, sweep_id=sweep_id, wandb_group=wandb_group
                    )
                except Exception as e:  # noqa: BLE001 — another child may take the task
                    if tries < SUBMIT_TRIES:
                        queue.appendleft((name, params, tries + 1))
                    else:
                        self._fail(name, params, child.name)
                    return retire(child, f"submitting {name} failed ({e!r}; try {tries})")
                self._owner[f"{child.name}:{job_id}"] = (child, job_id)

        try:
            # A retiring child may hand its task back after the others ran dry: go round again.
            while queue and (live := [c for c in self._child_sources if c.name not in retired]):
                async with asyncio.TaskGroup() as tg:
                    for child in live:
                        tg.create_task(feed(child))
            if queue:
                logger.error(f"{len(queue)} task(s) never submitted: no child could take them")
            for name, params, _ in queue:
                self._fail(name, params, "")
        except BaseException:
            for c in self._child_sources:  # like SSHSlurmComputeSource.submit_batch; no scancel
                if isinstance(c, SlurmBase) and c.active_jobs:
                    host, ids = getattr(c, "host", "localhost"), " ".join(c.active_jobs)
                    logger.error(
                        f"{c.name}: submission stopped; to cancel: ssh {host} scancel {ids}"
                    )
            raise
        finally:
            self._write_mapping()
        return list(self._owner)

    def _fail(self, job_name: str, params: dict[str, Any], source_name: str) -> None:
        now = datetime.now()
        job_id = f"{source_name or 'unplaced'}:{job_name}"
        self._unplaced[job_id] = JobInfo(
            job_id, job_name, params, source_name, "FAILED", submit_time=now, complete_time=now
        )

    def _jobs(self) -> dict[str, JobInfo]:
        """Every task of the sweep: its job on the owning child, or its FAILED record."""
        placed = {
            k: c.active_jobs.get(j) or c.completed_jobs.get(j) for k, (c, j) in self._owner.items()
        }
        return {**self._unplaced, **{k: info for k, info in placed.items() if info}}

    async def submit_job(
        self,
        params: dict[str, Any],
        job_name: str,
        sweep_id: str,
        wandb_group: str | None = None,
        spec: ResourceSpec | None = None,
    ) -> str:
        """Submit a single job (a batch of one)."""
        ids = await self.submit_batch(
            [params], sweep_id, wandb_group=wandb_group, job_name_prefix=job_name
        )
        return ids[0] if ids else ""

    async def wait_for_all(self, poll_interval: float = 5.0, on_progress=None) -> dict[str, str]:
        """Wait on every child at once; return ``job_id -> final status`` for the whole sweep.

        A child whose wait raises (a lost connection) can't be followed any more: its active
        jobs are reported FAILED, which also makes its collect keep the remote dir.
        """
        dones: dict[int, int] = {}

        def progress(i: int):
            if (callback := on_progress) is None:
                return None

            def report(done: int, _total: int) -> None:  # the sweep's total is known here
                dones[i] = done
                n = len(self._unplaced)
                callback(n + sum(dones.values()), n + len(self._owner))

            return report

        children = self._child_sources
        try:
            results = await asyncio.gather(
                *(
                    c.wait_for_all(poll_interval=poll_interval, on_progress=progress(i))
                    for i, c in enumerate(children)
                ),
                return_exceptions=True,
            )
            for child, result in zip(children, results, strict=True):
                if isinstance(result, BaseException):
                    logger.error(
                        f"{child.name}: lost track of {len(child.active_jobs)} job(s) ({result!r})"
                    )
                    for job_id in list(child.active_jobs):
                        child.update_job_status(job_id, "FAILED")
            self.completed_jobs = self._jobs()
        finally:
            self._write_mapping()
        return {job_id: info.status for job_id, info in self.completed_jobs.items()}

    async def collect_results(
        self, job_ids: list[str] | None = None, *, defer_cleanup: bool = False
    ) -> bool:
        """Have each child pull (and clean up) its results, never while a task is active.

        Whether the remote dirs go is decided once for the sweep: every child keeps its dir
        when a task did not complete (a lost child's jobs are FAILED, and may still run) or
        when two children share a (host, sweep dir), whose ``rm -rf`` would take the other's
        tasks. One child at a time, as two may share that dir.
        """
        if busy := [c.name for c in self._child_sources if c.active_jobs]:
            logger.warning(f"Not collecting yet: {', '.join(busy)} still have active jobs")
            return False
        remotes = [c for c in self._child_sources if hasattr(c, "keep_remote_on_success")]
        why = []
        if any(info.status != "COMPLETED" for info in self._jobs().values()):
            why.append("a task did not complete")
        if len({(c.host, c._remote_sweep_dir) for c in remotes}) < len(remotes):
            why.append("two remotes share a sweep dir")
        if why and remotes:
            for c in remotes:
                c.keep_remote_on_success = True
            logger.warning(
                f"Keeping the remote sweep dirs ({'; '.join(why)}); `hsm remote clean` removes them"
            )
        ok = True
        for child in self._child_sources:
            try:
                ok = await child.collect_results() and ok
            except Exception as e:  # noqa: BLE001 — collect the other children anyway
                logger.warning(f"{child.name}: collecting results failed: {e}")
                ok = False
        return ok

    def _write_mapping(self) -> None:
        """``source_mapping.yaml``: where each task ran (read by ``hsm sweep status``).

        Keyed by the task's dir under ``tasks/`` (``task_003``; ``<sweep_id>_task_003`` on a
        Slurm child); a task never submitted has no job id.
        """
        if self.sweep_dir is None:
            return
        hosts = {c.name: getattr(c, "host", "localhost") for c in self._child_sources}
        tasks = {
            Path(info.task_dir or "task_" + info.job_name.rsplit("_task_", 1)[-1]).name: {
                "compute_source": info.source_name,
                "host": hosts.get(info.source_name),
                "job_id": None if key in self._unplaced else info.job_id,
                "status": info.status,
                "complete_time": info.complete_time.isoformat() if info.complete_time else None,
            }
            for key, info in sorted(self._jobs().items(), key=lambda kv: kv[1].job_name)
        }
        meta = {
            "total_tasks": len(tasks),
            "compute_sources": [c.name for c in self._child_sources],
            "timestamp": datetime.now().isoformat(),
        }
        mapping = {"sweep_metadata": meta, "task_assignments": tasks}
        (self.sweep_dir / "source_mapping.yaml").write_text(
            yaml.safe_dump(mapping, sort_keys=False)
        )

    async def get_job_status(self, job_id: str) -> str:
        if owned := self._owner.get(job_id):
            return await owned[0].get_job_status(owned[1])
        info = self.completed_jobs.get(job_id) or self._unplaced.get(job_id)
        return info.status if info else "UNKNOWN"

    async def cancel_job(self, job_id: str) -> bool:
        owned = self._owner.get(job_id)
        return bool(owned) and await owned[0].cancel_job(owned[1])

    async def health_check(self) -> dict[str, Any]:
        child_health: dict[str, Any] = {}
        healthy = 0
        for source in self._child_sources:
            try:
                result = await source.health_check()
                child_health[source.name] = result
                if result.get("status") == "healthy":
                    healthy += 1
            except Exception as e:  # noqa: BLE001 - report, don't crash health check
                child_health[source.name] = {"status": "unhealthy", "error": str(e)}

        status = "healthy" if healthy > 0 else "unhealthy"
        self.stats.health_status = status
        self.stats.last_health_check = datetime.now()
        return {
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "healthy_sources": healthy,
            "total_sources": len(self._child_sources),
            "sources": child_health,
        }

    async def cleanup(self) -> None:
        await asyncio.gather(*(c.cleanup() for c in self._child_sources), return_exceptions=True)
