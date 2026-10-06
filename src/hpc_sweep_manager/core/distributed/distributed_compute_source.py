"""Distributed compute source: one sweep fanned across several child sources.

Each child (the local box, a ``backend: ssh`` remote, a ``backend: slurm`` remote) is a
full :class:`ComputeSource`. This class only hands out the tasks and then defers to them:

- :meth:`submit_batch` runs one worker per child over a shared task queue. A worker takes
  the next task whenever its child has room, so faster children take more tasks. Room means
  fewer active jobs than the child's ``max_parallel_jobs``; for a Slurm child, queued jobs
  count. A local or ssh child also waits for a free slot of its own inside ``submit_job``.
- A failed submit records that task as FAILED, and the child takes no more tasks. Tasks
  that no child could take are FAILED as well. Nothing hangs, and cancellation propagates.
- :meth:`wait_for_all` waits on every child; :meth:`collect_results` then has each child
  pull (and clean up) its own results. Collection starts only once every task of the sweep
  is terminal, so no child deletes a remote dir under a running task.
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

logger = logging.getLogger(__name__)

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
        self._owner: dict[str, ComputeSource] = {}  # job id -> the child running it
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
        queue = deque((f"{prefix}_task_{i:03d}", p) for i, p in enumerate(params_list, 1))

        async def feed(child: ComputeSource) -> None:
            while queue:
                if len(child.active_jobs) >= max(child.max_parallel_jobs, 1):
                    await asyncio.sleep(self.poll_interval)
                    try:
                        await child.update_all_job_statuses()
                    except Exception as e:  # noqa: BLE001 — still full as far as we know
                        logger.warning(f"{child.name}: status refresh failed: {e}")
                    continue
                name, params = queue.popleft()
                try:
                    job_id = await child.submit_job(
                        params=params, job_name=name, sweep_id=sweep_id, wandb_group=wandb_group
                    )
                except Exception as e:  # noqa: BLE001 — the sweep goes on without this child
                    logger.error(
                        f"{child.name}: submitting {name} failed ({e!r}); it takes no more tasks"
                    )
                    self._fail(name, params, child.name)
                    return
                self._owner[job_id] = child

        await asyncio.gather(*(feed(c) for c in self._child_sources))
        if queue:
            logger.error(f"{len(queue)} task(s) never submitted: no child could take them")
        for name, params in queue:
            self._fail(name, params, "")
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
            j: c.active_jobs.get(j) or c.completed_jobs.get(j) for j, c in self._owner.items()
        }
        return {**self._unplaced, **{j: info for j, info in placed.items() if info}}

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
        counts: dict[int, tuple[int, int]] = {}

        def progress(i: int):
            if (callback := on_progress) is None:
                return None

            def report(done: int, total: int) -> None:
                counts[i] = (done, total)
                n = len(self._unplaced)
                callback(
                    n + sum(d for d, _ in counts.values()), n + sum(t for _, t in counts.values())
                )

            return report

        children = self._child_sources
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
        self._write_mapping()
        return {job_id: info.status for job_id, info in self.completed_jobs.items()}

    async def collect_results(
        self, job_ids: list[str] | None = None, *, defer_cleanup: bool = False
    ) -> bool:
        """Have each child pull (and clean up) its results, never while a task is active.

        One child at a time: two remote entries may share a host and its sweep dir.
        """
        if busy := [c.name for c in self._child_sources if c.active_jobs]:
            logger.warning(f"Not collecting yet: {', '.join(busy)} still have active jobs")
            return False
        ok = True
        for child in self._child_sources:
            try:
                ok = await child.collect_results() and ok
            except Exception as e:  # noqa: BLE001 — collect the other children anyway
                logger.warning(f"{child.name}: collecting results failed: {e}")
                ok = False
        return ok

    def _write_mapping(self) -> None:
        """``source_mapping.yaml``: which child ran each task (read by ``hsm sweep status``)."""
        assert self.sweep_dir is not None
        tasks = {
            "task_" + info.job_name.rsplit("_task_", 1)[-1]: {
                "compute_source": info.source_name,
                "status": info.status,
                "complete_time": info.complete_time.isoformat() if info.complete_time else None,
            }
            for info in self._jobs().values()
        }
        meta = {
            "total_tasks": len(tasks),
            "compute_sources": [c.name for c in self._child_sources],
            "timestamp": datetime.now().isoformat(),
        }
        mapping = {"sweep_metadata": meta, "task_assignments": dict(sorted(tasks.items()))}
        (self.sweep_dir / "source_mapping.yaml").write_text(
            yaml.safe_dump(mapping, sort_keys=False)
        )

    async def get_job_status(self, job_id: str) -> str:
        if child := self._owner.get(job_id):
            return await child.get_job_status(job_id)
        info = self.completed_jobs.get(job_id) or self._unplaced.get(job_id)
        return info.status if info else "UNKNOWN"

    async def cancel_job(self, job_id: str) -> bool:
        child = self._owner.get(job_id)
        return bool(child) and await child.cancel_job(job_id)

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
