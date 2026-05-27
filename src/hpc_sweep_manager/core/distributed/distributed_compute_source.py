"""Distributed compute source: a :class:`ComputeSource` that fans out to children.

This wraps the existing :class:`DistributedJobManager` orchestration (job
queue + round-robin/least-loaded/capability strategies + result collection)
behind the unified :class:`ComputeSource` ABC, so ``hsm sweep run --mode
distributed`` can go through the same ``setup → submit_batch → wait_for_all``
lifecycle as the local and Slurm backends.

KNOWN WART (intentional for now): :meth:`submit_batch` is a *fused*
submit+wait+collect call — it delegates to
``DistributedJobManager.submit_distributed_sweep`` which blocks until every
child job reaches a terminal state, then collects + normalizes results.
:meth:`wait_for_all` therefore just returns the already-final statuses. A
future pass can split submission from waiting (the seed of a cross-host
queuing UX where submit returns immediately and a separate waiter polls),
but that requires decomposing the manager's blocking ``_wait_for_completion``.
"""

from __future__ import annotations

from datetime import datetime
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..common.compute_source import ComputeSource, JobInfo, SubmissionMode, TERMINAL_STATES
from ..common.resource_spec import ResourceSpec
from .distributed_manager import DistributedJobManager, DistributedSweepConfig

logger = logging.getLogger(__name__)


class DistributedComputeSource(ComputeSource):
    """Fan a sweep across several child :class:`ComputeSource` instances.

    Construct with the child sources (already-configured ``LocalComputeSource``
    / ``SSHComputeSource`` / ``SlurmComputeSource`` objects). The internal
    :class:`DistributedJobManager` is created lazily in :meth:`setup` so
    construction stays cheap and signal handlers register only when a real run
    starts.
    """

    def __init__(
        self,
        name: str = "distributed",
        child_sources: Optional[List[ComputeSource]] = None,
        config: Optional[DistributedSweepConfig] = None,
        show_progress: bool = False,
    ):
        self._child_sources: List[ComputeSource] = list(child_sources or [])
        # Aggregate capacity across children; 0 children -> 1 placeholder slot.
        total_slots = sum(s.max_parallel_jobs for s in self._child_sources) or 1
        super().__init__(name, "distributed", total_slots)
        self._config = config
        self._show_progress = show_progress
        self._manager: Optional[DistributedJobManager] = None
        self.sweep_dir: Optional[Path] = None
        self.sweep_id: Optional[str] = None

    def add_source(self, source: ComputeSource) -> None:
        """Register a child compute source (before :meth:`setup`)."""
        self._child_sources.append(source)
        self.max_parallel_jobs += source.max_parallel_jobs

    # ------------------------------------------------------------------ setup

    async def setup(self, sweep_dir: Path, sweep_id: str) -> bool:
        if not self._child_sources:
            logger.error("DistributedComputeSource has no child sources to set up")
            self.stats.health_status = "unhealthy"
            return False

        # Ensure the unified sweep layout exists. Real child sources create
        # these too, but the manager's result-normalization / mapping-save
        # steps assume sweep_dir + subdirs are present regardless of backend.
        sweep_dir.mkdir(parents=True, exist_ok=True)
        for sub in ("tasks", "logs", "distributed_scripts"):
            (sweep_dir / sub).mkdir(parents=True, exist_ok=True)

        self._manager = DistributedJobManager(
            sweep_dir=sweep_dir,
            config=self._config,
            show_progress=self._show_progress,
        )
        for source in self._child_sources:
            self._manager.add_compute_source(source)

        ok = await self._manager.setup_all_sources(sweep_id)
        self.sweep_dir = sweep_dir
        self.sweep_id = sweep_id
        self.stats.health_status = "healthy" if ok else "unhealthy"
        self.stats.last_health_check = datetime.now()
        return ok

    # ------------------------------------------------------------- submission

    async def submit_batch(
        self,
        params_list: List[Dict[str, Any]],
        sweep_id: str,
        mode: SubmissionMode = "individual",
        spec: Optional[ResourceSpec] = None,
        wandb_group: Optional[str] = None,
        job_name_prefix: Optional[str] = None,
    ) -> List[str]:
        """Run the whole sweep across children (blocks until all jobs finish).

        ``mode`` is ignored — distribution always fans individual jobs across
        sources; there is no scheduler-side array concept here. ``spec`` is
        likewise not applied at this level (each child source carries its own
        ``default_spec``).
        """
        if self._manager is None:
            raise RuntimeError(
                f"DistributedComputeSource {self.name!r} not set up; call setup() first"
            )

        job_ids = await self._manager.submit_distributed_sweep(
            param_combinations=params_list,
            sweep_id=sweep_id,
            wandb_group=wandb_group,
        )

        # Mirror the manager's final job records into the ABC bookkeeping. The
        # call above blocked until completion, so everything is terminal; park
        # it all in completed_jobs so wait_for_all returns immediately.
        for jid, info in self._manager.all_jobs.items():
            self.completed_jobs[jid] = info
            if info.status == "COMPLETED":
                self.stats.completed_jobs += 1
            elif info.status == "FAILED":
                self.stats.failed_jobs += 1
        self.stats.total_submitted = len(self._manager.all_jobs)
        return job_ids

    async def submit_job(
        self,
        params: Dict[str, Any],
        job_name: str,
        sweep_id: str,
        wandb_group: Optional[str] = None,
        spec: Optional[ResourceSpec] = None,
    ) -> str:
        """Submit a single job (degenerate batch of one)."""
        ids = await self.submit_batch(
            [params], sweep_id, wandb_group=wandb_group, job_name_prefix=job_name
        )
        return ids[0] if ids else ""

    async def wait_for_all(
        self,
        poll_interval: float = 5.0,
        on_progress=None,
    ) -> Dict[str, str]:
        """Return final statuses.

        Because :meth:`submit_batch` blocks until completion, the statuses are
        already final by the time this is called. We surface them directly
        rather than polling.
        """
        final = {jid: info.status for jid, info in self.completed_jobs.items()}
        if on_progress is not None:
            total = max(len(final), 1)
            on_progress(len(final), total)
        return final

    # ----------------------------------------------------------------- status

    async def get_job_status(self, job_id: str) -> str:
        if self._manager and job_id in self._manager.all_jobs:
            return self._manager.all_jobs[job_id].status
        if job_id in self.completed_jobs:
            return self.completed_jobs[job_id].status
        return "UNKNOWN"

    async def cancel_job(self, job_id: str) -> bool:
        if self._manager is None:
            return False
        source_name = self._manager.job_to_source.get(job_id)
        if not source_name:
            return False
        source = self._manager.source_by_name.get(source_name)
        if source is None:
            return False
        return await source.cancel_job(job_id)

    async def collect_results(self, job_ids: Optional[List[str]] = None) -> bool:
        # The manager collects + normalizes results during submit_distributed_sweep.
        return True

    async def health_check(self) -> Dict[str, Any]:
        child_health: Dict[str, Any] = {}
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
        if self._manager is not None:
            await self._manager.cleanup()
