"""What the native and SSH-driven Slurm sources share: one status refresh.

Each poll asks the scheduler twice, whatever the number of jobs: one ``squeue --me`` for the jobs
still queued, one ``sacct`` for the ones that left. A failed call is never a verdict. When squeue
fails (slurmctld down, a maintenance) no job changes state that cycle; a job that left the queue
waits for sacct to name its terminal state, and is assumed COMPLETED only after ``SACCT_GRACE``
polls without one (accounting disabled). Subclasses provide :meth:`_sh`, the one transport seam.
"""

from __future__ import annotations

import asyncio
import logging
from abc import abstractmethod
from collections.abc import Iterable, Sequence

from ..common.compute_source import TERMINAL_STATES, ComputeSource, JobInfo
from .scheduler_queue import parse_sacct_job_states, sacct_args, strip_array_suffix
from .slurm_protocol import SLURM_STATE_MAP

logger = logging.getLogger(__name__)

# Polls a job may sit outside squeue without a sacct verdict before it is assumed COMPLETED.
SACCT_GRACE = 3
SQUEUE_ARGV = ["squeue", "--me", "-h", "-o", "%i %T"]


def queued_states(live: Iterable[str], squeue_out: str) -> dict[str, str]:
    """``{job: PENDING|RUNNING}`` for the live jobs still in ``squeue -o '%i %T'`` output.

    Array tasks (``123_4``, ``123_[5-9%2]``) count towards their parent; a parent is PENDING
    only while every row of it is. Rows in a terminal state don't keep a job queued.
    """
    rows: dict[str, set[str]] = {}
    for line in squeue_out.splitlines():
        parts = line.split()
        if len(parts) >= 2:
            state = SLURM_STATE_MAP.get(parts[1], "RUNNING")
            rows.setdefault(strip_array_suffix(parts[0]), set()).add(state)
    live = set(live)
    return {
        job: "PENDING" if states == {"PENDING"} else "RUNNING"
        for job, states in rows.items()
        if job in live and states - TERMINAL_STATES
    }


def sacct_verdicts(gone: Iterable[str], sacct_out: str) -> dict[str, str]:
    """``{job: state}`` from ``sacct -n -X -P -o JobID,State``; jobs without rows are omitted.

    A job with any task still pending or running is RUNNING (sacct settles after squeue
    forgets); otherwise FAILED beats CANCELLED beats COMPLETED.
    """
    counts = parse_sacct_job_states(sacct_out)
    verdicts = {}
    for job in gone:
        states = set(counts.get(job, ()))
        if states & {"PENDING", "RUNNING"}:
            verdicts[job] = "RUNNING"
        elif states:
            verdicts[job] = next(s for s in ("FAILED", "CANCELLED", "COMPLETED") if s in states)
    return verdicts


class SlurmBase(ComputeSource):
    """A :class:`ComputeSource` whose jobs live in a Slurm scheduler."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sacct_misses: dict[str, int] = {}  # job -> polls out of squeue with no sacct verdict

    @abstractmethod
    async def _sh(self, argv: Sequence[str]) -> tuple[int, str, str]:
        """Run a Slurm command where the jobs live; return ``(rc, stdout, stderr)``."""

    async def update_all_job_statuses(self) -> None:
        live = list(self.active_jobs)
        if not live:
            return
        rc, out, err = await self._sh(SQUEUE_ARGV)
        if rc != 0:
            logger.warning(f"squeue failed (rc={rc}): {err.strip()}; job states kept")
            return
        states = queued_states(live, out)
        gone = [job for job in live if job not in states]
        if gone:
            rc, out, err = await self._sh(["sacct", *sacct_args(gone)])
            verdicts = sacct_verdicts(gone, out) if rc == 0 else {}
            for job in gone:
                if job in verdicts:
                    self._sacct_misses.pop(job, None)
                    states[job] = verdicts[job]
                    continue
                misses = self._sacct_misses[job] = self._sacct_misses.get(job, 0) + 1
                if misses >= SACCT_GRACE:
                    logger.warning(
                        f"job {job} left the queue and sacct gave no state for {misses} polls "
                        f"(rc={rc}: {err.strip()}); assuming COMPLETED, verify it"
                    )
                    states[job] = "COMPLETED"
        for job, state in states.items():
            self.update_job_status(job, state)

    async def get_job_status(self, job_id: str) -> str:
        await self.update_all_job_statuses()
        info = self.active_jobs.get(job_id) or self.completed_jobs.get(job_id)
        return info.status if info else "UNKNOWN"

    async def adopt(self, job_ids: Sequence[str], pause: float = 2.0) -> dict[str, str]:
        """Track already-submitted jobs (a re-attach) and return their settled statuses.

        Polls up to ``SACCT_GRACE`` times, until every job is either queued or named by sacct, so
        a fresh process reaches the same verdict a live launcher would.
        """
        for job in job_ids:
            self.active_jobs.setdefault(job, JobInfo(job, job, {}, self.name))
        for poll in range(SACCT_GRACE):
            await self.update_all_job_statuses()
            if not self._sacct_misses.keys() & self.active_jobs.keys():
                break
            if poll < SACCT_GRACE - 1:
                await asyncio.sleep(pause)
        tracked = {**self.completed_jobs, **self.active_jobs}
        return {job: tracked[job].status for job in job_ids}
