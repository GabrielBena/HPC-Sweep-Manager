"""What the native and SSH-driven Slurm sources share: one status refresh.

Each poll asks the scheduler twice, whatever the number of jobs: one ``squeue -u <user>`` for the
jobs still queued, one ``sacct`` for the ones that left. A failed call is never a verdict: when
squeue or sacct fails (slurmctld or slurmdbd down, a maintenance) no job changes state that cycle.
A job that left the queue waits for sacct to name its terminal state; it is assumed COMPLETED only
when accounting has no answer for it (no rows, or accounting absent) on ``SACCT_GRACE`` polls in a
row. Subclasses provide :meth:`_sh`, the one transport seam, and may set :attr:`slurm_user`.
"""

from __future__ import annotations

import asyncio
import getpass
import logging
from abc import abstractmethod
from collections.abc import Iterable, Sequence

from ..common.compute_source import TERMINAL_STATES, ComputeSource, JobInfo
from .scheduler_queue import parse_sacct_job_states, sacct_args, strip_array_suffix
from .slurm_protocol import SLURM_STATE_MAP

logger = logging.getLogger(__name__)

# Polls in a row a job may be out of squeue with no accounting record before it counts as done.
SACCT_GRACE = 3


def accounting_absent(rc: int, err: str) -> bool:
    """True when sacct can't answer at all (missing, or accounting disabled), not merely failing."""
    return rc == 127 or "accounting storage is disabled" in err


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

    slurm_user: str | None = None  # whose queue to read; defaults to the local user

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sacct_misses: dict[str, int] = {}  # job -> polls in a row with no accounting record

    @abstractmethod
    async def _sh(self, argv: Sequence[str]) -> tuple[int, str, str]:
        """Run a Slurm command where the jobs live; return ``(rc, stdout, stderr)``."""

    async def update_all_job_statuses(self) -> None:
        live = list(self.active_jobs)
        if not live:
            return
        user = self.slurm_user or getpass.getuser()
        rc, out, err = await self._sh(["squeue", "-u", user, "-h", "-o", "%i %T"])
        if rc != 0:
            logger.warning(f"squeue failed (rc={rc}): {err.strip()}; job states kept")
            return
        states = queued_states(live, out)
        for job in states:
            self._sacct_misses.pop(job, None)
        gone = [job for job in live if job not in states]
        if gone:
            rc, out, err = await self._sh(["sacct", *sacct_args(gone)])
            if rc != 0 and not accounting_absent(rc, err):
                logger.warning(f"sacct failed (rc={rc}): {err.strip()}; job states kept")
                gone = []
            verdicts = sacct_verdicts(gone, out) if rc == 0 else {}
            for job in gone:
                if job in verdicts:
                    self._sacct_misses.pop(job, None)
                    states[job] = verdicts[job]
                    continue
                misses = self._sacct_misses[job] = self._sacct_misses.get(job, 0) + 1
                if misses >= SACCT_GRACE:
                    logger.warning(
                        f"job {job} left the queue and accounting has no record of it after "
                        f"{misses} polls; assuming COMPLETED, verify it"
                    )
                    states[job] = "COMPLETED"
        for job, state in states.items():
            self.update_job_status(job, state)

    async def get_job_status(self, job_id: str) -> str:
        """The state the last refresh saw (refresh with :meth:`update_all_job_statuses`)."""
        info = self.active_jobs.get(job_id) or self.completed_jobs.get(job_id)
        return info.status if info else "UNKNOWN"

    async def adopt(self, job_ids: Sequence[str], pause: float = 20.0) -> dict[str, str]:
        """Track already-submitted jobs (a re-attach) and return their settled statuses.

        Polls up to ``SACCT_GRACE`` times, ``pause`` apart, until every job is either queued or
        named by sacct, so a fresh process reaches the verdict a live launcher would. A job Slurm
        couldn't be asked about stays ``UNKNOWN``.
        """
        for job in job_ids:
            self.active_jobs.setdefault(job, JobInfo(job, job, {}, self.name, status="UNKNOWN"))
        for poll in range(SACCT_GRACE):
            await self.update_all_job_statuses()
            if not self._sacct_misses.keys() & self.active_jobs.keys():
                break
            if poll < SACCT_GRACE - 1:
                await asyncio.sleep(pause)
        tracked = {**self.completed_jobs, **self.active_jobs}
        return {job: tracked[job].status for job in job_ids}
