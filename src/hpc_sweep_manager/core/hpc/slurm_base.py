"""What the native and SSH-driven Slurm sources share: one status refresh.

Each poll asks the scheduler twice, whatever the number of jobs: one ``squeue -u <user>`` for the
jobs still queued, one ``sacct`` for the ones that left. A failed call is never a verdict: when
squeue or sacct fails (slurmctld or slurmdbd down, a maintenance) no job changes state that cycle.
A job that left the queue waits for sacct to name its terminal state; it is assumed COMPLETED only
when accounting has no answer for it (no rows, or accounting absent) on ``SACCT_GRACE`` polls in a
row. At collect, :meth:`SlurmBase.record_task_states` writes how Slurm ended each task (one more
sacct). Subclasses provide :meth:`_sh`, the one transport seam, and may set :attr:`slurm_user`.
Both write ``.hsm_manifest.json`` as they submit, naming the jobs (``hsm sweep cancel``).
"""

from __future__ import annotations

import asyncio
import getpass
import json
import logging
from abc import abstractmethod
from collections.abc import Callable, Iterable, Sequence
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

from ..common.compute_source import TERMINAL_STATES, ComputeSource, JobInfo
from ..common.resource_spec import ResourceSpec
from ..common.utils import format_walltime, parse_walltime, write_atomic
from .gpu_planner import jobs_manifest_entries
from .scheduler_queue import (
    Reservation,
    parse_reservations_output,
    parse_sacct_job_states,
    sacct_args,
    strip_array_suffix,
)
from .slurm_protocol import SLURM_STATE_MAP, TASK_STATES_FILE, directive_flag

logger = logging.getLogger(__name__)

# Polls in a row a job may be out of squeue with no accounting record before it counts as done.
SACCT_GRACE = 3
# One sacct row per array task (``-X``), with what TASK_STATES_FILE keeps of it.
TASK_STATE_FIELDS = "JobID,State,ExitCode,NodeList,ElapsedRaw"


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


def task_states(sacct_out: str, task_of: Callable[[str], str | None]) -> dict[str, dict]:
    """``{task dir: {state, exit_code, node, elapsed_s}}`` from ``sacct -P -n -X -o``
    :data:`TASK_STATE_FIELDS`, for the ended rows that ``task_of`` maps to a task dir."""
    states = {}
    for row in sacct_out.splitlines():
        cols = [c.strip() for c in row.split("|")]
        if len(cols) != 5 or not cols[1]:
            continue
        job, state, code, node, elapsed = cols
        state = state.split()[0].rstrip("+")  # "CANCELLED by 123" -> CANCELLED
        if SLURM_STATE_MAP.get(state) in TERMINAL_STATES and (task := task_of(job)):
            secs = int(elapsed) if elapsed.isdigit() else None
            states[task] = {"state": state, "exit_code": code, "node": node, "elapsed_s": secs}
    return states


class SlurmBase(ComputeSource):
    """A :class:`ComputeSource` whose jobs live in a Slurm scheduler."""

    slurm_user: str | None = None  # whose queue to read; defaults to the local user
    poll_interval = 60.0  # Slurm jobs run for hours: a poll a minute is soon enough
    sweep_dir: Path | None = None  # the local sweep dir, set by setup(): manifest, task states
    sweep_id: str | None = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sacct_misses: dict[str, int] = {}  # job -> polls in a row with no accounting record
        self._gpu_nodes: dict[str, list[str]] = {}  # partition -> its GPU nodes (one sinfo each)

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

    async def adopt(
        self, job_ids: Sequence[str], pause: float = 20.0, jobs: Sequence[dict] = ()
    ) -> dict[str, str]:
        """Track already-submitted jobs (a re-attach) and return their settled statuses.

        Polls up to ``SACCT_GRACE`` times, ``pause`` apart, until every job is either queued or
        named by sacct, so a fresh process reaches the verdict a live launcher would. A job Slurm
        couldn't be asked about stays ``UNKNOWN``. ``jobs`` (the manifest's entries) gives each
        array its task-dir map back.
        """
        order = {e["job_id"]: e["global_indices"] for e in jobs if e.get("global_indices")}
        for job in job_ids:
            params = {"_global_indices": order[job]} if job in order else {}
            self.active_jobs.setdefault(job, JobInfo(job, job, params, self.name, status="UNKNOWN"))
        for poll in range(SACCT_GRACE):
            await self.update_all_job_statuses()
            if not self._sacct_misses.keys() & self.active_jobs.keys():
                break
            if poll < SACCT_GRACE - 1:
                await asyncio.sleep(pause)
        tracked = {**self.completed_jobs, **self.active_jobs}
        return {job: tracked[job].status for job in job_ids}

    async def _queued_id(self, job_name: str, script: str) -> str | None:
        """The one live job named ``job_name`` that runs ``script`` (a path unique to the
        sweep; a chain's finished chunks share both, hence live only), or None."""
        user = self.slurm_user or getpass.getuser()
        argv = ["squeue", "-h", "-u", user, "-n", job_name, "-o", "%i %T %o"]
        rc, out, _ = await self._sh(argv)
        rows = [line.split() for line in out.splitlines()] if rc == 0 else []
        ids = {
            strip_array_suffix(row[0])
            for row in rows
            if row[2:] == [script] and SLURM_STATE_MAP.get(row[1]) not in TERMINAL_STATES
        }
        return ids.pop() if len(ids) == 1 else None

    async def _queued_chunk(self, job_name: str, script: str) -> str | None:
        """A chain's chunk already queued, by a driver that stopped between its sbatch and its
        manifest: the next driver adopts it rather than queueing the chunk twice."""
        if job_id := await self._queued_id(job_name, script):
            logger.warning(f"{job_name}: adopting job {job_id}, queued by a driver that stopped")
        return job_id

    async def _write_manifest(
        self,
        job_ids: list[str],
        submission_mode: str,
        num_tasks: int,
        *,
        resumable_manifest: dict[str, Any] | None = None,
        chain: dict[str, Any] | None = None,
    ) -> None:
        """Write ``.hsm_manifest.json``: the sweep's jobs, and a chain's state. SSH-Slurm's
        override also records what a re-attach needs, and writes a copy on the remote."""
        params = {jid: info.params or {} for jid, info in self.active_jobs.items()}
        manifest: dict[str, Any] = {
            "sweep_id": self.sweep_id,
            "backend": "slurm",
            "name": self.name,
            "submission_mode": submission_mode,
            "job_ids": list(job_ids),
            "num_tasks": num_tasks,
            "jobs": jobs_manifest_entries(job_ids, params),
            "submitted_at": datetime.now().isoformat(),
        }
        if chain is not None:
            manifest |= {"resumable": resumable_manifest, "chain": chain}
        if self.sweep_dir is not None:
            try:
                write_atomic(self.sweep_dir / ".hsm_manifest.json", json.dumps(manifest, indent=2))
            except OSError as e:
                logger.warning(f"could not write the manifest: {e}")

    async def persist_chain_manifest(
        self,
        *,
        resumable: dict[str, Any],
        chain: dict[str, Any],
        job_ids: list[str],
        num_tasks: int,
    ) -> None:
        """A chain's manifest: its state, so a detached ``hsm sweep advance`` can drive it."""
        await self._write_manifest(
            job_ids, "array", num_tasks, resumable_manifest=resumable, chain=chain
        )

    async def record_task_states(self) -> None:
        """Write :data:`TASK_STATES_FILE` into the local sweep dir: how Slurm ended each task.

        One sacct for every tracked job, best effort and never a guess: a failed or absent sacct,
        or one without rows, writes nothing; rows update the file's entries. An array row maps
        to ``task_<global index>`` through its job's ``_global_indices`` (a re-attached array
        has none: 1..N, when it is the only job); an individual job's row to its own task dir.
        """
        jobs = {**self.completed_jobs, **self.active_jobs}
        if not jobs or self.sweep_dir is None:
            return

        def task_of(row: str) -> str | None:
            base, _, index = row.partition("_")
            if (info := jobs.get(base)) is None:
                return None
            if not index:  # an individual job (an array only has <id>_<i> rows)
                own = info.task_dir and "_array_size" not in info.params
                return Path(info.task_dir).name if own else None
            if not index.isdigit():  # a collapsed range (123_[4-9]) never ran
                return None
            i, order = int(index), info.params.get("_global_indices")
            if order is None:
                return f"task_{i}" if len(jobs) == 1 else None
            return f"task_{order[i - 1]}" if 0 < i <= len(order) else None

        try:
            argv = ["sacct", "-j", ",".join(jobs), "-P", "-n", "-X", "-o", TASK_STATE_FIELDS]
            rc, out, err = await self._sh(argv)
            states = task_states(out, task_of) if rc == 0 else {}
            if states:  # merged: a distributed sweep's Slurm children share the sweep dir
                path = self.sweep_dir / TASK_STATES_FILE
                from ..common.sweep_analysis import load_task_states

                old = load_task_states(self.sweep_dir)  # {} when absent or unreadable
                path.write_text(json.dumps({**old, **states}, indent=2, sort_keys=True))
            elif rc != 0:
                logger.info(f"sacct failed (rc={rc}): {err.strip()}; no {TASK_STATES_FILE}")
        except Exception as e:  # noqa: BLE001 — best effort: the collect goes on without it
            logger.warning(f"{TASK_STATES_FILE} not written: {e}")

    async def _off_gpu_nodes(self, spec: ResourceSpec) -> ResourceSpec:
        """A CPU-only job (:func:`cpu_only`) excludes its partition's GPU nodes, unless that is
        every node; an ``--exclude`` given is kept. 24 of ~300 CPU tasks once sat on GPU nodes."""
        if not cpu_only(spec):
            return spec
        part = spec.partition
        if part not in self._gpu_nodes:
            rc, out, _ = await self._sh(["sinfo", "-h", "-N", "-p", part, "-o", "%N %G"])
            if rc != 0:  # not cached: the next job asks again
                logger.warning(
                    f"sinfo -p {part} failed (rc={rc}): GPU nodes not excluded this time"
                )
                return spec
            rows = [ln.split()[:2] for ln in out.splitlines() if len(ln.split()) > 1]
            gpu = {node for node, gres in rows if "gpu" in gres}
            self._gpu_nodes[part] = sorted(gpu) if gpu < {node for node, _ in rows} else []
            if self._gpu_nodes[part]:
                logger.info(
                    f"CPU-only jobs exclude {len(gpu)} GPU node(s) of {part} "
                    f"(spec.cpu_only_nodes: false allows them)"
                )
        if not self._gpu_nodes[part]:
            return spec
        given = [v for k, v in spec.extra_directives if directive_flag(k) == "--exclude"]
        extra = {directive_flag(k): v for k, v in spec.extra_directives}
        extra["--exclude"] = ",".join([*given, *self._gpu_nodes[part]])
        return replace(spec, extra_directives=tuple(extra.items()))

    async def _warn_reservations(self, walltime: str | None) -> None:
        """Warn when a maintenance reservation starts before a job of ``walltime`` could end:
        Slurm then holds the job until the reservation is over (once ~20 h, 1,600 CPUs idle).
        Best effort: a check that fails (transport, clock, an unbounded walltime) says nothing."""
        try:
            rc, out, _ = await self._sh(["bash", "-c", "date +%FT%T; scontrol show reservations"])
            if rc != 0:
                return
            now = datetime.fromisoformat(out.splitlines()[0].strip())
            span = parse_walltime(walltime) if walltime else 0
            hits = blocking_reservations(parse_reservations_output(out), now, span)
        except Exception as e:  # noqa: BLE001
            logger.debug(f"reservation check skipped: {e}")
            return
        for res in hits:
            free = int((datetime.fromisoformat(res.start_time) - now).total_seconds())
            hint = f"; a walltime ≤ {format_walltime(free)} would start now" if free > 0 else ""
            logger.warning(
                f"Reservation {res.name} ({res.start_time} → {res.end_time}) overlaps a "
                f"{walltime or 'job'} walltime: jobs won't start before {res.end_time}{hint}."
            )


# Directives that ask for GPUs or pin placement: such a job keeps every node it could use.
_PLACED = ("--gres", "--gpu", "--tres-per", "--nodelist", "--constraint", "-w", "-C", "-G")


def cpu_only(spec: ResourceSpec) -> bool:
    """Whether :meth:`SlurmBase._off_gpu_nodes` would keep this job off GPU nodes."""
    placed = any(directive_flag(k).startswith(_PLACED) for k, _ in spec.extra_directives)
    return not (spec.gpus or placed or spec.cpu_only_nodes is False or not spec.partition)


def blocking_reservations(
    reservations: Iterable[Reservation], now: datetime, walltime_s: int
) -> list[Reservation]:
    """Maintenance reservations (MAINT, or all nodes) overlapping ``[now, now + walltime_s]``."""
    hits = []
    for res in reservations:
        try:
            start, end = (datetime.fromisoformat(t) for t in (res.start_time, res.end_time))
        except ValueError:
            continue
        maint = "MAINT" in res.flags or "ALL_NODES" in res.flags or res.nodes == "ALL"
        if maint and end > now and start.timestamp() < now.timestamp() + walltime_s:
            hits.append(res)
    return hits
