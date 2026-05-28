"""Slurm queue inspection: where am I in line, what's running, what's reserved.

Read-only wrappers around ``squeue`` / ``sprio`` / ``scontrol show reservations``
that the CLI's ``hsm queue`` subcommands build on. All shell-outs use
tab-delimited ``--format`` strings so parsing is robust against the
configurable column widths squeue uses by default.

S3IT-specific notes worth knowing (see also CLAUDE.md gotcha #6):

- Pending jobs with reason ``(Resources)`` are at the front of the line —
  Slurm has decided which nodes they'll run on and is waiting on them to
  free up. ``(Priority)`` means higher-priority jobs are ahead of you.
- "Where will my job run / when?" has **no definitive answer** per S3IT
  docs (https://docs.s3it.uzh.ch/cluster/job_management/). The position
  number is a snapshot — new higher-priority submissions can displace you.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
import re
import shutil
import subprocess
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------- dataclasses


@dataclass(frozen=True)
class QueueJob:
    """One row from ``squeue``, with GPU info parsed out of ``tres_per_job``.

    ``gpu_type`` is the GRES type token (e.g. ``"H100"``, ``"L4"``) when present;
    ``None`` means "any GPU" or "no GPU." ``gpu_count`` is the per-job GPU count
    (0 for CPU-only jobs).
    """

    job_id: str
    name: str
    user: str
    state: str  # "PENDING" | "RUNNING" | "COMPLETING" | ...
    reason: str  # "(Resources)" | "(Priority)" | "(ReqNodeNotAvail)" | node list
    partition: str
    tres_per_job: str  # raw, e.g. "cpu=1,mem=4G,gres/gpu:h100=1"
    expected_start: str  # "N/A" or ISO timestamp
    priority: int
    gpu_count: int = 0
    gpu_type: Optional[str] = None


@dataclass(frozen=True)
class Reservation:
    """One row from ``scontrol show reservations``."""

    name: str
    start_time: str
    end_time: str
    duration: str
    nodes: str
    node_count: int


# --------------------------------------------------------------- parse helpers


# Match Slurm's tres-per-job GRES syntax — both flavors observed in the wild:
#   gres/gpu:h100=1     (Slurm 23+, type:count)
#   gres/gpu=1          (untyped, just a count)
# Case-insensitive on type so the parser tolerates either casing the cluster
# happens to use; we preserve the original casing in the result.
_GRES_RE = re.compile(r"gres/gpu(?::([A-Za-z0-9_]+))?=(\d+)", re.IGNORECASE)


def _parse_gpu_from_tres(tres: str) -> tuple[int, Optional[str]]:
    """Pull (count, type) for GPUs out of a tres-per-job string.

    Picks the *typed* entry if present (`gres/gpu:H100=1`); falls back to the
    untyped count otherwise. Slurm sometimes emits both — the typed one wins
    because that's what the job actually asked for.
    """
    if not tres:
        return 0, None
    count = 0
    gpu_type: Optional[str] = None
    for m in _GRES_RE.finditer(tres):
        t, c = m.group(1), int(m.group(2))
        if t is not None:
            # Typed entry — preferred.
            return c, t
        count = max(count, c)
    return count, gpu_type


def _parse_priority(raw: str) -> int:
    """Slurm sometimes prints priority as float in scientific notation."""
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return 0


# ------------------------------------------------------------ scheduler probe


def slurm_available() -> bool:
    """Cheap PATH check used by the CLI to print a friendly error vs crash."""
    return shutil.which("squeue") is not None


# ------------------------------------------------------------------- SlurmQueue


class SlurmQueue:
    """Read-only Slurm queue introspection.

    All methods shell out to ``squeue`` / ``sprio`` / ``scontrol`` with explicit
    tab-delimited format strings. Returns dataclasses, not raw text — the CLI
    layer renders these into rich.Tables.

    Constructed with the two binaries as injectable seams so tests can point at
    the PATH-stub ``tests/fixtures/fake_slurm/`` without monkeypatching
    ``shutil.which``.
    """

    # squeue format string: tab-delimited, picked to be tres-aware and stable.
    # %i job_id, %j name, %u user, %T state, %R reason/nodelist, %P partition,
    # %b tres-per-job, %S expected_start, %Q priority
    _SQUEUE_FORMAT = "%i\t%j\t%u\t%T\t%R\t%P\t%b\t%S\t%Q"
    _SQUEUE_FIELD_COUNT = 9

    def __init__(
        self,
        squeue_bin: str = "squeue",
        scontrol_bin: str = "scontrol",
        timeout_s: float = 15.0,
    ):
        self.squeue_bin = squeue_bin
        self.scontrol_bin = scontrol_bin
        self.timeout_s = timeout_s

    # -------------------------------------------------------------- raw queries

    def _run_squeue(self, extra_args: List[str]) -> List[QueueJob]:
        """Run squeue with the canonical format string + caller's filters.

        Returns parsed `QueueJob` list. Filters out rows whose field count
        doesn't match (defensive against future format changes); logs them.
        """
        cmd = [
            self.squeue_bin,
            "--noheader",
            f"--format={self._SQUEUE_FORMAT}",
            *extra_args,
        ]
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=self.timeout_s
            )
        except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as e:
            logger.error(f"squeue invocation failed: {e}")
            return []
        if result.returncode != 0:
            logger.warning(
                f"squeue exited rc={result.returncode}: {result.stderr.strip()}"
            )
            return []

        jobs: List[QueueJob] = []
        for raw in result.stdout.splitlines():
            if not raw.strip():
                continue
            parts = raw.split("\t")
            if len(parts) != self._SQUEUE_FIELD_COUNT:
                logger.warning(
                    f"squeue row has {len(parts)} fields, expected "
                    f"{self._SQUEUE_FIELD_COUNT}; skipping: {raw!r}"
                )
                continue
            job_id, name, user, state, reason, partition, tres, start, prio = parts
            gpu_count, gpu_type = _parse_gpu_from_tres(tres)
            jobs.append(
                QueueJob(
                    job_id=job_id,
                    name=name,
                    user=user,
                    state=state,
                    reason=reason,
                    partition=partition,
                    tres_per_job=tres,
                    expected_start=start,
                    priority=_parse_priority(prio),
                    gpu_count=gpu_count,
                    gpu_type=gpu_type,
                )
            )
        return jobs

    # ------------------------------------------------------------- user-facing

    def list_user_jobs(self, user: str) -> List[QueueJob]:
        """All jobs (any state) belonging to ``user``."""
        return self._run_squeue(["-u", user])

    def pending_gpu_jobs_sorted(self) -> List[QueueJob]:
        """Cluster-wide pending GPU jobs, highest priority first.

        ``squeue -S '-Q'`` sorts by priority descending — same order Slurm
        uses when deciding what runs next. Filters to ``state == 'PENDING'``
        AND ``gpu_count > 0``; the state filter is also passed to squeue via
        ``--state=PENDING`` for efficiency, but we re-apply locally so the
        function's contract doesn't depend on squeue actually honoring the
        flag (defensive).
        """
        jobs = self._run_squeue(["--state=PENDING", "-S", "-Q"])
        return [j for j in jobs if j.state == "PENDING" and j.gpu_count > 0]

    def gpu_summary(self) -> Dict[str, Dict[str, int]]:
        """Per-GPU-type queue depth: ``{type: {state: count}}``.

        Type ``"<untyped>"`` collects jobs requesting GPUs without a specific
        type (e.g. ``--gpus=1`` without a ``--gres=gpu:TYPE``).
        """
        jobs = self._run_squeue([])
        summary: Dict[str, Dict[str, int]] = {}
        for j in jobs:
            if j.gpu_count == 0:
                continue
            type_key = j.gpu_type or "<untyped>"
            summary.setdefault(type_key, {}).setdefault(j.state, 0)
            summary[type_key][j.state] += j.gpu_count
        return summary

    def position_in_gpu_queue(self, job_id: str) -> Optional[tuple[int, int]]:
        """Find ``job_id``'s position in the pending GPU queue.

        Returns ``(position, total)`` (1-based) or ``None`` if the job
        isn't pending. ``total`` is the total count of pending GPU jobs
        cluster-wide; ``position`` is your slot, with ``1`` meaning "next
        up." Reflects a snapshot — scheduler is dynamic, see module docstring.
        """
        pending = self.pending_gpu_jobs_sorted()
        total = len(pending)
        for idx, job in enumerate(pending, start=1):
            if job.job_id == job_id:
                return idx, total
        return None

    # ----------------------------------------------------------- reservations

    def reservations(self) -> List[Reservation]:
        """Parse ``scontrol show reservations`` — upcoming maintenance windows."""
        try:
            result = subprocess.run(
                [self.scontrol_bin, "show", "reservations"],
                capture_output=True,
                text=True,
                timeout=self.timeout_s,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as e:
            logger.error(f"scontrol invocation failed: {e}")
            return []
        if result.returncode != 0:
            return []

        # scontrol emits "key=value" pairs separated by whitespace, one
        # reservation per stanza (stanzas separated by blank lines).
        stanzas = re.split(r"\n\s*\n", result.stdout.strip())
        out: List[Reservation] = []
        for stanza in stanzas:
            if not stanza.strip() or stanza.lower().startswith("no reservations"):
                continue
            fields = dict(_KV_RE.findall(stanza))
            try:
                node_count = int(fields.get("NodeCnt", "0"))
            except ValueError:
                node_count = 0
            out.append(
                Reservation(
                    name=fields.get("ReservationName", "?"),
                    start_time=fields.get("StartTime", "?"),
                    end_time=fields.get("EndTime", "?"),
                    duration=fields.get("Duration", "?"),
                    nodes=fields.get("Nodes", "?"),
                    node_count=node_count,
                )
            )
        return out


_KV_RE = re.compile(r"([A-Za-z][A-Za-z0-9_]*)=(\S+)")
