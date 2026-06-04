"""Slurm queue inspection: where am I in line, what's running, what's reserved.

Read-only wrappers around ``squeue`` / ``scontrol show reservations`` (plus
optional ``sacct`` enrichment for per-array completed/failed counts) that the
CLI's ``hsm queue`` subcommands build on, in two transport flavors:

- :class:`SlurmQueue` — sync, shells out locally (cluster login node).
- :class:`SSHSlurmQueue` — async twin running the *same* commands over an
  established SSH connection (monitor a cluster from your workstation).

Both delegate to shared pure command builders (:func:`squeue_args`) and
parsers/aggregators (:func:`parse_squeue_output`, :func:`summarize_gpu_jobs`,
:func:`parse_reservations_output`, ...) so the two transports cannot drift —
same idiom as :mod:`core.hpc.slurm_protocol`.

Field-audit notes (S3IT, Slurm 25.05.4, live census 2026-06-04):

- ``%b`` is **tres-per-node** (not per-job, as this module once claimed) and
  emits *colon*-count GRES — ``gres/gpu:A100:1``, ``gres/gpu:3`` — NOT the
  ``=``-count TRES accounting grammar (``gres/gpu:h100=1``) the original
  parser expected. :func:`_parse_gpu_from_tres` accepts both. Per-job GPU
  count = per-node count × nodes; HSM sweep tasks are single-node, where the
  two coincide.
- A *pending* array job is ONE squeue row (``123_[690-1920%4]`` — 1231
  tasks). Counting paths pass ``-r/--array`` so Slurm itself expands
  per-task rows; display paths keep collapsed rows and surface
  :func:`parse_array_task_count`.

S3IT-specific notes worth knowing (see also CLAUDE.md gotcha #6):

- Pending jobs with reason ``(Resources)`` are at the front of the line —
  Slurm has decided which nodes they'll run on and is waiting on them to
  free up. ``(Priority)`` means higher-priority jobs are ahead of you;
  ``(QOSMaxJobsPerUserLimit)`` means *your own* QoS cap is the gate.
- "Where will my job run / when?" has **no definitive answer** per S3IT
  docs (https://docs.s3it.uzh.ch/cluster/job_management/). The position
  number is a snapshot — new higher-priority submissions can displace you.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
import logging
import re
import shlex
import shutil
import subprocess
from typing import Any, Dict, List, Optional, Sequence

from .slurm_protocol import SLURM_STATE_MAP

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------- dataclasses


@dataclass(frozen=True)
class QueueJob:
    """One row from ``squeue``, with GPU info parsed out of ``tres_per_node``.

    ``gpu_type`` is the GRES type token (e.g. ``"H100"``, ``"A100"``) when
    present; ``None`` means "any GPU" or "no GPU." ``gpu_count`` is the
    per-node GPU count (0 for CPU-only jobs; equals the per-task count for
    HSM's single-node tasks).

    ``task_count`` is 1 for a normal row and >1 for a *collapsed* pending
    array row (``123_[5-90]`` — squeue without ``-r`` shows the whole
    pending range as one line).
    """

    job_id: str
    name: str
    user: str
    state: str  # "PENDING" | "RUNNING" | "COMPLETING" | ...
    reason: str  # "(Resources)" | "(Priority)" | "(QOSMaxJobsPerUserLimit)" | node list
    partition: str
    tres_per_node: str  # raw %b, e.g. "gres/gpu:A100:1" or "N/A"
    expected_start: str  # "N/A" or ISO timestamp
    priority: int
    gpu_count: int = 0
    gpu_type: Optional[str] = None
    task_count: int = 1


@dataclass(frozen=True)
class Reservation:
    """One row from ``scontrol show reservations``."""

    name: str
    start_time: str
    end_time: str
    duration: str
    nodes: str
    node_count: int


@dataclass(frozen=True)
class JobGroup:
    """One *array* (or single job) aggregated for the grouped ``mine`` view.

    squeue-derived fields (``running``/``pending``/``other``, task-weighted)
    are live; ``completed``/``failed``/``total`` come from optional sacct
    enrichment via :meth:`with_accounting` and stay ``None`` when accounting
    is unavailable — renderers must distinguish "0 failed" from "unknown".
    """

    base_id: str
    name: str
    user: str
    partition: str
    gpu_count: int
    gpu_type: Optional[str]
    is_array: bool
    running: int = 0
    pending: int = 0
    other: int = 0  # COMPLETING / CONFIGURING / ... — still occupying the queue
    nodes: tuple = ()  # distinct nodelists of RUNNING rows
    reason: str = ""  # first pending row's reason, e.g. "(Priority)"
    completed: Optional[int] = None  # sacct: COMPLETED task count
    failed: Optional[int] = None  # sacct: FAILED + CANCELLED (incl. TIMEOUT/OOM)
    total: Optional[int] = None  # sacct: sum over every task the array ever had

    @property
    def in_queue(self) -> int:
        return self.running + self.pending + self.other

    def with_accounting(self, completed: int, failed: int, total: int) -> "JobGroup":
        return replace(self, completed=completed, failed=failed, total=total)


# --------------------------------------------------------------- parse helpers


# A GRES entry in %b output starts with "gres/gpu" and runs to the next comma.
# What follows varies by Slurm version / request style (live census S3IT
# 25.05 + legacy grammar):
#   gres/gpu:A100:1     colon-count, typed        (live S3IT)
#   gres/gpu:3          colon-count, untyped      (live S3IT)
#   gres/gpu:h100=1     equals-count, typed       (TRES accounting style)
#   gres/gpu=1          equals-count, untyped     (TRES accounting style)
#   gres/gpu:a100       bare type                 (count defaults to 1)
#   N/A                 no GRES at all
_GPU_ENTRY_RE = re.compile(r"gres/gpu(?P<rest>[^,\s]*)", re.IGNORECASE)


def _parse_one_gpu_entry(rest: str) -> tuple[int, Optional[str]]:
    """Parse the part after ``gres/gpu`` in a single GRES entry."""
    # Strip index decorations some clusters append, e.g. "(IDX:0-1)".
    rest = re.sub(r"\(.*\)$", "", rest)
    if not rest:
        return 1, None  # bare "gres/gpu" — a GPU was asked for, count unknown → 1
    if rest.startswith("="):
        try:
            return int(rest[1:]), None
        except ValueError:
            return 0, None
    if not rest.startswith(":"):
        return 0, None  # e.g. "gres/gpufoo" — not a gpu entry after all
    tokens = rest[1:].split(":")
    first = tokens[0]
    if "=" in first:  # "TYPE=N"
        type_tok, _, count_tok = first.partition("=")
        try:
            return int(count_tok), (type_tok or None)
        except ValueError:
            return 0, None
    if first.isdigit():  # ":N" — untyped colon-count
        return int(first), None
    # First token is a type name; count is the next numeric token if present.
    if len(tokens) >= 2 and tokens[1].isdigit():  # ":TYPE:N"
        return int(tokens[1]), first
    # ":TYPE" — bare type, count defaults to 1. `or None` so a malformed
    # trailing colon ("gres/gpu:") can't produce a falsy-but-not-None type.
    return 1, (first or None)


def _parse_gpu_entry_any(entry: str) -> tuple[int, Optional[str]]:
    """Parse ONE gres entry (any source) into (gpu_count, gpu_type).

    Accepts both the squeue ``gres/gpu...`` and the sinfo ``gpu...`` prefix
    forms; non-gpu gres (``shard:...``) and lookalikes (``gpumem...``)
    return ``(0, None)``.
    """
    m = re.match(r"(?:gres/)?gpu(?![A-Za-z0-9_])(?P<rest>.*)$", entry.strip(), re.IGNORECASE)
    if not m:
        return 0, None
    return _parse_one_gpu_entry(m.group("rest"))


def _split_gres_entries(field: str) -> List[str]:
    """Split a gres field on commas NOT inside parentheses.

    Load-bearing for sinfo's GresUsed: index decorations contain commas —
    ``gpu:A100:6(IDX:0-1,4-7)`` is ONE entry. A naive ``split(",")`` would
    truncate it to ``gpu:A100:6(IDX:0-1`` and mis-parse the count as 1.
    """
    entries: List[str] = []
    depth = 0
    current: List[str] = []
    for ch in field:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(depth - 1, 0)
        if ch == "," and depth == 0:
            if current:
                entries.append("".join(current))
                current = []
            continue
        current.append(ch)
    if current:
        entries.append("".join(current))
    return entries


def _parse_gpu_from_tres(tres: str) -> tuple[int, Optional[str]]:
    """Pull (count, type) for GPUs out of a tres-per-node string.

    Accepts BOTH GRES grammars (colon-count ``gres/gpu:A100:1`` as emitted
    live by ``%b`` on Slurm 25.05, and equals-count ``gres/gpu:h100=1`` from
    TRES accounting strings). Picks the *typed* entry if present; falls back
    to the max untyped count. ``N/A`` / empty / no-gpu strings → ``(0, None)``.
    """
    if not tres or tres.strip().upper() == "N/A":
        return 0, None
    count = 0
    for m in _GPU_ENTRY_RE.finditer(tres):
        c, t = _parse_one_gpu_entry(m.group("rest"))
        if t is not None:
            # Typed entry — preferred: it's what the job actually asked for.
            return c, t
        count = max(count, c)
    return count, None


def _parse_priority(raw: str) -> int:
    """Slurm sometimes prints priority as float in scientific notation."""
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return 0


# Slurm array tasks show up in squeue as `<base>_<index>` (running) or
# `<base>_[spec]` (collapsed pending range, e.g. "123_[5-90%4]").
_ARRAY_SUFFIX_RE = re.compile(r"_(\d+|\[.+\])$")


def strip_array_suffix(job_id: str) -> str:
    """``"123_7"`` / ``"123_[5-90%4]"`` → ``"123"``; plain ids pass through."""
    return _ARRAY_SUFFIX_RE.sub("", job_id)


def parse_array_task_count(job_id: str) -> int:
    """Number of tasks a (possibly collapsed) squeue job id stands for.

    ``"123"`` / ``"123_7"`` → 1. ``"123_[5-9]"`` → 5. ``"123_[5-9%2]"`` → 5
    (``%N`` is a *throttle*, not a count). ``"123_[1,3,7-9]"`` → 5.
    ``"123_[0-100:10]"`` → 11 (``:step`` ranges — Slurm reconstructs them
    for evenly-spaced pending indices, e.g. ``sbatch --array=0-100:10``).
    Unparseable specs conservatively count 1 per comma-separated part.
    """
    m = re.search(r"_\[([^\]]+)\]$", job_id)
    if not m:
        return 1
    spec = m.group(1).split("%")[0]  # strip throttle suffix
    total = 0
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        lo, sep, hi = part.partition("-")
        hi, _, step = hi.partition(":")
        if sep and lo.strip().isdigit() and hi.strip().isdigit():
            span = abs(int(hi) - int(lo))
            step_n = int(step) if step.strip().isdigit() and int(step) > 0 else 1
            total += span // step_n + 1
        else:
            total += 1
    return max(total, 1)


# ------------------------------------------------- pure command builders/parsers


# squeue format string: tab-delimited, picked to be tres-aware and stable.
# %i job_id, %j name, %u user, %T state, %R reason/nodelist, %P partition,
# %b tres-per-node, %S expected_start, %Q priority
SQUEUE_FORMAT = "%i\t%j\t%u\t%T\t%R\t%P\t%b\t%S\t%Q"
_SQUEUE_FIELD_COUNT = 9


def squeue_args(extra_args: Sequence[str] = ()) -> List[str]:
    """Canonical squeue argument list (sans binary) shared by both transports."""
    return ["--noheader", f"--format={SQUEUE_FORMAT}", *extra_args]


def sacct_args(base_ids: Sequence[str]) -> List[str]:
    """Canonical sacct argument list (sans binary) shared by both transports.

    ``-X`` = one row per allocation (per array task), ``-P`` = pipe-delimited
    parsable output immune to column truncation.
    """
    return ["-j", ",".join(base_ids), "-n", "-X", "-P", "-o", "JobID,State"]


def sinfo_capacity_args() -> List[str]:
    """Canonical sinfo argument list (sans binary) shared by both transports.

    ``-N`` = one row per node (per partition — duplicates deduped at parse
    time); generous column widths because ``GresUsed`` strings carry long
    ``(IDX:...)`` decorations that must not truncate mid-entry. ``Features``
    rides along for VRAM discovery (clusters like S3IT publish
    ``GPUMEM96GB``-style feature tags) — parsed by scanning the whole line,
    so an empty ``GresUsed`` column shifting fields can degrade VRAM info
    but never corrupt the count columns.
    """
    return [
        "-h",
        "-N",
        "-O",
        "NodeHost:40,StateCompact:20,Gres:60,GresUsed:90,Features:120",
    ]


# VRAM advertised by the cluster itself, e.g. S3IT's GPUMEM80GB / GPUMEM96GB
# / GPUMEM140GB feature tags. Scanned line-wide (column-position-proof).
_GPUMEM_FEATURE_RE = re.compile(r"GPUMEM(\d+)\s*GB", re.IGNORECASE)


# Model-typical VRAM (GB) used ONLY when the cluster doesn't report it via
# features — and only for models with a single common configuration. The
# ambiguous ones are deliberately absent (A100 = 40/80, V100 = 16/32,
# H100 = 80/94+: the live S3IT H100s report 96GB — a static entry would lie).
KNOWN_GPU_VRAM_GB: Dict[str, int] = {
    "L4": 24,
    "T4": 16,
    "A30": 24,
    "A40": 48,
    "A6000": 48,
    "H200": 141,
    "L40": 48,
    "L40S": 48,
    "P100": 16,
    "RTX2080TI": 11,
    "RTX3090": 24,
    "RTX4090": 24,
}


# Node states whose GPUs can't host work — excluded from capacity totals.
# "drng" (drainING — jobs still running) deliberately stays IN: its GPUs are
# both present and in use; "drain" (drainED, empty) is out.
_UNUSABLE_STATE_PREFIXES = (
    "down", "drain", "fail", "maint", "boot", "unk", "inval", "err", "futr",
)


def parse_sinfo_gpu_capacity(stdout: str) -> tuple[Dict[str, Dict[str, int]], int]:
    """Parse ``sinfo -h -N -O NodeHost,StateCompact,Gres,GresUsed`` output.

    Returns ``({gpu_type: {"total": N, "used": M}}, excluded_gpu_count)``.
    Pure, shared by both transports.

    - Rows are one-per-node-per-partition → deduped by node name (first
      occurrence wins).
    - ``used`` comes from Slurm's own allocation accounting (``GresUsed``),
      so GPUs consumed by *untyped* job requests are still attributed to
      their physical type — squeue job rows can't do that.
    - Nodes in unusable states (down/drained/failed/...) contribute to
      neither total nor used; their GPU count is returned separately so
      the UI can disclose what was excluded. State suffix flags
      (``*~#%$@!+-``) are stripped before classification.
    - When the node line carries a ``GPUMEM<N>GB`` feature tag, ``N`` is
      recorded under the type's ``"vram_gb"`` key (sorted list — a type
      served by mixed-VRAM node groups lists every variant). The key is
      ABSENT when the cluster doesn't report VRAM; callers must treat
      absent as unknown, not zero.
    """
    capacity: Dict[str, Dict[str, int]] = {}
    vram_seen: Dict[str, set] = {}
    excluded_gpus = 0
    seen: set = set()
    for line in (stdout or "").splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        node, state, gres = parts[0], parts[1], parts[2]
        gres_used = parts[3] if len(parts) > 3 else ""
        if node in seen:
            continue
        seen.add(node)
        if gres.lower().startswith("(null)"):
            continue
        base_state = state.lower().rstrip("*~#%$@!+-")
        unusable = any(base_state.startswith(p) for p in _UNUSABLE_STATE_PREFIXES)
        vram_match = _GPUMEM_FEATURE_RE.search(line)
        node_types: List[str] = []
        for entry in _split_gres_entries(gres):
            count, gtype = _parse_gpu_entry_any(entry)
            if count <= 0:
                continue
            type_key = gtype or "<untyped>"
            if unusable:
                excluded_gpus += count
                continue
            node_types.append(type_key)
            cap = capacity.setdefault(type_key, {"total": 0, "used": 0})
            cap["total"] += count
        if unusable:
            continue
        if vram_match:
            for type_key in node_types:
                vram_seen.setdefault(type_key, set()).add(int(vram_match.group(1)))
        for entry in _split_gres_entries(gres_used):
            count, gtype = _parse_gpu_entry_any(entry)
            if count <= 0:
                continue
            cap = capacity.setdefault(gtype or "<untyped>", {"total": 0, "used": 0})
            cap["used"] += count
    for type_key, values in vram_seen.items():
        capacity[type_key]["vram_gb"] = sorted(values)  # type: ignore[assignment]
    return capacity, excluded_gpus


def parse_squeue_output(stdout: str) -> List[QueueJob]:
    """Parse canonical-format squeue stdout into :class:`QueueJob` rows.

    Pure (no subprocess) so SSH-driven callers can reuse it over their own
    transport. Filters out rows whose field count doesn't match (defensive
    against future format changes); logs them.
    """
    jobs: List[QueueJob] = []
    for raw in (stdout or "").splitlines():
        if not raw.strip():
            continue
        parts = raw.split("\t")
        if len(parts) != _SQUEUE_FIELD_COUNT:
            logger.warning(
                f"squeue row has {len(parts)} fields, expected "
                f"{_SQUEUE_FIELD_COUNT}; skipping: {raw!r}"
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
                tres_per_node=tres,
                expected_start=start,
                priority=_parse_priority(prio),
                gpu_count=gpu_count,
                gpu_type=gpu_type,
                task_count=parse_array_task_count(job_id),
            )
        )
    return jobs


# ------------------------------------------------------------ pure aggregators


def filter_pending_gpu(jobs: Sequence[QueueJob]) -> List[QueueJob]:
    """Pending GPU jobs only. The state filter is re-applied locally so the
    contract doesn't depend on squeue honoring ``--state=PENDING`` (defensive)."""
    return [j for j in jobs if j.state == "PENDING" and j.gpu_count > 0]


def summarize_gpu_jobs(jobs: Sequence[QueueJob]) -> Dict[str, Dict[str, int]]:
    """Per-GPU-type queue depth: ``{type: {state: count}}``.

    Counts GPUs, weighted by ``task_count`` so a collapsed pending array row
    contributes its full task range (identity for ``-r``-expanded rows).
    Type ``"<untyped>"`` collects jobs requesting GPUs without a specific
    type (e.g. ``--gpus=1`` without a ``--gres=gpu:TYPE``).
    """
    summary: Dict[str, Dict[str, int]] = {}
    for j in jobs:
        if j.gpu_count == 0:
            continue
        type_key = j.gpu_type or "<untyped>"
        summary.setdefault(type_key, {}).setdefault(j.state, 0)
        summary[type_key][j.state] += j.gpu_count * j.task_count
    return summary


def find_queue_position(
    pending: Sequence[QueueJob], job_id: str
) -> Optional[tuple[int, int]]:
    """Exact-id position of ``job_id`` in a priority-sorted pending list.

    Returns ``(position, total)`` (1-based) or ``None`` when absent.
    """
    total = len(pending)
    for idx, job in enumerate(pending, start=1):
        if job.job_id == job_id:
            return idx, total
    return None


def positions_by_base(pending: Sequence[QueueJob]) -> Dict[str, List[int]]:
    """Map base job id → sorted 1-based positions of its tasks in ``pending``.

    With ``-r``-expanded rows, an array's tasks land at several positions;
    callers typically report the first (best) one plus the task count.
    """
    out: Dict[str, List[int]] = {}
    for idx, j in enumerate(pending, start=1):
        out.setdefault(strip_array_suffix(j.job_id), []).append(idx)
    return out


def group_jobs_by_array(jobs: Sequence[QueueJob]) -> List[JobGroup]:
    """Collapse a (collapsed-display) squeue job list into one group per array.

    Input is ``list_user_jobs`` output: running array tasks as individual
    rows, pending ranges as collapsed rows. Groups by base job id,
    task-weighting pending counts, collecting distinct running nodelists,
    and keeping the first pending reason. First-seen order is preserved.
    Single (non-array) jobs become one-group-of-one with ``is_array=False``.
    """
    order: List[str] = []
    agg: Dict[str, dict] = {}
    for j in jobs:
        base = strip_array_suffix(j.job_id)
        a = agg.get(base)
        if a is None:
            order.append(base)
            a = agg[base] = {
                "name": j.name,
                "user": j.user,
                "partition": j.partition,
                "gpu_count": j.gpu_count,
                "gpu_type": j.gpu_type,
                "is_array": False,
                "running": 0,
                "pending": 0,
                "other": 0,
                "nodes": [],
                "reason": "",
            }
        if j.job_id != base:
            a["is_array"] = True
        if j.gpu_count and not a["gpu_count"]:
            # First GPU-bearing row wins (rows of one array share the spec).
            a["gpu_count"], a["gpu_type"] = j.gpu_count, j.gpu_type
        if j.state == "RUNNING":
            a["running"] += j.task_count
            # %R for a RUNNING row is its nodelist.
            if j.reason and j.reason not in a["nodes"]:
                a["nodes"].append(j.reason)
        elif j.state == "PENDING":
            a["pending"] += j.task_count
            if not a["reason"]:
                a["reason"] = j.reason
        else:
            a["other"] += j.task_count
    groups: List[JobGroup] = []
    for base in order:
        a = agg[base]
        nodes = tuple(a.pop("nodes"))
        groups.append(JobGroup(base_id=base, nodes=nodes, **a))
    return groups


def parse_sacct_job_states(stdout: str) -> Dict[str, Dict[str, int]]:
    """Parse ``sacct -n -X -P -o JobID,State`` into ``{base: {state: tasks}}``.

    Pure, shared by both transports. One row per array task that has
    started; never-started pending tasks appear as a collapsed range row
    (``123_[710-1920]``) — counted via :func:`parse_array_task_count`.
    States normalize through :data:`slurm_protocol.SLURM_STATE_MAP`
    (``TIMEOUT``/``OOM`` → FAILED, ...) tolerating the ``CANCELLED by
    <uid>`` long form and trailing ``+`` markers.
    """
    out: Dict[str, Dict[str, int]] = {}
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not line or "|" not in line:
            continue
        job_id, _, raw_state = line.partition("|")
        job_id = job_id.strip()
        raw_state = raw_state.strip()
        if not job_id or not raw_state:
            continue
        raw = raw_state.split()[0].rstrip("+")
        state = SLURM_STATE_MAP.get(raw, "RUNNING")
        counts = out.setdefault(strip_array_suffix(job_id), {})
        counts[state] = counts.get(state, 0) + parse_array_task_count(job_id)
    return out


def enrich_groups_with_accounting(
    groups: Sequence[JobGroup], states: Optional[Dict[str, Dict[str, int]]]
) -> List[JobGroup]:
    """Fold sacct per-state task counts into groups (no-op when ``states`` is None).

    ``completed`` = COMPLETED; ``failed`` = FAILED + CANCELLED (the state map
    already folds TIMEOUT/OOM/... into FAILED); ``total`` = every task sacct
    knows about, including its own view of running/pending — a complete,
    self-consistent snapshot even if squeue has moved on by a few seconds.
    """
    if states is None:
        return list(groups)
    enriched: List[JobGroup] = []
    for g in groups:
        s = states.get(g.base_id)
        if not s:
            enriched.append(g)
            continue
        enriched.append(
            g.with_accounting(
                completed=s.get("COMPLETED", 0),
                failed=s.get("FAILED", 0) + s.get("CANCELLED", 0),
                total=sum(s.values()),
            )
        )
    return enriched


# ------------------------------------------------------------ scheduler probe


def slurm_available() -> bool:
    """Cheap PATH check used by the CLI to print a friendly error vs crash."""
    return shutil.which("squeue") is not None


class QueueCommandError(RuntimeError):
    """A scheduler query failed (binary missing, non-zero exit, timeout).

    Raised by the SSH transport so a broken remote never renders as an
    innocent empty table — over SSH, "no output" must mean "no jobs".
    """


# ------------------------------------------------------------------- SlurmQueue


class SlurmQueue:
    """Read-only Slurm queue introspection (local subprocess transport).

    All methods shell out to ``squeue`` / ``scontrol`` with explicit
    tab-delimited format strings. Returns dataclasses, not raw text — the CLI
    layer renders these into rich.Tables.

    Constructed with the two binaries as injectable seams so tests can point at
    the PATH-stub ``tests/fixtures/fake_slurm/`` without monkeypatching
    ``shutil.which``.
    """

    # Back-compat aliases (the canonical constants are module-level).
    _SQUEUE_FORMAT = SQUEUE_FORMAT
    _SQUEUE_FIELD_COUNT = _SQUEUE_FIELD_COUNT

    def __init__(
        self,
        squeue_bin: str = "squeue",
        scontrol_bin: str = "scontrol",
        timeout_s: float = 15.0,
        sacct_bin: str = "sacct",
        sinfo_bin: str = "sinfo",
    ):
        self.squeue_bin = squeue_bin
        self.scontrol_bin = scontrol_bin
        self.sacct_bin = sacct_bin
        self.sinfo_bin = sinfo_bin
        self.timeout_s = timeout_s

    # -------------------------------------------------------------- raw queries

    def _run_squeue(self, extra_args: List[str]) -> List[QueueJob]:
        """Run squeue with the canonical format string + caller's filters."""
        cmd = [self.squeue_bin, *squeue_args(extra_args)]
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
        return parse_squeue_output(result.stdout)

    # ------------------------------------------------------------- user-facing

    def list_user_jobs(self, user: str) -> List[QueueJob]:
        """All jobs (any state) belonging to ``user``.

        Pending arrays stay *collapsed* (one row, ``task_count`` > 1) —
        the compact view display paths want.
        """
        return self._run_squeue(["-u", user])

    def pending_gpu_jobs_sorted(self) -> List[QueueJob]:
        """Cluster-wide pending GPU *tasks*, highest priority first.

        ``squeue -S '-Q'`` sorts by priority descending — same order Slurm
        uses when deciding what runs next. ``-r`` expands pending arrays
        into per-task rows so positions/totals count tasks, not collapsed
        array rows.
        """
        jobs = self._run_squeue(["-r", "--state=PENDING", "-S", "-Q"])
        return filter_pending_gpu(jobs)

    def gpu_summary(self) -> Dict[str, Dict[str, int]]:
        """Per-GPU-type queue depth (``-r``-expanded; counts tasks × GPUs)."""
        return summarize_gpu_jobs(self._run_squeue(["-r"]))

    def position_in_gpu_queue(self, job_id: str) -> Optional[tuple[int, int]]:
        """Find ``job_id``'s position in the pending GPU queue.

        Returns ``(position, total)`` (1-based) or ``None`` if the job
        isn't pending. ``total`` counts pending GPU *tasks* cluster-wide;
        ``position`` is your slot, with ``1`` meaning "next up." Reflects a
        snapshot — scheduler is dynamic, see module docstring.
        """
        return find_queue_position(self.pending_gpu_jobs_sorted(), job_id)

    def sacct_job_states(
        self, base_ids: Sequence[str]
    ) -> Optional[Dict[str, Dict[str, int]]]:
        """Per-task state counts from accounting — OPTIONAL enrichment.

        Unlike squeue (mandatory; its failures are loud), sacct is routinely
        absent or disabled, so every failure path returns ``None`` and the
        caller degrades (no ✓/✗ split, no exact totals) instead of erroring.
        Empty ``base_ids`` short-circuits to ``{}`` ("nothing to ask" ≠
        "accounting broken").
        """
        if not base_ids:
            return {}
        try:
            result = subprocess.run(
                [self.sacct_bin, *sacct_args(base_ids)],
                capture_output=True,
                text=True,
                timeout=self.timeout_s,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as e:
            logger.debug(f"sacct unavailable: {e}")
            return None
        if result.returncode != 0:
            logger.debug(f"sacct rc={result.returncode}: {result.stderr.strip()}")
            return None
        return parse_sacct_job_states(result.stdout)

    def gpu_capacity(self) -> Optional[tuple[Dict[str, Dict[str, int]], int]]:
        """Per-type GPU totals + in-use counts from sinfo — OPTIONAL enrichment.

        Same contract as :meth:`sacct_job_states`: any failure returns
        ``None`` and the caller degrades to the queue-only view.
        """
        try:
            result = subprocess.run(
                [self.sinfo_bin, *sinfo_capacity_args()],
                capture_output=True,
                text=True,
                timeout=self.timeout_s,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as e:
            logger.debug(f"sinfo unavailable: {e}")
            return None
        if result.returncode != 0:
            logger.debug(f"sinfo rc={result.returncode}: {result.stderr.strip()}")
            return None
        return parse_sinfo_gpu_capacity(result.stdout)

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
        return parse_reservations_output(result.stdout)


# --------------------------------------------------------------- SSHSlurmQueue


class SSHSlurmQueue:
    """Async twin of :class:`SlurmQueue` over an established SSH connection.

    ``conn`` is duck-typed: anything with ``async run(cmd, check=False)``
    returning an object with ``.returncode`` / ``.stdout`` / ``.stderr``
    (``asyncssh.SSHClientConnection`` in production, ``FakeConn`` in tests).
    The connection's lifecycle belongs to the caller.

    Commands are built from the same pure helpers as the local transport and
    joined with :func:`shlex.join` — load-bearing, NOT cosmetic: the format
    string contains literal tabs, which an unquoted remote shell would
    word-split into separate argv entries (squeue treats a textual ``\\t``
    as two characters, only real tabs delimit).

    Failures raise :class:`QueueCommandError` instead of returning ``[]`` —
    over SSH an empty table must mean "no jobs", never "squeue quietly broke".
    """

    def __init__(
        self,
        conn: Any,
        *,
        squeue_bin: str = "squeue",
        scontrol_bin: str = "scontrol",
        timeout_s: float = 15.0,
        sacct_bin: str = "sacct",
        sinfo_bin: str = "sinfo",
    ):
        self._conn = conn
        self.squeue_bin = squeue_bin
        self.scontrol_bin = scontrol_bin
        self.sacct_bin = sacct_bin
        self.sinfo_bin = sinfo_bin
        self.timeout_s = timeout_s

    # -------------------------------------------------------------- raw queries

    async def _run(self, argv: Sequence[str]) -> str:
        """Run ``argv`` on the remote; return stdout or raise QueueCommandError."""
        cmd = shlex.join(argv)
        try:
            result = await asyncio.wait_for(
                self._conn.run(cmd, check=False), timeout=self.timeout_s
            )
        except asyncio.TimeoutError:
            raise QueueCommandError(
                f"remote {argv[0]!r} timed out after {self.timeout_s:.0f}s"
            ) from None
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            raise QueueCommandError(
                f"remote {argv[0]!r} exited rc={result.returncode}"
                + (f": {stderr}" if stderr else " (is Slurm on the remote's PATH?)")
            )
        return result.stdout or ""

    async def whoami(self) -> str:
        """The remote-side username (≠ local ``$USER`` in general)."""
        user = (await self._run(["whoami"])).strip()
        if not user:
            raise QueueCommandError("remote `whoami` returned nothing")
        return user

    async def _run_squeue(self, extra_args: List[str]) -> List[QueueJob]:
        stdout = await self._run([self.squeue_bin, *squeue_args(extra_args)])
        return parse_squeue_output(stdout)

    # ------------------------------------------------------------- user-facing

    async def list_user_jobs(self, user: str) -> List[QueueJob]:
        """All jobs (any state) belonging to ``user`` (collapsed arrays)."""
        return await self._run_squeue(["-u", user])

    async def pending_gpu_jobs_sorted(self) -> List[QueueJob]:
        """Cluster-wide pending GPU tasks, highest priority first (``-r``)."""
        jobs = await self._run_squeue(["-r", "--state=PENDING", "-S", "-Q"])
        return filter_pending_gpu(jobs)

    async def gpu_summary(self) -> Dict[str, Dict[str, int]]:
        """Per-GPU-type queue depth (``-r``-expanded; counts tasks × GPUs)."""
        return summarize_gpu_jobs(await self._run_squeue(["-r"]))

    async def position_in_gpu_queue(self, job_id: str) -> Optional[tuple[int, int]]:
        """Exact-id position in the pending GPU queue — see :class:`SlurmQueue`."""
        return find_queue_position(await self.pending_gpu_jobs_sorted(), job_id)

    async def sacct_job_states(
        self, base_ids: Sequence[str]
    ) -> Optional[Dict[str, Dict[str, int]]]:
        """Per-task state counts from accounting — OPTIONAL enrichment.

        Deliberate asymmetry with the squeue paths: those raise
        :class:`QueueCommandError` (a broken squeue must be loud), while a
        missing/disabled sacct is a normal cluster configuration → every
        failure here returns ``None`` and the view degrades gracefully.
        Does NOT use :meth:`_run` for exactly that reason.
        """
        if not base_ids:
            return {}
        cmd = shlex.join([self.sacct_bin, *sacct_args(base_ids)])
        try:
            result = await asyncio.wait_for(
                self._conn.run(cmd, check=False), timeout=self.timeout_s
            )
        except asyncio.TimeoutError:
            logger.debug(f"remote sacct timed out after {self.timeout_s:.0f}s")
            return None
        if result.returncode != 0:
            logger.debug(
                f"remote sacct rc={result.returncode}: {(result.stderr or '').strip()}"
            )
            return None
        return parse_sacct_job_states(result.stdout or "")

    async def gpu_capacity(self) -> Optional[tuple[Dict[str, Dict[str, int]], int]]:
        """Per-type GPU totals + in-use from sinfo — OPTIONAL enrichment.

        Same None-on-failure contract as :meth:`sacct_job_states` (and the
        same deliberate asymmetry with the raise-on-failure squeue paths).
        """
        cmd = shlex.join([self.sinfo_bin, *sinfo_capacity_args()])
        try:
            result = await asyncio.wait_for(
                self._conn.run(cmd, check=False), timeout=self.timeout_s
            )
        except asyncio.TimeoutError:
            logger.debug(f"remote sinfo timed out after {self.timeout_s:.0f}s")
            return None
        if result.returncode != 0:
            logger.debug(
                f"remote sinfo rc={result.returncode}: {(result.stderr or '').strip()}"
            )
            return None
        return parse_sinfo_gpu_capacity(result.stdout or "")

    # ----------------------------------------------------------- reservations

    async def reservations(self) -> List[Reservation]:
        """Parse ``scontrol show reservations`` — upcoming maintenance windows."""
        stdout = await self._run([self.scontrol_bin, "show", "reservations"])
        return parse_reservations_output(stdout)


_KV_RE = re.compile(r"([A-Za-z][A-Za-z0-9_]*)=(\S+)")


def parse_reservations_output(stdout: str) -> List[Reservation]:
    """Parse ``scontrol show reservations`` stdout into :class:`Reservation` rows.

    Pure (no subprocess) so SSH-driven sources can reuse it over their own
    transport (run ``scontrol`` via SSH, hand the stdout here). scontrol emits
    whitespace-separated ``key=value`` pairs, one reservation per blank-line-
    separated stanza.
    """
    stanzas = re.split(r"\n\s*\n", (stdout or "").strip())
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
