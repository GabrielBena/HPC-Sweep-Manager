"""Pure planning helpers for heterogeneous GPU-type scheduling (issue #7).

No I/O, no scheduler calls — same idiom as :mod:`slurm_protocol`: both Slurm
sources (local sbatch and sbatch-over-SSH) call :func:`plan_gpu_split`, so the
partitioning logic cannot drift between transports, and the CLI dry-run calls
the SAME function so the previewed plan is exactly what submits.

Semantics (user-facing docs: HPC_EXECUTION.md):

- ``speed_factors`` maps GPU type → relative RUNTIME multiplier. The
  reference type has factor 1.0; smaller = faster (``h200: 0.4`` runs in 40%
  of the time), larger = slower (``l4: 3.0``). Keys are matched
  case-insensitively — config may say ``a100`` while the GRES directive needs
  ``A100``; the plan keeps the cased form from ``gpu_types`` for rendering
  (GRES names are case-sensitive, CLAUDE.md gotcha #6). Factors are
  workload-specific numbers the user measures or estimates (spec-sheet ratios
  can be badly wrong — see issue #7's V100 discussion); a future
  ``hsm calibrate`` will write them from probe runs.
- ``base_walltime`` = budget for the MAX-cost task on a factor-1.0 type. A
  sub-array's walltime = ``base × factor × (its max cost / global max cost)``,
  ceiled to the minute, floored at 10 minutes. Deliberately NO upper cap:
  slower types legitimately need longer than base.
- Assignment is greedy LPT (longest-processing-time-first) on uniform
  machines: tasks sorted by cost descending, each placed on the bin with the
  smallest resulting completion time ``(load + cost) × factor`` — the classic
  4/3-approx for makespan, plenty here. With uniform costs this degenerates
  to balanced counts ∝ 1/factor. Deterministic: stable sort + first-wins
  tie-break in ``gpu_types`` order.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
import logging
import math
import re
from typing import Any, Dict, List, Mapping, Optional, Sequence

from ..common.resource_spec import ResourceSpec
from ..common.utils import format_walltime, parse_walltime

logger = logging.getLogger(__name__)

_MIN_WALLTIME_S = 600  # 10 min — tiny cost ratios must not starve a task


@dataclass(frozen=True)
class GpuTypePlan:
    """One typed sub-array: which tasks, what walltime, which GRES type."""

    gpu_type: str  # cased as configured — rendered verbatim into --gres
    indices: tuple[int, ...]  # 0-based positions into the original params_list
    walltime: Optional[str]  # scaled HH:MM:SS, or None when no base walltime
    speed_factor: float


def normalize_speed_factors(
    factors: Optional[Mapping[Any, Any]],
    *,
    warn_context: str = "speed_factors",
) -> Optional[Dict[str, float]]:
    """Validate + normalize a speed_factors mapping (the ONE implementation).

    Lowercased string keys, float values; entries that are non-numeric,
    non-positive, or non-finite are dropped with a warning. Returns ``None``
    for unset/empty/non-mapping input. Shared by the ``slurm:``-block
    accessor, the per-remote factory, and (for display) the CLI — so the
    validators cannot drift.
    """
    if not factors:
        return None
    if not isinstance(factors, Mapping):
        logger.warning(
            f"{warn_context} must be a mapping of gpu type → number; "
            f"got {type(factors).__name__}. Ignoring."
        )
        return None
    out: Dict[str, float] = {}
    for k, v in factors.items():
        try:
            f = float(v)
        except (TypeError, ValueError):
            logger.warning(
                f"{warn_context}[{k!r}] is not a number ({v!r}). Ignoring entry."
            )
            continue
        if f <= 0 or not math.isfinite(f):
            logger.warning(
                f"{warn_context}[{k!r}] must be a finite number > 0, got {f}. "
                f"Ignoring entry."
            )
            continue
        out[str(k).lower()] = f
    return out or None


def _lookup_cost_map(raw: Any, cost_map: Mapping[Any, Any]) -> Optional[float]:
    """Tolerant cost_map lookup: exact key first, then string-equal match
    (YAML round-trips can turn int keys into strings and vice versa).
    Booleans are excluded — `True == 1` would silently match an int key."""
    if isinstance(raw, bool):
        return None
    try:
        if raw in cost_map:
            return float(cost_map[raw])
    except TypeError:
        return None  # unhashable param value — cannot be a map key
    for k, v in cost_map.items():
        if str(k) == str(raw):
            return float(v)
    return None


def task_costs(
    params_list: Sequence[Mapping[str, Any]],
    cost_param: Optional[str],
    cost_map: Optional[Mapping[Any, Any]] = None,
) -> List[float]:
    """Per-task relative costs read from the swept param named ``cost_param``.

    With ``cost_map``, the param's value is translated (e.g.
    ``{5: 7.0, 16: 23.0}`` — measured hours; only ratios matter). Without it,
    the value itself is used (must be numeric).

    Tasks with no usable cost (param missing, non-numeric without a map,
    value absent from the map, or non-positive) default to the MAX usable
    cost — an unknown cost is treated as expensive, so its bin's walltime
    can never be scaled DOWN by it (under-provisioning → TIMEOUT is the
    failure that burns real allocation; over-provisioning merely queues a
    little longer). The warning names the tasks and the consequence.

    ``cost_param`` references an EXISTING swept param: costs never enter the
    hydra override string, so templating is untouched.
    """
    if not cost_param:
        return [1.0] * len(params_list)
    costs: List[Optional[float]] = []
    defaulted: List[int] = []
    for i, params in enumerate(params_list):
        raw = params.get(cost_param)
        value: Optional[float] = None
        if raw is not None:
            if cost_map:
                value = _lookup_cost_map(raw, cost_map)
            elif isinstance(raw, (int, float)) and not isinstance(raw, bool):
                value = float(raw)
        if value is not None and (value <= 0 or not math.isfinite(value)):
            value = None
        if value is None:
            defaulted.append(i + 1)
        costs.append(value)
    usable = [c for c in costs if c is not None]
    fallback = max(usable) if usable else 1.0
    if defaulted:
        shown = ", ".join(str(t) for t in defaulted[:10])
        more = f" (+{len(defaulted) - 10} more)" if len(defaulted) > 10 else ""
        logger.warning(
            f"cost_param {cost_param!r}: {len(defaulted)} task(s) had no usable "
            f"cost (missing, non-numeric, non-positive, or absent from "
            f"cost_map) — treating them as the MAX known cost ({fallback:g}) so "
            f"their sub-array's walltime cannot be under-provisioned (TIMEOUT "
            f"risk). Fix the cost_map/param and re-check with --dry-run. "
            f"Task(s): {shown}{more}"
        )
    return [c if c is not None else fallback for c in costs]


def plan_gpu_split(
    *,
    costs: Sequence[float],
    gpu_types: Sequence[str],
    speed_factors: Optional[Mapping[str, Any]] = None,
    base_walltime: Optional[str] = None,
) -> List[GpuTypePlan]:
    """Partition tasks across GPU types — greedy LPT on uniform machines.

    Returns one :class:`GpuTypePlan` per type that received work (types left
    empty when there are more types than tasks are dropped, with an info
    log). ``indices`` are ascending within each plan, so downstream
    ``global_index`` numbering stays ordered.
    """
    if not gpu_types:
        raise ValueError("plan_gpu_split: gpu_types must be non-empty")
    if base_walltime and len(base_walltime.split(":")) != 3:
        # parse_walltime reads "48:00" as 48 MINUTES (MM:SS) — scaled and
        # multiplied across sub-arrays, that silent 60x under-provision
        # would TIMEOUT everything. Multi-type mode demands the explicit form.
        raise ValueError(
            f"plan_gpu_split: base walltime must be HH:MM:SS when gpu_type "
            f"is a list, got {base_walltime!r} (ambiguous — '48:00' would "
            f"mean 48 minutes, not 48 hours)"
        )
    factors_norm = {
        str(k).lower(): float(v) for k, v in (speed_factors or {}).items()
    }
    bins: List[dict] = []
    missing: List[str] = []
    for t in gpu_types:
        factor = factors_norm.get(t.lower())
        if factor is None:
            missing.append(t)
            factor = 1.0
        if factor <= 0 or not math.isfinite(factor):
            raise ValueError(
                f"plan_gpu_split: speed factor for {t!r} must be > 0, got {factor}"
            )
        bins.append({"type": t, "factor": factor, "indices": [], "load": 0.0})
    if missing and factors_norm:
        # The user IS using speed factors but didn't cover these types —
        # defaulting a genuinely-slower type to 1.0 would over-assign work
        # to it AND under-provision its walltime (mass TIMEOUT, the
        # expensive failure). A partial map is a config bug: refuse.
        raise ValueError(
            f"plan_gpu_split: speed_factors is set but has no entry for gpu "
            f"type(s) {missing} — add them (relative runtime multipliers; "
            f"reference type = 1.0). Refusing to guess: a wrong factor "
            f"under-provisions walltime → TIMEOUT."
        )
    if missing:
        logger.warning(
            f"no speed_factors configured — treating all of {list(gpu_types)} "
            f"as equally fast (even split, unscaled walltime). If any of "
            f"these types is slower, its tasks may TIMEOUT; set "
            f"`speed_factors:` and re-check with --dry-run."
        )

    # LPT: costliest first; min() is stable → ties go to the earlier bin in
    # the user's gpu_types order. Sort is stable → equal-cost tasks keep
    # their original relative order.
    order = sorted(range(len(costs)), key=lambda i: -costs[i])
    for i in order:
        c = costs[i]
        best = min(bins, key=lambda b: (b["load"] + c) * b["factor"])
        best["indices"].append(i)
        best["load"] += c

    max_cost = max(costs) if costs else 1.0
    if max_cost <= 0:
        max_cost = 1.0  # pure-API guard: explicit all-zero costs must not ZeroDivide
    plans: List[GpuTypePlan] = []
    for b in bins:
        if not b["indices"]:
            logger.info(
                f"gpu type {b['type']!r}: no tasks assigned (more types than "
                f"work) — skipping its sub-array"
            )
            continue
        walltime = None
        if base_walltime:
            bin_max = max(costs[i] for i in b["indices"])
            seconds = parse_walltime(base_walltime) * b["factor"] * (bin_max / max_cost)
            seconds = max(int(math.ceil(seconds / 60.0)) * 60, _MIN_WALLTIME_S)
            walltime = format_walltime(seconds)
        plans.append(
            GpuTypePlan(
                gpu_type=b["type"],
                indices=tuple(sorted(b["indices"])),
                walltime=walltime,
                speed_factor=b["factor"],
            )
        )
    return plans


# --------------------------------------------------- array-submission descriptors


@dataclass(frozen=True)
class SubArraySubmission:
    """Everything one ``sbatch --array`` call needs — built identically for
    both transports so the local and SSH Slurm sources cannot drift."""

    job_name: str
    params_filename: str
    # ({"index": array-local 1..k, "global_index": original 1..N, "params": {...}}, ...)
    entries: tuple[dict, ...]
    spec: ResourceSpec  # scalarized: gpu_type str|None, walltime already scaled
    gpu_type: Optional[str]  # None on the single-type path
    # The factor the planner actually used (incl. defaults) — display layers
    # must read THIS, not re-derive it from config (drift risk).
    speed_factor: float = 1.0


def replace_sub_walltime(sub: SubArraySubmission, walltime: str) -> SubArraySubmission:
    """Return a copy of ``sub`` with its spec's walltime overridden (issue #12).

    Resumable chains CAP every chunk at ``chunk_walltime`` rather than letting
    the planner's cost-SCALED per-type walltime stand: chunking caps, it does
    not scale. #7's cost-based *placement* (which type a task lands on) still
    applies — only the per-chunk wall-time is flattened to the cap.
    """
    return replace(sub, spec=replace(sub.spec, walltime=walltime))


def _safe_type_token(gpu_type: str) -> str:
    """A gpu type as a filename/job-name fragment (defensive — GRES names
    are alphanumeric in practice)."""
    return re.sub(r"[^A-Za-z0-9_.-]+", "", gpu_type) or "gpu"


def build_array_submissions(
    *,
    params_list: Sequence[Mapping[str, Any]],
    effective_spec: ResourceSpec,
    prefix: str,
    speed_factors: Optional[Mapping[str, Any]] = None,
    costs: Optional[Sequence[float]] = None,
) -> List[SubArraySubmission]:
    """Turn one logical array submission into 1..K concrete ones.

    Single-type specs (str/None ``gpu_type``) yield EXACTLY today's shapes —
    job name ``<prefix>_array``, file ``parameter_combinations.json``, spec
    untouched — so the scalar path stays byte-identical. Multi-type specs
    are planned via :func:`plan_gpu_split`: per-type job names / params
    files, scalarized specs with scaled walltimes, and ``global_index``
    preserving each task's original 1..N position (task dirs stay globally
    numbered; ``index`` is what ``$SLURM_ARRAY_TASK_ID`` matches within the
    sub-array).
    """
    if not isinstance(effective_spec.gpu_type, tuple):
        entries = tuple(
            {"index": i + 1, "global_index": i + 1, "params": p}
            for i, p in enumerate(params_list)
        )
        return [
            SubArraySubmission(
                job_name=f"{prefix}_array",
                params_filename="parameter_combinations.json",
                entries=entries,
                spec=effective_spec,
                gpu_type=None,
            )
        ]

    if costs is None:
        cost_seq: List[float] = [1.0] * len(params_list)
    else:
        cost_seq = [float(c) for c in costs]
        if len(cost_seq) != len(params_list):
            raise ValueError(
                f"build_array_submissions: {len(cost_seq)} costs for "
                f"{len(params_list)} tasks"
            )
    plans = plan_gpu_split(
        costs=cost_seq,
        gpu_types=effective_spec.gpu_type,
        speed_factors=speed_factors,
        base_walltime=effective_spec.walltime,
    )
    tokens = [_safe_type_token(p.gpu_type) for p in plans]
    if len(set(tokens)) != len(tokens):
        # Two types sanitizing to the same token would share a params file
        # (second write CLOBBERS the first) → one sub-array silently runs
        # the other's parameter slice. Refuse — this is the repo's worst
        # failure class (silent wrong-config, gotcha #11's cousin).
        dupes = sorted({t for t in tokens if tokens.count(t) > 1})
        raise ValueError(
            f"build_array_submissions: gpu_type entries collide after "
            f"sanitization ({dupes!r} from {list(effective_spec.gpu_type)!r}) "
            f"— their params files/job names would overwrite each other. "
            f"Use distinct type names."
        )
    submissions: List[SubArraySubmission] = []
    for plan, token in zip(plans, tokens):
        sub_spec = replace(
            effective_spec,
            gpu_type=plan.gpu_type,
            walltime=plan.walltime or effective_spec.walltime,
        )
        entries = tuple(
            {"index": j + 1, "global_index": i + 1, "params": params_list[i]}
            for j, i in enumerate(plan.indices)
        )
        submissions.append(
            SubArraySubmission(
                job_name=f"{prefix}_array_{token}",
                params_filename=f"parameter_combinations_{token}.json",
                entries=entries,
                spec=sub_spec,
                gpu_type=plan.gpu_type,
                speed_factor=plan.speed_factor,
            )
        )
    return submissions


def jobs_manifest_entries(
    job_ids: Sequence[str], jobinfo_params: Mapping[str, Mapping[str, Any]]
) -> List[Dict[str, Any]]:
    """Per-job manifest entries (`jobs:`) from submitted JobInfo params.

    Gives downstream consumers (queue linkage, collect) per-job task counts
    and gpu types — the `len(job_ids)==1` heuristic stops being load-bearing.
    """
    out: List[Dict[str, Any]] = []
    for jid in job_ids:
        p = jobinfo_params.get(jid) or {}
        out.append(
            {
                "job_id": jid,
                "gpu_type": p.get("_gpu_type"),
                "num_tasks": int(p.get("_array_size", 1)),
            }
        )
    return out
