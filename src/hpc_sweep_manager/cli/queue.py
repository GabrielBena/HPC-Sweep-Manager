"""``hsm queue`` — cluster-wide Slurm queue inspection.

Four subcommands backed by :mod:`core.hpc.scheduler_queue`:

- ``mine`` — your jobs, with sweep IDs linked back to local
  ``sweeps/outputs/`` so you know which sweep each job belongs to.
- ``position`` — where each of your pending GPU jobs sits in the
  cluster-wide priority-sorted pending-GPU queue (the classic
  "you are task 50 / 125" view; arrays are counted per-task).
- ``gpus`` — per-GPU-type queue depth: how many H100/L4/A100/... GPUs
  are running and pending right now.
- ``reservations`` — upcoming maintenance windows from
  ``scontrol show reservations``.

All four are read-only and run over one of two transports:

- **local** — ``squeue``/``scontrol`` on this machine (cluster login node);
- **SSH** — the same commands on a remote, via ``--remote <alias>`` (a
  registered ``backend: slurm`` remote from ``distributed.remotes`` or a
  plain ``~/.ssh/config`` alias). When this machine has no ``squeue`` and
  exactly ONE slurm-backend remote is registered, it is used automatically
  (a note is printed) — so ``hsm queue mine`` "just works" from the
  workstation that drives the sweeps, which is also where the sweep-ID
  linkage data lives.

``mine`` and ``gpus`` take ``--watch [--refresh N]`` for a continuously
refreshing view; in SSH mode the connection is opened once and reused
across refreshes.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
import json
import os
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional

import click
from rich.console import Console
from rich.table import Table

from ..core.common.config import HSMConfig
from ..core.hpc.scheduler_queue import (
    KNOWN_GPU_VRAM_GB,
    JobGroup,
    QueueCommandError,
    QueueJob,
    Reservation,
    SlurmQueue,
    SSHSlurmQueue,
    enrich_groups_with_accounting,
    find_queue_position,
    group_jobs_by_array,
    positions_by_base,
    slurm_available,
    strip_array_suffix,
)
from .common import common_options


def _manifest_meta(sweep_dir: Path) -> List[tuple]:
    """``[(job_id, per_job_task_total|None), ...]`` from ``.hsm_manifest.json``.

    Remote-submitted sweeps record their Slurm job ids in the manifest, not
    in ``submission_summary.txt`` — without this fallback the job→sweep
    linkage is blank for exactly the sweeps you monitor from the driving
    workstation. Newer manifests carry per-job detail under ``jobs:``
    (multi-gpu_type sweeps legitimately submit several arrays); legacy
    manifests fall back to the sweep-level ``num_tasks``, attributable only
    when there's a single job.
    """
    manifest = sweep_dir / ".hsm_manifest.json"
    if not manifest.exists():
        return []
    try:
        data = json.loads(manifest.read_text())
    except (OSError, ValueError):
        return []
    if not isinstance(data, dict):
        return []
    jobs = data.get("jobs")
    if isinstance(jobs, list) and jobs and all(isinstance(j, dict) for j in jobs):
        out = []
        for j in jobs:
            jid = j.get("job_id")
            if jid is None:
                continue
            n = j.get("num_tasks")
            out.append((str(jid), n if isinstance(n, int) and n > 0 else None))
        if out:
            return out
    job_ids = data.get("job_ids")
    if not isinstance(job_ids, list):
        return []  # corrupt manifest must not masquerade as a query failure
    num_tasks = data.get("num_tasks")
    total = num_tasks if isinstance(num_tasks, int) and num_tasks > 0 else None
    per_job = total if len(job_ids) == 1 else None
    return [(str(j), per_job) for j in job_ids]


def _manifest_chain_meta(sweep_dir: Path) -> Optional[str]:
    """``"chunk k/max"`` when the sweep's manifest is a resumable chain (#12).

    Under option B only the latest chunk is ever in the queue, so a chain shows
    as ~one array row already — this just labels which chunk it is.
    """
    manifest = sweep_dir / ".hsm_manifest.json"
    if not manifest.exists():
        return None
    try:
        data = json.loads(manifest.read_text())
    except (OSError, ValueError):
        return None
    rblock = data.get("resumable") if isinstance(data, dict) else None
    if not (isinstance(rblock, dict) and rblock.get("enabled")):
        return None
    state = (data.get("chain") or {}).get("state") or {}
    cur = int(state.get("chunk_index", 0)) + 1
    cap = rblock.get("max_chunks")
    return f"chunk {cur}/{cap}" if cap else f"chunk {cur}"


def _build_sweep_meta_index(sweeps_root: Path) -> Dict[str, tuple]:
    """Walk local sweep dirs → ``{base_job_id: (sweep_id, array_total|None, chain_label|None)}``.

    Job IDs and totals come from ``submission_summary.txt`` (local/array
    submissions, via :func:`cli.sweep._load_sweep_meta` so we don't drift
    from the canonical parser) with a fallback to ``.hsm_manifest.json``
    (SSH-Slurm). A total is attributed only when the sweep maps to a SINGLE
    job id (one array == whole sweep); for individual-mode sweeps (N
    one-task jobs) a per-job total would be a lie.
    """
    from .sweep import _load_sweep_meta

    index: Dict[str, tuple] = {}
    if not sweeps_root.exists():
        return index
    for sweep_dir in sorted(sweeps_root.iterdir()):
        if not sweep_dir.is_dir():
            continue
        meta = _load_sweep_meta(sweep_dir)
        job_ids = [str(j) for j in (meta.get("job_ids") or [])]
        if job_ids:
            total: Optional[int] = meta.get("total_combinations") or None
            per_job_total = total if len(job_ids) == 1 else None
            pairs = [(j, per_job_total) for j in job_ids]
        else:
            pairs = _manifest_meta(sweep_dir)
        chain_label = _manifest_chain_meta(sweep_dir)
        for job_id, per_job_total in pairs:
            index[strip_array_suffix(job_id)] = (
                meta["sweep_id"],
                per_job_total,
                chain_label,
            )
    return index


def _resolve_user() -> str:
    return os.environ.get("USER") or os.environ.get("LOGNAME") or "?"


def _state_color(state: str) -> str:
    return {
        "RUNNING": "green",
        "PENDING": "yellow",
        "COMPLETING": "cyan",
        "FAILED": "red",
        "CANCELLED": "red",
        "TIMEOUT": "red",
    }.get(state, "white")


# ----------------------------------------------------------- transport plumbing


class _LocalQueueAsync:
    """Awaitable facade over the sync :class:`SlurmQueue`.

    Lets one driver (:func:`_run_queue_command`) serve both transports —
    the per-command ``gather`` coroutines are written once against the
    async surface and never know whether they run locally or over SSH.
    """

    def __init__(self) -> None:
        self._q = SlurmQueue()

    async def whoami(self) -> str:
        return _resolve_user()

    async def list_user_jobs(self, user: str) -> List[QueueJob]:
        return self._q.list_user_jobs(user)

    async def pending_gpu_jobs_sorted(self) -> List[QueueJob]:
        return self._q.pending_gpu_jobs_sorted()

    async def gpu_summary(self) -> Dict[str, Dict[str, int]]:
        return self._q.gpu_summary()

    async def position_in_gpu_queue(self, job_id: str) -> Optional[tuple[int, int]]:
        # Surface parity with SSHSlurmQueue — no gather uses this today, but
        # a facade missing a twin's method is a local-mode-only AttributeError
        # waiting to happen.
        return self._q.position_in_gpu_queue(job_id)

    async def sacct_job_states(self, base_ids) -> Optional[Dict[str, Dict[str, int]]]:
        return self._q.sacct_job_states(base_ids)

    async def gpu_capacity(self) -> Optional[tuple]:
        return self._q.gpu_capacity()

    async def reservations(self) -> List[Reservation]:
        return self._q.reservations()


def _slurm_backend_remotes(hsm_config: Optional[HSMConfig]) -> Dict[str, dict]:
    """Registered remotes with ``backend: slurm`` — the auto-fallback candidates."""
    if hsm_config is None:
        return {}
    distributed = hsm_config.config_data.get("distributed") or {}
    remotes = distributed.get("remotes") or {}
    return {
        name: (cfg or {})
        for name, cfg in remotes.items()
        if (cfg or {}).get("backend") == "slurm"
    }


def _remote_params(alias: str, hsm_config: Optional[HSMConfig]) -> Dict[str, Any]:
    """Resolve an alias to SSH connection params.

    Mirrors ``build_ssh_slurm_source``: per-remote ``host``/``ssh_key``/
    ``ssh_port`` from ``distributed.remotes.<alias>``; an unregistered alias
    is treated as a bare ``~/.ssh/config`` alias (host = alias).
    """
    remotes: Dict[str, Any] = {}
    if hsm_config is not None:
        remotes = (hsm_config.config_data.get("distributed") or {}).get("remotes") or {}
    cfg = remotes.get(alias) or {}
    return {
        "alias": alias,
        "host": cfg.get("host") or alias,
        "ssh_key": cfg.get("ssh_key"),
        "ssh_port": cfg.get("ssh_port"),
    }


def _resolve_queue_target(
    remote_alias: Optional[str], console: Console
) -> Optional[Dict[str, Any]]:
    """Pick the transport: ``None`` → local subprocess, dict → SSH params.

    Explicit ``--remote`` always wins. Otherwise local ``squeue`` if present.
    Otherwise auto-fallback to the *sole* registered slurm-backend remote
    (with a printed note); zero or several candidates → a clear error, never
    a silently empty view.
    """
    hsm_config = HSMConfig.load()
    if remote_alias:
        return _remote_params(remote_alias, hsm_config)
    if slurm_available():
        return None
    candidates = _slurm_backend_remotes(hsm_config)
    if len(candidates) == 1:
        alias = next(iter(candidates))
        console.print(
            f"[dim]No local squeue — using remote {alias!r} "
            f"(sole slurm-backend remote).[/dim]"
        )
        return _remote_params(alias, hsm_config)
    if candidates:
        names = ", ".join(sorted(candidates))
        raise click.ClickException(
            f"No local `squeue`, and {len(candidates)} slurm-backend remotes "
            f"are registered ({names}). Pick one with --remote <alias>."
        )
    raise click.ClickException(
        "No Slurm scheduler on PATH and no slurm-backend remotes registered. "
        "Run on a cluster login node, or pass --remote <alias> (an entry in "
        "distributed.remotes or a plain ~/.ssh/config alias)."
    )


def _run_queue_command(
    console: Console,
    target: Optional[Dict[str, Any]],
    gather: Callable[[Any], Awaitable[Any]],
    render: Callable[[Any], None],
    *,
    watch: bool = False,
    refresh: int = 30,
    title: str = "",
) -> None:
    """Drive one queue view over the chosen transport, optionally watching.

    ``gather(q)`` fetches data against the shared async surface
    (:class:`_LocalQueueAsync` or :class:`SSHSlurmQueue`); ``render(data)``
    paints it. In watch mode the SSH connection is opened ONCE and reused
    across refreshes (unlike ``hsm remote health --watch``, which reconnects
    per cycle).
    """

    async def cycle(q: Any) -> None:
        data = await gather(q)
        if watch:
            console.clear()
            where = f" — {target['alias']}" if target else ""
            stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            console.print(
                f"[bold]{title}{where} — {stamp}[/bold] "
                f"[dim](refresh {refresh}s, Ctrl+C to stop)[/dim]\n"
            )
        render(data)

    async def drive(q: Any) -> None:
        while True:
            await cycle(q)
            if not watch:
                return
            await asyncio.sleep(refresh)

    async def main() -> None:
        if target is None:
            await drive(_LocalQueueAsync())
            return
        from ..core.remote.discovery import create_ssh_connection

        conn = await create_ssh_connection(
            target["host"], target.get("ssh_key"), target.get("ssh_port")
        )
        try:
            await drive(SSHSlurmQueue(conn))
        finally:
            conn.close()
            try:
                await conn.wait_closed()
            except Exception:  # pragma: no cover - close-time noise only
                pass

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[yellow]Queue monitoring stopped.[/yellow]")
    except click.ClickException:
        raise
    except QueueCommandError as e:
        raise click.ClickException(str(e)) from e
    except Exception as e:
        # Connection refused / DNS / auth / ... — honest non-zero exit with
        # the host named, never a silently empty table.
        where = target["host"] if target else "local scheduler"
        raise click.ClickException(f"queue query against {where!r} failed: {e}") from e


def _remote_option(func):
    return click.option(
        "--remote",
        "remote_alias",
        default=None,
        metavar="ALIAS",
        help="Run the query over SSH on this remote (a `backend: slurm` entry "
        "from distributed.remotes, or a plain ~/.ssh/config alias).",
    )(func)


def _watch_options(func):
    func = click.option("--watch", is_flag=True, help="Auto-refresh continuously")(func)
    func = click.option(
        "--refresh",
        default=30,
        show_default=True,
        metavar="SECONDS",
        type=click.IntRange(min=1),  # 0/negative would busy-spin the watch loop
        help="Watch-mode refresh interval",
    )(func)
    return func


# ------------------------------------------------------------------- renderers


def _render_mine(console: Console, user: str, jobs: List[QueueJob]) -> None:
    """Flat (per-task) view — the `--flat` escape hatch."""
    if not jobs:
        console.print(f"[dim]No jobs in queue for {user!r}.[/dim]")
        return

    meta_index = _build_sweep_meta_index(Path.cwd() / "sweeps" / "outputs")

    table = Table(title=f"My queue ({user})")
    table.add_column("Job ID", style="cyan", no_wrap=True)
    table.add_column("State")
    table.add_column("Name", overflow="fold")
    table.add_column("Reason / Node")
    table.add_column("Tasks", justify="right")
    table.add_column("GPU", justify="right")
    table.add_column("Sweep", style="magenta")
    total_tasks = 0
    for j in jobs:
        total_tasks += j.task_count
        gpu_cell = f"{j.gpu_count}×{j.gpu_type}" if j.gpu_type else str(j.gpu_count or "")
        tasks_cell = f"×{j.task_count}" if j.task_count > 1 else ""
        sweep = meta_index.get(strip_array_suffix(j.job_id), ("", None, None))[0]
        table.add_row(
            j.job_id,
            f"[{_state_color(j.state)}]{j.state}[/{_state_color(j.state)}]",
            j.name,
            j.reason,
            tasks_cell,
            gpu_cell,
            sweep,
        )
    console.print(table)
    console.print(f"[dim]{len(jobs)} queue rows · {total_tasks} tasks.[/dim]")


def _bar(frac: float, width: int = 10) -> str:
    k = max(0, min(width, round(frac * width)))
    return "▰" * k + "▱" * (width - k)


def _render_mine_grouped(console: Console, user: str, groups: List[JobGroup]) -> None:
    """Default `mine` view: one row per array, with live progress.

    ▶/⏳ counts are squeue (live); ✓/✗ and the array total come from sacct
    enrichment — when accounting is unavailable they're omitted and the
    Progress bar falls back to the sweep-metadata total (or `—`): an
    unknown is shown as unknown, never as zero.
    """
    if not groups:
        console.print(f"[dim]No jobs in queue for {user!r}.[/dim]")
        return

    meta_index = _build_sweep_meta_index(Path.cwd() / "sweeps" / "outputs")

    table = Table(title=f"My queue ({user}) — grouped by array")
    table.add_column("Job ID", style="cyan", no_wrap=True)
    table.add_column("Name", overflow="fold")
    table.add_column("Tasks", no_wrap=True)
    table.add_column("Progress", no_wrap=True)
    table.add_column("GPU", justify="right")
    table.add_column("Where / Why")
    table.add_column("Sweep", style="magenta")

    n_arrays = n_singles = 0
    tot_running = tot_pending = tot_other = tot_finished = tot_failed = 0
    accounting_seen = False
    for g in groups:
        sweep_id, meta_total, chain_label = meta_index.get(g.base_id, ("", None, None))
        n_arrays += 1 if g.is_array else 0
        n_singles += 0 if g.is_array else 1
        tot_running += g.running
        tot_pending += g.pending
        tot_other += g.other

        parts = []
        if g.running:
            parts.append(f"[green]▶{g.running}[/green]")
        if g.pending:
            parts.append(f"[yellow]⏳{g.pending}[/yellow]")
        if g.other:
            parts.append(f"[dim]+{g.other}[/dim]")
        if g.completed is not None:
            accounting_seen = True
            if g.completed:
                parts.append(f"[green]✓{g.completed}[/green]")
            if g.failed:
                parts.append(f"[red]✗{g.failed}[/red]")
                tot_failed += g.failed
        tasks_cell = " ".join(parts) or "[dim]0[/dim]"

        # Progress: sacct total preferred; sweep-metadata total as fallback
        # (finished = left-the-queue, ✓/✗ split unknown); else no bar.
        total = g.total or meta_total
        if g.completed is not None:
            finished: Optional[int] = g.completed + (g.failed or 0)
        elif total:
            finished = max(total - g.in_queue, 0)
        else:
            finished = None
        if total and finished is not None:
            frac = min(finished / total, 1.0)
            progress_cell = f"{_bar(frac)} {finished}/{total}"
            tot_finished += finished
        else:
            progress_cell = "[dim]—[/dim]"

        gpu_cell = f"{g.gpu_count}×{g.gpu_type}" if g.gpu_type else str(g.gpu_count or "")
        if g.nodes:
            where = g.nodes[0] if len(g.nodes) == 1 else f"{len(g.nodes)} nodes"
        else:
            where = g.reason
        # Resumable chains (#12): label which chunk this is.
        sweep_cell = sweep_id + (f" [dim]({chain_label})[/dim]" if chain_label else "")
        table.add_row(
            g.base_id, g.name, tasks_cell, progress_cell, gpu_cell, where, sweep_cell
        )

    console.print(table)

    footer = []
    if n_arrays:
        footer.append(f"{n_arrays} array(s)")
    if n_singles:
        footer.append(f"{n_singles} single job(s)")
    in_queue = tot_running + tot_pending + tot_other
    footer.append(
        f"{in_queue} task(s) in queue ({tot_running} running, {tot_pending} pending"
        + (f", {tot_other} other" if tot_other else "")
        + ")"
    )
    if tot_finished:
        footer.append(f"{tot_finished} finished")
    line = "[dim]" + " · ".join(footer) + "[/dim]"
    if tot_failed:
        line += f" · [red]{tot_failed} FAILED[/red]"
    elif not accounting_seen:
        line += " [dim](no accounting data — ✓/✗ unavailable)[/dim]"
    console.print(line)
    console.print("[dim]`--flat` for individual tasks.[/dim]")


_REASON_LEGEND = (
    "[dim]Reason codes: (Resources) = next-up, (Priority) = waiting on "
    "higher-priority jobs ahead, (QOSMaxJobsPerUserLimit) = your own QoS cap — "
    "tasks start as your running ones finish.[/dim]"
)


def _render_position_single(
    console: Console, job_id: str, pending: List[QueueJob]
) -> None:
    total = len(pending)
    exact = find_queue_position(pending, job_id)
    if exact is not None:
        pos, total = exact
        console.print(
            f"[bold]{job_id}[/bold]: position [cyan]{pos}[/cyan] / "
            f"[cyan]{total}[/cyan] pending GPU tasks cluster-wide"
        )
        return
    # Array base id? Report the first (best-placed) pending task of the array.
    hits = positions_by_base(pending).get(strip_array_suffix(job_id))
    if hits:
        console.print(
            f"[bold]{job_id}[/bold]: [cyan]{len(hits)}[/cyan] pending GPU task(s) — "
            f"first at position [cyan]{hits[0]}[/cyan] / [cyan]{total}[/cyan] "
            f"pending GPU tasks cluster-wide"
        )
        return
    console.print(
        f"[yellow]Job {job_id!r} not found in pending GPU queue "
        f"(may be RUNNING, COMPLETED, or not a GPU job).[/yellow]"
    )


def _render_position_all(
    console: Console, user: str, my_jobs: List[QueueJob], pending: List[QueueJob]
) -> None:
    my_pending_gpu = [j for j in my_jobs if j.state == "PENDING" and j.gpu_count > 0]
    cpu_only_tasks = sum(
        j.task_count for j in my_jobs if j.state == "PENDING" and j.gpu_count == 0
    )
    if not my_pending_gpu:
        msg = f"No pending GPU jobs for {user!r}"
        if cpu_only_tasks:
            msg += (
                f" — {cpu_only_tasks} pending task(s) are CPU-only "
                f"(no GPU queue to position in)"
            )
        else:
            msg += " (you have nothing in the GPU queue to position)"
        console.print(f"[dim]{msg}.[/dim]")
        return

    total = len(pending)
    by_base = positions_by_base(pending)
    my_task_total = sum(j.task_count for j in my_pending_gpu)

    table = Table(
        title=f"GPU queue position — {my_task_total} task(s) of yours / "
        f"{total} total pending GPU tasks"
    )
    table.add_column("Job ID", style="cyan", no_wrap=True)
    table.add_column("Tasks", justify="right")
    table.add_column("First position", justify="right")
    table.add_column("Reason")
    table.add_column("GPU", justify="right")
    table.add_column("Expected Start")
    for j in my_pending_gpu:
        hits = by_base.get(strip_array_suffix(j.job_id))
        pos_cell = f"{hits[0]} / {total}" if hits else "?"
        gpu_cell = f"{j.gpu_count}×{j.gpu_type}" if j.gpu_type else str(j.gpu_count)
        table.add_row(
            j.job_id,
            f"×{j.task_count}" if j.task_count > 1 else "1",
            pos_cell,
            j.reason,
            gpu_cell,
            j.expected_start,
        )
    console.print(table)
    if cpu_only_tasks:
        console.print(
            f"[dim]Plus {cpu_only_tasks} pending CPU-only task(s) of yours, "
            f"not part of the GPU queue.[/dim]"
        )
    console.print(_REASON_LEGEND)


def _mine_gpu_contribution(
    mine_jobs: Optional[List[QueueJob]],
) -> tuple[Dict[str, int], Dict[str, int]]:
    """Per-type GPU counts of the user's running/pending jobs (task-weighted)."""
    mine_running: Dict[str, int] = {}
    mine_pending: Dict[str, int] = {}
    for j in mine_jobs or []:
        if j.gpu_count == 0:
            continue
        t = j.gpu_type or "<untyped>"
        if j.state == "RUNNING":
            mine_running[t] = mine_running.get(t, 0) + j.gpu_count * j.task_count
        elif j.state == "PENDING":
            mine_pending[t] = mine_pending.get(t, 0) + j.gpu_count * j.task_count
    return mine_running, mine_pending


def _format_vram(type_key: str, cap_entry: Dict) -> tuple[str, bool]:
    """VRAM cell for a GPU type: ``(text, used_model_typical_fallback)``.

    Cluster-reported (``vram_gb`` from GPUMEM feature tags) wins — shown
    plain, with mixed node groups joined (``40/80G``). Otherwise the
    model-typical table gives a ``~``-prefixed value, and unknown models
    show ``?`` — an unknown is never displayed as a confident number.
    """
    vram = cap_entry.get("vram_gb")
    if vram:
        return "/".join(str(v) for v in vram) + "G", False
    known = KNOWN_GPU_VRAM_GB.get(type_key.upper())
    if known:
        return f"~{known}G", True
    return "[dim]?[/dim]", False


def _render_gpus(
    console: Console,
    summary: Dict[str, Dict[str, int]],
    mine_jobs: Optional[List[QueueJob]],
    capacity: Optional[tuple] = None,
) -> None:
    """GPU depth table — capacity-aware when sinfo data is available.

    With capacity: Type | Total | In use | Free | Pending | Mine. "In use"
    is Slurm's own per-node allocation accounting (GresUsed), so GPUs
    consumed by *untyped* job requests are attributed to their physical
    type — and Free = Total − In use is real, not an estimate. Without
    capacity (sinfo absent): the legacy queue-only view.
    """
    mine_running, mine_pending = _mine_gpu_contribution(mine_jobs)

    if capacity is not None:
        cap_by_type, excluded_gpus = capacity
        type_keys = sorted(set(cap_by_type) | set(summary))
        if not type_keys:
            console.print("[dim]No GPUs configured and no GPU jobs in queue.[/dim]")
            return
        table = Table(title="GPU capacity & queue by type")
        table.add_column("Type", style="cyan")
        table.add_column("VRAM/GPU", justify="right")
        table.add_column("Total", justify="right")
        table.add_column("In use", justify="right", style="green")
        table.add_column("Free", justify="right", style="bold green")
        table.add_column("Pending", justify="right", style="yellow")
        if mine_jobs is not None:
            table.add_column("Mine (R/P)", justify="right", style="magenta")
        free_total = 0
        any_model_typical = False
        for type_key in type_keys:
            cap = cap_by_type.get(type_key)
            pending = summary.get(type_key, {}).get("PENDING", 0)
            if cap:
                vram_cell, model_typical = _format_vram(type_key, cap)
                any_model_typical |= model_typical
                free = max(cap["total"] - cap["used"], 0)
                free_total += free
                row = [
                    type_key,
                    vram_cell,
                    str(cap["total"]),
                    str(cap["used"]),
                    str(free) if free else "0",
                    str(pending) if pending else "",
                ]
            else:
                # Demand for a type sinfo doesn't list (e.g. the "<untyped>"
                # request bucket) — no physical inventory to show.
                row = [type_key, "", "", "", "", str(pending) if pending else ""]
            if mine_jobs is not None:
                row.append(
                    f"{mine_running.get(type_key, 0)}/{mine_pending.get(type_key, 0)}"
                )
            table.add_row(*row)
        console.print(table)
        line = f"[bold green]{free_total}[/bold green] GPU(s) free right now"
        if excluded_gpus:
            line += f" [dim](+{excluded_gpus} on down/drained nodes, excluded)[/dim]"
        console.print(line)
        if "<untyped>" in type_keys:
            console.print(
                "[dim]<untyped> = jobs requesting a GPU without a type "
                "(e.g. --gpus=1) — demand only; once running, their GPUs are "
                "attributed to the physical type in the In-use column.[/dim]"
            )
        if any_model_typical:
            console.print(
                "[dim]~ = model-typical VRAM (this cluster doesn't report it).[/dim]"
            )
        return

    # Legacy queue-only view (sinfo unavailable).
    if not summary:
        console.print("[dim]No GPU jobs in queue right now.[/dim]")
        return
    table = Table(title="GPU queue depth by type")
    table.add_column("Type", style="cyan")
    table.add_column("Running", justify="right", style="green")
    table.add_column("Pending", justify="right", style="yellow")
    table.add_column("Other", justify="right", style="dim")
    if mine_jobs is not None:
        table.add_column("Mine (R/P)", justify="right", style="magenta")
    for type_key in sorted(summary.keys()):
        per_state = summary[type_key]
        running = per_state.get("RUNNING", 0)
        pending = per_state.get("PENDING", 0)
        other = sum(c for s, c in per_state.items() if s not in ("RUNNING", "PENDING"))
        row = [type_key, str(running), str(pending), str(other) if other else ""]
        if mine_jobs is not None:
            row.append(f"{mine_running.get(type_key, 0)}/{mine_pending.get(type_key, 0)}")
        table.add_row(*row)
    console.print(table)
    console.print("[dim]No sinfo capacity data — totals/free unavailable.[/dim]")


def _render_reservations(console: Console, reservations: List[Reservation]) -> None:
    if not reservations:
        console.print(
            "[dim]No upcoming reservations. Cluster is free of scheduled "
            "maintenance windows right now.[/dim]"
        )
        return

    table = Table(title="Upcoming reservations")
    table.add_column("Name", style="cyan")
    table.add_column("Start")
    table.add_column("End")
    table.add_column("Duration")
    table.add_column("Nodes", justify="right")
    table.add_column("Node spec", style="dim")
    for r in reservations:
        table.add_row(
            r.name, r.start_time, r.end_time, r.duration, str(r.node_count), r.nodes
        )
    console.print(table)


# ------------------------------------------------------------ CLI definitions


@click.group()
def queue():
    """Cluster-wide Slurm queue inspection (read-only, local or --remote)."""


@queue.command("mine")
@click.option("--flat", is_flag=True, help="One row per task (ungrouped legacy view)")
@_remote_option
@_watch_options
@common_options
@click.pass_context
def queue_mine(
    ctx,
    flat: bool,
    remote_alias: str,
    watch: bool,
    refresh: int,
    verbose: bool,
    quiet: bool,
):
    """Your jobs grouped by array, with live progress and sweep linkage.

    One row per array: running/pending counts from squeue (live),
    completed/failed counts and the array total from sacct accounting
    (gracefully omitted on clusters without it), sweep IDs linked back to
    local sweeps/outputs/. Use --flat for the per-task rows.
    """
    console = ctx.obj["console"]
    target = _resolve_queue_target(remote_alias, console)

    async def gather(q):
        user = await q.whoami()
        jobs = await q.list_user_jobs(user)
        if flat:
            return {"user": user, "jobs": jobs}
        groups = group_jobs_by_array(jobs)
        states = (
            await q.sacct_job_states([g.base_id for g in groups]) if groups else {}
        )
        return {"user": user, "groups": enrich_groups_with_accounting(groups, states)}

    def render(data):
        if flat:
            _render_mine(console, data["user"], data["jobs"])
        else:
            _render_mine_grouped(console, data["user"], data["groups"])

    _run_queue_command(
        console, target, gather, render, watch=watch, refresh=refresh, title="My queue"
    )


@queue.command("position")
@click.argument("job_id", required=False)
@_remote_option
@common_options
@click.pass_context
def queue_position(ctx, job_id: str, remote_alias: str, verbose: bool, quiet: bool):
    """Position of your pending GPU job(s) in the cluster-wide priority queue.

    With JOB_ID: report position of that specific job (array base IDs match
    their first pending task). Without: report position of every one of your
    pending GPU jobs. Positions and totals count TASKS — a pending array of
    50 tasks occupies 50 slots.

    Position is a snapshot — Slurm is dynamic, higher-priority submissions
    can displace pending jobs. Reason codes: (Resources) = next-up,
    (Priority) = waiting on jobs ahead, (QOSMaxJobsPerUserLimit) = your own
    QoS cap.
    """
    console = ctx.obj["console"]
    target = _resolve_queue_target(remote_alias, console)

    async def gather(q):
        pending = await q.pending_gpu_jobs_sorted()
        if job_id:
            return {"pending": pending}
        user = await q.whoami()
        return {"pending": pending, "user": user, "my_jobs": await q.list_user_jobs(user)}

    def render(data):
        if job_id:
            _render_position_single(console, job_id, data["pending"])
        else:
            _render_position_all(console, data["user"], data["my_jobs"], data["pending"])

    _run_queue_command(console, target, gather, render)


@queue.command("gpus")
@click.option(
    "--mine/--no-mine",
    "show_mine",
    default=True,
    show_default=True,
    help="Annotate rows with your GPU counts",
)
@_remote_option
@_watch_options
@common_options
@click.pass_context
def queue_gpus(
    ctx,
    show_mine: bool,
    remote_alias: str,
    watch: bool,
    refresh: int,
    verbose: bool,
    quiet: bool,
):
    """Per-GPU-type capacity and queue depth, cluster-wide.

    Total / In use / Free come from sinfo's per-node allocation accounting
    (gracefully omitted on clusters without it); Pending demand from
    squeue; your own contribution in the Mine column (--no-mine to hide).
    """
    console = ctx.obj["console"]
    target = _resolve_queue_target(remote_alias, console)

    async def gather(q):
        summary = await q.gpu_summary()
        capacity = await q.gpu_capacity()
        mine_jobs = None
        if show_mine:
            mine_jobs = await q.list_user_jobs(await q.whoami())
        return summary, mine_jobs, capacity

    def render(data):
        summary, mine_jobs, capacity = data
        _render_gpus(console, summary, mine_jobs, capacity)

    _run_queue_command(
        console,
        target,
        gather,
        render,
        watch=watch,
        refresh=refresh,
        title="GPU capacity & queue",
    )


@queue.command("reservations")
@_remote_option
@common_options
@click.pass_context
def queue_reservations(ctx, remote_alias: str, verbose: bool, quiet: bool):
    """Upcoming Slurm maintenance windows (scontrol show reservations)."""
    console = ctx.obj["console"]
    target = _resolve_queue_target(remote_alias, console)

    async def gather(q):
        return await q.reservations()

    _run_queue_command(
        console, target, gather, lambda data: _render_reservations(console, data)
    )


__all__ = ["queue"]
