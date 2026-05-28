"""``hsm queue`` — cluster-wide Slurm queue inspection.

Four subcommands backed by :mod:`core.hpc.scheduler_queue`:

- ``mine`` — your jobs, with sweep IDs linked back to local
  ``sweeps/outputs/`` so you know which sweep each job belongs to.
- ``position`` — where each of your pending GPU jobs sits in the
  cluster-wide priority-sorted pending-GPU queue (the classic
  "you are job 50 / 125" view).
- ``gpus`` — per-GPU-type queue depth: how many H100/L4/A100/... jobs
  are running and pending right now.
- ``reservations`` — upcoming maintenance windows from
  ``scontrol show reservations``.

All four are read-only and degrade gracefully when ``squeue`` isn't on
PATH (prints a one-line "no scheduler detected" message rather than
crashing).
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Dict, List

import click
from rich.console import Console
from rich.table import Table

from ..core.hpc.scheduler_queue import (
    QueueJob,
    Reservation,
    SlurmQueue,
    slurm_available,
)
from .common import common_options


# Slurm array tasks show up in squeue as `<base>_<index>` (e.g. "3553026_1").
# Strip the suffix to match against the base ID we store in submission_summary.txt.
_ARRAY_SUFFIX_RE = re.compile(r"_(\d+|\[.+\])$")


def _strip_array_suffix(job_id: str) -> str:
    return _ARRAY_SUFFIX_RE.sub("", job_id)


def _build_sweep_id_index(sweeps_root: Path) -> Dict[str, str]:
    """Walk ``sweeps/outputs/*/submission_summary.txt`` and build a job→sweep map.

    Returns ``{base_job_id: sweep_id}``. Uses :func:`cli.sweep._load_sweep_meta`
    so we don't drift from the canonical parser.
    """
    from .sweep import _load_sweep_meta

    index: Dict[str, str] = {}
    if not sweeps_root.exists():
        return index
    for sweep_dir in sorted(sweeps_root.iterdir()):
        if not sweep_dir.is_dir():
            continue
        meta = _load_sweep_meta(sweep_dir)
        for job_id in meta.get("job_ids") or []:
            index[_strip_array_suffix(job_id)] = meta["sweep_id"]
    return index


def _resolve_user() -> str:
    return os.environ.get("USER") or os.environ.get("LOGNAME") or "?"


def _no_scheduler(console: Console) -> None:
    console.print(
        "[yellow]No Slurm scheduler detected on PATH "
        "(`squeue` not found). `hsm queue` is for HPC clusters.[/yellow]"
    )


def _state_color(state: str) -> str:
    return {
        "RUNNING": "green",
        "PENDING": "yellow",
        "COMPLETING": "cyan",
        "FAILED": "red",
        "CANCELLED": "red",
        "TIMEOUT": "red",
    }.get(state, "white")


# ------------------------------------------------------------ CLI definitions


@click.group()
def queue():
    """Cluster-wide Slurm queue inspection (read-only)."""


@queue.command("mine")
@common_options
@click.pass_context
def queue_mine(ctx, verbose: bool, quiet: bool):
    """List your jobs with sweep IDs linked back to local sweeps/outputs/."""
    console = ctx.obj["console"]
    if not slurm_available():
        _no_scheduler(console)
        return

    user = _resolve_user()
    jobs = SlurmQueue().list_user_jobs(user)
    if not jobs:
        console.print(f"[dim]No jobs in queue for {user!r}.[/dim]")
        return

    sweep_index = _build_sweep_id_index(Path.cwd() / "sweeps" / "outputs")

    table = Table(title=f"My queue ({user})")
    table.add_column("Job ID", style="cyan", no_wrap=True)
    table.add_column("State")
    table.add_column("Name")
    table.add_column("Reason / Node")
    table.add_column("GPU", justify="right")
    table.add_column("Sweep", style="magenta")
    for j in jobs:
        gpu_cell = f"{j.gpu_count}×{j.gpu_type}" if j.gpu_type else str(j.gpu_count or "")
        sweep = sweep_index.get(_strip_array_suffix(j.job_id), "")
        table.add_row(
            j.job_id,
            f"[{_state_color(j.state)}]{j.state}[/{_state_color(j.state)}]",
            j.name,
            j.reason,
            gpu_cell,
            sweep,
        )
    console.print(table)


@queue.command("position")
@click.argument("job_id", required=False)
@common_options
@click.pass_context
def queue_position(ctx, job_id: str, verbose: bool, quiet: bool):
    """Position of your pending GPU job(s) in the cluster-wide priority queue.

    With JOB_ID: report position of that specific job.
    Without: report position of every one of your pending GPU jobs.

    Position is a snapshot — Slurm is dynamic, higher-priority submissions
    can displace pending jobs. Reason codes: (Resources) = next-up,
    (Priority) = waiting on jobs ahead in the queue.
    """
    console = ctx.obj["console"]
    if not slurm_available():
        _no_scheduler(console)
        return

    q = SlurmQueue()

    if job_id:
        result = q.position_in_gpu_queue(job_id)
        if result is None:
            console.print(
                f"[yellow]Job {job_id!r} not found in pending GPU queue "
                f"(may be RUNNING, COMPLETED, or not a GPU job).[/yellow]"
            )
            return
        pos, total = result
        console.print(
            f"[bold]{job_id}[/bold]: position [cyan]{pos}[/cyan] / "
            f"[cyan]{total}[/cyan] pending GPU jobs cluster-wide"
        )
        return

    # No job_id given — list every pending GPU job of mine.
    user = _resolve_user()
    my_jobs = [
        j for j in q.list_user_jobs(user) if j.state == "PENDING" and j.gpu_count > 0
    ]
    if not my_jobs:
        console.print(
            f"[dim]No pending GPU jobs for {user!r} "
            f"(you have nothing in the GPU queue to position).[/dim]"
        )
        return

    pending = q.pending_gpu_jobs_sorted()
    total = len(pending)
    positions = {j.job_id: idx for idx, j in enumerate(pending, start=1)}

    table = Table(title=f"GPU queue position — {len(my_jobs)} of yours / {total} total pending")
    table.add_column("Job ID", style="cyan", no_wrap=True)
    table.add_column("Position", justify="right")
    table.add_column("Reason")
    table.add_column("GPU", justify="right")
    table.add_column("Expected Start")
    for j in my_jobs:
        pos = positions.get(j.job_id, "?")
        gpu_cell = f"{j.gpu_count}×{j.gpu_type}" if j.gpu_type else str(j.gpu_count)
        table.add_row(
            j.job_id,
            f"{pos} / {total}",
            j.reason,
            gpu_cell,
            j.expected_start,
        )
    console.print(table)
    console.print(
        "[dim]Reason codes: (Resources) = next-up, "
        "(Priority) = waiting on higher-priority jobs ahead.[/dim]"
    )


@queue.command("gpus")
@click.option("--mine", "show_mine", is_flag=True, help="Annotate rows with your job counts")
@common_options
@click.pass_context
def queue_gpus(ctx, show_mine: bool, verbose: bool, quiet: bool):
    """Per-GPU-type queue depth (running vs pending, cluster-wide)."""
    console = ctx.obj["console"]
    if not slurm_available():
        _no_scheduler(console)
        return

    q = SlurmQueue()
    summary = q.gpu_summary()
    if not summary:
        console.print("[dim]No GPU jobs in queue right now.[/dim]")
        return

    # Optionally compute the user's per-type contribution to each row.
    mine_running: Dict[str, int] = {}
    mine_pending: Dict[str, int] = {}
    if show_mine:
        user = _resolve_user()
        for j in q.list_user_jobs(user):
            if j.gpu_count == 0:
                continue
            t = j.gpu_type or "<untyped>"
            if j.state == "RUNNING":
                mine_running[t] = mine_running.get(t, 0) + j.gpu_count
            elif j.state == "PENDING":
                mine_pending[t] = mine_pending.get(t, 0) + j.gpu_count

    table = Table(title="GPU queue depth by type")
    table.add_column("Type", style="cyan")
    table.add_column("Running", justify="right", style="green")
    table.add_column("Pending", justify="right", style="yellow")
    table.add_column("Other", justify="right", style="dim")
    if show_mine:
        table.add_column("Mine (R/P)", justify="right", style="magenta")
    for type_key in sorted(summary.keys()):
        per_state = summary[type_key]
        running = per_state.get("RUNNING", 0)
        pending = per_state.get("PENDING", 0)
        other = sum(c for s, c in per_state.items() if s not in ("RUNNING", "PENDING"))
        row = [type_key, str(running), str(pending), str(other) if other else ""]
        if show_mine:
            row.append(f"{mine_running.get(type_key, 0)}/{mine_pending.get(type_key, 0)}")
        table.add_row(*row)
    console.print(table)


@queue.command("reservations")
@common_options
@click.pass_context
def queue_reservations(ctx, verbose: bool, quiet: bool):
    """Upcoming Slurm maintenance windows (scontrol show reservations)."""
    console = ctx.obj["console"]
    if not slurm_available():
        _no_scheduler(console)
        return

    reservations = SlurmQueue().reservations()
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


__all__ = ["queue"]
