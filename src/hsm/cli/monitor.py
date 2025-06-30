"""Monitoring CLI commands."""

import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any

import click
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel

from ..core.tracker import SweepTracker, CompletionStatus, TaskStatus
from ..utils.logging import get_logger


@click.group("monitor")
@click.pass_context
def monitor(ctx):
    """Monitoring and status commands."""
    pass


@monitor.command("watch")
@click.argument("sweep_id", required=False)
@click.option("--refresh", "-r", type=int, default=5, help="Refresh interval in seconds")
@click.option("--sources", is_flag=True, help="Show compute source details")
@click.pass_context
def watch_cmd(ctx, sweep_id: Optional[str], refresh: int, sources: bool):
    """Watch sweep progress in real-time."""
    console: Console = ctx.obj["console"]
    logger = get_logger()

    try:
        if sweep_id:
            _watch_sweep(console, sweep_id, refresh, sources)
        else:
            _watch_all_sweeps(console, refresh)

    except KeyboardInterrupt:
        console.print("\n[yellow]Monitoring stopped[/yellow]")
    except Exception as e:
        logger.error(f"Monitoring failed: {e}")
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


@monitor.command("recent")
@click.option("--days", "-d", type=int, default=7, help="Show sweeps from last N days")
@click.option("--limit", "-l", type=int, default=20, help="Maximum number of sweeps to show")
@click.pass_context
def recent_cmd(ctx, days: int, limit: int):
    """Show recent sweeps."""
    console: Console = ctx.obj["console"]

    try:
        _show_recent_sweeps(console, days, limit)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


@monitor.command("errors")
@click.argument("sweep_id")
@click.option("--limit", "-l", type=int, default=10, help="Maximum number of errors to show")
@click.option("--pattern", help="Filter errors by pattern")
@click.pass_context
def errors_cmd(ctx, sweep_id: str, limit: int, pattern: Optional[str]):
    """Show sweep errors and failures."""
    console: Console = ctx.obj["console"]

    try:
        _show_sweep_errors(console, sweep_id, limit, pattern)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


# Helper functions


def _find_sweep_directory(sweep_id: str) -> Optional[Path]:
    """Find the directory for a given sweep ID."""
    # Look in common sweep output locations
    search_paths = [
        Path("sweeps/outputs"),
        Path("outputs"),
        Path("."),
    ]

    for base_path in search_paths:
        if base_path.exists():
            # Look for exact match
            sweep_dir = base_path / sweep_id
            if sweep_dir.exists():
                return sweep_dir

            # Look for directories starting with sweep_id
            for dir_path in base_path.iterdir():
                if dir_path.is_dir() and dir_path.name.startswith(sweep_id):
                    return dir_path

    return None


def _discover_sweep_directories() -> List[Path]:
    """Discover all sweep directories."""
    sweep_dirs = []

    search_paths = [
        Path("sweeps/outputs"),
        Path("outputs"),
    ]

    for base_path in search_paths:
        if base_path.exists():
            for dir_path in base_path.iterdir():
                if dir_path.is_dir():
                    # Check if it looks like a sweep directory
                    if (dir_path / "sweep_metadata.yaml").exists() or (
                        dir_path / "task_mapping.yaml"
                    ).exists():
                        sweep_dirs.append(dir_path)

    return sorted(sweep_dirs, key=lambda x: x.stat().st_mtime, reverse=True)


def _load_sweep_data(sweep_dir: Path) -> Optional[Dict[str, Any]]:
    """Load sweep data from directory."""
    try:
        tracker = SweepTracker(sweep_dir, sweep_dir.name)

        # Sync from disk to get latest state
        asyncio.run(tracker.sync_from_disk())

        summary = tracker.get_task_summary()
        completion_status = tracker.get_completion_status()

        # Calculate progress
        total_tasks = summary.get("total", 0)
        completed_tasks = summary.get("completed", 0)
        progress_percent = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0

        # Get metadata
        metadata = tracker.metadata

        return {
            "sweep_id": sweep_dir.name,
            "sweep_dir": sweep_dir,
            "total_tasks": total_tasks,
            "completed_tasks": completed_tasks,
            "failed_tasks": summary.get("failed", 0),
            "running_tasks": summary.get("running", 0),
            "pending_tasks": summary.get("pending", 0),
            "cancelled_tasks": summary.get("cancelled", 0),
            "progress_percent": progress_percent,
            "completion_status": completion_status,
            "metadata": metadata,
            "tracker": tracker,
        }
    except Exception as e:
        logger = get_logger()
        logger.warning(f"Could not load sweep data from {sweep_dir}: {e}")
        return None


def _format_duration(start_time: Optional[datetime], end_time: Optional[datetime] = None) -> str:
    """Format duration between start and end time."""
    if not start_time:
        return "Unknown"

    if end_time is None:
        end_time = datetime.now()

    delta = end_time - start_time

    if delta.days > 0:
        return f"{delta.days}d {delta.seconds // 3600}h"
    elif delta.seconds >= 3600:
        return f"{delta.seconds // 3600}h {(delta.seconds % 3600) // 60}m"
    elif delta.seconds >= 60:
        return f"{delta.seconds // 60}m {delta.seconds % 60}s"
    else:
        return f"{delta.seconds}s"


def _estimate_eta(sweep_data: Dict[str, Any]) -> str:
    """Estimate time to completion based on current progress."""
    if not sweep_data["metadata"] or sweep_data["completed_tasks"] == 0:
        return "Unknown"

    start_time = sweep_data["metadata"].created_time
    current_time = datetime.now()
    elapsed = current_time - start_time

    completed = sweep_data["completed_tasks"]
    total = sweep_data["total_tasks"]
    remaining = total - completed

    if remaining <= 0:
        return "Complete"

    # Calculate average time per task
    avg_time_per_task = elapsed.total_seconds() / completed
    estimated_remaining_seconds = avg_time_per_task * remaining

    eta_delta = timedelta(seconds=estimated_remaining_seconds)

    if eta_delta.days > 0:
        return f"{eta_delta.days}d {eta_delta.seconds // 3600}h"
    elif eta_delta.seconds >= 3600:
        return f"{eta_delta.seconds // 3600}h {(eta_delta.seconds % 3600) // 60}m"
    else:
        return f"{eta_delta.seconds // 60}m"


def _watch_sweep(console: Console, sweep_id: str, refresh: int, show_sources: bool):
    """Watch a specific sweep."""
    sweep_dir = _find_sweep_directory(sweep_id)
    if not sweep_dir:
        console.print(f"[red]Sweep '{sweep_id}' not found[/red]")
        return

    def create_sweep_table():
        sweep_data = _load_sweep_data(sweep_dir)
        if not sweep_data:
            table = Table(title=f"Sweep Status: {sweep_id}")
            table.add_column("Status", style="red")
            table.add_row("Error loading sweep data")
            return table

        table = Table(title=f"Sweep Status: {sweep_id}")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")

        # Status color based on completion
        status_color = (
            "green" if sweep_data["completion_status"] == CompletionStatus.COMPLETE else "yellow"
        )
        if sweep_data["completion_status"] == CompletionStatus.FAILED:
            status_color = "red"

        table.add_row(
            "Status",
            f"[{status_color}]{sweep_data['completion_status'].value.title()}[/{status_color}]",
        )
        table.add_row(
            "Progress",
            f"{sweep_data['completed_tasks']}/{sweep_data['total_tasks']} ({sweep_data['progress_percent']:.1f}%)",
        )
        table.add_row("Completed", str(sweep_data["completed_tasks"]))
        table.add_row("Failed", str(sweep_data["failed_tasks"]))
        table.add_row("Running", str(sweep_data["running_tasks"]))
        table.add_row("Pending", str(sweep_data["pending_tasks"]))

        if sweep_data["metadata"]:
            duration = _format_duration(sweep_data["metadata"].created_time)
            table.add_row("Duration", duration)

            eta = _estimate_eta(sweep_data)
            table.add_row("ETA", eta)

        if show_sources and sweep_data["metadata"]:
            console.print("\n")
            source_table = Table(title="Compute Sources")
            source_table.add_column("Source", style="cyan")
            source_table.add_column("Active Tasks", style="green")
            source_table.add_column("Status", style="yellow")

            # Get task counts by source
            tracker = sweep_data["tracker"]
            source_counts = {}
            for source in sweep_data["metadata"].compute_sources:
                source_tasks = tracker.get_tasks_by_source(source)
                running_count = len(
                    [
                        tid
                        for tid in source_tasks
                        if tracker.get_task_status(tid) == TaskStatus.RUNNING
                    ]
                )
                source_counts[source] = running_count

            for source, count in source_counts.items():
                source_table.add_row(source, str(count), "Active" if count > 0 else "Idle")

            console.print(source_table)

        return table

    with Live(create_sweep_table(), refresh_per_second=1 / refresh) as live:
        try:
            while True:
                import time

                time.sleep(refresh)
                live.update(create_sweep_table())
        except KeyboardInterrupt:
            pass


def _watch_all_sweeps(console: Console, refresh: int):
    """Watch all active sweeps."""

    def create_all_sweeps_table():
        sweep_dirs = _discover_sweep_directories()

        table = Table(title="All Sweeps")
        table.add_column("Sweep ID", style="cyan")
        table.add_column("Progress", style="green")
        table.add_column("Status", style="yellow")
        table.add_column("Duration", style="blue")
        table.add_column("ETA", style="magenta")

        for sweep_dir in sweep_dirs[:10]:  # Show top 10 most recent
            sweep_data = _load_sweep_data(sweep_dir)
            if not sweep_data:
                continue

            # Status color
            status = sweep_data["completion_status"]
            if status == CompletionStatus.COMPLETE:
                status_color = "green"
            elif status == CompletionStatus.FAILED:
                status_color = "red"
            else:
                status_color = "yellow"

            progress_str = f"{sweep_data['completed_tasks']}/{sweep_data['total_tasks']}"
            status_str = f"[{status_color}]{status.value.title()}[/{status_color}]"

            duration = "Unknown"
            eta = "Unknown"
            if sweep_data["metadata"]:
                duration = _format_duration(sweep_data["metadata"].created_time)
                eta = _estimate_eta(sweep_data)

            table.add_row(
                sweep_data["sweep_id"][:20],  # Truncate long IDs
                progress_str,
                status_str,
                duration,
                eta,
            )

        if not sweep_dirs:
            table.add_row("No sweeps found", "", "", "", "")

        return table

    with Live(create_all_sweeps_table(), refresh_per_second=1 / refresh) as live:
        try:
            while True:
                import time

                time.sleep(refresh)
                live.update(create_all_sweeps_table())
        except KeyboardInterrupt:
            pass


def _show_recent_sweeps(console: Console, days: int, limit: int):
    """Show recent sweeps."""
    sweep_dirs = _discover_sweep_directories()

    # Filter by date
    cutoff_date = datetime.now() - timedelta(days=days)
    recent_sweeps = []

    for sweep_dir in sweep_dirs:
        try:
            mtime = datetime.fromtimestamp(sweep_dir.stat().st_mtime)
            if mtime >= cutoff_date:
                recent_sweeps.append((sweep_dir, mtime))
        except Exception:
            continue

    # Sort by modification time (most recent first) and limit
    recent_sweeps.sort(key=lambda x: x[1], reverse=True)
    recent_sweeps = recent_sweeps[:limit]

    if not recent_sweeps:
        console.print(f"[yellow]No sweeps found in the last {days} days[/yellow]")
        return

    table = Table(title=f"Recent Sweeps (last {days} days)")
    table.add_column("Sweep ID", style="cyan")
    table.add_column("Started", style="green")
    table.add_column("Status", style="yellow")
    table.add_column("Progress", style="blue")
    table.add_column("Duration", style="magenta")

    for sweep_dir, mtime in recent_sweeps:
        sweep_data = _load_sweep_data(sweep_dir)
        if not sweep_data:
            continue

        # Format relative time
        time_diff = datetime.now() - mtime
        if time_diff.days > 0:
            started_str = f"{time_diff.days}d ago"
        elif time_diff.seconds >= 3600:
            started_str = f"{time_diff.seconds // 3600}h ago"
        else:
            started_str = f"{time_diff.seconds // 60}m ago"

        # Status color
        status = sweep_data["completion_status"]
        if status == CompletionStatus.COMPLETE:
            status_color = "green"
        elif status == CompletionStatus.FAILED:
            status_color = "red"
        else:
            status_color = "yellow"

        progress_str = f"{sweep_data['completed_tasks']}/{sweep_data['total_tasks']}"
        status_str = f"[{status_color}]{status.value.title()}[/{status_color}]"

        duration = "Unknown"
        if sweep_data["metadata"]:
            duration = _format_duration(sweep_data["metadata"].created_time)

        table.add_row(
            sweep_data["sweep_id"][:30],  # Truncate long IDs
            started_str,
            status_str,
            progress_str,
            duration,
        )

    console.print(table)
    console.print(f"\n[dim]Showing {len(recent_sweeps)} sweeps (max {limit})[/dim]")


def _show_sweep_errors(console: Console, sweep_id: str, limit: int, pattern: Optional[str]):
    """Show errors for a specific sweep."""
    sweep_dir = _find_sweep_directory(sweep_id)
    if not sweep_dir:
        console.print(f"[red]Sweep '{sweep_id}' not found[/red]")
        return

    sweep_data = _load_sweep_data(sweep_dir)
    if not sweep_data:
        console.print(f"[red]Could not load data for sweep '{sweep_id}'[/red]")
        return

    tracker = sweep_data["tracker"]
    failed_tasks = tracker.get_tasks_by_status(TaskStatus.FAILED)

    if not failed_tasks:
        console.print(f"[green]No failed tasks found in sweep '{sweep_id}'[/green]")
        return

    # Load error details from task directories
    errors = []
    for task_id in failed_tasks[:limit]:  # Limit results
        task_dir = sweep_dir / "tasks" / task_id
        if task_dir.exists():
            stderr_file = task_dir / "stderr.log"
            status_file = task_dir / "status.yaml"

            error_msg = "Unknown error"
            timestamp = "Unknown"

            # Try to get error from stderr
            if stderr_file.exists():
                try:
                    with open(stderr_file) as f:
                        stderr_content = f.read()
                        if stderr_content.strip():
                            # Get last few lines of stderr
                            lines = stderr_content.strip().split("\n")
                            error_msg = lines[-1][:100]  # Truncate long errors
                except Exception:
                    pass

            # Try to get timestamp from status
            if status_file.exists():
                try:
                    import yaml

                    with open(status_file) as f:
                        status_data = yaml.safe_load(f)
                        if status_data and "failed_time" in status_data:
                            timestamp = status_data["failed_time"]
                except Exception:
                    pass

            # Apply pattern filter
            if pattern and pattern.lower() not in error_msg.lower():
                continue

            errors.append({"task_id": task_id, "error_msg": error_msg, "timestamp": timestamp})

    if not errors:
        if pattern:
            console.print(
                f"[yellow]No errors matching pattern '{pattern}' found in sweep '{sweep_id}'[/yellow]"
            )
        else:
            console.print(f"[yellow]Could not load error details for sweep '{sweep_id}'[/yellow]")
        return

    table = Table(title=f"Errors in {sweep_id}")
    table.add_column("Task", style="cyan")
    table.add_column("Error Message", style="red")
    table.add_column("Time", style="blue")

    for error in errors:
        table.add_row(error["task_id"], error["error_msg"], error["timestamp"])

    console.print(table)

    if pattern:
        console.print(f"\n[dim]Filtered by pattern: {pattern}[/dim]")

    if len(failed_tasks) > limit:
        console.print(f"\n[dim]Showing {len(errors)} of {len(failed_tasks)} failed tasks[/dim]")
