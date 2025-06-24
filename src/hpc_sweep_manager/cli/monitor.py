"""Monitoring CLI commands."""

import subprocess
import time
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
import logging

from ..core.utils import format_duration


class SweepMonitor:
    """Monitor PBS-based sweeps."""

    def __init__(self, console: Console, logger: logging.Logger):
        self.console = console
        self.logger = logger
        self.sweeps_dir = Path("sweeps/outputs")

    def discover_recent_sweeps(self, days: int = 7) -> List[Dict]:
        """Discover recent sweeps from the outputs directory."""
        if not self.sweeps_dir.exists():
            return []

        recent_sweeps = []
        cutoff_date = datetime.now() - timedelta(days=days)

        for sweep_dir in self.sweeps_dir.iterdir():
            if not sweep_dir.is_dir():
                continue

            # Parse sweep ID timestamp
            if not sweep_dir.name.startswith("sweep_"):
                continue

            try:
                timestamp_str = sweep_dir.name.replace("sweep_", "")
                sweep_date = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")

                if sweep_date >= cutoff_date:
                    sweep_info = self._load_sweep_info(sweep_dir)
                    if sweep_info:
                        recent_sweeps.append(sweep_info)

            except ValueError:
                continue

        # Sort by submission time (newest first)
        recent_sweeps.sort(key=lambda x: x["submission_time"], reverse=True)
        return recent_sweeps

    def _load_sweep_info(self, sweep_dir: Path) -> Optional[Dict]:
        """Load sweep information from submission summary."""
        summary_file = sweep_dir / "submission_summary.txt"
        if not summary_file.exists():
            return None

        try:
            with open(summary_file, "r") as f:
                content = f.read()

            # Parse the summary file
            info = {
                "sweep_id": sweep_dir.name,
                "sweep_dir": sweep_dir,
                "submission_time": None,
                "mode": None,
                "total_combinations": 0,
                "job_ids": [],
                "walltime": None,
                "resources": None,
                "wandb_group": None,
            }

            for line in content.split("\n"):
                if line.startswith("Submission Time:"):
                    time_str = line.split(":", 1)[1].strip()
                    try:
                        info["submission_time"] = datetime.fromisoformat(time_str)
                    except ValueError:
                        # Try parsing without microseconds
                        try:
                            info["submission_time"] = datetime.strptime(
                                time_str, "%Y-%m-%d %H:%M:%S"
                            )
                        except ValueError:
                            pass

                elif line.startswith("Mode:"):
                    info["mode"] = line.split(":", 1)[1].strip()
                elif line.startswith("Total Combinations:"):
                    info["total_combinations"] = int(line.split(":", 1)[1].strip())
                elif line.startswith("Job IDs:"):
                    job_ids_str = line.split(":", 1)[1].strip()
                    info["job_ids"] = [jid.strip() for jid in job_ids_str.split(",")]
                elif line.startswith("Walltime:"):
                    info["walltime"] = line.split(":", 1)[1].strip()
                elif line.startswith("Resources:"):
                    info["resources"] = line.split(":", 1)[1].strip()
                elif line.startswith("W&B Group:"):
                    info["wandb_group"] = line.split(":", 1)[1].strip()

            return info

        except Exception as e:
            self.logger.warning(f"Error loading sweep info from {summary_file}: {e}")
            return None

    def get_pbs_job_status(self, job_ids: List[str]) -> Dict:
        """Get status of PBS jobs."""
        status_info = {
            "queued": 0,
            "running": 0,
            "completed": 0,
            "failed": 0,
            "unknown": 0,
            "details": {},
        }

        try:
            # Query job status using qstat
            for job_id in job_ids:
                try:
                    # For array jobs, we need to handle PBS format like 512429[].pbs-7
                    # Extract the base job ID properly
                    if "[" in job_id and "]" in job_id:
                        # For PBS array jobs like 512429[].pbs-7, we use the full job ID
                        base_job_id = job_id
                    else:
                        base_job_id = job_id

                    # Check if it's an array job (includes both [1-N] and [] formats)
                    if "[" in job_id and "]" in job_id:
                        # Array job - use qstat -t to get sub-job details
                        # For PBS, we can use the full job ID including the suffix
                        result = subprocess.run(
                            ["qstat", "-t", base_job_id],
                            capture_output=True,
                            text=True,
                            timeout=10,
                        )

                        if result.returncode == 0:
                            job_details = self._parse_array_job_output(
                                result.stdout, job_id
                            )
                            status_info["details"][job_id] = job_details

                            # Count array job sub-job states
                            for detail in job_details:
                                state = detail.get("state", "unknown").lower()
                                self._categorize_job_state(state, status_info)
                        else:
                            # Try regular qstat to see if job exists at all
                            regular_result = subprocess.run(
                                ["qstat", base_job_id],
                                capture_output=True,
                                text=True,
                                timeout=10,
                            )
                            if regular_result.returncode == 0:
                                # Job exists but may be in a different state
                                job_details = self._parse_regular_job_output(
                                    regular_result.stdout, job_id
                                )
                                status_info["details"][job_id] = job_details
                                for detail in job_details:
                                    state = detail.get("state", "unknown").lower()
                                    self._categorize_job_state(state, status_info)
                            else:
                                # Job not found - finished (completed or failed)
                                status_info["completed"] += 1
                                status_info["details"][job_id] = [
                                    {
                                        "state": "not_found",
                                        "job_id": job_id,
                                        "note": "Job finished (not in PBS queue)",
                                    }
                                ]
                    else:
                        # Regular job
                        result = subprocess.run(
                            ["qstat", job_id],
                            capture_output=True,
                            text=True,
                            timeout=10,
                        )

                        if result.returncode == 0:
                            job_details = self._parse_regular_job_output(
                                result.stdout, job_id
                            )
                            status_info["details"][job_id] = job_details

                            # Count job states
                            for detail in job_details:
                                state = detail.get("state", "unknown").lower()
                                self._categorize_job_state(state, status_info)
                        else:
                            # Job not found in queue - finished (completed or failed)
                            status_info["completed"] += 1
                            status_info["details"][job_id] = [
                                {
                                    "state": "not_found",
                                    "job_id": job_id,
                                    "note": "Job finished (not in PBS queue)",
                                }
                            ]

                except subprocess.TimeoutExpired:
                    self.logger.warning(f"Timeout querying job {job_id}")
                    status_info["unknown"] += 1
                    status_info["details"][job_id] = [
                        {"state": "timeout", "job_id": job_id}
                    ]

        except Exception as e:
            self.logger.error(f"Error getting PBS job status: {e}")

        return status_info

    def _categorize_job_state(self, state: str, status_info: Dict):
        """Categorize a job state into the appropriate counter."""
        if state in ["q", "h"]:  # Queued or held
            status_info["queued"] += 1
        elif state in ["r", "e"]:  # Running or exiting
            status_info["running"] += 1
        elif state in ["c"]:  # Completed
            status_info["completed"] += 1
        elif state in ["f", "a"]:  # Failed or aborted
            status_info["failed"] += 1
        elif state in ["b"]:  # Array job begun (some sub-jobs running)
            status_info["running"] += 1
        elif state in ["not_found"]:  # Job not in PBS system anymore (finished)
            status_info["completed"] += 1
        else:
            status_info["unknown"] += 1

    def _parse_array_job_output(self, output: str, job_id: str) -> List[Dict]:
        """Parse qstat -t output for array jobs to extract sub-job details."""
        jobs = []
        lines = output.strip().split("\n")

        # Look for the data section
        data_lines = []
        header_found = False

        for i, line in enumerate(lines):
            if "Job ID" in line or "Job id" in line or "---" in line:
                header_found = True
                continue
            elif header_found and line.strip():
                # This should be data
                data_lines.append(line)

        for line in data_lines:
            if not line.strip():
                continue

            parts = line.split()
            if len(parts) >= 5:
                # Array job format: jobid[index] name user time_use state queue
                job_info = {
                    "job_id": parts[0],
                    "name": parts[1] if len(parts) > 1 else "",
                    "user": parts[2] if len(parts) > 2 else "",
                    "time_use": parts[3] if len(parts) > 3 else "",
                    "state": parts[4] if len(parts) > 4 else "",
                    "queue": parts[5] if len(parts) > 5 else "",
                }

                # Extract array index if present
                if "[" in job_info["job_id"] and "]" in job_info["job_id"]:
                    array_index = job_info["job_id"].split("[")[1].split("]")[0]
                    job_info["array_index"] = array_index

                    # Only include sub-jobs with actual array indices, not the main array job
                    # Main array job has empty brackets [] while sub-jobs have [1], [2], etc.
                    if array_index.strip():  # Only include if array_index is not empty
                        jobs.append(job_info)
                else:
                    # Regular job (not array), include it
                    jobs.append(job_info)

        return jobs

    def _parse_regular_job_output(self, output: str, job_id: str) -> List[Dict]:
        """Parse regular qstat output for single jobs."""
        jobs = []
        lines = output.strip().split("\n")

        # Skip header lines
        data_lines = []
        for i, line in enumerate(lines):
            if "Job ID" in line or "Job id" in line:
                # Found header, data starts from next line
                data_lines = lines[i + 2 :]  # Skip header and separator
                break

        if not data_lines:
            # Try alternative parsing - sometimes qstat format varies
            data_lines = [
                line
                for line in lines
                if not line.startswith("-") and "Job" not in line and line.strip()
            ]

        for line in data_lines:
            if not line.strip():
                continue

            parts = line.split()
            if len(parts) >= 5:
                job_info = {
                    "job_id": parts[0],
                    "name": parts[1] if len(parts) > 1 else "",
                    "user": parts[2] if len(parts) > 2 else "",
                    "time_use": parts[3] if len(parts) > 3 else "",
                    "state": parts[4] if len(parts) > 4 else "",
                    "queue": parts[5] if len(parts) > 5 else "",
                }
                jobs.append(job_info)

        return jobs

    def create_status_table(self, sweeps: List[Dict], detailed: bool = False) -> Table:
        """Create a rich table showing sweep status."""
        table = Table(title="Recent Sweeps Status")
        table.add_column("Sweep ID", style="cyan", no_wrap=True)
        table.add_column("Age", style="magenta")
        table.add_column("Mode", style="blue")
        table.add_column("Jobs", style="green")
        table.add_column("Status", style="yellow")
        table.add_column("Progress", style="bright_blue")

        if detailed:
            table.add_column("Array Jobs Detail", style="dim")

        for sweep in sweeps:
            # Calculate age
            if sweep["submission_time"]:
                age = datetime.now() - sweep["submission_time"]
                age_str = format_duration(age.total_seconds())
            else:
                age_str = "Unknown"

            # Get job status
            status_info = self.get_pbs_job_status(sweep["job_ids"])

            # Create status summary
            total_jobs = sum(
                [
                    status_info["queued"],
                    status_info["running"],
                    status_info["completed"],
                    status_info["failed"],
                    status_info["unknown"],
                ]
            )

            if total_jobs == 0:
                total_jobs = sweep["total_combinations"]

            status_parts = []
            if status_info["running"] > 0:
                status_parts.append(f"{status_info['running']} running")
            if status_info["queued"] > 0:
                status_parts.append(f"{status_info['queued']} queued")
            if status_info["completed"] > 0:
                status_parts.append(f"{status_info['completed']} done")
            if status_info["failed"] > 0:
                status_parts.append(f"{status_info['failed']} failed")

            status_str = ", ".join(status_parts) if status_parts else "Unknown"

            # Progress bar
            if total_jobs > 0:
                completed_pct = (status_info["completed"] / total_jobs) * 100
                progress_str = (
                    f"{status_info['completed']}/{total_jobs} ({completed_pct:.1f}%)"
                )
            else:
                progress_str = "N/A"

            # Array job detail for detailed view
            array_detail = ""
            if detailed:
                for job_id, job_details in status_info["details"].items():
                    if "[" in job_id and "]" in job_id and job_details:  # Array job
                        # Group subjobs by state
                        state_counts = {}
                        for detail in job_details:
                            state = detail.get("state", "unknown").upper()
                            state_counts[state] = state_counts.get(state, 0) + 1

                        # Format state summary
                        state_parts = []
                        for state, count in sorted(state_counts.items()):
                            color = _get_state_color(state)
                            state_parts.append(f"[{color}]{count} {state}[/{color}]")

                        if state_parts:
                            array_detail = ", ".join(state_parts)
                        break  # Only show first array job detail to keep table readable

            # Add row with or without array detail
            if detailed:
                table.add_row(
                    sweep["sweep_id"],
                    age_str,
                    sweep["mode"] or "Unknown",
                    str(total_jobs),
                    status_str,
                    progress_str,
                    array_detail,
                )
            else:
                table.add_row(
                    sweep["sweep_id"],
                    age_str,
                    sweep["mode"] or "Unknown",
                    str(total_jobs),
                    status_str,
                    progress_str,
                )

        return table


def monitor_sweep(
    sweep_id: Optional[str],
    watch: bool,
    refresh: int,
    detailed: bool,
    console: Console,
    logger: logging.Logger,
):
    """Monitor sweep progress."""
    monitor = SweepMonitor(console, logger)

    if sweep_id:
        # Monitor specific sweep
        console.print(f"[green]Monitoring sweep: {sweep_id}[/green]")

        # Find the sweep
        sweep_dir = Path("sweeps/outputs") / sweep_id
        if not sweep_dir.exists():
            console.print(f"[red]Error: Sweep directory not found: {sweep_dir}[/red]")
            return

        sweep_info = monitor._load_sweep_info(sweep_dir)
        if not sweep_info:
            console.print(f"[red]Error: Could not load sweep information[/red]")
            return

        if watch:
            console.print(
                f"[yellow]Watch mode enabled. Refreshing every {refresh} seconds. Press Ctrl+C to exit.[/yellow]"
            )

            try:
                while True:
                    console.clear()
                    _display_single_sweep_status(sweep_info, monitor, console)
                    time.sleep(refresh)
            except KeyboardInterrupt:
                console.print("\n[yellow]Monitoring stopped.[/yellow]")
        else:
            _display_single_sweep_status(sweep_info, monitor, console)
    else:
        # Monitor all recent sweeps
        console.print("[green]Monitoring all recent sweeps[/green]")

    if watch:
        console.print(
            f"[yellow]Watch mode enabled. Refreshing every {refresh} seconds. Press Ctrl+C to exit.[/yellow]"
        )

        try:
            while True:
                console.clear()
                _display_all_sweeps_status(monitor, console, detailed)
                time.sleep(refresh)
        except KeyboardInterrupt:
            console.print("\n[yellow]Monitoring stopped.[/yellow]")
    else:
        _display_all_sweeps_status(monitor, console, detailed)


def _display_single_sweep_status(
    sweep_info: Dict, monitor: SweepMonitor, console: Console
):
    """Display detailed status for a single sweep."""
    console.print(f"\n[bold blue]Sweep Status: {sweep_info['sweep_id']}[/bold blue]")

    # Basic info panel
    info_text = f"""
[bold]Submission Time:[/bold] {sweep_info["submission_time"].strftime("%Y-%m-%d %H:%M:%S") if sweep_info["submission_time"] else "Unknown"}
[bold]Mode:[/bold] {sweep_info["mode"]}
[bold]Total Combinations:[/bold] {sweep_info["total_combinations"]}
[bold]Walltime:[/bold] {sweep_info["walltime"]}
[bold]Resources:[/bold] {sweep_info["resources"]}
    """.strip()

    if sweep_info["wandb_group"]:
        info_text += f"\n[bold]W&B Group:[/bold] {sweep_info['wandb_group']}"

    console.print(Panel(info_text, title="Sweep Information"))

    # Job status
    status_info = monitor.get_pbs_job_status(sweep_info["job_ids"])

    # Status summary
    total_jobs = sum(
        [
            status_info["queued"],
            status_info["running"],
            status_info["completed"],
            status_info["failed"],
            status_info["unknown"],
        ]
    )

    if total_jobs == 0:
        total_jobs = sweep_info["total_combinations"]

    status_text = f"""
[bold green]Completed:[/bold green] {status_info["completed"]}
[bold yellow]Running:[/bold yellow] {status_info["running"]}
[bold blue]Queued:[/bold blue] {status_info["queued"]}
[bold red]Failed:[/bold red] {status_info["failed"]}
[bold gray]Unknown:[/bold gray] {status_info["unknown"]}
[bold]Total:[/bold] {total_jobs}
    """.strip()

    console.print(Panel(status_text, title="Job Status"))

    # Detailed array job information
    for job_id, job_details in status_info["details"].items():
        if "[" in job_id and "]" in job_id:  # Array job
            console.print(f"\n[bold cyan]Array Job Details: {job_id}[/bold cyan]")

            if job_details:
                # Group by state for array jobs
                state_groups = {}
                for detail in job_details:
                    state = detail.get("state", "unknown")
                    if state not in state_groups:
                        state_groups[state] = []
                    state_groups[state].append(detail)

                # Create table for array job sub-jobs
                array_table = Table(title=f"Sub-jobs for {job_id}")
                array_table.add_column("State", style="yellow")
                array_table.add_column("Count", style="green")
                array_table.add_column("Sample Job IDs", style="cyan")

                for state, jobs in state_groups.items():
                    sample_ids = [job.get("job_id", "") for job in jobs[:3]]
                    sample_str = ", ".join(sample_ids)
                    if len(jobs) > 3:
                        sample_str += f" ... (+{len(jobs) - 3} more)"

                    # Color code the state
                    if state.lower() == "r":
                        state_display = f"[green]{state}[/green] (Running)"
                    elif state.lower() == "q":
                        state_display = f"[yellow]{state}[/yellow] (Queued)"
                    elif state.lower() == "c":
                        state_display = f"[blue]{state}[/blue] (Completed)"
                    elif state.lower() in ["f", "a"]:
                        state_display = f"[red]{state}[/red] (Failed/Aborted)"
                    elif state.lower() == "b":
                        state_display = f"[magenta]{state}[/magenta] (Array Begun)"
                    elif state.lower() == "not_found":
                        state_display = (
                            f"[blue]{state}[/blue] (Finished - not in PBS queue)"
                        )
                    else:
                        state_display = state

                    array_table.add_row(state_display, str(len(jobs)), sample_str)

                console.print(array_table)
            else:
                console.print(
                    "[yellow]No detailed sub-job information available[/yellow]"
                )

    # Progress bar
    if total_jobs > 0:
        progress_pct = (status_info["completed"] / total_jobs) * 100
        console.print(
            f"\n[bold]Progress: {status_info['completed']}/{total_jobs} ({progress_pct:.1f}%)[/bold]"
        )

        # Progress bar visualization
        completed_blocks = int((progress_pct / 100) * 20)
        remaining_blocks = 20 - completed_blocks
        progress_bar = "█" * completed_blocks + "░" * remaining_blocks
        console.print(f"[green]{progress_bar}[/green] {progress_pct:.1f}%")


def _display_all_sweeps_status(
    monitor: SweepMonitor, console: Console, detailed: bool = False
):
    """Display status for all recent sweeps."""
    console.print("\n[bold blue]Recent Sweeps Monitor[/bold blue]")

    sweeps = monitor.discover_recent_sweeps(days=7)

    if not sweeps:
        console.print("[yellow]No recent sweeps found in the last 7 days.[/yellow]")
        return

    table = monitor.create_status_table(sweeps, detailed=detailed)
    console.print(table)

    if detailed:
        console.print(
            f"\n[dim]Found {len(sweeps)} recent sweeps with array job details. Last updated: {datetime.now().strftime('%H:%M:%S')}[/dim]"
        )
    else:
        console.print(
            f"\n[dim]Found {len(sweeps)} recent sweeps. Last updated: {datetime.now().strftime('%H:%M:%S')}[/dim]"
        )
        console.print(
            "[dim]Use --detailed flag to see array job subjob breakdown[/dim]"
        )


def show_status(console: Console, logger: logging.Logger):
    """Show status of all active sweeps."""
    monitor = SweepMonitor(console, logger)
    _display_all_sweeps_status(monitor, console, detailed=False)


def cancel_sweep(sweep_id: str, force: bool, console: Console, logger: logging.Logger):
    """Cancel a running sweep."""
    console.print(f"[red]Cancelling sweep: {sweep_id}[/red]")

    monitor = SweepMonitor(console, logger)

    # Find the sweep
    sweep_dir = Path("sweeps/outputs") / sweep_id
    if not sweep_dir.exists():
        console.print(f"[red]Error: Sweep directory not found: {sweep_dir}[/red]")
        return

    sweep_info = monitor._load_sweep_info(sweep_dir)
    if not sweep_info:
        console.print(f"[red]Error: Could not load sweep information[/red]")
        return

    # Determine if this is a local sweep
    mode = sweep_info.get("mode", "").lower()
    is_local_sweep = mode == "local" or any(
        job_id.startswith("local_") for job_id in sweep_info["job_ids"]
    )

    if not force:
        response = console.input(
            f"Are you sure you want to cancel sweep {sweep_id}? (y/N): "
        )
        if response.lower() != "y":
            console.print("[yellow]Cancellation aborted.[/yellow]")
            return

    if is_local_sweep:
        console.print(
            "[blue]Detected local sweep, attempting to cancel local jobs...[/blue]"
        )
        _cancel_local_sweep(sweep_info, sweep_dir, console, logger)
    else:
        console.print(
            "[blue]Detected HPC sweep, attempting to cancel HPC jobs...[/blue]"
        )
        _cancel_hpc_sweep(sweep_info, console, logger)


def _cancel_local_sweep(
    sweep_info: dict, sweep_dir: Path, console: Console, logger: logging.Logger
):
    """Cancel a local sweep by looking for running processes."""
    from ..core.job_manager import LocalJobManager

    # Try to find and cancel running local processes
    # First, check if we can find a running LocalJobManager instance
    # Since we don't have access to the original instance, we'll try to find processes manually

    # Look for process info in task directories
    tasks_dir = sweep_dir / "tasks"
    cancelled_count = 0
    failed_count = 0
    not_found_count = 0

    if tasks_dir.exists():
        console.print(f"[blue]Looking for running tasks in {tasks_dir}...[/blue]")

        for task_dir in tasks_dir.iterdir():
            if not task_dir.is_dir():
                continue

            task_info_file = task_dir / "task_info.txt"
            if not task_info_file.exists():
                continue

            try:
                # Read task info to get process information
                with open(task_info_file, "r") as f:
                    content = f.read()

                # Check if task is still running
                if "Status: RUNNING" in content or "Status: CANCELLED" not in content:
                    # Try to find and kill the process
                    # Look for Python processes running the train script
                    import psutil
                    import re

                    # Extract job ID from task info
                    job_id_match = re.search(r"Job ID: (local_\w+_\d+)", content)
                    if job_id_match:
                        job_id = job_id_match.group(1)

                        # Try to find the process
                        killed = False
                        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
                            try:
                                if (
                                    proc.info["name"]
                                    and "python" in proc.info["name"].lower()
                                ):
                                    cmdline = " ".join(proc.info["cmdline"] or [])
                                    # Look for processes that might be this job
                                    if (
                                        "train.py" in cmdline or "train" in cmdline
                                    ) and job_id in cmdline:
                                        console.print(
                                            f"  Found and killing process {proc.pid} for job {job_id}"
                                        )
                                        try:
                                            # Kill the entire process group
                                            parent = psutil.Process(proc.pid)
                                            children = parent.children(recursive=True)
                                            for child in children:
                                                child.kill()
                                            parent.kill()
                                            killed = True
                                            break
                                        except (
                                            psutil.NoSuchProcess,
                                            psutil.AccessDenied,
                                        ):
                                            pass
                            except (
                                psutil.NoSuchProcess,
                                psutil.AccessDenied,
                                psutil.ZombieProcess,
                            ):
                                continue

                        if killed:
                            cancelled_count += 1
                            # Update task status
                            with open(task_info_file, "a") as f:
                                f.write(f"Status: CANCELLED\n")
                                f.write(f"End Time: {datetime.now()}\n")
                        else:
                            # Process might have finished on its own
                            not_found_count += 1
                    else:
                        not_found_count += 1

            except Exception as e:
                logger.warning(f"Error processing task {task_dir.name}: {e}")
                failed_count += 1

    # Report results
    total_tasks = cancelled_count + failed_count + not_found_count
    if total_tasks > 0:
        console.print(f"[green]Local sweep cancellation summary:[/green]")
        console.print(f"  - Cancelled: {cancelled_count}")
        console.print(f"  - Not found/already finished: {not_found_count}")
        if failed_count > 0:
            console.print(f"  - Failed: {failed_count}")
    else:
        console.print("[yellow]No running local tasks found to cancel[/yellow]")

    # Also try to kill by job name pattern (fallback)
    console.print("[blue]Searching for remaining processes by pattern...[/blue]")
    additional_killed = _kill_processes_by_pattern(sweep_info["sweep_id"], console)
    if additional_killed > 0:
        console.print(f"[green]Killed {additional_killed} additional processes[/green]")

    logger.info(
        f"Local sweep {sweep_info['sweep_id']} cancellation completed. "
        f"Cancelled: {cancelled_count}, Not found: {not_found_count}, Failed: {failed_count}"
    )


def _kill_processes_by_pattern(sweep_id: str, console: Console) -> int:
    """Kill processes that match the sweep pattern."""
    try:
        import psutil

        killed_count = 0

        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                if proc.info["name"] and "python" in proc.info["name"].lower():
                    cmdline = " ".join(proc.info["cmdline"] or [])
                    # Look for processes that contain the sweep ID
                    if sweep_id in cmdline and (
                        "train" in cmdline or "wandb.group" in cmdline
                    ):
                        console.print(
                            f"  Killing process {proc.pid}: {proc.info['name']}"
                        )
                        try:
                            parent = psutil.Process(proc.pid)
                            children = parent.children(recursive=True)
                            for child in children:
                                child.kill()
                            parent.kill()
                            killed_count += 1
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue

        return killed_count
    except ImportError:
        console.print(
            "[yellow]psutil not available, cannot search for additional processes[/yellow]"
        )
        return 0


def _cancel_hpc_sweep(sweep_info: dict, console: Console, logger: logging.Logger):
    """Cancel HPC jobs using PBS/Slurm commands."""
    # Cancel PBS/Slurm jobs
    cancelled_jobs = []
    failed_jobs = []

    for job_id in sweep_info["job_ids"]:
        try:
            # Try PBS first, then Slurm
            result = subprocess.run(
                ["qdel", job_id], capture_output=True, text=True, timeout=10
            )

            if result.returncode == 0:
                cancelled_jobs.append(job_id)
            else:
                # Try Slurm
                result = subprocess.run(
                    ["scancel", job_id], capture_output=True, text=True, timeout=10
                )
                if result.returncode == 0:
                    cancelled_jobs.append(job_id)
                else:
                    failed_jobs.append((job_id, result.stderr))

        except subprocess.TimeoutExpired:
            failed_jobs.append((job_id, "Timeout"))
        except Exception as e:
            failed_jobs.append((job_id, str(e)))

    # Report results
    if cancelled_jobs:
        console.print(
            f"[green]Successfully cancelled {len(cancelled_jobs)} job(s):[/green]"
        )
        for job_id in cancelled_jobs:
            console.print(f"  - {job_id}")

    if failed_jobs:
        console.print(f"[red]Failed to cancel {len(failed_jobs)} job(s):[/red]")
        for job_id, error in failed_jobs:
            console.print(f"  - {job_id}: {error}")

    logger.info(
        f"HPC sweep {sweep_info['sweep_id']} cancellation completed. "
        f"Cancelled: {len(cancelled_jobs)}, Failed: {len(failed_jobs)}"
    )


def collect_results(
    sweep_id: str,
    output_path: Optional[Path],
    format: str,
    console: Console,
    logger: logging.Logger,
):
    """Collect and analyze sweep results."""
    console.print(f"[green]Collecting results for sweep: {sweep_id}[/green]")
    console.print(f"Output format: {format}")

    if output_path:
        console.print(f"Output directory: {output_path}")
    else:
        console.print("Output directory: current directory (default)")

    # TODO: Implement result collection logic
    logger.info(f"Result collection requested for sweep {sweep_id}")
    console.print("[yellow]Note: Result collection not yet implemented[/yellow]")


def show_recent_sweeps(
    days: int, watch: bool, refresh: int, console: Console, logger: logging.Logger
):
    """Show recent sweeps from the last N days."""
    monitor = SweepMonitor(console, logger)

    if watch:
        console.print(
            f"[yellow]Watch mode enabled. Showing sweeps from last {days} days. Refreshing every {refresh} seconds. Press Ctrl+C to exit.[/yellow]"
        )

        try:
            while True:
                console.clear()
                console.print(
                    f"\n[bold blue]Recent Sweeps (Last {days} days)[/bold blue]"
                )

                sweeps = monitor.discover_recent_sweeps(days=days)

                if not sweeps:
                    console.print(
                        f"[yellow]No sweeps found in the last {days} days.[/yellow]"
                    )
                else:
                    table = monitor.create_status_table(sweeps, detailed=False)
                    console.print(table)

                console.print(
                    f"\n[dim]Found {len(sweeps)} sweeps. Last updated: {datetime.now().strftime('%H:%M:%S')}[/dim]"
                )
                time.sleep(refresh)
        except KeyboardInterrupt:
            console.print("\n[yellow]Monitoring stopped.[/yellow]")
    else:
        console.print(f"\n[bold blue]Recent Sweeps (Last {days} days)[/bold blue]")

        sweeps = monitor.discover_recent_sweeps(days=days)

        if not sweeps:
            console.print(f"[yellow]No sweeps found in the last {days} days.[/yellow]")
        else:
            table = monitor.create_status_table(sweeps, detailed=False)
            console.print(table)
            console.print(f"\n[dim]Found {len(sweeps)} sweeps.[/dim]")


def show_queue_status(
    watch: bool, refresh: int, console: Console, logger: logging.Logger
):
    """Show current queue status for all user jobs."""

    if watch:
        console.print(
            f"[yellow]Watch mode enabled. Refreshing every {refresh} seconds. Press Ctrl+C to exit.[/yellow]"
        )

        try:
            while True:
                console.clear()
                _display_queue_status(console, logger)
                time.sleep(refresh)
        except KeyboardInterrupt:
            console.print("\n[yellow]Queue monitoring stopped.[/yellow]")
    else:
        _display_queue_status(console, logger)


def _display_queue_status(console: Console, logger: logging.Logger):
    """Display current queue status."""
    console.print("\n[bold blue]Current Queue Status[/bold blue]")

    try:
        username = os.getenv("USER")

        # Get queue status using qstat
        result = subprocess.run(
            ["qstat", "-u", username], capture_output=True, text=True, timeout=10
        )

        if result.returncode != 0:
            console.print(f"[red]Error running qstat: {result.stderr}[/red]")
            return

        lines = result.stdout.strip().split("\n")

        if len(lines) <= 2:  # Only header lines
            console.print("[green]No jobs currently in queue.[/green]")
            return

        # Parse job information
        jobs = []
        data_lines = lines[2:]  # Skip header

        for line in data_lines:
            if not line.strip():
                continue

            parts = line.split()
            if len(parts) >= 5:
                job_info = {
                    "job_id": parts[0],
                    "name": parts[1],
                    "user": parts[2],
                    "time_use": parts[3],
                    "state": parts[4],
                    "queue": parts[5] if len(parts) > 5 else "",
                }
                jobs.append(job_info)

        # Create summary table
        table = Table(title=f"Queue Status for {username}")
        table.add_column("Job ID", style="cyan", no_wrap=True)
        table.add_column("Name", style="blue")
        table.add_column("State", style="yellow")
        table.add_column("Time Used", style="green")
        table.add_column("Queue", style="magenta")

        # Count states
        state_counts = {}
        for job in jobs:
            state = job["state"]
            state_counts[state] = state_counts.get(state, 0) + 1

            # Color code the state
            if state.lower() == "r":
                state_display = f"[green]{state}[/green]"
            elif state.lower() == "q":
                state_display = f"[yellow]{state}[/yellow]"
            elif state.lower() in ["c"]:
                state_display = f"[blue]{state}[/blue]"
            elif state.lower() in ["f", "a"]:
                state_display = f"[red]{state}[/red]"
            else:
                state_display = state

            table.add_row(
                job["job_id"],
                job["name"][:20] + "..." if len(job["name"]) > 20 else job["name"],
                state_display,
                job["time_use"],
                job["queue"],
            )

        console.print(table)

        # Summary
        summary_parts = []
        if "R" in state_counts:
            summary_parts.append(f"[green]{state_counts['R']} running[/green]")
        if "Q" in state_counts:
            summary_parts.append(f"[yellow]{state_counts['Q']} queued[/yellow]")
        if "C" in state_counts:
            summary_parts.append(f"[blue]{state_counts['C']} completed[/blue]")
        for state, count in state_counts.items():
            if state not in ["R", "Q", "C"]:
                summary_parts.append(f"{count} {state}")

        if summary_parts:
            console.print(f"\n[bold]Summary:[/bold] {', '.join(summary_parts)}")

        console.print(
            f"\n[dim]Total jobs: {len(jobs)}. Last updated: {datetime.now().strftime('%H:%M:%S')}[/dim]"
        )

    except subprocess.TimeoutExpired:
        console.print("[red]Timeout while querying queue status[/red]")
    except FileNotFoundError:
        console.print(
            "[red]qstat command not found. Are you on an HPC system with PBS?[/red]"
        )
    except Exception as e:
        console.print(f"[red]Error getting queue status: {e}[/red]")
        logger.error(f"Queue status error: {e}")


def delete_sweep_jobs(
    sweep_id: str,
    pattern: Optional[str],
    state: Optional[str],
    dry_run: bool,
    force: bool,
    all_states: bool,
    console: Console,
    logger: logging.Logger,
):
    """Delete specific jobs from a sweep with filtering options."""
    console.print(f"[blue]Deleting jobs from sweep: {sweep_id}[/blue]")

    monitor = SweepMonitor(console, logger)

    # Find the sweep
    sweep_dir = Path("sweeps/outputs") / sweep_id
    if not sweep_dir.exists():
        console.print(f"[red]Error: Sweep directory not found: {sweep_dir}[/red]")
        return

    sweep_info = monitor._load_sweep_info(sweep_dir)
    if not sweep_info:
        console.print(f"[red]Error: Could not load sweep information[/red]")
        return

    # Get detailed job status
    status_info = monitor.get_pbs_job_status(sweep_info["job_ids"])

    # Collect all jobs to potentially delete
    jobs_to_delete = []

    for job_id, job_details in status_info["details"].items():
        for job_detail in job_details:
            job_state = job_detail.get("state", "unknown").upper()
            job_name = job_detail.get("name", "")
            job_full_id = job_detail.get("job_id", job_id)

            # Apply filtering
            should_include = True

            # State filtering
            if not all_states:
                # By default, exclude completed and failed jobs unless explicitly requested
                if job_state.lower() in ["c", "f"] and not state:
                    should_include = False

            if state and job_state.upper() != state.upper():
                should_include = False

            # Pattern filtering
            if pattern and should_include:
                import fnmatch

                if not fnmatch.fnmatch(job_name, pattern) and not fnmatch.fnmatch(
                    job_full_id, pattern
                ):
                    should_include = False

            if should_include:
                # Determine if this is an array job and extract details
                is_array_job = "[" in job_full_id and "]" in job_full_id
                array_index = None
                parent_job_id = job_id

                if is_array_job:
                    # Extract parent job ID and array index
                    if "[" in job_full_id:
                        parent_job_id = job_full_id.split("[")[0]
                        array_part = job_full_id.split("[")[1].split("]")[0]
                        if array_part.strip():  # Only if there's an actual index
                            array_index = array_part

                jobs_to_delete.append(
                    {
                        "job_id": job_full_id,
                        "name": job_name,
                        "state": job_state,
                        "base_job_id": job_id,
                        "parent_job_id": parent_job_id,
                        "array_index": array_index,
                        "is_array_job": is_array_job,
                    }
                )

    if not jobs_to_delete:
        console.print("[yellow]No jobs found matching the specified criteria.[/yellow]")
        return

    # Display what will be deleted with array job info
    table = Table(title=f"Jobs to Delete from {sweep_id}")
    table.add_column("Job ID", style="cyan")
    table.add_column("Type", style="magenta")
    table.add_column("Name", style="blue")
    table.add_column("State", style="yellow")

    for job in jobs_to_delete:
        state_color = _get_state_color(job["state"])
        job_type = (
            "Array SubJob" if job["is_array_job"] and job["array_index"] else "Regular"
        )
        if job["is_array_job"] and not job["array_index"]:
            job_type = "Array Parent"

        table.add_row(
            job["job_id"],
            job_type,
            job["name"],
            f"[{state_color}]{job['state']}[/{state_color}]",
        )

    console.print(table)
    console.print(f"\n[bold]Total jobs to delete: {len(jobs_to_delete)}[/bold]")

    if dry_run:
        console.print("\n[yellow]DRY RUN: No jobs were actually deleted.[/yellow]")
        return

    # Confirmation
    if not force:
        response = console.input(
            f"\nAre you sure you want to delete {len(jobs_to_delete)} job(s)? (y/N): "
        )
        if response.lower() != "y":
            console.print("[yellow]Deletion aborted.[/yellow]")
            return

    # Delete jobs
    console.print(f"\n[red]Deleting {len(jobs_to_delete)} jobs...[/red]")

    deleted_jobs = []
    failed_jobs = []

    # Group jobs by deletion strategy
    array_parents_to_delete = set()  # Parent job IDs for entire array deletion
    individual_subjobs_to_delete = []  # Individual subjobs to delete
    regular_jobs_to_delete = []  # Regular non-array jobs

    for job in jobs_to_delete:
        if job["is_array_job"]:
            if job["array_index"]:
                # This is a specific array subjob
                individual_subjobs_to_delete.append(job)
            else:
                # This is an array parent - mark for entire array deletion
                array_parents_to_delete.add(job["parent_job_id"])
        else:
            # Regular job
            regular_jobs_to_delete.append(job)

    # 1. Delete entire arrays (parent jobs)
    for parent_job_id in array_parents_to_delete:
        try:
            result = subprocess.run(
                ["qdel", parent_job_id], capture_output=True, text=True, timeout=10
            )

            if result.returncode == 0:
                # Mark all related jobs as deleted
                related_jobs = [
                    j for j in jobs_to_delete if j["parent_job_id"] == parent_job_id
                ]
                deleted_jobs.extend(related_jobs)
                console.print(
                    f"[green]Deleted entire array job: {parent_job_id}[/green]"
                )
            else:
                # Mark all related jobs as failed
                related_jobs = [
                    j for j in jobs_to_delete if j["parent_job_id"] == parent_job_id
                ]
                for job in related_jobs:
                    failed_jobs.append(
                        (job["job_id"], f"Array deletion failed: {result.stderr}")
                    )
                console.print(
                    f"[red]Failed to delete array job {parent_job_id}: {result.stderr}[/red]"
                )

        except subprocess.TimeoutExpired:
            related_jobs = [
                j for j in jobs_to_delete if j["parent_job_id"] == parent_job_id
            ]
            for job in related_jobs:
                failed_jobs.append((job["job_id"], "Timeout"))
        except Exception as e:
            related_jobs = [
                j for j in jobs_to_delete if j["parent_job_id"] == parent_job_id
            ]
            for job in related_jobs:
                failed_jobs.append((job["job_id"], str(e)))

    # 2. Delete individual array subjobs (only if their parent wasn't already deleted)
    for job in individual_subjobs_to_delete:
        if job["parent_job_id"] not in array_parents_to_delete:
            try:
                # Delete specific array subjob using the full job ID (e.g., "12345[3]")
                result = subprocess.run(
                    ["qdel", job["job_id"]], capture_output=True, text=True, timeout=10
                )

                if result.returncode == 0:
                    deleted_jobs.append(job)
                    console.print(
                        f"[green]Deleted array subjob: {job['job_id']}[/green]"
                    )
                else:
                    failed_jobs.append((job["job_id"], result.stderr))
                    console.print(
                        f"[red]Failed to delete subjob {job['job_id']}: {result.stderr}[/red]"
                    )

            except subprocess.TimeoutExpired:
                failed_jobs.append((job["job_id"], "Timeout"))
            except Exception as e:
                failed_jobs.append((job["job_id"], str(e)))

    # 3. Delete regular jobs
    for job in regular_jobs_to_delete:
        try:
            result = subprocess.run(
                ["qdel", job["job_id"]], capture_output=True, text=True, timeout=10
            )

            if result.returncode == 0:
                deleted_jobs.append(job)
            else:
                failed_jobs.append((job["job_id"], result.stderr))

        except subprocess.TimeoutExpired:
            failed_jobs.append((job["job_id"], "Timeout"))
        except Exception as e:
            failed_jobs.append((job["job_id"], str(e)))

    # Report results
    if deleted_jobs:
        console.print(
            f"\n[green]Successfully deleted {len(deleted_jobs)} job(s):[/green]"
        )

        # Group results for better reporting
        array_deletions = len(array_parents_to_delete)
        subjob_deletions = len(
            [
                j
                for j in deleted_jobs
                if j["is_array_job"]
                and j["array_index"]
                and j["parent_job_id"] not in array_parents_to_delete
            ]
        )
        regular_deletions = len([j for j in deleted_jobs if not j["is_array_job"]])

        if array_deletions > 0:
            console.print(f"  - {array_deletions} entire array job(s)")
        if subjob_deletions > 0:
            console.print(f"  - {subjob_deletions} individual array subjob(s)")
        if regular_deletions > 0:
            console.print(f"  - {regular_deletions} regular job(s)")

        # Show first few individual job IDs
        for job in deleted_jobs[:5]:
            job_type = "array" if job["is_array_job"] else "regular"
            console.print(f"  - {job['job_id']} ({job_type}: {job['name']})")
        if len(deleted_jobs) > 5:
            console.print(f"  ... and {len(deleted_jobs) - 5} more")

    if failed_jobs:
        console.print(f"\n[red]Failed to delete {len(failed_jobs)} job(s):[/red]")
        for job_id, error in failed_jobs[:3]:  # Show first 3 errors
            console.print(f"  - {job_id}: {error}")
        if len(failed_jobs) > 3:
            console.print(f"  ... and {len(failed_jobs) - 3} more errors")

    logger.info(
        f"Job deletion completed for sweep {sweep_id}. Deleted: {len(deleted_jobs)}, Failed: {len(failed_jobs)}. "
        f"Array parents: {len(array_parents_to_delete)}, Individual subjobs: {len(individual_subjobs_to_delete)}, Regular: {len(regular_jobs_to_delete)}"
    )


def cleanup_old_sweeps(
    days: int,
    states: Optional[List[str]],
    dry_run: bool,
    force: bool,
    console: Console,
    logger: logging.Logger,
):
    """Clean up old sweep jobs based on age and state."""
    console.print(f"[blue]Cleaning up sweeps older than {days} days[/blue]")

    monitor = SweepMonitor(console, logger)

    # Find old sweeps
    all_sweeps = monitor.discover_recent_sweeps(
        days=days * 2
    )  # Get more sweeps to filter
    cutoff_date = datetime.now() - timedelta(days=days)

    old_sweeps = [
        sweep
        for sweep in all_sweeps
        if sweep["submission_time"] and sweep["submission_time"] < cutoff_date
    ]

    if not old_sweeps:
        console.print(f"[green]No sweeps found older than {days} days.[/green]")
        return

    console.print(f"[yellow]Found {len(old_sweeps)} old sweeps[/yellow]")

    total_jobs_to_delete = 0
    sweep_job_info = []

    for sweep in old_sweeps:
        status_info = monitor.get_pbs_job_status(sweep["job_ids"])
        jobs_in_scope = []

        for job_id, job_details in status_info["details"].items():
            for job_detail in job_details:
                job_state = job_detail.get("state", "unknown").upper()

                # Filter by state if specified
                if states and job_state not in [s.upper() for s in states]:
                    continue

                jobs_in_scope.append(
                    {
                        "job_id": job_detail.get("job_id", job_id),
                        "state": job_state,
                        "base_job_id": job_id,
                    }
                )

        if jobs_in_scope:
            sweep_job_info.append({"sweep": sweep, "jobs": jobs_in_scope})
            total_jobs_to_delete += len(jobs_in_scope)

    if total_jobs_to_delete == 0:
        console.print("[green]No jobs found matching cleanup criteria.[/green]")
        return

    # Display summary
    table = Table(title="Sweeps to Clean Up")
    table.add_column("Sweep ID", style="cyan")
    table.add_column("Age", style="magenta")
    table.add_column("Jobs to Delete", style="red")

    for item in sweep_job_info:
        sweep = item["sweep"]
        jobs = item["jobs"]
        age = datetime.now() - sweep["submission_time"]
        age_str = f"{age.days} days"

        # State summary
        state_counts = {}
        for job in jobs:
            state = job["state"]
            state_counts[state] = state_counts.get(state, 0) + 1

        state_summary = ", ".join(
            [f"{count} {state}" for state, count in state_counts.items()]
        )

        table.add_row(sweep["sweep_id"], age_str, f"{len(jobs)} ({state_summary})")

    console.print(table)
    console.print(
        f"\n[bold red]Total jobs to delete: {total_jobs_to_delete}[/bold red]"
    )

    if dry_run:
        console.print("\n[yellow]DRY RUN: No jobs were actually deleted.[/yellow]")
        return

    # Confirmation
    if not force:
        response = console.input(
            f"\nAre you sure you want to delete {total_jobs_to_delete} job(s) from {len(sweep_job_info)} sweep(s)? (y/N): "
        )
        if response.lower() != "y":
            console.print("[yellow]Cleanup aborted.[/yellow]")
            return

    # Delete jobs from each sweep
    total_deleted = 0
    total_failed = 0

    for item in sweep_job_info:
        sweep = item["sweep"]
        jobs = item["jobs"]

        console.print(f"\n[blue]Cleaning up sweep: {sweep['sweep_id']}[/blue]")

        # Group by base job ID
        job_groups = {}
        for job in jobs:
            base_id = job["base_job_id"]
            if base_id not in job_groups:
                job_groups[base_id] = []
            job_groups[base_id].append(job)

        for base_job_id, job_group in job_groups.items():
            try:
                if "[" in base_job_id and "]" in base_job_id:
                    clean_job_id = base_job_id.split("[")[0]
                    result = subprocess.run(
                        ["qdel", clean_job_id],
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                else:
                    result = subprocess.run(
                        ["qdel", base_job_id],
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )

                if result.returncode == 0:
                    total_deleted += len(job_group)
                else:
                    total_failed += len(job_group)

            except Exception:
                total_failed += len(job_group)

    console.print(f"\n[green]Cleanup completed![/green]")
    console.print(f"[green]Successfully deleted: {total_deleted} jobs[/green]")
    if total_failed > 0:
        console.print(f"[red]Failed to delete: {total_failed} jobs[/red]")

    logger.info(f"Cleanup completed. Deleted: {total_deleted}, Failed: {total_failed}")


def _get_state_color(state: str) -> str:
    """Get color for job state display."""
    state_lower = state.lower()
    if state_lower == "r":
        return "green"
    elif state_lower == "q":
        return "yellow"
    elif state_lower == "c":
        return "blue"
    elif state_lower in ["f", "a"]:
        return "red"
    elif state_lower == "h":
        return "magenta"
    elif state_lower == "not_found":
        return "blue"  # Same as completed since they're finished
    else:
        return "white"
