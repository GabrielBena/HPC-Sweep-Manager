"""Sweep execution CLI commands."""

from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import click
from rich.console import Console

if TYPE_CHECKING:
    from ..core.common.config import HSMConfig, SweepConfig

import asyncio

from ..core.common.config import HSMConfig
from ..core.common.sweep_orchestrator import (
    SUPPORTED_MODES as _ORCHESTRATOR_MODES,
)
from ..core.common.sweep_orchestrator import (
    build_compute_source,
    resolve_auto_mode,
    run_sweep_async,
    spec_from_cli,
)
from ..core.common.templating import params_to_hydra_args
from .common import common_options

logger = logging.getLogger(__name__)


def _load_and_validate_config(
    config_path: Path, console: Console, logger: logging.Logger
) -> Optional["SweepConfig"]:
    """Load and validate sweep configuration."""
    from ..core.common.config import SweepConfig

    try:
        config = SweepConfig.from_yaml(config_path)
        logger.info(f"Loaded sweep config from {config_path}")

        errors = config.validate()
        if errors:
            console.print("[red]Configuration validation failed:[/red]")
            for error in errors:
                console.print(f"  - {error}")
            return None

        return config
    except FileNotFoundError:
        console.print(f"[red]Error: Sweep config file not found: {config_path}[/red]")
        return None
    except Exception as e:
        console.print(f"[red]Error loading configuration: {e}[/red]")
        logger.error(f"Configuration loading failed: {e}")
        return None


def _generate_parameter_combinations(
    config: "SweepConfig", max_runs: Optional[int], count_only: bool, console: Console
) -> Optional[list]:
    """Generate and display parameter combinations."""
    from rich.table import Table

    from ..core.common.param_generator import ParameterGenerator

    generator = ParameterGenerator(config)

    if count_only:
        total_combinations = generator.count_combinations()
        console.print(f"[green]Total parameter combinations: {total_combinations}[/green]")
        return None

    combinations = generator.generate_combinations(max_runs)
    info = generator.get_parameter_info()

    table = Table(title="Sweep Information")
    table.add_column("Parameter", style="cyan")
    table.add_column("Type", style="magenta")
    table.add_column("Values", style="green")

    for param_name, param_info in info["grid_parameters"].items():
        table.add_row(param_name, "Grid", str(param_info["values"])[:50] + "...")

    for param_name, param_info in info["paired_parameters"].items():
        table.add_row(param_name, "Paired", str(param_info["values"])[:50] + "...")

    console.print(table)
    console.print(f"\n[bold]Total combinations to run: {len(combinations)}[/bold]")

    return combinations


def _detect_project_paths(
    hsm_config: Optional["HSMConfig"], sweep_config: Optional["SweepConfig"] = None
) -> tuple[str, str, str]:
    """Detect or get project paths for execution.

    Returns ``(python_path, script_path, project_dir)``. Priority for the
    script path: sweep_config.script > hsm_config.train_script > auto-detect.
    """
    from ..core.common.path_detector import PathDetector

    detector = PathDetector()

    if hsm_config:
        python_path = hsm_config.get_default_python_path() or detector.detect_python_path()
        project_dir = hsm_config.get_project_root() or str(Path.cwd())
    else:
        python_path = detector.detect_python_path()
        project_dir = str(Path.cwd())

    if sweep_config and sweep_config.script:
        script_path = sweep_config.script
        script_path_obj = Path(script_path)
        if not script_path_obj.is_absolute():
            script_path = str(Path(project_dir) / script_path)
    elif hsm_config:
        script_path = hsm_config.get_default_script_path() or detector.detect_train_script()
    else:
        script_path = detector.detect_train_script()

    return python_path, script_path, project_dir


def _run_sweep_via_orchestrator(
    *,
    config_path: Path,
    hsm_config: Optional["HSMConfig"],
    mode: str,
    python_path: str,
    script_path: str,
    project_dir: str,
    combinations: list,
    dry_run: bool,
    walltime: str,
    resources: str,
    group: Optional[str],
    parallel_jobs: Optional[int],
    no_progress: bool,
    console: Console,
    logger: logging.Logger,
    remote_alias: Optional[str] = None,
    gpus_arg: Optional[str] = None,
) -> None:
    """Route a sweep through the unified ComputeSource orchestrator.

    Handles ``--mode local|auto|array|individual|distributed|remote``.
    """
    import shutil

    from ..core.common.utils import create_sweep_id
    from ..core.remote.ssh_compute_source import parse_gpus_arg

    # Resolve auto BEFORE asking spec_from_cli which config block to read —
    # otherwise mode='auto' would silently read the slurm: block on every machine.
    resolved_mode_for_spec = resolve_auto_mode(mode)
    scheduler_hint = "slurm" if resolved_mode_for_spec in ("array", "individual") else None
    spec = spec_from_cli(
        walltime=walltime,
        resources=resources,
        scheduler=scheduler_hint,
        hsm_config=hsm_config,
        mode=resolved_mode_for_spec,
    )
    try:
        gpus_override = parse_gpus_arg(gpus_arg) if gpus_arg is not None else None
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        return
    try:
        source, resolved_mode, sub_mode = build_compute_source(
            mode=mode,
            python_path=python_path,
            script_path=script_path,
            project_dir=project_dir,
            default_spec=spec,
            hsm_config=hsm_config,
            parallel_jobs=parallel_jobs,
            remote_alias=remote_alias,
            gpus_override=gpus_override,
        )
    except (ValueError, RuntimeError) as e:
        console.print(f"[red]Error building compute source: {e}[/red]")
        return

    console.print(
        f"[green]Execution backend: {source.source_type} "
        f"(mode={resolved_mode}, submission={sub_mode})[/green]"
    )
    if resolved_mode != mode:
        console.print(f"[cyan](mode auto-resolved from {mode!r} → {resolved_mode!r})[/cyan]")

    if dry_run:
        console.print("\n[yellow]DRY RUN - No jobs will be submitted[/yellow]")
        console.print("\n[bold]Effective ResourceSpec:[/bold]")
        for k, v in spec.to_dict().items():
            if v in (None, [], {}, ()):
                continue
            console.print(f"  {k:18s} = {v!r}")
        console.print(f"\n[bold]Python:[/bold] {python_path}")
        console.print(f"[bold]Script:[/bold] {script_path}")
        console.print(f"[bold]Project dir:[/bold] {project_dir}")
        if group:
            console.print(f"[bold]W&B group:[/bold] {group}")
        console.print("\n[bold]First 3 parameter combinations:[/bold]")
        for i, combo in enumerate(combinations[:3], 1):
            console.print(f"  {i}. {combo}")
            console.print(f"     args: {params_to_hydra_args(combo)}")
        console.print(f"\nTotal combinations: {len(combinations)}")
        return

    sweep_id = create_sweep_id()
    sweep_dir = Path("sweeps") / "outputs" / sweep_id
    sweep_dir.mkdir(parents=True, exist_ok=True)
    console.print(f"\n[green]Sweep ID: {sweep_id}[/green]")
    console.print(f"Sweep directory: {sweep_dir}")

    shutil.copy2(config_path, sweep_dir / "sweep_config.yaml")

    console.print(
        f"\n[bold]Submitting {len(combinations)} job(s) via {source.source_type}...[/bold]"
    )

    progress_cb = None
    if not no_progress:
        def _progress(done: int, total: int) -> None:
            console.print(f"  {done}/{total} done", end="\r")
        progress_cb = _progress

    try:
        result = asyncio.run(
            run_sweep_async(
                source=source,
                sweep_dir=sweep_dir,
                sweep_id=sweep_id,
                params_list=combinations,
                submission_mode=sub_mode,
                spec=spec,
                wandb_group=group,
                job_name_prefix=sweep_id,
                wait=True,
                poll_interval=10.0,
                on_progress=progress_cb,
            )
        )
    except Exception as e:
        console.print(f"[red]Sweep execution failed: {e}[/red]")
        logger.exception("orchestrator run failed")
        raise

    console.print(
        f"\n[green]Submitted {len(result.job_ids)} job(s) "
        f"in {sub_mode} mode: {', '.join(result.job_ids)}[/green]"
    )

    if result.final_statuses:
        completed = sum(1 for s in result.final_statuses.values() if s == "COMPLETED")
        failed = sum(1 for s in result.final_statuses.values() if s == "FAILED")
        cancelled = sum(1 for s in result.final_statuses.values() if s == "CANCELLED")
        console.print(
            f"[bold]Final: {completed} COMPLETED, "
            f"{failed} FAILED, {cancelled} CANCELLED[/bold]"
        )

    summary_file = sweep_dir / "submission_summary.txt"
    with open(summary_file, "w") as f:
        f.write("Sweep Submission Summary\n")
        f.write("========================\n")
        f.write(f"Sweep ID: {sweep_id}\n")
        f.write(f"Submission Time: {datetime.now()}\n")
        f.write(f"Mode: {resolved_mode} (submission={sub_mode})\n")
        f.write(f"Backend: {source.source_type}\n")
        f.write(f"Total Combinations: {len(combinations)}\n")
        f.write(f"Job IDs: {', '.join(result.job_ids)}\n")
        if walltime:
            f.write(f"Walltime: {walltime}\n")
        if resources:
            f.write(f"Resources: {resources}\n")
        if group:
            f.write(f"W&B Group: {group}\n")
        if result.final_statuses:
            f.write("\nFinal Statuses:\n")
            for jid, status in result.final_statuses.items():
                f.write(f"  {jid}: {status}\n")
    console.print(f"Summary saved to: {summary_file}")

    logger.info(
        f"Sweep {sweep_id} via orchestrator ({source.source_type}) "
        f"submitted {len(result.job_ids)} job(s) with {len(combinations)} combinations"
    )


def run_sweep(
    config_path: Path,
    mode: str,
    dry_run: bool,
    count_only: bool,
    max_runs: Optional[int],
    walltime: str,
    resources: str,
    group: Optional[str],
    parallel_jobs: Optional[int],
    no_progress: bool,
    console: Console,
    logger: logging.Logger,
    hsm_config: Optional["HSMConfig"] = None,
    remote_alias: Optional[str] = None,
    gpus_arg: Optional[str] = None,
):
    """Run parameter sweep (orchestrator-only path)."""

    console.print("[bold blue]HPC Sweep Manager[/bold blue]")
    console.print(f"Config: {config_path}")
    console.print(f"Mode: {mode}")

    if hsm_config:
        hsm_config_path = None
        for path in (
            Path.cwd() / ".hsm" / "config.yaml",
            Path.cwd() / "sweeps" / "hsm_config.yaml",
            Path.cwd() / "hsm_config.yaml",
        ):
            if path.exists():
                hsm_config_path = path
                break
        if hsm_config_path:
            console.print(f"[green]Using HSM config: {hsm_config_path}[/green]")
    else:
        console.print("[yellow]No hsm_config.yaml found - using default values[/yellow]")

    try:
        config = _load_and_validate_config(config_path, console, logger)
        if config is None:
            return

        if config.complete is not None:
            console.print(
                "[red]`config.complete: <sweep_id>` (sweep-resume mode) is not supported in this build.[/red]"
            )
            console.print(
                "[yellow]The legacy completion runner was deleted in Pass B-heavy "
                "(see CLAUDE.md → 'Do NOT reintroduce'). A cleanly redesigned "
                "`hsm sweep complete` is planned as a small build on top of the "
                "unified ComputeSource API.[/yellow]"
            )
            console.print(
                "[yellow]For now, use `hsm sweep status <id>` / `hsm sweep report <id>` "
                "to inspect, then manually re-submit a filtered sweep.[/yellow]"
            )
            return

        combinations = _generate_parameter_combinations(config, max_runs, count_only, console)
        if combinations is None:
            return

        python_path, script_path, project_dir = _detect_project_paths(hsm_config, config)

        if mode not in _ORCHESTRATOR_MODES:
            console.print(
                f"[red]Unknown --mode {mode!r}. Expected one of {sorted(_ORCHESTRATOR_MODES)}.[/red]"
            )
            return

        _run_sweep_via_orchestrator(
            config_path=config_path,
            hsm_config=hsm_config,
            mode=mode,
            python_path=python_path,
            script_path=script_path,
            project_dir=project_dir,
            combinations=combinations,
            dry_run=dry_run,
            walltime=walltime,
            resources=resources,
            group=group,
            parallel_jobs=parallel_jobs,
            no_progress=no_progress,
            console=console,
            logger=logger,
            remote_alias=remote_alias,
            gpus_arg=gpus_arg,
        )

    except FileNotFoundError:
        console.print(f"[red]Error: Sweep config file not found: {config_path}[/red]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.error(f"Sweep execution failed: {e}")
        raise


@click.group("sweep")
@click.pass_context
def sweep_cmd(ctx):
    """Run and manage parameter sweeps."""
    pass


@sweep_cmd.command("run")
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    default=Path("sweeps/sweep.yaml"),
    help="Path to sweep configuration file",
)
@click.option(
    "--mode",
    type=click.Choice(
        ["auto", "individual", "array", "local", "distributed", "remote"]
    ),
    default=None,
    help="Job submission mode (default: 'remote' if --remote given, else 'auto')",
)
@click.option(
    "--remote",
    "remote_alias",
    help="Single remote alias (ssh-config name) to push the sweep to. Implies --mode remote.",
)
@click.option(
    "--gpus",
    "gpus_arg",
    help=(
        "GPU selection on the remote: 'all' (default), 'cpu', an int N (first N), "
        "or a comma-separated allowlist like '0,1,3'. Used with --mode remote."
    ),
)
@click.option("--dry-run", "-d", is_flag=True, help="Show what would be executed without running")
@click.option("--count-only", is_flag=True, help="Count combinations and exit")
@click.option("--max-runs", type=int, help="Maximum number of runs to execute")
@click.option("--walltime", help="Job walltime (overrides config default)")
@click.option("--resources", help="Job resources (overrides config default)")
@click.option("--group", help="W&B group name for this sweep")
@click.option("--parallel-jobs", "-p", type=int, help="Maximum parallel jobs")
@click.option("--no-progress", is_flag=True, help="Disable progress tracking")
@common_options
@click.pass_context
def run_cmd(
    ctx,
    config,
    mode,
    remote_alias,
    gpus_arg,
    dry_run,
    count_only,
    max_runs,
    walltime,
    resources,
    group,
    parallel_jobs,
    no_progress,
    verbose,
    quiet,
):
    """Run parameter sweep."""
    if mode is None:
        mode = "remote" if remote_alias else "auto"
    elif mode == "remote" and not remote_alias:
        ctx.obj["console"].print(
            "[red]--mode remote requires --remote <alias>[/red]"
        )
        return
    elif remote_alias and mode not in ("remote", "auto"):
        ctx.obj["console"].print(
            f"[red]--remote is only valid with --mode remote (got --mode {mode!r}).[/red]"
        )
        return

    hsm_config = HSMConfig.load()

    if walltime is None:
        walltime = hsm_config.get_default_walltime() if hsm_config else "23:59:59"

    if resources is None:
        resources = (
            hsm_config.get_default_resources() if hsm_config else "select=1:ncpus=4:mem=64gb"
        )

    config_path = Path(config)

    run_sweep(
        config_path=config_path,
        mode=mode,
        dry_run=dry_run,
        count_only=count_only,
        max_runs=max_runs,
        walltime=walltime,
        resources=resources,
        group=group,
        parallel_jobs=parallel_jobs,
        no_progress=no_progress,
        console=ctx.obj["console"],
        logger=ctx.obj["logger"],
        hsm_config=hsm_config,
        remote_alias=remote_alias,
        gpus_arg=gpus_arg,
    )


@sweep_cmd.command("status")
@click.argument("sweep_id", required=False)
@click.option("--all", "-a", is_flag=True, help="Show status of all sweeps")
@click.option("--incomplete-only", is_flag=True, help="Show only incomplete sweeps")
@common_options
@click.pass_context
def status_cmd(ctx, sweep_id, all, incomplete_only, verbose, quiet):
    """Show completion status of sweep(s)."""
    from rich.table import Table

    from ..core.common.sweep_analysis import SweepCompletionAnalyzer, find_incomplete_sweeps

    console = ctx.obj["console"]

    if all or incomplete_only:
        sweeps_dir = Path("sweeps/outputs")
        if not sweeps_dir.exists():
            console.print("[yellow]No sweeps directory found.[/yellow]")
            return

        sweeps_to_show = []

        if incomplete_only:
            sweeps_to_show = find_incomplete_sweeps(sweeps_dir)
        else:
            for sweep_dir in sweeps_dir.iterdir():
                if sweep_dir.is_dir() and sweep_dir.name.startswith("sweep_"):
                    analyzer = SweepCompletionAnalyzer(sweep_dir)
                    analysis = analyzer.analyze_completion_status()
                    if "error" not in analysis:
                        sweeps_to_show.append(
                            {
                                "sweep_id": sweep_dir.name,
                                "completion_rate": analysis["completion_rate"],
                                "total_expected": analysis["total_expected"],
                                "total_completed": analysis["total_completed"],
                                "total_missing": analysis["total_missing"],
                                "total_failed": analysis["total_failed"],
                                "total_cancelled": analysis.get("total_cancelled", 0),
                            }
                        )

        if not sweeps_to_show:
            if incomplete_only:
                console.print("[green]No incomplete sweeps found![/green]")
            else:
                console.print("[yellow]No sweeps found.[/yellow]")
            return

        table = Table(title="Sweep Status Summary")
        table.add_column("Sweep ID", style="cyan")
        table.add_column("Progress", style="green")
        table.add_column("Completion %", style="yellow")
        table.add_column("Missing", style="red")
        table.add_column("Failed", style="magenta")
        table.add_column("Cancelled", style="orange")
        table.add_column("Status", style="blue")

        for sweep_info in sorted(sweeps_to_show, key=lambda x: x["sweep_id"]):
            completion_rate = sweep_info["completion_rate"]
            status = "COMPLETE" if completion_rate >= 100.0 else "INCOMPLETE"
            status_style = "green" if status == "COMPLETE" else "red"

            table.add_row(
                sweep_info["sweep_id"],
                f"{sweep_info['total_completed']}/{sweep_info['total_expected']}",
                f"{completion_rate:.1f}%",
                str(sweep_info["total_missing"]),
                str(sweep_info["total_failed"]),
                str(sweep_info["total_cancelled"]),
                f"[{status_style}]{status}[/{status_style}]",
            )

        console.print(table)

    elif sweep_id:
        sweep_dir = Path("sweeps/outputs") / sweep_id
        if not sweep_dir.exists():
            console.print(f"[red]Error: Sweep directory not found: {sweep_dir}[/red]")
            return

        analyzer = SweepCompletionAnalyzer(sweep_dir)
        analysis = analyzer.analyze_completion_status()

        if "error" in analysis:
            console.print(f"[red]Error analyzing sweep: {analysis['error']}[/red]")
            return

        console.print(f"[bold blue]Sweep Status: {sweep_id}[/bold blue]")
        console.print(f"Directory: {sweep_dir}")
        console.print()

        table = Table()
        table.add_column("Metric", style="cyan")
        table.add_column("Count", style="green")
        table.add_column("Percentage", style="yellow")

        total_expected = analysis["total_expected"]

        table.add_row("Expected Combinations", str(analysis["total_expected"]), "100.0%")
        table.add_row(
            "Completed", str(analysis["total_completed"]), f"{analysis['completion_rate']:.1f}%"
        )
        table.add_row(
            "Failed",
            str(analysis["total_failed"]),
            f"{analysis['total_failed'] / total_expected * 100:.1f}%"
            if total_expected > 0
            else "0%",
        )
        total_cancelled = analysis.get("total_cancelled", 0)
        if total_cancelled > 0:
            table.add_row(
                "Cancelled",
                str(total_cancelled),
                f"{total_cancelled / total_expected * 100:.1f}%" if total_expected > 0 else "0%",
            )
        table.add_row(
            "Missing",
            str(analysis["total_missing"]),
            f"{analysis['total_missing'] / total_expected * 100:.1f}%"
            if total_expected > 0
            else "0%",
        )

        if analysis["total_running"] > 0:
            table.add_row(
                "Running",
                str(analysis["total_running"]),
                f"{analysis['total_running'] / total_expected * 100:.1f}%",
            )

        console.print(table)

        if analysis.get("status_fixes_made", False):
            console.print("\n[yellow]🔧 Status corrections were applied during analysis.[/yellow]")

        if not analysis["needs_completion"]:
            console.print("\n[green]✓ Sweep is complete![/green]")
        else:
            console.print(
                f"\n[yellow]⚠ Sweep has {analysis['total_missing']} missing + "
                f"{analysis['total_failed']} failed task(s). "
                "Manual re-submission with a filtered sweep config is the current path "
                "(an `hsm sweep complete` rebuild is planned).[/yellow]"
            )

    else:
        console.print("[red]Error: Please specify a sweep ID or use --all/--incomplete-only[/red]")


@sweep_cmd.command("report")
@click.argument("sweep_id")
@click.option(
    "--scan-tasks", is_flag=True, help="Force scanning task directories (useful for array jobs)"
)
@click.option("--save-json", is_flag=True, help="Save detailed report to JSON file")
@common_options
@click.pass_context
def report_cmd(ctx, sweep_id, scan_tasks, save_json, verbose, quiet):
    """Generate detailed completion report for a sweep."""
    import json

    from rich.table import Table

    from ..core.common.sweep_analysis import SweepCompletionAnalyzer

    console = ctx.obj["console"]

    sweep_dir = Path("sweeps/outputs") / sweep_id
    if not sweep_dir.exists():
        console.print(f"[red]Error: Sweep directory not found: {sweep_dir}[/red]")
        return

    console.print(f"[bold blue]Sweep Completion Report: {sweep_id}[/bold blue]")
    console.print(f"Directory: {sweep_dir}")

    analyzer = SweepCompletionAnalyzer(sweep_dir)

    if scan_tasks:
        console.print("\n[cyan]Scanning task directories...[/cyan]")
        analysis = analyzer.analyze_from_task_directories()
    else:
        console.print("\n[cyan]Analyzing completion status...[/cyan]")
        analysis = analyzer.analyze_completion_status()

    if "error" in analysis:
        console.print(f"[red]Error analyzing sweep: {analysis['error']}[/red]")
        return

    console.print("\n[bold]Summary:[/bold]")
    table = Table()
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="green")
    table.add_column("Percentage", style="yellow")

    total_expected = analysis["total_expected"]
    total_completed = analysis["total_completed"]
    total_failed = analysis["total_failed"]
    total_missing = analysis["total_missing"]
    total_running = analysis.get("total_running", 0)

    table.add_row("Expected Combinations", str(total_expected), "100.0%")
    table.add_row("Completed", str(total_completed), f"{analysis['completion_rate']:.1f}%")
    table.add_row(
        "Failed",
        str(total_failed),
        f"{total_failed / total_expected * 100:.1f}%" if total_expected > 0 else "0%",
    )
    table.add_row(
        "Missing",
        str(total_missing),
        f"{total_missing / total_expected * 100:.1f}%" if total_expected > 0 else "0%",
    )
    if total_running > 0:
        table.add_row(
            "Running",
            str(total_running),
            f"{total_running / total_expected * 100:.1f}%" if total_expected > 0 else "0%",
        )

    console.print(table)

    if "task_statuses" in analysis and analysis["task_statuses"]:
        console.print(f"\n[bold]Task Status Details:[/bold]")
        console.print(f"Total tasks found: {len(analysis['task_statuses'])}")

        status_groups: dict = {}
        for task_id, status in analysis["task_statuses"].items():
            status_groups.setdefault(status, []).append(task_id)

        for status, tasks in sorted(status_groups.items()):
            console.print(f"  {status}: {len(tasks)} tasks")

    if "missing_task_numbers" in analysis and analysis["missing_task_numbers"]:
        missing_numbers = analysis["missing_task_numbers"]
        if len(missing_numbers) <= 20:
            console.print(f"\n[yellow]Missing task numbers: {missing_numbers}[/yellow]")
        else:
            console.print(
                f"\n[yellow]Missing task numbers (first 20): {missing_numbers[:20]}...[/yellow]"
            )
            console.print(f"[yellow]Total missing: {len(missing_numbers)}[/yellow]")

    if analysis.get("failed_tasks"):
        failed_tasks = analysis["failed_tasks"]
        if len(failed_tasks) <= 20:
            console.print(f"\n[red]Failed tasks: {failed_tasks}[/red]")
        else:
            console.print(f"\n[red]Failed tasks (first 20): {failed_tasks[:20]}...[/red]")
            console.print(f"[red]Total failed: {len(failed_tasks)}[/red]")

    if save_json:
        report_file = sweep_dir / "detailed_completion_report.json"
        try:
            json_report = {
                "sweep_id": sweep_id,
                "sweep_dir": str(sweep_dir),
                "timestamp": analysis.get("timestamp", datetime.now().isoformat()),
                "scan_method": analysis.get("scan_method", "source_mapping"),
                "summary": {
                    "total_expected": total_expected,
                    "total_completed": total_completed,
                    "total_failed": total_failed,
                    "total_missing": total_missing,
                    "total_running": total_running,
                    "completion_rate": analysis["completion_rate"],
                },
                "completed_tasks": analysis.get("completed_tasks", []),
                "failed_tasks": analysis.get("failed_tasks", []),
                "running_tasks": analysis.get("running_tasks", []),
                "missing_task_numbers": analysis.get("missing_task_numbers", []),
                "task_statuses": analysis.get("task_statuses", {}),
            }

            with open(report_file, "w") as f:
                json.dump(json_report, f, indent=2)

            console.print(f"\n[green]✓ Detailed report saved to: {report_file}[/green]")
        except Exception as e:
            console.print(f"[red]Error saving report: {e}[/red]")

    if not analysis.get("needs_completion"):
        console.print("\n[green]✓ Sweep is complete![/green]")

    source_mapping = sweep_dir / "source_mapping.yaml"
    if source_mapping.exists():
        console.print(f"\n[dim]Task tracking: {source_mapping}[/dim]")


@sweep_cmd.command("errors")
@click.argument("sweep_id")
@click.option("--all", "-a", is_flag=True, help="Show all error details")
@click.option("--pattern", help="Filter errors by pattern (e.g., 'ImportError')")
@common_options
@click.pass_context
def errors_cmd(ctx, sweep_id, all, pattern, verbose, quiet):
    """Show error summaries for a specific sweep."""
    console = ctx.obj["console"]

    sweep_dir = Path("sweeps/outputs") / sweep_id
    if not sweep_dir.exists():
        console.print(f"[red]Error: Sweep directory not found: {sweep_dir}[/red]")
        return

    error_dir = sweep_dir / "errors"
    if not error_dir.exists():
        console.print(f"[yellow]No error directory found for sweep {sweep_id}[/yellow]")
        console.print("This means either:")
        console.print("• No jobs have failed yet")
        console.print("• Error collection is not yet implemented for this execution mode")
        return

    error_files = list(error_dir.glob("*_error.txt"))
    if not error_files:
        console.print("[green]No error files found - all jobs may have succeeded![/green]")
        return

    console.print(f"[bold blue]Error Summary for Sweep: {sweep_id}[/bold blue]")
    console.print(f"Found {len(error_files)} error files in: {error_dir}")

    if pattern:
        filtered_files = []
        for error_file in error_files:
            try:
                with open(error_file) as f:
                    content = f.read()
                    if pattern.lower() in content.lower():
                        filtered_files.append(error_file)
            except Exception:
                pass
        error_files = filtered_files
        console.print(f"Filtered to {len(error_files)} files containing '{pattern}'")

    if not error_files:
        console.print(f"[yellow]No error files match the pattern '{pattern}'[/yellow]")
        return

    for i, error_file in enumerate(error_files):
        console.print(f"\n[bold red]Error {i + 1}: {error_file.stem}[/bold red]")
        try:
            with open(error_file) as f:
                content = f.read()
                if all:
                    console.print(content)
                else:
                    preview = content[:400]
                    if len(content) > 400:
                        preview += "\n... (use --all to see full content)"
                    console.print(preview)
        except Exception as e:
            console.print(f"[red]Could not read error file: {e}[/red]")

        if not all and i >= 4:
            remaining = len(error_files) - i - 1
            if remaining > 0:
                console.print(
                    f"\n[yellow]... and {remaining} more errors (use --all to see all)[/yellow]"
                )
            break

    console.print("\n[cyan]💡 Use --all to see full error details[/cyan]")
    console.print("[cyan]💡 Use --pattern to filter by specific error types[/cyan]")


# --------------------------------------------------------------------------
# Sweep lifecycle commands moved here from the deleted `hsm monitor` group:
# watch / recent / queue / cancel / cleanup. All read on-disk metadata via
# SweepCompletionAnalyzer — no scheduler polling, works for every backend.
# --------------------------------------------------------------------------


def _load_sweep_meta(sweep_dir: Path) -> dict:
    """Parse ``submission_summary.txt`` into a small dict for lifecycle ops.

    Missing fields just stay ``None`` — every consumer tolerates that. The
    truth-source for *progress* is ``SweepCompletionAnalyzer`` (reads
    ``tasks/*/task_info.txt``); this is only for sweep-level metadata like
    submission time, backend type, and the list of job IDs.
    """
    info: dict = {
        "sweep_id": sweep_dir.name,
        "sweep_dir": sweep_dir,
        "submission_time": None,
        "mode": None,
        "backend": None,
        "job_ids": [],
        "total_combinations": 0,
    }
    summary = sweep_dir / "submission_summary.txt"
    if not summary.exists():
        return info
    try:
        for line in summary.read_text().splitlines():
            if line.startswith("Submission Time:"):
                ts = line.split(":", 1)[1].strip()
                for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
                    try:
                        info["submission_time"] = datetime.strptime(ts, fmt)
                        break
                    except ValueError:
                        continue
            elif line.startswith("Mode:"):
                info["mode"] = line.split(":", 1)[1].strip()
            elif line.startswith("Backend:"):
                info["backend"] = line.split(":", 1)[1].strip()
            elif line.startswith("Total Combinations:"):
                try:
                    info["total_combinations"] = int(line.split(":", 1)[1].strip())
                except ValueError:
                    pass
            elif line.startswith("Job IDs:"):
                ids = line.split(":", 1)[1].strip()
                info["job_ids"] = [j.strip() for j in ids.split(",") if j.strip()]
    except Exception:  # noqa: BLE001 — never break on a malformed summary file
        pass
    return info


@sweep_cmd.command("watch")
@click.argument("sweep_id")
@click.option("--refresh", default=5, type=int, help="Refresh interval in seconds (default 5)")
@click.option(
    "--once", is_flag=True, help="Show progress once and exit (don't refresh)"
)
@common_options
@click.pass_context
def watch_cmd(ctx, sweep_id, refresh, once, verbose, quiet):
    """Live progress view for a single sweep (Ctrl+C to exit).

    Reads ``tasks/*/task_info.txt`` via the same analyzer that backs
    ``hsm sweep status`` / ``hsm sweep report``. Works for every backend
    (local / Slurm / SSH push / distributed) because the source of truth
    is on-disk task state, not a scheduler queue.
    """
    import time as _time

    from rich.live import Live
    from rich.panel import Panel
    from rich.progress_bar import ProgressBar

    from ..core.common.sweep_analysis import SweepCompletionAnalyzer

    console = ctx.obj["console"]
    sweep_dir = Path("sweeps/outputs") / sweep_id
    if not sweep_dir.exists():
        console.print(f"[red]Error: Sweep directory not found: {sweep_dir}[/red]")
        return

    analyzer = SweepCompletionAnalyzer(sweep_dir)

    def _render() -> Panel:
        a = analyzer.analyze_completion_status()
        if "error" in a:
            return Panel(f"[red]{a['error']}[/red]", title=sweep_id)
        total = a["total_expected"]
        done = a["total_completed"]
        failed = a["total_failed"]
        missing = a["total_missing"]
        running = a.get("total_running", 0)
        cancelled = a.get("total_cancelled", 0)
        rate = a["completion_rate"]
        bar = ProgressBar(total=max(total, 1), completed=done, width=40)
        lines = [
            f"[bold]{sweep_id}[/bold]",
            "",
            f"[green]Completed:[/green] {done}",
            f"[red]Failed:[/red]    {failed}",
            f"[yellow]Missing:[/yellow]   {missing}",
        ]
        if running:
            lines.append(f"[blue]Running:[/blue]   {running}")
        if cancelled:
            lines.append(f"Cancelled: {cancelled}")
        lines.append(f"[bold]Total:[/bold]     {total}")
        lines.append("")
        lines.append(f"Progress: {done}/{total} ({rate:.1f}%)")
        from rich.console import Group
        return Panel(
            Group("\n".join(lines), bar),
            title=f"hsm sweep watch — {sweep_id}",
            border_style="cyan",
        )

    if once:
        console.print(_render())
        return

    try:
        with Live(_render(), console=console, refresh_per_second=1) as live:
            while True:
                _time.sleep(max(refresh, 1))
                live.update(_render())
                # Stop refreshing once everything's terminal.
                a = analyzer.analyze_completion_status()
                if "error" not in a and not a.get("needs_completion"):
                    break
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopped watching.[/yellow]")


@sweep_cmd.command("recent")
@click.option("-d", "--days", default=7, type=int, help="Show sweeps from last N days")
@common_options
@click.pass_context
def recent_cmd(ctx, days, verbose, quiet):
    """List sweeps submitted in the last N days."""
    from rich.table import Table

    from ..core.common.sweep_analysis import SweepCompletionAnalyzer

    console = ctx.obj["console"]

    sweeps_dir = Path("sweeps/outputs")
    if not sweeps_dir.exists():
        console.print("[yellow]No sweeps/outputs directory found.[/yellow]")
        return

    cutoff = datetime.now() - timedelta(days=days)

    rows = []
    for sweep_dir in sweeps_dir.iterdir():
        if not sweep_dir.is_dir() or not sweep_dir.name.startswith("sweep_"):
            continue
        meta = _load_sweep_meta(sweep_dir)
        ts = meta["submission_time"]
        # Fallback: directory mtime when there's no summary file yet.
        if ts is None:
            ts = datetime.fromtimestamp(sweep_dir.stat().st_mtime)
        if ts < cutoff:
            continue
        analysis = SweepCompletionAnalyzer(sweep_dir).analyze_completion_status()
        if "error" in analysis:
            rate = -1
            status = "?"
            done = total = 0
        else:
            rate = analysis["completion_rate"]
            done = analysis["total_completed"]
            total = analysis["total_expected"]
            status = "COMPLETE" if not analysis["needs_completion"] else "INCOMPLETE"
        rows.append((ts, meta, done, total, rate, status))

    if not rows:
        console.print(f"[yellow]No sweeps in the last {days} day(s).[/yellow]")
        return

    rows.sort(key=lambda r: r[0], reverse=True)

    table = Table(title=f"Recent Sweeps (last {days} day(s))")
    table.add_column("Sweep ID", style="cyan")
    table.add_column("Submitted", style="dim")
    table.add_column("Backend", style="magenta")
    table.add_column("Progress", style="green")
    table.add_column("%", style="yellow", justify="right")
    table.add_column("Status", style="bold")

    for ts, meta, done, total, rate, status in rows:
        progress = f"{done}/{total}" if total else "?"
        rate_str = f"{rate:.0f}%" if rate >= 0 else "?"
        status_style = (
            "green" if status == "COMPLETE" else ("red" if status == "INCOMPLETE" else "dim")
        )
        table.add_row(
            meta["sweep_id"],
            ts.strftime("%Y-%m-%d %H:%M"),
            meta["backend"] or "?",
            progress,
            rate_str,
            f"[{status_style}]{status}[/{status_style}]",
        )

    console.print(table)


@sweep_cmd.command("queue")
@click.pass_context
def queue_cmd(ctx):
    """Show the cluster's job queue (auto-detects ``squeue`` or ``qstat``)."""
    import shutil
    import subprocess

    console = ctx.obj["console"]

    user = os.environ.get("USER") or os.environ.get("LOGNAME")
    if shutil.which("squeue"):
        cmd = ["squeue", "-u", user] if user else ["squeue"]
    elif shutil.which("qstat"):
        cmd = ["qstat", "-u", user] if user else ["qstat"]
    else:
        console.print(
            "[yellow]Neither `squeue` nor `qstat` is on PATH — no cluster scheduler "
            "detected. (`hsm sweep queue` is for HPC clusters.)[/yellow]"
        )
        return

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    except Exception as e:  # noqa: BLE001
        console.print(f"[red]Failed to run {' '.join(cmd)}: {e}[/red]")
        return
    if result.returncode != 0:
        console.print(f"[red]{cmd[0]} exited with rc={result.returncode}[/red]")
        if result.stderr:
            console.print(result.stderr.strip())
        return
    output = (result.stdout or "").rstrip()
    if not output:
        console.print(f"[green]No jobs currently in the queue ({cmd[0]}).[/green]")
        return
    console.print(output)


@sweep_cmd.command("cancel")
@click.argument("sweep_id")
@click.option("-y", "--yes", is_flag=True, help="Skip the confirmation prompt")
@click.pass_context
def cancel_cmd(ctx, sweep_id, yes):
    """Cancel a running sweep.

    Routes by backend (read from ``submission_summary.txt``):

    - Slurm → ``scancel <job_id>`` per job ID.
    - PBS   → ``qdel <job_id>`` per job ID.
    - Local → kills processes matching the sweep ID via ``pkill -f``.
    - SSH push / distributed → cannot remote-cancel reliably (no remote-pid
      tracking once the local ``hsm sweep run`` exits). Documents this and
      tells the user to Ctrl+C the local driver process instead.
    """
    import shutil
    import subprocess

    console = ctx.obj["console"]
    sweep_dir = Path("sweeps/outputs") / sweep_id
    if not sweep_dir.exists():
        console.print(f"[red]Error: Sweep directory not found: {sweep_dir}[/red]")
        return

    meta = _load_sweep_meta(sweep_dir)
    backend = (meta["backend"] or "").lower()
    job_ids = meta["job_ids"]

    console.print(f"[bold]Cancelling sweep:[/bold] {sweep_id}")
    console.print(f"  Backend: {backend or '?'}")
    console.print(f"  Job IDs: {job_ids or '(none recorded)'}")

    if not yes:
        if not click.confirm("Proceed?", default=False):
            console.print("Cancelled.")
            return

    if backend == "slurm":
        tool = "scancel"
    elif backend == "pbs":
        tool = "qdel"
    elif backend == "local":
        # The local backend records job IDs like local_<pid>_<n>; killing the
        # parent driver process tree is what works.
        killed = 0
        try:
            result = subprocess.run(
                ["pkill", "-f", sweep_id], capture_output=True, text=True, timeout=10
            )
            if result.returncode in (0, 1):  # 0 = killed, 1 = nothing matched
                killed = 1 if result.returncode == 0 else 0
            else:
                console.print(f"[yellow]pkill exited rc={result.returncode}[/yellow]")
        except Exception as e:  # noqa: BLE001
            console.print(f"[red]pkill failed: {e}[/red]")
            return
        msg = "✓ killed local processes" if killed else "no matching local processes found"
        console.print(f"[green]{msg}[/green]")
        return
    elif backend in ("ssh_remote", "distributed"):
        console.print(
            "[yellow]Cannot reliably remote-cancel push-model SSH sweeps from here. "
            "If the local `hsm sweep run` process is still active, Ctrl+C it; "
            "otherwise the remote tasks are already done or you can kill them via "
            "`ssh <alias> 'pkill -f <sweep_id>'`.[/yellow]"
        )
        return
    else:
        console.print(
            f"[yellow]Unknown backend {backend!r}; can't choose a cancel command.[/yellow]"
        )
        return

    if not shutil.which(tool):
        console.print(f"[red]{tool} not on PATH — can't cancel.[/red]")
        return

    ok = fail = 0
    for jid in job_ids:
        try:
            r = subprocess.run([tool, jid], capture_output=True, text=True, timeout=15)
            if r.returncode == 0:
                ok += 1
            else:
                fail += 1
                console.print(f"[red]✗ {tool} {jid}: {r.stderr.strip()}[/red]")
        except Exception as e:  # noqa: BLE001
            fail += 1
            console.print(f"[red]✗ {tool} {jid}: {e}[/red]")

    console.print(f"[bold]{ok} cancelled, {fail} failed.[/bold]")


@sweep_cmd.command("cleanup")
@click.option(
    "-d", "--older-than-days", default=30, type=int, help="Delete sweep dirs older than N days"
)
@click.option(
    "--keep-incomplete",
    is_flag=True,
    help="Skip sweeps that still have missing or failed tasks",
)
@click.option("-y", "--yes", is_flag=True, help="Skip the confirmation prompt")
@click.option("--dry-run", "-n", is_flag=True, help="List candidates without deleting")
@click.pass_context
def cleanup_cmd(ctx, older_than_days, keep_incomplete, yes, dry_run):
    """Delete old sweep output dirs to reclaim disk.

    Default policy: dirs whose submission time (or mtime if no summary) is
    more than ``--older-than-days`` old. Use ``--keep-incomplete`` to spare
    sweeps that still have failed/missing tasks. ``--dry-run`` to preview.
    """
    import shutil as _shutil

    from ..core.common.sweep_analysis import SweepCompletionAnalyzer

    console = ctx.obj["console"]

    sweeps_dir = Path("sweeps/outputs")
    if not sweeps_dir.exists():
        console.print("[yellow]No sweeps/outputs directory found.[/yellow]")
        return

    cutoff = datetime.now() - timedelta(days=older_than_days)
    candidates: list[Path] = []
    skipped: list[tuple[Path, str]] = []

    for sweep_dir in sweeps_dir.iterdir():
        if not sweep_dir.is_dir() or not sweep_dir.name.startswith("sweep_"):
            continue
        meta = _load_sweep_meta(sweep_dir)
        ts = meta["submission_time"] or datetime.fromtimestamp(sweep_dir.stat().st_mtime)
        if ts >= cutoff:
            continue
        if keep_incomplete:
            a = SweepCompletionAnalyzer(sweep_dir).analyze_completion_status()
            if "error" not in a and a.get("needs_completion"):
                skipped.append((sweep_dir, "incomplete"))
                continue
        candidates.append(sweep_dir)

    if not candidates:
        console.print(
            f"[green]No sweeps older than {older_than_days} day(s) to delete.[/green]"
        )
        if skipped:
            console.print(f"[dim]({len(skipped)} skipped — incomplete.)[/dim]")
        return

    console.print(
        f"[bold]Would delete {len(candidates)} sweep dir(s) older than "
        f"{older_than_days} day(s):[/bold]"
    )
    for path in candidates:
        console.print(f"  {path}")
    if dry_run:
        console.print(f"\n[dim]Dry run — nothing removed.[/dim]")
        return

    if not yes:
        if not click.confirm(f"Delete {len(candidates)} sweep dir(s)?", default=False):
            console.print("Cancelled.")
            return

    removed = 0
    for path in candidates:
        try:
            _shutil.rmtree(path)
            removed += 1
        except Exception as e:  # noqa: BLE001
            console.print(f"[red]Failed to remove {path}: {e}[/red]")
    console.print(f"[green]✓ Removed {removed} sweep dir(s).[/green]")
    if skipped:
        console.print(
            f"[dim]Kept {len(skipped)} incomplete sweep(s) (use without --keep-incomplete to remove).[/dim]"
        )


# Legacy aliases — kept hidden for backwards-compat invocation patterns.
@click.command("sweep-legacy", hidden=True)
@click.pass_context
def sweep_legacy_cmd(ctx):
    """Legacy sweep command (use 'hsm sweep run' instead)."""
    ctx.invoke(run_cmd)


@click.group(hidden=True)
def sweep():
    """Run and manage parameter sweeps (legacy group)."""
    pass


@sweep.command("run", hidden=True)
@click.pass_context
def _legacy_inner_run(ctx):
    """Run parameter sweep (legacy interface)."""
    ctx.invoke(sweep_cmd)
