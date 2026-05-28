"""Sweep execution CLI commands."""

from datetime import datetime
import logging
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

    scheduler_hint = "slurm" if mode in ("array", "individual") else None
    spec = spec_from_cli(
        walltime=walltime,
        resources=resources,
        scheduler=scheduler_hint,
        hsm_config=hsm_config,
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
