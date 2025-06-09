"""Sweep execution CLI commands."""

from pathlib import Path
from rich.console import Console
from rich.table import Table
import logging
from typing import Optional
from datetime import datetime

from ..core.config_parser import SweepConfig
from ..core.param_generator import ParameterGenerator
from ..core.job_manager import JobManager
from ..core.utils import create_sweep_id


def run_sweep(
    config_path: Path,
    mode: str,
    dry_run: bool,
    count_only: bool,
    max_runs: Optional[int],
    walltime: str,
    resources: str,
    group: Optional[str],
    priority: Optional[int],
    console: Console,
    logger: logging.Logger,
):
    """Run parameter sweep."""

    console.print(f"[bold blue]HPC Sweep Manager[/bold blue]")
    console.print(f"Config: {config_path}")
    console.print(f"Mode: {mode}")

    try:
        # Load sweep configuration
        config = SweepConfig.from_yaml(config_path)
        logger.info(f"Loaded sweep config from {config_path}")

        # Validate configuration
        errors = config.validate()
        if errors:
            console.print("[red]Configuration validation failed:[/red]")
            for error in errors:
                console.print(f"  - {error}")
            return

        # Generate parameter combinations
        generator = ParameterGenerator(config)

        if count_only:
            total_combinations = generator.count_combinations()
            console.print(
                f"[green]Total parameter combinations: {total_combinations}[/green]"
            )
            return

        combinations = generator.generate_combinations(max_runs)

        # Show parameter info
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

        if dry_run:
            console.print("\n[yellow]DRY RUN - No jobs will be submitted[/yellow]")

            # Show first few combinations
            console.print("\n[bold]First 3 parameter combinations:[/bold]")
            for i, combo in enumerate(combinations[:3], 1):
                console.print(f"  {i}. {combo}")

            return

        # Generate sweep ID
        sweep_id = create_sweep_id()
        console.print(f"\n[green]Sweep ID: {sweep_id}[/green]")

        # Create sweep directory
        sweep_dir = Path("sweeps") / "outputs" / sweep_id
        sweep_dir.mkdir(parents=True, exist_ok=True)

        console.print(f"Sweep directory: {sweep_dir}")

        # Create subdirectories for organization
        pbs_dir = sweep_dir / "pbs_files"
        pbs_dir.mkdir(exist_ok=True)

        logs_dir = sweep_dir / "logs"
        logs_dir.mkdir(exist_ok=True)

        console.print(f"PBS files will be stored in: {pbs_dir}")
        console.print(f"Job logs will be stored in: {logs_dir}")

        # Save sweep config for reference
        config_backup = sweep_dir / "sweep_config.yaml"
        import shutil

        shutil.copy2(config_path, config_backup)

        # Create job manager with project configuration
        from ..core.path_detector import PathDetector

        detector = PathDetector()

        # Try to get paths from project config or auto-detect
        python_path = detector.detect_python_path()
        script_path = detector.detect_train_script()
        project_dir = str(Path.cwd())

        job_manager = JobManager.auto_detect(
            walltime=walltime,
            resources=resources,
            python_path=python_path,
            script_path=script_path,
            project_dir=project_dir,
        )
        console.print(f"Detected HPC system: {job_manager.system_type}")

        # Submit jobs
        console.print(
            f"\n[bold]Submitting {len(combinations)} jobs in {mode} mode...[/bold]"
        )

        try:
            job_ids = job_manager.submit_sweep(
                param_combinations=combinations,
                mode=mode,
                sweep_dir=sweep_dir,
                sweep_id=sweep_id,
                wandb_group=group,
                pbs_dir=pbs_dir,
                logs_dir=logs_dir,
            )

            console.print(
                f"\n[green]Successfully submitted {len(job_ids)} job(s):[/green]"
            )
            for job_id in job_ids:
                console.print(f"  - {job_id}")

            # Create submission summary
            summary_file = sweep_dir / "submission_summary.txt"
            with open(summary_file, "w") as f:
                f.write(f"Sweep Submission Summary\n")
                f.write(f"========================\n")
                f.write(f"Sweep ID: {sweep_id}\n")
                f.write(f"Submission Time: {datetime.now()}\n")
                f.write(f"Mode: {mode}\n")
                f.write(f"Total Combinations: {len(combinations)}\n")
                f.write(f"Job IDs: {', '.join(job_ids)}\n")
                f.write(f"Walltime: {walltime}\n")
                f.write(f"Resources: {resources}\n")
                if group:
                    f.write(f"W&B Group: {group}\n")

            console.print(f"\nSummary saved to: {summary_file}")
            logger.info(
                f"Sweep {sweep_id} submitted successfully with {len(combinations)} combinations"
            )

        except Exception as e:
            console.print(f"[red]Error submitting jobs: {e}[/red]")
            logger.error(f"Job submission failed: {e}")
            raise

    except FileNotFoundError:
        console.print(f"[red]Error: Sweep config file not found: {config_path}[/red]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.error(f"Sweep execution failed: {e}")
        raise
