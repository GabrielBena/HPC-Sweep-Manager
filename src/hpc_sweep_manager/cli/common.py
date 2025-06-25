"""Common CLI utilities shared across all command modules."""

import logging
from pathlib import Path
from typing import Any, Dict, Optional

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ..core.common.config import HSMConfig
from ..core.common.path_detector import PathDetector


def setup_cli_context(ctx: click.Context, verbose: bool = False, quiet: bool = False):
    """Setup CLI context with logging and console."""
    from ..core.common.utils import setup_logging

    # Set up logging
    if quiet:
        log_level = "ERROR"
    elif verbose:
        log_level = "DEBUG"
    else:
        log_level = "INFO"

    logger = setup_logging(log_level)
    console = Console()

    # Ensure context object exists
    ctx.ensure_object(dict)
    ctx.obj["logger"] = logger
    ctx.obj["console"] = console

    return logger, console


def validate_project_setup(console: Console, logger: logging.Logger) -> bool:
    """Validate that the project has proper HSM setup."""
    # Check for HSM config
    hsm_config = HSMConfig.load()
    if not hsm_config:
        console.print("[red]No hsm_config.yaml found. Run 'hsm init' to setup the project.[/red]")
        return False

    # Check for sweep directory
    sweep_dir = Path("sweeps")
    if not sweep_dir.exists():
        console.print(
            "[yellow]No sweeps directory found. Run 'hsm init' to setup infrastructure.[/yellow]"
        )
        return False

    return True


def load_hsm_config_with_validation(
    console: Console, logger: logging.Logger
) -> Optional[HSMConfig]:
    """Load HSM config with validation and user-friendly error messages."""
    hsm_config = HSMConfig.load()

    if not hsm_config:
        console.print(
            Panel(
                "[red]HSM Configuration Not Found[/red]\n\n"
                "No hsm_config.yaml file was found in the current directory or sweeps/ subdirectory.\n\n"
                "[bold]To fix this:[/bold]\n"
                "1. Run [cyan]hsm init[/cyan] to initialize HSM in your project\n"
                "2. Or ensure you're in the correct project directory",
                title="Configuration Error",
                border_style="red",
            )
        )
        return None

    return hsm_config


def display_project_status(console: Console, logger: logging.Logger) -> Dict[str, Any]:
    """Display comprehensive project status information."""
    console.print("\n[bold blue]Project Status Overview[/bold blue]")

    # Detect project info
    detector = PathDetector()
    project_info = detector.get_project_info()

    # Load HSM config
    hsm_config = HSMConfig.load()

    status = {
        "project_root": project_info["project_root"],
        "has_hsm_config": hsm_config is not None,
        "python_path": project_info["python_path"],
        "train_script": project_info["train_script"],
        "config_dir": project_info["config_dir"],
        "sweep_dir_exists": Path("sweeps").exists(),
    }

    # Create status table
    table = Table(title="Project Configuration")
    table.add_column("Component", style="cyan", width=20)
    table.add_column("Status", style="bold", width=15)
    table.add_column("Value/Path", style="dim", width=50)

    # Project root
    table.add_row(
        "Project Root",
        "[green]✓[/green]" if status["project_root"] else "[red]✗[/red]",
        str(status["project_root"]) if status["project_root"] else "Not detected",
    )

    # HSM Config
    table.add_row(
        "HSM Config",
        "[green]✓[/green]" if status["has_hsm_config"] else "[red]✗[/red]",
        "Found" if status["has_hsm_config"] else "Run 'hsm init'",
    )

    # Python interpreter
    table.add_row(
        "Python",
        "[green]✓[/green]" if status["python_path"] else "[yellow]?[/yellow]",
        str(status["python_path"]) if status["python_path"] else "Using system default",
    )

    # Training script
    table.add_row(
        "Training Script",
        "[green]✓[/green]" if status["train_script"] else "[yellow]?[/yellow]",
        str(status["train_script"]) if status["train_script"] else "Not detected",
    )

    # Config directory (Hydra)
    table.add_row(
        "Config Directory",
        "[green]✓[/green]" if status["config_dir"] else "[dim]N/A[/dim]",
        str(status["config_dir"]) if status["config_dir"] else "No Hydra configs",
    )

    # Sweep directory
    table.add_row(
        "Sweep Directory",
        "[green]✓[/green]" if status["sweep_dir_exists"] else "[red]✗[/red]",
        "sweeps/" if status["sweep_dir_exists"] else "Run 'hsm init'",
    )

    console.print(table)

    # Show execution mode availability
    console.print("\n[bold blue]Execution Mode Availability[/bold blue]")

    mode_table = Table(title="Available Execution Modes")
    mode_table.add_column("Mode", style="cyan", width=15)
    mode_table.add_column("Status", style="bold", width=10)
    mode_table.add_column("Requirements", style="dim", width=50)

    # Local mode - always available
    mode_table.add_row("Local", "[green]✓[/green]", "Always available")

    # Remote mode - check if remotes configured
    if hsm_config and hsm_config.config_data.get("distributed", {}).get("remotes"):
        mode_table.add_row("Remote", "[green]✓[/green]", "SSH remotes configured")
    else:
        mode_table.add_row("Remote", "[yellow]○[/yellow]", "Run 'hsm remote add' to configure")

    # Distributed mode - check if distributed enabled
    if hsm_config and hsm_config.config_data.get("distributed", {}).get("enabled"):
        mode_table.add_row("Distributed", "[green]✓[/green]", "Distributed computing enabled")
    else:
        mode_table.add_row(
            "Distributed", "[yellow]○[/yellow]", "Run 'hsm distributed init' to enable"
        )

    # HPC mode - check for schedulers
    try:
        import shutil

        has_pbs = shutil.which("qsub") and shutil.which("qstat")
        has_slurm = shutil.which("sbatch") and shutil.which("squeue")

        if has_pbs or has_slurm:
            scheduler = "PBS/Torque" if has_pbs else "Slurm"
            mode_table.add_row("HPC", "[green]✓[/green]", f"{scheduler} detected")
        else:
            mode_table.add_row("HPC", "[red]✗[/red]", "No HPC scheduler detected")
    except Exception:
        mode_table.add_row("HPC", "[yellow]?[/yellow]", "Could not detect scheduler")

    console.print(mode_table)

    return status


def format_duration_human(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.1f}m"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        if minutes > 0:
            return f"{hours}h {minutes}m"
        else:
            return f"{hours}h"


def format_memory_human(bytes_value: int) -> str:
    """Format memory in human-readable format."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f}{unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f}PB"


def create_status_panel(title: str, content: str, status: str = "info") -> Panel:
    """Create a styled status panel."""
    colors = {"success": "green", "warning": "yellow", "error": "red", "info": "blue"}

    return Panel(content, title=title, border_style=colors.get(status, "blue"))


def confirm_action(console: Console, message: str, default: bool = False) -> bool:
    """Get user confirmation for an action."""
    suffix = " [Y/n]" if default else " [y/N]"
    response = console.input(f"{message}{suffix}: ")

    if not response:
        return default

    return response.lower().startswith("y")


def display_sweep_summary(
    console: Console,
    total_combinations: int,
    mode: str,
    config_path: Path,
    max_runs: Optional[int] = None,
):
    """Display a summary before sweep execution."""
    effective_runs = min(total_combinations, max_runs) if max_runs else total_combinations

    summary_text = f"""
[bold]Sweep Configuration Summary[/bold]

Configuration: [cyan]{config_path}[/cyan]
Execution Mode: [green]{mode.upper()}[/green]
Total Combinations: [yellow]{total_combinations}[/yellow]
Jobs to Execute: [yellow]{effective_runs}[/yellow]
"""

    if max_runs and max_runs < total_combinations:
        summary_text += f"[dim](Limited by --max-runs={max_runs})[/dim]\n"

    console.print(Panel(summary_text, title="Sweep Preview", border_style="blue"))


def handle_cli_error(console: Console, logger: logging.Logger, error: Exception, context: str = ""):
    """Handle CLI errors with user-friendly messages."""
    error_msg = str(error)

    # Common error patterns and user-friendly messages
    if "No hsm_config.yaml found" in error_msg:
        console.print(
            create_status_panel(
                "Configuration Error",
                "No HSM configuration found. Run [cyan]hsm init[/cyan] to setup your project.",
                "error",
            )
        )
    elif "SSH connection failed" in error_msg:
        console.print(
            create_status_panel(
                "Remote Connection Error",
                f"Could not connect to remote machine.\n\n"
                f"Check your SSH configuration and ensure:\n"
                f"• SSH key is correct and accessible\n"
                f"• Remote host is reachable\n"
                f"• SSH agent is running (if using agent)\n\n"
                f"Error: {error_msg}",
                "error",
            )
        )
    elif "FileNotFoundError" in error_msg:
        console.print(
            create_status_panel(
                "File Not Found",
                f"Required file not found: {error_msg}\n\n"
                f"Ensure all necessary files exist and paths are correct.",
                "error",
            )
        )
    else:
        # Generic error handling
        console.print(
            create_status_panel(
                f"Error{' in ' + context if context else ''}",
                f"{error_msg}\n\nFor more details, run with --verbose flag.",
                "error",
            )
        )

    logger.error(f"CLI error{' in ' + context if context else ''}: {error}", exc_info=True)


# Common CLI options as decorators
def common_options(func):
    """Add common CLI options to a command."""
    func = click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")(func)
    func = click.option("--quiet", "-q", is_flag=True, help="Suppress non-error output")(func)
    return func


def sweep_options(func):
    """Add common sweep options to a command."""
    func = click.option(
        "--config",
        "-c",
        type=click.Path(exists=True, path_type=Path),
        default=Path("sweeps/sweep.yaml"),
        help="Path to sweep configuration file",
    )(func)
    func = click.option(
        "--dry-run", is_flag=True, help="Show what would be executed without running"
    )(func)
    func = click.option("--max-runs", type=int, help="Maximum number of runs to execute")(func)
    func = click.option("--walltime", help="Job walltime (overrides config default)")(func)
    func = click.option("--group", help="W&B group name for this sweep")(func)
    return func
