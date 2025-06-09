"""Project initialization CLI commands."""

from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt, Confirm
from rich.panel import Panel
import logging
import yaml
from datetime import datetime
from typing import Dict, Any

from ..core.path_detector import PathDetector
from ..core.job_manager import JobManager


def init_project(
    project_path: Path, interactive: bool, console: Console, logger: logging.Logger
):
    """Initialize sweep infrastructure in a project."""
    console.print(
        Panel.fit(
            "[bold blue]HPC Sweep Manager - Project Initialization[/bold blue]",
            border_style="blue",
        )
    )

    project_path = project_path.resolve()
    console.print(f"[green]Initializing project at: {project_path}[/green]")

    # Initialize path detector
    detector = PathDetector(project_path)

    # Get project information
    console.print("\n[yellow]Scanning project structure...[/yellow]")
    project_info = detector.get_project_info()
    issues = detector.validate_paths()

    # Display detected information
    _display_project_info(project_info, console)

    if issues:
        console.print("\n[red]⚠️  Issues detected:[/red]")
        for issue in issues:
            console.print(f"  - {issue}")

    # Interactive configuration if requested
    config = {}
    if interactive:
        console.print("\n[bold]Interactive Configuration[/bold]")
        config = _interactive_configuration(project_info, console)
    else:
        config = _auto_configuration(project_info)

    # Create sweep infrastructure
    console.print(f"\n[yellow]Creating sweep infrastructure...[/yellow]")
    success = _create_sweep_infrastructure(project_path, config, console, logger)

    if success:
        console.print(
            "\n[green]✅ Project initialization completed successfully![/green]"
        )
        _display_next_steps(console)
    else:
        console.print("\n[red]❌ Project initialization failed![/red]")


def _display_project_info(info: Dict[str, Any], console: Console):
    """Display detected project information."""
    table = Table(title="Detected Project Information")
    table.add_column("Component", style="cyan")
    table.add_column("Status", style="magenta")
    table.add_column("Path/Value", style="green")

    # Core components
    table.add_row(
        "Config Directory",
        "✅ Found" if info["config_dir"] else "❌ Not found",
        str(info["config_dir"]) if info["config_dir"] else "None",
    )

    table.add_row(
        "Training Script",
        "✅ Found" if info["train_script"] else "❌ Not found",
        str(info["train_script"]) if info["train_script"] else "None",
    )

    table.add_row(
        "Python Interpreter",
        "✅ Found" if info["python_path"] else "❌ Not found",
        str(info["python_path"]) if info["python_path"] else "None",
    )

    table.add_row("HPC System", "✅ Detected", info["hpc_system"].upper())
    table.add_row("Output Directory", "📁 Available", str(info["output_dir"]))

    # Project features
    table.add_row("Git Repository", "✅ Yes" if info["has_git"] else "❌ No", "")
    table.add_row(
        "Dependencies",
        "✅ Yes"
        if any([info["has_requirements"], info["has_pyproject"], info["has_conda_env"]])
        else "❌ No",
        "",
    )

    console.print(table)


def _interactive_configuration(
    project_info: Dict[str, Any], console: Console
) -> Dict[str, Any]:
    """Interactive configuration prompts."""
    config = {}

    # Project name
    default_name = project_info["project_root"].name
    config["project_name"] = Prompt.ask("Project name", default=default_name)

    # Python interpreter
    if project_info["python_path"]:
        use_detected = Confirm.ask(
            f"Use detected Python interpreter: {project_info['python_path']}?",
            default=True,
        )
        if use_detected:
            config["python_path"] = str(project_info["python_path"])
        else:
            config["python_path"] = Prompt.ask("Python interpreter path")
    else:
        config["python_path"] = Prompt.ask("Python interpreter path", default="python")

    # Training script
    if project_info["train_script"]:
        use_detected = Confirm.ask(
            f"Use detected training script: {project_info['train_script']}?",
            default=True,
        )
        if use_detected:
            config["train_script"] = str(project_info["train_script"])
        else:
            config["train_script"] = Prompt.ask("Training script path")
    else:
        config["train_script"] = Prompt.ask(
            "Training script path", default="scripts/train.py"
        )

    # Config directory
    if project_info["config_dir"]:
        config["config_dir"] = str(project_info["config_dir"])
    else:
        config["config_dir"] = Prompt.ask("Hydra config directory", default="configs")

    # HPC settings
    config["hpc_system"] = project_info["hpc_system"]

    # Resource defaults
    if project_info["hpc_system"] == "pbs":
        default_resources = "select=1:ncpus=4:mem=16gb"
    else:  # slurm
        default_resources = "--nodes=1 --ntasks=4 --mem=16G"

    config["default_walltime"] = Prompt.ask("Default job walltime", default="04:00:00")

    config["default_resources"] = Prompt.ask(
        "Default job resources", default=default_resources
    )

    # W&B settings
    if Confirm.ask("Configure Weights & Biases integration?", default=True):
        config["wandb_project"] = Prompt.ask(
            "W&B project name", default=config["project_name"]
        )
        config["wandb_entity"] = Prompt.ask("W&B entity (optional)", default="")

    return config


def _auto_configuration(project_info: Dict[str, Any]) -> Dict[str, Any]:
    """Automatic configuration based on detected information."""
    config = {
        "project_name": project_info["project_root"].name,
        "python_path": str(project_info["python_path"])
        if project_info["python_path"]
        else "python",
        "train_script": str(project_info["train_script"])
        if project_info["train_script"]
        else "scripts/train.py",
        "config_dir": str(project_info["config_dir"])
        if project_info["config_dir"]
        else "configs",
        "hpc_system": project_info["hpc_system"],
        "default_walltime": "04:00:00",
        "wandb_project": project_info["project_root"].name,
    }

    # Set default resources based on HPC system
    if project_info["hpc_system"] == "pbs":
        config["default_resources"] = "select=1:ncpus=4:mem=16gb"
    else:  # slurm
        config["default_resources"] = "--nodes=1 --ntasks=4 --mem=16G"

    return config


def _create_sweep_infrastructure(
    project_path: Path, config: Dict[str, Any], console: Console, logger: logging.Logger
) -> bool:
    """Create sweep directories and configuration files."""
    try:
        # Create sweep directory structure
        sweeps_dir = project_path / "sweeps"
        sweeps_dir.mkdir(exist_ok=True)

        outputs_dir = sweeps_dir / "outputs"
        outputs_dir.mkdir(exist_ok=True)

        logs_dir = sweeps_dir / "logs"
        logs_dir.mkdir(exist_ok=True)

        console.print("  ✅ Created sweep directory structure")

        # Create HSM configuration file
        hsm_config_path = sweeps_dir / "hsm_config.yaml"
        hsm_config = {
            "project": {
                "name": config["project_name"],
                "root": str(project_path),
            },
            "paths": {
                "python_interpreter": config["python_path"],
                "train_script": config["train_script"],
                "config_dir": config["config_dir"],
                "output_dir": "outputs",
            },
            "hpc": {
                "system": config["hpc_system"],
                "default_walltime": config["default_walltime"],
                "default_resources": config["default_resources"],
                "max_array_size": 10000,
            },
            "metadata": {
                "created_by": "HSM (HPC Sweep Manager)",
                "created_at": datetime.now().isoformat(),
                "version": "1.0.0",
            },
        }

        # Add W&B config if specified
        if config.get("wandb_project"):
            hsm_config["wandb"] = {
                "project": config["wandb_project"],
                "entity": config.get("wandb_entity", ""),
            }

        with open(hsm_config_path, "w") as f:
            yaml.dump(hsm_config, f, default_flow_style=False, indent=2)

        console.print("  ✅ Created HSM configuration file")

        # Create example sweep configuration
        example_sweep_path = sweeps_dir / "example_sweep.yaml"
        example_sweep = {
            "defaults": ["override hydra/launcher: basic"],
            "sweep": {
                "grid": {
                    "seed": [1, 2, 3, 4, 5],
                    "model.lr": [0.001, 0.01, 0.1],
                    "data.batch_size": [32, 64],
                }
            },
            "metadata": {
                "description": "Example hyperparameter sweep",
                "wandb_project": config.get("wandb_project", config["project_name"]),
                "tags": ["example", "baseline"],
            },
        }

        with open(example_sweep_path, "w") as f:
            yaml.dump(example_sweep, f, default_flow_style=False, indent=2)

        console.print("  ✅ Created example sweep configuration")

        # Create README
        readme_path = sweeps_dir / "README.md"
        readme_content = f"""# {config["project_name"]} - HPC Sweeps

This directory contains HPC sweep configurations and outputs for the {config["project_name"]} project.

## Files

- `hsm_config.yaml` - HSM project configuration
- `example_sweep.yaml` - Example sweep configuration
- `outputs/` - Sweep results and logs
- `logs/` - HPC job logs

## Usage

1. **Count combinations**: `hsm sweep --config example_sweep.yaml --count`
2. **Dry run**: `hsm sweep --config example_sweep.yaml --dry-run`
3. **Submit individual jobs**: `hsm sweep --config example_sweep.yaml --individual`
4. **Submit array job**: `hsm sweep --config example_sweep.yaml --array`

## Configuration

Edit `example_sweep.yaml` or create new sweep configurations. See the HSM documentation for details.

Generated by HSM on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
"""

        with open(readme_path, "w") as f:
            f.write(readme_content)

        console.print("  ✅ Created README documentation")

        logger.info(f"Successfully initialized sweep infrastructure at {project_path}")
        return True

    except Exception as e:
        console.print(f"  ❌ Error creating infrastructure: {e}")
        logger.error(f"Failed to create sweep infrastructure: {e}")
        return False


def _display_next_steps(console: Console):
    """Display next steps after initialization."""
    next_steps = """
[bold]Next Steps:[/bold]

1. **Review configuration**: Edit `sweeps/hsm_config.yaml` if needed
2. **Create sweep config**: Use `hsm configure` or edit `sweeps/example_sweep.yaml`
3. **Test the setup**: Run `hsm sweep --config sweeps/example_sweep.yaml --dry-run`
4. **Submit jobs**: Run `hsm sweep --config sweeps/example_sweep.yaml --mode array`

[bold]Useful Commands:[/bold]
- `hsm sweep --help` - Show sweep options
- `hsm configure` - Interactive sweep configuration builder
- `hsm monitor` - Monitor active sweeps
- `hsm --help` - Show all available commands
"""

    console.print(Panel(next_steps, title="Getting Started", border_style="green"))
