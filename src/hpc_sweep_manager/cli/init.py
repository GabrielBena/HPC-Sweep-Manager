"""Project initialization CLI commands."""

from datetime import datetime
import logging
from pathlib import Path
from typing import Any, Dict

import click
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table
import yaml

from ..core.common.path_detector import PathDetector
from .common import common_options


def init_project(project_path: Path, interactive: bool, console: Console, logger: logging.Logger):
    """Initialize sweep infrastructure in a project."""
    console.print(
        Panel.fit(
            "[bold blue]HPC Sweep Manager - Project Initialization[/bold blue]",
            border_style="blue",
        )
    )

    project_path = project_path.resolve()
    console.print(f"[green]Initializing project at: {project_path}[/green]")

    # Check for existing old-style config (migration)
    old_config_path = project_path / "sweeps" / "hsm_config.yaml"
    migrating = False

    if old_config_path.exists():
        console.print("\n[yellow]📦 Found existing config at sweeps/hsm_config.yaml[/yellow]")
        console.print("[cyan]Migrating to new .hsm/ structure...[/cyan]\n")

        try:
            # Load existing config to preserve settings
            with open(old_config_path) as f:
                existing_config = yaml.safe_load(f)

            config = _extract_config_from_existing(existing_config, project_path)
            migrating = True

        except Exception as e:
            console.print(f"[yellow]Warning: Could not read old config: {e}[/yellow]")
            console.print("[yellow]Proceeding with fresh initialization...[/yellow]\n")
            migrating = False

    if not migrating:
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
    console.print("\n[yellow]Creating sweep infrastructure...[/yellow]")
    success = _create_sweep_infrastructure(project_path, config, console, logger)

    if success:
        # If we migrated, offer to clean up old file
        if migrating and old_config_path.exists():
            new_config_path = project_path / ".hsm" / "config.yaml"
            if new_config_path.exists():
                console.print(
                    "\n[green]✅ Successfully migrated config to .hsm/config.yaml[/green]"
                )
                if Confirm.ask("Remove old config at sweeps/hsm_config.yaml?", default=False):
                    old_config_path.unlink()
                    console.print("  ✅ Removed old config file")
                else:
                    console.print(
                        "  [dim]Kept old config (you can safely delete it manually)[/dim]"
                    )

        console.print("\n[green]✅ Project initialization completed successfully![/green]")
        _display_next_steps(console)
    else:
        console.print("\n[red]❌ Project initialization failed![/red]")


def _extract_config_from_existing(
    existing_config: Dict[str, Any], project_path: Path
) -> Dict[str, Any]:
    """Extract configuration from existing hsm_config.yaml for migration."""
    config = {}

    # Extract project settings
    project_section = existing_config.get("project", {})
    config["project_name"] = project_section.get("name", project_path.name)

    # Extract path settings
    paths_section = existing_config.get("paths", {})
    config["python_path"] = paths_section.get("python_interpreter", "python")
    config["train_script"] = paths_section.get("train_script", "scripts/train.py")
    config["config_dir"] = paths_section.get("config_dir", "configs")

    # Extract HPC settings
    hpc_section = existing_config.get("hpc", {})
    config["hpc_system"] = hpc_section.get("system", "pbs")
    config["default_walltime"] = hpc_section.get("default_walltime", "04:00:00")
    config["default_resources"] = hpc_section.get("default_resources", "select=1:ncpus=4:mem=16gb")

    # Extract W&B settings if present
    wandb_section = existing_config.get("wandb", {})
    if wandb_section:
        config["wandb_project"] = wandb_section.get("project", config["project_name"])
        config["wandb_entity"] = wandb_section.get("entity", "")

    return config


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


def _interactive_configuration(project_info: Dict[str, Any], console: Console) -> Dict[str, Any]:
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
        config["train_script"] = Prompt.ask("Training script path", default="scripts/train.py")

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

    config["default_resources"] = Prompt.ask("Default job resources", default=default_resources)

    # W&B settings
    if Confirm.ask("Configure Weights & Biases integration?", default=True):
        config["wandb_project"] = Prompt.ask("W&B project name", default=config["project_name"])
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
        "config_dir": str(project_info["config_dir"]) if project_info["config_dir"] else "configs",
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
        # Create .hsm directory structure (NEW)
        hsm_dir = project_path / ".hsm"
        hsm_dir.mkdir(exist_ok=True)

        cache_dir = hsm_dir / "cache"
        cache_dir.mkdir(exist_ok=True)

        logs_dir = hsm_dir / "logs"
        logs_dir.mkdir(exist_ok=True)

        console.print("  ✅ Created .hsm/ directory structure")

        # Create sweep directory structure
        sweeps_dir = project_path / "sweeps"
        sweeps_dir.mkdir(exist_ok=True)

        outputs_dir = sweeps_dir / "outputs"
        outputs_dir.mkdir(exist_ok=True)

        sweep_logs_dir = sweeps_dir / "logs"
        sweep_logs_dir.mkdir(exist_ok=True)

        console.print("  ✅ Created sweeps/ directory structure")

        # Create HSM configuration file (in new location)
        hsm_config_path = hsm_dir / "config.yaml"
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

        console.print(
            f"  ✅ Created HSM configuration file: {hsm_config_path.relative_to(project_path)}"
        )

        # Create sync configuration template
        sync_config_path = hsm_dir / "sync_config.yaml"
        from ..sync.config import SyncConfig

        SyncConfig.create_template(sync_config_path, config["project_name"])
        rel_path = sync_config_path.relative_to(project_path)
        console.print(f"  ✅ Created sync configuration template: {rel_path}")

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
        project_name = config["project_name"]
        readme_content = f"""# {project_name} - HPC Sweeps

This directory contains HPC sweep configurations and outputs for the {project_name} project.

## Directory Structure

```
.hsm/
├── config.yaml          # HSM project configuration
├── sync_config.yaml     # Sync targets configuration
├── cache/               # Cached metadata
└── logs/                # HSM operation logs

sweeps/
├── example_sweep.yaml   # Example sweep configuration
├── outputs/             # Sweep results and logs
└── logs/                # HPC job logs
```

## Usage

### Running Sweeps

1. **Count combinations**: `hsm sweep --config sweeps/example_sweep.yaml --count`
2. **Dry run**: `hsm sweep --config sweeps/example_sweep.yaml --dry-run`
3. **Submit individual jobs**: `hsm sweep --config sweeps/example_sweep.yaml --individual`
4. **Submit array job**: `hsm sweep --config sweeps/example_sweep.yaml --array`

### Syncing Results

1. **Configure sync targets**: Edit `.hsm/sync_config.yaml` with your destination machines
2. **Sync sweep results**: `hsm sync <sweep-id> --target desktop`
3. **Dry run**: `hsm sync <sweep-id> --target desktop --dry-run`

## Configuration

- **HSM config**: `.hsm/config.yaml` - Project settings, paths, HPC resources
- **Sync config**: `.hsm/sync_config.yaml` - Remote sync destinations
- **Sweep configs**: Create YAML files in `sweeps/` directory

See the HSM documentation for details.

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

1. **Review HSM configuration**: Edit `.hsm/config.yaml` if needed
2. **Configure sync targets**: Edit `.hsm/sync_config.yaml` with your destination machines
3. **Create sweep config**: Use `hsm configure` or edit `sweeps/example_sweep.yaml`
4. **Test the setup**: Run `hsm sweep --config sweeps/example_sweep.yaml --dry-run`
5. **Submit jobs**: Run `hsm sweep --config sweeps/example_sweep.yaml --mode array`
6. **Sync results**: Run `hsm sync <sweep-id> --target <your-target>`

[bold]Useful Commands:[/bold]
- `hsm sweep --help` - Show sweep options
- `hsm sync --help` - Show sync options
- `hsm configure` - Interactive sweep configuration builder
- `hsm monitor` - Monitor active sweeps
- `hsm --help` - Show all available commands
"""

    console.print(Panel(next_steps, title="Getting Started", border_style="green"))


# CLI command group structure
@click.group()
def setup():
    """Setup and configure HSM projects."""
    pass


@setup.command("init")
@click.option("--interactive", "-i", is_flag=True, help="Interactive setup mode")
@click.option("--project-root", type=click.Path(exists=True), help="Project root directory")
@common_options
@click.pass_context
def init_cmd(ctx, interactive: bool, project_root: str, verbose: bool, quiet: bool):
    """Initialize sweep infrastructure in a project."""
    project_path = Path(project_root) if project_root else Path.cwd()
    init_project(project_path, interactive, ctx.obj["console"], ctx.obj["logger"])


@setup.command("configure")
@click.option(
    "--from-file",
    type=click.Path(exists=True),
    help="Build from existing Hydra config file",
)
@click.option("--output", "-o", type=click.Path(), help="Output sweep config file")
@common_options
@click.pass_context
def configure_cmd(ctx, from_file: str, output: str, verbose: bool, quiet: bool):
    """Interactive sweep configuration builder."""
    from .configure import configure_sweep

    config_file = Path(from_file) if from_file else None
    output_file = Path(output) if output else None

    configure_sweep(config_file, output_file, ctx.obj["console"], ctx.obj["logger"])


if __name__ == "__main__":
    setup()
