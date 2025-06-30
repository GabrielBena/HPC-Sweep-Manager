"""Configuration CLI commands."""

from datetime import datetime
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.syntax import Syntax
from rich.table import Table

from ..config.hsm import HSMConfig
from ..config.sweep import SweepConfig
from ..config.validation import HSMConfigValidator, SweepConfigValidator
from ..utils.logging import get_logger
from ..utils.paths import PathDetector


@click.group("config")
@click.pass_context
def config_cmd(ctx):
    """Configuration management commands."""
    pass


@config_cmd.command("init")
@click.option("--project-root", type=click.Path(path_type=Path), help="Project root directory")
@click.option("--interactive", "-i", is_flag=True, help="Interactive configuration setup")
@click.option("--force", "-f", is_flag=True, help="Overwrite existing configuration")
@click.pass_context
def init_cmd(ctx, project_root: Optional[Path], interactive: bool, force: bool):
    """Initialize HSM configuration with project detection."""
    console: Console = ctx.obj["console"]
    logger = get_logger()

    try:
        # Determine project root
        if project_root is None:
            project_root = Path.cwd()

        project_root = project_root.resolve()
        config_file = project_root / "hsm_config.yaml"

        # Display header
        console.print(
            Panel.fit(
                "[bold blue]HPC Sweep Manager v2 - Project Initialization[/bold blue]",
                border_style="blue",
            )
        )
        console.print(f"[green]Initializing project at: {project_root}[/green]")

        # Check if config already exists
        if config_file.exists() and not force:
            console.print(f"[yellow]Configuration already exists at {config_file}[/yellow]")
            console.print("Use --force to overwrite or --interactive to modify")
            return

        # Initialize path detector and scan project
        console.print("\n[yellow]Scanning project structure...[/yellow]")
        detector = PathDetector(project_root)
        project_info = detector.get_project_info()
        issues = detector.validate_paths()

        # Display detected information
        _display_project_info(project_info, console)

        if issues:
            console.print("\n[red]⚠️  Issues detected:[/red]")
            for issue in issues:
                console.print(f"  - {issue}")

        # Get configuration
        if interactive:
            config_data = _interactive_config_setup(project_info, console)
        else:
            config_data = _auto_configuration(project_info)

        # Create HSM configuration
        hsm_config = HSMConfig.from_dict(config_data)
        hsm_config.save(config_file)

        # Create sweep infrastructure
        console.print("\n[yellow]Creating sweep infrastructure...[/yellow]")
        success = _create_sweep_infrastructure(project_root, config_data, console, logger)

        if success:
            console.print(f"\n[green]✅ HSM configuration initialized at {config_file}[/green]")
            _display_next_steps(console)
        else:
            console.print("\n[red]❌ Project initialization failed![/red]")

    except Exception as e:
        logger.error(f"Configuration initialization failed: {e}")
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


@config_cmd.command("validate")
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    help="HSM config file to validate",
)
@click.option(
    "--sweep-config",
    "-s",
    type=click.Path(exists=True, path_type=Path),
    help="Sweep config file to validate",
)
@click.pass_context
def validate_cmd(ctx, config: Optional[Path], sweep_config: Optional[Path]):
    """Validate configuration files."""
    console: Console = ctx.obj["console"]
    logger = get_logger()

    try:
        all_valid = True

        # Validate HSM config
        if config:
            console.print(f"[cyan]Validating HSM config: {config}[/cyan]")
            hsm_config = HSMConfig.load(config)
            if hsm_config is None:
                console.print(f"[red]Could not load HSM config from {config}[/red]")
                raise click.Abort()
            validator = HSMConfigValidator(hsm_config)
            result = validator.validate()
            _display_validation_result(console, "HSM Config", result)
            all_valid = all_valid and result.is_valid

        # Validate sweep config
        if sweep_config:
            console.print(f"[cyan]Validating sweep config: {sweep_config}[/cyan]")
            sweep_cfg = SweepConfig.from_yaml(sweep_config)
            validator = SweepConfigValidator()
            result = validator.validate(sweep_cfg)
            _display_validation_result(console, "Sweep Config", result)
            all_valid = all_valid and result.is_valid

        # Auto-discover and validate if no specific files provided
        if not config and not sweep_config:
            console.print("[cyan]Auto-discovering configuration files...[/cyan]")

            # Try to find and validate HSM config
            hsm_config_path = Path("hsm_config.yaml")
            if hsm_config_path.exists():
                console.print(f"[cyan]Found HSM config: {hsm_config_path}[/cyan]")
                hsm_config = HSMConfig.load(hsm_config_path)
                if hsm_config is None:
                    console.print(f"[red]Could not load HSM config from {hsm_config_path}[/red]")
                    raise click.Abort()
                validator = HSMConfigValidator(hsm_config)
                result = validator.validate()
                _display_validation_result(console, "HSM Config", result)
                all_valid = all_valid and result.is_valid

            # Try to find and validate sweep config
            sweep_config_path = Path("sweeps/sweep_config.yaml")
            if sweep_config_path.exists():
                console.print(f"[cyan]Found sweep config: {sweep_config_path}[/cyan]")
                sweep_cfg = SweepConfig.from_yaml(sweep_config_path)
                validator = SweepConfigValidator()
                result = validator.validate(sweep_cfg)
                _display_validation_result(console, "Sweep Config", result)
                all_valid = all_valid and result.is_valid

            if not hsm_config_path.exists() and not sweep_config_path.exists():
                console.print("[yellow]No configuration files found[/yellow]")
                console.print("Run 'hsm config init' to create initial configuration")

        if all_valid:
            console.print("[green]✓ All configurations are valid[/green]")
        else:
            console.print("[red]✗ Some configurations have issues[/red]")
            raise click.Abort()

    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


@config_cmd.command("show")
@click.option(
    "--config", "-c", type=click.Path(exists=True, path_type=Path), help="HSM config file to show"
)
@click.option("--format", type=click.Choice(["yaml", "json"]), default="yaml", help="Output format")
@click.pass_context
def show_cmd(ctx, config: Optional[Path], format: str):
    """Show current configuration."""
    console: Console = ctx.obj["console"]

    try:
        # Load configuration
        hsm_config = HSMConfig.load(config) if config else HSMConfig.load()

        if hsm_config is None:
            console.print("[red]Could not load HSM configuration[/red]")
            console.print("Run 'hsm config init' to create a configuration file")
            raise click.Abort()

        # Display configuration
        if format == "yaml":
            import yaml

            config_dict = hsm_config.to_dict()
            config_text = yaml.dump(config_dict, default_flow_style=False, indent=2)
            syntax = Syntax(config_text, "yaml", theme="monokai", line_numbers=True)
            console.print(syntax)
        elif format == "json":
            import json

            config_dict = hsm_config.to_dict()
            config_text = json.dumps(config_dict, indent=2)
            syntax = Syntax(config_text, "json", theme="monokai", line_numbers=True)
            console.print(syntax)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()


# Helper functions


def _display_project_info(info: dict, console: Console):
    """Display detected project information."""
    table = Table(title="Detected Project Information")
    table.add_column("Component", style="cyan")
    table.add_column("Status", style="magenta")
    table.add_column("Path/Value", style="green")

    # Core components
    table.add_row(
        "Project Name",
        "✅ Detected",
        info["project_name"],
    )

    table.add_row(
        "Config Directory",
        "✅ Found" if info["config_dir"] else "❌ Not found",
        str(info["config_dir"]) if info["config_dir"] else "None",
    )

    table.add_row(
        "Training Script",
        "✅ Found" if info["training_script"] else "❌ Not found",
        str(info["training_script"]) if info["training_script"] else "None",
    )

    table.add_row(
        "Python Interpreter",
        "✅ Found" if info["python_interpreter"] else "❌ Not found",
        str(info["python_interpreter"]) if info["python_interpreter"] else "None",
    )

    table.add_row(
        "Conda Environment",
        "✅ Active" if info["conda_env"] else "❌ None",
        info["conda_env"] or "None",
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


def _interactive_config_setup(project_info: dict, console: Console) -> dict:
    """Interactive configuration setup with intelligent defaults."""
    console.print("\n[bold]Interactive Configuration[/bold]")
    config_data = {}

    # Project settings
    config_data["project"] = {}
    config_data["project"]["name"] = Prompt.ask(
        "Project name", default=project_info["project_name"]
    )

    # Paths
    config_data["paths"] = {}

    # Python interpreter
    if project_info["python_interpreter"]:
        use_detected = Confirm.ask(
            f"Use detected Python interpreter: {project_info['python_interpreter']}?",
            default=True,
        )
        if use_detected:
            config_data["paths"]["python_interpreter"] = str(project_info["python_interpreter"])
        else:
            config_data["paths"]["python_interpreter"] = Prompt.ask("Python interpreter path")
    else:
        config_data["paths"]["python_interpreter"] = Prompt.ask(
            "Python interpreter path", default="python"
        )

    # Training script
    if project_info["training_script"]:
        use_detected = Confirm.ask(
            f"Use detected training script: {project_info['training_script']}?",
            default=True,
        )
        if use_detected:
            config_data["paths"]["training_script"] = str(project_info["training_script"])
        else:
            config_data["paths"]["training_script"] = Prompt.ask("Training script path")
    else:
        config_data["paths"]["training_script"] = Prompt.ask(
            "Training script path", default="scripts/train.py"
        )

    # Conda environment
    if project_info["conda_env"]:
        config_data["paths"]["conda_env"] = project_info["conda_env"]
    else:
        config_data["paths"]["conda_env"] = (
            Prompt.ask("Conda environment (optional)", default="") or None
        )

    # Project root
    config_data["paths"]["project_root"] = str(project_info["project_root"])

    # Compute settings
    config_data["compute"] = {}
    config_data["compute"]["max_concurrent_local"] = int(
        Prompt.ask("Max concurrent local jobs", default="4")
    )
    config_data["compute"]["max_concurrent_remote"] = int(
        Prompt.ask("Max concurrent remote jobs", default="10")
    )

    # Handle timeout - default to None (no timeout)
    timeout_input = Prompt.ask(
        "Default job timeout in seconds (or press enter for no timeout)", default=""
    )
    if timeout_input.strip() and timeout_input.strip().lower() not in ["null", "none", ""]:
        config_data["compute"]["default_timeout"] = int(timeout_input)
    else:
        config_data["compute"]["default_timeout"] = None

    # HPC settings
    config_data["hpc"] = {}
    config_data["hpc"]["system"] = project_info["hpc_system"]

    if project_info["hpc_system"] != "local":
        # Set default resources based on HPC system
        detector = PathDetector(project_info["project_root"])
        default_resources = detector.get_default_resources(project_info["hpc_system"])

        config_data["hpc"]["default_resources"] = Prompt.ask(
            "Default job resources", default=default_resources
        )
        config_data["hpc"]["default_walltime"] = Prompt.ask(
            "Default job walltime", default="23:59:59"
        )
        config_data["hpc"]["default_queue"] = (
            Prompt.ask("Default queue (optional)", default="") or None
        )

    # Experiment tracking
    config_data["experiment_tracking"] = {}
    if Confirm.ask("Configure Weights & Biases integration?", default=False):
        config_data["experiment_tracking"]["wandb_enabled"] = True
        config_data["experiment_tracking"]["wandb_project"] = Prompt.ask(
            "W&B project name", default=config_data["project"]["name"]
        )
        config_data["experiment_tracking"]["wandb_entity"] = (
            Prompt.ask("W&B entity (optional)", default="") or None
        )
    else:
        config_data["experiment_tracking"]["wandb_enabled"] = False

    # Logging
    config_data["logging"] = {
        "level": "INFO",
        "console_logging": True,
        "file_logging": True,
    }

    return config_data


def _auto_configuration(project_info: dict) -> dict:
    """Automatic configuration based on detected information."""
    detector = PathDetector(project_info["project_root"])

    config_data = {
        "project": {
            "name": project_info["project_name"],
        },
        "paths": {
            "python_interpreter": str(project_info["python_interpreter"])
            if project_info["python_interpreter"]
            else "python",
            "training_script": str(project_info["training_script"])
            if project_info["training_script"]
            else "scripts/train.py",
            "project_root": str(project_info["project_root"]),
            "conda_env": project_info["conda_env"],
        },
        "compute": {
            "max_concurrent_local": 4,
            "max_concurrent_remote": 10,
            "default_timeout": None,  # No timeout by default
            "health_check_interval": 300,
            "result_collection_interval": 60,
            "retry_attempts": 3,
            "retry_delay": 60,
        },
        "hpc": {
            "system": project_info["hpc_system"],
            "default_walltime": "23:59:59",
            "max_array_size": None,
            "module_commands": [],
            "default_queue": None,
        },
        "experiment_tracking": {
            "wandb_enabled": False,
            "wandb_project": None,
            "wandb_entity": None,
            "mlflow_enabled": False,
            "mlflow_tracking_uri": None,
            "tensorboard_enabled": False,
        },
        "logging": {
            "level": "INFO",
            "console_logging": True,
            "file_logging": True,
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "max_log_size": "100MB",
            "backup_count": 5,
        },
    }

    # Set default resources based on HPC system
    if project_info["hpc_system"] != "local":
        config_data["hpc"]["default_resources"] = detector.get_default_resources(
            project_info["hpc_system"]
        )

    return config_data


def _create_sweep_infrastructure(
    project_root: Path, config_data: dict, console: Console, logger
) -> bool:
    """Create sweep directories and configuration files."""
    try:
        # Create sweep directory structure
        sweeps_dir = project_root / "sweeps"
        sweeps_dir.mkdir(exist_ok=True)

        outputs_dir = sweeps_dir / "outputs"
        outputs_dir.mkdir(exist_ok=True)

        logs_dir = sweeps_dir / "logs"
        logs_dir.mkdir(exist_ok=True)

        console.print("  ✅ Created sweep directory structure")

        # Create example sweep configuration
        example_sweep_path = sweeps_dir / "sweep_config.yaml"
        if not example_sweep_path.exists():
            example_sweep = {
                "defaults": [{"override": "hydra/launcher: basic"}],
                "sweep": {
                    "grid": {
                        "seed": [1, 2, 3, 4, 5],
                        "learning_rate": [0.001, 0.01, 0.1],
                        "batch_size": [32, 64],
                    }
                },
                "metadata": {
                    "description": "Example hyperparameter sweep",
                    "tags": ["example", "baseline"],
                },
            }

            import yaml

            with open(example_sweep_path, "w") as f:
                yaml.dump(example_sweep, f, default_flow_style=False, indent=2)

            console.print("  ✅ Created example sweep configuration")

        # Create README
        readme_path = sweeps_dir / "README.md"
        if not readme_path.exists():
            readme_content = f"""# {config_data["project"]["name"]} - HPC Sweeps

This directory contains HPC sweep configurations and outputs for the {config_data["project"]["name"]} project.

## Files

- `sweep_config.yaml` - Main sweep configuration (Hydra compatible)
- `outputs/` - Sweep results and logs  
- `logs/` - System logs

## Usage

1. **Count combinations**: `hsm sweep run --config sweeps/sweep_config.yaml --count-only`
2. **Dry run**: `hsm sweep run --config sweeps/sweep_config.yaml --dry-run`
3. **Run locally**: `hsm sweep run --config sweeps/sweep_config.yaml --sources local`
4. **Run on HPC**: `hsm sweep run --config sweeps/sweep_config.yaml --sources hpc`

## Configuration Format

The sweep configuration uses Hydra-compatible format:

```yaml
defaults:
  - override hydra/launcher: basic

sweep:
  grid:  # Parameters to combine in all possible ways
    param1: [value1, value2, value3]
    param2: [valueA, valueB]
    
metadata:
  description: "Your sweep description"
  tags: ["tag1", "tag2"]
```

Edit `sweep_config.yaml` or create new sweep configurations. See the HSM documentation for details.

Generated by HSM v2 on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
"""

            with open(readme_path, "w") as f:
                f.write(readme_content)

            console.print("  ✅ Created README documentation")

        logger.info(f"Successfully created sweep infrastructure at {project_root}")
        return True

    except Exception as e:
        console.print(f"  ❌ Error creating infrastructure: {e}")
        logger.error(f"Failed to create sweep infrastructure: {e}")
        return False


def _display_next_steps(console: Console):
    """Display next steps after initialization."""
    next_steps = """
[bold]Next Steps:[/bold]

1. **Review configuration**: Edit `hsm_config.yaml` if needed
2. **Create sweep config**: Edit `sweeps/sweep_config.yaml` for your parameters
3. **Test the setup**: Run `hsm sweep run --config sweeps/sweep_config.yaml --dry-run`
4. **Submit jobs**: Run `hsm sweep run --config sweeps/sweep_config.yaml --sources local`

[bold]Useful Commands:[/bold]
- `hsm config validate` - Validate configuration files
- `hsm config show` - Display current configuration  
- `hsm sweep run --help` - Show sweep execution options
- `hsm monitor watch` - Monitor sweep progress
"""

    console.print(Panel(next_steps, title="Getting Started", border_style="green"))


def _display_validation_result(console: Console, config_type: str, result):
    """Display validation results."""
    if result.is_valid:
        console.print(f"[green]✓ {config_type} is valid[/green]")
    else:
        console.print(f"[red]✗ {config_type} has issues[/red]")

    # Show errors
    if result.errors:
        console.print(f"[red]Errors in {config_type}:[/red]")
        for error in result.errors:
            console.print(f"  • {error.message}")
            if error.suggestion:
                console.print(f"    → {error.suggestion}")

    # Show warnings
    if result.warnings:
        console.print(f"[yellow]Warnings in {config_type}:[/yellow]")
        for warning in result.warnings:
            console.print(f"  • {warning.message}")
            if warning.suggestion:
                console.print(f"    → {warning.suggestion}")

    # Show info messages
    if result.info:
        console.print(f"[blue]Info for {config_type}:[/blue]")
        for info in result.info:
            console.print(f"  • {info.message}")
            if info.suggestion:
                console.print(f"    → {info.suggestion}")
