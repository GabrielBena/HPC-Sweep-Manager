"""Interactive configuration CLI commands."""

from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt, Confirm, IntPrompt
from rich.panel import Panel
from rich.columns import Columns
from rich.text import Text
import logging
import yaml
from typing import Optional, Dict, Any, List, Set
from datetime import datetime
import re

from ..core.path_detector import PathDetector


def configure_sweep(
    config_file: Optional[Path],
    output_file: Optional[Path],
    console: Console,
    logger: logging.Logger,
):
    """Interactive sweep configuration builder."""
    console.print(
        Panel.fit(
            "[bold blue]HPC Sweep Manager - Interactive Configuration Builder[/bold blue]",
            border_style="blue",
        )
    )

    # Determine output file
    if not output_file:
        output_file = Path("sweeps/sweep.yaml")

    # Initialize path detector
    detector = PathDetector(Path.cwd())
    project_info = detector.get_project_info()

    # Show project info
    console.print(f"\n[green]Project: {project_info['project_root'].name}[/green]")
    if project_info["config_dir"]:
        console.print(f"[green]Config directory: {project_info['config_dir']}[/green]")
    else:
        console.print("[yellow]No Hydra config directory detected[/yellow]")

    # Scan for parameters
    console.print("\n[yellow]Scanning for parameters...[/yellow]")

    if config_file:
        # Build from specific config file
        console.print(f"Building from config file: {config_file}")
        if not config_file.exists():
            console.print(f"[red]Error: Config file not found: {config_file}[/red]")
            return
        parameters = _extract_parameters_from_file(config_file, console)
    elif project_info["config_dir"]:
        # Scan config directory
        parameters = _scan_config_directory(project_info["config_dir"], console)
    else:
        # Manual parameter entry
        console.print(
            "[yellow]No config files found. Using manual parameter entry.[/yellow]"
        )
        parameters = {}

    # Interactive parameter selection and configuration
    sweep_config = _interactive_parameter_selection(parameters, console)

    # Add metadata
    metadata = _configure_metadata(console)
    sweep_config["metadata"] = metadata

    # Save configuration
    _save_sweep_config(sweep_config, output_file, console, logger)

    # Show summary and next steps
    _show_configuration_summary(sweep_config, output_file, console)


def _scan_config_directory(config_dir: Path, console: Console) -> Dict[str, Any]:
    """Scan Hydra config directory for parameters."""
    parameters = {}

    # Find all YAML files
    yaml_files = list(config_dir.rglob("*.yaml")) + list(config_dir.rglob("*.yml"))

    console.print(f"Found {len(yaml_files)} config files")

    for yaml_file in yaml_files:
        try:
            with open(yaml_file, "r") as f:
                config = yaml.safe_load(f)

            if config:
                relative_path = yaml_file.relative_to(config_dir)
                file_params = _extract_parameters_from_config(
                    config, str(relative_path)
                )
                parameters.update(file_params)

        except Exception as e:
            console.print(f"[yellow]Warning: Could not parse {yaml_file}: {e}[/yellow]")

    return parameters


def _extract_parameters_from_file(
    config_file: Path, console: Console
) -> Dict[str, Any]:
    """Extract parameters from a specific config file."""
    try:
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)

        if config:
            return _extract_parameters_from_config(config, config_file.name)
        else:
            return {}

    except Exception as e:
        console.print(f"[red]Error parsing config file: {e}[/red]")
        return {}


def _extract_parameters_from_config(
    config: Dict[str, Any], source: str, prefix: str = ""
) -> Dict[str, Any]:
    """Extract parameters from a config dictionary."""
    parameters = {}

    for key, value in config.items():
        if prefix:
            full_key = f"{prefix}.{key}"
        else:
            full_key = key

        if isinstance(value, dict):
            # Recurse into nested dictionaries
            nested_params = _extract_parameters_from_config(value, source, full_key)
            parameters.update(nested_params)
        elif isinstance(value, (int, float, str, bool)) and not key.startswith("_"):
            # This is a potential parameter
            parameters[full_key] = {
                "type": type(value).__name__,
                "default": value,
                "source": source,
                "suggested_values": _suggest_parameter_values(key, value),
            }

    return parameters


def _suggest_parameter_values(param_name: str, default_value: Any) -> List[Any]:
    """Suggest parameter values based on name and default."""
    suggestions = [default_value]

    # Common patterns for different parameter types
    if "lr" in param_name.lower() or "learning_rate" in param_name.lower():
        suggestions = [0.001, 0.01, 0.1, 0.3]
    elif "batch_size" in param_name.lower():
        suggestions = [16, 32, 64, 128, 256]
    elif "hidden_size" in param_name.lower() or "dim" in param_name.lower():
        suggestions = [64, 128, 256, 512, 1024]
    elif "dropout" in param_name.lower():
        suggestions = [0.0, 0.1, 0.2, 0.3, 0.5]
    elif "seed" in param_name.lower():
        suggestions = [1, 2, 3, 4, 5, 42, 123]
    elif "epoch" in param_name.lower():
        suggestions = [10, 20, 50, 100]
    elif isinstance(default_value, bool):
        suggestions = [True, False]
    elif isinstance(default_value, int):
        # Suggest some variations around the default
        suggestions = [max(1, default_value // 2), default_value, default_value * 2]
    elif isinstance(default_value, float):
        # Suggest some variations around the default
        suggestions = [default_value / 10, default_value, default_value * 10]

    return list(set(suggestions))  # Remove duplicates


def _interactive_parameter_selection(
    parameters: Dict[str, Any], console: Console
) -> Dict[str, Any]:
    """Interactive parameter selection and sweep configuration."""

    if not parameters:
        return _manual_parameter_entry(console)

    # Display available parameters
    console.print(f"\n[bold]Found {len(parameters)} potential parameters:[/bold]")

    table = Table(title="Available Parameters")
    table.add_column("Parameter", style="cyan")
    table.add_column("Type", style="magenta")
    table.add_column("Default", style="green")
    table.add_column("Source", style="dim")

    for param_name, info in list(parameters.items())[:20]:  # Show first 20
        table.add_row(param_name, info["type"], str(info["default"]), info["source"])

    if len(parameters) > 20:
        table.add_row("...", "...", "...", f"and {len(parameters) - 20} more")

    console.print(table)

    # Parameter selection
    console.print(f"\n[bold]Select parameters for sweep:[/bold]")

    selected_params = {}
    grid_params = {}
    paired_groups = []

    while True:
        console.print(f"\n[yellow]Parameter selection menu:[/yellow]")
        console.print("1. Add grid parameter (all combinations)")
        console.print("2. Add paired parameters (vary together)")
        console.print("3. Review current selection")
        console.print("4. Finish configuration")

        choice = Prompt.ask("Choose option", choices=["1", "2", "3", "4"], default="1")

        if choice == "1":
            param = _select_grid_parameter(parameters, grid_params, console)
            if param:
                grid_params.update(param)

        elif choice == "2":
            group = _select_paired_parameters(parameters, console)
            if group:
                paired_groups.append(group)

        elif choice == "3":
            _review_selection(grid_params, paired_groups, console)

        elif choice == "4":
            if grid_params or paired_groups:
                break
            else:
                console.print("[yellow]Please select at least one parameter[/yellow]")

    # Build sweep configuration
    sweep_config = {"defaults": ["override hydra/launcher: basic"], "sweep": {}}

    if grid_params:
        sweep_config["sweep"]["grid"] = grid_params

    if paired_groups:
        sweep_config["sweep"]["paired"] = paired_groups

    return sweep_config


def _select_grid_parameter(
    parameters: Dict[str, Any], current_grid: Dict[str, Any], console: Console
) -> Optional[Dict[str, Any]]:
    """Select a parameter for grid search."""
    # Show available parameters
    available = {k: v for k, v in parameters.items() if k not in current_grid}

    if not available:
        console.print("[yellow]No more parameters available[/yellow]")
        return None

    # Parameter name selection
    param_names = list(available.keys())

    console.print(f"\n[yellow]Available parameters ({len(param_names)}):[/yellow]")
    for i, name in enumerate(param_names[:10], 1):
        console.print(
            f"  {i}. {name} ({available[name]['type']}) = {available[name]['default']}"
        )

    if len(param_names) > 10:
        console.print(f"  ... and {len(param_names) - 10} more")

    # Parameter selection
    param_name = Prompt.ask("Enter parameter name (or partial match)")

    # Find matching parameter
    matches = [name for name in param_names if param_name.lower() in name.lower()]

    if not matches:
        console.print(f"[red]No parameter found matching '{param_name}'[/red]")
        return None
    elif len(matches) > 1:
        console.print(f"Multiple matches found:")
        for i, match in enumerate(matches[:10], 1):
            console.print(f"  {i}. {match}")

        try:
            idx = IntPrompt.ask(
                "Select parameter",
                choices=[str(i) for i in range(1, min(11, len(matches) + 1))],
            )
            param_name = matches[idx - 1]
        except:
            return None
    else:
        param_name = matches[0]

    param_info = available[param_name]

    # Value selection
    console.print(f"\n[green]Configuring parameter: {param_name}[/green]")
    console.print(f"Type: {param_info['type']}, Default: {param_info['default']}")
    console.print(f"Suggested values: {param_info['suggested_values']}")

    use_suggested = Confirm.ask("Use suggested values?", default=True)

    if use_suggested:
        values = param_info["suggested_values"]
    else:
        values_str = Prompt.ask("Enter values (comma-separated)")
        try:
            # Parse values based on type
            if param_info["type"] == "int":
                values = [int(x.strip()) for x in values_str.split(",")]
            elif param_info["type"] == "float":
                values = [float(x.strip()) for x in values_str.split(",")]
            elif param_info["type"] == "bool":
                values = [
                    x.strip().lower() in ["true", "1", "yes"]
                    for x in values_str.split(",")
                ]
            else:
                values = [x.strip() for x in values_str.split(",")]
        except ValueError as e:
            console.print(f"[red]Error parsing values: {e}[/red]")
            return None

    return {param_name: values}


def _select_paired_parameters(
    parameters: Dict[str, Any], console: Console
) -> Optional[Dict[str, Any]]:
    """Select parameters that should vary together."""
    console.print(
        f"\n[yellow]Paired parameters vary together (same length required)[/yellow]"
    )

    group_name = Prompt.ask("Group name", default="paired_group")
    paired_params = {}

    while True:
        param_name = Prompt.ask("Parameter name (or 'done' to finish)")
        if param_name.lower() == "done":
            break

        if param_name not in parameters:
            # Try partial match
            matches = [
                name for name in parameters.keys() if param_name.lower() in name.lower()
            ]
            if matches:
                param_name = matches[0]
                console.print(f"Using: {param_name}")
            else:
                console.print(f"[red]Parameter not found: {param_name}[/red]")
                continue

        values_str = Prompt.ask("Enter values (comma-separated)")
        try:
            param_info = parameters[param_name]
            if param_info["type"] == "int":
                values = [int(x.strip()) for x in values_str.split(",")]
            elif param_info["type"] == "float":
                values = [float(x.strip()) for x in values_str.split(",")]
            elif param_info["type"] == "bool":
                values = [
                    x.strip().lower() in ["true", "1", "yes"]
                    for x in values_str.split(",")
                ]
            else:
                values = [x.strip() for x in values_str.split(",")]

            paired_params[param_name] = values
            console.print(f"Added {param_name} with {len(values)} values")

        except ValueError as e:
            console.print(f"[red]Error parsing values: {e}[/red]")

    if not paired_params:
        return None

    # Validate all parameters have same length
    lengths = [len(values) for values in paired_params.values()]
    if len(set(lengths)) > 1:
        console.print(
            f"[red]Error: All paired parameters must have same length. Found: {lengths}[/red]"
        )
        return None

    return {group_name: paired_params}


def _manual_parameter_entry(console: Console) -> Dict[str, Any]:
    """Manual parameter entry when no configs are found."""
    console.print(f"\n[yellow]Manual parameter entry:[/yellow]")

    sweep_config = {
        "defaults": ["override hydra/launcher: basic"],
        "sweep": {"grid": {}},
    }

    while True:
        param_name = Prompt.ask("Parameter name (or 'done' to finish)")
        if param_name.lower() == "done":
            break

        values_str = Prompt.ask("Values (comma-separated)")
        try:
            # Try to infer type and parse
            values = []
            for val_str in values_str.split(","):
                val_str = val_str.strip()
                # Try int, float, bool, then string
                try:
                    if "." in val_str:
                        values.append(float(val_str))
                    else:
                        values.append(int(val_str))
                except ValueError:
                    if val_str.lower() in ["true", "false"]:
                        values.append(val_str.lower() == "true")
                    else:
                        values.append(val_str)

            sweep_config["sweep"]["grid"][param_name] = values
            console.print(f"Added {param_name}: {values}")

        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")

    return sweep_config


def _review_selection(
    grid_params: Dict[str, Any], paired_groups: List[Dict[str, Any]], console: Console
):
    """Review current parameter selection."""
    console.print(f"\n[bold]Current Selection:[/bold]")

    if grid_params:
        console.print(f"\n[green]Grid Parameters (all combinations):[/green]")
        for param, values in grid_params.items():
            console.print(f"  {param}: {values}")

    if paired_groups:
        console.print(f"\n[green]Paired Parameters (vary together):[/green]")
        for group in paired_groups:
            for group_name, params in group.items():
                console.print(f"  Group '{group_name}':")
                for param, values in params.items():
                    console.print(f"    {param}: {values}")


def _configure_metadata(console: Console) -> Dict[str, Any]:
    """Configure sweep metadata."""
    console.print(f"\n[bold]Sweep Metadata:[/bold]")

    metadata = {}

    metadata["description"] = Prompt.ask("Description", default="Hyperparameter sweep")
    metadata["wandb_project"] = Prompt.ask("W&B project name", default="")

    tags_str = Prompt.ask("Tags (comma-separated)", default="")
    if tags_str:
        metadata["tags"] = [tag.strip() for tag in tags_str.split(",")]

    metadata["created_by"] = "HSM (HPC Sweep Manager)"
    metadata["created_at"] = datetime.now().isoformat()

    return metadata


def _save_sweep_config(
    sweep_config: Dict[str, Any],
    output_file: Path,
    console: Console,
    logger: logging.Logger,
):
    """Save sweep configuration to file."""
    try:
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w") as f:
            yaml.dump(sweep_config, f, default_flow_style=False, indent=2)

        console.print(
            f"\n[green]✅ Sweep configuration saved to: {output_file}[/green]"
        )
        logger.info(f"Sweep configuration saved to {output_file}")

    except Exception as e:
        console.print(f"[red]❌ Error saving configuration: {e}[/red]")
        logger.error(f"Failed to save sweep configuration: {e}")


def _show_configuration_summary(
    sweep_config: Dict[str, Any], output_file: Path, console: Console
):
    """Show configuration summary and next steps."""

    # Count total combinations
    from ..core.param_generator import ParameterGenerator
    from ..core.config_parser import SweepConfig

    try:
        config = SweepConfig.from_dict(sweep_config)
        generator = ParameterGenerator(config)
        total_combinations = generator.count_combinations()

        console.print(f"\n[bold]Configuration Summary:[/bold]")
        console.print(
            f"Total parameter combinations: [green]{total_combinations}[/green]"
        )
        console.print(f"Configuration file: [green]{output_file}[/green]")

        next_steps = f"""
[bold]Next Steps:[/bold]

1. **Review configuration**: Edit `{output_file}` if needed
2. **Test the sweep**: `hsm sweep --config {output_file} --dry-run --count`
3. **Run dry-run**: `hsm sweep --config {output_file} --dry-run --max-runs 5`
4. **Submit jobs**: `hsm sweep --config {output_file} --array`

[bold]Useful Commands:[/bold]
- `hsm sweep --config {output_file} --count` - Count total combinations
- `hsm sweep --config {output_file} --dry-run` - Preview jobs
- `hsm sweep --config {output_file} --array` - Submit as array job
"""

        console.print(
            Panel(next_steps, title="Configuration Complete", border_style="green")
        )

    except Exception as e:
        console.print(
            f"[yellow]Configuration saved, but could not validate: {e}[/yellow]"
        )
