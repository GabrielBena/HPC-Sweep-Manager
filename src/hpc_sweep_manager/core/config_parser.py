"""Configuration parsing and validation for sweep configs."""

import yaml
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from omegaconf import DictConfig, OmegaConf


@dataclass
class SweepConfig:
    """Configuration for parameter sweeps."""

    grid: Dict[str, List[Any]] = field(default_factory=dict)
    paired: List[Dict[str, Dict[str, List[Any]]]] = field(default_factory=list)
    defaults: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_yaml(cls, config_path: Union[str, Path]) -> "SweepConfig":
        """Load sweep config from YAML file."""
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Sweep config not found: {config_path}")

        with open(config_path, "r") as f:
            raw_config = yaml.safe_load(f)

        return cls.from_dict(raw_config)

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "SweepConfig":
        """Create sweep config from dictionary."""
        # Extract sweep section if it exists, otherwise treat entire dict as sweep config
        if "sweep" in config_dict:
            sweep_config = config_dict["sweep"]
        else:
            sweep_config = config_dict

        return cls(
            grid=sweep_config.get("grid", {}),
            paired=sweep_config.get("paired", []),
            defaults=config_dict.get("defaults", {}),
            metadata=config_dict.get("metadata", {}),
        )

    @classmethod
    def from_hydra_config(
        cls, hydra_config: Union[DictConfig, Dict[str, Any]], selected_params: Dict[str, List[Any]]
    ) -> "SweepConfig":
        """Create sweep config from Hydra config with selected parameters."""
        if isinstance(hydra_config, DictConfig):
            hydra_config = OmegaConf.to_container(hydra_config, resolve=True)

        return cls(
            grid=selected_params,
            paired=[],
            defaults=hydra_config.get("defaults", {}),
            metadata={"source": "hydra_config", "hydra_config_keys": list(hydra_config.keys())},
        )

    def validate(self) -> List[str]:
        """Validate the sweep configuration and return any errors."""
        errors = []

        # Validate grid parameters
        for key, values in self.grid.items():
            if not isinstance(values, list):
                errors.append(f"Grid parameter '{key}' must be a list, got {type(values)}")
            elif len(values) == 0:
                errors.append(f"Grid parameter '{key}' cannot be empty")

        # Validate paired parameters
        for i, group in enumerate(self.paired):
            if not isinstance(group, dict):
                errors.append(f"Paired group {i} must be a dictionary")
                continue

            # Check that all parameters in a group have the same length
            lengths = []
            for group_name, params in group.items():
                for param_name, values in params.items():
                    if not isinstance(values, list):
                        errors.append(f"Paired parameter '{param_name}' in group '{group_name}' must be a list")
                    else:
                        lengths.append(len(values))

            if lengths and not all(length == lengths[0] for length in lengths):
                errors.append(f"All parameters in paired group {i} must have the same length. Found lengths: {lengths}")

        return errors

    def get_total_combinations(self) -> int:
        """Calculate total number of parameter combinations."""
        from .param_generator import ParameterGenerator

        generator = ParameterGenerator(self)
        return generator.count_combinations()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "sweep": {"grid": self.grid, "paired": self.paired},
            "defaults": self.defaults,
            "metadata": self.metadata,
        }

    def save(self, output_path: Union[str, Path]) -> None:
        """Save sweep config to YAML file."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            yaml.dump(self.to_dict(), f, default_flow_style=False, indent=2)


class HydraConfigParser:
    """Parser for Hydra configuration files."""

    def __init__(self, config_dir: Path):
        self.config_dir = Path(config_dir)

    def discover_configs(self) -> Dict[str, Path]:
        """Discover all Hydra config files in the config directory."""
        configs = {}

        if not self.config_dir.exists():
            return configs

        for config_file in self.config_dir.rglob("*.yaml"):
            # Skip sweep configs and override configs
            if "sweep" in config_file.name or config_file.name.startswith("override"):
                continue

            relative_path = config_file.relative_to(self.config_dir)
            config_name = str(relative_path).replace(".yaml", "").replace("/", ".")
            configs[config_name] = config_file

        return configs

    def load_config(self, config_path: Path) -> Dict[str, Any]:
        """Load a single Hydra config file."""
        with open(config_path, "r") as f:
            return yaml.safe_load(f)

    def extract_parameters(self, config: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
        """Extract all parameters from a config that could be swept."""
        parameters = {}

        for key, value in config.items():
            full_key = f"{prefix}.{key}" if prefix else key

            if isinstance(value, dict):
                # Recursively extract from nested dicts
                nested_params = self.extract_parameters(value, full_key)
                parameters.update(nested_params)
            elif isinstance(value, (int, float, str, bool)) or value is None:
                # These are potential sweep parameters
                parameters[full_key] = value

        return parameters

    def suggest_sweep_ranges(self, parameters: Dict[str, Any]) -> Dict[str, List[Any]]:
        """Suggest reasonable sweep ranges for parameters."""
        suggestions = {}

        for key, value in parameters.items():
            if isinstance(value, (int, float)):
                if value == 0:
                    suggestions[key] = [0, 0.1, 0.5, 1.0]
                elif "lr" in key.lower() or "rate" in key.lower():
                    # Learning rate-like parameters
                    suggestions[key] = [value / 10, value, value * 10]
                elif isinstance(value, int) and value > 1:
                    # Integer parameters like hidden_size, batch_size
                    suggestions[key] = [value // 2, value, value * 2]
                else:
                    # General numeric parameters
                    suggestions[key] = [value * 0.5, value, value * 1.5]
            elif isinstance(value, str):
                suggestions[key] = [value]  # Single value for now
            elif isinstance(value, bool):
                suggestions[key] = [True, False]

        return suggestions
