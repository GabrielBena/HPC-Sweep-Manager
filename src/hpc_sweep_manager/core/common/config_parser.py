"""Configuration parsing and validation for sweep configs."""

from pathlib import Path
from typing import Any, Dict, List

import yaml


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
        with open(config_path) as f:
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
