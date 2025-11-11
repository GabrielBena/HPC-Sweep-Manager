"""HSM configuration loading utilities."""

from dataclasses import dataclass, field
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml

try:
    from omegaconf import DictConfig, OmegaConf

    OMEGACONF_AVAILABLE = True
except ImportError:
    OMEGACONF_AVAILABLE = False
    DictConfig = None

from .utils import format_walltime

logger = logging.getLogger(__name__)


@dataclass
class SweepConfig:
    """Configuration for parameter sweeps."""

    grid: Dict[str, List[Any]] = field(default_factory=dict)
    paired: List[Dict[str, Dict[str, List[Any]]]] = field(default_factory=list)
    defaults: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    script: str = None  # Training script path (optional)
    complete: str = None  # Completion sweep ID (optional)

    @classmethod
    def from_yaml(cls, config_path: Union[str, Path]) -> "SweepConfig":
        """Load sweep config from YAML file."""
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Sweep config not found: {config_path}")

        with open(config_path) as f:
            raw_config = yaml.safe_load(f)

        return cls.from_dict(raw_config)

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "SweepConfig":
        """Create sweep config from dictionary."""
        # Extract sweep section if it exists, otherwise treat entire dict as sweep config
        sweep_config = config_dict.get("sweep", config_dict)

        return cls(
            grid=sweep_config.get("grid", {}),
            paired=sweep_config.get("paired", []),
            defaults=config_dict.get("defaults", {}),
            metadata=config_dict.get("metadata", {}),
            script=config_dict.get("script"),  # Extract script from top level
            complete=config_dict.get("complete"),  # Extract completion sweep ID from top level
        )

    @classmethod
    def from_hydra_config(
        cls,
        hydra_config: Union[DictConfig, Dict[str, Any]],
        selected_params: Dict[str, List[Any]],
    ) -> "SweepConfig":
        """Create sweep config from Hydra config with selected parameters."""
        if not OMEGACONF_AVAILABLE:
            raise ImportError("omegaconf is required for Hydra config support")

        if isinstance(hydra_config, DictConfig):
            hydra_config = OmegaConf.to_container(hydra_config, resolve=True)

        return cls(
            grid=selected_params,
            paired=[],
            defaults=hydra_config.get("defaults", {}),
            metadata={
                "source": "hydra_config",
                "hydra_config_keys": list(hydra_config.keys()),
            },
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
                        errors.append(
                            f"Paired parameter '{param_name}' in group '{group_name}' must be a list"
                        )
                    else:
                        lengths.append(len(values))

            if lengths and not all(length == lengths[0] for length in lengths):
                errors.append(
                    f"All parameters in paired group {i} must have the same length. Found lengths: {lengths}"
                )

        return errors

    def get_total_combinations(self) -> int:
        """Calculate total number of parameter combinations."""
        from .param_generator import ParameterGenerator

        generator = ParameterGenerator(self)
        return generator.count_combinations()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result = {
            "sweep": {"grid": self.grid, "paired": self.paired},
            "defaults": self.defaults,
            "metadata": self.metadata,
        }
        if self.script:
            result["script"] = self.script
        return result

    def save(self, output_path: Union[str, Path]) -> None:
        """Save sweep config to YAML file."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            yaml.dump(self.to_dict(), f, default_flow_style=False, indent=2)


class HSMConfig:
    """HSM configuration loader and manager."""

    def __init__(self, config_data: Dict[str, Any]):
        """Initialize with configuration data."""
        self.config_data = config_data

    @classmethod
    def load(cls, config_path: Optional[Path] = None) -> Optional["HSMConfig"]:
        """
        Load HSM configuration from file.

        Args:
            config_path: Path to hsm_config.yaml file. If None, searches standard locations.

        Returns:
            HSMConfig instance or None if not found
        """
        if config_path is None:
            # Search for config in standard locations (new .hsm/ location takes priority)
            search_paths = [
                Path.cwd() / ".hsm" / "config.yaml",  # NEW: primary location
                Path(".hsm") / "config.yaml",  # NEW: relative .hsm/
                Path.cwd() / "sweeps" / "hsm_config.yaml",  # Legacy: for backwards compatibility
                Path.cwd() / "hsm_config.yaml",  # Legacy
                Path("sweeps") / "hsm_config.yaml",  # Legacy
                Path("hsm_config.yaml"),  # Legacy
            ]

            for path in search_paths:
                if path.exists():
                    config_path = path
                    logger.debug(f"Found HSM config at: {path}")
                    break

        if config_path is None or not config_path.exists():
            logger.debug(
                "No HSM config found in standard locations (.hsm/config.yaml or sweeps/hsm_config.yaml)"
            )
            return None

        try:
            with open(config_path) as f:
                config_data = yaml.safe_load(f)

            logger.debug(f"Loaded HSM config from {config_path}")
            return cls(config_data)

        except Exception as e:
            logger.warning(f"Failed to load HSM config from {config_path}: {e}")
            return None

    def get_default_walltime(self) -> str:
        """Get default walltime from config."""
        walltime_value = self.config_data.get("hpc", {}).get("default_walltime", "23:59:59")

        # If the value is an integer (seconds), convert to HH:MM:SS format
        if isinstance(walltime_value, int):
            return format_walltime(walltime_value)

        # If it's already a string, assume it's in the correct format
        return str(walltime_value)

    def get_default_resources(self) -> str:
        """Get default resources from config."""
        return self.config_data.get("hpc", {}).get("default_resources", "select=1:ncpus=4:mem=64gb")

    def get_default_python_path(self) -> Optional[str]:
        """Get default Python interpreter path from config."""
        return self.config_data.get("paths", {}).get("python_interpreter")

    def get_default_script_path(self) -> Optional[str]:
        """Get default training script path from config."""
        return self.config_data.get("paths", {}).get("train_script")

    def get_project_root(self) -> Optional[str]:
        """Get project root directory from config."""
        return self.config_data.get("project", {}).get("root")

    def get_wandb_config(self) -> Dict[str, Any]:
        """Get wandb configuration from config."""
        return self.config_data.get("wandb", {})

    def get_hpc_system(self) -> Optional[str]:
        """Get HPC system type from config."""
        return self.config_data.get("hpc", {}).get("system")

    def get_max_array_size(self) -> Optional[int]:
        """Get maximum array size from config."""
        return self.config_data.get("hpc", {}).get("max_array_size")


class HydraConfigParser:
    """Parser for Hydra configuration files."""

    def __init__(self, config_dir: Path):
        self.config_dir = config_dir

    def discover_configs(self) -> Dict[str, Path]:
        """Discover all configuration files in the config directory."""
        configs = {}

        if not self.config_dir.exists():
            return configs

        # Look for YAML files
        for yaml_file in self.config_dir.rglob("*.yaml"):
            relative_path = yaml_file.relative_to(self.config_dir)
            config_name = str(relative_path).replace("/", ".").replace(".yaml", "")
            configs[config_name] = yaml_file

        return configs

    def load_config(self, config_path: Path) -> Dict[str, Any]:
        """Load a configuration file."""
        with open(config_path) as f:
            return yaml.safe_load(f)

    def extract_parameters(self, config: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
        """Extract parameters from a nested configuration."""
        params = {}

        for key, value in config.items():
            full_key = f"{prefix}.{key}" if prefix else key

            if isinstance(value, dict):
                params.update(self.extract_parameters(value, full_key))
            else:
                params[full_key] = value

        return params

    def suggest_sweep_ranges(self, parameters: Dict[str, Any]) -> Dict[str, List[Any]]:
        """Suggest sweep ranges for parameters based on their types and values."""
        suggestions = {}

        for param_name, value in parameters.items():
            if isinstance(value, (int, float)):
                if isinstance(value, int):
                    suggestions[param_name] = [value // 2, value, value * 2]
                else:
                    suggestions[param_name] = [value * 0.5, value, value * 2.0]
            elif isinstance(value, bool):
                suggestions[param_name] = [True, False]
            elif isinstance(value, str):
                suggestions[param_name] = [value]

        return suggestions
