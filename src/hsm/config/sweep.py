"""Sweep configuration management for HSM v2.

This module provides comprehensive sweep configuration management with support
for grid and paired parameter sweeps, Hydra integration, and extensive validation.
"""

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

logger = logging.getLogger(__name__)


@dataclass
class SweepConfig:
    """Configuration for parameter sweeps.

    This class manages both grid and paired parameter configurations,
    providing comprehensive validation and parameter generation capabilities.
    """

    grid: Dict[str, List[Any]] = field(default_factory=dict)
    paired: List[Dict[str, Dict[str, List[Any]]]] = field(default_factory=list)
    defaults: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_yaml(cls, config_path: Union[str, Path]) -> "SweepConfig":
        """Load sweep config from YAML file.

        Args:
            config_path: Path to the YAML configuration file

        Returns:
            SweepConfig instance

        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If YAML parsing fails
        """
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Sweep config not found: {config_path}")

        try:
            with open(config_path) as f:
                raw_config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Failed to parse YAML config {config_path}: {e}")

        if raw_config is None:
            raw_config = {}

        return cls.from_dict(raw_config)

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "SweepConfig":
        """Create sweep config from dictionary.

        Args:
            config_dict: Dictionary containing configuration data

        Returns:
            SweepConfig instance
        """
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
        cls,
        hydra_config: Union[DictConfig, Dict[str, Any]],
        selected_params: Dict[str, List[Any]],
    ) -> "SweepConfig":
        """Create sweep config from Hydra config with selected parameters.

        Args:
            hydra_config: Hydra configuration object or dictionary
            selected_params: Dictionary of parameters to sweep over

        Returns:
            SweepConfig instance

        Raises:
            ImportError: If omegaconf is not available
        """
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

    @classmethod
    def from_detected_parameters(
        cls,
        project_root: Union[str, Path],
        max_params: int = 5,
        include_seeds: bool = True,
        description: Optional[str] = None,
    ) -> "SweepConfig":
        """Create sweep config from auto-detected project parameters.

        Args:
            project_root: Path to project root directory
            max_params: Maximum number of parameters to include in sweep
            include_seeds: Whether to include seed parameter if not detected
            description: Custom description for the sweep

        Returns:
            SweepConfig instance with auto-detected parameters
        """
        from ..utils.paths import PathDetector

        detector = PathDetector(Path(project_root))
        project_info = detector.get_project_info()
        sweep_suggestions = project_info.get("sweep_suggestions", {})

        # Select the most relevant parameters
        grid_params = {}
        param_count = 0

        # Prioritize certain parameter types
        priority_patterns = [
            "learning_rate",
            "lr",
            "batch_size",
            "dropout",
            "seed",
            "hidden_dim",
            "weight_decay",
            "epochs",
        ]

        # First, add high-priority parameters
        for pattern in priority_patterns:
            for param_name, values in sweep_suggestions.items():
                if (
                    param_count < max_params
                    and pattern in param_name.lower()
                    and param_name not in grid_params
                ):
                    grid_params[param_name] = values
                    param_count += 1

        # Then add other parameters to fill up to max_params
        for param_name, values in sweep_suggestions.items():
            if param_count >= max_params:
                break
            if param_name not in grid_params:
                grid_params[param_name] = values
                param_count += 1

        # Add seed if requested and not already present
        if include_seeds and "seed" not in grid_params:
            grid_params["seed"] = [1, 2, 3, 4, 5]

        # Create metadata
        project_name = project_info.get("project_name", "Unknown Project")
        if description is None:
            description = f"Auto-generated hyperparameter sweep for {project_name}"

        metadata = {
            "description": description,
            "tags": ["auto-generated", "baseline"],
            "auto_detected_params": list(sweep_suggestions.keys()),
            "total_detected_params": len(sweep_suggestions),
            "selected_params": list(grid_params.keys()),
            "project_root": str(project_root),
            "generated_by": "HSM PathDetector",
        }

        return cls(
            grid=grid_params,
            paired=[],
            defaults={"override": "hydra/launcher: basic"},
            metadata=metadata,
        )

    def validate(self) -> List[str]:
        """Validate the sweep configuration and return any errors.

        Returns:
            List of validation error messages
        """
        errors = []

        # Validate grid parameters
        for key, values in self.grid.items():
            if not isinstance(values, list):
                errors.append(f"Grid parameter '{key}' must be a list, got {type(values).__name__}")
            elif len(values) == 0:
                errors.append(f"Grid parameter '{key}' cannot be empty")
            else:
                # Check for valid parameter names
                if not key.replace(".", "").replace("_", "").isalnum():
                    errors.append(f"Grid parameter name '{key}' contains invalid characters")

        # Validate paired parameters
        for i, group in enumerate(self.paired):
            if not isinstance(group, dict):
                errors.append(f"Paired group {i} must be a dictionary")
                continue

            # Check that all parameters in a group have the same length
            lengths = []
            param_names = []

            for group_name, params in group.items():
                if not isinstance(params, dict):
                    errors.append(
                        f"Paired group '{group_name}' must contain parameter dictionaries"
                    )
                    continue

                for param_name, values in params.items():
                    param_names.append(param_name)
                    if not isinstance(values, list):
                        errors.append(
                            f"Paired parameter '{param_name}' in group '{group_name}' must be a list"
                        )
                    else:
                        lengths.append(len(values))
                        if len(values) == 0:
                            errors.append(
                                f"Paired parameter '{param_name}' in group '{group_name}' cannot be empty"
                            )

            # Check length consistency within group
            if lengths and not all(length == lengths[0] for length in lengths):
                errors.append(
                    f"All parameters in paired group {i} must have the same length. "
                    f"Found lengths: {lengths} for parameters: {param_names}"
                )

        # Check for parameter name conflicts between grid and paired
        grid_params = set(self._flatten_dict(self.grid).keys())
        paired_params = set()
        for group in self.paired:
            for group_name, params in group.items():
                paired_params.update(self._flatten_dict(params).keys())

        conflicts = grid_params.intersection(paired_params)
        if conflicts:
            errors.append(
                f"Parameter names appear in both grid and paired configs: {sorted(conflicts)}"
            )

        return errors

    def get_total_combinations(self) -> int:
        """Calculate total number of parameter combinations.

        Returns:
            Total number of parameter combinations
        """
        from ..utils.params import ParameterGenerator

        generator = ParameterGenerator(self)
        return generator.count_combinations()

    def generate_parameters(self, max_runs: Optional[int] = None) -> List[Dict[str, Any]]:
        """Generate all parameter combinations.

        Args:
            max_runs: Maximum number of parameter combinations to generate

        Returns:
            List of parameter dictionaries
        """
        from ..utils.params import ParameterGenerator

        generator = ParameterGenerator(self)
        return generator.generate_combinations(max_runs=max_runs)

    def preview_parameters(self, max_preview: int = 5) -> List[Dict[str, Any]]:
        """Generate a preview of parameter combinations.

        Args:
            max_preview: Maximum number of combinations to preview

        Returns:
            List of parameter dictionaries (limited to max_preview)
        """
        from ..utils.params import ParameterGenerator

        generator = ParameterGenerator(self)
        return generator.preview_combinations(max_preview=max_preview)

    def get_parameter_info(self) -> Dict[str, Any]:
        """Get detailed information about parameters and combinations.

        Returns:
            Dictionary with parameter information including counts and types
        """
        from ..utils.params import ParameterGenerator

        generator = ParameterGenerator(self)
        return generator.get_parameter_info()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation.

        Returns:
            Dictionary representation of the sweep config
        """
        return {
            "sweep": {"grid": self.grid, "paired": self.paired},
            "defaults": self.defaults,
            "metadata": self.metadata,
        }

    def save(self, output_path: Union[str, Path]) -> None:
        """Save sweep config to YAML file.

        Args:
            output_path: Path where to save the configuration
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            yaml.dump(self.to_dict(), f, default_flow_style=False, indent=2)

        logger.debug(f"Saved sweep config to {output_path}")

    def _flatten_dict(
        self, d: Dict[str, Any], parent_key: str = "", sep: str = "."
    ) -> Dict[str, Any]:
        """Flatten nested dictionary with dot notation.

        Args:
            d: Dictionary to flatten
            parent_key: Parent key for nested items
            sep: Separator for nested keys

        Returns:
            Flattened dictionary
        """
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def merge_defaults(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Merge sweep parameters with default values.

        Args:
            params: Parameter dictionary from sweep

        Returns:
            Merged parameter dictionary with defaults applied
        """
        merged = self.defaults.copy()
        merged.update(params)
        return merged

    def __str__(self) -> str:
        """String representation of the sweep configuration."""
        grid_count = len(self.grid)
        paired_count = len(self.paired)
        total_combinations = self.get_total_combinations()

        return (
            f"SweepConfig(grid_params={grid_count}, "
            f"paired_groups={paired_count}, "
            f"total_combinations={total_combinations})"
        )

    def __repr__(self) -> str:
        """Detailed representation of the sweep configuration."""
        return (
            f"SweepConfig("
            f"grid={self.grid}, "
            f"paired={self.paired}, "
            f"defaults={self.defaults}, "
            f"metadata={self.metadata})"
        )
