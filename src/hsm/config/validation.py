"""Configuration validation utilities for HSM v2.

This module provides comprehensive validation for sweep configurations,
HSM settings, and system requirements with detailed error reporting.
"""

import logging
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

from .hsm import HSMConfig
from .sweep import SweepConfig

logger = logging.getLogger(__name__)


class ValidationError:
    """Represents a validation error with severity and context."""

    def __init__(
        self,
        message: str,
        severity: str = "error",
        field: Optional[str] = None,
        suggestion: Optional[str] = None,
    ):
        self.message = message
        self.severity = severity  # "error", "warning", "info"
        self.field = field
        self.suggestion = suggestion

    def __str__(self) -> str:
        prefix = f"[{self.severity.upper()}]"
        if self.field:
            prefix += f" {self.field}:"

        result = f"{prefix} {self.message}"
        if self.suggestion:
            result += f" (Suggestion: {self.suggestion})"

        return result


class ValidationResult:
    """Container for validation results with errors and warnings."""

    def __init__(self):
        self.errors: List[ValidationError] = []
        self.warnings: List[ValidationError] = []
        self.info: List[ValidationError] = []

    def add_error(
        self, message: str, field: Optional[str] = None, suggestion: Optional[str] = None
    ):
        """Add an error to the validation result."""
        self.errors.append(ValidationError(message, "error", field, suggestion))

    def add_warning(
        self, message: str, field: Optional[str] = None, suggestion: Optional[str] = None
    ):
        """Add a warning to the validation result."""
        self.warnings.append(ValidationError(message, "warning", field, suggestion))

    def add_info(self, message: str, field: Optional[str] = None, suggestion: Optional[str] = None):
        """Add an info message to the validation result."""
        self.info.append(ValidationError(message, "info", field, suggestion))

    @property
    def is_valid(self) -> bool:
        """Check if validation passed (no errors)."""
        return len(self.errors) == 0

    @property
    def has_warnings(self) -> bool:
        """Check if there are warnings."""
        return len(self.warnings) > 0

    def get_summary(self) -> str:
        """Get a summary of validation results."""
        if self.is_valid and not self.has_warnings:
            return "✓ Validation passed"
        elif self.is_valid:
            return f"✓ Validation passed with {len(self.warnings)} warnings"
        else:
            return f"✗ Validation failed with {len(self.errors)} errors and {len(self.warnings)} warnings"

    def get_all_messages(self) -> List[str]:
        """Get all validation messages as strings."""
        messages = []
        for error in self.errors:
            messages.append(str(error))
        for warning in self.warnings:
            messages.append(str(warning))
        for info in self.info:
            messages.append(str(info))
        return messages


class SweepConfigValidator:
    """Validator for sweep configurations."""

    def __init__(self, config: SweepConfig):
        self.config = config

    def validate(self) -> ValidationResult:
        """Perform comprehensive validation of sweep configuration."""
        result = ValidationResult()

        # Validate grid parameters
        self._validate_grid_parameters(result)

        # Validate paired parameters
        self._validate_paired_parameters(result)

        # Validate parameter conflicts
        self._validate_parameter_conflicts(result)

        # Validate parameter values
        self._validate_parameter_values(result)

        # Validate defaults
        self._validate_defaults(result)

        # Performance checks
        self._check_performance_implications(result)

        return result

    def _validate_grid_parameters(self, result: ValidationResult):
        """Validate grid parameter configuration."""
        if not self.config.grid:
            result.add_warning("No grid parameters defined", "grid")
            return

        for param_name, values in self.config.grid.items():
            field = f"grid.{param_name}"

            # Check parameter name
            if not param_name:
                result.add_error("Parameter name cannot be empty", field)
                continue

            # Check for valid parameter names
            if not self._is_valid_parameter_name(param_name):
                result.add_error(
                    f"Invalid parameter name: '{param_name}'",
                    field,
                    "Use alphanumeric characters, dots, and underscores only",
                )

            # Check values
            if not isinstance(values, list):
                result.add_error("Parameter values must be a list", field)
                continue

            if len(values) == 0:
                result.add_error("Parameter cannot have empty values list", field)
                continue

            if len(values) == 1:
                result.add_warning(
                    "Parameter has only one value", field, "Consider using defaults instead"
                )

            # Check for duplicate values
            if len(values) != len(set(str(v) for v in values)):
                result.add_warning("Parameter has duplicate values", field)

            # Check value types consistency
            self._validate_value_types(values, field, result)

    def _validate_paired_parameters(self, result: ValidationResult):
        """Validate paired parameter configuration."""
        if not self.config.paired:
            return

        for i, group in enumerate(self.config.paired):
            group_field = f"paired[{i}]"

            if not isinstance(group, dict):
                result.add_error("Paired group must be a dictionary", group_field)
                continue

            if not group:
                result.add_error("Paired group cannot be empty", group_field)
                continue

            # Collect all parameter lengths in this group
            group_lengths = []
            group_params = []

            for group_name, params in group.items():
                group_field_name = f"{group_field}.{group_name}"

                if not isinstance(params, dict):
                    result.add_error(
                        "Paired group must contain parameter dictionaries", group_field_name
                    )
                    continue

                for param_name, values in params.items():
                    param_field = f"{group_field_name}.{param_name}"
                    group_params.append(param_name)

                    # Validate parameter name
                    if not self._is_valid_parameter_name(param_name):
                        result.add_error(
                            f"Invalid parameter name: '{param_name}'",
                            param_field,
                            "Use alphanumeric characters, dots, and underscores only",
                        )

                    # Validate values
                    if not isinstance(values, list):
                        result.add_error("Paired parameter values must be a list", param_field)
                        continue

                    if len(values) == 0:
                        result.add_error(
                            "Paired parameter cannot have empty values list", param_field
                        )
                        continue

                    group_lengths.append(len(values))

                    # Check value types
                    self._validate_value_types(values, param_field, result)

            # Check length consistency within group
            if group_lengths and not all(length == group_lengths[0] for length in group_lengths):
                result.add_error(
                    f"All parameters in paired group must have the same length. "
                    f"Found lengths: {group_lengths} for parameters: {group_params}",
                    group_field,
                    "Ensure all paired parameters have equal-length value lists",
                )

    def _validate_parameter_conflicts(self, result: ValidationResult):
        """Check for parameter name conflicts between grid and paired."""
        grid_params = set(self._flatten_dict(self.config.grid).keys())
        paired_params = set()

        for group in self.config.paired:
            for group_name, params in group.items():
                paired_params.update(self._flatten_dict(params).keys())

        conflicts = grid_params.intersection(paired_params)
        if conflicts:
            result.add_error(
                f"Parameter names appear in both grid and paired configs: {sorted(conflicts)}",
                suggestion="Use unique parameter names across grid and paired configurations",
            )

    def _validate_parameter_values(self, result: ValidationResult):
        """Validate individual parameter values."""
        all_params = {}
        all_params.update(self._flatten_dict(self.config.grid))

        for group in self.config.paired:
            for group_name, params in group.items():
                all_params.update(self._flatten_dict(params))

        for param_name, values in all_params.items():
            field = f"parameter.{param_name}"

            for i, value in enumerate(values):
                # Check for problematic values
                if value is None and len(values) > 1:
                    result.add_warning(
                        f"Parameter '{param_name}' contains None mixed with other values",
                        field,
                        "Consider using a default value instead of None",
                    )

                # Check for extremely large numbers that might cause issues
                if isinstance(value, (int, float)) and abs(value) > 1e10:
                    result.add_warning(
                        f"Parameter '{param_name}' has very large value: {value}",
                        field,
                        "Large values may cause numerical instability",
                    )

                # Check for empty strings
                if isinstance(value, str) and value == "":
                    result.add_warning(f"Parameter '{param_name}' has empty string value", field)

    def _validate_defaults(self, result: ValidationResult):
        """Validate default parameters."""
        if not self.config.defaults:
            result.add_info("No default parameters defined")
            return

        # Check for defaults that conflict with sweep parameters
        sweep_params = set(self._flatten_dict(self.config.grid).keys())
        for group in self.config.paired:
            for group_name, params in group.items():
                sweep_params.update(self._flatten_dict(params).keys())

        default_params = set(self._flatten_dict(self.config.defaults).keys())
        conflicts = sweep_params.intersection(default_params)

        for conflict in conflicts:
            result.add_warning(
                f"Default parameter '{conflict}' will be overridden by sweep parameter",
                "defaults",
                "Remove from defaults or rename the sweep parameter",
            )

    def _check_performance_implications(self, result: ValidationResult):
        """Check for potential performance issues."""
        total_combinations = self.config.get_total_combinations()

        if total_combinations > 10000:
            result.add_warning(
                f"Large number of parameter combinations: {total_combinations}",
                suggestion="Consider reducing parameter ranges or using sampling",
            )
        elif total_combinations > 1000:
            result.add_info(
                f"Parameter combinations: {total_combinations} (this may take significant time)"
            )

        # Check for excessive parameter ranges
        for param_name, values in self._flatten_dict(self.config.grid).items():
            if len(values) > 100:
                result.add_warning(
                    f"Parameter '{param_name}' has {len(values)} values",
                    suggestion="Consider reducing the range or using fewer values",
                )

    def _validate_value_types(self, values: List[Any], field: str, result: ValidationResult):
        """Validate consistency of value types."""
        if not values:
            return

        # Get types of all values
        value_types = [type(v).__name__ for v in values]
        unique_types = set(value_types)

        # Allow mixing of int and float
        if unique_types == {"int", "float"}:
            return

        # Warn about mixed types (excluding None)
        non_none_types = {t for t in unique_types if t != "NoneType"}
        if len(non_none_types) > 1:
            result.add_warning(
                f"Mixed value types in parameter: {list(non_none_types)}",
                field,
                "Consider using consistent types for better clarity",
            )

    def _is_valid_parameter_name(self, name: str) -> bool:
        """Check if parameter name is valid."""
        if not name:
            return False

        # Allow alphanumeric, dots, and underscores
        import re

        return bool(re.match(r"^[a-zA-Z0-9._]+$", name))

    def _flatten_dict(
        self, d: Dict[str, Any], parent_key: str = "", sep: str = "."
    ) -> Dict[str, Any]:
        """Flatten nested dictionary with dot notation."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)


class HSMConfigValidator:
    """Validator for HSM configurations."""

    def __init__(self, config: HSMConfig):
        self.config = config

    def validate(self) -> ValidationResult:
        """Perform comprehensive validation of HSM configuration."""
        result = ValidationResult()

        # Validate paths
        self._validate_paths(result)

        # Validate HPC settings
        self._validate_hpc_settings(result)

        # Validate compute settings
        self._validate_compute_settings(result)

        # Validate logging settings
        self._validate_logging_settings(result)

        # Validate experiment tracking
        self._validate_experiment_tracking(result)

        return result

    def _validate_paths(self, result: ValidationResult):
        """Validate path configurations."""
        # Python interpreter
        if self.config.paths.python_interpreter:
            python_path = Path(self.config.paths.python_interpreter)
            if not python_path.exists():
                result.add_error(
                    f"Python interpreter not found: {python_path}", "paths.python_interpreter"
                )
            elif not python_path.is_file():
                result.add_error(
                    f"Python interpreter is not a file: {python_path}", "paths.python_interpreter"
                )

        # Training script
        if self.config.paths.training_script:
            script_path = Path(self.config.paths.training_script)
            if not script_path.exists():
                result.add_warning(
                    f"Training script not found: {script_path}",
                    "paths.training_script",
                    "Ensure the script exists before running sweeps",
                )

        # Project root
        if self.config.paths.project_root:
            project_path = Path(self.config.paths.project_root)
            if not project_path.exists():
                result.add_error(
                    f"Project root directory not found: {project_path}", "paths.project_root"
                )
            elif not project_path.is_dir():
                result.add_error(
                    f"Project root is not a directory: {project_path}", "paths.project_root"
                )

    def _validate_hpc_settings(self, result: ValidationResult):
        """Validate HPC-specific settings."""
        # Walltime format
        if self.config.hpc.default_walltime:
            if not self._is_valid_walltime(self.config.hpc.default_walltime):
                result.add_error(
                    f"Invalid walltime format: {self.config.hpc.default_walltime}",
                    "hpc.default_walltime",
                    "Use format like '12:00:00' or '1:30:00'",
                )

        # Max array size
        if self.config.hpc.max_array_size is not None:
            if self.config.hpc.max_array_size <= 0:
                result.add_error("Max array size must be positive", "hpc.max_array_size")

    def _validate_compute_settings(self, result: ValidationResult):
        """Validate compute-related settings."""
        # Concurrent task limits
        if self.config.compute.max_concurrent_local <= 0:
            result.add_error(
                "Max concurrent local tasks must be positive", "compute.max_concurrent_local"
            )

        if self.config.compute.max_concurrent_remote <= 0:
            result.add_error(
                "Max concurrent remote tasks must be positive", "compute.max_concurrent_remote"
            )

        # Timeout settings
        if self.config.compute.default_timeout <= 0:
            result.add_error("Default timeout must be positive", "compute.default_timeout")

        # Retry settings
        if self.config.compute.retry_attempts < 0:
            result.add_error("Retry attempts cannot be negative", "compute.retry_attempts")

        if self.config.compute.retry_delay <= 0:
            result.add_warning("Retry delay should be positive", "compute.retry_delay")

    def _validate_logging_settings(self, result: ValidationResult):
        """Validate logging configuration."""
        # Log level
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.config.logging.level.upper() not in valid_levels:
            result.add_error(
                f"Invalid log level: {self.config.logging.level}",
                "logging.level",
                f"Use one of: {valid_levels}",
            )

        # Backup count
        if self.config.logging.backup_count < 0:
            result.add_error("Backup count cannot be negative", "logging.backup_count")

    def _validate_experiment_tracking(self, result: ValidationResult):
        """Validate experiment tracking configuration."""
        # W&B validation
        if self.config.experiment_tracking.wandb_enabled:
            if not self.config.experiment_tracking.wandb_project:
                result.add_warning(
                    "W&B enabled but no project specified",
                    "experiment_tracking.wandb_project",
                    "Specify a project name for better organization",
                )

        # MLflow validation
        if self.config.experiment_tracking.mlflow_enabled:
            if not self.config.experiment_tracking.mlflow_tracking_uri:
                result.add_info(
                    "MLflow enabled but no tracking URI specified",
                    "experiment_tracking.mlflow_tracking_uri",
                    "Will use default local tracking",
                )

    def _is_valid_walltime(self, walltime: str) -> bool:
        """Check if walltime format is valid."""
        import re

        # Matches formats like "12:00:00", "1:30:00", "24:00:00"
        pattern = r"^\d{1,2}:\d{2}:\d{2}$"
        if not re.match(pattern, walltime):
            return False

        # Validate time components
        parts = walltime.split(":")
        try:
            hours, minutes, seconds = map(int, parts)
            return 0 <= minutes < 60 and 0 <= seconds < 60
        except ValueError:
            return False


class SystemValidator:
    """Validator for system requirements and capabilities."""

    @staticmethod
    def validate_python_environment() -> ValidationResult:
        """Validate Python environment and dependencies."""
        result = ValidationResult()

        # Check Python version
        python_version = sys.version_info
        if python_version < (3, 8):
            result.add_error(
                f"Python {python_version.major}.{python_version.minor} is not supported",
                suggestion="Use Python 3.8 or higher",
            )

        # Check required packages
        required_packages = [
            ("yaml", "PyYAML"),
            ("asyncio", None),
            ("pathlib", None),
        ]

        for package, pip_name in required_packages:
            try:
                __import__(package)
            except ImportError:
                install_name = pip_name or package
                result.add_error(
                    f"Required package '{package}' not found",
                    suggestion=f"Install with: pip install {install_name}",
                )

        # Check optional packages
        optional_packages = [
            ("psutil", "System monitoring capabilities"),
            ("omegaconf", "Hydra configuration support"),
        ]

        for package, description in optional_packages:
            try:
                __import__(package)
            except ImportError:
                result.add_info(
                    f"Optional package '{package}' not found",
                    suggestion=f"Install for {description}: pip install {package}",
                )

        return result

    @staticmethod
    def validate_system_resources() -> ValidationResult:
        """Validate system resources and capabilities."""
        result = ValidationResult()

        try:
            import psutil

            # Check available memory
            memory = psutil.virtual_memory()
            if memory.available < 1 * 1024**3:  # Less than 1GB
                result.add_warning(
                    f"Low available memory: {memory.available / 1024**3:.1f} GB",
                    suggestion="Close other applications or add more RAM",
                )

            # Check disk space
            disk = psutil.disk_usage("/")
            if disk.free < 5 * 1024**3:  # Less than 5GB
                result.add_warning(
                    f"Low disk space: {disk.free / 1024**3:.1f} GB free",
                    suggestion="Free up disk space before running large sweeps",
                )

            # Check CPU cores
            cpu_count = psutil.cpu_count()
            result.add_info(f"System has {cpu_count} CPU cores available")

        except ImportError:
            result.add_info(
                "psutil not available - cannot check system resources",
                suggestion="Install psutil for system monitoring: pip install psutil",
            )

        return result


def validate_full_configuration(
    sweep_config: SweepConfig, hsm_config: Optional[HSMConfig] = None
) -> ValidationResult:
    """Perform comprehensive validation of all configurations.

    Args:
        sweep_config: Sweep configuration to validate
        hsm_config: HSM configuration to validate (optional)

    Returns:
        Combined validation result
    """
    result = ValidationResult()

    # Validate sweep config
    sweep_validator = SweepConfigValidator(sweep_config)
    sweep_result = sweep_validator.validate()
    result.errors.extend(sweep_result.errors)
    result.warnings.extend(sweep_result.warnings)
    result.info.extend(sweep_result.info)

    # Validate HSM config if provided
    if hsm_config:
        hsm_validator = HSMConfigValidator(hsm_config)
        hsm_result = hsm_validator.validate()
        result.errors.extend(hsm_result.errors)
        result.warnings.extend(hsm_result.warnings)
        result.info.extend(hsm_result.info)

    # Validate system
    system_result = SystemValidator.validate_python_environment()
    result.errors.extend(system_result.errors)
    result.warnings.extend(system_result.warnings)
    result.info.extend(system_result.info)

    return result
