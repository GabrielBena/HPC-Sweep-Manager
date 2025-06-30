"""Parameter combination generation for sweeps.

This module provides comprehensive parameter generation capabilities for
grid and paired parameter configurations, supporting complex nested
parameter structures and efficient combination generation.
"""

import itertools
import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class ParameterGenerator:
    """Generate parameter combinations from sweep configuration.

    This class handles both grid and paired parameter configurations,
    providing efficient generation of parameter combinations and
    comprehensive validation of parameter structures.
    """

    def __init__(self, config):
        """Initialize with a SweepConfig object.

        Args:
            config: SweepConfig instance containing grid and paired parameters
        """
        self.config = config

    def generate_combinations(self, max_runs: Optional[int] = None) -> List[Dict[str, Any]]:
        """Generate all parameter combinations.

        Args:
            max_runs: Maximum number of combinations to generate

        Returns:
            List of parameter dictionaries
        """
        grid_combinations = self._generate_grid_combinations()
        paired_combinations = self._generate_paired_combinations()

        final_combinations = []

        # Combine grid and paired parameters
        for grid_combo in grid_combinations:
            if not paired_combinations:
                # No paired parameters, just use grid combinations
                final_combinations.append(grid_combo)
            else:
                # Combine each grid combination with each paired combination
                for paired_combo in paired_combinations:
                    combo = {**grid_combo, **paired_combo}
                    final_combinations.append(combo)

        # Apply run limit if specified
        if max_runs is not None and max_runs < len(final_combinations):
            final_combinations = final_combinations[:max_runs]
            logger.info(
                f"Limited parameter combinations to {max_runs} (from {len(final_combinations)} total)"
            )

        logger.debug(f"Generated {len(final_combinations)} parameter combinations")
        return final_combinations

    def count_combinations(self) -> int:
        """Count total number of parameter combinations without generating them.

        Returns:
            Total number of parameter combinations
        """
        grid_count = self._count_grid_combinations()
        paired_count = self._count_paired_combinations()

        if paired_count == 0:
            return grid_count
        else:
            return grid_count * paired_count

    def preview_combinations(self, max_preview: int = 5) -> List[Dict[str, Any]]:
        """Generate a preview of parameter combinations.

        Args:
            max_preview: Maximum number of combinations to preview

        Returns:
            List of parameter dictionaries (limited to max_preview)
        """
        all_combinations = self.generate_combinations()
        preview = all_combinations[:max_preview]

        if len(all_combinations) > max_preview:
            logger.info(f"Showing {max_preview} of {len(all_combinations)} total combinations")

        return preview

    def get_parameter_info(self) -> Dict[str, Any]:
        """Get detailed information about parameters and combinations.

        Returns:
            Dictionary with parameter information including counts and types
        """
        grid_info = {}
        paired_info = {}

        # Grid parameter info
        if self.config.grid:
            flattened_grid = self._flatten_dict(self.config.grid)
            for param_name, values in flattened_grid.items():
                grid_info[param_name] = {
                    "type": "grid",
                    "values": values,
                    "count": len(values),
                    "value_types": list(set(type(v).__name__ for v in values)),
                }

        # Paired parameter info
        for i, group in enumerate(self.config.paired):
            group_info = {}
            for group_name, params in group.items():
                flat_params = self._flatten_dict(params)
                for param_name, values in flat_params.items():
                    group_info[param_name] = {
                        "type": "paired",
                        "group": i,
                        "group_name": group_name,
                        "values": values,
                        "count": len(values),
                        "value_types": list(set(type(v).__name__ for v in values)),
                    }
            paired_info.update(group_info)

        return {
            "grid_parameters": grid_info,
            "paired_parameters": paired_info,
            "total_combinations": self.count_combinations(),
            "grid_combinations": self._count_grid_combinations(),
            "paired_combinations": self._count_paired_combinations(),
            "parameter_summary": {
                "grid_param_count": len(grid_info),
                "paired_param_count": len(paired_info),
                "total_param_count": len(grid_info) + len(paired_info),
            },
        }

    def validate_parameters(self) -> List[str]:
        """Validate parameter configuration.

        Returns:
            List of validation error messages
        """
        errors = []

        # Validate grid parameters
        for param_name, values in self.config.grid.items():
            if not isinstance(values, list):
                errors.append(f"Grid parameter '{param_name}' must be a list")
            elif len(values) == 0:
                errors.append(f"Grid parameter '{param_name}' cannot be empty")

            # Check for None values in inappropriate contexts
            if None in values and len(values) > 1:
                errors.append(
                    f"Grid parameter '{param_name}' contains None mixed with other values"
                )

        # Validate paired parameters
        for i, group in enumerate(self.config.paired):
            if not isinstance(group, dict):
                errors.append(f"Paired group {i} must be a dictionary")
                continue

            # Collect all parameter lengths in this group
            group_lengths = []
            group_params = []

            for group_name, params in group.items():
                if not isinstance(params, dict):
                    errors.append(
                        f"Paired group '{group_name}' must contain parameter dictionaries"
                    )
                    continue

                for param_name, values in params.items():
                    group_params.append(f"{group_name}.{param_name}")
                    if not isinstance(values, list):
                        errors.append(
                            f"Paired parameter '{param_name}' in group '{group_name}' must be a list"
                        )
                    elif len(values) == 0:
                        errors.append(
                            f"Paired parameter '{param_name}' in group '{group_name}' cannot be empty"
                        )
                    else:
                        group_lengths.append(len(values))

            # Check length consistency within group
            if group_lengths and not all(length == group_lengths[0] for length in group_lengths):
                errors.append(
                    f"All parameters in paired group {i} must have the same length. "
                    f"Found lengths: {group_lengths} for parameters: {group_params}"
                )

        return errors

    def _generate_grid_combinations(self) -> List[Dict[str, Any]]:
        """Generate grid parameter combinations.

        Returns:
            List of parameter dictionaries for grid combinations
        """
        if not self.config.grid:
            return [{}]

        flattened = self._flatten_dict(self.config.grid)
        if not flattened:
            return [{}]

        names = list(flattened.keys())
        values = [flattened[name] for name in names]

        combinations = []
        try:
            for combo in itertools.product(*values):
                combinations.append(dict(zip(names, combo)))
        except Exception as e:
            logger.error(f"Error generating grid combinations: {e}")
            return [{}]

        return combinations

    def _generate_paired_combinations(self) -> List[Dict[str, Any]]:
        """Generate paired parameter combinations.

        Returns:
            List of parameter dictionaries for paired combinations
        """
        if not self.config.paired:
            return [{}]

        all_paired_combinations = []

        for group in self.config.paired:
            # Flatten each group
            flat_group = {}
            for group_name, params in group.items():
                flat_params = self._flatten_dict(params)
                flat_group.update(flat_params)

            if not flat_group:
                continue

            # Verify all parameters have same length
            lengths = [len(values) for values in flat_group.values()]
            if not all(length == lengths[0] for length in lengths):
                logger.error(f"Paired parameters have inconsistent lengths: {lengths}")
                continue

            # Create combinations by zipping parameter values
            param_names = list(flat_group.keys())
            param_values = [flat_group[name] for name in param_names]

            try:
                group_combinations = []
                for combination in zip(*param_values):
                    combo_dict = dict(zip(param_names, combination))
                    group_combinations.append(combo_dict)

                all_paired_combinations.extend(group_combinations)
            except Exception as e:
                logger.error(f"Error generating paired combinations for group: {e}")

        return all_paired_combinations if all_paired_combinations else [{}]

    def _count_grid_combinations(self) -> int:
        """Count grid combinations without generating them.

        Returns:
            Number of grid combinations
        """
        if not self.config.grid:
            return 1

        flattened = self._flatten_dict(self.config.grid)
        if not flattened:
            return 1

        count = 1
        for values in flattened.values():
            count *= len(values)

        return count

    def _count_paired_combinations(self) -> int:
        """Count paired combinations without generating them.

        Returns:
            Number of paired combinations
        """
        if not self.config.paired:
            return 0

        total_count = 0
        for group in self.config.paired:
            # Get the length of the first parameter in the group
            for group_name, params in group.items():
                flat_params = self._flatten_dict(params)
                if flat_params:
                    # All params in a group have same length, so take any one
                    group_length = len(next(iter(flat_params.values())))
                    total_count += group_length
                    break

        return total_count

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
                # Handle nested lists by converting them to tuples for JSON serialization
                if isinstance(v, list) and v and isinstance(v[0], list):
                    v = [tuple(item) if isinstance(item, list) else item for item in v]
                items.append((new_key, v))
        return dict(items)

    def create_command_line_args(self, params: Dict[str, Any]) -> List[str]:
        """Convert parameter dictionary to command line arguments for Hydra.

        Args:
            params: Parameter dictionary

        Returns:
            List of command line argument strings
        """
        args = []
        for key, value in params.items():
            if isinstance(value, (list, tuple)):
                # Convert list/tuple to Hydra format: [item1,item2,...]
                value_str = str(list(value))  # Ensure it's in list format
                args.append(f"{key}={value_str}")
            elif value is None:
                args.append(f"{key}=null")
            elif isinstance(value, bool):
                args.append(f"{key}={str(value).lower()}")
            elif isinstance(value, str) and (" " in value or "," in value):
                # Quote strings that contain spaces or commas
                args.append(f'"{key}={value}"')
            else:
                args.append(f"{key}={value}")

        return args

    def get_parameter_ranges(self) -> Dict[str, Tuple[Any, Any]]:
        """Get min/max ranges for numeric parameters.

        Returns:
            Dictionary mapping parameter names to (min, max) tuples
        """
        ranges = {}

        # Grid parameters
        flattened_grid = self._flatten_dict(self.config.grid)
        for param_name, values in flattened_grid.items():
            numeric_values = [v for v in values if isinstance(v, (int, float))]
            if numeric_values:
                ranges[param_name] = (min(numeric_values), max(numeric_values))

        # Paired parameters
        for group in self.config.paired:
            for group_name, params in group.items():
                flat_params = self._flatten_dict(params)
                for param_name, values in flat_params.items():
                    numeric_values = [v for v in values if isinstance(v, (int, float))]
                    if numeric_values:
                        ranges[param_name] = (min(numeric_values), max(numeric_values))

        return ranges

    def suggest_sweep_ranges(self, parameters: Dict[str, Any]) -> Dict[str, List[Any]]:
        """Suggest sweep ranges for given parameters.

        Args:
            parameters: Dictionary of parameter names to base values

        Returns:
            Dictionary of suggested parameter ranges
        """
        suggestions = {}

        for param_name, base_value in parameters.items():
            if isinstance(base_value, (int, float)):
                if isinstance(base_value, int):
                    # Integer parameter - suggest range around base value
                    suggestions[param_name] = [
                        max(1, base_value // 2),
                        base_value,
                        base_value * 2,
                        base_value * 4,
                    ]
                else:
                    # Float parameter - suggest logarithmic range
                    suggestions[param_name] = [
                        base_value / 10,
                        base_value / 3,
                        base_value,
                        base_value * 3,
                        base_value * 10,
                    ]
            elif isinstance(base_value, bool):
                # Boolean parameter
                suggestions[param_name] = [True, False]
            elif isinstance(base_value, str):
                # String parameter - just include the base value
                suggestions[param_name] = [base_value]

        return suggestions

    def estimate_runtime(self, avg_task_time: float) -> Dict[str, Any]:
        """Estimate total runtime for the sweep.

        Args:
            avg_task_time: Average time per task in seconds

        Returns:
            Dictionary with runtime estimates
        """
        total_combinations = self.count_combinations()
        total_time_seconds = total_combinations * avg_task_time

        # Convert to human-readable format
        hours = total_time_seconds // 3600
        minutes = (total_time_seconds % 3600) // 60
        seconds = total_time_seconds % 60

        return {
            "total_combinations": total_combinations,
            "avg_task_time_seconds": avg_task_time,
            "total_time_seconds": total_time_seconds,
            "total_time_formatted": f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}",
            "total_time_hours": total_time_seconds / 3600,
        }
