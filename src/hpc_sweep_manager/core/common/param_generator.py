"""Parameter combination generation for sweeps."""

import itertools
from typing import Any, Dict, List, Optional


class ParameterGenerator:
    """Generate parameter combinations from sweep configuration."""

    def __init__(self, config):
        """Initialize with a SweepConfig object."""
        self.config = config

    def generate_combinations(self, max_runs: Optional[int] = None) -> List[Dict[str, Any]]:
        """Generate all parameter combinations."""
        grid_combinations = self._generate_grid_combinations()
        paired_combinations = self._generate_paired_combinations()

        final_combinations = []

        # Combine grid and paired parameters
        for grid_combo in grid_combinations:
            if not paired_combinations:
                final_combinations.append(grid_combo)
            else:
                for paired_combo in paired_combinations:
                    combo = {**grid_combo, **paired_combo}
                    final_combinations.append(combo)

        # Apply run limit if specified
        if max_runs is not None and max_runs < len(final_combinations):
            final_combinations = final_combinations[:max_runs]

        return final_combinations

    def count_combinations(self) -> int:
        """Count total number of parameter combinations without generating them."""
        grid_count = self._count_grid_combinations()
        paired_count = self._count_paired_combinations()

        if paired_count == 0:
            return grid_count
        else:
            return grid_count * paired_count

    def _generate_grid_combinations(self) -> List[Dict[str, Any]]:
        """Generate grid parameter combinations."""
        if not self.config.grid:
            return [{}]

        flattened = self._flatten_dict(self.config.grid)
        names = list(flattened.keys())
        values = [flattened[name] for name in names]

        combinations = []
        for combo in itertools.product(*values):
            combinations.append(dict(zip(names, combo)))

        return combinations

    def _generate_paired_combinations(self) -> List[Dict[str, Any]]:
        """Generate paired parameter combinations."""
        if not self.config.paired:
            return [{}]

        all_paired_combinations = []

        for group in self.config.paired:
            # Flatten each group
            flat_group = {}
            for group_name, params in group.items():
                flat_params = self._flatten_dict(params)
                flat_group.update(flat_params)

            # Verify all parameters have same length
            lengths = [len(values) for values in flat_group.values()]
            if not all(length == lengths[0] for length in lengths):
                raise ValueError(
                    f"All paired parameters must have same length. Found lengths: {lengths}"
                )

            # Create combinations by zipping parameter values
            param_names = list(flat_group.keys())
            param_values = [flat_group[name] for name in param_names]

            group_combinations = []
            for combination in zip(*param_values):
                combo_dict = dict(zip(param_names, combination))
                group_combinations.append(combo_dict)

            all_paired_combinations.extend(group_combinations)

        return all_paired_combinations if all_paired_combinations else [{}]

    def _count_grid_combinations(self) -> int:
        """Count grid combinations without generating them."""
        if not self.config.grid:
            return 1

        flattened = self._flatten_dict(self.config.grid)
        count = 1
        for values in flattened.values():
            count *= len(values)

        return count

    def _count_paired_combinations(self) -> int:
        """Count paired combinations without generating them."""
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
        """Flatten nested dictionary with dot notation."""
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

    def get_parameter_info(self) -> Dict[str, Any]:
        """Get detailed information about parameters and combinations."""
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
                    }
            paired_info.update(group_info)

        return {
            "grid_parameters": grid_info,
            "paired_parameters": paired_info,
            "total_combinations": self.count_combinations(),
            "grid_combinations": self._count_grid_combinations(),
            "paired_combinations": self._count_paired_combinations(),
        }

    def preview_combinations(self, max_preview: int = 5) -> List[Dict[str, Any]]:
        """Generate a preview of parameter combinations."""
        all_combinations = self.generate_combinations()
        return all_combinations[:max_preview]

    def create_command_line_args(self, params: Dict[str, Any]) -> List[str]:
        """Convert parameter dictionary to command line arguments for Hydra."""
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
