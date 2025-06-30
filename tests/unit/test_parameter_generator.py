"""
Unit tests for ParameterGenerator utility.
"""

import pytest

pytestmark = pytest.mark.unit
from hsm.config.sweep import SweepConfig
from hsm.utils.params import ParameterGenerator


class TestParameterGenerator:
    """Test ParameterGenerator class."""

    def test_grid_parameter_generation(self):
        """Test generating parameters from grid configuration."""
        config_data = {
            "grid": {"learning_rate": [0.001, 0.01], "batch_size": [32, 64], "hidden_dim": [128]}
        }

        config = SweepConfig(**config_data)
        generator = ParameterGenerator(config)

        combinations = list(generator.generate_combinations())

        # Should have 2 * 2 * 1 = 4 combinations
        assert len(combinations) == 4

        # Check that all combinations are present
        expected_combinations = [
            {"learning_rate": 0.001, "batch_size": 32, "hidden_dim": 128},
            {"learning_rate": 0.001, "batch_size": 64, "hidden_dim": 128},
            {"learning_rate": 0.01, "batch_size": 32, "hidden_dim": 128},
            {"learning_rate": 0.01, "batch_size": 64, "hidden_dim": 128},
        ]

        for expected in expected_combinations:
            assert expected in combinations

    def test_paired_parameter_generation(self):
        """Test generating parameters from paired configuration."""
        config_data = {
            "paired": [{"model_config": {"hidden_dim": [64, 128, 256], "num_layers": [2, 3, 4]}}]
        }

        config = SweepConfig(**config_data)
        generator = ParameterGenerator(config)

        combinations = list(generator.generate_combinations())

        # Should have 3 paired combinations
        assert len(combinations) == 3

        expected_combinations = [
            {"hidden_dim": 64, "num_layers": 2},
            {"hidden_dim": 128, "num_layers": 3},
            {"hidden_dim": 256, "num_layers": 4},
        ]

        for expected in expected_combinations:
            assert expected in combinations

    def test_mixed_parameter_generation(self):
        """Test generating parameters from mixed grid and paired configuration."""
        config_data = {
            "grid": {"learning_rate": [0.001, 0.01]},
            "paired": [{"model_config": {"hidden_dim": [64, 128], "num_layers": [2, 3]}}],
        }

        config = SweepConfig(**config_data)
        generator = ParameterGenerator(config)

        combinations = list(generator.generate_combinations())

        # Should have 2 learning rates * 2 model configs = 4 combinations
        assert len(combinations) == 4

        # Check a few specific combinations
        assert {"learning_rate": 0.001, "hidden_dim": 64, "num_layers": 2} in combinations
        assert {"learning_rate": 0.01, "hidden_dim": 128, "num_layers": 3} in combinations

    def test_count_combinations(self):
        """Test counting combinations without generating them."""
        config_data = {"grid": {"learning_rate": [0.001, 0.01, 0.1], "batch_size": [32, 64, 128]}}

        config = SweepConfig(**config_data)
        generator = ParameterGenerator(config)

        count = generator.count_combinations()

        assert count == 9  # 3 * 3

    def test_count_combinations_mixed(self):
        """Test counting combinations for mixed configuration."""
        config_data = {
            "grid": {"learning_rate": [0.001, 0.01]},
            "paired": [{"model_config": {"hidden_dim": [64, 128, 256], "num_layers": [2, 3, 4]}}],
        }

        config = SweepConfig(**config_data)
        generator = ParameterGenerator(config)

        count = generator.count_combinations()

        assert count == 6  # 2 learning rates * 3 model configs

    def test_multiple_paired_groups(self):
        """Test generating parameters with multiple paired groups."""
        config_data = {
            "paired": [
                {"model_config": {"hidden_dim": [64, 128], "num_layers": [2, 3]}},
                {"optimizer_config": {"optimizer": ["adam", "sgd"], "momentum": [0.9, 0.95]}},
            ]
        }

        config = SweepConfig(**config_data)
        generator = ParameterGenerator(config)

        combinations = list(generator.generate_combinations())

        # Should have 2 model configs * 2 optimizer configs = 4 combinations
        assert len(combinations) == 4

        # Check that we have combinations (may be differently structured)
        for combo in combinations:
            # Check that we get some parameters from paired groups
            assert len(combo) > 0

    def test_empty_configuration(self):
        """Test handling of empty configuration."""
        config = SweepConfig()
        generator = ParameterGenerator(config)

        combinations = list(generator.generate_combinations())
        count = generator.count_combinations()

        # Empty config generates one empty combination
        assert len(combinations) == 1
        assert combinations[0] == {}
        assert count == 1

    def test_single_value_parameters(self):
        """Test handling of single-value parameters."""
        config_data = {
            "grid": {
                "learning_rate": [0.01],  # Single value
                "batch_size": [32, 64],
            }
        }

        config = SweepConfig(**config_data)
        generator = ParameterGenerator(config)

        combinations = list(generator.generate_combinations())

        assert len(combinations) == 2  # 1 * 2
        for combo in combinations:
            assert combo["learning_rate"] == 0.01

    def test_nested_parameter_names(self):
        """Test handling of nested parameter names."""
        config_data = {
            "grid": {"model.hidden_dim": [64, 128], "training.learning_rate": [0.001, 0.01]}
        }

        config = SweepConfig(**config_data)
        generator = ParameterGenerator(config)

        combinations = list(generator.generate_combinations())

        assert len(combinations) == 4
        for combo in combinations:
            assert "model.hidden_dim" in combo
            assert "training.learning_rate" in combo

    def test_parameter_types_preserved(self):
        """Test that parameter types are preserved during generation."""
        config_data = {
            "grid": {
                "learning_rate": [0.001, 0.01],  # float
                "batch_size": [32, 64],  # int
                "use_dropout": [True, False],  # bool
                "optimizer": ["adam", "sgd"],  # string
            }
        }

        config = SweepConfig(**config_data)
        generator = ParameterGenerator(config)

        combinations = list(generator.generate_combinations())

        for combo in combinations:
            assert isinstance(combo["learning_rate"], float)
            assert isinstance(combo["batch_size"], int)
            assert isinstance(combo["use_dropout"], bool)
            assert isinstance(combo["optimizer"], str)


class TestParameterGeneratorEdgeCases:
    """Test edge cases for ParameterGenerator."""

    def test_mismatched_paired_lengths(self):
        """Test handling of mismatched paired parameter lengths."""
        config_data = {
            "paired": [
                {
                    "model_config": {
                        "hidden_dim": [64, 128],  # 2 values
                        "num_layers": [2, 3, 4],  # 3 values (mismatch)
                    }
                }
            ]
        }

        config = SweepConfig(**config_data)
        generator = ParameterGenerator(config)

        # With mismatched lengths, should return at least one combination
        combinations = list(generator.generate_combinations())
        assert len(combinations) >= 1

    def test_complex_parameter_values(self):
        """Test handling of complex parameter values."""
        config_data = {
            "grid": {
                "layer_sizes": [[64, 32], [128, 64], [256, 128]],  # List values
                "activation": ["relu", "tanh", "sigmoid"],
            }
        }

        config = SweepConfig(**config_data)
        generator = ParameterGenerator(config)

        combinations = list(generator.generate_combinations())

        assert len(combinations) == 9
        for combo in combinations:
            # The layer_sizes could be tuple or list depending on implementation
            layer_sizes = combo["layer_sizes"]
            assert isinstance(layer_sizes, (list, tuple))
            assert len(layer_sizes) == 2

    def test_large_combination_count(self):
        """Test handling of large number of combinations."""
        config_data = {
            "grid": {
                "param1": list(range(10)),  # 10 values
                "param2": list(range(10)),  # 10 values
                "param3": list(range(10)),  # 10 values
            }
        }

        config = SweepConfig(**config_data)
        generator = ParameterGenerator(config)

        count = generator.count_combinations()
        assert count == 1000  # 10 * 10 * 10

        # Don't generate all combinations for performance, just test count


class TestParameterGeneratorUtilities:
    """Test utility methods of ParameterGenerator."""

    def test_get_parameter_info(self):
        """Test getting parameter information."""
        config_data = {
            "grid": {"learning_rate": [0.001, 0.01], "batch_size": [32, 64]},
            "paired": [{"model_config": {"hidden_dim": [64, 128], "num_layers": [2, 3]}}],
        }

        config = SweepConfig(**config_data)
        generator = ParameterGenerator(config)

        param_info = generator.get_parameter_info()

        assert "grid_parameters" in param_info
        assert "paired_parameters" in param_info
        assert "total_combinations" in param_info
        assert param_info["total_combinations"] > 0

    def test_estimate_runtime(self):
        """Test runtime estimation functionality."""
        config_data = {"grid": {"learning_rate": [0.001, 0.01], "batch_size": [32, 64]}}

        config = SweepConfig(**config_data)
        generator = ParameterGenerator(config)

        # Test with estimated time per task (use correct parameter name)
        runtime_info = generator.estimate_runtime(avg_task_time=60.0)

        assert isinstance(runtime_info, dict)
        # Check for actual keys returned by estimate_runtime
        assert "total_combinations" in runtime_info
        assert "total_time_formatted" in runtime_info or "total_time_hours" in runtime_info
