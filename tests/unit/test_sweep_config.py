"""
Unit tests for SweepConfig class.
"""

import pytest

pytestmark = pytest.mark.unit
import tempfile
from pathlib import Path
import yaml

from hsm.config.sweep import SweepConfig


class TestSweepConfig:
    """Test SweepConfig class."""

    def test_init_with_grid_parameters(self):
        """Test SweepConfig initialization with grid parameters."""
        config_data = {
            "grid": {"learning_rate": [0.001, 0.01, 0.1], "batch_size": [32, 64]},
            "metadata": {"description": "Test sweep", "tags": ["test"]},
        }

        config = SweepConfig(**config_data)

        assert config.grid == config_data["grid"]
        assert config.metadata["description"] == "Test sweep"
        assert config.metadata["tags"] == ["test"]

    def test_init_with_paired_parameters(self):
        """Test SweepConfig initialization with paired parameters."""
        config_data = {
            "paired": [{"model_config": {"hidden_dim": [64, 128], "num_layers": [2, 3]}}]
        }

        config = SweepConfig(**config_data)

        assert config.paired == config_data["paired"]
        assert config.grid == {}

    def test_init_with_mixed_parameters(self):
        """Test SweepConfig initialization with both grid and paired parameters."""
        config_data = {
            "grid": {"learning_rate": [0.001, 0.01]},
            "paired": [{"model_config": {"hidden_dim": [64, 128], "num_layers": [2, 3]}}],
        }

        config = SweepConfig(**config_data)

        assert config.grid == config_data["grid"]
        assert config.paired == config_data["paired"]

    def test_from_yaml_file(self, temp_dir):
        """Test loading SweepConfig from YAML file."""
        config_data = {
            "grid": {"learning_rate": [0.001, 0.01], "batch_size": [32, 64]},
            "metadata": {"description": "Test sweep from file"},
        }

        config_file = temp_dir / "test_sweep.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_data, f)

        config = SweepConfig.from_yaml(config_file)

        assert config.grid == config_data["grid"]
        assert config.metadata["description"] == "Test sweep from file"

    def test_from_dict(self):
        """Test loading SweepConfig from dictionary."""
        config_data = {
            "grid": {"learning_rate": [0.001, 0.01], "batch_size": [32, 64]},
            "metadata": {"description": "Test sweep from dict"},
        }

        config = SweepConfig.from_dict(config_data)

        assert config.grid["learning_rate"] == [0.001, 0.01]
        assert config.grid["batch_size"] == [32, 64]
        assert config.metadata["description"] == "Test sweep from dict"

    def test_save_and_load(self, temp_dir):
        """Test saving and loading SweepConfig."""
        config_data = {
            "grid": {"learning_rate": [0.001, 0.01], "batch_size": [32, 64]},
            "metadata": {"description": "Test sweep", "tags": ["test"]},
        }

        config = SweepConfig(**config_data)

        # Save config
        config_file = temp_dir / "test_config.yaml"
        config.save(config_file)

        # Load it back
        loaded_config = SweepConfig.from_yaml(config_file)
        assert loaded_config.grid == config_data["grid"]
        assert loaded_config.metadata["description"] == "Test sweep"

    def test_to_dict(self):
        """Test converting SweepConfig to dictionary."""
        config_data = {
            "grid": {"learning_rate": [0.001, 0.01], "batch_size": [32, 64]},
            "metadata": {"description": "Test sweep"},
        }

        config = SweepConfig(**config_data)
        config_dict = config.to_dict()

        # The structure nests grid under "sweep"
        assert config_dict["sweep"]["grid"] == config_data["grid"]
        assert config_dict["metadata"]["description"] == "Test sweep"

    def test_validate_valid_config(self):
        """Test validation of valid configuration."""
        config_data = {"grid": {"learning_rate": [0.001, 0.01], "batch_size": [32, 64]}}

        config = SweepConfig(**config_data)
        errors = config.validate()

        assert len(errors) == 0

    def test_validate_empty_config(self):
        """Test validation of empty configuration."""
        config = SweepConfig()
        errors = config.validate()

        # Empty config should be valid (no errors)
        assert len(errors) == 0

    def test_validate_invalid_parameter_values(self):
        """Test validation with invalid parameter values."""
        config_data = {
            "grid": {
                "learning_rate": [],  # Empty list should be invalid
                "batch_size": [32, 64],
            }
        }

        config = SweepConfig(**config_data)
        errors = config.validate()

        assert len(errors) > 0
        assert any("cannot be empty" in error for error in errors)

    def test_get_total_combinations(self):
        """Test estimating number of parameter combinations."""
        config_data = {
            "grid": {
                "learning_rate": [0.001, 0.01, 0.1],  # 3 values
                "batch_size": [32, 64],  # 2 values
            }
        }

        config = SweepConfig(**config_data)
        combinations = config.get_total_combinations()

        assert combinations == 6  # 3 * 2

    def test_generate_parameters(self):
        """Test generating parameter combinations."""
        config_data = {"grid": {"learning_rate": [0.001, 0.01], "batch_size": [32, 64]}}

        config = SweepConfig(**config_data)
        parameters = config.generate_parameters()

        assert len(parameters) == 4  # 2 * 2
        # Check that all combinations are present
        lr_values = {p["learning_rate"] for p in parameters}
        bs_values = {p["batch_size"] for p in parameters}
        assert lr_values == {0.001, 0.01}
        assert bs_values == {32, 64}


class TestSweepConfigErrors:
    """Test SweepConfig error handling."""

    def test_from_yaml_file_not_found(self):
        """Test loading from non-existent file."""
        with pytest.raises(FileNotFoundError):
            SweepConfig.from_yaml("nonexistent.yaml")

    def test_from_yaml_invalid_yaml(self, temp_dir):
        """Test loading from invalid YAML file."""
        config_file = temp_dir / "invalid.yaml"
        config_file.write_text("invalid: yaml: content: [")

        with pytest.raises(yaml.YAMLError):
            SweepConfig.from_yaml(config_file)

    def test_to_dict_structure(self):
        """Test to_dict method structure."""
        config_data = {
            "grid": {"learning_rate": [0.001, 0.01]},
            "metadata": {"description": "Test"},
        }

        config = SweepConfig(**config_data)
        config_dict = config.to_dict()

        assert "sweep" in config_dict
        assert config_dict["sweep"]["grid"] == config_data["grid"]
        assert config_dict["metadata"] == config_data["metadata"]


class TestSweepMetadata:
    """Test SweepMetadata functionality."""

    def test_metadata_with_all_fields(self):
        """Test metadata with all fields populated."""
        config_data = {
            "grid": {"lr": [0.01]},
            "metadata": {
                "description": "Test description",
                "tags": ["test", "grid"],
                "author": "test_user",
                "version": "1.0",
                "notes": "Test notes",
            },
        }

        config = SweepConfig(**config_data)

        assert config.metadata["description"] == "Test description"
        assert config.metadata["tags"] == ["test", "grid"]
        assert config.metadata["author"] == "test_user"
        assert config.metadata["version"] == "1.0"
        assert config.metadata["notes"] == "Test notes"

    def test_metadata_optional_fields(self):
        """Test metadata with only required fields."""
        config_data = {"grid": {"lr": [0.01]}, "metadata": {"description": "Minimal metadata"}}

        config = SweepConfig(**config_data)

        assert config.metadata["description"] == "Minimal metadata"
        assert "tags" not in config.metadata or config.metadata.get("tags") is None
        assert "author" not in config.metadata or config.metadata.get("author") is None

    def test_metadata_defaults(self):
        """Test metadata defaults when not provided."""
        config_data = {"grid": {"lr": [0.01]}}

        config = SweepConfig(**config_data)

        # Should have default metadata (empty dict)
        assert config.metadata is not None
        assert isinstance(config.metadata, dict)
        assert "description" not in config.metadata or config.metadata.get("description") is None
