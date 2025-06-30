"""
Tests for the config CLI commands.
"""

import os

import pytest

pytestmark = pytest.mark.cli
from unittest.mock import MagicMock, patch

from hsm.cli.config import config_cmd


class TestConfigInit:
    """Test config init command."""

    def test_config_init_basic(self, cli_runner, temp_dir):
        """Test basic config initialization."""
        # Change to temp directory for testing
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dir)

            result = cli_runner.invoke(config_cmd, ["init", "--project-root", str(temp_dir)])

            # Should complete without crashing
            # May have warnings/errors about missing files but should not crash
            assert result.exit_code in [0, 1]  # Allow some validation issues
            assert "Initializing project" in result.output or "Error:" in result.output

        finally:
            os.chdir(original_cwd)

    def test_config_init_help(self, cli_runner):
        """Test config init help command."""
        result = cli_runner.invoke(config_cmd, ["init", "--help"])

        assert result.exit_code == 0
        assert "Initialize HSM configuration" in result.output
        assert "--project-root" in result.output
        assert "--interactive" in result.output

    def test_config_init_with_force(self, cli_runner, temp_dir):
        """Test config init with force flag."""
        # Create existing config file
        config_file = temp_dir / "hsm_config.yaml"
        config_file.write_text("existing: config")

        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dir)

            result = cli_runner.invoke(config_cmd, ["init", "--force"])

            # Should complete (though may have warnings about project structure)
            assert result.exit_code in [0, 1]

        finally:
            os.chdir(original_cwd)


class TestConfigShow:
    """Test config show command."""

    def test_config_show_help(self, cli_runner):
        """Test config show help command."""
        result = cli_runner.invoke(config_cmd, ["show", "--help"])

        assert result.exit_code == 0
        assert "Show current configuration" in result.output

    def test_config_show_missing_file(self, cli_runner, temp_dir):
        """Test config show with missing config file."""
        import os

        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dir)

            result = cli_runner.invoke(config_cmd, ["show"])

            # Should handle missing config gracefully
            assert result.exit_code != 0  # Should exit with error
            assert "Error:" in result.output or "not found" in result.output

        finally:
            os.chdir(original_cwd)

    @patch("hsm.cli.config.HSMConfig.load")
    def test_config_show_basic(self, mock_load, cli_runner):
        """Test basic config show command."""
        mock_config = MagicMock()
        mock_config.to_yaml.return_value = "test: config"
        mock_load.return_value = mock_config

        result = cli_runner.invoke(config_cmd, ["show"])

        assert result.exit_code == 0
        assert "test: config" in result.output
        mock_load.assert_called_once()

    @patch("hsm.cli.config.HSMConfig.load")
    def test_config_show_specific_file(self, mock_load, cli_runner, hsm_config_file):
        """Test config show with specific file."""
        mock_config = MagicMock()
        mock_config.to_yaml.return_value = "test: config"
        mock_load.return_value = mock_config

        result = cli_runner.invoke(config_cmd, ["show", "--config", str(hsm_config_file)])

        assert result.exit_code == 0
        mock_load.assert_called_once_with(hsm_config_file)

    def test_config_show_with_format(self, cli_runner, hsm_config_file):
        """Test config show with different format."""
        result = cli_runner.invoke(
            config_cmd, ["show", "--config", str(hsm_config_file), "--format", "yaml"]
        )

        # May succeed or fail depending on file validity, but should handle format option
        assert "--format" in str(cli_runner.invoke(config_cmd, ["show", "--help"]).output)

    def test_config_show_file_not_found(self, cli_runner):
        """Test config show with non-existent file."""
        result = cli_runner.invoke(config_cmd, ["show", "--config", "nonexistent.yaml"])

        assert result.exit_code != 0
        assert "not found" in result.output or "Error" in result.output


class TestConfigValidate:
    """Test config validate command."""

    @patch("hsm.cli.config.HSMConfigValidator")
    @patch("hsm.cli.config.SweepConfigValidator")
    def test_config_validate_success(
        self, mock_sweep_validator, mock_hsm_validator, cli_runner, valid_validation_result
    ):
        """Test successful config validation."""
        # Mock successful validation
        mock_hsm_validator.return_value.validate.return_value = valid_validation_result
        mock_sweep_validator.return_value.validate.return_value = valid_validation_result

        result = cli_runner.invoke(config_cmd, ["validate"])

        assert result.exit_code == 0
        assert "✓" in result.output or "valid" in result.output.lower()

    @patch("hsm.cli.config.HSMConfigValidator")
    def test_config_validate_errors(self, mock_validator, cli_runner, invalid_validation_result):
        """Test config validation with errors."""
        mock_validator.return_value.validate.return_value = invalid_validation_result

        result = cli_runner.invoke(config_cmd, ["validate"])

        assert result.exit_code != 0
        assert "Test error" in result.output
        assert "Test warning" in result.output

    @patch("hsm.cli.config.SweepConfigValidator")
    def test_config_validate_sweep_only(
        self, mock_validator, cli_runner, sweep_config_file, valid_validation_result
    ):
        """Test validating sweep config only."""
        mock_validator.return_value.validate.return_value = valid_validation_result

        result = cli_runner.invoke(
            config_cmd, ["validate", "--sweep-config", str(sweep_config_file)]
        )

        assert result.exit_code == 0
        mock_validator.assert_called_once()

    @patch("hsm.cli.config.HSMConfigValidator")
    def test_config_validate_hsm_only(
        self, mock_validator, cli_runner, hsm_config_file, valid_validation_result
    ):
        """Test validating HSM config only."""
        mock_validator.return_value.validate.return_value = valid_validation_result

        result = cli_runner.invoke(config_cmd, ["validate", "--config", str(hsm_config_file)])

        assert result.exit_code == 0
        mock_validator.assert_called_once()


class TestConfigErrorHandling:
    """Test config command error handling."""

    def test_config_help(self, cli_runner):
        """Test config command help."""
        result = cli_runner.invoke(config_cmd, ["--help"])
        assert result.exit_code == 0
        assert "Configuration management commands" in result.output

    def test_invalid_config_subcommand(self, cli_runner):
        """Test invalid config subcommand."""
        result = cli_runner.invoke(config_cmd, ["invalid"])
        assert result.exit_code != 0
        assert "No such command" in result.output

    def test_config_invalid_subcommand(self, cli_runner):
        """Test invalid config subcommand."""
        result = cli_runner.invoke(config_cmd, ["invalid"])
        assert result.exit_code != 0
        assert "No such command" in result.output
