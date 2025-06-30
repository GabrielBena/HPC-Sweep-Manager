"""
Tests for the main CLI entry point and command shortcuts.
"""

import pytest

pytestmark = pytest.mark.cli
from click.testing import CliRunner
from unittest.mock import patch, MagicMock

from hsm.cli.main import cli


class TestMainCLI:
    """Test the main CLI entry point."""

    def test_cli_help(self, cli_runner):
        """Test main CLI help output."""
        result = cli_runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "HPC Sweep Manager v2" in result.output
        assert "Commands:" in result.output
        assert "run" in result.output
        assert "status" in result.output
        assert "config" in result.output

    def test_cli_version(self, cli_runner):
        """Test version command."""
        result = cli_runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "2.0.0" in result.output

    def test_cli_verbose_flag(self, cli_runner):
        """Test verbose flag is properly passed."""
        with patch("hsm.cli.main.setup_logging") as mock_logging:
            result = cli_runner.invoke(cli, ["--verbose", "--help"])
            assert result.exit_code == 0
            mock_logging.assert_called_once_with("DEBUG")

    def test_cli_quiet_flag(self, cli_runner):
        """Test quiet flag is properly passed."""
        with patch("hsm.cli.main.setup_logging") as mock_logging:
            result = cli_runner.invoke(cli, ["--quiet", "--help"])
            assert result.exit_code == 0
            mock_logging.assert_called_once_with("ERROR")


class TestCLIShortcuts:
    """Test CLI command shortcuts."""

    @patch("hsm.cli.sweep.sweep_cmd")
    def test_run_shortcut(self, mock_sweep_cmd, cli_runner):
        """Test 'hsm run' shortcut invokes sweep run command."""
        mock_command = MagicMock()
        mock_sweep_cmd.commands = {"run": mock_command}

        result = cli_runner.invoke(
            cli, ["run", "--config", "test.yaml", "--sources", "local", "--dry-run"]
        )

        # The command should be invoked
        mock_command.invoke.assert_called_once()

    @patch("hsm.cli.sweep.sweep_cmd")
    def test_complete_shortcut(self, mock_sweep_cmd, cli_runner):
        """Test 'hsm complete' shortcut invokes sweep complete command."""
        mock_command = MagicMock()
        mock_sweep_cmd.commands = {"complete": mock_command}

        result = cli_runner.invoke(cli, ["complete", "test_sweep_id", "--sources", "local"])

        mock_command.invoke.assert_called_once()

    @patch("hsm.cli.sweep.sweep_cmd")
    def test_status_shortcut_no_watch(self, mock_sweep_cmd, cli_runner):
        """Test 'hsm status' shortcut without watch flag."""
        mock_command = MagicMock()
        mock_sweep_cmd.commands = {"status": mock_command}

        result = cli_runner.invoke(cli, ["status", "test_sweep_id"])

        mock_command.invoke.assert_called_once()

    @patch("hsm.cli.monitor.monitor")
    def test_status_shortcut_with_watch(self, mock_monitor, cli_runner):
        """Test 'hsm status --watch' shortcut invokes monitor watch command."""
        mock_command = MagicMock()
        mock_monitor.commands = {"watch": mock_command}

        result = cli_runner.invoke(cli, ["status", "--watch", "test_sweep_id"])

        mock_command.invoke.assert_called_once()

    @patch("hsm.cli.sweep.sweep_cmd")
    def test_cancel_shortcut(self, mock_sweep_cmd, cli_runner):
        """Test 'hsm cancel' shortcut invokes sweep cancel command."""
        mock_command = MagicMock()
        mock_sweep_cmd.commands = {"cancel": mock_command}

        result = cli_runner.invoke(cli, ["cancel", "test_sweep_id", "--force"])

        mock_command.invoke.assert_called_once()


class TestEnvironmentValidation:
    """Test environment validation on startup."""

    @patch("hsm.cli.main.validate_environment")
    def test_environment_validation_success(
        self, mock_validate, cli_runner, valid_validation_result
    ):
        """Test successful environment validation."""
        mock_validate.return_value = valid_validation_result

        result = cli_runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        mock_validate.assert_called_once()

    @patch("hsm.cli.main.validate_environment")
    def test_environment_validation_warnings(
        self, mock_validate, cli_runner, validation_result_with_warnings
    ):
        """Test environment validation with warnings."""
        mock_validate.return_value = validation_result_with_warnings

        result = cli_runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Warning: Environment validation issues detected" in result.output
        assert "Test warning" in result.output


class TestCLIErrorHandling:
    """Test CLI error handling scenarios."""

    def test_invalid_command(self, cli_runner):
        """Test invalid command handling."""
        result = cli_runner.invoke(cli, ["invalid_command"])
        assert result.exit_code != 0
        assert "No such command" in result.output

    def test_missing_required_argument(self, cli_runner):
        """Test missing required argument handling."""
        result = cli_runner.invoke(cli, ["complete"])  # Missing sweep_id
        assert result.exit_code != 0
        assert "Missing argument" in result.output

    @patch("hsm.cli.main.validate_environment")
    def test_environment_validation_failure(
        self, mock_validate, cli_runner, invalid_validation_result
    ):
        """Test handling of environment validation failures."""
        mock_validate.return_value = invalid_validation_result

        # Should still work but show warnings
        result = cli_runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Warning: Environment validation issues detected" in result.output
