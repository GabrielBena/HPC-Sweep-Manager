"""Every command answers `--help` (G1's smoke) and has a line in the CLI reference (chore-7)."""

from __future__ import annotations

from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from hpc_sweep_manager.cli.main import cli

DOC = Path(__file__).parents[2] / "docs" / "cli" / "README.md"


def _commands(cmd: click.Command, path: str) -> list[str]:
    subs = cmd.commands.items() if isinstance(cmd, click.Group) else ()
    return [path] + [c for n, s in subs if not s.hidden for c in _commands(s, f"{path} {n}")]


def test_every_command_is_in_the_cli_reference():
    doc = DOC.read_text()
    assert [c for c in _commands(cli, "hsm") if f"`{c}" not in doc] == []


@pytest.mark.parametrize("command", _commands(cli, "hsm"))
def test_every_command_answers_help(command):
    res = CliRunner().invoke(cli, [*command.split()[1:], "--help"])
    assert res.exit_code == 0, res.output
