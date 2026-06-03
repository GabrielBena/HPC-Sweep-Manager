"""Status-honesty + re-run contract tests for ``hsm setup init`` (B1/G1,
field report 2026-06-03).

The report's theme: the reported status must match reality. Init used to
crash on an undefined ``interactive`` AFTER writing every file (false
"failed"), exit 0 regardless of success, clobber a hand-edited
``.hsm/config.yaml`` on re-run with no backup, and prompt on the migration
path even when non-interactive.
"""

from __future__ import annotations

import io
import logging

from click.testing import CliRunner
import pytest
from rich.console import Console

from hpc_sweep_manager.cli import init as init_mod
from hpc_sweep_manager.cli.init import init_cmd, init_project


@pytest.fixture()
def project(tmp_path, monkeypatch):
    """A minimal consumer project + safe machine-config redirection."""
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "train.py").write_text("print('hi')\n")
    # Never touch the real ~/.hsm; force the non-TTY stub branch.
    monkeypatch.setattr(init_mod, "MACHINE_CONFIG_PATH", tmp_path / "machine" / "config.yaml")
    monkeypatch.setattr(init_mod.sys.stdin, "isatty", lambda: False)
    return proj


def _console():
    buf = io.StringIO()
    return Console(file=buf, width=200), buf


def _null_logger():
    # pytest's log_cli handler writes through the streams CliRunner swaps
    # out (the Click-8 + log_cli interaction noted in CLAUDE.md) — keep CLI
    # invocations logging-silent so the capture machinery stays intact.
    lg = logging.getLogger("hsm-test-null")
    lg.handlers = [logging.NullHandler()]
    lg.propagate = False
    return lg


def _invoke_cli(proj):
    buf = io.StringIO()
    obj = {"console": Console(file=buf, width=200), "logger": _null_logger()}
    res = CliRunner().invoke(
        init_cmd, ["--project-root", str(proj)], obj=obj, catch_exceptions=False
    )
    return res, buf.getvalue()


class TestNonInteractiveInitSucceeds:
    """B1: a non-interactive init must report success AND exit 0 — the
    NameError used to fire after all files were written, printing
    '❌ Project initialization failed!' for a successful init."""

    def test_exit_zero_and_files_present(self, project):
        res, out = _invoke_cli(project)
        assert res.exit_code == 0, out
        assert "Project initialization completed successfully" in out
        assert "failed" not in out.lower()
        for rel in (
            ".hsm/config.yaml",
            "sweeps/example_sweep.yaml",
            "sweeps/README.md",
        ):
            assert (project / rel).is_file(), rel

    def test_real_failure_exits_nonzero(self, project, monkeypatch):
        # The inverse lie: a real failure used to print ❌ but exit 0.
        monkeypatch.setattr(
            init_mod, "_create_sweep_infrastructure", lambda *a, **k: False
        )
        buf = io.StringIO()
        obj = {"console": Console(file=buf, width=200), "logger": _null_logger()}
        res = CliRunner().invoke(
            init_cmd, ["--project-root", str(project)], obj=obj
        )
        assert res.exit_code != 0
        assert "Project initialization failed" in buf.getvalue()


class TestRerunContract:
    """G1: re-running init regenerates the three generated files, backs up
    the previous config.yaml, and leaves other sweep configs alone."""

    def test_rerun_backs_up_config_and_spares_other_sweeps(self, project):
        console, _ = _console()
        assert init_project(project, False, console, logging.getLogger("t")) is True

        # Hand edits between runs — exactly what the field agent did.
        config_path = project / ".hsm" / "config.yaml"
        edited = config_path.read_text() + "\n# my hand edit\n"
        config_path.write_text(edited)
        my_sweep = project / "sweeps" / "my_sweep.yaml"
        my_sweep.write_text("sweep:\n  grid:\n    seed: [1]\n")

        console, out_buf = _console()
        assert init_project(project, False, console, logging.getLogger("t")) is True

        backup = project / ".hsm" / "config.yaml.bak"
        assert backup.is_file()
        assert backup.read_text() == edited  # the previous copy, hand edit intact
        assert config_path.read_text() != edited  # regenerated
        assert my_sweep.read_text() == "sweep:\n  grid:\n    seed: [1]\n"
        assert "backed up" in out_buf.getvalue()

    def test_first_run_writes_no_backup(self, project):
        console, _ = _console()
        init_project(project, False, console, logging.getLogger("t"))
        assert not (project / ".hsm" / "config.yaml.bak").exists()


class TestMigrationNeverPromptsNonInteractive:
    def test_old_config_migrates_without_prompt(self, project, monkeypatch):
        (project / "sweeps").mkdir()
        (project / "sweeps" / "hsm_config.yaml").write_text(
            "project:\n  name: oldproj\npaths:\n  train_script: train.py\n"
        )

        def _tripwire(*a, **k):  # pragma: no cover - failure path
            raise AssertionError("non-interactive init must never prompt")

        monkeypatch.setattr(init_mod.Confirm, "ask", _tripwire)

        console, out_buf = _console()
        assert init_project(project, False, console, logging.getLogger("t")) is True
        # Old file kept (deleting it is the interactive-consent path).
        assert (project / "sweeps" / "hsm_config.yaml").is_file()
        assert "Kept old config" in out_buf.getvalue()
