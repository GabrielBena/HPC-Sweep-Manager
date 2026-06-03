"""Tests for the consumer/agent cold-start additions to `hsm setup init` + `hsm docs`.

Covers the self-contained generated guide, the `hsm docs` command, and — most
importantly — the *consent* behavior of the agent-pointer hook: HSM must never
modify an existing CLAUDE.md/AGENTS.md, and only create one with explicit
interactive opt-in.
"""

from __future__ import annotations

import io
import logging

from click.testing import CliRunner
from rich.console import Console

from hpc_sweep_manager.cli import docs as docs_mod
from hpc_sweep_manager.cli import init as init_mod
from hpc_sweep_manager.cli.docs import docs as docs_cmd
from hpc_sweep_manager.cli.init import (
    _offer_agent_pointer,
    _render_agents_stub,
    _render_project_readme,
)


def _console():
    buf = io.StringIO()
    return Console(file=buf, width=200), buf


class TestGeneratedGuide:
    def test_readme_is_self_contained(self):
        r = _render_project_readme("myproj")
        for s in (
            "myproj",
            "outputs/<sweep_id>/",   # output layout
            "tasks/<task>/",
            "params.yaml",           # self-describing checkpoints
            "--mode array",
            "hsm sweep collect",     # recovery
            "hsm docs",              # docs pointer (not dead local paths)
        ):
            assert s in r, s
        # The dead-link pattern must be gone.
        assert "docs/user_guide/" not in r

    def test_agents_stub_points_at_readme_and_docs(self):
        r = _render_agents_stub("myproj")
        assert "sweeps/README.md" in r
        assert "hsm docs" in r


class TestAgentPointerConsent:
    def test_existing_claude_is_left_untouched(self, tmp_path):
        (tmp_path / "CLAUDE.md").write_text("# my curated notes\n")
        console, buf = _console()
        _offer_agent_pointer(tmp_path, "proj", interactive=True, console=console)
        assert not (tmp_path / "AGENTS.md").exists()
        assert (tmp_path / "CLAUDE.md").read_text() == "# my curated notes\n"
        assert "left untouched" in buf.getvalue()

    def test_existing_agents_is_left_untouched(self, tmp_path):
        (tmp_path / "AGENTS.md").write_text("original\n")
        console, buf = _console()
        _offer_agent_pointer(tmp_path, "proj", interactive=True, console=console)
        assert (tmp_path / "AGENTS.md").read_text() == "original\n"

    def test_non_interactive_writes_nothing(self, tmp_path):
        console, buf = _console()
        _offer_agent_pointer(tmp_path, "proj", interactive=False, console=console)
        assert not (tmp_path / "AGENTS.md").exists()
        assert "No AGENTS.md/CLAUDE.md written" in buf.getvalue()

    def test_interactive_yes_creates_agents(self, tmp_path, monkeypatch):
        monkeypatch.setattr(init_mod.Confirm, "ask", lambda *a, **k: True)
        console, _ = _console()
        _offer_agent_pointer(tmp_path, "proj", interactive=True, console=console)
        agents = tmp_path / "AGENTS.md"
        assert agents.exists()
        assert "sweeps/README.md" in agents.read_text()

    def test_interactive_no_does_not_create(self, tmp_path, monkeypatch):
        monkeypatch.setattr(init_mod.Confirm, "ask", lambda *a, **k: False)
        console, _ = _console()
        _offer_agent_pointer(tmp_path, "proj", interactive=True, console=console)
        assert not (tmp_path / "AGENTS.md").exists()


def _invoke_docs():
    buf = io.StringIO()
    obj = {"console": Console(file=buf, width=200), "logger": logging.getLogger("t")}
    res = CliRunner().invoke(docs_cmd, [], obj=obj, catch_exceptions=False)
    return res, buf.getvalue()


class TestDocsCommand:
    def test_prints_repo_and_guides(self):
        res, out = _invoke_docs()
        assert res.exit_code == 0
        assert "github.com/GabrielBena/HPC-Sweep-Manager" in out
        assert "SSH_EXECUTION.md" in out
        assert "getting_started.md" in out

    def test_shows_local_path_in_this_checkout(self):
        # Running from the source tree → docs/user_guide is present.
        _res, out = _invoke_docs()
        assert "Local copy" in out

    def test_handles_pip_install_without_local_docs(self, monkeypatch):
        monkeypatch.setattr(docs_mod, "_local_docs_dir", lambda: None)
        _res, out = _invoke_docs()
        assert "No local docs" in out
