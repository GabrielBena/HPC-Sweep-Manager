"""Tests for ``_ensure_machine_config`` in ``cli/init.py``.

Covers the three branches the user-visible behavior splits into:
1. Machine config already exists → log "using existing", leave it alone.
2. Doesn't exist, no TTY (or no disk candidates) → write commented stub.
3. Doesn't exist, TTY + candidates detected → prompt, write active stub
   with the chosen ``sweeps_root``.

We monkeypatch ``MACHINE_CONFIG_PATH`` (imported by ``cli/init.py``) to
land inside ``tmp_path``, so tests never touch the real ``~/.hsm/``.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from rich.console import Console

from hpc_sweep_manager.cli import init as init_mod


def _patch_machine_path(monkeypatch, target: Path) -> None:
    """Redirect MACHINE_CONFIG_PATH to ``target`` for the duration of a test."""
    monkeypatch.setattr(init_mod, "MACHINE_CONFIG_PATH", target)


def _silent_console() -> Console:
    # Force a non-terminal console so rich doesn't try to detect width.
    return Console(record=True, force_terminal=False)


class TestEnsureMachineConfig:
    def test_existing_file_left_alone(self, monkeypatch, tmp_path):
        machine_path = tmp_path / ".hsm" / "config.yaml"
        machine_path.parent.mkdir()
        original = "local:\n  sweeps_root: /already/here\n"
        machine_path.write_text(original)

        _patch_machine_path(monkeypatch, machine_path)
        # Force non-TTY so we wouldn't prompt anyway.
        monkeypatch.setattr(init_mod.sys.stdin, "isatty", lambda: False)

        init_mod._ensure_machine_config(_silent_console())

        assert machine_path.read_text() == original

    def test_no_tty_writes_commented_stub(self, monkeypatch, tmp_path):
        machine_path = tmp_path / ".hsm" / "config.yaml"
        _patch_machine_path(monkeypatch, machine_path)
        monkeypatch.setattr(init_mod.sys.stdin, "isatty", lambda: False)

        init_mod._ensure_machine_config(_silent_console())

        content = machine_path.read_text()
        # Stub form: `local:` exists only as a commented hint.
        assert "# local:" in content
        # Not active.
        assert "\nlocal:\n" not in content

    def test_tty_no_candidates_writes_stub(self, monkeypatch, tmp_path):
        machine_path = tmp_path / ".hsm" / "config.yaml"
        _patch_machine_path(monkeypatch, machine_path)
        monkeypatch.setattr(init_mod.sys.stdin, "isatty", lambda: True)
        # No candidate disks.
        monkeypatch.setattr(init_mod, "_detect_candidate_sweeps_roots", lambda: [])

        init_mod._ensure_machine_config(_silent_console())

        content = machine_path.read_text()
        assert "# local:" in content
        assert "\nlocal:\n" not in content

    def test_tty_with_candidates_picks_default(self, monkeypatch, tmp_path):
        machine_path = tmp_path / ".hsm" / "config.yaml"
        _patch_machine_path(monkeypatch, machine_path)
        monkeypatch.setattr(init_mod.sys.stdin, "isatty", lambda: True)

        # Two fake candidates; first one (largest) is the default.
        big = tmp_path / "mnt" / "8TB"
        big.mkdir(parents=True)
        small = tmp_path / "mnt" / "1TB"
        small.mkdir(parents=True)
        monkeypatch.setattr(
            init_mod,
            "_detect_candidate_sweeps_roots",
            lambda: [(big, 8 * 1024**4), (small, 1 * 1024**4)],
        )
        # Auto-accept the default ("1").
        monkeypatch.setattr(init_mod.Prompt, "ask", lambda *a, **kw: "1")

        init_mod._ensure_machine_config(_silent_console())

        content = machine_path.read_text()
        # The active form should win — `local:` uncommented + sweeps_root set
        # to the largest candidate / hsm-sweeps.
        assert "\nlocal:\n" in content
        assert f"sweeps_root: {big / 'hsm-sweeps'}" in content
        # Pre-creates the chosen sweeps_root so the first sweep doesn't fail.
        assert (big / "hsm-sweeps").is_dir()

    def test_tty_user_picks_skip_writes_stub(self, monkeypatch, tmp_path):
        machine_path = tmp_path / ".hsm" / "config.yaml"
        _patch_machine_path(monkeypatch, machine_path)
        monkeypatch.setattr(init_mod.sys.stdin, "isatty", lambda: True)

        candidate = tmp_path / "mnt" / "data"
        candidate.mkdir(parents=True)
        monkeypatch.setattr(
            init_mod,
            "_detect_candidate_sweeps_roots",
            lambda: [(candidate, 5 * 1024**4)],
        )
        # The "skip" choice is the LAST option (n_candidates + 2).
        # 1 candidate → choices are "1" (candidate), "2" (other), "3" (skip).
        monkeypatch.setattr(init_mod.Prompt, "ask", lambda *a, **kw: "3")

        init_mod._ensure_machine_config(_silent_console())

        content = machine_path.read_text()
        assert "# local:" in content
        assert "\nlocal:\n" not in content


class TestDetectCandidateSweepsRoots:
    """Lightly exercise the disk-detection helper — the value of a unit test
    here is mostly that it doesn't blow up when /mnt etc. don't exist."""

    def test_returns_empty_when_no_parents_exist(self, monkeypatch, tmp_path):
        # Redirect home so the home-dir filter doesn't accidentally hide
        # tmp_path subdirs in other tests.
        monkeypatch.setenv("HOME", str(tmp_path / "fake-home"))
        # The function probes /mnt, /data, /scratch which may or may not
        # exist on the test runner. We just want it to return a list and
        # not raise.
        result = init_mod._detect_candidate_sweeps_roots()
        assert isinstance(result, list)
