"""Unit tests for the derived version string (FEEDBACK.md 'Minor' item)."""

from __future__ import annotations

import hpc_sweep_manager
from hpc_sweep_manager import _resolve_version


class TestResolveVersion:
    def test_base_when_no_git_sha(self, monkeypatch):
        monkeypatch.setattr(hpc_sweep_manager, "_git_short_sha", lambda: None)
        assert _resolve_version() == "0.1.0"

    def test_appends_git_sha_suffix(self, monkeypatch):
        monkeypatch.setattr(hpc_sweep_manager, "_git_short_sha", lambda: "abc1234")
        assert _resolve_version() == "0.1.0+gabc1234"

    def test_module_version_starts_with_base(self):
        # Whatever the environment, __version__ is always a valid 0.1.0[+g…].
        assert hpc_sweep_manager.__version__.startswith("0.1.0")

    def test_git_short_sha_never_raises(self):
        # Must degrade gracefully (returns None) rather than break import.
        result = hpc_sweep_manager._git_short_sha()
        assert result is None or isinstance(result, str)
