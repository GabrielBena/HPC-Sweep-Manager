"""The version string: pyproject.toml holds it once; a source checkout appends its git SHA."""

from __future__ import annotations

from importlib.metadata import version

import hpc_sweep_manager
from hpc_sweep_manager import _resolve_version

INSTALLED = version("hpc-sweep-manager")  # pyproject's, as of the last install


class TestResolveVersion:
    def test_base_when_no_git_sha(self, monkeypatch):
        monkeypatch.setattr(hpc_sweep_manager, "_git_short_sha", lambda: None)
        assert _resolve_version() == INSTALLED

    def test_appends_git_sha_suffix(self, monkeypatch):
        monkeypatch.setattr(hpc_sweep_manager, "_git_short_sha", lambda: "abc1234")
        assert _resolve_version() == f"{INSTALLED}+gabc1234"

    def test_module_version_starts_with_base(self):
        assert hpc_sweep_manager.__version__.startswith(INSTALLED)

    def test_git_short_sha_never_raises(self):
        # Must degrade gracefully (returns None) rather than break import.
        result = hpc_sweep_manager._git_short_sha()
        assert result is None or isinstance(result, str)
