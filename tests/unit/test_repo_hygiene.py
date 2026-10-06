"""Repository hygiene: no tracked file grows past what a person could have written.

A scripted docs edit once replaced an empty slice and inserted a section between every
character of SSH_EXECUTION.md (30 MB); nothing else in CI looks at docs.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MAX_BYTES = 1_000_000


def test_no_tracked_file_is_huge():
    files = subprocess.run(
        ["git", "ls-files", "-z"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.split("\0")
    sizes = {f: (ROOT / f).stat().st_size for f in files if f and (ROOT / f).is_file()}
    assert not {f: n for f, n in sizes.items() if n > MAX_BYTES}
