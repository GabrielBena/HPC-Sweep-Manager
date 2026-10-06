#!/usr/bin/env bash
# The chunk cap: count a chunk's hand-written changed lines against its base (library source only:
# no tests, research (the findings, and the passes, filed verbatim), prose, lockfiles, generated json,
# the editor's workspace) and fail above the cap, so that a chunk stays readable in one sitting.
# Renames are paired over the whole diff and a file is judged by where it lands, so a moved file
# costs its edits only.
# Canonical in EIS-Hub/loom and vendored verbatim into EIS-Hub/mosaic-sim; loom's scripts/audit.sh
# flags any drift (loom's DISCIPLINE.md, the method).
set -euo pipefail
base="${1:?base ref}"; cap="${2:-150}"
n=$(git diff -M --numstat -z "$base...HEAD" | python3 -c '
import fnmatch, sys
EXCLUDED = ("tests/*", "research/*", "*.md", "*.json", "*.lock", "*.code-workspace")
f, n, i = sys.stdin.buffer.read().decode().split("\0"), 0, 0
while i < len(f) and f[i]:
    added, deleted, path = f[i].split("\t")
    i += 1
    if not path:  # a rename: the old path, then the new one
        path, i = f[i + 1], i + 2
    if added != "-" and not any(fnmatch.fnmatch(path, p) for p in EXCLUDED):
        n += int(added) + int(deleted)
print(n)')
echo "hand-written changed lines: $n (cap $cap)"
if [ "$n" -gt "$cap" ]; then
  echo "::error::this chunk has $n hand-written lines; split it, or Gabriel adds the oversize-approved label"
  exit 1
fi
