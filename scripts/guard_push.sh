#!/usr/bin/env bash
# The pre-push guard (.pre-commit-config.yaml, stage pre-push): refuses a push whose remote ref is
# main, per clone, the protection a private repository on the Free organisation no longer gets from
# GitHub's ruleset; and any push while the local main is ahead of the remote's, since a commit made
# on main is always a mistake here and `git push --all` would carry it. Work reaches main through a
# reviewed pull request, merged on green; force-pushes to a PR branch stay a convention (the
# program's 2026-07-16 pipeline ADR). pre-commit shows a hook the first ref of a push only, so a
# second refspec naming main (`git push origin feat HEAD:main`) is the one form it cannot see.
# Installed by `pre-commit install` (both hook types); `--no-verify` bypasses it.
# Canonical in EIS-Hub/loom and vendored verbatim into EIS-Hub/mosaic-sim; loom's scripts/audit.sh
# flags any drift (loom's DISCIPLINE.md, the method).
set -euo pipefail
refuse() { echo "guard: $1; open a pull request (scripts/guard_push.sh)" >&2; exit 1; }
[ "${PRE_COMMIT_REMOTE_BRANCH:-}" != refs/heads/main ] || refuse "no push to main"
remote="${PRE_COMMIT_REMOTE_NAME:-origin}"
if git rev-parse -q --verify refs/heads/main >/dev/null &&
   git rev-parse -q --verify "refs/remotes/$remote/main" >/dev/null; then
  ahead=$(git rev-list --count "refs/remotes/$remote/main..refs/heads/main")
  [ "$ahead" = 0 ] || refuse "the local main is $ahead commit(s) ahead of $remote/main"
fi
