#!/usr/bin/env bash
# Bidirectional sync of this project's Claude Code state between the
# laptop and anahita (the workstation HQ for HSM work).
#
# Scope (per-project): everything under
#   ~/.claude/projects/-home-gbena-code-packages-HPC-Sweep-Manager/
# which is the encoded path Claude Code uses for this repo. That dir
# holds session transcripts (`<uuid>.jsonl`), per-session subdirs, and
# the auto-memory (`memory/MEMORY.md` + the entries it indexes).
#
# Plus: ~/.claude/plans/ for approved-plan files referenced by memory.
#
# Strategy: rsync `--update` in both directions so each side keeps the
# newest mtime of every file. No `--delete` — neither machine nukes the
# other's content. Run this BEFORE starting a session (to pull the
# other machine's latest) and AFTER ending one (to push your edits).
#
# Safe to re-run; safe to interrupt; idempotent.
#
# Usage:
#   scripts/sync-claude-state.sh                    # default REMOTE=anahita
#   REMOTE=other-box scripts/sync-claude-state.sh   # different remote
#   scripts/sync-claude-state.sh --dry-run          # preview only
#
# Caveats:
#   - Don't run while a Claude Code session is actively writing on
#     either side (mid-line JSONL append → other side gets a partial
#     record). Sync between sessions.
#   - Per-machine settings (`~/.claude/settings.json`, etc.) are
#     intentionally NOT synced — those are environment-specific.

set -euo pipefail

REMOTE="${REMOTE:-anahita}"
PROJ="-home-gbena-code-packages-HPC-Sweep-Manager"
DRY=""

for arg in "$@"; do
  case "$arg" in
    --dry-run|-n) DRY="--dry-run" ;;
    -h|--help)
      sed -n '2,30p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown arg: $arg" >&2
      exit 2
      ;;
  esac
done

if ! ssh -o ConnectTimeout=8 -o BatchMode=yes "$REMOTE" 'true' 2>/dev/null; then
  echo "[sync-claude] cannot reach $REMOTE (VPN down? key missing?). Skipping." >&2
  exit 1
fi

# Make sure the destination dirs exist on the remote — first-time setup.
ssh "$REMOTE" "mkdir -p .claude/projects/$PROJ .claude/plans" 2>/dev/null

echo "[sync-claude] $REMOTE -> laptop  (pulling sessions + memory)"
rsync -av --update $DRY \
    "$REMOTE:.claude/projects/$PROJ/" \
    "$HOME/.claude/projects/$PROJ/"

echo "[sync-claude] laptop -> $REMOTE  (pushing sessions + memory)"
rsync -av --update $DRY \
    "$HOME/.claude/projects/$PROJ/" \
    "$REMOTE:.claude/projects/$PROJ/"

echo "[sync-claude] $REMOTE -> laptop  (pulling plans)"
rsync -av --update $DRY \
    "$REMOTE:.claude/plans/" \
    "$HOME/.claude/plans/" 2>/dev/null || true   # plans/ may not exist remotely yet

echo "[sync-claude] laptop -> $REMOTE  (pushing plans)"
rsync -av --update $DRY \
    "$HOME/.claude/plans/" \
    "$REMOTE:.claude/plans/"

echo "[sync-claude] done."
