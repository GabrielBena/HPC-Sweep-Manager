#!/usr/bin/env bash
# Smoke-test the push-model SSHComputeSource via `hsm sweep run --remote`.
#
# Builds a throwaway sweep project under $WORK, runs a dry-run, then submits a
# small (2-task) sweep to the remote. Tasks each write a sentinel file; we
# rsync them back and confirm they appear locally, then check that the remote
# per-sweep dir was auto-cleaned on success.
#
# Usage (REMOTE is required — set it to your ~/.ssh/config alias):
#     REMOTE=my-box bash examples/smoke_ssh_cli.sh
#     REMOTE=my-box CONDA_ENV=my-env bash examples/smoke_ssh_cli.sh   # use `conda run -n my-env python`
#     REMOTE=my-box GPUS=0,1 bash examples/smoke_ssh_cli.sh           # allowlist
#     REMOTE=my-box bash examples/smoke_ssh_cli.sh --dry-only         # no submission
#     REMOTE=my-box bash examples/smoke_ssh_cli.sh --keep              # keep $WORK between runs
#
# Prerequisites:
#   - `pip install -e ".[dev]"` of this repo locally.
#   - A working `~/.ssh/config` entry for $REMOTE (try `ssh $REMOTE` once first).
#   - `nvidia-smi` on $REMOTE is optional (CPU slots are the fallback).
#   - The interpreter on $REMOTE: bare `python` on PATH by default, or set
#     CONDA_ENV=<name> to run via `conda run -n <name> python`.
#
# Verify after submission:
#   ls $WORK/sweeps/outputs/*/tasks/*/task_info.txt
#   ls $WORK/sweeps/outputs/*/tasks/*/sentinel.txt
#   ssh $REMOTE ls ~/.hsm/runs/hsm-ssh-smoke/   # only code/ should remain
#
# Cleanup after:
#   hsm remote clean $REMOTE                    # removes ~/.hsm/runs/hsm-ssh-smoke/

set -euo pipefail

DRY_ONLY=0
KEEP=0
for arg in "$@"; do
  case "$arg" in
    --dry-only) DRY_ONLY=1 ;;
    --keep)     KEEP=1 ;;
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

# ----- tune for your setup ---------------------------------------------------
# REMOTE is required: must be a ~/.ssh/config alias you can ssh to.
REMOTE="${REMOTE:?set REMOTE=<your-ssh-alias> (must resolve via ~/.ssh/config)}"
# CONDA_ENV is optional: empty → bare `python` on the remote PATH;
# else uses `conda run -n <env> python` (the script auto-sources conda init).
CONDA_ENV="${CONDA_ENV:-}"
WORK="${TMPDIR:-/tmp}/hsm-ssh-smoke"
GPUS="${GPUS:-all}"            # all / cpu / N / i,j,k
MAX_PARALLEL="${MAX_PARALLEL:-2}"
# -----------------------------------------------------------------------------

echo "[smoke_ssh] remote     = $REMOTE"
echo "[smoke_ssh] conda_env  = ${CONDA_ENV:-<bare python>}"
echo "[smoke_ssh] working dir= $WORK"

if [[ -d "$WORK" && "$KEEP" -eq 0 ]]; then
  rm -rf "$WORK"
fi
mkdir -p "$WORK/sweeps" "$WORK/.hsm"
cd "$WORK"

# Tiny self-contained training script — only stdlib so it runs on any python.
# Writes a sentinel + the wrapper's --output.dir args back into the task dir so
# we can confirm the push→exec→pull cycle.
cat > train.py <<'PYEOF'
#!/usr/bin/env python3
"""Smoke train.py — prints env, sleeps, writes sentinel."""
import argparse
import os
import socket
import sys
import time
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("overrides", nargs="*")
args = parser.parse_args()

# Hydra-style key=value tokens — we just want to see them in the log + sentinel.
kv = {}
output_dir = None
for tok in args.overrides:
    if "=" in tok:
        k, v = tok.split("=", 1)
        kv[k] = v
        if k == "output.dir":
            output_dir = v

print(f"hello from {socket.gethostname()} at {time.ctime()}", flush=True)
print(f"  CUDA_VISIBLE_DEVICES = {os.environ.get('CUDA_VISIBLE_DEVICES', '<unset>')}", flush=True)
print(f"  PWD = {os.getcwd()}", flush=True)
print(f"  args = {args.overrides}", flush=True)
print("sleeping 3s ...", flush=True)
time.sleep(3)

if output_dir:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    sentinel = out / "sentinel.txt"
    sentinel.write_text(
        f"host={socket.gethostname()}\n"
        f"cuda={os.environ.get('CUDA_VISIBLE_DEVICES', '<unset>')}\n"
        f"params={kv}\n"
        f"finished={time.ctime()}\n"
    )
    print(f"wrote {sentinel}", flush=True)

print("done", flush=True)
PYEOF
chmod +x train.py

# A 2-task grid — enough to confirm slot queueing without going long.
cat > sweeps/sweep.yaml <<'YAMLEOF'
script: train.py
sweep:
  grid:
    seed: [1, 2]
YAMLEOF

# Minimal .hsm/config.yaml — registers the remote so per-remote settings stick.
cat > .hsm/config.yaml <<HSMEOF
project:
  root: $WORK
paths:
  train_script: train.py
distributed:
  enabled: true
  remote_root: ~/.hsm/runs
$( [[ -n "$CONDA_ENV" ]] && echo "  conda_env: $CONDA_ENV" )
  remotes:
    $REMOTE:
      max_parallel_jobs: $MAX_PARALLEL
HSMEOF

echo
echo "================================================================"
echo "[0/3] connectivity check — hsm remote gpus $REMOTE"
echo "================================================================"
hsm remote gpus "$REMOTE" || echo "[warn] gpus probe failed (may be CPU-only box)"

# spec.gpus (per-task GPU count) is separate from --gpus (the allowlist).
# Setting it to 1 makes the slot queue allocate one GPU per task when GPUs are
# available; it falls back to CPU slots when GPUS=cpu or none are detected.
GPUS_PER_TASK_RESOURCES="--gpus=1"

echo
echo "================================================================"
echo "[1/3] dry-run — hsm sweep run --remote $REMOTE --gpus $GPUS"
echo "================================================================"
hsm sweep run --remote "$REMOTE" --gpus "$GPUS" \
  --resources "$GPUS_PER_TASK_RESOURCES" \
  -c sweeps/sweep.yaml --dry-run

if [[ "$DRY_ONLY" -eq 1 ]]; then
  echo
  echo "[smoke_ssh] --dry-only set; stopping here."
  exit 0
fi

echo
echo "================================================================"
echo "[2/3] real submission — 2 tasks, blocking until done"
echo "================================================================"
hsm sweep run --remote "$REMOTE" --gpus "$GPUS" \
  --resources "$GPUS_PER_TASK_RESOURCES" \
  -c sweeps/sweep.yaml

echo
echo "================================================================"
echo "[3/3] verifying"
echo "================================================================"
LATEST=$(ls -td "$WORK"/sweeps/outputs/*/ | head -1)
echo "Latest sweep dir: $LATEST"
echo
echo "Tasks rsync'd back:"
ls -la "$LATEST/tasks/" 2>/dev/null || echo "  (no tasks/ dir?)"
echo
echo "Sentinel files (proves push→exec→pull):"
find "$LATEST/tasks" -name 'sentinel.txt' -exec cat {} +
echo
echo "Remote state — only code/ should remain (per-sweep dir auto-cleaned):"
ssh "$REMOTE" "ls -la ~/.hsm/runs/hsm-ssh-smoke/ 2>/dev/null || echo '  (gone)'"

echo
echo "[smoke_ssh] done. Useful follow-ups:"
echo "  ls $LATEST/tasks/*/task_info.txt"
echo "  hsm remote clean $REMOTE              # remove ~/.hsm/runs/hsm-ssh-smoke/"
echo "  hsm remote clean $REMOTE --all-projects  # nuke ~/.hsm/runs/ entirely"
