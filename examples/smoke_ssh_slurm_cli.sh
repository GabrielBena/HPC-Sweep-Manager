#!/usr/bin/env bash
# Smoke-test the SSH-driven Slurm path (SSHSlurmComputeSource) via
# `hsm sweep run --remote <alias>` with `backend: slurm` in .hsm/config.yaml.
#
# Builds a throwaway sweep project under $WORK, runs a dry-run, then submits a
# small (2-task) sweep that pushes code to $REMOTE, submits an `sbatch --array`
# over SSH, polls `squeue` over SSH, rsyncs results back, and (optionally)
# archives the run to a durable /shares location on the cluster before
# cleaning /scratch.
#
# Usage (REMOTE is required — set it to your ~/.ssh/config alias for a Slurm
# login node):
#     REMOTE=uzh bash examples/smoke_ssh_slurm_cli.sh
#     REMOTE=uzh CONDA_ENV=cpvr bash examples/smoke_ssh_slurm_cli.sh
#     REMOTE=uzh WORKDIR=/scratch/$USER/hsm-runs ARCHIVE_DIR=/shares/payvand.ini.uzh/hsm-archive \
#         bash examples/smoke_ssh_slurm_cli.sh
#     REMOTE=uzh bash examples/smoke_ssh_slurm_cli.sh --dry-only
#     REMOTE=uzh bash examples/smoke_ssh_slurm_cli.sh --keep
#
# Prerequisites:
#   - `pip install -e ".[dev]"` of this repo on the machine you're running
#     this script from (e.g., anahita).
#   - $REMOTE is a working `~/.ssh/config` alias for a Slurm login node;
#     `ssh $REMOTE hostname` should succeed.
#   - The interpreter on $REMOTE: bare `python` on PATH by default, or set
#     CONDA_ENV=<name> to run via `conda run -n <name> python`.
#   - $WORKDIR (optional) is a writable path on $REMOTE — defaults to
#     `~/.hsm/runs/$USER`. For S3IT users, set to `/scratch/$USER/hsm-runs`
#     to land sweeps on the 20TB ephemeral scratch.
#   - $ARCHIVE_DIR (optional) is a durable directory on $REMOTE. For S3IT
#     users, set to a writable subpath of your `/shares/<group>/` allocation.
#     When set, the server-side rsync `workdir → archive_dir/<sweep_id>` runs
#     before the local pull; a `.archived` sentinel records the transfer.
#
# Verify after submission:
#   ls $WORK/sweeps/outputs/*/tasks/*/task_info.txt   # pulled-back per-task info
#   ls $WORK/sweeps/outputs/*/tasks/*/sentinel.txt    # proves push→sbatch→pull cycle
#   ssh $REMOTE ls $ARCHIVE_DIR/<sweep_id>/.archived  # archive sentinel (if ARCHIVE_DIR set)
#   ssh $REMOTE ls $WORKDIR/                          # /scratch should be cleaned on success
#
# Cleanup after:
#   hsm remote clean $REMOTE                 # removes ~/.hsm/runs/hsm-ssh-slurm-smoke/

set -euo pipefail

DRY_ONLY=0
KEEP=0
for arg in "$@"; do
  case "$arg" in
    --dry-only) DRY_ONLY=1 ;;
    --keep)     KEEP=1 ;;
    -h|--help)
      sed -n '2,40p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown arg: $arg" >&2
      exit 2
      ;;
  esac
done

# ----- tune for your setup ---------------------------------------------------
# REMOTE is required: must be a ~/.ssh/config alias resolving to a Slurm login node.
REMOTE="${REMOTE:?set REMOTE=<your-ssh-alias> (must resolve via ~/.ssh/config to a Slurm login node)}"
# CONDA_ENV: empty → bare `python` on the remote PATH; else `conda run -n <env> python`.
CONDA_ENV="${CONDA_ENV:-}"
# WORKDIR: where the active sweep lives on the remote during the run.
# Defaults to ~/.hsm/runs (HSM's persistent layout). Set to /scratch/$USER/...
# on a cluster where ephemeral storage is preferred.
WORKDIR="${WORKDIR:-}"
# ARCHIVE_DIR: durable directory for the server-side archive step. Skipped when unset.
ARCHIVE_DIR="${ARCHIVE_DIR:-}"
ARCHIVE_ON="${ARCHIVE_ON:-completed}"
# Per-task Slurm spec — keep tiny for the smoke test so it lands on a small node fast.
WALLTIME="${WALLTIME:-00:10:00}"
CPUS_PER_TASK="${CPUS_PER_TASK:-1}"
MEM="${MEM:-2G}"
GPUS_PER_TASK="${GPUS_PER_TASK:-0}"
GPU_TYPE="${GPU_TYPE:-}"           # e.g. H100 on S3IT — leave empty for CPU-only
QOS="${QOS:-}"
ACCOUNT="${ACCOUNT:-}"
WORK="${TMPDIR:-/tmp}/hsm-ssh-slurm-smoke"
# -----------------------------------------------------------------------------

echo "[smoke_ssh_slurm] remote      = $REMOTE"
echo "[smoke_ssh_slurm] conda_env   = ${CONDA_ENV:-<bare python>}"
echo "[smoke_ssh_slurm] workdir     = ${WORKDIR:-<default ~/.hsm/runs>}"
echo "[smoke_ssh_slurm] archive_dir = ${ARCHIVE_DIR:-<archive disabled>}"
echo "[smoke_ssh_slurm] working dir = $WORK"

if [[ -d "$WORK" && "$KEEP" -eq 0 ]]; then
  rm -rf "$WORK"
fi
mkdir -p "$WORK/sweeps" "$WORK/.hsm"
cd "$WORK"

# Tiny stdlib-only train.py — writes a sentinel + the args we got, so we can
# confirm the push→sbatch→squeue→pull cycle picked up the params correctly.
cat > train.py <<'PYEOF'
#!/usr/bin/env python3
"""Smoke train.py for SSH-driven Slurm — prints env, sleeps, writes sentinel."""
import argparse
import os
import socket
import sys
import time
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("overrides", nargs="*")
args = parser.parse_args()

kv = {}
output_dir = None
for tok in args.overrides:
    if "=" in tok:
        k, v = tok.split("=", 1)
        kv[k] = v
        if k == "output.dir":
            output_dir = v

print(f"hello from {socket.gethostname()} at {time.ctime()}", flush=True)
print(f"  SLURM_JOB_ID = {os.environ.get('SLURM_JOB_ID', '<unset>')}", flush=True)
print(f"  SLURM_ARRAY_TASK_ID = {os.environ.get('SLURM_ARRAY_TASK_ID', '<unset>')}", flush=True)
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
        f"slurm_job_id={os.environ.get('SLURM_JOB_ID', '<unset>')}\n"
        f"slurm_array_task_id={os.environ.get('SLURM_ARRAY_TASK_ID', '<unset>')}\n"
        f"cuda={os.environ.get('CUDA_VISIBLE_DEVICES', '<unset>')}\n"
        f"params={kv}\n"
        f"finished={time.ctime()}\n"
    )
    print(f"wrote {sentinel}", flush=True)

print("done", flush=True)
PYEOF
chmod +x train.py

# A 2-task grid — minimal but enough to exercise array submission.
cat > sweeps/sweep.yaml <<'YAMLEOF'
script: train.py
sweep:
  grid:
    seed: [1, 2]
YAMLEOF

# Build the typed remote `spec:` block from the env vars above so we get
# proper #SBATCH directives. Leaving fields empty (gpus=0, no qos/account)
# is fine — they're simply omitted from the rendered script.
SPEC_LINES=(
  "        walltime: \"$WALLTIME\""
  "        cpus_per_task: $CPUS_PER_TASK"
  "        mem: \"$MEM\""
)
if [[ "$GPUS_PER_TASK" -gt 0 ]]; then
  SPEC_LINES+=("        gpus: $GPUS_PER_TASK")
  if [[ -n "$GPU_TYPE" ]]; then
    SPEC_LINES+=("        gpu_type: \"$GPU_TYPE\"")
  fi
fi
if [[ -n "$QOS" ]]; then
  SPEC_LINES+=("        qos: \"$QOS\"")
fi
if [[ -n "$ACCOUNT" ]]; then
  SPEC_LINES+=("        account: \"$ACCOUNT\"")
fi
SPEC_BLOCK="$(printf '%s\n' "${SPEC_LINES[@]}")"

# Optional storage-tier lines for the remote config.
STORAGE_LINES=()
if [[ -n "$WORKDIR" ]]; then
  STORAGE_LINES+=("      workdir: \"$WORKDIR\"")
fi
if [[ -n "$ARCHIVE_DIR" ]]; then
  STORAGE_LINES+=("      archive_dir: \"$ARCHIVE_DIR\"")
  STORAGE_LINES+=("      archive_on: $ARCHIVE_ON")
fi
STORAGE_BLOCK=""
if [[ ${#STORAGE_LINES[@]} -gt 0 ]]; then
  STORAGE_BLOCK="$(printf '%s\n' "${STORAGE_LINES[@]}")"
fi

# .hsm/config.yaml — declares the remote with backend: slurm.
{
  cat <<HSMHEAD
project:
  root: $WORK
paths:
  train_script: train.py
distributed:
  enabled: true
  remotes:
    $REMOTE:
      backend: slurm
$([ -n "$CONDA_ENV" ] && echo "      conda_env: $CONDA_ENV")
HSMHEAD
  # Storage block (if any). Indented to land inside the remote entry.
  if [[ -n "$STORAGE_BLOCK" ]]; then
    printf '%s\n' "$STORAGE_BLOCK"
  fi
  echo "      spec:"
  printf '%s\n' "$SPEC_BLOCK"
} > .hsm/config.yaml

echo
echo "================================================================"
echo "[0/3] connectivity check — ssh $REMOTE hostname"
echo "================================================================"
ssh -o ConnectTimeout=8 "$REMOTE" "hostname; command -v sbatch && command -v squeue || echo 'WARN: Slurm tools missing on remote PATH'"

echo
echo "================================================================"
echo "[1/3] dry-run — hsm sweep run --remote $REMOTE --dry-run"
echo "================================================================"
hsm sweep run --remote "$REMOTE" -c sweeps/sweep.yaml --dry-run

if [[ "$DRY_ONLY" -eq 1 ]]; then
  echo
  echo "[smoke_ssh_slurm] --dry-only set; stopping here."
  exit 0
fi

echo
echo "================================================================"
echo "[2/3] real submission — 2-task array, blocking until done"
echo "================================================================"
hsm sweep run --remote "$REMOTE" -c sweeps/sweep.yaml

echo
echo "================================================================"
echo "[3/3] verifying"
echo "================================================================"
LATEST=$(ls -td "$WORK"/sweeps/outputs/*/ | head -1)
SWEEP_ID=$(basename "$LATEST")
echo "Latest sweep dir : $LATEST"
echo "Sweep ID         : $SWEEP_ID"
echo
echo "Tasks rsync'd back:"
ls -la "$LATEST/tasks/" 2>/dev/null || echo "  (no tasks/ dir?)"
echo
echo "Sentinel files (proves push→sbatch→squeue→pull):"
find "$LATEST/tasks" -name 'sentinel.txt' -exec cat {} +
echo

if [[ -n "$ARCHIVE_DIR" ]]; then
  echo "Archive sentinel on $REMOTE ($ARCHIVE_DIR/$SWEEP_ID/.archived):"
  ssh "$REMOTE" "cat $ARCHIVE_DIR/$SWEEP_ID/.archived 2>/dev/null || echo '  (no .archived sentinel — check $ARCHIVE_DIR perms?)'"
  echo
fi

echo "Remote workdir state — sweep dir should be gone on success:"
if [[ -n "$WORKDIR" ]]; then
  ssh "$REMOTE" "ls -la $WORKDIR/hsm-ssh-slurm-smoke/sweeps/ 2>/dev/null || echo '  (cleaned — workdir/<project>/sweeps/ no longer exists)'"
else
  ssh "$REMOTE" "ls -la ~/.hsm/runs/hsm-ssh-slurm-smoke/sweeps/ 2>/dev/null || echo '  (cleaned — ~/.hsm/runs/hsm-ssh-slurm-smoke/sweeps/ no longer exists)'"
fi

echo
echo "[smoke_ssh_slurm] done. Useful follow-ups:"
echo "  ls $LATEST/tasks/*/task_info.txt"
echo "  cat $LATEST/tasks/*/task_info.txt"
echo "  hsm remote clean $REMOTE              # remove ~/.hsm/runs/hsm-ssh-slurm-smoke/"
if [[ -n "$ARCHIVE_DIR" ]]; then
  echo "  ssh $REMOTE 'ls $ARCHIVE_DIR/'        # browse archived sweeps"
  echo "  ssh $REMOTE 'rm -rf $ARCHIVE_DIR/$SWEEP_ID'   # nuke this archive entry if no longer needed"
fi
