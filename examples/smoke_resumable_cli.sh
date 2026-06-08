#!/usr/bin/env bash
# Smoke-test the resumable-chain path (issue #12) end-to-end against a real
# Slurm login node over SSH (`backend: slurm`).
#
# Builds a throwaway 1-config sweep whose train script is the contract-reference
# `examples/resumable_probe.py` (a NON-GPU sleep+checkpoint loop), with a
# deliberately tiny `chunk_walltime` so chunk 1 TIMES OUT (→ SIGTERM → save) and
# chunk 2 RESUMES from the checkpoint and finishes — proving:
#   * the pre-walltime --signal reaches the python child and it saves,
#   * chunk 2 carries --dependency=afterany:<chunk1> + the resume pointer,
#   * the chain stops on the `.hsm_done` sentinel (not exit code / walltime),
#   * the final pull lands `tasks/task_1/.hsm_done` locally and cleans the remote.
#
# Usage (REMOTE is required — your ~/.ssh/config alias for a Slurm login node):
#     REMOTE=uzh bash examples/smoke_resumable_cli.sh
#     REMOTE=uzh CONDA_ENV=hsm WORKDIR=/scratch/$USER/hsm-runs \
#         bash examples/smoke_resumable_cli.sh
#     # S3IT V100 lowprio (the motivating capped pool; schedules fast when idle):
#     REMOTE=uzh CONDA_ENV=hsm MODULES=miniforge3/25.3.0-3 \
#         WORKDIR=/scratch/$USER/hsm-runs PARTITION=lowprio QOS=normal \
#         ACCOUNT=payvand.ini.uzh GPU_TYPE=V100 GPUS=1 \
#         bash examples/smoke_resumable_cli.sh
#     REMOTE=uzh bash examples/smoke_resumable_cli.sh --dry-only
#
# Prerequisites: `pip install -e .` of this repo locally; `ssh $REMOTE hostname`
# works; the remote has python on PATH (or set CONDA_ENV). NO GPU needed.
set -euo pipefail

DRY_ONLY=0
KEEP=0
for arg in "$@"; do
  case "$arg" in
    --dry-only) DRY_ONLY=1 ;;
    --keep)     KEEP=1 ;;
    -h|--help)  sed -n '2,30p' "$0"; exit 0 ;;
    *) echo "Unknown arg: $arg" >&2; exit 2 ;;
  esac
done

REMOTE="${REMOTE:?set REMOTE=<your-ssh-alias> (a Slurm login node)}"
CONDA_ENV="${CONDA_ENV:-}"
WORKDIR="${WORKDIR:-}"
# Chain tuning. chunk_walltime must be > signal_grace. Sized so chunk 1 times
# out and chunk 2 finishes (budget ≈ 4 min over 2 × 3-min chunks).
CHUNK_WALLTIME="${CHUNK_WALLTIME:-00:03:00}"
SIGNAL_GRACE="${SIGNAL_GRACE:-30}"
TOTAL_STEPS="${TOTAL_STEPS:-120}"
STEP_SECONDS="${STEP_SECONDS:-2}"
QOS="${QOS:-}"
ACCOUNT="${ACCOUNT:-}"
PARTITION="${PARTITION:-}"
GPU_TYPE="${GPU_TYPE:-}"          # e.g. V100 (lowprio pool on S3IT) — empty = CPU-only
GPUS="${GPUS:-0}"                 # per-task GPU count (the probe ignores it; for pool routing)
# Non-flavour modules to load before conda-init (S3IT: the conda provider).
# e.g. MODULES=miniforge3/25.3.0-3 so `conda run -n $CONDA_ENV` resolves.
MODULES="${MODULES:-}"
HERE="$(cd "$(dirname "$0")" && pwd)"
WORK="${TMPDIR:-/tmp}/hsm-resumable-smoke"

echo "[smoke_resumable] remote=$REMOTE conda_env=${CONDA_ENV:-<bare python>} workdir=${WORKDIR:-<default>}"
echo "[smoke_resumable] chunk_walltime=$CHUNK_WALLTIME signal_grace=$SIGNAL_GRACE budget=${TOTAL_STEPS}×${STEP_SECONDS}s"

[[ -d "$WORK" && "$KEEP" -eq 0 ]] && rm -rf "$WORK"
mkdir -p "$WORK/sweeps" "$WORK/.hsm"
cd "$WORK"

# The training script IS the contract reference probe.
cp "$HERE/resumable_probe.py" train.py

# A single long "config" — the motivating case (one job that needs >1 chunk).
# The probe's budget goes in the GRID (single-valued) so it's passed to the
# script as a hydra override — a `defaults:` block is metadata, NOT passed.
cat > sweeps/sweep.yaml <<EOF
sweep:
  grid:
    seed: [0]
    total_steps: [$TOTAL_STEPS]
    step_seconds: [$STEP_SECONDS]
resumable:
  enabled: true
  chunk_walltime: "$CHUNK_WALLTIME"
  signal_grace: $SIGNAL_GRACE
  max_chunks: 5
EOF
# Note: no resume_arg → the probe resumes via the HSM_RESUME_FROM env var (the
# general, framework-agnostic default). Set resume_arg in the YAML to also pass
# a hydra CLI override.

# Per-remote backend:slurm + the typed spec. The walltime here is irrelevant in
# resumable mode (every chunk is capped at chunk_walltime).
SPEC="    spec:\n      walltime: \"01:00:00\"\n      cpus_per_task: 1\n      mem: 1G"
[[ -n "$PARTITION" ]] && SPEC="$SPEC\n      partition: $PARTITION"
[[ -n "$QOS" ]] && SPEC="$SPEC\n      qos: $QOS"
[[ -n "$ACCOUNT" ]] && SPEC="$SPEC\n      account: $ACCOUNT"
[[ "$GPUS" != "0" ]] && SPEC="$SPEC\n      gpus: $GPUS"
[[ -n "$GPU_TYPE" ]] && SPEC="$SPEC\n      gpu_type: $GPU_TYPE"
[[ -n "$MODULES" ]] && SPEC="$SPEC\n      modules: [$MODULES]"
{
  echo "distributed:"
  echo "  remotes:"
  echo "    $REMOTE:"
  echo "      host: $REMOTE"
  echo "      backend: slurm"
  [[ -n "$CONDA_ENV" ]] && echo "      conda_env: $CONDA_ENV"
  [[ -n "$WORKDIR" ]] && echo "      workdir: $WORKDIR"
  echo -e "$SPEC"
  echo "paths:"
  echo "  script: train.py"
} > .hsm/config.yaml

echo "[smoke_resumable] --- dry-run ---"
hsm sweep run --resumable --chunk-walltime "$CHUNK_WALLTIME" \
  --remote "$REMOTE" --mode array -c sweeps/sweep.yaml --dry-run

if [[ "$DRY_ONLY" -eq 1 ]]; then
  echo "[smoke_resumable] dry-only: stopping before submission."
  exit 0
fi

echo "[smoke_resumable] --- real run (drives the chain; may take a few minutes + queue wait) ---"
hsm sweep run --resumable --chunk-walltime "$CHUNK_WALLTIME" \
  --remote "$REMOTE" --mode array -c sweeps/sweep.yaml

# Verify: the done sentinel was pulled back for the single task.
SENT="$(find sweeps -path '*/tasks/task_*/.hsm_done' | head -1 || true)"
if [[ -n "$SENT" ]]; then
  echo "[smoke_resumable] PASS — chain completed; sentinel pulled: $SENT"
else
  echo "[smoke_resumable] WARN — no .hsm_done pulled locally; inspect sweeps/outputs/*/tasks + logs." >&2
  exit 1
fi
