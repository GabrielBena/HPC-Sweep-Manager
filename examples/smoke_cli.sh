#!/usr/bin/env bash
# Smoke-test the new SweepOrchestrator path via `hsm sweep run` on S3IT.
#
# Builds a throwaway sweep project under $SCRATCH/hsm-cli-smoke, runs three
# dry-runs (array / auto / individual), then submits a real 4-task array
# unless invoked with --dry-only.
#
# Usage:
#     bash examples/smoke_cli.sh              # dry-runs + real array submission
#     bash examples/smoke_cli.sh --dry-only   # dry-runs only, no submission
#     bash examples/smoke_cli.sh --keep       # keep $WORK between runs
#
# Verify after submission:
#     squeue -u $USER
#     ls $WORK/sweeps/outputs/*/tasks/*/task_info.txt
#     cat $WORK/sweeps/outputs/*/submission_summary.txt
#
# Prerequisite: `pip install -e .` of the HSM repo on the login node.

set -euo pipefail

DRY_ONLY=0
KEEP=0
for arg in "$@"; do
  case "$arg" in
    --dry-only) DRY_ONLY=1 ;;
    --keep)     KEEP=1 ;;
    -h|--help)
      sed -n '2,18p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown arg: $arg" >&2
      exit 2
      ;;
  esac
done

# ----- tune for your setup -----------------------------------------------------
# Prefer $SCRATCH on S3IT, then /scratch/$USER, else fall back to /tmp.
if [[ -n "${SCRATCH:-}" && -w "${SCRATCH}" ]]; then
  SCRATCH_ROOT="$SCRATCH"
elif [[ -w "/scratch/${USER:-}" ]]; then
  SCRATCH_ROOT="/scratch/${USER}"
else
  SCRATCH_ROOT="${TMPDIR:-/tmp}"
fi
WORK="${SCRATCH_ROOT}/hsm-cli-smoke"
WALLTIME="00:05:00"
# S3IT QOS tiers: normal (24h), medium (48h), long (7d).
RESOURCES="--cpus-per-task=1 --mem-per-cpu=1G --qos=normal"
# -----------------------------------------------------------------------------

echo "[smoke_cli] working dir: $WORK"

if [[ -d "$WORK" && "$KEEP" -eq 0 ]]; then
  rm -rf "$WORK"
fi
mkdir -p "$WORK/sweeps"
cd "$WORK"

cat > train.py <<'PYEOF'
#!/usr/bin/env python3
import os
import sys
import time

print(f"hello from {os.uname().nodename} at {time.ctime()}", flush=True)
print(f"  CUDA_VISIBLE_DEVICES = {os.environ.get('CUDA_VISIBLE_DEVICES', '<unset>')}", flush=True)
for arg in sys.argv[1:]:
    print(f"  arg: {arg}", flush=True)
print("sleeping 20s ...", flush=True)
time.sleep(20)
print("done", flush=True)
PYEOF
chmod +x train.py

cat > sweeps/sweep.yaml <<'YAMLEOF'
script: train.py
sweep:
  grid:
    seed: [1, 2, 3, 4]
YAMLEOF

echo
echo "================================================================"
echo "[1/3] dry-run --mode array (Slurm array path)"
echo "================================================================"
hsm sweep run --mode array --dry-run --walltime "$WALLTIME" --resources "$RESOURCES"

echo
echo "================================================================"
echo "[2/3] dry-run --mode auto (should auto-resolve to array on S3IT)"
echo "================================================================"
hsm sweep run --mode auto --dry-run --walltime "$WALLTIME" --resources "$RESOURCES"

echo
echo "================================================================"
echo "[3/3] dry-run --mode individual (one sbatch per task)"
echo "================================================================"
hsm sweep run --mode individual --dry-run --walltime "$WALLTIME" --resources "$RESOURCES"

if [[ "$DRY_ONLY" -eq 1 ]]; then
  echo
  echo "[smoke_cli] --dry-only set; stopping here."
  exit 0
fi

echo
echo "================================================================"
echo "[real submission] --mode array, 4 tasks, blocking until done"
echo "================================================================"
hsm sweep run --mode array --walltime "$WALLTIME" --resources "$RESOURCES"

echo
echo "[smoke_cli] done. Verify:"
echo "  ls $WORK/sweeps/outputs"
echo "  cat $WORK/sweeps/outputs/*/submission_summary.txt"
echo "  cat $WORK/sweeps/outputs/*/tasks/*/task_info.txt | head -40"
