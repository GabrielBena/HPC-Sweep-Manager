#!/usr/bin/env bash
# Smoke-test the new SweepOrchestrator path via `hsm sweep run` on S3IT.
#
# Builds a throwaway sweep project under $SCRATCH/hsm-cli-smoke, runs three
# dry-runs (array / auto / individual), then submits a real 4-task CPU
# array followed by a real 4-task GPU array (unless invoked with --dry-only
# or --no-gpu). The GPU step uses a typed `slurm:` block with `gpu_type` +
# `modules` (S3IT-style) and verifies each task actually saw a GPU by
# parsing `GPU_CHECK:` lines from the task stdouts.
#
# Usage:
#     bash examples/smoke_cli.sh                     # dry-runs + CPU + GPU array
#     bash examples/smoke_cli.sh --dry-only          # dry-runs only
#     bash examples/smoke_cli.sh --no-gpu            # CPU only, skip the GPU step
#     bash examples/smoke_cli.sh --gpu-type=l4       # use l4 instead of h100
#     bash examples/smoke_cli.sh --keep              # keep $WORK between runs
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
NO_GPU=0
GPU_TYPE="h100"
for arg in "$@"; do
  case "$arg" in
    --dry-only) DRY_ONLY=1 ;;
    --keep)     KEEP=1 ;;
    --no-gpu)   NO_GPU=1 ;;
    --gpu-type=*) GPU_TYPE="${arg#--gpu-type=}" ;;
    -h|--help)
      sed -n '2,22p' "$0"
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
"""Throwaway training stub for the HSM smoke harness.

Emits `GPU_CHECK:` lines that the verifier in smoke_cli.sh greps for to
confirm GPUs were actually allocated when requested.
"""
import os
import shutil
import subprocess
import sys
import time

print(f"hello from {os.uname().nodename} at {time.ctime()}", flush=True)

# --- GPU diagnostics (always printed; verifier in smoke_cli.sh greps these) ---
cuda_visible = os.environ.get("CUDA_VISIBLE_DEVICES", "")
print(f"GPU_CHECK: CUDA_VISIBLE_DEVICES={cuda_visible!r}", flush=True)

nvidia_smi = shutil.which("nvidia-smi")
if nvidia_smi:
    try:
        out = subprocess.check_output([nvidia_smi, "-L"], text=True, timeout=10)
        gpu_count = sum(1 for line in out.splitlines() if line.startswith("GPU "))
        print(f"GPU_CHECK: nvidia_smi_count={gpu_count}", flush=True)
        for line in out.splitlines():
            print(f"GPU_CHECK: nvidia_smi_line={line}", flush=True)
    except Exception as e:  # noqa: BLE001 - probe, not a real error path
        print(f"GPU_CHECK: nvidia_smi_count=0  # {e}", flush=True)
else:
    print("GPU_CHECK: nvidia_smi_count=0  # nvidia-smi not on PATH", flush=True)

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
echo "[real submission] --mode array (CPU), 4 tasks, blocking until done"
echo "================================================================"
hsm sweep run --mode array --walltime "$WALLTIME" --resources "$RESOURCES"

if [[ "$NO_GPU" -eq 1 ]]; then
  echo
  echo "[smoke_cli] --no-gpu set; skipping the GPU array submission."
  echo "  Verify CPU sweep with: cat $WORK/sweeps/outputs/*/submission_summary.txt"
  exit 0
fi

# ----- GPU array submission via typed slurm: block ----------------------------
# Why this step writes .hsm/config.yaml instead of just passing --resources:
# on S3IT-style clusters, `--resources --gpus=1` emits `#SBATCH --gpus=1` which
# does NOT actually allocate a GPU (confirmed empirically; see CLAUDE.md
# gotcha #6). You need `gpus: 1, gpu_type: <type>, modules: [<type>]` to emit
# `#SBATCH --gres=gpu:<type>:1` + `module load <type>` together. The opaque
# --resources CLI string can't express `gpu_type` or `modules` — only the
# typed slurm: block can.
echo
echo "================================================================"
echo "[GPU smoke] --mode array with typed slurm: block (gpu_type=$GPU_TYPE)"
echo "================================================================"

mkdir -p .hsm
cat > .hsm/config.yaml <<YAMLEOF
project:
  name: hsm-cli-smoke
  root: $WORK
paths:
  python_interpreter: $(command -v python || command -v python3)
  train_script: train.py
slurm:
  walltime: "$WALLTIME"
  cpus_per_task: 1
  mem: "4gb"
  gpus: 1
  gpu_type: $GPU_TYPE
  modules: [$GPU_TYPE]
  qos: normal
YAMLEOF

echo "[smoke_cli] wrote .hsm/config.yaml with typed slurm: block:"
sed 's/^/    /' .hsm/config.yaml
echo

hsm sweep run --mode array

# Find the GPU sweep dir (most recently created sweep_* under outputs/).
LATEST_SWEEP=$(ls -td "$WORK"/sweeps/outputs/sweep_* 2>/dev/null | head -1)
if [[ -z "$LATEST_SWEEP" ]]; then
  echo "[smoke_cli] ❌ no sweep dir found under $WORK/sweeps/outputs" >&2
  exit 1
fi

echo
echo "================================================================"
echo "[GPU verify] greping $LATEST_SWEEP/logs/*.out for GPU_CHECK"
echo "================================================================"

shopt -s nullglob
stdout_files=("$LATEST_SWEEP"/logs/*.out)
if [[ ${#stdout_files[@]} -eq 0 ]]; then
  echo "❌ no task stdout files in $LATEST_SWEEP/logs/ — tasks may not have launched" >&2
  exit 1
fi

pass=0
fail=0
for f in "${stdout_files[@]}"; do
  base=$(basename "$f")
  cuda_line=$(grep -m1 "GPU_CHECK: CUDA_VISIBLE_DEVICES=" "$f" || true)
  smi_line=$(grep -m1 "GPU_CHECK: nvidia_smi_count=" "$f" || true)

  if [[ -z "$cuda_line" ]]; then
    echo "  ❌ $base: no GPU_CHECK output (task may have crashed before train.py)"
    fail=$((fail + 1))
    continue
  fi

  # train.py renders this as: GPU_CHECK: CUDA_VISIBLE_DEVICES='0'  (or '<unset>' if missing)
  cuda_val=$(echo "$cuda_line" | sed -E "s/.*CUDA_VISIBLE_DEVICES=//" | tr -d "'\"")
  smi_count=$(echo "$smi_line" | sed -E "s/.*nvidia_smi_count=([0-9]+).*/\1/")

  if [[ -z "$cuda_val" || "$cuda_val" == "<unset>" ]]; then
    echo "  ❌ $base: CUDA_VISIBLE_DEVICES is unset/empty"
    fail=$((fail + 1))
    continue
  fi
  if [[ -z "$smi_count" || "$smi_count" -lt 1 ]]; then
    echo "  ❌ $base: nvidia-smi -L reports $smi_count GPUs (expected ≥1)"
    fail=$((fail + 1))
    continue
  fi

  echo "  ✅ $base: CUDA_VISIBLE_DEVICES=$cuda_val, nvidia_smi_count=$smi_count"
  pass=$((pass + 1))
done

echo
if [[ $fail -gt 0 ]]; then
  echo "[smoke_cli] ❌ GPU verification: $pass passed, $fail FAILED out of ${#stdout_files[@]} tasks"
  echo "  Inspect failed task stdouts under $LATEST_SWEEP/logs/"
  exit 1
fi
echo "[smoke_cli] ✅ GPU verification: all $pass/${#stdout_files[@]} tasks saw an allocated GPU"

echo
echo "[smoke_cli] done. Verify:"
echo "  ls $WORK/sweeps/outputs"
echo "  cat $WORK/sweeps/outputs/*/submission_summary.txt"
echo "  cat $WORK/sweeps/outputs/*/tasks/*/task_info.txt | head -40"
