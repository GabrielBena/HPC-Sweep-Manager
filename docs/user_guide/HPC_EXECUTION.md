# HPC (Slurm / PBS) execution

Submit sweeps to a Slurm or PBS cluster as either a single array job
(`--mode array`) or one job per parameter combination
(`--mode individual`). `--mode auto` resolves to `array` if `sbatch` is
on PATH, else falls back to `local`.

For SSH push-model (no scheduler) see
[SSH_EXECUTION.md](SSH_EXECUTION.md).

## Prerequisites

- You're on a cluster login node (or any host where `sbatch` /
  `squeue` / `scancel` / `sinfo` are on PATH). HSM auto-detects.
- HSM installed in your environment: `pip install -e ".[dev]"` from this
  repo.
- A shared filesystem where outputs land — the cluster's `/scratch` or
  similar. HSM writes scripts/logs/results to `sweeps/outputs/` under
  the project root.

## Quickstart

In your project directory:

```bash
# 1. Initialize HSM scaffolding (one-time per project).
hsm setup init
# Edit .hsm/config.yaml: set hpc.default_walltime and hpc.default_resources
# to sensible defaults for your cluster.

# 2. Dry-run to confirm the resolved spec.
hsm sweep run --mode array \
    --config sweeps/example_sweep.yaml \
    --walltime 00:30:00 \
    --resources "--cpus-per-task=4 --mem=16gb --qos=normal" \
    --dry-run

# 3. Real submission — blocks until the array finishes.
hsm sweep run --mode array \
    --config sweeps/example_sweep.yaml \
    --walltime 00:30:00 \
    --resources "--cpus-per-task=4 --mem=16gb --qos=normal"

# 4. Inspect.
ls sweeps/outputs/<sweep-id>/tasks/*/task_info.txt
cat sweeps/outputs/<sweep-id>/submission_summary.txt
```

The array submission produces one `sbatch --array=1-N` invocation;
`--mode individual` produces N `sbatch` calls. Use array when N is
larger than ~10 and you can use Slurm array-task semantics.

## The `--resources` string

A free-form opaque string passed through `spec_from_legacy_resources()`
into a typed `ResourceSpec`. Recognized tokens (Slurm flavor — pass
`scheduler="slurm"` is implicit when `sbatch` is on PATH):

```
--time=01:00:00            # walltime
--cpus-per-task=4
--mem=16gb                  # OR --mem-per-cpu=2G (mutually exclusive)
--gpus=1
--partition=gpu
--qos=normal
--account=my-project
```

PBS flavor (when `qsub` is on PATH):

```
select=1:ncpus=4:mem=16gb:ngpus=1
```

Unknown tokens land in `ResourceSpec.extra_directives` and are emitted
verbatim into the rendered submit script.

## The typed `slurm:` block — reach fields `--resources` can't

The CLI's opaque `--resources` string covers the common fields above
but **cannot** express `gpu_type`, `modules`, `pre_script`, `account`,
or arbitrary `extra_directives`. On clusters where you need both
`--gres=gpu:h100:1` AND `module load h100` (this is the case on many
production setups), use the typed `slurm:` block in `.hsm/config.yaml`:

```yaml
# .hsm/config.yaml
slurm:
  walltime: "01:00:00"
  cpus_per_task: 4
  mem: "16gb"
  gpus: 1
  gpu_type: "h100"           # → #SBATCH --gres=gpu:h100:1
  partition: "gpu"
  qos: "normal"
  account: "my-project"
  modules:                   # → `module load h100; module load cuda/12`
    - h100
    - cuda/12
  pre_script:                # arbitrary shell commands before the training script
    - "source ~/.bashrc"
    - "conda activate my-env"
  extra_directives:          # any extra #SBATCH directive
    mail-type: FAIL
    mail-user: me@example.com
  qos_whitelist:             # optional guard — errors on submit if --qos isn't in this list
    - normal
    - medium
    - long
```

Then submit normally — the block is picked up automatically:

```bash
hsm sweep run --mode array --config sweeps/sweep.yaml
```

**Precedence** (highest wins): CLI `--walltime` > CLI `--resources`
parsed fields > `slurm:` block > defaults. So you can set base
resources in the config and override `--walltime` per-run from the CLI.

### Direct Python (still supported)

For fully programmatic use, you can also drive `SlurmComputeSource`
directly with a typed `ResourceSpec`:

```python
from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
from hpc_sweep_manager.core.hpc.slurm_compute_source import SlurmComputeSource
from hpc_sweep_manager.core.common.sweep_orchestrator import run_sweep_async
from pathlib import Path
import asyncio

spec = ResourceSpec(
    walltime="00:30:00",
    cpus_per_task=4,
    mem="16gb",
    gpus=1,
    gpu_type="h100",
    modules=("h100",),
    qos="normal",
    pre_script=("source ~/.bashrc", "conda activate my-env"),
)

source = SlurmComputeSource(
    python_path="python",
    script_path="train.py",
    project_dir=".",
    default_spec=spec,
)

params_list = [{"seed": s} for s in range(1, 5)]

asyncio.run(run_sweep_async(
    source=source,
    sweep_dir=Path("sweeps/outputs/my_sweep"),
    sweep_id="my_sweep",
    params_list=params_list,
    submission_mode="array",
))
```

See [`examples/smoke_slurm.py`](../../examples/smoke_slurm.py) for the
runnable template.

## QOS whitelist

If your cluster has tiered QOS (e.g. `normal` / `medium` / `long` with
different walltime caps), set `slurm.qos_whitelist` in `.hsm/config.yaml`
to guard against typos — submission errors with a clear message if
`--qos=...` (or the spec's `qos` field) isn't in the whitelist. The
scheduler would reject invalid values anyway, but this catches them
before submission.

## Inspecting + cancelling

Use `hsm monitor watch <sweep_id>` for live job state or `scancel <id>`
directly to cancel a Slurm job.

Per-task outputs land in `sweeps/outputs/<sweep-id>/tasks/task_NNN/`
(see [`examples/test_train.py`](../../examples/test_train.py) for the
contract).

## Known gotcha: array progress reports 1/1

`SlurmComputeSource` inserts one `JobInfo` for the whole array, so the
`on_progress(done, total)` callback reports `1/1 done` even when N
tasks succeed. The per-task truth lives in
`tasks/*/task_info.txt`. This is a known limitation pending a per-task
polling refactor (`sacct` parsing).

## See also

- [SSH_EXECUTION.md](SSH_EXECUTION.md) — push-model SSH (no scheduler).
- [getting_started.md](getting_started.md) — quickstart across all modes.
- [../api_reference/compute_sources.md](../api_reference/compute_sources.md) —
  `SlurmComputeSource` API.
- [../../ARCHITECTURE.md](../../ARCHITECTURE.md#slurm-path) — internals.
