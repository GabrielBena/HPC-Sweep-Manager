# HPC (Slurm / PBS) execution

Submit sweeps to a Slurm or PBS cluster as either a single array job
(`--mode array`) or one job per parameter combination
(`--mode individual`). `--mode auto` resolves to `array` if `sbatch` is
on PATH, else falls back to `local`.

This guide is for the **on-cluster** case — i.e., `hsm` is running where
`sbatch` lives. To drive a Slurm cluster *from off-cluster* (e.g., your
lab workstation submitting to S3IT over SSH), see
[SSH_EXECUTION.md#driving-slurm-over-ssh-backend-slurm](SSH_EXECUTION.md#driving-slurm-over-ssh-backend-slurm).
For the SSH push-model (no scheduler) see
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
# Edit .hsm/config.yaml: uncomment the typed `slurm:` block at the bottom
# and set walltime / cpus_per_task / mem / gpus / qos / account / modules
# for your cluster (the block reaches fields the opaque --resources string can't).

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

## Mode-scoped config blocks — no cross-mode bleed

`.hsm/config.yaml` accepts three independent typed blocks for resource
defaults. Each is read **only** when its matching `--mode` runs; there
is no cross-pollination:

| Block | Read by | Use for |
|---|---|---|
| `local:` | `--mode local` | local GPU/CPU/walltime defaults (no Slurm-only fields) |
| `slurm:` | `--mode array` / `--mode individual` | full Slurm spec including `gpu_type` / `modules` / `qos` / `account` |
| `distributed.remotes.<alias>.spec:` | `--remote <alias>` / `--mode distributed` | per-remote defaults; `local:` and `slurm:` are NOT read for these modes |

This means a `slurm:` block with `gpus: 4` will **not** apply to
`--mode local` (or vice versa). If you want the same defaults across
modes, repeat the fields in each block — it's noisier but unambiguous.

## The typed `slurm:` block — reach fields `--resources` can't

The CLI's opaque `--resources` string covers the common fields above
but **cannot** express `gpu_type`, `modules`, `pre_script`, `account`,
or arbitrary `extra_directives`. On clusters where you need both
a specific `gpu_type`, `account`, or `pre_script`, use the typed
`slurm:` block in `.hsm/config.yaml`:

```yaml
# .hsm/config.yaml
slurm:
  walltime: "01:00:00"
  cpus_per_task: 4
  mem: "16gb"
  gpus: 1
  gpu_type: "H100"           # → #SBATCH --gres=gpu:H100:1
                             #    GRES names are CASE-SENSITIVE — check `sinfo -o "%P %G"`
                             #    on your cluster (S3IT uses uppercase: H100/L4/A100/H200).
  partition: "gpu"
  qos: "normal"
  account: "my-project"
  modules:                   # → `module load matlab; module load openmpi`
    - matlab                 #    Use ONLY for non-flavour modules. GPU flavour modules
    - openmpi                #    (h100/l4/multigpu) should be loaded on the command line
                             #    BEFORE `hsm sweep run`, not in the script — they set Slurm
                             #    constraints that can conflict with the directives above.
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

> **S3IT note:** Earlier docs claimed S3IT NEEDED `modules: [h100]`
> alongside `gpu_type`. That was wrong — `gpu_type: H100` alone produces
> `--gres=gpu:H100:1` which is sufficient. CUDA comes from your conda env.
>
> The [S3IT docs](https://docs.s3it.uzh.ch/cluster/job_submission/) warn
> that flavour modules (`h100`, `l4`, `multigpu`) "set Slurm constraints
> that may conflict with job directives" — but in practice (verified
> 2026-05-28) having `module load h100` inside the script alongside a
> matching `--gres=gpu:H100:1` works because both set consistent
> constraints. The safer default is to omit the flavour module; the
> `modules:` field is still the right home for non-flavour modules like
> `matlab` or `openmpi`. Also worth knowing: "Requested node configuration
> is not available" can be a *transient* sbatch error on a busy cluster —
> retry after a wait before assuming the directives are wrong.

Then submit normally — the block is picked up automatically:

```bash
hsm sweep run --mode array --config sweeps/sweep.yaml
```

**Precedence** (highest wins): CLI `--walltime` > CLI `--resources`
parsed fields > `slurm:` block > defaults. So you can set base
resources in the config and override `--walltime` per-run from the CLI.

## The typed `local:` block — defaults for `--mode local`

Mirror of the `slurm:` block above, but for `--mode local`. Restricted
to fields that make sense outside a batch scheduler:

```yaml
# .hsm/config.yaml
local:
  walltime: "04:00:00"
  cpus_per_task: 4
  mem: "16gb"
  gpus: 1                  # per-task GPU count; LocalComputeSource probes
                           # nvidia-smi -L and partitions GPUs into slots of this size
  visible_gpus: [1, 2, 3]  # optional allowlist (see below)
  pre_script:
    - "conda activate my-env"
```

Slurm-only fields (`gpu_type`, `modules`, `qos`, `account`,
`extra_directives`) are silently dropped with a warning if they appear
here — they belong in `slurm:`.

`hsm setup init` auto-detects local GPUs via `nvidia-smi -L` and pre-seeds
this block with `gpus: 1` *active* when GPUs are found, so `hsm sweep run
--mode local` uses them by default. Toggle off with `gpus: 0` or by
commenting the block.

**Precedence** (highest wins): CLI `--walltime` > CLI `--resources`
parsed fields > `local:` block > defaults.

### `local.sweeps_root` — put sweep dirs on a different filesystem

By default a sweep dir lands at `<project>/sweeps/outputs/<sweep_id>/`.
On boxes where the system disk is tight but a large secondary mount
exists, redirect via the `local:` block:

```yaml
local:
  sweeps_root: "/mnt/8TB_HDD/$USER/hsm-sweeps"   # or /data, /shares, etc.
```

`~` / `$HOME` / `$USER` are expanded once at submission. The sweep
data lives at `<sweeps_root>/<sweep_id>/`, and HSM drops a symlink at
`<project>/sweeps/outputs/<sweep_id>` pointing to it — so `hsm sweep
status` / `hsm sweep report` and any project-local tooling keep working
transparently.

**Put it in `~/.hsm/config.yaml`, not the project's `.hsm/config.yaml`.**
`sweeps_root` is a *machine* fact (the path `/mnt/8TB_HDD` exists on
your workstation but not on your laptop), so the natural home is the
machine-wide config:

```yaml
# ~/.hsm/config.yaml — machine-wide HSM defaults
local:
  sweeps_root: /mnt/8TB_HDD/gbena/hsm-sweeps
```

HSM merges this with each project's `.hsm/config.yaml` at load time
(see [Machine vs project config](#machine-vs-project-config) below).
The first time you run `hsm setup init` on a fresh machine, HSM offers
to detect mount points with significant free space and writes the file
for you; if none are found (or you're not on a TTY) it leaves a
commented stub.

Use cases:
- Workstation with a small NVMe `/` but a large `/mnt/8TB_HDD` —
  keep `/` breathing without touching project tooling.
- Multi-user shared box where you want sweep outputs under your own
  group-quota mount.

If a symlink at the discovery path already points somewhere, it's
replaced. If a *real* directory squats there (e.g., from an earlier
local-only run), HSM warns and leaves it alone — the data still lands
at `<sweeps_root>/<sweep_id>/`, you'll just need to navigate there
manually.

**Hard error if the path is missing.** HSM refuses to silently
`mkdir -p` `sweeps_root` if it doesn't exist on the current machine —
that's almost always a sign that a config file written on one machine
(where `/mnt/8TB_HDD` is mounted) was copied to another (where it
isn't). Either `mkdir -p <sweeps_root>` ahead of time or remove the
field. The error message names both possible config locations
(`~/.hsm/config.yaml` and `<project>/.hsm/config.yaml`).

### Machine vs project config

HSM loads two YAML files and merges them per top-level block:

| Layer | Path | Scope | When to use |
|---|---|---|---|
| Machine | `~/.hsm/config.yaml` | defaults for this user on this box | machine-specific facts: `sweeps_root`, `visible_gpus`, `python_path` |
| Project | `<project>/.hsm/config.yaml` | per-project | everything else: remotes, slurm spec, paths |

**Merge rule:**
- `local:` — **deep-merged** field-by-field, project wins on collisions.
  So the machine's `sweeps_root` flows through even when the project
  sets `local: { gpus: 1 }`.
- `slurm:`, `distributed:`, `paths:`, `project:`, `wandb:` — **only
  honored from the project file**. If you put any of these in
  `~/.hsm/config.yaml`, HSM drops them with a warning at load time
  (rationale: a `distributed:` block in the machine file would
  silently leak the same remotes into every project, which is
  surprising; if you want the same remote everywhere, configure it
  per-project).

Both files are optional. With neither, HSM falls back to built-in
defaults. The machine file is created on first `hsm setup init` if
absent; it's safe to delete and re-create.

### `local.visible_gpus` — restrict which GPU indices the slot queue uses

By default `LocalComputeSource` calls `nvidia-smi -L` and uses *every*
index it reports. On a shared GPU box where (e.g.) `GPU:0` is reserved
for interactive work, set `visible_gpus` to exclude it:

```yaml
local:
  gpus: 1
  visible_gpus: [1, 2, 3]   # never schedule on GPU:0
```

CLI `--gpus` uses the same shape and overrides the config value:

```bash
hsm sweep run --mode local --gpus 1,2,3        # explicit allowlist (overrides config)
hsm sweep run --mode local --gpus 2            # first 2 visible GPUs
hsm sweep run --mode local --gpus cpu          # CPU-only
hsm sweep run --mode local --gpus all          # every detected GPU (default)
```

Indices in `visible_gpus` that aren't in `nvidia-smi -L` output are
warned-and-dropped at setup time (stale allowlists fail loud but don't
crash the sweep).

**Precedence** (highest wins): CLI `--gpus` > `local.visible_gpus` >
every detected GPU.

## Per-remote `spec:` — defaults for `--remote` and `--mode distributed`

`--mode remote` / `--mode distributed` deliberately read **neither**
the `local:` nor the `slurm:` block — remote boxes are heterogeneous,
so their defaults live per-remote under `distributed.remotes.<alias>`:

```yaml
# .hsm/config.yaml
distributed:
  enabled: true
  remotes:
    anahita:
      max_parallel_jobs: 4
      gpus: all                  # CLI --gpus default for this remote
      conda_env: my-env
      spec:                      # default ResourceSpec for this remote
        walltime: "04:00:00"
        cpus_per_task: 4
        mem: "16gb"
        gpus: 1                  # per-task GPU count (≠ allowlist above)
        pre_script:
          - "module load cuda/12"
```

**Precedence** (highest wins): CLI `--gpus`/`--walltime`/`--resources` >
per-remote `spec:` > hardcoded defaults. `hsm remote add` writes the
connection fields only — add `spec:` and `gpus:` by hand-editing.

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

Use `hsm sweep watch <sweep_id>` for live job state, `hsm sweep queue`
for the cluster's queue, or `scancel <id>` directly to cancel a Slurm
job. `hsm sweep cancel <sweep_id>` dispatches to `scancel` for every
job ID recorded in the sweep.

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
