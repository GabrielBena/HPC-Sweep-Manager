# Multi-cluster sweeps — the HQ pattern

This guide is for the workflow where one machine (your **HQ**) drives
sweeps that fan out across a heterogeneous mix of compute: HQ's own
GPUs + one or more SSH workstations + one or more SSH-reachable Slurm
clusters. Results land back on HQ in a single sweep dir.

Why a separate doc: this is the only place that connects the four
features that make multi-cluster sweeps actually work end-to-end —
`local.sweeps_root`, `backend: ssh`, `backend: slurm`, and
`--mode distributed`. Each is documented individually elsewhere; this
guide is the glue.

If you only have one remote, you don't need this doc — read
[SSH_EXECUTION.md](SSH_EXECUTION.md) instead.

## The pattern

```
laptop ──ssh──> HQ workstation (your control plane)
                  │
                  ├── local GPUs (LocalComputeSource)
                  ├──ssh──> SSH workstation (backend: ssh)
                  └──ssh──> Slurm cluster login (backend: slurm)
                                                 ↓
                                                /scratch (active sweep)
                                                 ↓ archive on completion
                                                /shares (durable)
```

**HQ** is whatever Linux box you mostly work from — a lab workstation
with GPUs, near-100% uptime, generous local storage. It's where:

- HSM is installed (one editable clone per project's conda env).
- Sweep directories live (you may want `local.sweeps_root` if your
  system disk is tight — see below).
- The `hsm` driver process lives during the sweep, holding open SSH
  connections to each remote and polling their queues.

The laptop is just a viewer: you `ssh HQ` and either drive `hsm`
interactively or check on existing sweeps with `hsm sweep status` /
`hsm sweep report`.

## Setup (one-time per HQ)

1. **Install HSM on HQ.** Editable from a clone in each project's conda
   env (mirroring how you'd install it on your laptop):

   ```bash
   git clone <this-repo> ~/code/packages/HPC-Sweep-Manager
   cd ~/code/packages/HPC-Sweep-Manager
   conda activate <your-research-env>
   pip install -e ".[dev]"
   hsm --version
   ```

2. **Generate an SSH key on HQ for each cluster you'll drive.** Without
   this, HSM can't reach the remotes. From HQ:

   ```bash
   ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519_cluster -C "hq → cluster"
   # Copy the public key to the cluster's authorized_keys. Simplest
   # path: do it once from your laptop (which already has cluster
   # access):
   ssh hq "cat ~/.ssh/id_ed25519_cluster.pub" \
       | ssh cluster "cat >> ~/.ssh/authorized_keys"
   ```

3. **Add `Host` blocks to HQ's `~/.ssh/config`** matching the aliases
   you'll reference in `.hsm/config.yaml`. Mirror whatever you have on
   the laptop:

   ```sshconfig
   Host cluster
       HostName cluster.example.edu
       User <username>
       IdentityFile ~/.ssh/id_ed25519_cluster
       PreferredAuthentications publickey

   Host ssh-box
       HostName 10.x.y.z
       User <username>
   ```

4. **Verify from HQ:**

   ```bash
   ssh cluster hostname && ssh cluster command -v sbatch
   ssh ssh-box hostname
   ```

5. **One-time per project: `hsm setup init`.** This scaffolds
   `.hsm/config.yaml` with the `local:` and `distributed:` blocks
   commented out — uncomment the parts you need.

## The headline config

Put this in your project's `.hsm/config.yaml` on HQ. Replace alias
names, conda env, group share path, etc.

```yaml
project:
  name: my-research-project
  root: /home/<user>/code/my-research-project

paths:
  python_interpreter: python
  train_script: scripts/train.py
  config_dir: configs

# HQ-local config: HQ's own GPUs as a child of distributed.
# (sweeps_root belongs in ~/.hsm/config.yaml — it's a machine fact, not
# a project fact. See HPC_EXECUTION.md#machine-vs-project-config.)
local:
  gpus: all                                       # HQ's GPUs are visible by default

# Multi-cluster fan-out
distributed:
  enabled: true
  strategy: least_loaded

  remotes:
    # An SSH workstation, e.g., another lab box. backend: ssh is the
    # default; tasks run as `conda run -n <env> python ...` directly.
    ssh-box:
      host: ssh-box
      max_parallel_jobs: 4
      conda_env: my-env
      gpus: all
      spec:
        # SSH-bash spec — walltime is a guard, not a Slurm directive.
        cpus_per_task: 4
        gpus: 1

    # A real Slurm cluster, reached over SSH. backend: slurm makes HSM
    # submit `sbatch` jobs (one array per sweep) and poll `squeue` over
    # the same SSH connection.
    cluster:
      backend: slurm
      host: cluster
      max_parallel_jobs: 32
      conda_env: my-env
      # Storage tier — /scratch for the active run, /shares for the
      # durable copy. Set these if your cluster splits ephemeral vs
      # permanent storage (S3IT does — see note below).
      workdir: "/scratch/$USER/hsm-runs"
      archive_dir: "/shares/<group>/hsm-archive"
      archive_on: completed
      qos_whitelist: [normal, medium, long]
      spec:
        walltime: "06:00:00"
        cpus_per_task: 4
        mem: "32G"
        gpus: 1
        gpu_type: H100              # case-sensitive on S3IT
```

## Running

From HQ, in the project directory:

```bash
hsm sweep run --mode distributed -c sweeps/my_sweep.yaml
```

HSM:

1. Builds three children — `LocalComputeSource`, `SSHComputeSource`,
   `SSHSlurmComputeSource` — based on the `distributed:` block.
2. Rsyncs your project up to each remote in parallel.
3. Verifies `sbatch`/`squeue` on the Slurm remote.
4. Submits tasks to whichever source is least-loaded (each parameter
   combination lands on one source's queue).
5. Polls each remote's status (squeue over SSH for `backend: slurm`;
   process exit codes for `backend: ssh`).
6. When the cluster's portion finishes, server-side rsyncs
   `/scratch → /shares` and writes a `.archived` sentinel.
7. Pulls each remote's `tasks/` back to HQ's sweep dir.
8. Cleans up the per-sweep dir on each remote (failed runs are kept on
   `/scratch` for inspection).

Final state on HQ:

```
/mnt/8TB_HDD/<user>/hsm-sweeps/<sweep_id>/
├── sweep_config.yaml
├── tasks/                    # union of all backends' outputs
│   ├── task_001/             # ran on local
│   ├── task_002/             # ran on ssh-box
│   ├── task_003/             # ran on cluster (via sbatch)
│   └── ...
├── logs/
└── scripts/
```

…and on the cluster:

```
/shares/<group>/hsm-archive/<sweep_id>/
├── .archived                 # timestamp + provenance
├── tasks/                    # full snapshot — survives /scratch's purge
├── logs/
└── scripts/
```

The HQ copy is the working data. The cluster archive is your insurance
against `/scratch` being garbage-collected (S3IT auto-deletes files
unread for 30 days).

## Distributed + Slurm queue interaction (read this before going big)

When an SSH-Slurm child sits in a distributed sweep, HSM submits one
`sbatch` per task — *not* one array job. That's not an oversight: the
whole point of distributed is dynamic per-task dispatch, and an array
would commit the whole batch to one child up front. But it changes how
queue time behaves vs. a plain `--mode array` submission.

### How slot accounting interacts with PENDING jobs

The dispatcher counts a Slurm-PENDING job as occupying a slot on its
child source. That's intentional — without it, the dispatcher would
flood `sbatch` while the cluster's queue is already backed up. The
consequence: **`max_parallel_jobs` on a `backend: slurm` remote is your
practical rate limiter, not a parallelism cap**. If you set it to 32,
HSM submits up to 32 jobs and then waits for completions to free slots.

**Watch the default.** When `max_parallel_jobs` is unset on an
SSH-Slurm child, HSM falls back to an effectively-unlimited
`max_parallel_jobs=10_000`. For S3IT and most fair-share-priced
clusters, set this explicitly:

```yaml
distributed:
  remotes:
    uzh:
      backend: slurm
      max_parallel_jobs: 32       # rate-limit sbatch submissions
      ...
```

Reasonable values: small enough that you don't dominate the cluster's
fair-share, large enough that a few jobs can be PENDING while others
run. On S3IT, 32–64 is usually fine.

### What about the queue time itself?

Long queue waits on one child **don't block the other children**. The
dispatcher polls all sources every ~5 seconds; if anahita has free
slots and S3IT has 32 jobs sitting PENDING, anahita keeps churning
through the remaining work. You only see head-of-line behavior when
*every* child is at `max_parallel_jobs`.

Status polling itself is cheap — `SSHSlurmComputeSource` batches every
live job ID into one `squeue -j 1,2,3,…` call per poll cycle, so the
polling cost is O(1) round-trips regardless of how many jobs are in
flight. Only submission is N round-trips (one `ssh + sbatch` per task).

### `wait_for_all` blocks on the slowest task

If S3IT's last task waits five hours in queue, your `hsm sweep run`
blocks for five hours. Two practical implications:

- **Use `tmux`/`screen` on anahita** so dropping your laptop SSH
  doesn't kill the driver.
- **Ctrl-C does the right thing**: HSM's signal handler calls
  `scancel` on every still-pending S3IT job before exiting. You won't
  leave orphans, but the rsync-pull doesn't run, so re-submit after
  fixing the cause if you want results back.

There's no "submit-and-detach" mode yet. If you need one, run the
driver under a long-lived `tmux` session.

### Strategies — pick `least_loaded`

The three available `distributed.strategy` values interact very
differently with Slurm children:

| Strategy | Slurm-friendly? | Why |
|---|---|---|
| `round_robin` | ⚠️ | Fills anahita to `max_parallel_jobs` then cycles to uzh regardless of queue depth. For a 5-task sweep with 4-slot anahita, task 5 lands on uzh and sits in queue while anahita is idle. |
| `least_loaded` (**recommended**) | ✓ | Picks `min(sources, key=utilization)`. Local sources are built before SSH children, so anahita wins ties at the start of a sweep — small sweeps stay on anahita and only overflow goes to uzh. |
| `capability_based` | ✗ **avoid with Slurm** | Picks `max(sources, key=available_slots)`. uzh's 32 always beats anahita's 4, so this dumps everything on the cluster and leaves anahita idle. Fine for SSH-only fan-out; bad for SSH + Slurm. |

A future `queue_aware` strategy that consults `SlurmQueue.position()`
and prefers the source with shortest expected wait is on the wishlist;
plumbing in `core/hpc/scheduler_queue.py` already exists. Until then,
`least_loaded` + a sensibly capped `max_parallel_jobs` is the right
combination.

### When to skip distributed entirely

If a sweep only needs the cluster, **don't** wrap it in distributed —
go straight through the single-remote path:

```bash
ssh anahita "cd ~/code/<proj> && hsm sweep run --remote uzh -c sweep.yaml --mode array"
```

This routes through `SSHSlurmComputeSource._submit_array`: **one
sbatch with `--array=1-N`**. The cluster sees one queue position for
the whole batch, the fair-share calculation amortizes across all
tasks, and `hsm queue mine` shows one entry instead of N. Submission
is also one ssh + sbatch round-trip instead of N.

Use distributed when you genuinely want heterogeneous fan-out (mix of
local + SSH workstation + cluster). For "I just want this on S3IT,"
`--remote uzh --mode array` is faster, cheaper, and easier to inspect.

## S3IT-specific notes

The example above is tuned for the UZH S3IT cluster. The cluster-side
specifics:

| Path | Quota | Persistence | What to put here |
|---|---|---|---|
| `~/` (`$HOME`) | 400 GB | permanent (no backups) | conda envs, configs, small data |
| `/scratch/$USER` | 20 TB | **30-day no-access purge** | active sweeps (`workdir`) |
| `/shares/<group>` | per-group quota | permanent (no backups) | sweep archives (`archive_dir`) |

S3IT login lives at `cluster.s3it.uzh.ch`. GRES names are
case-sensitive — use `gpu_type: H100` (uppercase), not `h100`. Check
what's actually available with `sinfo -o "%P %G"` on the cluster.

S3IT docs:
[Storage](https://docs.s3it.uzh.ch/cluster/data/) |
[Transfer](https://docs.s3it.uzh.ch/cluster/transfer/) |
[Job submission](https://docs.s3it.uzh.ch/cluster/job_submission/)

## Watching from your laptop

When you're not at your HQ box, two patterns work:

```bash
# Quick status check — runs on HQ, prints to your laptop's terminal.
ssh hq 'cd ~/code/my-project && hsm sweep status'

# Pull a specific sweep's results back to your laptop (read-only):
rsync -av hq:/mnt/8TB_HDD/<user>/hsm-sweeps/<sweep_id>/ /tmp/<sweep_id>/
```

There's no `hsm sweep collect` command yet — for now, plain `rsync` is
the recommended path.

## Smoke test before turning on distributed

Before fanning across multiple clusters, validate each leg
individually:

```bash
# 1. Local-only sweep to confirm the project itself works.
hsm sweep run --mode local -c sweeps/tiny.yaml --parallel-jobs 2

# 2. SSH workstation alone.
hsm sweep run --remote ssh-box --gpus 1 --resources "--gpus=1" -c sweeps/tiny.yaml

# 3. SSH-Slurm cluster alone.
hsm sweep run --remote cluster -c sweeps/tiny.yaml --mode array

# 4. Full distributed fan-out — only after 1-3 work.
hsm sweep run --mode distributed -c sweeps/tiny.yaml
```

Or use the runnable smoke scripts:

- [`examples/smoke_cli.sh`](../../examples/smoke_cli.sh) — local /
  Slurm-on-this-machine variants.
- [`examples/smoke_ssh_cli.sh`](../../examples/smoke_ssh_cli.sh) —
  `backend: ssh` (bash-over-SSH).
- [`examples/smoke_ssh_slurm_cli.sh`](../../examples/smoke_ssh_slurm_cli.sh) —
  `backend: slurm` (sbatch-over-SSH). Pass `WORKDIR=...` and
  `ARCHIVE_DIR=...` to exercise the storage-tier flow.

## See also

- [SSH_EXECUTION.md](SSH_EXECUTION.md) — the SSH push model in depth,
  both `backend: ssh` and `backend: slurm`.
- [HPC_EXECUTION.md](HPC_EXECUTION.md) — the on-cluster Slurm flow
  + `slurm:` config block reference.
- [getting_started.md](getting_started.md) — first-time setup.
- [../../CLAUDE.md](../../CLAUDE.md) — agent-on-boarding (read first if
  you're an AI assistant landing on this repo).
- [../../ARCHITECTURE.md](../../ARCHITECTURE.md) — design rationale.
