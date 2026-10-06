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
  conda_env: my-project           # single source of truth for the python env
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
4. Hands out the tasks: each child takes the next one whenever it has
   room (fewer active jobs than its `max_parallel_jobs`), so faster
   children take more. A child takes no more tasks once a submit or a
   status check fails (its task goes back to the queue for another child,
   up to 3 tries), or once 40% of its finished jobs (from 5 on) FAILED.
5. Polls each remote's status (squeue over SSH for `backend: slurm`;
   process exit codes for `backend: ssh`).
6. Once **every** task of the sweep is done, each child collects its own
   results: the cluster archives `/scratch → /shares` server-side (with a
   `.archived` sentinel), every remote's `tasks/` is pulled back to HQ's
   sweep dir, and each remote's per-sweep dir is cleaned up. Every remote
   keeps its dir when a task did not complete, or when two remotes share
   one (same host and root, e.g. `uzh` + `uzh-v100`); `hsm remote clean`
   removes them. Nothing is pulled or deleted while any task still runs.
7. Writes `source_mapping.yaml`: each task's child, host, job id and
   status. It is rewritten while tasks are handed out, and on Ctrl-C.

Final state on HQ:

```
/mnt/8TB_HDD/<user>/hsm-sweeps/<sweep_id>/
├── sweep_config.yaml
├── tasks/                    # union of all backends' outputs
│   ├── task_001/             # ran on local
│   ├── task_002/             # ran on ssh-box
│   ├── <sweep_id>_task_003/  # ran on cluster (via sbatch; named after the job)
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
SSH-Slurm child, a distributed sweep caps it at 50 jobs queued or
running (the fair-share default; `--dry-run` shows each child's cap).
Set it explicitly to fit your sweep:

```yaml
distributed:
  remotes:
    uzh:
      backend: slurm
      max_parallel_jobs: 32       # rate-limit sbatch submissions
      ...
```

On a `backend: slurm` remote, `max_parallel_jobs` is also the default
**array throttle** (`--array=1-K%32`) unless `spec.array_throttle` sets one:
an array never has more tasks running at once. See `hsm queue share` (QUEUE.md)
for how loaded the shared account is.

Reasonable values: small enough that you don't dominate the cluster's
fair-share, large enough that a few jobs can be PENDING while others
run. On S3IT, 32–64 is usually fine.

### What about the queue time itself?

Long queue waits on one child **don't block the other children**. A
child at its `max_parallel_jobs` re-checks its jobs every 10 s before it
takes another task; meanwhile, if anahita has free slots and S3IT has 32
jobs sitting PENDING, anahita keeps churning through the remaining work.
You only see head-of-line behavior when *every* child is full.

Status polling itself is cheap: every Slurm source asks one `squeue -u <user>`
and one `sacct` per poll cycle (once a minute; local and ssh sources poll every
10 s), whatever the number of jobs in flight. A
failed `squeue` or `sacct` (a controller outage, a maintenance) changes no
job's state, so it can't end a sweep early. Nor does a dropped connection to
the login node: it is reopened, and while it stays down the polls just fail.
Only individual submission is N round-trips (one `ssh + sbatch` per task).

### `wait_for_all` blocks on the slowest task

If S3IT's last task waits five hours in queue, your `hsm sweep run`
blocks for five hours. Two practical implications:

- **Use `tmux`/`screen` on anahita** so dropping your laptop SSH
  doesn't kill the driver.
- **Ctrl-C stops the driver, not the jobs.** Tasks already started keep
  running and Slurm jobs stay queued; nothing is pulled. Cancel the
  cluster's with `scancel` (`hsm queue mine --remote uzh` lists them; a
  Ctrl-C while tasks are still being handed out logs the
  `ssh <host> scancel <ids>` line, and `source_mapping.yaml` has every
  task's host and job id).

There's no "submit-and-detach" mode yet. If you need one, run the
driver under a long-lived `tmux` session.

### How tasks are placed

There is no placement strategy to pick: each child takes the next task as
soon as it has room. A `backend: slurm` child counts its PENDING jobs as
active, so with a large `max_parallel_jobs` it can take tasks that would
have started sooner on anahita. Cap it (see above) to keep small sweeps
local. The old `strategy`, `collect_interval` and failsafe keys of the
`distributed:` block no longer change anything (the failsafe is the fixed
rule in step 4 above); HSM warns when it sees them.

### When to skip distributed entirely

If a sweep only needs the cluster, **don't** wrap it in distributed —
go straight through the single-remote path:

```bash
ssh anahita "cd ~/code/<proj> && hsm sweep run --remote uzh -c sweep.yaml --mode array"
```

This routes through `SSHSlurmComputeSource._submit_array`: **one
sbatch with `--array=1-N`**. The cluster sees one queue position for
the whole batch, the fair-share calculation amortizes across all
tasks, and `hsm queue mine` shows one entry (with a `×N` Tasks count)
instead of N rows. Submission is also one ssh + sbatch round-trip
instead of N.

The per-remote `spec.gpu_type` also accepts a **list** (e.g.
`[A100, H200]`) with a sibling `speed_factors:` map — HSM then splits
the sweep into one Slurm array per GPU type, biasing expensive tasks
toward fast types and scaling each sub-array's walltime. See
[HPC_EXECUTION.md → Heterogeneous GPU types](HPC_EXECUTION.md#heterogeneous-gpu-types--one-sweep-one-slurm-array-per-type).

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

If your training imports your project's own (editable-installed)
package, that import won't resolve from the rsynced tree alone — see
[SSH_EXECUTION.md → Your project's own package on the remote](SSH_EXECUTION.md#your-projects-own-package-on-the-remote)
(`pip install -e` into the remote env, or a `PYTHONPATH` `pre_script`).

S3IT docs:
[Storage](https://docs.s3it.uzh.ch/cluster/data/) |
[Transfer](https://docs.s3it.uzh.ch/cluster/transfer/) |
[Job submission](https://docs.s3it.uzh.ch/cluster/job_submission/)

## Monitoring the cluster queue from HQ

`hsm queue mine|position|gpus|reservations` run their `squeue`/`scontrol`
queries **over SSH** with `--remote <alias>` — no Slurm needed on the
machine you're sitting at. When the project registers exactly one
`backend: slurm` remote, the flag is optional (auto-fallback with a
printed note), so from HQ this just works:

```bash
cd ~/code/my-project
hsm queue mine                          # auto-uses the sole slurm remote
hsm queue gpus --mine --watch           # live dashboard, one SSH connection
hsm queue position                      # pending GPU tasks, per-task counted
```

Run it from the project directory that launched the sweeps and the
`Sweep` column links each job back to its local sweep dir (for SSH-Slurm
sweeps via `.hsm_manifest.json`) — the from-HQ view is *richer* than the
same command on the login node. See [QUEUE.md](QUEUE.md).

## Watching from your laptop

When you're not at your HQ box, two patterns work:

```bash
# Quick status check — runs on HQ, prints to your laptop's terminal.
ssh hq 'cd ~/code/my-project && hsm sweep status'

# Pull a specific sweep's results back to your laptop (read-only):
rsync -av hq:/mnt/8TB_HDD/<user>/hsm-sweeps/<sweep_id>/ /tmp/<sweep_id>/
```

(For queue state specifically, `hsm queue ... --remote <alias>` works
from the laptop too — any box with SSH access to the cluster.)

### Re-attaching to a sweep whose launcher died

For SSH-Slurm sweeps, if the `hsm sweep run` process exits before every task
finishes (long run + overnight + a maintenance reservation — the classic
`/scratch` purge trap), re-attach from HQ and pull/archive what's done:

```bash
ssh hq 'cd ~/code/my-project && hsm sweep collect <sweep_id>'
```

It reads the sweep's `.hsm_manifest.json`, classifies each job via `sacct`,
pulls terminal task dirs, and runs the `/scratch → /shares` archive once all
tasks are done. Idempotent — re-run as more finish. HSM also pulls completed
tasks **incrementally during the run**, so a single stuck task can't strand the
rest, and warns at submit if a reservation window could outlast the launcher.

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
  + `slurm:` config block reference, heterogeneous GPU-type scheduling,
  and [resumable chained runs](HPC_EXECUTION.md#resumable-chained-runs--finish-a-walltime-job-on-a-capped-pool)
  (finish a >walltime job on a capped/preemptible pool like V100 `lowprio`).
- [getting_started.md](getting_started.md) — first-time setup.
- [../../CLAUDE.md](../../CLAUDE.md) — agent-on-boarding (read first if
  you're an AI assistant landing on this repo).
- [../../ARCHITECTURE.md](../../ARCHITECTURE.md) — design rationale.
