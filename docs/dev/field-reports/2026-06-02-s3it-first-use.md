# HSM first-use feedback — SSH-Slurm to S3IT (UZH), 2026-06-02

First real use of `backend: slurm` (SSH-Slurm) driving `cluster.s3it.uzh.ch` from
anahita, for the `boolean_nca_cc` SODC demo sweep. Connection/auth/env all worked;
these are the friction points hit during setup, with proposed fixes. Severity:
🔴 blocker · 🟡 confusing/wrong-but-recoverable · 🟢 polish.

> **✅ RESOLVED 2026-06-03** — all 8 items fixed on branch
> `field-report-s3it-fixes` (3 commits: data-integrity / UX / reliability).
> Suite 517 passed, 0 failed. The only deferred slice is #8 Tier-3's
> `--dependency=afterany` server-side epilog archive (a clean follow-up; #8's
> T0/T1/T2 + maintenance warning landed). See CLAUDE.md "Recently landed
> (2026-06-03)" and the `field-report-s3it-fixes` memory note. The two live
> workarounds are now obsolete: `$USER` expands in `workdir`/`archive_dir`, and
> `pre_script: [module load miniforge3]` makes conda work without the
> `~/miniforge3` symlink.

---

## 🔴 1. `$USER` not expanded in the rsync destination (`workdir`)
**Symptom**
```
rsync push to uzh:/scratch/$USER/hsm-runs/boolean_nca_cc/code
rsync: [Receiver] mkdir "/scratch/$USER/hsm-runs/boolean_nca_cc/code" failed: No such file or directory (2)
```
The docs (`SSH_EXECUTION.md`, `HPC_EXECUTION.md`) explicitly **recommend**
`workdir: "/scratch/$USER/hsm-runs"`, but rsync receives the path literally (no
remote shell expands it), so it tries to create a directory literally named `$USER`.

**Root cause / inconsistency**
The `mkdir -p <workdir>` setup step runs through a remote *shell* (so `$USER`
expands there), but the rsync **destination** path is passed raw to `rsync`
(no shell) — so the two disagree. Hard-coding `workdir: /scratch/gbena/...`
fixes it.

**Proposed fix** — expand env vars in `workdir`/`archive_dir` once at connect time
by resolving them on the remote (e.g. `ssh <host> 'echo <workdir>'` or
`whoami` substitution) before building the rsync destination; OR, if expansion
is intentionally unsupported, change every doc example away from `$USER` and add
a one-line warning. (`core/remote/ssh_slurm_compute_source.py`, rsync push path.)

---

## 🟡 2. `--dry-run` for `--mode remote` doesn't render the per-remote `spec`
**Symptom** — dry-run prints `GPUs: none requested (spec.gpus=0)` and an empty
`Effective ResourceSpec:`, implying the GPU / account / qos / partition aren't
set, even though they are correctly configured under
`distributed.remotes.<alias>.spec`.

**Reality** — the spec *is* applied: `core/remote/ssh_compute_source.py:607`
reads `remote_cfg.get("spec")` and `:620` merges it into `default_spec`. A real
submit produced the correct `gres/gpu:A100:1` (confirmed via `squeue`/`sacct`).
The dry-run just builds a generic preview before the source resolves its spec
("probed when the run connects").

**Proposed fix** — in the remote dry-run path, build + display the merged
per-remote `ResourceSpec` (and the resulting `#SBATCH` directives) without
opening a connection. This is the single most reassuring thing a first-time user
wants from a dry-run.

---

## 🟡 3. `--remote <alias> --mode array` is rejected, but the docs show it
**Symptom**
```
$ hsm sweep run -c ... --remote uzh --mode array --dry-run
--remote is only valid with --mode remote (got --mode 'array').
```
Both `HPC_EXECUTION.md` and `SSH_EXECUTION.md` show
`hsm sweep run --remote uzh -c sweeps/sweep.yaml --mode array`. The correct
invocation is just `--remote uzh` (backend:slurm decides array-vs-individual).

**Proposed fix** — either accept `--mode array` with `--remote` (map to the
ssh-slurm array path) or correct the doc examples to drop `--mode array`.
Also: the remote ssh-slurm submission currently resolves to
`submission=individual` even for N tasks — worth documenting how to force array.

---

## 🔴 4. hsm reports FAILED jobs as COMPLETED (false-positive success)
**This is the most dangerous one** — it can make an entirely failed sweep look
successful, so a user would proceed to analysis/deploy on artifacts that were
never produced.

**Two observed cases:**
- **Crash at `wandb.init` (no API key):** `sacct` shows both tasks
  `State=FAILED, ExitCode=1:0` (jobs `3690708/3690709`, sweep
  `sweep_20260602_161115`), and the archived `.err` correctly contains the
  Python traceback — yet `hsm sweep run` printed `2/2 done` and
  `Final: 2 COMPLETED, 0 FAILED`.
- **Crash at import (`pandas` missing):** here it *did* surface as FAILED
  (`task_info.txt: Status: FAILED`, hsm `0/2 done`). So failure detection is
  **inconsistent** depending on timing / how the job leaves the queue.

**Root cause** — the SLURM wrapper propagates the exit code correctly (SLURM
State=FAILED is right). The gap is hsm's **terminal-state detection**: it appears
to treat "job no longer in `squeue`" as success rather than querying
`sacct -j <id> --format=State,ExitCode` for the actual terminal state. (Same
family as the documented "array progress reports 1/1" gotcha — queue-presence is
not a completion signal.)

**Proposed fix** — once a job leaves `squeue`, classify it via
`sacct` State (COMPLETED vs FAILED/CANCELLED/TIMEOUT/OOM); surface an accurate
`N COMPLETED / M FAILED` summary, exit nonzero if any failed, and print the path
to each failing task's `.out`/`.err`. Optionally also gate "COMPLETED" on a
success sentinel/artifact (e.g. the task's `final_results.csv` existing).

---

## 🟢 5. `DEFAULT_RSYNC_EXCLUDES` misses common ML artifact dirs
`push_exec.py:16` excludes `.git, __pycache__, *.pyc, .venv, venv,
sweeps/outputs, *.ckpt, *.pt, wandb`. It does **not** exclude `checkpoints/`,
`outputs/` (only `sweeps/outputs`), `multirun/`, or `*.pkl` — so a typical
training repo pushes hundreds of MB the cluster never needs (here: 639 MB of
`.pkl` checkpoints). Suggest either adding these to the defaults or calling out
prominently in the quickstart that `rsync_excludes` should list artifact dirs.

---

## 🟢 6. Misc
- `hsm setup init` mis-detected **PBS** as the local HPC system on a box that
  drives Slurm over SSH (harmless for `--mode remote`, but confusing).
- `docs/user_guide/*.md` referenced from the project README live in the **HSM
  package repo**, not the project — a `hsm docs` command or a note would help.
- The conda-init partial works on S3IT only with `pre_script: [module load
  miniforge3/...]`; the auto-probe of standard conda paths does not find the
  S3IT module-provided miniforge. Worth documenting the `module load` pre_script
  as the canonical S3IT recipe.
- Undeclared dep surfaced downstream: `train.py` imports `pandas` which wasn't in
  the project's `pyproject.toml` — not an HSM issue, but the kind of thing the
  3-second-FAILED-with-buried-stderr (#4) made slow to diagnose.

---

## 🔴 7. conda-init probe misses module-based conda → silently trains on CPU
**Highest-impact bug** (cost hours; produced a sweep training on CPU at ~20× slowdown while sstat/hsm looked fine).

**Mechanism** — the rendered script's conda-init probes only fixed paths
(`$HOME/{miniconda3,anaconda3,miniforge3,.miniconda3}`, `/opt/conda`). On S3IT
conda comes from `module load miniforge3` (prefix under `/apps/...`), so none
match → it falls through to the micromamba branch, finds `~/micromamba`, and
defines `conda() { micromamba "$@"; }`. The pre_script `module load miniforge3`
runs **after** this and puts the real conda on PATH — but the **function shadows
it**, so `conda run -n <env>` becomes `micromamba run -n <env>` against
`MAMBA_ROOT_PREFIX=~/micromamba`, resolving a *different* env than the one
created with the module's mamba (`~/conda/envs/<env>`). jax-cuda then can't init
(`cuInit: CUDA_ERROR_NO_DEVICE`) and **falls back to CPU** (~6.9 s/epoch vs ms).
Verified: same job via real conda → `[CudaDevice(id=0)]`; via the micromamba
bridge → CPU. (Compounds with #4: the CPU run "succeeds" and is reported done.)

**Workaround applied** — `ln -sfn <module-prefix> ~/miniforge3` so the probe
finds a real `conda.sh` first and never defines the bridge. Confirmed → GPU.

**Proposed fix** — (a) run the pre_script `module load` BEFORE the conda-init
probe; and/or (b) also probe `command -v conda` / `$CONDA_EXE` (catches a
module-provided conda); and/or (c) don't define the `conda()->micromamba` bridge
if a real conda/mamba is/will be on PATH; (d) the micromamba branch shouldn't
hard-assume `~/micromamba` as the env root. The ordering (module load after the
probe) is the core bug.

---

## 🔴 8. Result retrieval is all-or-nothing AND coupled to launcher liveness
*(2026-06-03 follow-up, same S3IT sweep `sweep_20260602_172806`. This is the
single biggest reliability gap we hit — it can silently strand an entire sweep's
results. Tiers below are our read on the fix space given a real cluster workflow;
final design calls are the maintainer's.)*

**What happened.** An 8-task sweep launched ~17:28; 7/8 tasks finished overnight,
task 6 (`max_neighbors=33`) is held behind a scheduled **maintenance reservation**
(06:00–18:00 next day). By the next morning: the launching process was **gone**,
the cluster was **in maintenance** (SSH login refused), and on the workstation we
had per-task `checkpoints/*.pkl` + `command.txt`/`final_results.csv`/`task_info.txt`
— but **no run configs at all**, and **no server-side archive** on `/shares`.

**Three coupled root causes:**

1. **No incremental sync on the ssh-slurm backend.** `collect_results()` is called
   **exactly once** (`sweep_orchestrator.py:367`), *after* `wait_for_all()` reports
   every job terminal. `wait_for_all()` only polls `squeue` — it pulls nothing
   mid-flight. So a sweep returns **all results or none**, gated on the *slowest /
   most-stuck* task (here: one task blocked for ~12h by a maintenance window).
   Notably the pattern already exists in the codebase —
   `distributed_manager.py:598 _collect_results_continuously` does per-job
   collection for `--mode distributed` — it's just never wired to ssh-slurm.

2. **Retrieval is coupled to the launcher process.** Both the final pull **and** the
   server-side archive-to-`/shares` live *inside* `collect_results()`, which only
   runs if the original `hsm sweep run` process is still alive to execute it. Ours
   wasn't (long sweep + overnight + maintenance window = the babysitter died). There
   is **no re-attach / resume path**: no `hsm sweep collect <sweep_id>` to pull a
   sweep whose launcher is gone. The durable safety net (`/shares` archive) is a
   good idea **gated in the wrong place** — it never fires precisely in the scenario
   it's meant to protect against (an interrupted long run).
   *(The per-task checkpoints we do have arrived via an out-of-band manual `rsync`,
   not the hsm pipeline — which is exactly why they came without configs.)*

3. **Config is not co-located with the checkpoint.** Even on a clean full run, the
   only config that the `tasks/`-pull delivers is buried in
   `tasks/<t>/wandb/run-*/files/config.yaml` (next to `output.log`,
   `requirements.txt`, the `.wandb` binary, debug logs) — not a clean artifact, and
   absent entirely from any partial/manual pull. Hydra's tidy `.hydra/config.yaml`
   is written to the **job cwd (the rsync'd code mirror)**, *outside* `tasks/`, so
   the `tasks/`-only pull **never** retrieves it. Net: a synced checkpoint is **not
   self-describing** — you cannot rebuild its model without separately recovering
   the config. (We recovered ours from the **W&B cloud** run record + verified it by
   rebuilding to the exact param count — but that only works if W&B logging was on.)

**Proposed fixes — tiered (maintainer picks the cut line):**

- **Tier 0 — decouple collection from the launcher (highest value / lowest cost).**
  Add `hsm sweep collect <sweep_id>` that re-attaches by id and pulls/archives
  whatever is terminal, with **no dependence on the original process's in-memory
  state**. Make it work by writing a small **sweep manifest server-side at submit
  time** (`{job_ids, remote task paths, resolved config per task, sweep_id}`), so a
  fresh client can find and classify everything via `sacct`. This alone would have
  fully recovered our sweep.

- **Tier 1 — per-job / continuous sync for ssh-slurm.** Port
  `_collect_results_continuously` to the ssh-slurm source: as each task hits a
  terminal state, immediately pull **that task dir** (checkpoints + config) and
  run its `/shares` archive. Partial progress then survives a launcher death and a
  single stuck task can't hold the other 7 hostage. (Pair with Tier 0 so a *restart*
  resumes where streaming left off.)

- **Tier 2 — make every checkpoint self-describing.** Drop a clean
  `resolved_config.yaml` into each task's output dir (beside `checkpoints/`) at job
  start — either hsm copies `.hydra/config.yaml` into `tasks/<t>/` in the wrapper
  before the job body runs, or the wrapper serializes the composed config there.
  Then no pull, partial or full, can ever orphan a checkpoint from its config.
  (Partly a project convention, but hsm is the right place to standardize/enforce
  it, since hsm owns the wrapper.)

- **Tier 3 — survive long runs without a live client at all.** Move the archive
  server-side as a **Slurm epilog / dependency job** (`--dependency=afterany` on the
  array) that rsyncs `/scratch → /shares` and writes a `.archived` sentinel from
  *inside* the cluster — so durability never depends on the workstation being up.
  And/or a detachable monitor daemon (survives the interactive session). Bonus:
  **detect a scheduled maintenance reservation** at submit time (`scontrol show res`)
  and warn when a sweep's walltime can't complete within one launcher lifetime —
  that's the trap we fell into.

**Severity rationale.** This is 🔴 because the failure is *silent and total*: no
error, the launcher just exits, and a multi-hour sweep's outputs sit on ephemeral
`/scratch` (30-day purge) with no config and no archive, recoverable only by luck
(manual rsync) + an external config source (W&B). Tier 0 + Tier 2 together remove
the whole failure class cheaply.

## What worked well
- `~/.ssh/config` alias resolution, `hsm remote test/health` — clean and fast.
- Per-remote `spec` (gres/account/qos/partition/pre_script) → correct `#SBATCH`.
- `sweeps_root` redirect to `/mnt/8TB_HDD` + discovery symlink — transparent.
- `archive_dir` on the group share, ephemeral `workdir` on `/scratch` — good model.
