# Changelog

All notable changes to HPC-Sweep-Manager are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/); this project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed (2026-10 maintenance pass — `docs/dev/maintenance-2026-10.md`)

- **A Slurm outage could delete a live sweep dir (S1).** When `squeue` failed
  (rc≠0) and `sacct` failed too, a job was taken as COMPLETED; the launcher (or
  `hsm sweep collect`) then archived and `rm -rf`'d the remote sweep dir while
  tasks were still queued. A failed call is now never a verdict: a failing squeue
  or sacct changes no state that cycle, and a job that left the queue waits for
  sacct to name its terminal state. COMPLETED is assumed only when accounting has
  no record of the job (no rows, or accounting absent) on 3 polls in a row.
- **Status polling is batched for every Slurm source (S2).** One `squeue -u <user>` and
  one `sacct` per poll, shared by the native and SSH-driven sources
  (`core/hpc/slurm_base.py`); it was one `squeue` (+ `sacct`) per job per cycle,
  600 SSH channels a cycle at 600 jobs. `collect` and `advance` use the same
  refresh.
- **`sbatch` output without a `Submitted batch job <id>` line now raises** instead
  of guessing an id from the last word of the output.
- **Slurm remotes submit one job array by default (S3).** `--remote <slurm>` used to
  submit one `sbatch` per task (about 1,500 SSH sessions locked a user out of a
  login node); it now submits one `sbatch --array` and says so. `--mode individual`
  keeps the old style and warns above 50 tasks.
- **A submission stopped partway is still tracked (S3).** An error or a Ctrl-C in the
  middle of a loop of `sbatch` calls used to leave the live jobs out of the
  manifest; it is now written before the error propagates, so `hsm sweep collect`
  can re-attach. (Inside a resumable chain the chain's own manifest is kept, and the
  log names the jobs to cancel before `hsm sweep advance`.) A cluster whose
  `MaxArraySize` is smaller than the sweep rejects the array at submission, and the
  error now suggests `--mode individual`.
- **A stale SSH agent no longer breaks the connection (X2).** When `SSH_AUTH_SOCK`
  pointed at an agent that no longer answers (often one forwarded by an old
  session), asyncssh waited in auth until the server reset the connection: HSM
  reported a bare `Connection reset by peer` while `ssh <host>` worked, and the
  workaround was `env -u SSH_AUTH_SOCK hsm …`. The login is now bounded at 30 s; a
  login timeout or reset while an agent is in use is retried once without the
  agent, with a warning that names it and the lasting fix (`IdentityAgent none` in
  `~/.ssh/config`). HSM then skips the agent for that host for the rest of the run,
  rsync included. A login that still fails says so: `SSH login to <host> timed out
  or was reset …`.
- **An unreachable host fails within 60 s, with a clear message.** The whole
  connect, ProxyJump hops included, is bounded at 60 s (`Could not reach <host>
  within 60 s …`), instead of the OS's TCP timeout (about 2 min).
- **rsync no longer hangs on a prompt or a dead link.** Every rsync runs ssh with
  `BatchMode=yes`, `ConnectTimeout=30` and `ServerAliveInterval=30` (passed with
  `-e`, so it overrides `RSYNC_RSH`): a push or a pull fails instead of waiting for
  a password, or for TCP keepalive to notice a dead link (about 2 h).
- **`--remote X --mode auto` ran locally (C1).** `auto` resolved to
  `local`/`array` and ignored the alias. With `--remote`, `auto` now means
  remote, and `build_compute_source` refuses an alias with any other mode.
- **One unknown key dropped a whole spec block (C2).** A typo such as
  `cpus: 4` in a per-remote `spec:` or the `slurm:` block discarded every
  field (account, qos, `--exclude`, …) behind a single log line. Now only the
  unknown key is dropped, with a warning naming it; an invalid value of a
  known key stops the run with an error naming the block.
- **An unregistered `--remote` alias became a bare ssh-bash remote (C6).** A
  typo'd `uzh` could train on the cluster's login node. When the project has
  a `distributed.remotes:` block, an alias missing from it is now an error
  with a "did you mean" suggestion; projects without the block keep bare
  `~/.ssh/config` aliases.
- **Same-second launches shared a sweep dir (C7).** Sweep ids have
  one-second resolution and the dir was created with `exist_ok`, so two
  launches shared their local and remote dirs, and one's cleanup could delete
  the other's. The dir is now created exclusively; on a collision the id gets
  a `_2` (`_3`, …) suffix.
- **Each sweep runs its own code (S4).** Every launch re-synced one shared remote
  `code/` dir with `--delete`, so tasks of an earlier sweep still queued on a
  cluster ran the newest code, and files tasks wrote in their working dir were
  deleted by the next push. Each sweep now pushes to `snapshots/<sweep_id>/`
  (hard-linked against the previous one, so it is cheap), its tasks run there, and
  every wrapper exports `$HSM_CODE_DIR`. A snapshot lives as long as its sweep dir,
  and with an `archive_dir` it is archived with the results. A `code/` dir from an
  older HSM is left untouched (remove it once nothing runs from it), and a
  `pre_script` naming it is pointed at `$HSM_CODE_DIR` with a warning. **Upgrade
  every HSM that launches on a remote together:** an older one keeps pushing to the
  shared `code/` dir.
- **`--mode distributed` deleted results under running tasks (X3).** Its
  collector ran every 30 s and called an ssh child's `collect_results`, which
  `rm -rf`'d the remote sweep dir while other tasks still ran there; the last
  tasks to finish were never pulled, and `backend: slurm` children were never
  pulled, archived or cleaned. `distributed_manager.py` (1,632 lines) is deleted;
  `DistributedComputeSource` now hands out the tasks itself: one worker per child
  takes the next task whenever the child has room, then every child waits on its
  own jobs and collects its own results, only once no task of the sweep is active.
  Two remotes with the same host and root (`uzh` + `uzh-v100`) share one remote
  sweep dir, so one's cleanup could delete the other's tasks: now every remote
  keeps its dir when a task did not complete or when two remotes share one, and
  the log says so (`hsm remote clean` removes them).
- **A failed submit hung a distributed sweep (X3).** A task whose submit failed
  three times was never counted, so the launcher waited forever, and a Ctrl-C was
  swallowed. Now a failed submit retires that child and hands the task to
  another, up to 3 tries; then the task is FAILED. Tasks no child could take are
  FAILED too, and the sweep exits non-zero. A child also retires when its status
  check fails (a dead connection is no longer polled forever) or once 40% of its
  finished jobs (from 5 on) FAILED, the old failsafe as a fixed rule. A child that
  loses its connection mid-run has its jobs reported FAILED, and the other
  children are still collected.
- **A distributed Slurm child no longer sbatches the whole queue (X3).** Without
  `max_parallel_jobs` (or with `0`) it is capped at 50 jobs queued or running, the
  fair-share default; `--dry-run` shows each Slurm child's cap. A `max_parallel_jobs`
  that isn't a whole number is an error even when the spec sets `array_throttle`;
  `0` still means no cap outside distributed mode.
- **`source_mapping.yaml` records the submitted jobs (X3).** Each task has its
  host and job id, a Slurm child's task is keyed by its real dir
  (`<sweep_id>_task_003`, which `hsm sweep status` reads), and the file is
  written while tasks are handed out and on Ctrl-C, not only at the end. It lost
  `sweep_metadata.strategy` and the per-task `start_time`. A distributed run's
  job ids are `<child>:<job id>`: two clusters' ids could collide and hide a
  FAILED.
- **The local child of a distributed sweep ignored the `local:` block (X3).** It
  ran without `local.visible_gpus` and the per-task spec, so its tasks could land
  on a reserved GPU (anahita's GPU 0). It now uses both, like `--mode local`, and
  an invalid `local:` block fails the run, as in `--mode local`.
- **Distributed no longer installs process-wide signal handlers (X3).** Ctrl-C
  stops the driver and leaves started tasks running and Slurm jobs queued, as in
  the other modes; a Ctrl-C during dispatch logs the `ssh <host> scancel <ids>`
  line. Ctrl-C now exits 1 (`Aborted!`) and SIGTERM 143; both used to exit 0
  after the handler's cleanup. The `strategy`, `collect_interval` and failsafe
  keys of the `distributed:` block change nothing (the failsafe keys never did),
  and HSM warns when it sees them.
- **YAML 1.1 numbers changed sweep values and walltimes (C4).** `1e-1`
  loaded as a string, `[007, 010]` as `[7, 8]` (octal), and an unquoted
  `walltime: 12:00:00` as the int 43200, which rendered `--time=43200`
  (30 days); an unquoted `chunk_walltime` crashed the resumable check. Sweep
  files and `.hsm/config.yaml` now load with YAML 1.2 numbers (decimal ints,
  `1e-1` floats; `yes`/`no` unchanged), and HSM writes them back quoting every
  string either grammar would read as a number. A `walltime` must be a Slurm
  time: `H:M:S`, `D-H[:M[:S]]`, an int of minutes, or `UNLIMITED` (`0` still
  writes no limit, as before). **A two-part `walltime: 23:00` is now an error:**
  unquoted it used to mean 23 hours (YAML 1.1's 1380 minutes), while Slurm
  reads `"23:00"` as 23 minutes, so write `"23:00:00"`. `chunk_walltime` must
  be a quoted `HH:MM:SS`, and an unquoted `signal_grace: 5:00` is an error
  asking for seconds. Two things a job sees differently: a sweep value written
  `2e-4` reaches it as `0.0002` (the same float), and a list value such as
  `[1e-3, 1e-4]` now arrives as floats where it used to arrive as strings.
  `hsm sweep advance` re-reads the sweep file with the new loader, so finish a
  chain started before this change with the HSM that started it.
- **`hsm remote add/remove` rewrote the project file from the merged config
  (C3).** They stripped every comment, copied machine keys
  (`local.sweeps_root`, `visible_gpus`) into the git-tracked project file, and
  `add` replaced an existing entry, so re-adding `uzh` erased `backend: slurm`,
  `workdir` and `spec`. They now edit the project file only (never the machine
  config, even from `$HOME`), `add` updates just the fields you pass, and a
  file with comments is left untouched: the YAML to paste is printed and the
  command exits 1.
- **`hsm remote clean` could delete the wrong directory, or `~` (C8).** It ran
  an unquoted `rm -rf`, named the project after the cwd, ignored a slurm
  remote's `workdir`, and with `remote_root: ~` plus `--all-projects` removed
  the home directory. It now targets the same dir the sweep sources use
  (`workdir` or `remote_root`, plus the project root's name). Before any
  prompt, one remote command canonicalises the target with `realpath` and
  lists it; the target is refused if it is `/` or `$HOME` (or above it) under
  any spelling or symlink, or if it holds anything but HSM's own
  `<project>/{code,sweeps,snapshots}`. The prompt shows the canonical path,
  `-y` skips only the prompt, and the `rm` is quoted. A root with shell
  metacharacters, or a default-mode run outside a project, is refused first.
- **Arrays can be throttled (S5).** `spec.array_throttle: N` renders
  `--array=1-K%N`; before, HSM had no throttle and the consumer ran
  `scontrol update ArrayTaskThrottle=N` after every submission. On a `backend:
  slurm` remote, `max_parallel_jobs` now becomes that throttle when the spec sets
  none: it used to be a client-side count that Slurm arrays never saw. A multi-GPU-type
  sweep shares the throttle across its per-type arrays, so `N` caps the whole sweep. An
  older HSM running `hsm sweep advance` on a newer chain drops the throttle: upgrade
  together.
- **`hsm queue share` (FR#10).** How loaded the shared account is, and how much of it
  is you: the account's usage against its share, running CPUs by user, co-workers
  waiting and why. It exits 3 when the account is hot. One SSH round trip.
- **Slurm launches check the account's fair share (S-4).** `hsm sweep run` prints the
  account's load before every Slurm launch whose spec has an `account` (not a dry run
  or `--mode distributed`), and when the
  account is hot, or the check can't tell (it failed, or `sshare` gave nothing), it
  asks: throttle to 50 at once and go (the default, also taken with no terminal, so
  an unattended launch on a hot account is throttled), launch as asked (`--force`
  skips the question), wait for the account to cool (re-checked every 30 min, at
  most 12 h), or cancel.
- **CPU-only jobs keep off GPU nodes (S6).** A Slurm job without GPUs could land on a
  GPU node and take its CPUs and memory (24 of ~300 replay tasks did). HSM now adds
  the partition's GPU nodes (one `sinfo` per partition) to `--exclude`, merged with
  any you set; `spec.cpu_only_nodes: false` allows them. It does so only when the spec
  names a partition that has CPU nodes too, and never for a job that asks for GPUs
  through `gpus` or an `extra_directives` `--gres`/`--gpus*`. `extra_directives` keys
  without their dashes (`exclude:`) now render as `--exclude` instead of being ignored.
- **Reservations are checked against the walltime (S7).** HSM used to list every
  reservation at setup. It now warns at submission only about a maintenance window
  that starts before a job of this walltime would end ("won't start before <end>; a
  walltime ≤ X would start now"), using the cluster's clock, for native Slurm too.
- **A crashed resumable chunk is FAILED, with its exit code; a FAILED chain archives (S10,
  #15, #16).** A chunk's script exited 0 whenever the run left no `.hsm_done`, so a crash
  (a CUDA OOM, an exception) was recorded `COMPLETED 0:0`, looked like the walltime seam,
  and was run again every chunk while other tasks progressed. The script now exits 0 only
  when the run exited 0 or the batch shell caught a SIGTERM (walltime, preemption). Any
  other non-zero exit is a crash: the script exits with that code, so Slurm records FAILED,
  and appends `exit=<code> job=<array>_<task> <date>` to `tasks/task_<i>/.hsm_failed`. A
  chunk that ends any other way removes the file; a run that dies of a TERM itself (143) is
  a seam. A task with `max_task_crashes` (a new knob, default 3) crashes in a row is out of
  retries: later chunks skip it (exit 1), and once
  every other task is done the chain ends FAILED at once, naming it. `.hsm_done` stays the
  only proof of done. A chain that ends FAILED is now archived to `archive_dir` before the
  pull, as a DONE one is; its remote dir is still kept. **For consumers:** with
  `archive_on: completed` (the default) a FAILED chain is now archived too, with
  `any_failed: True` in `.archived`; only `archive_on: never` skips it. A crashing task now
  costs `max_task_crashes` chunks, not one per chunk until the chain stops. To retry a chain
  that ended FAILED: delete the tasks' `.hsm_failed`, set `chain.state.failed` to `false` in
  `.hsm_manifest.json`, and run `hsm sweep advance <id>`.
- **`module load` works in a job submitted from a non-login shell (S10, #15).** Lmod and
  Environment Modules define `module` from `/etc/profile.d`, which only a login shell
  sources, so `modules:` and a `pre_script` `module load` failed with `module: command not
  found` in jobs sbatch'd over SSH (the S3IT `module load miniforge3` recipe was a no-op;
  conda was found by the fallback probe). When a script loads modules, every template now
  first sources the first init script that exists (`$LMOD_PKG/init/bash`,
  `/etc/profile.d/lmod.sh`, `/etc/profile.d/z00_lmod.sh`, `/usr/share/lmod/lmod/init/bash`,
  `/etc/profile.d/modules.sh`) if `module` is undefined, and warns if none defines it.
  Scripts that load no modules are unchanged.
- **A login-node blip no longer ends a Slurm-over-SSH launcher (S11, R9).** A dropped
  connection made the next `squeue` raise and killed a multi-day wait. The connection
  now has a 30 s keepalive and each command a 5 min bound (none for the archive rsync
  and `rm -rf`); a command whose connection failed reconnects and runs once more. While
  the host stays unreachable the polls fail and every job keeps its state; after 30 min
  the launcher gives up, and the jobs stay in Slurm (`hsm queue mine --remote <name>`
  lists them; `hsm sweep collect <id>`, or `advance` for a chain, re-attaches). A command
  cut off mid-run (no exit status) no longer counts as a success: a server-side archive
  cut short keeps the remote dir (no pull, no `rm -rf`) and `collect` says so (also for
  a finished chain, which `collect` now takes), `scancel` marks nothing CANCELLED, an
  unexpanded `$USER` is an error, a failed chain-progress probe is asked again (a few
  polls, then the launcher stops; a detached `advance` leaves it to its next run)
  instead of counting as a chunk without progress, and a failed `sinfo` is no longer
  cached. A job script is written first, then submitted by a `sbatch` with no time
  bound, and `sbatch` is never resent after a lost reply (the job may be queued): HSM
  looks for the one live job with its name and script, else names the `squeue`,
  `sacct` and `scancel` commands to check by hand; a submission whose channel never
  opened is sent again. A remote file write
  (params file, manifest, `.archived`) that fails now raises instead of passing
  silently. Slurm sources poll every 60 s instead of 10 s; local and ssh sources stay at
  10 s (a distributed sweep still polls its Slurm children every 10 s).
- **Results come back during a long array; a dropped link no longer fails an rsync (R7).**
  The mid-run pull fired when a *job* finished, and an array is one job, so a 1–3-day
  array brought nothing back until its last task was done. A Slurm-over-SSH launcher now
  pulls `tasks/` every 10 min while any job is live, in place of the pull per finished
  job (the final pull is `collect`'s), timed from the end of the last pull and without
  weight files (`*.ckpt`, `*.pt`, `*.pth`, which the final pull brings); a failed pull
  logs a warning and the wait goes on. Every rsync of both SSH sources, push and pull, is
  tried again after a dropped link (rc 255, 10, 12, 30 or 35), 5 s and then 20 s later;
  any other rc is final. **For consumers:** finished tasks' logs and results reach the
  local sweep dir within about 10 min, their weights at the end; with `--mode
  individual`, a job's tasks arrive up to 10 min after it ends instead of at the next
  poll.
- **GPU indices are nvidia-smi's, and HSM never joins a busy GPU unless told `all` (X4).**
  HSM numbers GPUs as `nvidia-smi` does (PCI bus order), but CUDA's default order is
  fastest-first, so on anahita an index inside a list (`--gpus 1,2`) ran on nvidia-smi #3
  and #1. The local and SSH task scripts now export `CUDA_DEVICE_ORDER=PCI_BUS_ID` with
  `CUDA_VISIBLE_DEVICES`. HSM now skips the GPUs `nvidia-smi` shows busy (5% utilisation
  or more, 500 MB used or more, or unreadable as on a MIG GPU) and logs them, for no
  allowlist, a list (`--gpus 1,2`, `local.visible_gpus`, a remote's `gpus:`) and a count
  (`--gpus N` = the first N free); it used to take them, a co-tenant's included. Only
  `--gpus all` takes every GPU, busy or not. A GPU job (`spec.gpus > 0`) that finds no
  full slot of free allowed GPUs on a box with GPUs now fails setup with an error naming
  the busy ones ("wait, or pass --gpus all"), instead of running on CPU; it still runs on
  CPU, with a warning, on a box without GPUs or with `--gpus cpu`. A task's GPU
  visibility follows the config, not the load: `--gpus cpu` renders
  `CUDA_VISIBLE_DEVICES=` (no GPU; the variable used to be unset), and a CPU task under
  any other allowlist keeps its environment. A local or SSH source takes at most one task
  per slot in `--mode distributed` (its `max_parallel_jobs` is capped by its slot count).
  A remote's `gpus:` written as a string (`"1,2"`, `ALL`, `cpu`) is parsed like `--gpus`
  (`"1,2"` used to mean CPU-only, as did `gpus: all`), and a hung local `nvidia-smi` times
  out after 30 s. A remote GPU probe with no answer (a dropped link, a hung driver) stops a
  GPU job at setup instead of running it on CPU. **Compatibility:** an allowlist written in
  CUDA's default order must be restated in nvidia-smi order (on anahita, `local.visible_gpus: [2, 3]`, the A6000s,
  becomes `[1, 2]`). **For consumers:** a remote's `gpus: 0` now renders an empty
  `CUDA_VISIBLE_DEVICES` (no GPU), and lists and counts skip busy GPUs (`--gpus all`
  takes them).

- **ssh tasks run detached (X1, X-2).** Each task held an ssh channel for its whole run, so
  about 10 concurrent tasks hit sshd's `MaxSessions` and a submit failure aborted the sweep;
  a dropped connection marked every running task FAILED; a stop killed them all; their
  output was buffered in the launcher and lost. Now each task starts detached (`setsid
  nohup`) in one short command and holds no channel. Its output goes to
  `tasks/<task>/hsm.log` and its exit code to `.hsm_rc`, which one command per poll reads
  for every running task (alive means its process group is). A dropped connection
  reconnects once (with a 30 s keepalive) and changes no status; a host unreachable for
  30 min ends the launcher, its tasks still running. A task that can't be started in
  three tries ends the sweep, naming it (a distributed sweep hands it to another remote).
  **Ctrl-C now leaves started tasks running**, as Slurm jobs do, and logs the command that
  stops them; a cancel TERMs the task's process group, which keeps its slot until it has
  exited. Commands run in `bash` whatever the login shell. The remote root must resolve
  to an absolute path without spaces (a relative one is under `~`), and the remote sweep
  dir is never removed while a task runs, by a collect or by `hsm remote clean`.
- **`hsm sweep collect` re-attaches ssh sweeps (X-2).** An ssh sweep keeps a
  `.hsm_manifest.json` (each task's host, pid and dir) as its tasks start. After a
  Ctrl-C or a dead launcher, `hsm sweep collect <id>` reads every task's exit code in
  one command, pulls `tasks/` back, and removes the remote sweep dir once every task
  COMPLETED; re-run it as more tasks finish. A task is listed before it starts, and one
  listed without a pid has it read back from the remote, so a launcher killed mid-launch
  never gets a running task's dir removed. While the launcher still runs (it holds a lock
  on the sweep dir), collect refuses: the launcher collects the sweep itself. (A
  distributed sweep's ssh children write no manifest: they share the sweep dir.)
- **A remote's own `python_path` is used (C5, FR#15b).** The project's `paths.conda_env`
  beat a `python_path` set on a remote, so a box whose env lives elsewhere ran the
  project's env. The interpreter now comes from the narrowest place that sets one: the
  remote's entry, the `distributed:` block, then `paths.conda_env`. A level that sets
  `conda_env` or `python_path` is taken whole (a key left empty is unset), by one helper
  shared by the ssh and ssh-slurm factories. **For consumers:** a remote with a
  `python_path` and no `conda_env`, or with neither under a `distributed:` block that sets a
  `python_path`, now runs that `python_path` instead of `conda run -n <paths.conda_env>`.
- **The conda probe sources the install that has the env (R11, FR#15a).** With no conda on
  PATH, the task script sourced the first `conda.sh` it found, so a leftover
  `~/miniconda3` shadowed the `~/miniforge3` that holds the env. It now sources the first
  install with `envs/<env>` (else the first found, as before, unless the env is in
  micromamba's root), and also tries
  `$CONDA_EXE`'s prefix and `~/mambaforge`. A conda already on PATH (`module load
  miniforge3`) still wins.
- **`hsm sweep cancel` cancels a live Slurm sweep (S9).** It took the job ids from
  `submission_summary.txt`, which is written only once the sweep's wait ends, so a running
  sweep had none; an SSH-Slurm sweep fell through to "unknown backend", and native Slurm
  wrote no manifest. Native Slurm now writes `.hsm_manifest.json` as it submits (also when
  submission stops partway, and a native chain keeps its chunks there), as SSH-Slurm does.
  `cancel` reads it and runs one `scancel` naming every job, over ssh for a `backend:
  slurm` remote and on this machine for native Slurm, and reports the ids; a `scancel` that
  fails or gets no answer exits 1. For a resumable chain it cancels the running chunk and
  any chunk queued after it, then marks the chain stopped: `hsm sweep advance` won't
  resubmit it, a launcher still driving it stops when the chunk ends, and `hsm sweep
  collect` pulls its results and keeps its remote dir. A sweep without a manifest is
  cancelled as before. A CANCELLED job now counts as not completed when a sweep is
  collected, as FAILED does: the remote dir is kept, and `archive_on: completed` doesn't
  archive it (before, a cancelled sweep was archived as a success and its remote dir
  deleted). **For consumers:** native Slurm sweep dirs now hold a
  `.hsm_manifest.json`, so `hsm queue mine` links a live native sweep's jobs to it;
  `hsm sweep advance` refuses a native chain (it re-attaches over SSH only).
- **`hsm sweep cancel` cancels an ssh sweep (S9 follow-up).** It printed "cannot reliably
  remote-cancel" and suggested Ctrl-C, which since detached tasks (X-2) stops only the
  launcher. From the sweep's `.hsm_manifest.json` it now sends TERM to each running task's
  process group (a task may checkpoint on TERM), and exits 1 if any send fails; while the
  launcher runs it refuses, since the launcher would start the tasks still queued, and when
  its poll fails it sends nothing (every listed task would read as running). Any cancel
  of an ssh task now checks the pid is still the task's: a live process younger than the
  task's `.hsm_pid` (a pid reused after a hard kill or a reboot) gets no signal. A
  cancelled task counts as not COMPLETED: `collect` keeps the remote dir and says so.
- **How Slurm ended each task is recorded; a walltime kill no longer reads as RUNNING
  (S8, FR#11, #13).** TIMEOUT, OUT_OF_MEMORY, NODE_FAIL and PREEMPTED all became FAILED, and
  a task killed at its walltime wrote no `Status:` line, so `hsm sweep status`/`report`
  showed it RUNNING forever and the run's failing-task list skipped it. At collect, both
  Slurm sources now ask `sacct` once for every job and write `tasks_state.json` (state, exit
  code, node, elapsed seconds per task dir) into the local sweep dir, never the remote one.
  The analyzer reads it for a task without a `Status:` line (the `.hsm_done` sentinel and a
  written `Status:` line still win). The run summary, and `hsm sweep collect`, count the
  tasks per state, TIMEOUT and OUT_OF_MEMORY apart, and list those that failed within 60 s
  as "infra suspect" with an `--exclude=<nodes>` hint. A failed, absent or empty `sacct`
  writes nothing, and the old behaviour stays. **For consumers:** a walltime-killed task
  now counts as failed (TIMEOUT) in `status`/`report`, and the run names it.
- **The pre-walltime SIGTERM reaches the training process (S12, found while fixing S10).**
  The resumable array script forwarded the TERM to `$PY_PID`, the subshell `eval ... &`
  forks, and `conda run` doesn't forward TERM either: python never got it, so a chain's
  chunk ended without its final save and resumed from the last periodic checkpoint. The
  run now gets its own process group (`set -m`), the TERM goes to the whole group, and the
  batch shell waits for the group to exit after a TERM (Slurm's walltime bounds that wait).
  **For consumers:** a training script's SIGTERM handler now runs at the chunk seam; one
  that does slow work there must finish within `signal_grace`.
- **Unknown config keys warn, with the key they probably meant (R4, FR#17).** A
  remote-level `pre_script` (it belongs in the remote's `spec:`) or a sweep's `gird:` was
  silently ignored. `hsm sweep run` (and `--dry-run`) now prints a yellow warning for every
  key HSM doesn't read, in `.hsm/config.yaml` and in the sweep file, with a suggestion: the
  same key one level up or down (`did you mean distributed.remotes.uzh.spec.pre_script?`)
  or a near spelling (`did you mean sweep.grid?`). The known keys of each block are one
  table (`config.KNOWN_KEYS`, derived from `ResourceSpec` and `ResumableConfig`), which
  replaces the ad-hoc key sets of the `local:`/`slurm:` accessors and the resumable block.
  **For consumers:** warnings only; nothing stops a run. The old dispatcher's
  `distributed.strategy` / `sync_method` / … now warn on every run, not only in
  `--mode distributed`: they have changed nothing since X3, so delete them.
- **A stale path stops `hsm sweep run` before it creates anything (R6, FR#19).** After a repo
  move, a `project.root` or `paths.train_script` (or the sweep's `script:`) that no longer
  exists failed only once the run started, after a sweep dir had been created.
  `HSMConfig.check_paths()` now runs first, ahead of the dry-run output, and the run exits
  2 with an error naming each missing path and its config key.
- **Commands no longer report a failure as success (R8).** A pre-flight error of `hsm sweep
  run` (an invalid sweep or resumable config, a bad `spec:` value, an unknown `--remote`
  alias, `--remote` with `--mode local`, a `gpu_type` list outside array mode, `--max-runs`
  with `--resumable`, `config.complete:`) printed in red and exited 0, and so did
  `status`/`report`/`watch`/`cancel` on a sweep id that doesn't exist. They now exit 1 with
  `Error: …` on stderr. A sweep that ends with CANCELLED jobs exits 1, like one with FAILED
  jobs. A missing `local.sweeps_root` was reported as "Sweep config file not found"; its own
  message is shown now. `hsm sweep report` crashed (`unhashable type: 'dict'`) the first time
  it read a sweep without task assignments in `source_mapping.yaml`, that is every
  non-distributed sweep, and every time with `--scan-tasks`; it reads them now.
  `hsm setup init` on an initialized project rewrote `.hsm/config.yaml` and dropped every
  hand-added block, `distributed:` among them; it now leaves the project as it is, and
  `--regenerate` rewrites the config (the old copy goes to `.hsm/config.yaml.bak`, as
  before), `sweeps/README.md` and `sweeps/example_sweep.yaml`.
  **For consumers:** exit codes only change in one direction: a failure that exited 0 now
  exits non-zero (1). A script that re-runs `hsm setup init` to refresh the generated files
  must add `--regenerate`.
- **`hsm sweep status`/`report` read individual-mode task dirs (R5, FR#18).** The sources name
  task dirs three ways: `task_7` (array), `task_007` (local, ssh), `<sweep_id>_task_007`
  (individual Slurm jobs). The analyzer kept only names starting with `task_`, so every task
  of an individual-mode sweep counted as missing and its combinations as never run. One
  reader, `task_index(name)`, now parses all three schemes wherever HSM reads a task dir
  name: the analyzer and both Slurm sources' chunk-progress probes. No dir is renamed.
  **For consumers:** individual-mode sweeps now report their real completed, failed and
  missing counts; task dir names are unchanged.
- **A resumable chain is never submitted twice (R10).** Nothing stopped a cron
  `hsm sweep advance` from driving a chain its live launcher was driving too, and a
  driver that died between a chunk's `sbatch` and its manifest left that chunk out of
  it: either way the next chunk could be queued twice. The manifest itself was written
  in place, so a crash mid-write could leave half a file, and `advance` dropped the
  sweep's cost hints, so a multi-type `gpu_type` chain could split its tasks differently
  after a re-attach. Now one process drives a chain at a time (it holds
  `.hsm_launcher.lock` in the sweep dir for its whole run); before queueing a chunk, a
  driver looks for this chain's job already in the queue (`squeue -n <job name>`, the
  same script) and adopts it rather than submitting again (SSH-Slurm and native Slurm);
  when squeue can't say (three tries) or names several, it submits nothing and stops, and
  the next `advance` submits that chunk without judging the previous one a second time
  (which cost a strike). The manifest is written to a temp file and renamed, and the chain records its `costs`
  for `advance`. **For consumers:** `hsm sweep advance` is now safe beside a live
  launcher, so a cron of it needs no care: it prints "Another process drives chain …"
  and exits 0.

### Removed (2026-10 maintenance pass)

- **Unused heavy dependencies.** HSM no longer installs `wandb`, `pandas`, `numpy`,
  `hydra-core` or `omegaconf`; none is imported by HSM (an install drops from about
  215 MB to 56 MB). Training environments that relied on HSM to pull them in must
  list them themselves. `requirements.txt` (a stale copy of `pyproject.toml`) and the
  empty `docs` extra are gone.
- **Dead code (no caller anywhere):** ten helpers in `cli/common.py`,
  `HydraConfigParser` and `SweepConfig.from_hydra_config`, six `utils` helpers
  (`ProgressTracker`, `format_duration`, …), `PathDetector.suggest_setup`, two
  `ParameterGenerator` helpers, `get_sweep_completion_summary`, and the unused
  `templates/sweep.yaml.j2`.
- **`hsm sweep errors` (R8).** It read `errors/*_error.txt`, which nothing writes, so it
  always said "No error directory found". `hsm sweep status <id> --errors` replaces it: for
  each failed task (the first 10) it prints the `Status:`/`Exit Code:` lines of
  `task_info.txt`, how Slurm ended it (`tasks_state.json`), and the last lines of its
  newest log: an ssh task's `tasks/<task>/hsm.log`, a local or Slurm task's `logs/*.err`.
  The end-of-run hint points at it. **For consumers:** `hsm sweep errors` is gone; use
  `hsm sweep status <id> --errors`.

Two field reports drove this cycle: SSH-Slurm → S3IT first use
([`2026-06-02-s3it-first-use.md`](docs/dev/field-reports/2026-06-02-s3it-first-use.md))
and the first blind agent-driven consumer run from Comp-PVR
([`2026-06-03-comp-pvr-first-run.md`](docs/dev/field-reports/2026-06-03-comp-pvr-first-run.md)).

### Fixed (queue inspection audit, 2026-06-04)

Live audit against S3IT (Slurm 25.05) with two sweeps in flight found
`hsm queue` blind on real clusters:

- **GPU parsing missed the colon-count GRES grammar.** `squeue %b` emits
  `gres/gpu:A100:1` / `gres/gpu:3` on Slurm 25.05; the parser only knew the
  `=`-count accounting style (`gres/gpu:h100=1`), so every job parsed as
  0 GPUs — `gpus` reported an empty GPU queue, `position` denied pending GPU
  jobs existed, `mine`'s GPU column was blank. Both grammars are accepted
  now, with the live cluster census as test fixtures. Also corrected the
  field's semantics: `%b` is TRES per **node** (`QueueJob.tres_per_node`).
- **Pending arrays counted as 1 task.** A pending array is one squeue row
  (`123_[690-1920%4]` = 1231 tasks). Counting paths (`position`, `gpus`)
  now query with `squeue -r` so totals/positions count tasks; `mine` keeps
  the compact row and shows a `×N` Tasks column.
- **`position` silently hid CPU-only pending jobs** — now surfaced as a
  note; the reason-code legend covers `(QOSMaxJobsPerUserLimit)`.

### Fixed (GPU-planner hardening — post-merge cold review of #10, 2026-06-04)

Three clean-slate review agents on the merged #10; all real findings
addressed. Theme: every default now biases toward OVER-provisioning
(harmless queue time) instead of UNDER-provisioning (mass TIMEOUT):

- **Partial `speed_factors` map → hard error** (a listed type missing from
  a provided map used to default to 1.0 — over-assigned work + unscaled
  walltime on a genuinely slower type). No map at all stays allowed
  ("equally fast"), with a warning that now names the TIMEOUT risk.
- **Unknown task costs default to the MAX known cost** (was 1.0 — a task
  missing from `cost_map` landing alone in a bin collapsed that bin's
  walltime to base/global_max). Warning names tasks + consequence.
- **Partial sub-array submission failure now writes the manifest** for
  whatever DID submit (was: orphaned live arrays invisible to
  `hsm sweep collect`) and names the live job ids + `scancel` hint; the
  native-local source logs the same.
- Multi-type base walltime must be `HH:MM:SS` (`"48:00"` = 48 *minutes*
  via the two-part parse — a silent 60× under-provision); gpu_type names
  colliding after sanitization error instead of clobbering each other's
  params files; all-zero-cost pure-API ZeroDivision guarded; bool param
  values no longer match int `cost_map` keys (`True == 1`).
- `cost_param` naming a non-swept param is now a `validate()` **error**
  (was a buried runtime warning that silently degraded to uniform costs).
- Speed-factor validation consolidated into one shared
  `normalize_speed_factors` (was 4 near-copies; NaN/inf now rejected at
  the config layer too); the dry-run Factor column reads the planner's
  actual `SubArraySubmission.speed_factor` instead of re-deriving from
  config; misplaced `speed_factors` inside `spec:` warns with the fix
  instead of nuking the spec block with an opaque TypeError; the
  distributed dry-run preview no longer crashes (silently) on list
  gpu_type. Local `SlurmComputeSource` multi-type path now has its own
  test coverage (was SSH-twin only).

### Added (heterogeneous GPU-type scheduling, issue #7 v0+v1, 2026-06-04)

- **`spec.gpu_type` accepts a list** (array mode): the sweep splits into one
  Slurm array per type via greedy LPT — costliest tasks first, each to the
  type minimizing `(load + cost) × factor`. With uniform costs this
  degenerates to counts ∝ 1/factor. Pure planner shared by the local and
  SSH-Slurm sources (`core/hpc/gpu_planner.py`); a multi-type spec reaching
  `render_sbatch_directives` raises (unplanned-path guard). Singleton lists
  degenerate to the plain scalar path.
- **`speed_factors`** (per-remote key, or in the `slurm:` block): GPU type →
  relative runtime multiplier; per-sub-array walltime = base × factor ×
  (bin max cost / global max cost), ceiled to the minute, floored at 10 min,
  uncapped. Workload-specific — measure, don't trust spec sheets; houses in
  the key a future `hsm calibrate` will write.
- **Per-task cost hints in the sweep YAML**: `cost_param` names a swept
  param; optional `cost_map` translates values to measured costs (ratios
  matter). Unusable costs default to 1.0 with a loud warning. Costs never
  enter the hydra override string.
- **`--dry-run` shows the exact split plan** (same planner call as
  submission): type / factor / tasks / Σcost / max cost / scaled walltime.
- Task layout unchanged (`tasks/task_%04d` stays globally numbered via
  per-sub-array params files with original `global_index`); `task_info.txt`
  records `GPU Type:` per task (arch-confound flag); manifest gains per-job
  `jobs:` entries so `hsm queue mine` shows per-type sub-arrays with correct
  per-array progress.

### Changed (`hsm queue gpus` capacity view, 2026-06-04)

- **`hsm queue gpus` shows Total / In use / Free per GPU type**, from
  `sinfo`'s per-node allocation accounting (`Gres`/`GresUsed`) — which also
  attributes GPUs consumed by *untyped* job requests to their physical
  type, making Free exact rather than an estimate. Down/drained nodes are
  excluded from totals (disclosed in a footer); idle types with no queue
  demand are now visible. sinfo is optional enrichment with the same
  None-on-failure contract as sacct — clusters without it degrade to the
  queue-only table with a note. Parser handles the live S3IT trap of
  commas inside `GresUsed` index decorations (`gpu:A100:6(IDX:0-1,4-7)`).
- **`--mine` is now the default** on `gpus` (`--no-mine` to hide) — there
  was no good reason to hide your own footprint.
- **VRAM/GPU column**: cluster-reported via `GPUMEM<N>GB` node-feature
  tags when available (mixed node groups list every variant — live S3IT
  H100s really are `80/96G`); model-typical fallback marked with `~` and
  restricted to unambiguous models; `?` otherwise — never a confident
  guess. An automatic legend explains the `<untyped>` demand row.

### Changed (grouped `hsm queue mine`, 2026-06-04)

- **`hsm queue mine` groups by array by default.** Mid-sweep, every running
  array task is its own squeue row — the old view printed hundreds of
  near-identical lines. Now: one row per array with `▶ running ⏳ pending`
  (squeue, live, task-weighted) plus `✓ completed ✗ failed` and a
  progress-bar-out-of-true-total from **sacct accounting** — tasks that
  already left the queue (including failures) were previously invisible in
  every queue view. sacct is optional enrichment: clusters without
  accounting degrade to sweep-metadata totals or `—`, never a guessed
  number, and a missing sacct never errors (asymmetric with squeue, which
  stays loud-on-failure). `--flat` restores the per-task rows. Failed-task
  counts are surfaced in red per-row and in the footer.

### Added (queue inspection from the driving workstation, 2026-06-04)

- **`--remote <alias>` on all four `hsm queue` subcommands** — runs the same
  queries over SSH (registered `backend: slurm` remote or bare `~/.ssh/config`
  alias). New `SSHSlurmQueue` async twin shares the pure command
  builders/parsers with the local `SlurmQueue` (the `slurm_protocol.py`
  anti-drift idiom); failures raise instead of rendering an empty table.
- **Auto-fallback:** with no local `squeue` and exactly one slurm-backend
  remote registered, `hsm queue` uses it automatically (note printed).
- **`--watch [--refresh N]` on `mine` and `gpus`** — live-refreshing view;
  one SSH connection reused across cycles.
- **Sweep linkage for SSH-Slurm sweeps:** `mine` resolves job→sweep from
  `.hsm_manifest.json` too (such sweeps have no `submission_summary.txt`),
  so the from-HQ view shows which sweep each job belongs to.

### Fixed (Comp-PVR consumer report, 2026-06-03)
- **Array tasks silently ran the default config under `conda run`.** The
  per-task params extraction fed Python over stdin (`python - <<heredoc`);
  `conda run` doesn't forward heredoc stdin inside `$()` on some clusters, so
  tasks got zero overrides and reported SUCCESS. The snippet now runs from a
  tempfile by path, and an empty extraction fails fast instead of training
  the default config.
- **`configs/wandb/` was silently stripped from the push.** Output-dir rsync
  excludes are now anchored to the project root (`/wandb`, `/checkpoints`,
  `/multirun`) so they can't shadow a same-named Hydra config group
  (`MissingConfigException` on every task, with no per-remote workaround).
- **`hsm setup init` reported failure after succeeding** (crash after all
  files were written). Also hardened: exits non-zero on *real* failure
  (previously exited 0), re-runs back up `.hsm/config.yaml` to
  `.hsm/config.yaml.bak` before regenerating, and non-interactive runs never
  prompt (migration path included).

### Added (Comp-PVR consumer report, 2026-06-03)
- `hsm init` — top-level alias for `hsm setup init`.
- The init re-run/overwrite contract is documented in `hsm setup init --help`,
  README, and getting_started.md.
- SSH_EXECUTION.md: "Your project's own package on the remote" — editable
  installs don't import from the rsynced tree; `pip install -e` into the
  remote env or a `PYTHONPATH` `pre_script`.
- `hsm docs` prints URLs unwrapped (copy-pasteable at any width) and notes
  that `main` is the canonical branch.

### Added
- `hsm docs` — prints the documentation URLs (+ local path on a source checkout),
  so consumers/agents can find the docs without spelunking the installed package.
- `hsm setup init` now writes a **self-contained** `sweeps/README.md` (execution
  modes, output layout, failure/recovery semantics, config knobs) instead of
  pointing at `docs/user_guide/*` paths that aren't shipped with `pip install`.
  It can also create an `AGENTS.md` pointer for coding agents — only on explicit
  interactive opt-in, and it never modifies an existing `AGENTS.md`/`CLAUDE.md`.
- `hsm sweep collect <sweep_id>` — re-attach to an SSH-Slurm sweep whose
  launching process died (long run / overnight / maintenance window) and
  pull + archive whatever's terminal, via a submit-time `.hsm_manifest.json`.
  Idempotent; classifies jobs with `squeue` then `sacct`.
- Continuous mid-flight result pull: completed task dirs are rsync'd back as
  soon as each task finishes, so one stuck task can't strand the rest.
- Self-describing checkpoints: every `tasks/<t>/` now contains a `params.yaml`
  with that task's exact overrides.
- Array submission over SSH-Slurm: `--remote <alias> --mode array` packs one
  `sbatch --array` instead of one job per combo.
- Maintenance-reservation warning at submit (`scontrol show reservations`).

### Fixed
- **FAILED Slurm jobs were reported COMPLETED.** Terminal state now comes from
  `sacct` once a job leaves `squeue` (queue-absence is not success); `hsm sweep
  run` exits non-zero on any failure and lists failing task dirs.
- **Module-provided conda was silently shadowed → jobs trained on CPU.** Rendered
  scripts now run `module load` / `pre_script` before the conda-init probe, which
  defers to a real conda on PATH instead of a micromamba bridge.
- `$USER` / `$HOME` / `~` are now expanded in remote `workdir` / `archive_dir` /
  `remote_root` (rsync got a literal `$USER` dir before).
- `--dry-run` for `--remote` now shows the merged per-remote `spec:` (gres /
  account / qos), not an empty preview.
- `hsm setup init` no longer mis-detects PBS on a box with no local scheduler
  (defaults to `unknown`; checks Slurm → SGE → PBS in order).

### Changed
- `DEFAULT_RSYNC_EXCLUDES` adds `*.pth` / `/checkpoints` / `/multirun` /
  `.hydra` (dir names root-anchored — see Fixed above); per-remote
  `rsync_excludes` now **extends** the defaults instead of replacing them
  (so adding `outputs/` can't accidentally start pushing `.git`).

## [0.1.0]
- Initial release: `local` / `array` / `individual` / `remote` / `distributed`
  execution over a unified `ComputeSource` API; SSH push-model + SSH-Slurm
  backends; `local.sweeps_root` redirection; `workdir` / `archive_dir` storage
  tiers.
