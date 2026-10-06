# Field notes 2026-09-29: Comp-PVR replay sweeps on uzh (consumer: Claude, modularity session)

HSM `main` at 5f87299, driven from anahita (`cpvr` env, asyncssh 2.23.0). No HSM source was changed;
the workaround below is on the consumer side.

## 1. Silent hang in asyncssh authentication when SSH_AUTH_SOCK points to a stale agent (HIGH)

**Symptom.** `hsm sweep run --remote uzh --mode array` failed in `setup()` with
`SSH connection to uzh failed: ConnectionResetError: [Errno 104] Connection reset by peer`.
Plain `ssh uzh` worked from the same shell at the same moment.

**Root cause.** The shell's `SSH_AUTH_SOCK` pointed to an agent socket forwarded by an earlier
session, which no longer answered. asyncssh queries the agent before it tries the key files, so it
hangs in auth ("Beginning auth for user gbena" and then nothing) until the server drops the
connection. With `agent_path=None` and explicit `client_keys`, asyncssh connects in under a second.
Later, OpenSSH itself started hanging on the same agent (`ssh -o IdentityAgent=none` fixed it).

**Repro.**
```python
asyncssh.connect('uzh', config=[os.path.expanduser('~/.ssh/config')])               # hangs in auth
asyncssh.connect('uzh', config=[...], agent_path=None, client_keys=['~/.ssh/id_ed25519'])  # ok
```

**Consumer workaround used tonight.** `env -u SSH_AUTH_SOCK hsm sweep run …` for every submission
and launcher; `ssh -o IdentityAgent=none` for manual probes.

**Proposed fix (for the reviewer).**
- Wrap `asyncssh.connect` in `discovery.py` in an explicit timeout (for example
  `asyncio.wait_for(..., 30)`) and report "auth timed out (agent?)", not a connection reset.
- On an auth timeout, retry once with `agent_path=None` (key files only), or honour
  `IdentityAgent none` from ssh_config.
- `hsm remote test <alias>` could print which auth path succeeded.

Also, `~/.ssh/config` passed as a literal string to asyncssh's `config=` is not tilde-expanded
(`FileNotFoundError`). HSM itself expands it, but any doc that suggests the literal form will mislead.

## 2. Stale project paths after a repo move (LOW, docs)

After the 2026-09-03 layout migration, Comp-PVR's `.hsm/config.yaml` still pointed `project.root`,
`paths.config_dir` and `paths.train_script` at `~/code/modularity/Comp-PVR`, which no longer exists.
HSM did not complain until submission time. Suggestion: `hsm sweep run` should validate that
`project.root` and `train_script` exist, and fail fast with the path in the message.

## 3. What worked well

- The array path (`--mode array`) submitted 2, 20, 300 and 128 tasks in one `sbatch` each. The
  launcher polled, archived to `/shares`, pulled with one rsync and cleaned scratch on completion
  (verified on the 2-task pipeline test, array 6696344).
- `parameter_combinations.json` in the remote sweep dir made a live dashboard easy to build.

## 2026-10-01 addendum: a Hydra run-dir race between array tasks (1 of 200 tasks lost)

- **What happened.** In array 6734161 (200 tasks), task 125 failed 12 s after starting with
  `PermissionError: [Errno 13] Permission denied: 'outputs/2026-09-30/16-24-00'`. That is raised from Hydra's `run_job` →
  `Path(output_dir).mkdir(parents=True, exist_ok=True)`.
- **Why.** Every task's working directory is the shared `<workdir>/<project>/code`. Hydra's default run dir is
  `outputs/${now:%Y-%m-%d}/${now:%H-%M-%S}`, so tasks that start in the same second resolve to the same path. One of
  them hit a transient permission/stat failure while another was creating it. Results are unaffected (the training
  script writes to `output.dir`), but the task dies before training.
- **Suggestion (not a hot-patch).** HSM could pass a per-task `hydra.run.dir` (e.g. under the task's own output dir)
  for Hydra scripts, or run each task from its own working dir. A cheap mitigation: stagger array starts by a few
  hundred ms per task index.
- **Recovered by** re-running the combination (array 6775212).

## 2026-10-01 addendum 2: being a good citizen on a shared account (two missing knobs)

- **An array throttle.** HSM renders `--array=1-K` with no `%N` and has no config/CLI option for one, so the only way to cap
  concurrency is `scontrol update JobId=<id> ArrayTaskThrottle=N` after submission. On a shared group account a
  1320-task replay pushed the account to ~17× its fair share and starved co-workers' jobs. Suggestion: a `spec`/CLI
  `array_throttle: N` rendered as `--array=1-K%N`.
- **CPU jobs land on GPU nodes.** With `gpus` unset, 24 of ~300 CPU tasks were scheduled on GPU nodes (S3IT `standard`
  mixes CPU-only and GPU nodes). `extra_directives: {"--exclude": "<hostlist>"}` works (verified rendering). Suggestion:
  document it, or offer `cpu_only_nodes: true` that resolves the GPU hostlist from `sinfo` at submit time.

## Addendum 2026-10-03: a node-level `conda run` failure (W4, array 6824201, task 12)

- **What happened:** one task of 80 failed in 4 s on `u24-cva0000-128`. `conda run` raised
  `PermissionError: [Errno 13] Permission denied: '$XDG_CONFIG_HOME/conda/.condarc'`: the variable reached conda
  unexpanded, so the fault is in that node's environment.
- **What HSM did, correctly:** it reported `1 FAILED`, kept the remote sweep dir for inspection, and pointed to
  `hsm sweep errors`. The task has no local `task_12/` directory.
- **Possible improvement:** a failure within seconds, in conda's own startup, could be marked "infrastructure"
  (re-runnable), apart from training failures. HSM could also offer to re-submit it with `--exclude=<node>`.

## Addendum 2026-10-05: the ssh backend caps at about 9 concurrent tasks per host (sshd MaxSessions)

- **What happened:** `hsm sweep run -c … --remote athena --gpus cpu` with `max_parallel_jobs: 12` submitted 10 tasks.
  The 11th failed with `asyncssh.misc.ChannelOpenError: open failed`. The orchestrator then aborted the whole sweep
  ("Sweep execution failed") and left the 10 started tasks running with no launcher, so nothing would submit the rest
  or collect them.
- **Cause:** Athena's sshd uses the default `MaxSessions 10` per connection, and the ssh backend seems to hold one
  channel per running task on a single asyncssh connection.
- **Workaround:** `max_parallel_jobs: 8` per remote, plus a second remote entry with the same `host:` (its own
  connection) for more slots.
- **Possible fixes:**
  1. cap concurrency at the server's `MaxSessions`, probed or configurable, and warn at submit time;
  2. or don't hold a channel per task: launch detached (`setsid nohup`), then poll with one channel;
  3. or open more connections;
  4. at least, don't orphan started tasks when a later submit fails.
- **Also:** the wrapper sources `conda.sh` only from `~/miniconda3`, `~/anaconda3`, `~/.miniconda3` and `/opt/conda`.
  Athena's conda is `~/miniforge3`, so the remote needs `pre_script: ["source ~/miniforge3/etc/profile.d/conda.sh"]`
  (or `python_path`). Adding `~/miniforge3` to the search list would help.
