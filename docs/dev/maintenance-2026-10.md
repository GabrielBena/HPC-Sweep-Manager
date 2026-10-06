# HSM maintenance pass, 2026-10

The tracker for the pass. Its inputs are the Comp-PVR campaign field report
([`field-reports/2026-10-06-comp-pvr-campaign.md`](field-reports/2026-10-06-comp-pvr-campaign.md);
`FR#n` is its item *n*) and a four-angle code audit of `main` at `5f87299`, covering the ssh/local/distributed
backends, the Slurm stack, config and the CLI, and tests, gates and docs. Each row is one item; a PR cites the
IDs it closes and flips their **PR** cell here. The pass closes with a release (`v0.2.0`), after which this file
is deleted; the CHANGELOG keeps the record.

## Rules of the pass

- **Live sweeps run from the anahita checkout** until the consumer's launchers exit (about 10-08/09). Until then:
  work in a separate worktree only, don't reinstall HSM in any env, run tests against mocks, and run nothing on
  uzh or athena without Gabriel's go. The account is shared: run `uzh-share` before any smoke test, and keep jobs
  tiny with a short walltime.
- **Occam's razor.** Make the smallest change that removes the whole class of bug. Consolidate the code a fix
  touches, one module at a time and never as a rewrite. Every PR reports its net LOC.
- **Backward compatible.** Comp-PVR's config keeps working. A default changes only when the new one is strictly
  safer, and it prints a message where it differs.
- **One open PR per lane** (`slurm`, `ssh`, `cli`; chores are exempt), each with its tests and a CHANGELOG entry.
  Execution-path PRs get a cold review before merge. Who merges is set under *Decisions* below.

## P0: correctness and safety

| ID | Finding | Fix → proving test | Compat | PR |
|---|---|---|---|---|
| S1 | *[NEW]* When `squeue` fails (rc≠0) and `sacct` also fails, the job is taken as **COMPLETED**. `collect` then archives and `rm -rf`s a live sweep dir. Both Slurm sources: `ssh_slurm_compute_source.py:817-852`, `slurm_compute_source.py:388-440`. A slurmctld/slurmdbd outage with the login node up is enough. | On rc≠0, keep the last known state (except squeue's "Invalid job id"). Assume COMPLETED only when sacct returns rc 0 with no rows, on two polls in a row. → A fake outage keeps the job active and issues no `rm`. | none | #21 ✓ |
| S2 | *[FR#4/#5 corrected]* Polling is per job. `wait_for_all` calls `get_job_status` for each job: 600 channels per cycle and 70–190 s cycles at N=600. The batched `update_all_job_statuses` is used only by distributed. `collect` loops the same way, and native Slurm runs one subprocess per job. | One shared `_refresh_statuses()`: 1 `squeue` + 1 `sacct` per cycle, used by both sources and by `collect`. → A counting FakeConn sees ≤2 channels per cycle at N=600. | none | #21 ✓ |
| S3 | *[FR#4]* `--remote <slurm>` defaults to individual submission: 2 channels and 1 slurmctld RPC per task. The manifest is written only after the whole loop, so a mid-loop failure (or Ctrl-C, since only `except Exception` is caught) leaves live jobs untracked. | Array becomes the default for slurm remotes. `--mode individual` warns above 50 tasks. Any partial submission writes the manifest (`BaseException`). One channel per script: `cat > p && sbatch --parsable p`, with the job id checked against `^\d+$`. → Tests for the default, the partial-failure manifest, and rejecting unparsable sbatch output. | the default changes; it prints a message; Comp-PVR already passes `--mode array` | #26 ✓ |
| S4 | *[FR#2, worse]* Every launch re-pushes the shared remote `code/` with `--delete`. Queued tasks of earlier sweeps, and chunk k+1 of a chain, run the newest code. Files that tasks write into their cwd are deleted. | A per-sweep snapshot, `<root>/<project>/snapshots/<sweep_id>/`, made with `rsync --link-dest=<newest>`. The manifest records it and a clean removes it. → Tests for the push command, the `cd` target, garbage collection, and `advance` reading the manifest path. | old manifests keep `code/` | #27 |
| X1 | *[FR#1, #21, + modularity's note]* The ssh backend holds 1 channel per running task, plus 1 per `cat >`, so 10 concurrent tasks hit sshd `MaxSessions`. Then:<br>- a submit failure aborts the sweep and orphans the started tasks (`run_sweep_async` has no `finally`);<br>- a dropped connection marks every running task FAILED (`exit_status=None`), and there is no keepalive;<br>- a graceful stop's `cleanup()` kills every running task;<br>- task stdout is buffered in the launcher's memory and never saved;<br>- there is no manifest, so nothing can re-attach. | **Detached launch:** `setsid nohup` with pid and rc files and a TERM trap in the wrapper.<br>**Supervision:** one poll command per cycle for all tasks, a `step()` loop (poll → free slots → launch pending → save the manifest), and a launch that fails 3 times becomes FAILED.<br>**Connection:** a reconnect changes no status.<br>**Cancel and cleanup:** cancel by process group; `cleanup()` only closes the connection.<br>**Re-attach:** `hsm sweep collect <id>` re-attaches ssh sweeps.<br>→ A FakeConn that refuses the 11th channel gives 10 COMPLETED plus 1 reported launch failure. Also: `parse_poll` tests, the traps run under bash, a reconnect keeps statuses, and collect never `rm`s while tasks are active. | Ctrl-C leaves tasks running, as Slurm does, and prints the attach hint | |
| X2 | *[FR#3, corrected]* asyncssh asks a stale `SSH_AUTH_SOCK` first and hangs until sshd's LoginGraceTime, so the server's reset arrives before asyncssh's 120 s `login_timeout`. The rsync calls have no `BatchMode` or `ConnectTimeout`, so the same agent can hang a push forever. Workaround that needs no code: `IdentityAgent none` in `~/.ssh/config`, which asyncssh honours. | `login_timeout=30`. On a timeout or reset with an agent in use, retry once with `agent_path=None` and say so. `keepalive_interval=30`. rsync gets `-e "ssh -o BatchMode=yes -o ConnectTimeout=30 -o ServerAliveInterval=30"`. → A connect that times out with the agent makes the second call with `agent_path=None`. | none | #24 ✓ |
| X3 | *[NEW]* `--mode distributed` loses data:<br>- its continuous collector calls `SSHComputeSource.collect_results`, which `rm -rf`s the remote sweep dir while tasks still run;<br>- the last tasks to finish are never pulled;<br>- `backend: slurm` children are never collected at all;<br>- a failed submit hangs the launcher forever, and Ctrl-C is swallowed;<br>- the local child ignores `visible_gpus`, so on anahita it can land on GPU 0.<br>All of this lives in `distributed_manager.py`, which is live code. | Replace `distributed_manager.py` (1,632 lines) with about 80 lines in `DistributedComputeSource`: one worker per child over a shared queue, failed submits recorded as FAILED, then gather each child's `wait_for_all` and `collect_results`. → An e2e test with fake children: the last task is pulled, no `rm` while tasks are active, and a submit failure ends FAILED instead of hanging. | `strategy` and `collect_interval` become warned no-ops | |
| C1 | *[NEW]* `--remote X --mode auto` silently runs locally (`cli/sweep.py:1044-1062`). | With `--remote`, `auto` means remote, and `build_compute_source` rejects an alias combined with a non-remote mode. → Dry-run backend test. | none | #23 ✓ |
| C2 | *[NEW]* One unknown key in a `spec:` or `slurm:` block drops the **whole block** (account, qos, `--exclude`, …), with only a log warning: `ResourceSpec.from_dict` raises a TypeError that the factories swallow. | Drop and name only the bad key. A Slurm-bound block with errors exits 2. → `from_dict({"account": "a", "cpus": 4}).account == "a"`. | none | #23 ✓ |
| C3 | *[NEW]* `hsm remote add/remove` writes the *merged* machine and project config into the project file. It strips comments, leaks machine keys, and `add` replaces an existing entry wholesale, erasing `backend: slurm`, `workdir` and `spec`. | Read and write the project file only, and merge into the entry. Refuse to rewrite a file that has comments; print the snippet instead. → Round-trip test with a temporary HOME. | none | |

## P1: shared-cluster citizenship and silent value changes

| ID | Finding | Fix → proving test | Compat | PR |
|---|---|---|---|---|
| S5 | *[FR#6]* There is no array throttle. `max_parallel_jobs` does nothing for `--remote` or native arrays, yet the placement block prints "client cap N". | `spec.array_throttle` renders `--array=1-K%N`. It round-trips through the manifest's spec, so chain chunks stay throttled. A slurm remote's `max_parallel_jobs` maps to it when it is unset. → The `--array=1-600%50` render. | opt-in; a cap that is already configured becomes real | |
| S6 | *[FR#7, #8]* `extra_directives` keys are rendered verbatim, so `exclude:` becomes `#SBATCH exclude=…`, which is invalid. | Normalise keys to `--key`. Document `--exclude` and `--nice`, with the note that Nice=10000 drops the job behind every account. `cpu_only_nodes` is deferred until someone asks. → Render test. | none | |
| S7 | *[FR#9]* The reservation warning ignores owner, time window and walltime, and native Slurm never checks. | A pure `blocking_reservations(res, now, walltime)` in `slurm_protocol`, used by both sources: "won't start before <end>; a walltime ≤ X starts now". → Pure-function table test. | none | |
| S8 | *[FR#11, #13]* TIMEOUT, OOM, NODE_FAIL and PREEMPTED all become FAILED. A walltime kill leaves no `Status:` line, so the analyzer reports RUNNING forever and the CLI's failing-task list skips the task. | At the end, one `sacct -P` per array writes `tasks_state.json` (state, exit code, node, elapsed). The summary counts TIMEOUT separately. Elapsed < 60 s is flagged "infra suspect", with an `--exclude=<node>` hint. → Fake sacct fixture. | additive | |
| S9 | *[NEW]* `hsm sweep cancel` cannot cancel a live Slurm sweep: it reads a summary written after the wait, and it has no ssh-slurm branch. Native Slurm writes no manifest at all. | Both Slurm sources write the manifest at submit time. `cancel` re-attaches and runs one `scancel`. → CLI test against FakeConn. | none | |
| S10 | *[issues #15, #16; NEW]* A crashed resumable chunk exits 0 and is recorded COMPLETED. A FAILED chain never archives, so finished checkpoints stay on purgeable /scratch. `module load` does nothing in jobs submitted from a non-login shell. | `.hsm_failed` plus the real exit code; archive on FAILED; a guarded Lmod init before `modules:`. → Run the rendered template with a crashing stub. | none | |
| S11 | *[NEW]* There is no keepalive or reconnect: a login-node blip kills a multi-day launcher. `_write_remote_file` ignores its rc. | keepalive, one reconnect in `_ssh_run`, check rc. → Fake connection that drops once. | none | |
| C4 | *[FR#16, NEW]* Under YAML 1.1, an unquoted `walltime: 12:00:00` loads as the int 43200 and renders as `--time=43200`, which is 30 days. `1e-1` loads as a string, and `[007, 010]` as `[7, 8]`. | One SafeLoader subclass (YAML 1.2 floats and ints, no base-60, no octal) at every load site, plus a `[D-]HH:MM:SS` check. → Loader tests. | new sweeps store floats | |
| C5 | *[FR#15b]* A per-remote `python_path` loses to the project's `paths.conda_env`. | Anything set on the remote wins, through one helper used by both factories. → Factory test. | Comp-PVR's athena entries start using their `python_path`, as intended | |
| C6 | *[NEW]* An unknown `--remote` alias quietly becomes a bare ssh-bash remote, which could train on a login node. | Error with a "did you mean" suggestion when a `remotes:` block exists. → CLI test. | an unregistered alias must be registered | #23 ✓ |
| C7 | *[NEW]* Sweep ids have one-second resolution, so same-second launches share the local and remote dirs, including the cleanup `rm -rf`. | Create the dir exclusively and retry with a suffix. → Collision test. | none | #23 ✓ |
| X4 | *[NEW]* GPU indices are wrong in two ways, and co-tenants are not avoided:<br>- Slot indices come from nvidia-smi (PCI order), but no template sets `CUDA_DEVICE_ORDER=PCI_BUS_ID`, so CUDA reads them fastest-first. On anahita, `--gpus 1` lands on nvidia-smi #3.<br>- With no allowlist, the default "all GPUs" joins cards co-tenants are using (`GpuInfo.is_free` is ignored).<br>- `--gpus cpu` leaves `CUDA_VISIBLE_DEVICES` unset.<br>- `max_parallel_jobs` ignores the real slot count, and falling back to CPU slots is silent. | Export `CUDA_DEVICE_ORDER=PCI_BUS_ID`; with no allowlist, use only free GPUs; for cpu, `CUDA_VISIBLE_DEVICES=`; capacity = slot count, and warn on the fallback. → Render and slot tests. | anahita's `local.visible_gpus: [2,3]` is in CUDA order and must be restated in nvidia-smi order | |
| C8 | *[NEW]* `hsm remote clean`:<br>- runs an unquoted `rm -rf {target}`;<br>- takes the target from the cwd name, not the project root;<br>- ignores `workdir`;<br>- with `remote_root: ~` plus `--all-projects`, becomes `rm -rf ~`. | Resolve the target from the project, include `workdir`, quote it, and refuse any target outside an HSM-made dir (marker file). → Guard tests. | none | |
| G1 | *[NEW]* `hsm remote health --watch` crashes: the module does `import datetime`, then calls `datetime.now()`. | Fix it, plus a smoke test that walks every CLI command. | none | |
| G2 | *[NEW]* The Jinja env is not `StrictUndefined`, so a missing variable renders as "". That is gotcha #11's class of bug. | `StrictUndefined`, autoescape off; fix the 3 test helpers. | none | |

## P2: ergonomics and robustness

| ID | Finding | Fix | PR |
|---|---|---|---|
| R1 | *[FR#11]* A re-run needs a hand-written sweep file. | `hsm sweep rerun <id> --failed --timeout [--missing] [--walltime X]`, built on `sweep_analysis` selections and `tasks_state.json`. It writes `rerun_<ts>.yaml` with `combinations:` and then runs it. | |
| R2 | *[FR#12]* `grid` is a full product; unnamed `paired:` groups crash and exit 0. | `exclude:` / `include:` filters and an explicit `combinations:` list in the generator. | |
| R3 | *[FR#14]* Tasks that start in the same second share Hydra's `outputs/<date>/<time>` in a shared cwd. The next sweep's `rsync --delete` also removes the logs that running tasks write there. | Inject `hydra.run.dir=<task_dir>/.hydra_run` next to `wandb.group=` and `output.dir=`, behind **one** switch that also makes the `wandb.group=` injection conditional (a known limitation). The per-sweep snapshot (S4) removes the `--delete` effect. | |
| R4 | *[FR#17]* Nothing validates keys: a remote-level `pre_script` and a sweep-level `gird:` are both silently ignored. | One schema of known keys per block, with "did you mean `spec.pre_script`". It replaces the ad-hoc key sets. | |
| R5 | *[FR#18, corrected: three schemes]* Task dirs are named `task_N` (array), `task_NNN` (local/ssh) or `<id>_task_NNN` (individual). The analyzer counts individual-mode tasks as missing. | Canonical `task_<N>` with a tolerant reader (`task_index(name)`). | |
| R6 | *[FR#19]* A stale `project.root` or `train_script` fails late; a real run first creates a ghost tree. | `HSMConfig.check_paths()` before the dry-run output. | |
| R7 | *[FR#20, corrected]* The mid-run pulls fire per *job*, so an array only pulls at the end. A pull has no retry and no `--partial`. | A periodic pull every 10 min while jobs are live; rsync retry with backoff on rc 255/10/12/30/35; `--partial`. | |
| R8 | *[NEW]* Several commands misreport success or failure: | | |
| | - pre-flight errors exit 0 | `click.ClickException` | |
| | - the `sweeps_root` error is reported as "config not found" | drop the `except` that masks it | |
| | - a CANCELLED sweep exits 0 | exit 1 | |
| | - `hsm sweep report` crashes on every non-distributed sweep | fix | |
| | - `hsm sweep errors` reads files nothing writes | delete it (it becomes `status --errors`) | |
| | - `init` re-run drops `distributed:` | never rewrite an existing config without `--regenerate` | |
| R9 | *[NEW]* The 10 s poll is hard-coded for jobs that run 47 h. | 60 s for Slurm sources. | |
| R10 | *[NEW]* The resumable chain can double-submit: there is a crash window between sbatch and persist, and nothing locks a live launcher against a cron `advance`. The manifest write isn't atomic, and `advance` drops cost hints. | Adopt a live chunk found by `squeue -n` instead of submitting another; write the manifest via temp file and rename; persist `costs`. | |
| R11 | *[FR#15a, corrected: `~/miniforge3` is already probed]* The conda probe stops at the first install it finds, so a leftover `~/miniconda3` shadows `~/miniforge3`. It also never tries `~/mambaforge` or `$CONDA_EXE`. | Pick the first install that has `envs/<env>`; add the two missing candidates. | |
| R12 | *[NEW]* ssh task stdout is buffered in the launcher's memory and thrown away; `logs/` stays empty. | Fixed by X1: logs go to files. | |

## Gates, docs, hygiene (the `chore` lane)

- **ruff:** 897 findings (740 autofixable); 41 of 78 files unformatted; half of `[tool.ruff]` is dead.
  - Shrink the config, then make one mechanical commit listed in `.git-blame-ignore-revs`.
  - Add loom's pre-commit (ruff, ruff-format, the push guard) and a CI `gates` job.
- **pyright:** not configured; 72 errors. They include G1 and drift in the ABC contract: `submit_batch(dependency=, resumable=)` and `_pull_excludes` exist only on the Slurm sources.
  - Add loom's pyright config, fix the errors, and gate on it.
  - Delete the `[tool.mypy]` block, which never runs (244 errors).
- **Tests:**
  - Execute every rendered template, not only string-match it. The resumable block, `ssh_compute_source.sh.j2` and `slurm_single.sh.j2` never run today.
  - Move the shared fakes into `tests/fakes.py` (`FakeConn` is copied 7 times).
  - Delete the 6 unused fixtures; use one async plugin and one pytest config.
  - Remove the hard-coded 5 s sleep in distributed (half the suite's runtime).
- **Dependencies:** `wandb`, `pandas`, `numpy`, `hydra-core` and `omegaconf` are not imported by `src`; together they are about 204 MB of a 215 MB install. Drop them, together with `requirements.txt` and the `docs` extra.
- **Dead code (about 1.3k LOC):**
  - `cli/common.py`, all but `common_options`;
  - `cli/configure.py`;
  - the never-registered `sweep-legacy`;
  - `sweep errors` and `sweep queue`;
  - `templates/sweep.yaml.j2`;
  - `HydraConfigParser`, unused `utils` helpers, and the PBS branches.
- **Docs:**
  - **Remove PBS claims;** there is no PBS backend.
  - **Add missing coverage:** document `collect`, `advance`, SSH-Slurm and resumable runs.
  - **Delete** `docs/api_reference/` and `PROJECT_STRUCTURE.md`.
  - **Gate CLI docs:** a test that every click command appears in `docs/cli/README.md`.
  - **Remove machine paths** from tracked files, including the shipped `_conda_init.sh.j2` probing `$HOME/code/packages/HPC-Sweep-Manager/bin/micromamba`.
  - **Slim `CLAUDE.md`:** drop the `sync-claude-state.sh` rule (Syncthing replaced it) and the dated "Recently landed" sections, which the CHANGELOG already holds.
- **Release:**
  - The CHANGELOG is missing resumable chains (#12).
  - The version lives in 3 places; move it to `importlib.metadata`.
  - The Makefile has 11 broken targets.
  - Cut `v0.2.0` at the end of the pass.

## Consolidation targets (taken as fixes touch the module)

| Target | Est. LOC removed |
|---|---|
| A shared Slurm base for native and SSH-Slurm over one `_sh(argv)` seam. Native gains manifest, collect, cancel and batched polling. | ≈ 330 |
| One async queue class (`SlurmQueue`, `_LocalQueueAsync`, `SSHSlurmQueue`). It also fixes the local `[]`-on-failure. | ≈ 150 |
| `cli/sweep.py` from 2.2k to about 1k lines: placement, summary, collect and advance move into `core/`; `status`, `report`, `watch` and `recent` merge into one command. | ≈ 1,200 |
| Delete `distributed_manager.py` (X3). | ≈ 1,550 |
| One SSH base for both SSH sources: identical `_open_connection` and `_run_rsync`, setup (connect → resolve → mkdir → push), and factory kwargs. | ≈ 150 |
| One `build_remote_source()` instead of the two copies of backend dispatch. | ≈ 35 |
| Local as a transport of the ssh source after X1: one wrapper template and one slot function. | ≈ 120–350 |
| Dead code (chore lane). | ≈ 1,300 |

All told, about −5k of `src`'s 17k lines, while adding the P0 machinery.

## Decisions (Gabriel, 2026-10-06)

- **Discipline:** HSM adopts the program's PR, formatting and chore discipline (`$SCM_ROOT/CLAUDE.md`
  §Working pipeline, `DISCIPLINE.md`):
  - draft PRs reviewed inline, landed as true merge commits, with scope frozen once a PR opens;
  - a chunk cap of 150 hand-written lines (`oversize-approved` is Gabriel's label);
  - the chore lane's first-line declaration;
  - the 🤖 marker on agent comments, and one closing digest per review round;
  - ruff and pyright as gates, identical in pre-commit and CI;
  - the pre-push guard on `main`.

  The method files are vendored verbatim from loom. The mechanical format (chore-2) lands before the cap
  exists (chore-4), with Gabriel's approval given in session.
- **Distributed:** rewrite it in about 80 lines (X3); don't delete the mode.
- **Formatting:** one mechanical ruff PR goes first, and every lane branches from it.
- **Merging:** pure chores self-merge on green CI: docs filing, the mechanical format, deletions of dead code and
  dependencies, the docs refresh. Anything that changes execution, config or CLI behaviour waits for Gabriel's merge.
  - *Superseded the same day:* Gabriel won't be reviewing, so the agent conducts every PR end to end: cold
    reviews, fixes folded in, a 🤖-marked digest, and a true merge once the PR is clean and CI is green.
  - Under that delegation the agent applies `oversize-approved` itself, only when a split would hurt
    readability, and states why in the PR.
  - Live runs on uzh/athena still need Gabriel's explicit go. Until then a merge rests on mocks plus the cold
    reviews; validating live is a separate step before the reinstall on anahita.
- **Fair share inside HSM (later the same day):** a Slurm launch probes the account, using the same
  `sshare`/`squeue`/`sinfo` round trip as `uzh-share`, and prints its summary line.
  - **Hot account** (over 2× its share, or co-workers pending on Priority): HSM warns and asks,
    defaulting to throttle-and-go. The other answers are launch-as-asked (`--force` skips the
    prompt), wait (re-check for up to about 12 h), or cancel. A launch with no TTY takes the default.
  - **CPU-only jobs** exclude the partition's GPU nodes by default.
  - **Tracking.** All of this is S-4, and it absorbs FR#10.
- **New defaults, each printing a message where it differs:**
  - array submission for slurm remotes (S3);
  - GPU indices in nvidia-smi order, using only free GPUs when no allowlist is given (X4). On anahita,
    `local.visible_gpus: [2,3]` becomes `[1,2]`; change it in the same step as the reinstall.
  - HSM drops its unused heavy dependencies;
  - one switch for HSM's Hydra overrides, adding `hydra.run.dir` (R3).

## PR plan

Lane branches start from `main` after `chore-4`. Each lane has one open PR at a time; within a lane, the row order is
the landing order.

**Execution-path PRs** (S-1…S-3, S-6, X-1…X-3) get a 2–3 angle cold review before they are marked ready. Each also
gets a tiny live smoke run on uzh or athena, but only with Gabriel's go and after `uzh-share`.

**Install.** Nothing is installed on anahita until the consumer's launchers have exited. After that: pull the main
checkout, reinstall, restate `visible_gpus`.

| Lane | PR | Closes | Sev |
|---|---|---|---|
| chore | chore-1 (#17 ✓) · this tracker plus the three field reports | — | — |
| chore | chore-2 (#18 ✓) · loom's `[tool.ruff]`, ruff pinned to 0.15.13; one mechanical `ruff check --fix` + `ruff format` commit, listed in `.git-blame-ignore-revs` | G (ruff) | — |
| chore | chore-3 (#19 ✓) · the 77 leftovers fixed by hand, so ruff is clean (the modules due for deletion get a temporary per-file ignore) | G (ruff) | — |
| chore | chore-4 (#20 ✓) · the method: pre-commit, the push guard, the chunk cap, the PR template, a CI `gates` job, CONTRIBUTING | G (gates) | — |
| chore | ci (#25 ✓) · CI runs on label changes, so `oversize-approved` takes effect without a push | — | — |
| chore | chore-5 · `tests/fakes.py`, a harness that runs rendered templates, one pytest config, dead fixtures | G (tests) | — |
| slurm | S-1 (#21 ✓) · transient-safe, batched status refresh for both Slurm sources; `collect` uses it; `sbatch --parsable` | S1, S2 | P0 |
| slurm | S-2 (#26 ✓) · array default; manifest on any partial submission (a chain keeps its own); one channel per script | S3 | P0 |
| slurm | S-3 (#27) · per-sweep code snapshot (`--link-dest`), shared by both SSH sources | S4 | P0 |
| slurm | S-4 · aware launches: `hsm queue share`; a pre-submit fair-share check (hot account → prompt: throttle-and-go by default, `--force`, wait, cancel); `array_throttle`; GPU nodes excluded for CPU-only jobs; reservation-vs-walltime check; key normalisation for `extra_directives` | S5–S7, FR#10 | P1 |
| slurm | S-5 · `tasks_state.json` (TIMEOUT/OOM/infra), periodic pull, rsync retry, 60 s poll; manifest at submit for native Slurm and a manifest-backed `cancel` | S8, S9, R7, R9 | P1 |
| slurm | S-6 · resumable fixes (#15, #16), archive on FAILED, Lmod init, chain double-submit, keepalive and reconnect | S10, S11, R10 | P1 |
| slurm | S-7 · one Slurm base for native and SSH-Slurm; one queue class | consolidation | — |
| ssh | X-1 (#24 ✓) · `login_timeout` with an agent-less retry remembered per host, a bounded connect, rsync over ssh with `BatchMode`/`ConnectTimeout`/`ServerAliveInterval` (keepalive moved to X-2, with the reconnect) | X2 | P0 |
| ssh | X-2 · detached ssh supervision, `hsm sweep collect` re-attaches ssh sweeps, task logs to files, keepalive + reconnect | X1, R12, S11 (ssh side) | P0 |
| ssh | X-3 · distributed rewritten over the children (−1.5k LOC) | X3 | P0 |
| ssh | X-4 · nvidia-smi GPU order, free-GPU default, capacity = slot count | X4 | P1 |
| ssh | X-5 · conda probe picks the install that has the env; the Hydra override switch | R11, R3 | P2 |
| ssh | X-6 · one SSH base, one `build_remote_source`, local as a transport | consolidation | — |
| cli | C-1 (#23 ✓) · `--remote` with `auto`, per-key spec filter, unknown alias, sweep-id collisions, honest exit codes | C1, C2, C6, C7, R8 | P0 |
| cli | C-2 · `remote add/remove` edit the project file only; `remote clean` guards | C3, C8 | P0 |
| cli | C-3 · YAML 1.2 loader and walltime check; per-remote interpreter precedence | C4, C5 | P1 |
| cli | C-4 · one schema of known config keys; path checks before submit | R4, R6 | P2 |
| cli | C-5 · one task-dir naming scheme; analyzer and `report` fixes | R5, R8 | P2 |
| cli | C-6 · `exclude`/`include`/`combinations` in the generator; `hsm sweep rerun` | R2, R1 | P2 |
| cli | C-7 · thin `cli/sweep.py` | consolidation | — |
| chore | chore-6 · dead code, unused dependencies, Makefile, `requirements.txt` | G (deps, dead code) | — |
| chore | chore-7 · docs refresh, a CLI-docs rot test, no machine paths, slim CLAUDE.md | G (docs) | — |
| chore | chore-8 · pyright config and fixes, plus the CI gate (after the deletions, so fewer errors) | G1, G2, pyright | — |
| chore | release · `v0.2.0`: CHANGELOG, version in one place, tag | — | — |
