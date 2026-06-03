# HSM Field Notes — First Consumer Run (Comp-PVR, 2026-06-03)

**Context.** First real init + use of HSM in the Comp-PVR project, deliberately driven
**blind by a coding agent** (no reading of HSM source first) to test whether the shipped
CLI + `hsm docs` + the generated `sweeps/README.md` are enough to drive HSM *without
spelunking the package* — the stated design goal. Frictions recorded while fresh.

> **✅ RESOLVED 2026-06-03** (same day, 5 commits on `main`):
> **B3** fixed — params extraction runs by file path (tempfile), never stdin,
> *plus* the empty-params fail-fast guard; regression-tested with a
> stdin-swallowing wrapper that reproduces the cluster behavior locally.
> **B2** fixed and broadened — `/wandb`, `/checkpoints`, `/multirun` all
> anchored (same shadowing class); real-rsync behavioral tests pin that
> `configs/<name>/` groups survive the push.
> **B1** fixed — `interactive` threaded through; plus same-theme hardening:
> `hsm setup init` exits non-zero on real failure, re-runs back up
> `.hsm/config.yaml` to `.bak`, and non-interactive runs never prompt
> (migration path included).
> **G1** — the re-run contract is now in `hsm setup init --help`, README, and
> getting_started.md. **Doc gap** — new "Your project's own package on the
> remote" section in SSH_EXECUTION.md (+ MULTI_CLUSTER cross-link).
> **U1** (`hsm init` alias), **U2** (canonical-branch note in `hsm docs` +
> README), **U3** (soft-wrapped URLs) all shipped.
> **Deferred:** G2 heuristic rework (the loud warning + per-sweep `script:`
> remain the mitigation — no static heuristic beats a stale `scripts/train.py`
> shadowing the real entrypoint); making the `wandb.group=` injection
> conditional (design follow-up — see CLAUDE.md "Known limitations").

**Status of the fixes below — please read.** Three bugs were root-caused and fix-prototyped
during this session, then **reverted to canonical**: HSM adjustments are the reviewer's call,
not the consumer's. The exact diffs are preserved in the sibling patch
[`2026-06-03-comp-pvr-first-run.patch`](./2026-06-03-comp-pvr-first-run.patch)
— apply with `git apply docs/dev/field-reports/2026-06-03-comp-pvr-first-run.patch` from the
repo root to reproduce them. Treat every "Proposed fix" line below as a *suggestion*, not an
applied change. (The landed fixes deviate deliberately in two places: plain `mktemp`, and
`|| { … }` error handling — the patch's `$?` check was dead code under `set -e`.)

**Verdict.** *Configuring* HSM blind was ~80% smooth (one false-failure aside). But the first
real **push → sbatch-array → collect** run on the S3IT/Slurm cluster hit **three bugs, two of
them silent and load-bearing** — they don't error at submit time; they corrupt or block the run
only once a task executes on a compute node. The two most expensive (B1, B3) are
**success-shaped silent failures** — the status says success, reality is otherwise — which is
the single worst failure mode for an autonomous consumer.

---

## Bugs

### B1 — `hsm setup init` crashes *after* writing all files → false "failed" status  **[HIGH · silent / success-shaped]**
`_create_sweep_infrastructure(project_path, config, console, logger)` does not accept
`interactive`, yet calls `_offer_agent_pointer(..., interactive, ...)` →
`NameError: name 'interactive' is not defined`. The crash fires **after** `config.yaml`,
`sweeps/README.md`, and `sweeps/example_sweep.yaml` are all written, so init has actually
*succeeded* — yet it prints `❌ Project initialization failed!` and exits non-zero.

- **Impact:** a false-negative terminal status. An agent/non-interactive caller trusting the
  exit code re-runs or escalates; the work was already done.
- **Proposed fix:** thread `interactive` through `_create_sweep_infrastructure` (signature +
  the one call site in `init_project`). — `cli/init.py`
- **Test worth adding:** assert exit-0 + all expected files present after a non-interactive init.

### B2 — rsync exclude `wandb` is unanchored → strips `configs/wandb/` → every task fails  **[HIGH · load-bearing]**
`DEFAULT_RSYNC_EXCLUDES` carries a bare `"wandb"` to drop the wandb *output* dir. Unanchored,
it *also* matches the Hydra **config group** `configs/wandb/`, silently removing it from the
pushed tree. The slurm-array template then injects `wandb.group=$WANDB_GROUP` into **every**
training command (not optional in HSM), so Hydra can't compose and each task dies with
`MissingConfigException: Could not find 'wandb/wandb'`.

- **Why it's load-bearing, not a project quirk:** HSM *itself* requires the `wandb` group to
  exist on the remote (it writes `wandb.group=`). So *any* Hydra project that keeps a
  `configs/wandb/` group is broken by this exclude out of the box.
- **No consumer-side workaround:** per-remote `rsync_excludes` only *extends* the defaults (the
  in-code comment states this); a default exclude cannot be un-set from config. So this can only
  be fixed in HSM — a consumer is stuck.
- **Proposed fix:** anchor to `"/wandb"` (repo-root only). — `core/remote/push_exec.py`
- **Sharper fix worth considering:** make *all* output-dir excludes anchored / trailing-slash
  (`/wandb`, `outputs/`, `checkpoints/`) so none can ever shadow a same-named `configs/<x>/`
  group. Config-dir vs output-dir name collisions are common (`wandb`, `outputs`, `logs`).

### B3 — array params via `conda run … python - <<heredoc` inside `$()` → empty params → silent default-config run  **[CRITICAL · load-bearing · silent / success-shaped]**
`slurm_array.sh.j2` extracts each task's Hydra overrides with
`PARAMS_JSON=$({{ python_path }} - <<'PYTHON_EOF' … PYTHON_EOF)`. When `python_path` resolves
to a conda **wrapper** — `conda run -n <env> python`, the default whenever `paths.conda_env` is
set — the heredoc on stdin is **not forwarded** through `conda run` to the Python subprocess
inside command substitution on this cluster. `python -` reads empty stdin, prints nothing,
`PARAMS_JSON` ends up empty, and `$?` is still `0`. The task then runs the training script with
**no overrides** — i.e. the project's *default* config (here: 500 epochs) — and reports SUCCESS.

- **How it bit us:** a CPU benchmark "hung" for ~29 min (RUNNING, none of the expected short
  epochs) before we realised every task was silently training the 500-epoch default instead of
  the 4/20-epoch benchmark cells. No error anywhere — the worst kind of waste.
- **Why it's load-bearing:** it triggers for the *default* execution path (`conda run` prefix +
  array mode). Any conda-based array sweep on a cluster with this `conda run` stdin behaviour is
  exposed.
- **Proposed fix (in the patch):** write the extraction snippet to a tempfile and run it by
  *path* (`python "$script"`) — no stdin, so the wrapper is irrelevant. `python -c '<snippet>'`
  works too. The general rule: **never feed a program over stdin into an interpreter prefix you
  don't control** — it may be a wrapper that doesn't pass stdin through. — `templates/slurm_array.sh.j2`
- **Defense-in-depth worth adding regardless of the fix:** after extraction, **fail fast** when
  `PARAMS_JSON` is empty for an array task
  (`[[ -z "$PARAMS_JSON" ]] && { echo "empty params for task $SLURM_ARRAY_TASK_ID"; exit 1; }`).
  A sweep task running with zero overrides is *never* intended; it should error, not silently
  run a default. That one line turns B3 from a 29-min silent waste into an instant, obvious
  failure — and guards against any *future* regression in param plumbing.

**Theme — silent, success-shaped failures cost the most.** B1 and B3 are the same class: the
reported status contradicts reality (false fail; false success). An autonomous consumer has no
error to react to, so it burns wall-clock and allocation before a human notices. Anywhere HSM
guesses or derives something quietly (init exit status, per-task param injection, train-script
pick), a cheap assert that *reality matches the reported status* pays for itself many times over.

---

## Agent-friendliness gaps (drivable, but required reading source)

### G1 — `setup init` idempotency / overwrite contract is undocumented  **[the #1 ask]**
Before re-running init on an existing `.hsm/`, the questions a careful consumer *must* answer
first are: does this overwrite `config.yaml`? clobber my hand-written sweep YAMLs? prompt and
hang under a non-TTY? None of `hsm --help`, `hsm setup init --help`, `hsm docs`, or
`sweeps/README.md` says — so the agent read `init.py` to be safe.

- **Suggested:** one line in `setup init --help` + README, e.g. *"Safe to re-run. Regenerates
  `.hsm/config.yaml`, `sweeps/README.md`, `sweeps/example_sweep.yaml`; leaves your other sweep
  configs untouched. Non-interactive mode never prompts."*
- This is the single highest-value doc addition for blind/agent use.

### G2 — Train-script auto-pick chose the wrong entrypoint
The detector found **9 candidates** and auto-picked `scripts/train.py`; the project's real
entrypoint is `scripts/train_2d.py` (what every sweep YAML's `script:` already declares). It
was *already* wrong in the prior `.hsm/config.yaml`, so it's a recurring trap, not a one-off.

- **What worked:** the loud warning (*"9 possible training scripts found — auto-picked … set
  `paths.train_script` …"*) was excellent and directly actionable — the agent fixed it from
  CLI output alone, no source needed. Keep that pattern; it's the gold standard.
- **Suggested:** improve the heuristic — prefer `scripts/*`, deprioritize package internals
  (`pvr/nn/training/*.py`, `*/utils/*`, `sweeps/**`), tie-break by mtime or name match. Or, in
  non-interactive mode when ambiguous, write a sentinel (`train_script: FIXME-AMBIGUOUS-…`)
  that can't silently run the wrong script. (Per-sweep `script:` saved us here.)

---

## Docs vs source — where the agent reached for source (re-evaluated)

The agent twice opened HSM source instead of `hsm docs`. On review:

- **Avoidable (docs had it).** The push-model mechanics (rolling code dir
  `~/.hsm/runs/<project>/code/`, `conda run -n <env> python`, `pre_script` /
  `modules:` for `module load`) are all in `SSH_EXECUTION.md` + `HPC_EXECUTION.md`.
  Reading `core/remote/push_exec.py` for these was a *process* miss — `hsm docs`
  should have been step one. Not an HSM defect; if anything it validates the docs.
- **Genuine doc gap.** None of the four guides explain how a project's **own
  importable package** resolves on the remote (`grep -i pythonpath` across the
  user guide → nothing). For a project whose training imports a locally
  *editable*-installed package (Comp-PVR's `pvr`), the rsynced tree alone won't
  make `import pvr` work: `python scripts/train.py` puts `scripts/` on `sys.path`,
  not the project root. The package must be installed in the remote conda env, or
  `PYTHONPATH` must include the code dir.
  - **Suggested:** a short "Your project's own package on the remote" subsection in
    `SSH_EXECUTION.md` — "if your training does `import yourpkg` and it's editable
    locally, either `pip install -e .` it into the remote env or set
    `pre_script: [export PYTHONPATH=~/.hsm/runs/<project>/code:$PYTHONPATH]`."
  - This is the kind of thing that fails *silently at runtime on the compute node*
    (ModuleNotFoundError mid-sweep), so it's high-value to document up front.

---

## Discoverability / UX (minor)

- **U1 — `hsm init` is not a command** — it lives at `hsm setup init`. Muscle memory and most
  tools put `init` at top level; the agent's first `hsm init` errored. Consider a top-level
  `hsm init` alias to the `setup init` group.
- **U2 — "What's the latest version?" needed git archaeology.** Installed editable as
  `0.1.0+g<hash>`; an apparently-newer `origin/v2` branch ("new package structure") is in
  fact an abandoned 2025-07 experiment, while `main` is current (84 commits ahead). Nothing
  in the CLI signals that `main` is canonical. A `hsm --version` note or a one-liner in
  `hsm docs` would prevent a wrong-branch checkout.
- **U3 — `hsm docs` URLs line-wrap** at normal terminal width, breaking copy/paste of the
  GitHub links. Consider printing bare URLs on their own unwrapped lines.

---

## What worked well (keep / don't regress)

- **Loud, actionable warnings** — the train-script ambiguity message is the model to copy
  everywhere a default is guessed.
- **`hsm docs`** surfacing both URLs *and* local checkout paths.
- **`--count-only` / `--dry-run`** — safe, no-launch verification an agent can lean on.
- **Self-contained `sweeps/README.md`** + auto-detected `conda_env` (from `$CONDA_DEFAULT_ENV`)
  and GPU seeding (`nvidia-smi -L` → `local.gpus: 1`).
- **Non-interactive defaulting** that did not prompt/hang under a non-TTY shell (machine
  config `~/.hsm/config.yaml` was acknowledged, not re-prompted).

---

## Priority for the reviewer (one-line each)

1. **B3** (CRITICAL) — silent default-config run on conda-array sweeps. Fix params plumbing
   *and* add the empty-params fail-fast guard.
2. **B2** (HIGH) — anchor output-dir rsync excludes so they can't strip `configs/<name>/`.
   No consumer workaround exists.
3. **B1** (HIGH) — init reports failure after succeeding. One-line signature fix + a test.
4. **G1 / doc gap** — document init idempotency and own-package-on-remote (`PYTHONPATH`).
