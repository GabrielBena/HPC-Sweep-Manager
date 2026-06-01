# First external-use feedback — 2026-05-29

First real use of HSM by an outside project (Comp-PVR — a Hydra/PyTorch RNN
sweep): `hsm setup init` followed by a 144-run local sweep, dry-run only.
Captured as a field report. Items already fixed are noted so the next agent
doesn't re-investigate.

## Already resolved by `7243a48` (conda_env as single source of truth)

- Runtime deprecation warning on a freshly-`init`-ed config is gone. `conda_env`
  is canonical, and `init` now auto-detects the active env + prompts. This is a
  stronger fix than just "stop writing the deprecated field" — good.

## Doc drift fixed in this branch (`docs/conda-env-drift`)

- `README.md`, `docs/PROJECT_STRUCTURE.md`: config examples still showed the
  deprecated `paths.python_interpreter` → switched to `conda_env`.
- `docs/user_guide/getting_started.md`: "point at your interpreter" wording;
  local GPU pinning shown via `--resources "--gpus=1"` (that's the Slurm opaque
  string — local control is the `--gpus` allowlist + `local.gpus` per-task
  count); and a reference to a `--show-output` flag that doesn't exist.
- `docs/cli/README.md`: the same phantom `--show-output` flag in the flag table.

## Resolved in this branch (docs/conda-env-drift)

These three were filed as needing a maintainer's design call. They were
addressed in this same branch after a maintainer-side review; the original
descriptions are kept for context, each followed by **Fixed:** noting where.

### P0 — `train_script` auto-detect can pick the wrong entrypoint, silently

`hsm setup init` chose `scripts/train.py` in a repo that also has
`scripts/train_2d.py` — the real entrypoint, with a different Hydra
`config_name` and guard logic. Nothing warned that >1 `train*.py` candidate
existed. A user who doesn't set a per-sweep `script:` then runs the wrong script
against the wrong config and gets plausible-but-wrong results with no error.

**Fixed:** `core/common/path_detector.py` gained `detect_train_script_candidates()`
(deterministic ordering; skips vendored/cache dirs and structurally-detected
virtualenvs, but not legitimately-named source dirs like `env/`). `cli/init.py`
now warns + lists every candidate in the detection table and forces an explicit
choice (`IntPrompt`) under interactive init when more than one exists.
`cli/sweep.py` echoes `Training script: …` on every run (dry or real).

### P1a — `--dry-run` shows a launch command that isn't the one that runs

With `conda_env` set, local tasks execute `conda run -n <env> python …` via a
wrapper that sources `_conda_init.sh.j2`. But `--dry-run` printed
`Python: /…/envs/<env>/bin/python` — a bare interpreter path that bypasses the
conda activation the real run performs. Copy-paste it to reproduce a failure and
you get different behavior.

**Fixed:** the `--dry-run` block in `cli/sweep.py` recomputes the run-prefix via
`resolve_run_prefix(conda_env, …)` and prints the full first-task command
(`cd <project> && conda run -n <env> python <script> <args> wandb.group=… output.dir=…`),
matching what the wrapper executes. For `--mode distributed` it notes that
per-child commands differ instead of printing one misleading line.

### P1b — `--dry-run` preview is illegible for a real (≈30-key) config

The "Sweep Information" table elided the parameter-name column and truncated
values to `…`; the combinations block was a line-wrapped dict-repr. The thing a
user most needs to verify before a multi-hour launch — **did my `paired:` groups
zip correctly?** — wasn't legibly answerable.

**Fixed:** `cli/sweep.py` stops truncating values (`_format_param_values`), adds
an `N` count column plus the originating group name, and renders an explicit
zipped view via `_render_paired_groups`:

    Paired groups (zipped):
      dilation — 3 pair(s):
        [0] model.rec_steps=1 · model.alpha=1.0
        [1] model.rec_steps=2 · model.alpha=0.75

### Bonus — placement / GPU-visibility preview

A follow-up request: every run (dry or real) now prints a **Placement** block —
where jobs land, the GPU allowlist actually in effect, and the resulting
concurrency (parallel slots → ≈ runs per GPU). See
`cli/sweep.py:_render_placement` + `local_compute_source.compute_gpu_slots` /
`plan_layout`.

## Minor — also resolved here

- **Flag-table reconcile:** `docs/cli/README.md` now matches `hsm sweep run`
  exactly — dropped the phantom `--priority` / `--show-output`, added `--gpus`,
  `-q/--quiet`, `-v/--verbose`.
- **Version string:** `hsm --version` now reports `0.1.0+g<sha>` from a source
  checkout (`__init__.py:_resolve_version` / `_git_short_sha`), so dev builds are
  distinguishable in bug reports. `pyproject.toml` keeps the base `0.1.0`.

---

Reported by an agent driving the Comp-PVR project; addressed in the same branch
after a maintainer-side adversarial review.
