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

## Open — need a maintainer's design call (not patched here)

### P0 — `train_script` auto-detect can pick the wrong entrypoint, silently

`hsm setup init` chose `scripts/train.py` in a repo that also has
`scripts/train_2d.py` — the real entrypoint, with a different Hydra
`config_name` and guard logic. Nothing warned that >1 `train*.py` candidate
existed. A user who doesn't set a per-sweep `script:` then runs the wrong script
against the wrong config and gets plausible-but-wrong results with no error.
- Suggest: when detection finds >1 candidate, warn + list them (or prompt under
  `-i`), and echo the chosen script at `hsm sweep run` time.
- Code: `core/common/path_detector.detect_train_script()`, surfaced via
  `cli/init.py`.

### P1a — `--dry-run` shows a launch command that isn't the one that runs

With `conda_env` set, local tasks execute `conda run -n <env> python …` via a
wrapper that sources `_conda_init.sh.j2` (`local_compute_source.py:116` + the
template include). But `--dry-run` prints `Python: /…/envs/<env>/bin/python` — a
bare interpreter path that bypasses the conda activation the real run performs.
Copy-paste it to reproduce a failure and you get different behavior.
- Suggest: dry-run should render the actual command (run-prefix + GPU pinning +
  script + args) for at least the first task.

### P1b — `--dry-run` preview is illegible for a real (≈30-key) config

The "Sweep Information" table elides the parameter-name column and truncates
values to `…`; the "First N combinations" block is a line-wrapped dict-repr
interleaved with the `args:` string. The thing a user most needs to verify
before a multi-hour launch — **did my `paired:` groups zip correctly?** — is not
legibly answerable from it.
- Suggest: render paired groups explicitly, e.g.
  `paired 'dilation': rec_steps=1↔alpha=1.0 | rec_steps=4↔alpha=0.5`, and don't
  truncate values under `--dry-run`.

## Minor

- `docs/cli/README.md`'s flag table has wider drift than the one line removed
  here: it lists `--priority` (not seen in `hsm sweep run --help`) and omits
  `--gpus`, `-q/--quiet`, `-v/--verbose`. Worth a full reconcile against
  `--help` output.
- `hsm --version` is hardcoded `0.1.0` in two places (`pyproject.toml`,
  `__init__.py`); it didn't change across today's `init`-template fix, so users
  can't distinguish builds during the active refactor. A `0.1.0+<gitsha>` dev
  suffix would help bug reports.

---

Reported by an agent driving the Comp-PVR project. Happy to convert P0/P1 into
separate issues if that fits the workflow better.
