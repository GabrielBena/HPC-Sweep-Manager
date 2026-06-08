#!/usr/bin/env python
"""Reference implementation of the HSM resumable-chain CONTRACT (issue #12).

A trivial, NON-GPU "training" loop that lets you smoke-test
`hsm sweep run --resumable` end-to-end (and serves as the copy-paste reference
for what a real training script must implement). HSM stays a general
orchestrator — it knows only the four environment variables below, never this
script's checkpoint format.

HSM invokes it like every other HSM task:

    python resumable_probe.py <hydra-style key=value overrides> \
        wandb.group=<g> output.dir=<dir> [training.resume_from=<path>]

and additionally exports (only in --resumable mode):

    HSM_WORKDIR        persistent per-task dir (survives between chunks)
    HSM_RESUME_TO      where to SAVE the resume checkpoint  ($HSM_WORKDIR/<subdir>)
    HSM_RESUME_FROM    where to LOAD it from (empty on chunk 1)
    HSM_DONE_SENTINEL  touch this when the WHOLE budget is complete

The four contract points are tagged [CONTRACT N] below.
"""

from __future__ import annotations

import os
import signal
import sys
import time
from pathlib import Path

# ---- parse the hydra-style `key=value` argv (ignore the ones we don't use) ---
overrides = {}
for tok in sys.argv[1:]:
    if "=" in tok:
        k, _, v = tok.partition("=")
        overrides[k] = v

# The TOTAL budget — read from the (unchanged-every-chunk) config, NOT inferred
# from the chunk. [CONTRACT 4: faithful budget]
TOTAL_STEPS = int(overrides.get("total_steps", "6"))
STEP_SECONDS = float(overrides.get("step_seconds", "1.0"))

# HSM-provided paths (with sensible standalone fallbacks so the script also runs
# outside HSM).
out_dir = Path(overrides.get("output.dir") or os.environ.get("HSM_WORKDIR") or ".")
resume_to = Path(os.environ.get("HSM_RESUME_TO") or (out_dir / "resume"))
resume_from = os.environ.get("HSM_RESUME_FROM") or overrides.get("training.resume_from") or ""
done_sentinel = Path(os.environ.get("HSM_DONE_SENTINEL") or (out_dir / ".hsm_done"))
resume_to.mkdir(parents=True, exist_ok=True)
ckpt = resume_to / "step.txt"


def save(step: int) -> None:
    """Atomically persist the resume state. [CONTRACT 2: save on SIGTERM + periodically]"""
    tmp = ckpt.with_suffix(".tmp")
    tmp.write_text(str(step))
    tmp.replace(ckpt)
    print(f"[probe] checkpoint saved at step {step} -> {ckpt}", flush=True)


# [CONTRACT 1: consume the resume pointer] — resume iff it's set/non-empty AND a
# checkpoint actually exists there (a hard crash may have left only a periodic one).
step = 0
if resume_from and ckpt.exists():
    step = int(ckpt.read_text().strip())
    print(f"[probe] resuming from step {step} (HSM_RESUME_FROM={resume_from})", flush=True)
else:
    print("[probe] fresh start", flush=True)

# [CONTRACT 2: save on the pre-walltime signal] — HSM sets --signal=B:TERM@<grace>;
# the rendered wrapper forwards SIGTERM here. Save and exit cleanly.
_current = {"step": step}


def _on_sigterm(signum, frame):
    print("[probe] SIGTERM — saving and exiting before walltime", flush=True)
    save(_current["step"])
    sys.exit(0)


signal.signal(signal.SIGTERM, _on_sigterm)

# ---- the "training" loop -----------------------------------------------------
print(f"[probe] budget={TOTAL_STEPS} steps, starting at {step}", flush=True)
while step < TOTAL_STEPS:
    time.sleep(STEP_SECONDS)
    step += 1
    _current["step"] = step
    save(step)  # periodic checkpoint — survives a hard crash without SIGTERM
    print(f"[probe] step {step}/{TOTAL_STEPS}", flush=True)

# [CONTRACT 3: signal done] — the WHOLE budget is complete; the chain stops here.
done_sentinel.parent.mkdir(parents=True, exist_ok=True)
done_sentinel.write_text("done\n")
print(f"[probe] DONE — wrote {done_sentinel}", flush=True)
