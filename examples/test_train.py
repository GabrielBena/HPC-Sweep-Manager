#!/usr/bin/env python3
"""Canonical reference for the HSM training-script contract.

This script demonstrates the minimum interface a training script needs to play
nicely with HSM:

1. Accept Hydra-style ``key=value`` command-line arguments, including dotted
   keys (``model.hidden_size=128`` becomes a nested update in real training
   loops; here we just record them).
2. Honor ``output.dir`` — create the directory and write task artifacts there.
3. Honor ``wandb.group`` — pass it to W&B if you use W&B (this script just
   records it; it does not import wandb so the example runs anywhere).
4. Print progress to stdout (so ``--show-output`` in local mode is useful).
5. Exit ``0`` on success, non-zero on failure.

Special params (for HSM smoke-testing — feel free to ignore in real scripts):

- ``_should_fail=true``: exit non-zero halfway through, to test the FAILED
  status path and ``keep_remote_on_success=false`` cleanup semantics.
- ``_iterations=N``: how many fake training steps to do (default 5).

Stdlib-only on purpose; no torch / wandb / hydra import needed to run this
example. Real training scripts will of course have those.
"""

from __future__ import annotations

import os
import socket
import sys
import time
from pathlib import Path


def parse_hydra_args(argv: list[str]) -> dict[str, str]:
    """Parse ``key=value`` tokens into a flat dict.

    Hydra renders nested params as dotted keys (``model.hidden_size=128``);
    we keep them as strings here. Real scripts hand them to Hydra/OmegaConf.
    """
    params: dict[str, str] = {}
    for tok in argv:
        if "=" not in tok:
            continue
        key, value = tok.split("=", 1)
        params[key.strip()] = value.strip()
    return params


def main() -> int:
    params = parse_hydra_args(sys.argv[1:])

    output_dir = Path(params.get("output.dir", "."))
    wandb_group = params.get("wandb.group", "<unset>")
    should_fail = params.get("_should_fail", "false").lower() == "true"
    iterations = int(params.get("_iterations", "5"))

    host = socket.gethostname()
    cuda = os.environ.get("CUDA_VISIBLE_DEVICES", "<unset>")

    print(f"[test_train] host={host}", flush=True)
    print(f"[test_train] CUDA_VISIBLE_DEVICES={cuda}", flush=True)
    print(f"[test_train] wandb.group={wandb_group}", flush=True)
    print(f"[test_train] output.dir={output_dir}", flush=True)
    print(f"[test_train] params={params}", flush=True)

    output_dir.mkdir(parents=True, exist_ok=True)

    # Fake "training loop" — sleep + progress prints. Total ~3-5s by default.
    for step in range(1, iterations + 1):
        if should_fail and step == max(iterations // 2, 1):
            print(f"[test_train] step {step}: _should_fail=true → exiting non-zero", flush=True)
            return 1
        loss = 1.0 / step  # placeholder
        print(f"[test_train] step {step}/{iterations} loss={loss:.4f}", flush=True)
        time.sleep(0.7)

    # Write the sentinel HSM smoke tests look for. Real scripts would write
    # checkpoints, metrics, configs, etc. here instead.
    sentinel = output_dir / "sentinel.txt"
    sentinel.write_text(
        "\n".join(
            [
                f"host={host}",
                f"cuda={cuda}",
                f"wandb.group={wandb_group}",
                f"params={params}",
                f"finished={time.ctime()}",
            ]
        )
        + "\n"
    )
    print(f"[test_train] wrote {sentinel}", flush=True)
    print("[test_train] done", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
