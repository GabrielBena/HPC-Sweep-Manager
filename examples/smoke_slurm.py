#!/usr/bin/env python3
"""Smoke test for SlurmComputeSource (Phase 1 backend, not yet CLI-wired).

Designed for use on an S3IT login node (or any host with Slurm tools in
PATH). After ``pip install -e .`` of the repo, run from anywhere:

    python examples/smoke_slurm.py --dry-run     # render only, don't submit
    python examples/smoke_slurm.py               # submit one ~30s job + wait
    python examples/smoke_slurm.py --array 4     # 4-task array submission + wait
    python examples/smoke_slurm.py --no-wait     # submit and exit

The script writes everything under ``$SCRATCH/hsm-smoke`` (or
``/tmp/hsm-smoke`` if ``$SCRATCH`` is unset). Edit ``DEFAULT_SPEC`` at the
top of this file to match your project — QOS, GPU module, account, etc.

After a successful run, verify:

- ``$SCRATCH/hsm-smoke/sweep_test/scripts/*.slurm`` — generated job scripts
- ``$SCRATCH/hsm-smoke/sweep_test/logs/*.out|.err`` — Slurm stdout/stderr
- ``$SCRATCH/hsm-smoke/sweep_test/tasks/*/task_info.txt`` — should show
  ``Status: SUCCESS``

NOTE: This script taps into ``SlurmComputeSource._effective_spec`` and
``_render_directives`` directly so the dry-run path can show what would be
submitted. Once a public CLI lands (see Phase 1 Pass A remaining work), this
example becomes a thin shell over ``hsm sweep run``.
"""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
import shutil
import sys
import tempfile

from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
from hpc_sweep_manager.core.hpc.slurm_compute_source import SlurmComputeSource


# -----------------------------------------------------------------------------
# Tune for your S3IT setup. Common knobs are flagged in the inline comments.
# -----------------------------------------------------------------------------
DEFAULT_SPEC = ResourceSpec(
    walltime="00:05:00",
    cpus_per_task=1,
    mem_per_cpu="1G",
    qos="normal",
    # GPU run on S3IT — uncomment as needed:
    # gpus=1,
    # gpu_type="h100",                           # or "l4"
    # modules=("h100",),                         # or ("l4",)
    # Account-billed projects:
    # account="proj-XYZ",
)

# S3IT enforces these QOS tiers (normal=24h, medium=48h, long=7d). Set to
# ``None`` to skip whitelist validation.
QOS_WHITELIST: frozenset[str] | None = frozenset({"normal", "medium", "long"})


# Trivial training simulator — used as the job body. Prints, sleeps, exits.
TRAIN_SCRIPT = """#!/usr/bin/env python3
import os
import sys
import time

print(f"hello from {os.uname().nodename} at {time.ctime()}", flush=True)
print(f"  CUDA_VISIBLE_DEVICES = {os.environ.get('CUDA_VISIBLE_DEVICES', '<unset>')}", flush=True)
for arg in sys.argv[1:]:
    print(f"  arg: {arg}", flush=True)
print("sleeping 30s ...", flush=True)
time.sleep(30)
print("done", flush=True)
"""


def workdir() -> Path:
    scratch = Path(os.environ.get("SCRATCH", f"/scratch/{os.environ.get('USER', 'user')}"))
    if not scratch.exists():
        scratch = Path(tempfile.gettempdir())
    return scratch / "hsm-smoke"


def write_train_script(work: Path) -> Path:
    work.mkdir(parents=True, exist_ok=True)
    train = work / "train.py"
    train.write_text(TRAIN_SCRIPT)
    train.chmod(0o755)
    return train


def build_source(script_path: str, project_dir: str) -> SlurmComputeSource:
    return SlurmComputeSource(
        name="s3it_smoke",
        python_path="python3",
        script_path=script_path,
        project_dir=project_dir,
        default_spec=DEFAULT_SPEC,
        qos_whitelist=QOS_WHITELIST,
    )


async def dry_render(src: SlurmComputeSource) -> int:
    """Print the resolved spec + #SBATCH directives without submitting."""
    spec = src._effective_spec(None)
    print("Effective ResourceSpec:")
    for k, v in spec.to_dict().items():
        if v in (None, [], {}, ()):
            continue
        print(f"  {k:18s} = {v!r}")
    print()
    print("Rendered #SBATCH directives:")
    print(src._render_directives(spec))
    print()
    print("(No submission performed; --dry-run.)")
    return 0


async def submit_and_wait(
    args: argparse.Namespace,
    src: SlurmComputeSource,
    sweep_dir: Path,
) -> int:
    if not await src.setup(sweep_dir, "smoke_test"):
        print(
            "setup() failed — sbatch / squeue / sinfo not on PATH? "
            "Are you on a Slurm host?",
            file=sys.stderr,
        )
        return 2

    if args.array > 0:
        params_list = [{"seed": s} for s in range(args.array)]
        job_ids = await src.submit_batch(params_list, "smoke_test", mode="array")
        print(f"Submitted array job {job_ids[0]} ({args.array} tasks)")
    else:
        job_id = await src.submit_job({"seed": 42}, "smoke_task_001", "smoke_test")
        print(f"Submitted single job {job_id}")

    print(f"\nGenerated scripts:  {sweep_dir / 'scripts'}")
    print(f"Logs:               {sweep_dir / 'logs'}")
    print(f"Per-task dirs:      {sweep_dir / 'tasks'}")
    print("Watch in queue:     squeue -u $USER")

    if args.no_wait:
        print("\n--no-wait set; exiting without polling.")
        return 0

    print(f"\nPolling every {args.poll_interval}s until all jobs finish ...")
    final = await src.wait_for_all(
        poll_interval=args.poll_interval,
        on_progress=lambda completed, total: print(f"  {completed}/{total} done"),
    )
    print(f"\nFinal statuses: {final}")

    info_files = sorted(sweep_dir.joinpath("tasks").glob("*/task_info.txt"))
    if info_files:
        print(f"\nFound {len(info_files)} task_info.txt file(s). First one:")
        print(f"--- {info_files[0]} ---")
        print(info_files[0].read_text())
    else:
        print("\nNo task_info.txt files found — jobs may still be initialising.")
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print effective ResourceSpec + #SBATCH directives; don't submit.",
    )
    p.add_argument(
        "--array",
        type=int,
        default=0,
        metavar="N",
        help="Submit a Slurm array of N tasks (default: 0 = single job).",
    )
    p.add_argument(
        "--no-wait",
        action="store_true",
        help="Submit and exit; don't poll for completion.",
    )
    p.add_argument(
        "--poll-interval",
        type=float,
        default=15.0,
        help="squeue polling interval in seconds (default: 15).",
    )
    p.add_argument(
        "--keep",
        action="store_true",
        help="Keep existing sweep dir contents (default: wipe before running).",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    if args.dry_run:
        src = build_source(
            script_path="<train.py path filled in at submit time>",
            project_dir="<work dir filled in at submit time>",
        )
        return asyncio.run(dry_render(src))

    work = workdir()
    print(f"Working in: {work}")
    train = write_train_script(work)
    sweep_dir = work / "sweep_test"
    if sweep_dir.exists() and not args.keep:
        shutil.rmtree(sweep_dir)
    sweep_dir.mkdir(parents=True, exist_ok=True)

    src = build_source(str(train), str(work))
    return asyncio.run(submit_and_wait(args, src, sweep_dir))


if __name__ == "__main__":
    sys.exit(main())
