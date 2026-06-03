"""``hsm docs`` — point users (and their agents) at the documentation.

Docs live in the HSM repo, not in the installed package (only ``templates/*.j2``
is packaged), so a ``pip install`` consumer has no local ``docs/``. This command
prints the canonical GitHub URLs, plus the on-disk path when HSM is running from
a source / editable checkout. It's the answer to "where are the docs?" for both
humans and agents that can only see the consumer project.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import click
from rich.console import Console

_REPO = "https://github.com/GabrielBena/HPC-Sweep-Manager"
_DOCS_BASE = f"{_REPO}/tree/main/docs/user_guide"
_PAGES = [
    ("getting_started.md", "Quickstart"),
    ("HPC_EXECUTION.md", "Native Slurm + the typed slurm: block"),
    ("SSH_EXECUTION.md", "SSH + SSH-Slurm remotes (incl. hsm sweep collect)"),
    ("MULTI_CLUSTER.md", "Distributed / multi-cluster fan-out"),
    ("QUEUE.md", "Cluster queue inspection (hsm queue ...)"),
]


def _local_docs_dir() -> Optional[Path]:
    """Return the on-disk docs/user_guide dir for source/editable installs, else None."""
    import hpc_sweep_manager

    # Source layout: <repo>/src/hpc_sweep_manager/__init__.py → <repo>/docs/user_guide
    cand = Path(hpc_sweep_manager.__file__).resolve().parents[2] / "docs" / "user_guide"
    return cand if cand.is_dir() else None


@click.command()
@click.pass_context
def docs(ctx: click.Context) -> None:
    """Show where the HSM documentation lives (URLs + local path if present)."""
    console: Console = (ctx.obj or {}).get("console") or Console()

    console.print(f"[bold]HSM documentation[/bold] — {_REPO}\n")
    local = _local_docs_dir()
    if local:
        console.print(f"[green]Local copy (this checkout):[/green] {local}")
    else:
        console.print(
            "[dim]No local docs/ (pip-installed) — use the URLs below, or "
            "clone the repo for offline reading.[/dim]"
        )

    console.print("\n[bold]Guides:[/bold]")
    for fname, desc in _PAGES:
        console.print(f"  • {desc}")
        console.print(f"      {_DOCS_BASE}/{fname}")
        if local:
            console.print(f"      {local / fname}")

    console.print(
        "\n[dim]In your project: sweeps/README.md is a self-contained "
        "quickstart. Architecture + gotchas live in CLAUDE.md in the HSM "
        "repo.[/dim]"
    )
