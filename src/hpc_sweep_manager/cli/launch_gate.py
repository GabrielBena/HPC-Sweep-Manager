"""The fair-share check before a Slurm launch (tracker S-4; Gabriel's rule, 2026-10-06).

The account's load is printed on every Slurm launch. When it is hot (over 2x its share, or a
co-worker waiting on priority), or the probe can't tell, HSM asks: throttle the array and go
(the default, also taken with no terminal), launch as asked (``--force`` skips the question),
wait for the account to cool down (re-checked every 30 min, at most 12 h), or cancel.
"""

from __future__ import annotations

import asyncio
import sys
import time
from typing import Any

import click
from rich.console import Console

from ..core.common.resource_spec import ResourceSpec
from ..core.hpc.fair_share import DEFAULT_THROTTLE, Share, probe_share

WAIT_EVERY_S, WAIT_AT_MOST_S = 1800, 12 * 3600


async def _probe(source: Any, spec: ResourceSpec) -> Share:
    if source.source_type != "ssh_slurm_remote":
        return await probe_share(spec.account, spec.partition or "")
    from ..core.remote.discovery import create_ssh_connection

    conn = await create_ssh_connection(source.host, source.ssh_key, source.ssh_port)
    try:
        return await probe_share(spec.account, spec.partition or "", conn=conn)
    finally:
        conn.close()


def _ask_why(source: Any, spec: ResourceSpec, console: Console) -> str | None:
    """Probe and print the account's load: ``None`` when it is known and cool, else why to ask."""
    try:
        share = asyncio.run(_probe(source, spec))
    except Exception as e:  # noqa: BLE001 — a failed probe asks, it never blocks a launch
        console.print(f"[yellow]Fair-share check failed: {e}[/yellow]")
        return "the account's load is unknown"
    console.print(f"[cyan]{share.summary()}[/cyan]")
    if share.hot:
        return "the account is hot: co-workers lose priority to every task we run"
    return None if share.known else "the account's load is unknown"


def fair_share_gate(
    source: Any,
    effective: ResourceSpec,
    spec: ResourceSpec,
    *,
    array: bool,
    force: bool,
    dry_run: bool,
    console: Console,
) -> ResourceSpec | None:
    """The spec to launch with (throttled if the user chose so), or ``None`` to cancel."""
    if source.source_type not in ("slurm", "ssh_slurm_remote") or not effective.account:
        return spec
    why = _ask_why(source, effective, console)
    if why is None or force or dry_run:
        return spec
    n = min(effective.array_throttle or DEFAULT_THROTTLE, DEFAULT_THROTTLE)
    console.print(f"[yellow]Asking first: {why}.[/yellow]")
    choice = "t"
    if sys.stdin.isatty():
        choice = click.prompt(
            f"[t] throttle to {n} at once and go, [a] launch as asked, [w] wait, [c] cancel",
            type=click.Choice(["t", "a", "w", "c"]),
            default="t",
        )
    if choice == "w":
        for waited in range(WAIT_EVERY_S, WAIT_AT_MOST_S + 1, WAIT_EVERY_S):
            time.sleep(WAIT_EVERY_S)
            console.print(f"[dim]after {waited // 60} min:[/dim]")
            if _ask_why(source, effective, console) is None:
                return spec
        choice = "t"  # still hot (or unknown) after the longest wait: go, throttled
    if choice == "c":
        return None
    if choice == "a":
        return spec
    if not array:
        console.print(
            "[yellow]Individual submissions can't be throttled; use --mode array.[/yellow]"
        )
        return spec
    console.print(f"[cyan]Throttled: at most {n} tasks at once (--array=1-K%{n}).[/cyan]")
    return spec.merge(ResourceSpec(array_throttle=n))
