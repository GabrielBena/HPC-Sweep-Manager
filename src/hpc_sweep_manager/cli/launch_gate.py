"""The fair-share check before a Slurm launch (tracker S-4; Gabriel's rule, 2026-10-06).

The account's load is printed on every Slurm launch (not a dry run, not ``--mode distributed``).
When it is hot (over 2x its share, or a co-worker waiting on priority), or the probe can't tell,
HSM asks: throttle the array to 50 at once and go (the default, also taken with no terminal),
launch as asked (``--force`` skips the question), wait for the account to cool down (re-checked
every 30 min, at most 12 h), or cancel.
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

WAIT_EVERY_S, WAIT_AT_MOST_S, PROBE_TIMEOUT_S = 1800, 12 * 3600, 90


async def _probe(source: Any, spec: ResourceSpec) -> Share:
    assert spec.account  # the gate only probes a spec that names its account
    if source.source_type != "ssh_slurm_remote":
        return await probe_share(spec.account, spec.partition or "")
    conn = await source._open_connection()
    try:
        return await probe_share(spec.account, spec.partition or "", conn=conn)
    finally:
        conn.close()
        await conn.wait_closed()


def _ask_why(source: Any, spec: ResourceSpec, console: Console) -> str | None:
    """Probe and print the account's load: ``None`` when it is known and cool, else why to ask."""
    try:
        share = asyncio.run(asyncio.wait_for(_probe(source, spec), PROBE_TIMEOUT_S))
    except Exception as e:  # noqa: BLE001 — a failed probe asks, it never blocks a launch
        console.print(f"[yellow]Fair-share check failed: {e or type(e).__name__}[/yellow]")
        return "the account's load is unknown"
    console.print(f"[cyan]{share.summary()}[/cyan]")
    if share.hot:
        return "the account is hot: co-workers lose priority to every task we run"
    return None if share.known else "the account's load is unknown"


def fair_share_gate(
    source: Any, spec: ResourceSpec, *, array: bool, force: bool, dry_run: bool, console: Console
) -> ResourceSpec | None:
    """The spec to launch with, or ``None`` to cancel.

    A throttle the user takes goes into the returned spec *and* ``source.default_spec``: the
    latter is what a resumable chain's manifest keeps for ``hsm sweep advance``.
    """
    asked = getattr(source, "default_spec", None)
    if source.source_type not in ("slurm", "ssh_slurm_remote") or not (asked and asked.account):
        return spec
    if dry_run:  # stays offline; a real launch checks
        console.print(
            "[dim]A launch checks the account's fair share first (hsm queue share).[/dim]"
        )
        return spec
    why = _ask_why(source, asked, console)
    if why is None or force:
        return spec
    console.print(f"[yellow]Careful: {why}.[/yellow]")
    menu = {
        "t": f"throttle to {DEFAULT_THROTTLE} at once and go",
        "a": "launch as asked",
        "w": "wait",
        "c": "cancel",
    }
    if not array:
        console.print(
            "[yellow]Individual submissions can't be throttled; --mode array can.[/yellow]"
        )
    if not array or (asked.array_throttle and asked.array_throttle <= DEFAULT_THROTTLE):
        del menu["t"]  # nothing to throttle, or already throttled at least as hard
    choice = default = next(iter(menu))
    if sys.stdin and sys.stdin.isatty():
        choice = click.prompt(
            ", ".join(f"[{k}] {v}" for k, v in menu.items()),
            type=click.Choice(list(menu)),
            default=default,
        )
    else:
        console.print(f"[yellow]No terminal to ask, so: {menu[default]}.[/yellow]")
    if choice == "w":
        for waited in range(WAIT_EVERY_S, WAIT_AT_MOST_S + 1, WAIT_EVERY_S):
            time.sleep(WAIT_EVERY_S)
            console.print(f"[dim]after {waited // 60} min:[/dim]")
            if _ask_why(source, asked, console) is None:
                return spec
        choice = default  # still hot (or unknown) after the longest wait
    if choice == "c":
        return None
    if choice == "a":
        return spec
    throttle = ResourceSpec(array_throttle=DEFAULT_THROTTLE)
    source.default_spec = asked.merge(throttle)
    console.print(
        f"[cyan]Throttled: at most {DEFAULT_THROTTLE} tasks at once (was "
        f"{asked.array_throttle or 'unthrottled'}); --force launches as asked.[/cyan]"
    )
    return spec.merge(throttle)
