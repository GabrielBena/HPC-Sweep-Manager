"""The fair-share prompt before a Slurm launch (tracker S-4; Gabriel's rule, 2026-10-06)."""

from __future__ import annotations

import io
from types import SimpleNamespace

import pytest
from rich.console import Console

from hpc_sweep_manager.cli import launch_gate
from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
from hpc_sweep_manager.core.hpc.fair_share import Share

HOT = Share("lab", "me", ratio=13.7, my_usage=0.99)
COOL = Share("lab", "me", ratio=0.5, my_usage=0.1)
SLURM = SimpleNamespace(source_type="ssh_slurm_remote")
SPEC = ResourceSpec(account="lab")


def gate(monkeypatch, shares, *, tty=False, answer="t", spec=SPEC, array=True, force=False):
    """Run the gate against scripted probe results; return (spec, console output)."""
    shares = list(shares)

    async def probe(source, spec):
        if isinstance(shares[0], Exception):
            raise shares.pop(0)
        return shares.pop(0)

    monkeypatch.setattr(launch_gate, "_probe", probe)
    monkeypatch.setattr(launch_gate.sys.stdin, "isatty", lambda: tty, raising=False)
    monkeypatch.setattr(launch_gate.click, "prompt", lambda *a, **k: answer)
    monkeypatch.setattr(launch_gate.time, "sleep", lambda s: None)
    out = io.StringIO()
    got = launch_gate.fair_share_gate(
        SLURM,
        spec,
        ResourceSpec(),
        array=array,
        force=force,
        dry_run=False,
        console=Console(file=out, width=300),
    )
    return got, out.getvalue()


def test_no_account_no_probe(monkeypatch):
    assert gate(monkeypatch, [], spec=ResourceSpec())[0] == ResourceSpec()


def test_a_cool_account_prints_the_line_and_goes(monkeypatch):
    got, out = gate(monkeypatch, [COOL])
    assert got == ResourceSpec() and "0.5x its fair share" in out


def test_hot_without_a_terminal_throttles_and_goes(monkeypatch):
    assert gate(monkeypatch, [HOT])[0].array_throttle == 50


def test_a_lower_configured_throttle_is_kept(monkeypatch):
    spec = ResourceSpec(account="lab", array_throttle=20)
    assert gate(monkeypatch, [HOT], spec=spec)[0].array_throttle == 20


def test_force_launches_as_asked(monkeypatch):
    assert gate(monkeypatch, [HOT], force=True)[0] == ResourceSpec()


@pytest.mark.parametrize(("answer", "expected"), [("a", ResourceSpec()), ("c", None)])
def test_the_prompt(monkeypatch, answer, expected):
    assert gate(monkeypatch, [HOT], tty=True, answer=answer)[0] == expected


def test_wait_launches_as_asked_once_the_account_cools(monkeypatch):
    got, out = gate(monkeypatch, [HOT, HOT, COOL], tty=True, answer="w")
    assert got == ResourceSpec() and "after 60 min" in out


def test_individual_submissions_are_not_throttled(monkeypatch):
    got, out = gate(monkeypatch, [HOT], array=False)
    assert got == ResourceSpec() and "can't be throttled" in out


def test_a_failed_probe_never_blocks_a_launch(monkeypatch):
    got, out = gate(monkeypatch, [OSError("ssh: connect timed out")])
    assert got == ResourceSpec() and "launching as asked" in out
