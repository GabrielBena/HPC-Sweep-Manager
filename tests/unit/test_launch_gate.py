"""The fair-share prompt before a Slurm launch (tracker S-4; Gabriel's rule, 2026-10-06)."""

from __future__ import annotations

import asyncio
import io
import logging
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from rich.console import Console

from hpc_sweep_manager.cli import launch_gate
from hpc_sweep_manager.cli import sweep as sweep_cli
from hpc_sweep_manager.core.common.config import HSMConfig
from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
from hpc_sweep_manager.core.hpc.fair_share import Share

HOT = Share("lab", "me", ratio=13.7, my_usage=0.99)
COOL = Share("lab", "me", ratio=0.5, my_usage=0.1)
UNKNOWN = Share("lab", "me", ratio=float("nan"), my_usage=0.0)


def gate(
    monkeypatch,
    shares,
    *,
    tty=False,
    answer=None,
    throttle=None,
    account="lab",
    source_type="ssh_slurm_remote",
    array=True,
    force=False,
    dry_run=False,
):
    """Run the gate against scripted probe results; return (spec, source, console output).

    A probe beyond the scripted ones, or a prompt with no ``answer``, fails the test.
    """
    shares, prompts = list(shares), []

    async def probe(source, spec):
        share = shares.pop(0)
        if isinstance(share, Exception):
            raise share
        return share

    def prompt(text, **kw):
        assert answer is not None, f"unexpected prompt: {text}"
        prompts.append(list(kw["type"].choices))
        return answer

    monkeypatch.setattr(launch_gate, "_probe", probe)
    monkeypatch.setattr(launch_gate.sys.stdin, "isatty", lambda: tty, raising=False)
    monkeypatch.setattr(launch_gate.click, "prompt", prompt)
    monkeypatch.setattr(launch_gate.time, "sleep", lambda s: None)
    source = SimpleNamespace(
        source_type=source_type,
        default_spec=ResourceSpec(account=account, array_throttle=throttle),
    )
    out = io.StringIO()
    got = launch_gate.fair_share_gate(
        source,
        ResourceSpec(),
        array=array,
        force=force,
        dry_run=dry_run,
        console=Console(file=out, width=300),
    )
    assert not shares, "scripted probes left unused"
    return got, source, out.getvalue() + "".join(map(str, prompts))


@pytest.mark.parametrize(
    "kw", [{"account": None}, {"source_type": "ssh_remote"}, {"source_type": "local"}]
)
def test_no_probe_without_an_account_or_off_slurm(monkeypatch, kw):
    assert gate(monkeypatch, [], **kw)[0] == ResourceSpec()


def test_a_dry_run_stays_offline_and_never_asks(monkeypatch):
    got, _, out = gate(monkeypatch, [], tty=True, dry_run=True)
    assert got == ResourceSpec() and "hsm queue share" in out


def test_a_cool_account_prints_the_line_and_goes(monkeypatch):
    got, source, out = gate(monkeypatch, [COOL])
    assert got == ResourceSpec() and "0.5x its fair share" in out
    assert source.default_spec.array_throttle is None


@pytest.mark.parametrize(("configured", "expected"), [(None, 50), (350, 50), (20, 20)])
def test_hot_without_a_terminal_takes_the_default(monkeypatch, configured, expected):
    got, source, out = gate(monkeypatch, [HOT], throttle=configured)
    # The source keeps it too: a chain's manifest saves default_spec for `hsm sweep advance`.
    assert source.default_spec.array_throttle == expected
    assert (got.array_throttle or configured) == expected and "No terminal to ask" in out


def test_force_launches_as_asked(monkeypatch):
    got, source, _ = gate(monkeypatch, [HOT], tty=True, force=True)
    assert got == ResourceSpec() and source.default_spec.array_throttle is None


@pytest.mark.parametrize(("answer", "expected"), [("a", ResourceSpec()), ("c", None)])
def test_the_prompt(monkeypatch, answer, expected):
    got, _, out = gate(monkeypatch, [HOT], tty=True, answer=answer)
    assert got == expected and "['t', 'a', 'w', 'c']" in out


@pytest.mark.parametrize("kw", [{"array": False}, {"throttle": 20}])
def test_no_throttle_offered_when_it_would_change_nothing(monkeypatch, kw):
    _, _, out = gate(monkeypatch, [HOT], tty=True, answer="a", **kw)
    assert "['a', 'w', 'c']" in out


def test_individual_submissions_launch_as_asked(monkeypatch):
    got, _, out = gate(monkeypatch, [HOT], array=False)
    assert got == ResourceSpec() and "can't be throttled" in out


@pytest.mark.parametrize("probe", [UNKNOWN, OSError("ssh: connect timed out")])
def test_an_unknown_load_asks_and_never_blocks(monkeypatch, probe):
    got, _, out = gate(monkeypatch, [probe])
    assert got.array_throttle == 50 and "load is unknown" in out


def test_a_stalled_probe_times_out(monkeypatch):
    async def stall(source, spec):
        await asyncio.sleep(60)

    monkeypatch.setattr(launch_gate, "PROBE_TIMEOUT_S", 0.01)
    monkeypatch.setattr(launch_gate, "_probe", stall)
    source = SimpleNamespace(source_type="slurm", default_spec=ResourceSpec(account="lab"))
    assert launch_gate._ask_why(source, source.default_spec, Console(file=io.StringIO()))


def test_wait_survives_a_failed_probe_and_needs_a_known_cool_account(monkeypatch):
    shares = [HOT, OSError("ssh: reset"), UNKNOWN, COOL]
    got, _, out = gate(monkeypatch, shares, tty=True, answer="w")
    assert got == ResourceSpec() and "after 90 min" in out


def test_still_hot_after_the_longest_wait_takes_the_default(monkeypatch):
    got, _, _ = gate(monkeypatch, [HOT] * 25, tty=True, answer="w")
    assert got.array_throttle == 50


@pytest.mark.parametrize("answer", [None, "c"])
def test_hsm_sweep_run_throttles_or_cancels(tmp_path, monkeypatch, answer):
    """On a hot account, `hsm sweep run` submits with %50 (no terminal), or not at all (c)."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "_home"))
    (tmp_path / "_home").mkdir()
    (tmp_path / "train.py").write_text("print('hi')\n")
    (tmp_path / "sweeps").mkdir()
    (tmp_path / "sweeps" / "sweep.yaml").write_text("sweep:\n  grid:\n    lr: [0.1, 0.2]\n")
    remote = {"host": "uzh", "backend": "slurm", "spec": {"account": "lab"}}
    (tmp_path / ".hsm").mkdir()
    (tmp_path / ".hsm" / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "paths": {"train_script": str(tmp_path / "train.py")},
                "distributed": {"remotes": {"uzh": remote}},
            }
        )
    )

    async def probe(source, spec):
        return HOT

    seen = {}

    async def run_sweep_async(*, source, spec, **kw):
        seen.update(spec=spec, default=source.default_spec, poll=kw["poll_interval"])
        raise RuntimeError("stop here")

    monkeypatch.setattr(launch_gate, "_probe", probe)
    monkeypatch.setattr(launch_gate.sys.stdin, "isatty", lambda: answer is not None, raising=False)
    monkeypatch.setattr(launch_gate.click, "prompt", lambda *a, **k: answer)
    monkeypatch.setattr(sweep_cli, "run_sweep_async", run_sweep_async)
    with pytest.raises(RuntimeError, match="stop here") if answer is None else nullcontext():
        sweep_cli.run_sweep(
            config_path=Path("sweeps/sweep.yaml"),
            mode="remote",
            dry_run=False,
            count_only=False,
            max_runs=None,
            walltime=None,
            resources=None,
            group=None,
            parallel_jobs=None,
            no_progress=True,
            console=Console(file=io.StringIO(), width=200),
            logger=logging.getLogger("test"),
            hsm_config=HSMConfig.load(),
            remote_alias="uzh",
        )
    if answer == "c":
        assert not seen
    else:
        assert seen["default"].merge(seen["spec"]).array_throttle == 50
        assert seen["poll"] == 60  # a Slurm source polls once a minute (R9)
