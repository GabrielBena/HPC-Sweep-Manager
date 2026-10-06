"""Fair share of a shared Slurm account (tracker S-4, field report #10)."""

from __future__ import annotations

import asyncio
import math
import os
import stat

from click.testing import CliRunner

from hpc_sweep_manager.cli import queue as queue_cli
from hpc_sweep_manager.cli.main import cli
from hpc_sweep_manager.core.hpc.fair_share import parse_share, probe_share

# What the probe printed on uzh on 2026-10-06 (shape; names anonymised).
HOT = """@@ME
me
@@SSHARE
lab||0.002000|5000|0.027400
lab|me|1|4950|0.027000
lab|bob|1|50|0.000400
@@RUN
bob|8|cpu-1
bob|8|gpu-2
@@PEND
bob|ReqNodeNotAvail, Reserved for maintenance
@@GPUN
gpu-1
gpu-2
gpu-2
"""


class TestParseShare:
    def test_the_hot_account_of_2026_10_06(self):
        share = parse_share(HOT, "lab")
        assert share.me == "me"
        assert math.isclose(share.ratio, 13.7)
        assert math.isclose(share.my_usage, 0.99)
        assert share.running == {"bob": (2, 16)}
        assert share.gpu_nodes == ("gpu-1", "gpu-2")
        assert share.hot  # over 2x its share
        assert share.summary().startswith("lab: usage 13.7x its fair share; me = 99% of")
        assert "0 of 16 running CPUs" in share.summary()

    def test_a_co_worker_waiting_on_priority_is_hot_even_under_share(self):
        out = "@@ME\nme\n@@SSHARE\nlab||0.5|1|0.1\n@@PEND\nbob|Priority\nme|Priority\n"
        share = parse_share(out, "lab")
        assert share.ratio < 2 and share.hot
        assert share.waiting == {"bob": {"Priority"}}  # our own pending jobs don't count

    def test_a_quiet_account_is_not_hot(self):
        share = parse_share(
            "@@ME\nme\n@@SSHARE\nlab||0.5|1|0.1\n@@RUN\nme|4|gpu-1\n@@GPUN\ngpu-1\n", "lab"
        )
        assert not share.hot
        assert share.mine_on_gpu == 1


def test_the_probe_runs_the_four_commands_in_one_shell(tmp_path, monkeypatch):
    replies = {
        "id": "echo me",
        "sshare": 'echo "lab||0.5|1|0.1"',
        "squeue": 'case "$*" in *"-t R"*) echo "me|4|n1";; *) echo "bob|Priority";; esac',
        "sinfo": 'echo "n1 (null)"; echo "g1 gpu:A100:4"',
    }
    for name, body in replies.items():
        stub = tmp_path / name
        stub.write_text(f"#!/bin/bash\n{body}\n")
        stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    share = asyncio.run(probe_share("lab", "standard"))
    assert (share.me, share.running, share.gpu_nodes) == ("me", {"me": (1, 4)}, ("g1",))
    assert share.hot  # bob waits on priority


def test_queue_share_exits_3_when_the_account_is_hot(monkeypatch):
    async def fake_probe(account, partition=""):
        return parse_share(HOT, account)

    monkeypatch.setattr(queue_cli, "probe_share", fake_probe)
    monkeypatch.setattr(queue_cli, "_resolve_queue_target", lambda alias, console: None)
    result = CliRunner().invoke(cli, ["queue", "share", "--account", "lab"])
    assert result.exit_code == 3, result.output
    assert "13.7x its fair share" in result.output
