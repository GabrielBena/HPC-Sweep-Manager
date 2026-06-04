"""Tests for the `hsm queue` CLI plumbing: transport resolution, the
local/SSH driver, and the renderers.

Helpers are exercised directly with a captured Console (house style — avoids
the Click-8/CliRunner interaction documented in CLAUDE.md). The SSH path runs
against a FakeConn via a monkeypatched ``create_ssh_connection``.
"""

from __future__ import annotations

import io
from typing import List, Optional

import click
import pytest
from rich.console import Console

from hpc_sweep_manager.cli import queue as queue_cli
from hpc_sweep_manager.cli.queue import (
    _remote_params,
    _render_gpus,
    _render_mine,
    _render_mine_grouped,
    _render_position_all,
    _render_position_single,
    _resolve_queue_target,
    _run_queue_command,
)
from hpc_sweep_manager.core.common.config import HSMConfig
from hpc_sweep_manager.core.hpc.scheduler_queue import (
    JobGroup,
    QueueJob,
    enrich_groups_with_accounting,
    group_jobs_by_array,
)


def _console_buf() -> tuple[Console, io.StringIO]:
    buf = io.StringIO()
    return Console(file=buf, width=200, no_color=True), buf


def _job(job_id: str, state: str = "PENDING", gpu_count: int = 1,
         gpu_type: Optional[str] = None, task_count: int = 1,
         reason: str = "(Priority)") -> QueueJob:
    return QueueJob(
        job_id=job_id, name=f"name_{job_id}", user="gbena", state=state,
        reason=reason, partition="standard", tres_per_node="",
        expected_start="N/A", priority=0, gpu_count=gpu_count,
        gpu_type=gpu_type, task_count=task_count,
    )


def _config(remotes: dict) -> HSMConfig:
    return HSMConfig({"distributed": {"remotes": remotes}})


# --------------------------------------------------------- transport resolution


class TestResolveQueueTarget:
    def test_explicit_alias_unregistered_is_bare_ssh_alias(self, monkeypatch):
        monkeypatch.setattr(queue_cli.HSMConfig, "load", classmethod(lambda cls, *a, **k: None))
        console, _ = _console_buf()
        target = _resolve_queue_target("uzh", console)
        assert target == {"alias": "uzh", "host": "uzh", "ssh_key": None, "ssh_port": None}

    def test_explicit_alias_registered_resolves_fields(self, monkeypatch):
        cfg = _config({"uzh": {"backend": "slurm", "host": "cluster.example.ch",
                               "ssh_key": "/k", "ssh_port": 2222}})
        monkeypatch.setattr(queue_cli.HSMConfig, "load", classmethod(lambda cls, *a, **k: cfg))
        console, _ = _console_buf()
        target = _resolve_queue_target("uzh", console)
        assert target["host"] == "cluster.example.ch"
        assert target["ssh_key"] == "/k"
        assert target["ssh_port"] == 2222

    def test_local_squeue_wins_without_alias(self, monkeypatch):
        monkeypatch.setattr(queue_cli, "slurm_available", lambda: True)
        console, _ = _console_buf()
        assert _resolve_queue_target(None, console) is None

    def test_sole_slurm_remote_auto_used_with_note(self, monkeypatch):
        monkeypatch.setattr(queue_cli, "slurm_available", lambda: False)
        cfg = _config({
            "uzh": {"backend": "slurm"},
            "box": {"backend": "ssh"},  # not a candidate
        })
        monkeypatch.setattr(queue_cli.HSMConfig, "load", classmethod(lambda cls, *a, **k: cfg))
        console, buf = _console_buf()
        target = _resolve_queue_target(None, console)
        assert target["alias"] == "uzh"
        assert "using remote 'uzh'" in buf.getvalue()

    def test_no_candidates_errors_with_guidance(self, monkeypatch):
        monkeypatch.setattr(queue_cli, "slurm_available", lambda: False)
        monkeypatch.setattr(queue_cli.HSMConfig, "load", classmethod(lambda cls, *a, **k: None))
        console, _ = _console_buf()
        with pytest.raises(click.ClickException, match="--remote"):
            _resolve_queue_target(None, console)

    def test_several_candidates_error_lists_them(self, monkeypatch):
        monkeypatch.setattr(queue_cli, "slurm_available", lambda: False)
        cfg = _config({"uzh": {"backend": "slurm"}, "csc": {"backend": "slurm"}})
        monkeypatch.setattr(queue_cli.HSMConfig, "load", classmethod(lambda cls, *a, **k: cfg))
        console, _ = _console_buf()
        with pytest.raises(click.ClickException, match="csc, uzh"):
            _resolve_queue_target(None, console)

    def test_remote_params_empty_block_tolerated(self):
        # `uzh:` with a null body in yaml → remotes["uzh"] is None.
        assert _remote_params("uzh", _config({"uzh": None}))["host"] == "uzh"


# ----------------------------------------------------------------- SSH driver


class _Result:
    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = ""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class FakeConn:
    def __init__(self):
        self.run_calls: List[str] = []
        self.closed = False
        self._responder: List[tuple] = []

    def add(self, sub: str, res: _Result) -> None:
        self._responder.append((sub, res))

    async def run(self, cmd: str, *, input: Optional[str] = None, check: bool = False):
        self.run_calls.append(cmd)
        for i, (sub, res) in enumerate(self._responder):
            if sub in cmd:
                del self._responder[i]
                return res
        return _Result(0, "")

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        pass


_ROW = "\t".join(
    ["3703585_14", "sweep_x_array", "gbena", "RUNNING", "u24-chaiam0-615",
     "standard", "gres/gpu:A100:1", "2026-06-04T09:05:27", "106515"]
)

_TARGET = {"alias": "uzh", "host": "uzh", "ssh_key": None, "ssh_port": None}


def _patch_connection(monkeypatch, conn: FakeConn) -> dict:
    """Route cli.queue's create_ssh_connection import to a FakeConn."""
    calls = {}

    async def fake_create(host, ssh_key=None, ssh_port=None):
        calls["host"], calls["ssh_key"], calls["ssh_port"] = host, ssh_key, ssh_port
        return conn

    monkeypatch.setattr(
        "hpc_sweep_manager.core.remote.discovery.create_ssh_connection", fake_create
    )
    return calls


class TestRunQueueCommandRemote:
    def test_remote_fetch_renders_and_closes(self, monkeypatch):
        conn = FakeConn()
        conn.add("whoami", _Result(0, "gbena\n"))
        conn.add("squeue", _Result(0, _ROW + "\n"))
        calls = _patch_connection(monkeypatch, conn)
        console, buf = _console_buf()

        async def gather(q):
            user = await q.whoami()
            return user, await q.list_user_jobs(user)

        _run_queue_command(
            console, _TARGET, gather,
            lambda data: _render_mine(console, data[0], data[1]),
        )
        out = buf.getvalue()
        assert calls["host"] == "uzh"
        assert "3703585_14" in out
        assert "1×A100" in out
        assert conn.closed  # connection released even on the happy path

    def test_remote_squeue_failure_raises_click_error(self, monkeypatch):
        """A broken remote must exit non-zero with a message — silent empty
        tables are the failure mode this whole feature exists to kill."""
        conn = FakeConn()
        conn.add("squeue", _Result(127, "", "bash: squeue: command not found"))
        _patch_connection(monkeypatch, conn)
        console, _ = _console_buf()

        async def gather(q):
            return await q.list_user_jobs("gbena")

        with pytest.raises(click.ClickException, match="rc=127"):
            _run_queue_command(console, _TARGET, gather, lambda data: None)
        assert conn.closed  # released on the error path too

    def test_connection_failure_names_the_host(self, monkeypatch):
        async def fake_create(host, ssh_key=None, ssh_port=None):
            raise OSError("Connection refused")

        monkeypatch.setattr(
            "hpc_sweep_manager.core.remote.discovery.create_ssh_connection", fake_create
        )
        console, _ = _console_buf()

        async def gather(q):  # pragma: no cover - never reached
            return None

        with pytest.raises(click.ClickException, match="uzh.*Connection refused"):
            _run_queue_command(console, _TARGET, gather, lambda data: None)

    def test_watch_reuses_single_connection(self, monkeypatch):
        """The headline watch-mode claim: ONE SSH connection across refresh
        cycles. A regression to reconnect-per-cycle (the `remote health
        --watch` anti-pattern) must fail here."""
        conn = FakeConn()
        created = {"n": 0}

        async def fake_create(host, ssh_key=None, ssh_port=None):
            created["n"] += 1
            return conn

        monkeypatch.setattr(
            "hpc_sweep_manager.core.remote.discovery.create_ssh_connection", fake_create
        )
        console, buf = _console_buf()
        cycles = {"n": 0}

        async def gather(q):
            return await q.list_user_jobs("gbena")

        def render(data):
            cycles["n"] += 1
            if cycles["n"] >= 3:
                raise KeyboardInterrupt  # stand-in for the user's Ctrl+C

        # refresh=0 keeps the test instant; user input is gated to >=1 by
        # the --refresh IntRange at the Click layer.
        _run_queue_command(console, _TARGET, gather, render, watch=True, refresh=0)
        assert cycles["n"] == 3
        assert created["n"] == 1  # one handshake total, not one per cycle
        assert conn.closed
        assert "stopped" in buf.getvalue()

    def test_local_transport_used_when_target_none(self, monkeypatch):
        seen = {}

        class _FakeLocal:
            async def whoami(self):
                return "gbena"

            async def list_user_jobs(self, user):
                seen["user"] = user
                return []

        monkeypatch.setattr(queue_cli, "_LocalQueueAsync", _FakeLocal)
        console, buf = _console_buf()

        async def gather(q):
            user = await q.whoami()
            return user, await q.list_user_jobs(user)

        _run_queue_command(
            console, None, gather,
            lambda data: _render_mine(console, data[0], data[1]),
        )
        assert seen["user"] == "gbena"
        assert "No jobs in queue" in buf.getvalue()


# ------------------------------------------------------------------- renderers


class TestRenderMine:
    def test_collapsed_array_shows_task_count(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)  # no sweeps/outputs → empty sweep index
        console, buf = _console_buf()
        _render_mine(console, "gbena", [
            _job("3710878_[690-1920]", task_count=1231, gpu_count=0),
            _job("3703585_14", state="RUNNING", gpu_type="A100"),
        ])
        out = buf.getvalue()
        assert "×1231" in out
        assert "2 queue rows · 1232 tasks" in out

    def test_sweep_linkage_from_manifest(self, tmp_path, monkeypatch):
        """SSH-Slurm sweeps have no submission_summary.txt — job ids must be
        picked up from .hsm_manifest.json so the Sweep column populates on
        the workstation that drove the sweep."""
        import json as _json

        sweep_dir = tmp_path / "sweeps" / "outputs" / "sweep_x"
        sweep_dir.mkdir(parents=True)
        (sweep_dir / ".hsm_manifest.json").write_text(
            _json.dumps({"sweep_id": "sweep_x", "job_ids": ["3710878"]})
        )
        monkeypatch.chdir(tmp_path)
        console, buf = _console_buf()
        _render_mine(console, "gbena", [_job("3710878_[1-5]", task_count=5, gpu_count=0)])
        assert "sweep_x" in buf.getvalue()


class TestRenderPosition:
    def test_single_exact_match(self):
        console, buf = _console_buf()
        _render_position_single(console, "p2", [_job("p1"), _job("p2"), _job("p3")])
        assert "position 2 / 3" in buf.getvalue()

    def test_single_array_base_matches_first_task(self):
        console, buf = _console_buf()
        pending = [_job("9_1"), _job("3703585_19"), _job("3703585_20")]
        _render_position_single(console, "3703585", pending)
        out = buf.getvalue()
        assert "2 pending GPU task(s)" in out
        assert "first at position 2 / 3" in out

    def test_single_not_found(self):
        console, buf = _console_buf()
        _render_position_single(console, "404", [_job("p1")])
        assert "not found" in buf.getvalue()

    def test_all_notes_cpu_only_pending(self):
        console, buf = _console_buf()
        my_jobs = [_job("3710878_[690-1920]", gpu_count=0, task_count=1231)]
        _render_position_all(console, "gbena", my_jobs, [])
        out = buf.getvalue()
        assert "No pending GPU jobs" in out
        assert "1231 pending task(s) are CPU-only" in out

    def test_all_positions_arrays_counted_per_task(self):
        console, buf = _console_buf()
        # Collapsed view of my pending array + the -r-expanded pending queue.
        my_jobs = [_job("3703585_[19-22]", gpu_type="A100", task_count=4)]
        pending = [_job("9_1"), *[_job(f"3703585_{i}", gpu_type="A100") for i in (19, 20, 21, 22)]]
        _render_position_all(console, "gbena", my_jobs, pending)
        out = buf.getvalue()
        assert "4 task(s) of yours / 5 total pending GPU tasks" in out
        assert "2 / 5" in out  # first task of the array sits at position 2
        assert "×4" in out
        assert "QOSMaxJobsPerUserLimit" in out  # legend mentions the QoS cap


def _grp(base: str, running: int = 0, pending: int = 0, completed=None,
         failed=None, total=None, gpu_type=None, is_array: bool = True,
         nodes: tuple = (), reason: str = "") -> JobGroup:
    return JobGroup(
        base_id=base, name=f"name_{base}", user="gbena", partition="standard",
        gpu_count=1 if gpu_type else 0, gpu_type=gpu_type, is_array=is_array,
        running=running, pending=pending, nodes=nodes, reason=reason,
        completed=completed, failed=failed, total=total,
    )


class TestRenderMineGrouped:
    def test_failed_surfaced_with_progress_and_footer(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        console, buf = _console_buf()
        _render_mine_grouped(console, "gbena", [
            _grp("3703585", running=9, pending=4, completed=8, failed=1,
                 total=22, gpu_type="A100", nodes=("a", "b", "c")),
        ])
        out = buf.getvalue()
        assert "✓8" in out
        assert "✗1" in out  # the whole point: failed tasks are in your face
        assert "9/22" in out  # (✓8 + ✗1) / sacct total
        assert "3 nodes" in out
        assert "1 FAILED" in out  # footer aggregate
        assert "▰" in out  # progress bar rendered

    def test_zero_failed_omitted(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        console, buf = _console_buf()
        _render_mine_grouped(console, "gbena", [
            _grp("1", running=5, completed=5, failed=0, total=10),
        ])
        out = buf.getvalue()
        assert "✗" not in out
        assert "FAILED" not in out
        assert "5/10" in out

    def test_no_accounting_falls_back_to_manifest_total(self, tmp_path, monkeypatch):
        """sacct unavailable → finished = manifest total − in-queue."""
        import json as _json

        sweep_dir = tmp_path / "sweeps" / "outputs" / "sweep_y"
        sweep_dir.mkdir(parents=True)
        (sweep_dir / ".hsm_manifest.json").write_text(
            _json.dumps({"sweep_id": "sweep_y", "job_ids": ["77"], "num_tasks": 10})
        )
        monkeypatch.chdir(tmp_path)
        console, buf = _console_buf()
        _render_mine_grouped(console, "gbena", [_grp("77", running=2, pending=3)])
        out = buf.getvalue()
        assert "5/10" in out  # 10 total − 5 in queue = 5 finished (✓/✗ unknown)
        assert "sweep_y" in out
        # No ✓N/✗N counts in the Tasks cell (the footer's "✓/✗ unavailable"
        # explainer is expected to mention the glyphs themselves).
        import re as _re

        assert not _re.search(r"[✓✗]\d", out)
        assert "no accounting data" in out

    def test_no_accounting_no_total_shows_dash(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        console, buf = _console_buf()
        _render_mine_grouped(console, "gbena", [_grp("9", running=1)])
        out = buf.getvalue()
        assert "—" in out  # never a faked total
        assert "no accounting data" in out

    def test_single_job_and_footer_counts(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        console, buf = _console_buf()
        _render_mine_grouped(console, "gbena", [
            _grp("1", running=3, pending=7, completed=2, failed=0, total=12),
            _grp("2", pending=1, is_array=False, reason="(Resources)"),
        ])
        out = buf.getvalue()
        assert "1 array(s)" in out
        assert "1 single job(s)" in out
        assert "11 task(s) in queue (3 running, 8 pending)" in out
        assert "(Resources)" in out

    def test_remote_grouped_end_to_end(self, monkeypatch, tmp_path):
        """Full pipeline over FakeConn: squeue rows → groups → sacct → render."""
        monkeypatch.chdir(tmp_path)
        conn = FakeConn()
        conn.add("whoami", _Result(0, "gbena\n"))
        conn.add("squeue", _Result(0, _ROW + "\n"))  # one RUNNING A100 task
        conn.add(
            "sacct",
            _Result(0, "3703585_1|COMPLETED\n3703585_11|FAILED\n3703585_14|RUNNING\n"),
        )
        _patch_connection(monkeypatch, conn)
        console, buf = _console_buf()

        async def gather(q):
            user = await q.whoami()
            groups = group_jobs_by_array(await q.list_user_jobs(user))
            states = await q.sacct_job_states([g.base_id for g in groups])
            return user, enrich_groups_with_accounting(groups, states)

        _run_queue_command(
            console, _TARGET, gather,
            lambda d: _render_mine_grouped(console, d[0], d[1]),
        )
        out = buf.getvalue()
        assert "3703585" in out
        assert "✓1" in out and "✗1" in out
        assert "2/3" in out  # 2 terminal of 3 total tasks


class TestRenderGpus:
    def test_mine_annotation_is_task_weighted(self):
        console, buf = _console_buf()
        summary = {"A100": {"RUNNING": 7, "PENDING": 4}}
        mine = [
            _job("a_1", state="RUNNING", gpu_type="A100"),
            _job("a_[19-22]", state="PENDING", gpu_type="A100", task_count=4),
        ]
        _render_gpus(console, summary, mine)
        out = buf.getvalue()
        assert "A100" in out
        assert "1/4" in out  # 1 running, 4 pending (task-weighted, not 1/1)

    def test_empty_summary_message(self):
        console, buf = _console_buf()
        _render_gpus(console, {}, None)
        assert "No GPU jobs in queue" in buf.getvalue()
