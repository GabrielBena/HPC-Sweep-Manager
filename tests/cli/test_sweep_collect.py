"""Tests for `hsm sweep collect` re-attach (#8 Tier 0).

Drives the async helper `_collect_via_manifest` with a fake SSH connection +
fake rsync, so the re-attach → sacct-classify → pull/archive flow runs offline.
"""

from __future__ import annotations

import io
from typing import List, Optional

import pytest
from rich.console import Console

from hpc_sweep_manager.cli.sweep import _collect_via_manifest
from hpc_sweep_manager.core.remote.ssh_slurm_compute_source import SSHSlurmComputeSource


class _Result:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class FakeConn:
    def __init__(self):
        self.run_calls: List[str] = []
        self._responder: List[tuple] = []

    def add(self, sub, res):
        self._responder.append((sub, res))

    async def run(self, cmd, *, input: Optional[str] = None, check: bool = False):
        self.run_calls.append(cmd)
        for i, (sub, res) in enumerate(self._responder):
            if sub in cmd:
                del self._responder[i]
                return res
        return _Result(0, "")

    def close(self):
        pass

    async def wait_closed(self):
        pass


def _manifest(tmp_path, *, job_ids, archive_dir=None):
    return {
        "sweep_id": "sw1",
        "backend": "slurm",
        "name": "uzh",
        "host": "uzh",
        "project_dir": str(tmp_path),
        "remote_root": "~/.hsm/runs",
        "remote_sweep_dir": "/scratch/gbena/hsm-runs/proj/sweeps/sw1",
        "remote_tasks_dir": "/scratch/gbena/hsm-runs/proj/sweeps/sw1/tasks",
        "resolved_archive_dir": archive_dir,
        "archive_dir": archive_dir,
        "archive_on": "completed",
        "keep_remote_on_success": False,
        "job_ids": job_ids,
    }


@pytest.fixture
def patched(monkeypatch):
    conn = FakeConn()
    rsync_calls: List[list] = []

    async def _fake_open(self):
        return conn

    async def _fake_rsync(self, cmd):
        rsync_calls.append(cmd)
        return 0

    monkeypatch.setattr(SSHSlurmComputeSource, "_open_connection", _fake_open)
    monkeypatch.setattr(SSHSlurmComputeSource, "_run_rsync", _fake_rsync)
    return conn, rsync_calls


class TestCollectViaManifest:
    @pytest.mark.asyncio
    async def test_all_completed_pulls_and_cleans(self, tmp_path, patched):
        conn, rsync_calls = patched
        conn.add("sacct", _Result(0, stdout="COMPLETED\n"))
        conn.add("sacct", _Result(0, stdout="COMPLETED\n"))
        conn.add("rm -rf", _Result(0))
        buf = io.StringIO()
        await _collect_via_manifest(
            tmp_path / "sweeps" / "outputs" / "sw1",
            _manifest(tmp_path, job_ids=["1", "2"]),
            Console(file=buf, width=200),
        )
        out = buf.getvalue()
        assert "2 COMPLETED, 0 FAILED" in out
        assert rsync_calls, "expected a tasks/ pull"
        # All succeeded → remote cleaned.
        assert any(c.startswith("rm -rf") for c in conn.run_calls)

    @pytest.mark.asyncio
    async def test_one_failed_reported_no_clean(self, tmp_path, patched):
        conn, rsync_calls = patched
        conn.add("sacct", _Result(0, stdout="COMPLETED\n"))
        conn.add("sacct", _Result(0, stdout="FAILED\n"))
        buf = io.StringIO()
        await _collect_via_manifest(
            tmp_path / "sweeps" / "outputs" / "sw1",
            _manifest(tmp_path, job_ids=["1", "2"]),
            Console(file=buf, width=200),
        )
        out = buf.getvalue()
        assert "1 COMPLETED, 1 FAILED" in out
        assert rsync_calls
        # A failure present → remote kept for inspection.
        assert not any(c.startswith("rm -rf") for c in conn.run_calls)

    @pytest.mark.asyncio
    async def test_pending_in_squeue_is_not_deleted(self, tmp_path, patched):
        # BLOCKER regression: a task still PENDING in squeue (e.g. held behind a
        # maintenance reservation, not yet in sacct) must be treated as running —
        # NEVER classified COMPLETED via the sacct fallback and then deleted.
        conn, rsync_calls = patched
        conn.add("squeue -j 1", _Result(0, stdout=""))  # job 1 gone → sacct
        conn.add("sacct", _Result(0, stdout="COMPLETED\n"))  # job 1 done
        conn.add("squeue -j 2", _Result(0, stdout="PENDING\n"))  # job 2 queued
        buf = io.StringIO()
        await _collect_via_manifest(
            tmp_path / "sweeps" / "outputs" / "sw1",
            _manifest(tmp_path, job_ids=["1", "2"]),
            Console(file=buf, width=200),
        )
        out = buf.getvalue()
        assert "still running" in out
        assert "1/2" in out
        assert rsync_calls  # pulled the finished task
        # The remote must NOT be deleted while a task is still queued.
        assert not any(c.startswith("rm -rf") for c in conn.run_calls)

    @pytest.mark.asyncio
    async def test_already_cleaned_remote_is_noop(self, tmp_path, patched):
        # Re-running after a successful collect (remote dir gone) is a clean no-op,
        # not a "pull reported an error".
        conn, rsync_calls = patched
        conn.add("test -d", _Result(1, stdout=""))  # remote sweep dir gone
        buf = io.StringIO()
        await _collect_via_manifest(
            tmp_path / "sweeps" / "outputs" / "sw1",
            _manifest(tmp_path, job_ids=["1", "2"]),
            Console(file=buf, width=200),
        )
        out = buf.getvalue()
        assert "already cleaned" in out.lower() or "nothing left" in out.lower()
        assert rsync_calls == []  # nothing pulled
        assert not any(c.startswith("rm -rf") for c in conn.run_calls)

    @pytest.mark.asyncio
    async def test_partial_running_pulls_only(self, tmp_path, patched):
        conn, rsync_calls = patched
        conn.add("sacct", _Result(0, stdout="COMPLETED\n"))
        conn.add("sacct", _Result(0, stdout="RUNNING\n"))
        buf = io.StringIO()
        await _collect_via_manifest(
            tmp_path / "sweeps" / "outputs" / "sw1",
            _manifest(tmp_path, job_ids=["1", "2"]),
            Console(file=buf, width=200),
        )
        out = buf.getvalue()
        assert "still running" in out
        assert "1/2" in out
        assert rsync_calls  # pulled what's done
        # Not all terminal → no cleanup.
        assert not any(c.startswith("rm -rf") for c in conn.run_calls)
