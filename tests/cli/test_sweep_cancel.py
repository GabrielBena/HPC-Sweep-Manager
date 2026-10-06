"""`hsm sweep cancel` on a live Slurm sweep (tracker S9).

The sweep is found by its `.hsm_manifest.json`; one `scancel` names every job, over a fake SSH
connection for SSH-Slurm and through the fake `scancel` PATH stub for native Slurm.
"""

from __future__ import annotations

import json
import logging

import pytest
from click.testing import CliRunner
from rich.console import Console

from hpc_sweep_manager.cli.sweep import sweep_cmd
from hpc_sweep_manager.core.remote.ssh_slurm_compute_source import SSHSlurmComputeSource


class _Result:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode, self.stdout, self.stderr = returncode, stdout, stderr


class FakeConn:
    """Answers ``scancel`` with ``scancel_result``; anything else succeeds silently."""

    def __init__(self, scancel_result):
        self.run_calls: list[str] = []
        self.scancel_result = scancel_result

    async def run(self, cmd, *, input=None, check=False, timeout=None):
        self.run_calls.append(cmd)
        return self.scancel_result if cmd.startswith("scancel") else _Result(0)

    def close(self):
        pass

    async def wait_closed(self):
        pass


def _cancel(tmp_path, monkeypatch, manifest, *args, input=None):
    monkeypatch.chdir(tmp_path)
    sweep_dir = tmp_path / "sweeps" / "outputs" / "sw1"
    sweep_dir.mkdir(parents=True)
    (sweep_dir / ".hsm_manifest.json").write_text(json.dumps(manifest))
    obj = {"console": Console(width=200), "logger": logging.getLogger("test")}
    return CliRunner().invoke(sweep_cmd, ["cancel", "sw1", *args], obj=obj, input=input)


def _ssh_manifest(**extra):
    return {
        "sweep_id": "sw1",
        "backend": "slurm",
        "name": "uzh",
        "host": "uzh",
        "remote_sweep_dir": "/scratch/u/hsm-runs/proj/sweeps/sw1",
        "job_ids": ["101", "102"],
        **extra,
    }


@pytest.fixture
def conn(monkeypatch):
    conn = FakeConn(_Result(0))

    async def _open(self):
        return conn

    monkeypatch.setattr(SSHSlurmComputeSource, "_open_connection", _open)
    return conn


def _scancels(conn):
    return [c for c in conn.run_calls if c.startswith("scancel")]


class TestSSHSlurm:
    def test_one_scancel_names_every_job(self, tmp_path, monkeypatch, conn):
        res = _cancel(tmp_path, monkeypatch, _ssh_manifest(), "--yes")
        assert res.exit_code == 0, res.output
        assert conn.run_calls[1:] == ["scancel 101 102"]  # after `echo $USER`: no rsync, no rm
        assert len(conn.run_calls) == 2 and conn.run_calls[0].startswith("echo ")
        assert "Cancelled job(s) 101 102 on uzh" in res.output

    @pytest.mark.parametrize("rc", [1, None])
    def test_a_failed_or_unanswered_scancel_exits_non_zero(self, tmp_path, monkeypatch, conn, rc):
        conn.scancel_result = _Result(rc, stderr="slurm_load_jobs error")
        res = _cancel(tmp_path, monkeypatch, _ssh_manifest(), "--yes")
        assert res.exit_code == 1
        assert "scancel failed" in res.output
        assert "Cancelled job(s)" not in res.output

    def test_the_prompt_still_guards(self, tmp_path, monkeypatch, conn):
        res = _cancel(tmp_path, monkeypatch, _ssh_manifest(), input="n\n")
        assert "Proceed?" in res.output
        assert _scancels(conn) == []

    def test_a_chain_cancels_its_last_chunks_and_stops(self, tmp_path, monkeypatch, conn):
        chunks = [{"index": i, "job_ids": [str(100 + i)]} for i in range(3)]
        chain = {"state": {"chunk_index": 2, "failed": False}, "chunks": chunks}
        manifest = _ssh_manifest(job_ids=["102"], resumable={"enabled": True}, chain=chain)
        res = _cancel(tmp_path, monkeypatch, manifest, "--yes")
        assert res.exit_code == 0, res.output
        # The running chunk and the one queued after it; never a third scancel call.
        assert _scancels(conn) == ["scancel 101 102"]
        saved = json.loads((tmp_path / "sweeps/outputs/sw1/.hsm_manifest.json").read_text())
        assert saved["chain"]["state"]["failed"] is True  # `advance` won't resubmit it


class TestNativeSlurm:
    def _manifest(self):
        return {"sweep_id": "sw1", "backend": "slurm", "name": "slurm", "job_ids": ["1001", "1002"]}

    def _queue(self, fake_slurm, *ids):
        jobs = "".join(json.dumps({"id": i, "state": "RUNNING"}) + "\n" for i in ids)
        (fake_slurm.state_dir / "jobs.jsonl").write_text(jobs)

    def test_one_local_scancel(self, tmp_path, monkeypatch, fake_slurm):
        self._queue(fake_slurm, "1001", "1002", "1003")
        res = _cancel(tmp_path, monkeypatch, self._manifest(), "--yes")
        assert res.exit_code == 0, res.output
        assert (fake_slurm.state_dir / "scancel.log").read_text() == "1001 1002\n"
        states = {j["id"]: j["state"] for j in fake_slurm.jobs()}
        assert states == {"1001": "CANCELLED", "1002": "CANCELLED", "1003": "RUNNING"}
        assert "on this machine" in res.output

    def test_a_job_already_ended_is_no_error(self, tmp_path, monkeypatch, fake_slurm):
        self._queue(fake_slurm, "1001")  # 1002 ended: real scancel exits 0 for it
        res = _cancel(tmp_path, monkeypatch, self._manifest(), "--yes")
        assert res.exit_code == 0, res.output

    def test_a_failed_scancel_exits_non_zero(self, tmp_path, monkeypatch, fake_slurm):
        self._queue(fake_slurm, "1001", "1002")
        monkeypatch.setenv("HSM_FAKE_SCANCEL_RC", "1")
        res = _cancel(tmp_path, monkeypatch, self._manifest(), "--yes")
        assert res.exit_code == 1
        assert "Unable to contact" in res.output


def test_advance_refuses_a_native_chain(tmp_path, monkeypatch):
    manifest = {"sweep_id": "sw1", "backend": "slurm", "resumable": {"enabled": True}}
    monkeypatch.chdir(tmp_path)
    (tmp_path / "sweeps/outputs/sw1").mkdir(parents=True)
    (tmp_path / "sweeps/outputs/sw1/.hsm_manifest.json").write_text(json.dumps(manifest))
    obj = {"console": Console(width=200), "logger": logging.getLogger("test")}
    res = CliRunner().invoke(sweep_cmd, ["advance", "sw1"], obj=obj)
    assert "Slurm-over-SSH chains only" in res.output
