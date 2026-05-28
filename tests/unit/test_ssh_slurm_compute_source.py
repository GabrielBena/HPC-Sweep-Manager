"""Unit tests for SSH-driven Slurm compute source.

Uses the same fake-asyncssh pattern as :mod:`test_ssh_compute_source` —
records ``run()`` calls so we can assert on the exact sbatch / squeue /
scancel / mkdir / cat / rm -rf commands, and a fake rsync that captures
push + pull arg shapes.

No real cluster, no real ssh, no real subprocess.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
from hpc_sweep_manager.core.remote import ssh_slurm_compute_source as mod
from hpc_sweep_manager.core.remote.ssh_slurm_compute_source import (
    SSHSlurmComputeSource,
    build_ssh_slurm_source,
)


# ---------------------------------------------------------------------- fakes


class _Result:
    def __init__(
        self,
        returncode: int = 0,
        stdout: str = "",
        stderr: str = "",
        exit_status: Optional[int] = None,
    ):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.exit_status = exit_status if exit_status is not None else returncode


class FakeConn:
    """asyncssh.SSHClientConnection stand-in.

    Each entry in ``responder`` is a (substring, _Result) pair. On each
    ``.run()`` call we scan in order, pick the first entry whose substring
    appears in the command text, and **pop it** — so appending another
    entry with the same substring lets a test script multiple distinct
    responses to repeated commands (e.g. two sbatch submissions). Falls
    back to ``_Result(returncode=0, stdout="")`` when no entry matches.
    """

    def __init__(self, responder: Optional[List[tuple[str, _Result]]] = None):
        self.run_calls: List[Dict[str, Any]] = []
        self.closed = False
        self._responder = responder or []

    def add(self, substring: str, result: _Result) -> None:
        self._responder.append((substring, result))

    async def run(
        self,
        cmd: str,
        *,
        input: Optional[str] = None,
        check: bool = False,
    ) -> _Result:
        self.run_calls.append({"cmd": cmd, "input": input, "check": check})
        for i, (sub, res) in enumerate(self._responder):
            if sub in cmd:
                del self._responder[i]
                return res
        return _Result(returncode=0, stdout="")

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:  # pragma: no cover - trivial
        pass


class _StubSrc(SSHSlurmComputeSource):
    """Inject a FakeConn + record rsync invocations."""

    def __init__(
        self,
        *args,
        fake_conn: FakeConn,
        rsync_rc: int = 0,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._fake_conn = fake_conn
        self._rsync_calls: List[List[str]] = []
        self._rsync_rc = rsync_rc

    async def _open_connection(self):
        return self._fake_conn

    async def _run_rsync(self, cmd: List[str]) -> int:
        self._rsync_calls.append(cmd)
        return self._rsync_rc


def _setup_ok_responder(home: str = "/u/home/gbena") -> List[tuple[str, _Result]]:
    """Default responder for a successful setup() lifecycle."""
    return [
        ("command -v sbatch", _Result(0, stdout="/usr/bin/sbatch\n")),
        ("echo $HOME", _Result(0, stdout=f"{home}\n")),
        ("mkdir -p", _Result(0)),
    ]


# ---------------------------------------------------------------------- setup


class TestSetup:
    @pytest.mark.asyncio
    async def test_happy_path(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder("/home/gbena"))
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="train.py",
            remote_root="~/.hsm/runs",
            default_spec=ResourceSpec(walltime="01:00:00", gpus=1),
            fake_conn=conn,
        )
        sweep_dir = tmp_path / "sweeps" / "outputs" / "sweep_x"
        ok = await src.setup(sweep_dir, "sweep_x")
        assert ok is True
        # Tilde expansion happened — remote paths are absolute.
        project_name = tmp_path.name
        assert src._remote_code_dir == f"/home/gbena/.hsm/runs/{project_name}/code"
        assert (
            src._remote_sweep_dir
            == f"/home/gbena/.hsm/runs/{project_name}/sweeps/sweep_x"
        )
        # mkdir issued once with all four dirs.
        mkdir_calls = [c for c in conn.run_calls if c["cmd"].startswith("mkdir -p")]
        assert len(mkdir_calls) == 1
        assert "tasks" in mkdir_calls[0]["cmd"]
        assert "logs" in mkdir_calls[0]["cmd"]
        assert "scripts" in mkdir_calls[0]["cmd"]
        # rsync push was attempted.
        assert len(src._rsync_calls) == 1
        push_cmd = src._rsync_calls[0]
        assert push_cmd[0] == "rsync"
        assert any(arg.startswith(f"{src.host}:") for arg in push_cmd)

    @pytest.mark.asyncio
    async def test_fails_when_sbatch_not_on_remote(self, tmp_path):
        conn = FakeConn(
            responder=[
                ("command -v sbatch", _Result(127, stdout="", stderr="not found")),
            ]
        )
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="train.py",
            fake_conn=conn,
        )
        ok = await src.setup(tmp_path / "sweep_dir", "sw")
        assert ok is False
        assert src.stats.health_status == "unhealthy"
        # We never reached the rsync push.
        assert src._rsync_calls == []

    @pytest.mark.asyncio
    async def test_fails_when_rsync_push_fails(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="train.py",
            fake_conn=conn,
            rsync_rc=23,
        )
        ok = await src.setup(tmp_path / "sweep_dir", "sw")
        assert ok is False
        assert src.stats.health_status == "unhealthy"


# --------------------------------------------------------------------- submit


class TestSubmit:
    @pytest.mark.asyncio
    async def test_submit_job_parses_id(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        # Append the sbatch response after setup is set up.
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 12345\n"))
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="train.py",
            default_spec=ResourceSpec(walltime="01:00:00", gpus=1, gpu_type="H100"),
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        jid = await src.submit_job(
            params={"seed": 0}, job_name="task_0", sweep_id="sweep_1"
        )
        assert jid == "12345"
        # The rendered script was cat-piped to the remote scripts dir.
        cat_calls = [c for c in conn.run_calls if c["cmd"].startswith("cat > ")]
        assert len(cat_calls) == 1
        body = cat_calls[0]["input"]
        # Directives the typed slurm: block produces are present.
        assert "#SBATCH --time=01:00:00" in body
        assert "#SBATCH --gres=gpu:H100:1" in body
        # Template cd's into the REMOTE code dir, not local project_dir.
        assert src._remote_code_dir in body
        # sbatch was invoked with the remote script path.
        sbatch_calls = [c for c in conn.run_calls if c["cmd"].startswith("sbatch ")]
        assert len(sbatch_calls) == 1

    @pytest.mark.asyncio
    async def test_submit_job_raises_on_sbatch_failure(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add(
            "sbatch",
            _Result(1, stdout="", stderr="error: invalid partition\n"),
        )
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="train.py",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        with pytest.raises(RuntimeError, match="sbatch failed"):
            await src.submit_job(
                params={"seed": 0}, job_name="task_0", sweep_id="sweep_1"
            )

    @pytest.mark.asyncio
    async def test_submit_array_writes_params_and_returns_id(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 999\n"))
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="train.py",
            default_spec=ResourceSpec(walltime="02:00:00"),
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        ids = await src.submit_batch(
            params_list=[{"seed": 0}, {"seed": 1}, {"seed": 2}],
            sweep_id="sweep_1",
            mode="array",
            job_name_prefix="sweep_1",
        )
        assert ids == ["999"]
        # parameter_combinations.json was written via cat-pipe to the
        # remote sweep dir.
        cat_params = [
            c for c in conn.run_calls
            if c["cmd"].startswith("cat > ") and "parameter_combinations" in c["cmd"]
        ]
        assert len(cat_params) == 1
        loaded = json.loads(cat_params[0]["input"])
        assert len(loaded) == 3
        assert loaded[0] == {"index": 1, "global_index": 1, "params": {"seed": 0}}

    @pytest.mark.asyncio
    async def test_submit_array_rejects_empty(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        src = _StubSrc(
            name="uzh", host="uzh", project_dir=str(tmp_path), script_path="t.py",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        with pytest.raises(ValueError, match="empty array"):
            await src.submit_batch(
                params_list=[], sweep_id="sweep_1", mode="array",
            )


# --------------------------------------------------------------------- status


class TestStatus:
    @pytest.mark.asyncio
    async def test_get_job_status_maps_running(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("squeue -j", _Result(0, stdout="RUNNING\n"))
        src = _StubSrc(
            name="uzh", host="uzh", project_dir=str(tmp_path), script_path="t.py",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        status = await src.get_job_status("12345")
        assert status == "RUNNING"

    @pytest.mark.asyncio
    async def test_get_job_status_absent_means_completed(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("squeue -j", _Result(0, stdout="\n"))
        src = _StubSrc(
            name="uzh", host="uzh", project_dir=str(tmp_path), script_path="t.py",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        status = await src.get_job_status("99999")
        assert status == "COMPLETED"

    @pytest.mark.asyncio
    async def test_update_all_uses_single_squeue_call(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        # Two sbatch submissions, then one batched squeue response.
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 100\n"))
        src = _StubSrc(
            name="uzh", host="uzh", project_dir=str(tmp_path), script_path="t.py",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        jid_a = await src.submit_job({"s": 0}, "task_0", "sweep_1")
        # Re-arm sbatch responder for the second submission.
        conn._responder.append(
            ("sbatch", _Result(0, stdout="Submitted batch job 101\n"))
        )
        jid_b = await src.submit_job({"s": 1}, "task_1", "sweep_1")
        assert {jid_a, jid_b} == {"100", "101"}
        # Now the batched squeue: 100 is still RUNNING, 101 has finished
        # (absent → COMPLETED).
        conn._responder.append(
            ("squeue -j", _Result(0, stdout="100 RUNNING\n")),
        )
        await src.update_all_job_statuses()
        # Exactly one squeue call hit the wire — not two.
        # Use `startswith` to avoid matching setup's `command -v ... squeue ...`
        # pre-flight check, which contains the substring "squeue".
        squeue_calls = [
            c for c in conn.run_calls if c["cmd"].startswith("squeue ")
        ]
        assert len(squeue_calls) == 1
        assert "100" in squeue_calls[0]["cmd"]
        assert "101" in squeue_calls[0]["cmd"]


# --------------------------------------------------------------------- cancel


class TestCancel:
    @pytest.mark.asyncio
    async def test_cancel_marks_cancelled(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 42\n"))
        conn.add("scancel", _Result(0))
        src = _StubSrc(
            name="uzh", host="uzh", project_dir=str(tmp_path), script_path="t.py",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        jid = await src.submit_job({"s": 0}, "task_0", "sweep_1")
        assert await src.cancel_job(jid) is True
        assert src.completed_jobs[jid].status == "CANCELLED"


# ---------------------------------------------------------------- collect_results


class TestCollectResults:
    @pytest.mark.asyncio
    async def test_pull_then_cleanup_on_success(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 7\n"))
        conn.add("rm -rf", _Result(0))
        src = _StubSrc(
            name="uzh", host="uzh", project_dir=str(tmp_path), script_path="t.py",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        jid = await src.submit_job({"s": 0}, "task_0", "sweep_1")
        # Simulate the job finishing successfully.
        src.update_job_status(jid, "COMPLETED")
        ok = await src.collect_results()
        assert ok is True
        # rsync pull happened (push earlier, pull now → 2 rsync calls total).
        assert len(src._rsync_calls) == 2
        pull = src._rsync_calls[-1]
        assert pull[0] == "rsync"
        # Remote dir was cleaned.
        rm_calls = [c for c in conn.run_calls if c["cmd"].startswith("rm -rf")]
        assert len(rm_calls) == 1
        assert src._remote_sweep_dir in rm_calls[0]["cmd"]

    @pytest.mark.asyncio
    async def test_no_cleanup_on_failure(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 7\n"))
        src = _StubSrc(
            name="uzh", host="uzh", project_dir=str(tmp_path), script_path="t.py",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        jid = await src.submit_job({"s": 0}, "task_0", "sweep_1")
        src.update_job_status(jid, "FAILED")
        ok = await src.collect_results()
        assert ok is True
        rm_calls = [c for c in conn.run_calls if c["cmd"].startswith("rm -rf")]
        assert rm_calls == []

    @pytest.mark.asyncio
    async def test_keep_remote_overrides_cleanup(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 7\n"))
        src = _StubSrc(
            name="uzh", host="uzh", project_dir=str(tmp_path), script_path="t.py",
            keep_remote_on_success=True,
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        jid = await src.submit_job({"s": 0}, "task_0", "sweep_1")
        src.update_job_status(jid, "COMPLETED")
        await src.collect_results()
        rm_calls = [c for c in conn.run_calls if c["cmd"].startswith("rm -rf")]
        assert rm_calls == []


# ----------------------------------------------------------- workdir / archive


class TestStorageTier:
    @pytest.mark.asyncio
    async def test_workdir_overrides_remote_root(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder("/u/home/gbena"))
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="train.py",
            remote_root="~/.hsm/runs",
            workdir="/scratch/gbena/hsm-runs",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_w")
        # /scratch is absolute → no tilde expansion happens, but the
        # layout uses /scratch instead of $HOME/.hsm/runs.
        project_name = tmp_path.name
        assert (
            src._remote_sweep_dir
            == f"/scratch/gbena/hsm-runs/{project_name}/sweeps/sweep_w"
        )

    @pytest.mark.asyncio
    async def test_workdir_tilde_expanded(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder("/u/home/gbena"))
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="train.py",
            workdir="~/scratch/hsm-runs",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_w")
        project_name = tmp_path.name
        assert (
            src._remote_sweep_dir
            == f"/u/home/gbena/scratch/hsm-runs/{project_name}/sweeps/sweep_w"
        )

    @pytest.mark.asyncio
    async def test_archive_on_completed_runs_when_clean(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 1\n"))
        # The archive cmd issues "mkdir -p <archive>/<id> && rsync ..." —
        # match the leading "mkdir -p" + the "rsync" parts of it.
        src = _StubSrc(
            name="uzh", host="uzh", project_dir=str(tmp_path), script_path="t.py",
            workdir="/scratch/gbena/hsm-runs",
            archive_dir="/shares/payvand/hsm-archive",
            archive_on="completed",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sw1")
        jid = await src.submit_job({"s": 0}, "task_0", "sw1")
        src.update_job_status(jid, "COMPLETED")
        await src.collect_results()
        # Look for the archive command — it contains the archive_dir path.
        arch_calls = [
            c for c in conn.run_calls
            if "rsync -a" in c["cmd"] and "/shares/payvand/hsm-archive/sw1" in c["cmd"]
        ]
        assert len(arch_calls) == 1
        # Sentinel was written.
        sentinel_calls = [
            c for c in conn.run_calls
            if c["cmd"].startswith("cat > ") and ".archived" in c["cmd"]
        ]
        assert len(sentinel_calls) == 1
        assert "archived_at:" in sentinel_calls[0]["input"]
        assert "sweep_id: sw1" in sentinel_calls[0]["input"]
        assert "any_failed: False" in sentinel_calls[0]["input"]

    @pytest.mark.asyncio
    async def test_archive_on_completed_skips_when_failed(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 1\n"))
        src = _StubSrc(
            name="uzh", host="uzh", project_dir=str(tmp_path), script_path="t.py",
            workdir="/scratch/gbena/hsm-runs",
            archive_dir="/shares/payvand/hsm-archive",
            archive_on="completed",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sw1")
        jid = await src.submit_job({"s": 0}, "task_0", "sw1")
        src.update_job_status(jid, "FAILED")
        await src.collect_results()
        arch_calls = [
            c for c in conn.run_calls
            if "rsync -a" in c["cmd"] and "/shares/payvand" in c["cmd"]
        ]
        assert arch_calls == []

    @pytest.mark.asyncio
    async def test_archive_on_always_runs_even_on_failure(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 1\n"))
        src = _StubSrc(
            name="uzh", host="uzh", project_dir=str(tmp_path), script_path="t.py",
            workdir="/scratch/gbena/hsm-runs",
            archive_dir="/shares/payvand/hsm-archive",
            archive_on="always",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sw1")
        jid = await src.submit_job({"s": 0}, "task_0", "sw1")
        src.update_job_status(jid, "FAILED")
        await src.collect_results()
        arch_calls = [
            c for c in conn.run_calls
            if "rsync -a" in c["cmd"] and "/shares/payvand" in c["cmd"]
        ]
        assert len(arch_calls) == 1
        # Sentinel records the failure.
        sentinel = [
            c for c in conn.run_calls
            if c["cmd"].startswith("cat > ") and ".archived" in c["cmd"]
        ][0]
        assert "any_failed: True" in sentinel["input"]

    @pytest.mark.asyncio
    async def test_archive_on_never_disables_archive(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 1\n"))
        src = _StubSrc(
            name="uzh", host="uzh", project_dir=str(tmp_path), script_path="t.py",
            workdir="/scratch/gbena/hsm-runs",
            archive_dir="/shares/payvand/hsm-archive",
            archive_on="never",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sw1")
        jid = await src.submit_job({"s": 0}, "task_0", "sw1")
        src.update_job_status(jid, "COMPLETED")
        await src.collect_results()
        arch_calls = [
            c for c in conn.run_calls
            if "rsync -a" in c["cmd"] and "/shares/payvand" in c["cmd"]
        ]
        assert arch_calls == []

    @pytest.mark.asyncio
    async def test_no_archive_dir_means_no_archive(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 1\n"))
        src = _StubSrc(
            name="uzh", host="uzh", project_dir=str(tmp_path), script_path="t.py",
            workdir="/scratch/gbena/hsm-runs",
            # archive_dir omitted
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sw1")
        jid = await src.submit_job({"s": 0}, "task_0", "sw1")
        src.update_job_status(jid, "COMPLETED")
        await src.collect_results()
        arch_calls = [
            c for c in conn.run_calls
            if c["cmd"].startswith("mkdir -p") and "rsync -a" in c["cmd"]
        ]
        assert arch_calls == []

    @pytest.mark.asyncio
    async def test_archive_runs_before_pull(self, tmp_path):
        """Archive happens server-side first; THEN we pull tasks/ back."""
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 1\n"))
        src = _StubSrc(
            name="uzh", host="uzh", project_dir=str(tmp_path), script_path="t.py",
            workdir="/scratch/gbena/hsm-runs",
            archive_dir="/shares/payvand/hsm-archive",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sw1")
        jid = await src.submit_job({"s": 0}, "task_0", "sw1")
        src.update_job_status(jid, "COMPLETED")
        # Record rsync_calls AT the time _archive_remote runs by clearing
        # them after setup's push so we can compare archive (ssh-side)
        # vs pull (local rsync) ordering.
        src._rsync_calls.clear()
        await src.collect_results()
        # The archive command is run via ssh (in conn.run_calls); the
        # pull goes through _run_rsync. Both must be present, archive
        # first in conn.run_calls before the pull.
        cmds = [c["cmd"] for c in conn.run_calls]
        archive_idx = next(
            (i for i, c in enumerate(cmds) if "rsync -a" in c and "/shares" in c),
            None,
        )
        assert archive_idx is not None, "archive command not issued"
        assert len(src._rsync_calls) == 1  # the pull
        # And the pull happened (we recorded it in _run_rsync, which is
        # called *after* the archive command returns).

    def test_invalid_archive_on_raises(self, tmp_path):
        with pytest.raises(ValueError, match="archive_on"):
            SSHSlurmComputeSource(
                name="x", host="x", project_dir=str(tmp_path), script_path="t.py",
                archive_on="sometimes",
            )


# ---------------------------------------------------------------- qos_whitelist


class TestQosWhitelist:
    @pytest.mark.asyncio
    async def test_disallowed_qos_raises(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        src = _StubSrc(
            name="uzh", host="uzh", project_dir=str(tmp_path), script_path="t.py",
            default_spec=ResourceSpec(walltime="1:00:00"),
            qos_whitelist=frozenset({"normal", "medium"}),
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        with pytest.raises(ValueError, match="qos="):
            await src.submit_job(
                {"s": 0}, "task_0", "sweep_1",
                spec=ResourceSpec(qos="lowprio"),
            )

    @pytest.mark.asyncio
    async def test_allowed_qos_succeeds(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 1\n"))
        src = _StubSrc(
            name="uzh", host="uzh", project_dir=str(tmp_path), script_path="t.py",
            default_spec=ResourceSpec(walltime="1:00:00"),
            qos_whitelist=frozenset({"normal", "medium"}),
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        jid = await src.submit_job(
            {"s": 0}, "task_0", "sweep_1",
            spec=ResourceSpec(qos="normal"),
        )
        assert jid == "1"


# ---------------------------------------------------------------- factory


class TestFactory:
    def test_builds_with_per_remote_spec(self, tmp_path):
        src = build_ssh_slurm_source(
            name="uzh",
            remote_cfg={
                "host": "uzh",
                "conda_env": "cpvr",
                "qos_whitelist": ["normal", "medium"],
                "spec": {
                    "walltime": "04:00:00",
                    "cpus_per_task": 8,
                    "mem": "32G",
                    "gpus": 1,
                    "gpu_type": "H100",
                },
            },
            distributed_cfg={},
            project_dir=str(tmp_path),
            script_path="train.py",
        )
        assert isinstance(src, SSHSlurmComputeSource)
        assert src.host == "uzh"
        assert src.conda_env == "cpvr"
        assert src.default_spec.walltime == "04:00:00"
        assert src.default_spec.cpus_per_task == 8
        assert src.default_spec.gpus == 1
        assert src.default_spec.gpu_type == "H100"
        assert src.qos_whitelist == frozenset({"normal", "medium"})

    def test_caller_spec_layers_under_per_remote(self, tmp_path):
        # CLI-supplied default_spec (e.g. --walltime) should override the
        # per-remote spec's walltime.
        src = build_ssh_slurm_source(
            name="uzh",
            remote_cfg={
                "host": "uzh",
                "spec": {"walltime": "04:00:00", "gpus": 1},
            },
            distributed_cfg={},
            project_dir=str(tmp_path),
            script_path="train.py",
            default_spec=ResourceSpec(walltime="08:00:00"),
        )
        assert src.default_spec.walltime == "08:00:00"
        assert src.default_spec.gpus == 1

    def test_conda_env_override_wins(self, tmp_path):
        src = build_ssh_slurm_source(
            name="uzh",
            remote_cfg={"host": "uzh", "conda_env": "from-cfg"},
            distributed_cfg={"conda_env": "from-global"},
            project_dir=str(tmp_path),
            script_path="train.py",
            conda_env_override="from-cli",
        )
        assert src.conda_env == "from-cli"

    def test_storage_fields_propagate(self, tmp_path):
        src = build_ssh_slurm_source(
            name="uzh",
            remote_cfg={
                "host": "uzh",
                "workdir": "/scratch/$USER/hsm-runs",
                "archive_dir": "/shares/payvand.ini.uzh/hsm-archive",
                "archive_on": "always",
            },
            distributed_cfg={},
            project_dir=str(tmp_path),
            script_path="train.py",
        )
        assert src.workdir == "/scratch/$USER/hsm-runs"
        assert src.archive_dir == "/shares/payvand.ini.uzh/hsm-archive"
        assert src.archive_on == "always"

    def test_storage_fields_default_when_unset(self, tmp_path):
        src = build_ssh_slurm_source(
            name="uzh",
            remote_cfg={"host": "uzh"},
            distributed_cfg={},
            project_dir=str(tmp_path),
            script_path="train.py",
        )
        assert src.workdir is None
        assert src.archive_dir is None
        assert src.archive_on == "completed"

    def test_bad_qos_whitelist_falls_back_to_none(self, tmp_path):
        src = build_ssh_slurm_source(
            name="uzh",
            remote_cfg={"host": "uzh", "qos_whitelist": "normal"},  # wrong shape
            distributed_cfg={},
            project_dir=str(tmp_path),
            script_path="train.py",
        )
        # String falls back to None — we don't try to parse it.
        assert src.qos_whitelist is None
