"""Unit tests for the push-model SSHComputeSource.

These exercise the orchestration logic with an injected fake asyncssh-like
connection and fake rsync — no real SSH endpoint is required. The pure
helpers (rsync arg shape, GPU partitioning) have their own coverage in
:mod:`test_push_exec`.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
from hpc_sweep_manager.core.remote import ssh_compute_source as ssh_mod
from hpc_sweep_manager.core.remote.ssh_compute_source import SSHComputeSource


# ---------------------------------------------------------------------- fakes

NVIDIA_SMI_SAMPLE_4_GPUS = (
    "0, NVIDIA H100, 12, 81920, 0\n"
    "1, NVIDIA H100, 12, 81920, 0\n"
    "2, NVIDIA H100, 12, 81920, 0\n"
    "3, NVIDIA H100, 12, 81920, 0\n"
)


class _Result:
    """Mimic asyncssh's `SSHCompletedProcess` / process result enough for our use."""

    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = "", exit_status: Optional[int] = None):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        # asyncssh's create_process().wait() returns a result with `exit_status`,
        # which is what _monitor reads.
        self.exit_status = exit_status if exit_status is not None else returncode


class FakeProc:
    """Minimal stand-in for asyncssh.SSHClientProcess.

    Call :meth:`finish` with an exit status to release the awaiting
    :meth:`wait`. Mirrors enough of the real surface that
    ``SSHComputeSource._monitor`` and ``cancel_job`` can drive it.
    """

    def __init__(self, cmd: str):
        self.cmd = cmd
        self._done = asyncio.Event()
        self.exit_status: Optional[int] = None
        self.terminated = False
        self.killed = False

    async def wait(self) -> _Result:
        await self._done.wait()
        return _Result(returncode=self.exit_status or 0, exit_status=self.exit_status)

    def finish(self, exit_status: int = 0) -> None:
        if self.exit_status is None:
            self.exit_status = exit_status
        self._done.set()

    def terminate(self) -> None:
        self.terminated = True
        # Signal a non-zero exit so the monitor sees it as FAILED (which
        # cancel_job overrides to CANCELLED).
        if self.exit_status is None:
            self.exit_status = -15
        self._done.set()

    def kill(self) -> None:
        self.killed = True
        if self.exit_status is None:
            self.exit_status = -9
        self._done.set()


class FakeConn:
    """Records run() / create_process() calls so tests can assert on them."""

    def __init__(self, *, gpu_csv: str = "", nvidia_smi_rc: int = 0):
        self.run_calls: List[Dict[str, Any]] = []
        self.processes: List[FakeProc] = []
        self.closed = False
        self._gpu_csv = gpu_csv
        self._nvidia_smi_rc = nvidia_smi_rc

    async def run(self, cmd: str, *, input: Optional[str] = None, check: bool = False) -> _Result:
        self.run_calls.append({"cmd": cmd, "input": input, "check": check})
        if "nvidia-smi" in cmd:
            return _Result(returncode=self._nvidia_smi_rc, stdout=self._gpu_csv)
        if cmd == "date":
            return _Result(returncode=0, stdout="Mon Jan 1 00:00:00 UTC 2026\n")
        return _Result(returncode=0, stdout="")

    async def create_process(self, cmd: str) -> FakeProc:
        proc = FakeProc(cmd)
        self.processes.append(proc)
        return proc

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:  # pragma: no cover - trivial
        pass


class _StubSSH(SSHComputeSource):
    """Inject a FakeConn + record rsync invocations instead of hitting the network."""

    def __init__(self, *args, fake_conn: FakeConn, rsync_rc: int = 0, **kwargs):
        super().__init__(*args, **kwargs)
        self._fake_conn = fake_conn
        self._rsync_calls: List[List[str]] = []
        self._rsync_rc = rsync_rc

    async def _open_connection(self) -> Any:
        return self._fake_conn

    async def _run_rsync(self, cmd: List[str]) -> int:
        self._rsync_calls.append(cmd)
        return self._rsync_rc


def _make_src(tmp_path: Path, **kwargs) -> _StubSSH:
    fake_conn = kwargs.pop("fake_conn", None) or FakeConn()
    defaults: Dict[str, Any] = dict(
        name="anahita",
        max_parallel_jobs=2,
        conda_env="lab",
        script_path="train.py",
        project_dir=str(tmp_path / "project"),
    )
    defaults.update(kwargs)
    (tmp_path / "project").mkdir(exist_ok=True)
    return _StubSSH(fake_conn=fake_conn, **defaults)


# ----------------------------------------------------------------- construction


class TestConstruction:
    def test_defaults(self, tmp_path):
        src = _make_src(tmp_path)
        assert src.source_type == "ssh_remote"
        assert src.host == "anahita"  # defaults to name = alias
        assert src.remote_root == "~/.hsm/runs"
        assert src.keep_remote_on_success is False

    def test_absolute_script_path_inside_project_made_relative(self, tmp_path):
        # The rendered command does `cd <remote_code_dir>` and then references
        # script_path — so an absolute LOCAL path must be normalized to a
        # relative path that resolves under the rsync'd mirror.
        proj = tmp_path / "project"
        proj.mkdir()
        src = SSHComputeSource(
            name="anahita",
            project_dir=str(proj),
            script_path=str(proj / "train.py"),
        )
        assert src.script_path == "train.py"

    def test_absolute_script_path_outside_project_kept_verbatim(self, tmp_path, caplog):
        proj = tmp_path / "project"
        proj.mkdir()
        other = tmp_path / "elsewhere" / "train.py"
        with caplog.at_level("WARNING"):
            src = SSHComputeSource(
                name="anahita",
                project_dir=str(proj),
                script_path=str(other),
            )
        assert src.script_path == str(other)
        assert any("outside project_dir" in r.message for r in caplog.records)

    def test_relative_script_path_kept_as_is(self, tmp_path):
        src = SSHComputeSource(
            name="anahita",
            project_dir=str(tmp_path),
            script_path="subdir/train.py",
        )
        assert src.script_path == "subdir/train.py"

    def test_explicit_host_overrides_name(self, tmp_path):
        src = _make_src(tmp_path, host="actual-host.example.com")
        assert src.host == "actual-host.example.com"

    def test_remote_manager_sentinel(self, tmp_path):
        # Legacy distributed_manager.py paths read `source.remote_manager`;
        # the sentinel keeps them short-circuiting cleanly.
        src = _make_src(tmp_path)
        assert src.remote_manager is None

    def test_minimum_parallel_jobs_is_one(self, tmp_path):
        src = _make_src(tmp_path, max_parallel_jobs=0)
        assert src.max_parallel_jobs == 1

    def test_remote_root_trailing_slash_stripped(self, tmp_path):
        src = _make_src(tmp_path, remote_root="~/.hsm/runs/")
        assert src.remote_root == "~/.hsm/runs"


class TestFromRemoteConfig:
    """Bridge from the legacy RemoteConfig shape (used by distributed _build_ssh_children)."""

    def test_pulls_ssh_fields(self, tmp_path):
        class RC:
            host = "h.example.com"
            ssh_key = "~/.ssh/id_ed25519"
            ssh_port = 2222
            python_interpreter = "/opt/py/bin/python"
            max_parallel_jobs = 8

        src = SSHComputeSource.from_remote_config(
            name="alpha",
            remote_config=RC(),
            project_dir=str(tmp_path),
            script_path="train.py",
        )
        assert src.host == "h.example.com"
        assert src.ssh_key == "~/.ssh/id_ed25519"
        assert src.ssh_port == 2222
        assert src.python_path == "/opt/py/bin/python"
        assert src.max_parallel_jobs == 8

    def test_max_parallel_jobs_defaults_to_one(self, tmp_path):
        class RC:
            host = "h"
            ssh_key = None
            ssh_port = None
            python_interpreter = None
            max_parallel_jobs = None

        src = SSHComputeSource.from_remote_config(name="alpha", remote_config=RC())
        assert src.max_parallel_jobs == 1


# ------------------------------------------------------------------------ setup


class TestSetup:
    pytestmark = pytest.mark.asyncio

    async def test_setup_runs_rsync_push(self, tmp_path):
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn)
        ok = await src.setup(tmp_path / "sweep", "test_sweep")
        assert ok is True
        # Exactly one rsync push call to <host>:<remote_code_dir>.
        assert len(src._rsync_calls) == 1
        push = src._rsync_calls[0]
        assert push[0] == "rsync"
        # Destination is host:remote_code_dir/
        assert push[-1].startswith("anahita:")
        assert push[-1].endswith("/code/")

    async def test_setup_creates_remote_layout(self, tmp_path):
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn)
        await src.setup(tmp_path / "sweep", "test_sweep")
        # First conn.run should be the mkdir -p for code/tasks/logs/scripts.
        mkdirs = [c for c in fake_conn.run_calls if "mkdir -p" in c["cmd"]]
        assert mkdirs, "expected at least one mkdir -p"
        layout = mkdirs[0]["cmd"]
        assert "/code" in layout
        assert "/sweeps/test_sweep/tasks" in layout
        assert "/sweeps/test_sweep/logs" in layout
        assert "/sweeps/test_sweep/scripts" in layout

    async def test_setup_resolves_tilde_via_remote_home(self, tmp_path):
        # FakeConn returns `Mon Jan 1 …` for `date`; teach it to answer `echo $HOME`.
        class HomeyConn(FakeConn):
            async def run(self, cmd, *, input=None, check=False):
                self.run_calls.append({"cmd": cmd, "input": input, "check": check})
                if cmd == "echo $HOME":
                    return _Result(returncode=0, stdout="/home/gbena\n")
                if "nvidia-smi" in cmd:
                    return _Result(returncode=self._nvidia_smi_rc, stdout=self._gpu_csv)
                return _Result(returncode=0, stdout="")

        fake_conn = HomeyConn()
        src = _make_src(tmp_path, fake_conn=fake_conn, remote_root="~/.hsm/runs")
        await src.setup(tmp_path / "sweep", "test_sweep")
        # remote_code_dir / remote_sweep_dir should be absolute, no ~.
        assert src._remote_code_dir.startswith("/home/gbena/.hsm/runs/")
        assert "~" not in src._remote_code_dir
        assert src._remote_sweep_dir.startswith("/home/gbena/.hsm/runs/")
        assert "~" not in src._remote_sweep_dir

    async def test_setup_absolute_remote_root_unchanged(self, tmp_path):
        # An already-absolute remote_root should NOT trigger the $HOME probe.
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn, remote_root="/scratch/hsm")
        await src.setup(tmp_path / "sweep", "test_sweep")
        assert src._remote_code_dir.startswith("/scratch/hsm/")
        # No `echo $HOME` call should have been made.
        assert not any(c["cmd"] == "echo $HOME" for c in fake_conn.run_calls)

    async def test_setup_probes_gpus_and_fills_slots(self, tmp_path):
        fake_conn = FakeConn(gpu_csv=NVIDIA_SMI_SAMPLE_4_GPUS)
        src = _make_src(
            tmp_path,
            fake_conn=fake_conn,
            max_parallel_jobs=4,
            default_spec=ResourceSpec(gpus=1),
        )
        await src.setup(tmp_path / "sweep", "test_sweep")
        assert src._gpu_indices == [0, 1, 2, 3]
        # 4 GPUs / 1 per job → 4 GPU slots.
        assert src._slot_count == 4
        slots = [src._slot_queue.get_nowait() for _ in range(4)]
        assert slots == [[0], [1], [2], [3]]

    async def test_gpus_allowlist_subset(self, tmp_path):
        fake_conn = FakeConn(gpu_csv=NVIDIA_SMI_SAMPLE_4_GPUS)
        src = _make_src(
            tmp_path,
            fake_conn=fake_conn,
            max_parallel_jobs=4,
            gpus=[1, 3],
            default_spec=ResourceSpec(gpus=1),
        )
        await src.setup(tmp_path / "sweep", "test_sweep")
        slots = [src._slot_queue.get_nowait() for _ in range(2)]
        assert slots == [[1], [3]]
        assert src._slot_count == 2

    async def test_no_gpus_falls_back_to_cpu_slots(self, tmp_path):
        fake_conn = FakeConn(nvidia_smi_rc=127)  # nvidia-smi not found
        src = _make_src(tmp_path, fake_conn=fake_conn, max_parallel_jobs=3)
        await src.setup(tmp_path / "sweep", "test_sweep")
        assert src._gpu_indices == []
        assert src._slot_count == 3
        slots = [src._slot_queue.get_nowait() for _ in range(3)]
        assert slots == [None, None, None]

    async def test_rsync_failure_marks_unhealthy(self, tmp_path):
        fake_conn = FakeConn()
        src = _StubSSH(
            fake_conn=fake_conn,
            rsync_rc=23,  # rsync's "partial transfer" error code
            name="anahita",
            conda_env="lab",
            project_dir=str(tmp_path),
            script_path="train.py",
        )
        ok = await src.setup(tmp_path / "sweep", "test_sweep")
        assert ok is False
        assert src.stats.health_status == "unhealthy"

    async def test_connect_failure_marks_unhealthy(self, tmp_path):
        src = _make_src(tmp_path)

        async def boom():
            raise ConnectionRefusedError("nope")

        src._open_connection = boom  # type: ignore[assignment]
        ok = await src.setup(tmp_path / "sweep", "test_sweep")
        assert ok is False
        assert src.stats.health_status == "unhealthy"


# ----------------------------------------------------------------- submission


class TestSubmit:
    pytestmark = pytest.mark.asyncio

    async def test_submit_without_setup_raises(self, tmp_path):
        src = _make_src(tmp_path)
        with pytest.raises(RuntimeError, match="not set up"):
            await src.submit_job({"x": 1}, "task_001", "test_sweep")

    async def test_submit_uploads_script_and_launches(self, tmp_path):
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn, max_parallel_jobs=2)
        await src.setup(tmp_path / "sweep", "test_sweep")

        job_id = await src.submit_job({"lr": 0.01}, "task_001", "test_sweep")
        assert job_id in src.active_jobs

        # The script-upload `cat > … && chmod +x …` is in run_calls with input set.
        uploads = [c for c in fake_conn.run_calls if c["input"] is not None]
        assert len(uploads) == 1
        assert "cat >" in uploads[0]["cmd"]
        # Script content should contain the rendered hydra args.
        content = uploads[0]["input"]
        assert "lr=0.01" in content
        # The wrapper invokes our run_prefix.
        assert "conda run -n lab python" in content

        # create_process called once with `bash <remote_script_path>`.
        assert len(fake_conn.processes) == 1
        assert fake_conn.processes[0].cmd.startswith("bash ")

    async def test_submit_picks_gpu_slot_into_template(self, tmp_path):
        fake_conn = FakeConn(gpu_csv=NVIDIA_SMI_SAMPLE_4_GPUS)
        src = _make_src(
            tmp_path,
            fake_conn=fake_conn,
            max_parallel_jobs=4,
            default_spec=ResourceSpec(gpus=1),
        )
        await src.setup(tmp_path / "sweep", "test_sweep")
        await src.submit_job({"i": 1}, "task_001", "test_sweep")

        uploads = [c for c in fake_conn.run_calls if c["input"] is not None]
        # First slot popped is [0]; CUDA_VISIBLE_DEVICES should be set to "0".
        assert "CUDA_VISIBLE_DEVICES=0" in uploads[0]["input"]

    async def test_slot_back_pressure(self, tmp_path):
        """Third submission blocks until a slot frees up."""
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn, max_parallel_jobs=2)
        await src.setup(tmp_path / "sweep", "test_sweep")

        await src.submit_job({"i": 1}, "task_001", "test_sweep")
        await src.submit_job({"i": 2}, "task_002", "test_sweep")
        third = asyncio.create_task(src.submit_job({"i": 3}, "task_003", "test_sweep"))
        await asyncio.sleep(0.05)
        assert not third.done(), "third submit should wait on slot"

        # Finish job #1 → frees a slot → third completes.
        fake_conn.processes[0].finish(exit_status=0)
        await asyncio.wait_for(third, timeout=1.0)
        assert third.done()
        # Drain the rest so cleanup doesn't await indefinitely.
        for p in fake_conn.processes:
            p.finish(exit_status=0)
        await src.wait_for_all()

    async def test_monitor_moves_active_to_completed(self, tmp_path):
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn, max_parallel_jobs=2)
        await src.setup(tmp_path / "sweep", "test_sweep")

        job_id = await src.submit_job({"i": 1}, "task_001", "test_sweep")
        assert job_id in src.active_jobs
        fake_conn.processes[0].finish(exit_status=0)
        await src.wait_for_all()
        assert job_id in src.completed_jobs
        assert src.completed_jobs[job_id].status == "COMPLETED"

    async def test_conda_env_emits_init_source_block(self, tmp_path):
        # conda_env="lab" → the rendered script must `. <conda.sh>` before
        # invoking `conda run`, otherwise non-interactive ssh shells won't
        # find conda on PATH.
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn, conda_env="lab")
        await src.setup(tmp_path / "sweep", "test_sweep")
        await src.submit_job({"i": 1}, "task_001", "test_sweep")

        uploads = [c for c in fake_conn.run_calls if c["input"] is not None]
        content = uploads[0]["input"]
        assert "miniconda3/etc/profile.d/conda.sh" in content
        # The source loop runs BEFORE the actual conda invocation.
        idx_source = content.index("conda.sh")
        idx_run = content.index("conda run -n lab")
        assert idx_source < idx_run

    async def test_no_conda_env_skips_init_block(self, tmp_path):
        fake_conn = FakeConn()
        src = _StubSSH(
            fake_conn=fake_conn,
            name="anahita",
            conda_env=None,
            python_path="/usr/bin/python3",
            project_dir=str(tmp_path / "project"),
            script_path="train.py",
            max_parallel_jobs=2,
        )
        (tmp_path / "project").mkdir(exist_ok=True)
        await src.setup(tmp_path / "sweep", "test_sweep")
        await src.submit_job({"i": 1}, "task_001", "test_sweep")

        uploads = [c for c in fake_conn.run_calls if c["input"] is not None]
        content = uploads[0]["input"]
        assert "conda.sh" not in content
        assert "/usr/bin/python3 train.py" in content

    async def test_failed_exit_marks_failed(self, tmp_path):
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn)
        await src.setup(tmp_path / "sweep", "test_sweep")
        job_id = await src.submit_job({"i": 1}, "task_001", "test_sweep")
        fake_conn.processes[0].finish(exit_status=2)
        await src.wait_for_all()
        assert src.completed_jobs[job_id].status == "FAILED"


# --------------------------------------------------------------------- cancel


class TestCancel:
    pytestmark = pytest.mark.asyncio

    async def test_cancel_marks_cancelled(self, tmp_path):
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn)
        await src.setup(tmp_path / "sweep", "test_sweep")
        job_id = await src.submit_job({"i": 1}, "task_001", "test_sweep")

        proc = fake_conn.processes[0]
        ok = await src.cancel_job(job_id)
        assert ok is True
        assert proc.terminated is True
        await src.wait_for_all()
        assert src.completed_jobs[job_id].status == "CANCELLED"


# ------------------------------------------------------------------- collect


class TestCollectResults:
    pytestmark = pytest.mark.asyncio

    async def test_collect_runs_rsync_pull(self, tmp_path):
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn)
        await src.setup(tmp_path / "sweep", "test_sweep")

        # Drop the initial push rsync from the record so the next assert is unambiguous.
        src._rsync_calls.clear()
        ok = await src.collect_results()
        assert ok is True
        assert len(src._rsync_calls) == 1
        pull = src._rsync_calls[0]
        assert pull[0] == "rsync"
        # No --delete on pull.
        assert "--delete" not in pull
        # Source is host:remote_tasks/, destination is local sweep/tasks/.
        assert pull[-2].startswith("anahita:") and pull[-2].endswith("/tasks/")
        assert pull[-1].endswith("/tasks/")

    async def test_collect_cleans_remote_on_success(self, tmp_path):
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn)
        await src.setup(tmp_path / "sweep", "test_sweep")
        job_id = await src.submit_job({"i": 1}, "task_001", "test_sweep")
        fake_conn.processes[0].finish(exit_status=0)
        await src.wait_for_all()

        # Spot the rm -rf call in conn.run history.
        before = len(fake_conn.run_calls)
        await src.collect_results()
        new_calls = fake_conn.run_calls[before:]
        rm_calls = [c for c in new_calls if c["cmd"].startswith("rm -rf")]
        assert len(rm_calls) == 1
        assert "/sweeps/test_sweep" in rm_calls[0]["cmd"]

    async def test_collect_keeps_remote_on_failure(self, tmp_path):
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn)
        await src.setup(tmp_path / "sweep", "test_sweep")
        await src.submit_job({"i": 1}, "task_001", "test_sweep")
        fake_conn.processes[0].finish(exit_status=1)
        await src.wait_for_all()

        before = len(fake_conn.run_calls)
        await src.collect_results()
        new_calls = fake_conn.run_calls[before:]
        rm_calls = [c for c in new_calls if c["cmd"].startswith("rm -rf")]
        assert not rm_calls, "must not rm remote sweep dir when a job FAILED"

    async def test_keep_remote_on_success_flag(self, tmp_path):
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn, keep_remote_on_success=True)
        await src.setup(tmp_path / "sweep", "test_sweep")
        await src.submit_job({"i": 1}, "task_001", "test_sweep")
        fake_conn.processes[0].finish(exit_status=0)
        await src.wait_for_all()

        before = len(fake_conn.run_calls)
        await src.collect_results()
        new_calls = fake_conn.run_calls[before:]
        assert not [c for c in new_calls if c["cmd"].startswith("rm -rf")]


# -------------------------------------------------------------------- health


class TestHealthCheck:
    pytestmark = pytest.mark.asyncio

    async def test_health_check_pings_remote(self, tmp_path):
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn)
        await src.setup(tmp_path / "sweep", "test_sweep")
        info = await src.health_check()
        assert info["status"] == "healthy"
        assert info["connection"] == "ok"
        assert info["host"] == "anahita"
        assert "remote_time" in info

    async def test_health_check_pre_setup(self, tmp_path):
        src = _make_src(tmp_path)
        info = await src.health_check()
        assert info["status"] == "unhealthy"
        assert info["connection"] == "not_connected"


# ------------------------------------------------------------------- cleanup


class TestCleanup:
    pytestmark = pytest.mark.asyncio

    async def test_cleanup_closes_connection(self, tmp_path):
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn)
        await src.setup(tmp_path / "sweep", "test_sweep")
        await src.cleanup()
        assert fake_conn.closed is True
        assert src._conn is None

    async def test_cleanup_cancels_active_jobs(self, tmp_path):
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn)
        await src.setup(tmp_path / "sweep", "test_sweep")
        await src.submit_job({"i": 1}, "task_001", "test_sweep")
        proc = fake_conn.processes[0]
        await src.cleanup()
        assert proc.terminated is True
