"""Unit tests for the push-model SSHComputeSource.

These exercise the orchestration logic with an injected fake asyncssh-like
connection and fake rsync — no real SSH endpoint is required. The pure
helpers (rsync arg shape, GPU partitioning) have their own coverage in
:mod:`test_push_exec`.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
from hpc_sweep_manager.core.remote.ssh_compute_source import SSHComputeSource

# ---------------------------------------------------------------------- fakes

NVIDIA_SMI_SAMPLE_4_GPUS = (
    "0, NVIDIA H100, 12, 81920, 0\n"
    "1, NVIDIA H100, 12, 81920, 0\n"
    "2, NVIDIA H100, 12, 81920, 0\n"
    "3, NVIDIA H100, 12, 81920, 0\n"
)


class _Result:
    """Mimic asyncssh's ``SSHCompletedProcess`` enough for our use."""

    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = ""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class FakeConn:
    """Records run() calls. A launch answers a pid; a poll answers each job's ``states``
    entry ("run" until a test sets "rc 0", "rc 2" or "gone")."""

    def __init__(self, *, gpu_csv: str = "", nvidia_smi_rc: int = 0):
        self.run_calls: list[dict[str, Any]] = []
        self.states: dict[str, str] = {}
        self.closed = False
        self._gpu_csv = gpu_csv
        self._nvidia_smi_rc = nvidia_smi_rc
        self._pid = 4241

    async def run(self, cmd: str, *, input: str | None = None, check: bool = False) -> _Result:
        self.run_calls.append({"cmd": cmd, "input": input, "check": check})
        if "nvidia-smi" in cmd:
            return _Result(returncode=self._nvidia_smi_rc, stdout=self._gpu_csv)
        if cmd.startswith("echo ~"):  # the remote shell expanding remote_root
            return _Result(stdout="/home/fake" + cmd[len("echo ~") :] + "\n")
        if cmd == "date":
            return _Result(returncode=0, stdout="Mon Jan 1 00:00:00 UTC 2026\n")
        if "setsid nohup" in cmd:
            self._pid += 1
            return _Result(stdout=f"{self._pid}\n")
        if cmd.startswith("p() {"):
            jobs = [call.split()[-1] for call in cmd.split("; ") if call.startswith("p ")]
            return _Result(stdout="".join(f"{j} {self.states.get(j, 'run')}\n" for j in jobs))
        return _Result(returncode=0, stdout="")

    def launches(self) -> list[dict[str, Any]]:
        return [c for c in self.run_calls if "setsid nohup" in c["cmd"]]

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:  # pragma: no cover - trivial
        pass


class _StubSSH(SSHComputeSource):
    """Inject a FakeConn + record rsync invocations instead of hitting the network."""

    def __init__(self, *args, fake_conn: FakeConn, rsync_rc: int = 0, **kwargs):
        super().__init__(*args, **kwargs)
        self._fake_conn = fake_conn
        self._rsync_calls: list[list[str]] = []
        self._rsync_rc = rsync_rc

    async def _open_connection(self) -> Any:
        return self._fake_conn

    async def _run_rsync(self, cmd: list[str]) -> int:
        self._rsync_calls.append(cmd)
        return self._rsync_rc


def _make_src(tmp_path: Path, **kwargs) -> _StubSSH:
    fake_conn = kwargs.pop("fake_conn", None) or FakeConn()
    defaults: dict[str, Any] = dict(
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

    def test_minimum_parallel_jobs_is_one(self, tmp_path):
        src = _make_src(tmp_path, max_parallel_jobs=0)
        assert src.max_parallel_jobs == 1

    def test_remote_root_trailing_slash_stripped(self, tmp_path):
        src = _make_src(tmp_path, remote_root="~/.hsm/runs/")
        assert src.remote_root == "~/.hsm/runs"


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
        assert push[-1].endswith("/snapshots/test_sweep/")

    async def test_rsync_skips_an_agent_that_stalled(self, tmp_path, monkeypatch):
        from hpc_sweep_manager.core.remote import discovery

        monkeypatch.setattr(discovery, "_AGENT_STALLED", {"anahita"})
        src = _make_src(tmp_path, fake_conn=FakeConn())
        await src.setup(tmp_path / "sweep", "test_sweep")
        await src.collect_results()
        push, pull = src._rsync_calls
        assert all("-o IdentityAgent=none" in cmd[cmd.index("-e") + 1] for cmd in (push, pull))

    async def test_setup_creates_remote_layout(self, tmp_path):
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn)
        await src.setup(tmp_path / "sweep", "test_sweep")
        # One mkdir -p for this sweep's code snapshot and its tasks/logs/scripts.
        mkdirs = [c for c in fake_conn.run_calls if "mkdir -p" in c["cmd"]]
        assert mkdirs, "expected at least one mkdir -p"
        layout = mkdirs[0]["cmd"]
        assert "/snapshots/test_sweep" in layout
        assert "/sweeps/test_sweep/tasks" in layout
        assert "/sweeps/test_sweep/logs" in layout
        assert "/sweeps/test_sweep/scripts" in layout

    async def test_setup_resolves_tilde_via_remote_home(self, tmp_path):
        # setup() resolves remote_root via `echo <path>`; simulate the remote
        # shell expanding ~ / $USER / $HOME.
        class HomeyConn(FakeConn):
            async def run(self, cmd, *, input=None, check=False):
                self.run_calls.append({"cmd": cmd, "input": input, "check": check})
                if cmd.startswith("echo "):
                    arg = cmd[len("echo ") :].strip()
                    if arg.startswith("~"):
                        arg = "/home/gbena" + arg[1:]
                    arg = arg.replace("$HOME", "/home/gbena").replace("$USER", "gbena")
                    return _Result(returncode=0, stdout=arg + "\n")
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

    @pytest.mark.parametrize(("echo", "ok"), [("", False), ("motd\n/home/x/r\n", True)])
    async def test_setup_needs_an_absolute_root(self, tmp_path, echo, ok):
        # Every remote command quotes its paths, so a "~" left unexpanded would never expand;
        # rc-file noise comes before the path.
        class EchoConn(FakeConn):
            async def run(self, cmd, *, input=None, check=False):
                if cmd.startswith("echo "):
                    return _Result(stdout=echo)
                return await super().run(cmd, input=input, check=check)

        src = _make_src(tmp_path, fake_conn=EchoConn())
        assert await src.setup(tmp_path / "sweep", "test_sweep") is ok

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

    async def test_one_command_writes_the_script_and_starts_it_detached(self, tmp_path):
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn, max_parallel_jobs=2)
        await src.setup(tmp_path / "sweep", "test_sweep")

        job_id = await src.submit_job({"lr": 0.01}, "task_001", "test_sweep")
        assert job_id in src.active_jobs and src._pids[job_id] == 4242

        (launch,) = fake_conn.launches()  # one channel, released at once
        assert "cat >" in launch["cmd"] and "/tasks/task_001/hsm.log" in launch["cmd"]
        assert "lr=0.01" in launch["input"]
        assert "conda run -n lab python" in launch["input"]

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
        # First slot popped is [0]; CUDA_VISIBLE_DEVICES should be set to "0".
        assert "CUDA_VISIBLE_DEVICES=0" in fake_conn.launches()[0]["input"]

    async def test_slot_back_pressure(self, tmp_path):
        """The third submission waits, polling, until a task finishes and frees its slot."""
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn, max_parallel_jobs=2)
        src.slot_poll_s = 0.01
        await src.setup(tmp_path / "sweep", "test_sweep")

        first = await src.submit_job({"i": 1}, "task_001", "test_sweep")
        await src.submit_job({"i": 2}, "task_002", "test_sweep")
        third = asyncio.create_task(src.submit_job({"i": 3}, "task_003", "test_sweep"))
        await asyncio.sleep(0.05)
        assert not third.done(), "third submit should wait on a slot"

        fake_conn.states[first] = "rc 0"
        await asyncio.wait_for(third, timeout=1.0)
        assert src.completed_jobs[first].status == "COMPLETED"

    @pytest.mark.parametrize(
        ("state", "status"), [("rc 0", "COMPLETED"), ("rc 2", "FAILED"), ("gone", "FAILED")]
    )
    async def test_a_poll_settles_finished_jobs(self, tmp_path, state, status):
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn)
        await src.setup(tmp_path / "sweep", "test_sweep")
        job_id = await src.submit_job({"i": 1}, "task_001", "test_sweep")
        await src.update_all_job_statuses()
        assert job_id in src.active_jobs  # "run"
        fake_conn.states[job_id] = state
        await src.wait_for_all(poll_interval=0)
        assert src.completed_jobs[job_id].status == status

    async def test_one_command_per_poll_whatever_the_task_count(self, tmp_path):
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn, max_parallel_jobs=8)
        await src.setup(tmp_path / "sweep", "test_sweep")
        for i in range(8):
            await src.submit_job({"i": i}, f"task_{i:03d}", "test_sweep")
        before = len(fake_conn.run_calls)
        await src.update_all_job_statuses()
        assert len(fake_conn.run_calls) == before + 1

    async def test_conda_env_emits_init_source_block(self, tmp_path):
        # conda_env="lab" → the rendered script must `. <conda.sh>` before
        # invoking `conda run`, otherwise non-interactive ssh shells won't
        # find conda on PATH.
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn, conda_env="lab")
        await src.setup(tmp_path / "sweep", "test_sweep")
        await src.submit_job({"i": 1}, "task_001", "test_sweep")

        content = fake_conn.launches()[0]["input"]
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

        content = fake_conn.launches()[0]["input"]
        assert "conda.sh" not in content
        assert "/usr/bin/python3 train.py" in content


class FlakyConn(FakeConn):
    """A FakeConn whose next ``drops`` commands raise as a dead connection would."""

    def __init__(self, drops: int = 0, **kw):
        super().__init__(**kw)
        self.drops = drops

    async def run(self, cmd: str, *, input: str | None = None, check: bool = False) -> _Result:
        if self.drops and not cmd.startswith(("echo", "mkdir", "nvidia-smi")):
            self.drops -= 1
            raise ConnectionResetError("connection lost")
        return await super().run(cmd, input=input, check=check)


class TestConnectionLoss:
    """Tracker S11 (ssh side): a blip reconnects once and never changes a status."""

    pytestmark = pytest.mark.asyncio

    async def test_a_dropped_poll_reconnects_and_keeps_states(self, tmp_path):
        conn = FlakyConn()
        src = _make_src(tmp_path, fake_conn=conn)
        await src.setup(tmp_path / "sweep", "test_sweep")
        job_id = await src.submit_job({"i": 1}, "task_001", "test_sweep")
        conn.drops = 1
        await src.update_all_job_statuses()  # dropped, reopened, polled
        assert src.active_jobs[job_id].status == "RUNNING"

    async def test_a_dead_link_keeps_states(self, tmp_path):
        conn = FlakyConn()
        src = _make_src(tmp_path, fake_conn=conn)
        await src.setup(tmp_path / "sweep", "test_sweep")
        job_id = await src.submit_job({"i": 1}, "task_001", "test_sweep")
        conn.drops = 2  # the reconnect's command fails too
        await src.update_all_job_statuses()
        assert src.active_jobs[job_id].status == "RUNNING"

    async def test_a_launch_that_keeps_failing_is_failed_and_frees_its_slot(self, tmp_path):
        conn = FlakyConn()
        src = _make_src(tmp_path, fake_conn=conn, max_parallel_jobs=1)
        await src.setup(tmp_path / "sweep", "test_sweep")
        conn.drops = 6  # three launches, each with its one reconnect
        job_id = await src.submit_job({"i": 1}, "task_001", "test_sweep")
        assert src.completed_jobs[job_id].status == "FAILED"
        assert src._slot_queue.qsize() == 1


# --------------------------------------------------------------------- cancel


class TestCancel:
    pytestmark = pytest.mark.asyncio

    async def test_cancel_terms_the_process_group(self, tmp_path):
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn)
        await src.setup(tmp_path / "sweep", "test_sweep")
        job_id = await src.submit_job({"i": 1}, "task_001", "test_sweep")

        assert await src.cancel_job(job_id) is True
        assert fake_conn.run_calls[-1]["cmd"].startswith("kill -TERM -- -4242")
        assert src.completed_jobs[job_id].status == "CANCELLED"
        assert await src.cancel_job(job_id) is False  # already done


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

    async def _collect_after(self, tmp_path, state, **kw) -> list[str]:
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn, **kw)
        await src.setup(tmp_path / "sweep", "test_sweep")
        job_id = await src.submit_job({"i": 1}, "task_001", "test_sweep")
        if state:
            fake_conn.states[job_id] = state
            await src.wait_for_all(poll_interval=0)
        before = len(fake_conn.run_calls)
        await src.collect_results()
        return [c["cmd"] for c in fake_conn.run_calls[before:] if c["cmd"].startswith("rm -rf")]

    async def test_collect_cleans_remote_on_success(self, tmp_path):
        (rm,) = await self._collect_after(tmp_path, "rc 0")
        assert "/sweeps/test_sweep" in rm

    async def test_collect_keeps_remote_on_failure(self, tmp_path):
        assert not await self._collect_after(tmp_path, "rc 1")

    async def test_keep_remote_on_success_flag(self, tmp_path):
        assert not await self._collect_after(tmp_path, "rc 0", keep_remote_on_success=True)

    async def test_never_removes_the_dir_of_a_running_task(self, tmp_path):
        # e.g. a collect after an interrupted wait: the task is still running there.
        assert not await self._collect_after(tmp_path, None)


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

    async def test_cleanup_leaves_running_tasks_running(self, tmp_path, caplog):
        fake_conn = FakeConn()
        src = _make_src(tmp_path, fake_conn=fake_conn)
        await src.setup(tmp_path / "sweep", "test_sweep")
        await src.submit_job({"i": 1}, "task_001", "test_sweep")
        await src.cleanup()
        assert not [c for c in fake_conn.run_calls if c["cmd"].startswith("kill")]
        assert "ssh anahita kill -TERM -- -4242" in caplog.text
