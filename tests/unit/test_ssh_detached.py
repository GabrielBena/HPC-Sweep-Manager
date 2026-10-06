"""Detached ssh tasks, run for real: the remote side is a local bash (tracker X1, R12).

The connection runs each command in ``bash -c`` under a temp ``$HOME``; rsync is a copy. So the
wrapper's traps, ``setsid nohup``, the one-command poll and the process-group kill all run as on
a remote, with a stub training script that sleeps, prints and exits with a chosen code.
"""

from __future__ import annotations

import asyncio
import io
import json
import os
import shutil
import signal
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from rich.console import Console

from hpc_sweep_manager.cli.sweep import _collect_ssh
from hpc_sweep_manager.core.remote.ssh_compute_source import SSHComputeSource

pytestmark = pytest.mark.asyncio

TRAIN = """import sys, time
args = dict(a.split("=", 1) for a in sys.argv[1:])
time.sleep(float(args.get("sleep", 0)))
print("trained", args.get("code", "0"))
sys.exit(int(args.get("code", 0)))
"""


class BashConn:
    """An asyncssh-like connection whose commands run in a local bash."""

    def __init__(self, home: Path):
        self.home, self.calls = home, []

    async def run(self, cmd, *, input=None, check=False, **kw):
        self.calls.append((cmd, input))
        proc = await asyncio.create_subprocess_exec(
            "bash",
            "-c",
            cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ, "HOME": str(self.home)},
        )
        out, err = await proc.communicate((input or "").encode())
        return SimpleNamespace(returncode=proc.returncode, stdout=out.decode(), stderr=err.decode())

    def close(self):
        pass

    async def wait_closed(self):
        pass


class LocalBox(SSHComputeSource):
    async def _open_connection(self):
        return BashConn(self.home)

    async def _run_rsync(self, cmd):
        src, dst = (arg.split(":", 1)[-1] for arg in cmd[-2:])
        shutil.copytree(src, dst, dirs_exist_ok=True)
        return 0


@pytest.fixture
def box(tmp_path):
    """A set-up-able source on the local box; kills any task group a test leaves behind."""
    (tmp_path / "home").mkdir()
    (tmp_path / "project").mkdir()
    (tmp_path / "project" / "train.py").write_text(TRAIN)
    src = LocalBox(
        name="box",
        project_dir=str(tmp_path / "project"),
        script_path="train.py",
        remote_root="~/runs",
        python_path=sys.executable,
        max_parallel_jobs=2,
    )
    src.home, src.slot_poll_s = tmp_path / "home", 0.05
    yield src
    for pid_file in (tmp_path / "home").rglob(".hsm_pid"):
        try:
            os.killpg(int(pid_file.read_text()), signal.SIGKILL)
        except (ProcessLookupError, ValueError):
            pass


def _remote_task(src, name="task_001") -> Path:
    return Path(src._remote_sweep_dir) / "tasks" / name


async def _until(predicate, timeout=10.0):
    for _ in range(int(timeout / 0.05)):
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("condition not met in time")


def _group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False


async def test_tasks_run_detached_to_their_exit_codes_and_logs(box, tmp_path):
    assert await box.setup(tmp_path / "sweep", "s1")
    ok = await box.submit_job({"code": 0}, "s1_task_001", "s1")
    bad = await box.submit_job({"code": 3}, "s1_task_002", "s1")
    statuses = await box.wait_for_all(poll_interval=0.05)
    assert statuses == {ok: "COMPLETED", bad: "FAILED"}
    assert await box.collect_results()
    log = (tmp_path / "sweep" / "tasks" / "task_002" / "hsm.log").read_text()
    assert "trained 3" in log  # the task's output is a file now, pulled with its results


async def test_cancel_ends_the_whole_process_group(box, tmp_path):
    assert await box.setup(tmp_path / "sweep", "s1")
    job = await box.submit_job({"sleep": 30}, "s1_task_001", "s1")
    pgid = box._pids[job]
    await _until(lambda: (_remote_task(box) / ".hsm_pid").exists())
    assert await box.cancel_job(job)
    await _until(lambda: not _group_alive(pgid))  # the python child too, not only the wrapper
    assert (_remote_task(box) / ".hsm_rc").read_text().strip() == "143"
    assert await box.wait_for_all(poll_interval=0.05) == {job: "CANCELLED"}


async def test_cleanup_leaves_a_task_running_to_its_end(box, tmp_path):
    assert await box.setup(tmp_path / "sweep", "s1")
    await box.submit_job({"sleep": 0.5}, "s1_task_001", "s1")
    await box.cleanup()  # the launcher goes away (Ctrl-C, a lost laptop)
    rc = _remote_task(box) / ".hsm_rc"
    await _until(rc.exists)
    assert rc.read_text().strip() == "0"


async def test_a_task_killed_hard_is_failed(box, tmp_path):
    assert await box.setup(tmp_path / "sweep", "s1")
    job = await box.submit_job({"sleep": 30}, "s1_task_001", "s1")
    await _until(lambda: (_remote_task(box) / ".hsm_pid").exists())
    os.killpg(box._pids[job], signal.SIGKILL)  # no trap runs: no exit code is written
    await _until(lambda: not _group_alive(box._pids[job]))
    assert await box.wait_for_all(poll_interval=0.05) == {job: "FAILED"}


@pytest.mark.parametrize("sleep", [30, 0])
async def test_a_repeated_launch_finds_the_task_running_or_done(box, tmp_path, sleep):
    # A launch whose reply was lost is retried: it must never start the task again.
    assert await box.setup(tmp_path / "sweep", "s1")
    job = await box.submit_job({"sleep": sleep, "code": 4}, "s1_task_001", "s1")
    if not sleep:
        await _until((_remote_task(box) / ".hsm_rc").exists)
    launch, script = next(c for c in box._conn.calls if "setsid nohup" in c[0])
    again = await box._conn.run(launch, input=script)
    assert again.stdout.strip() == str(box._pids[job])
    assert (_remote_task(box) / "hsm.log").read_text().count("trained") == (0 if sleep else 1)


async def test_a_task_runs_while_any_of_its_group_does(box, tmp_path):
    # The wrapper killed alone: its training process still holds the GPU, so not "gone".
    assert await box.setup(tmp_path / "sweep", "s1")
    job = await box.submit_job({"sleep": 30}, "s1_task_001", "s1")
    pgid = box._pids[job]
    await _until(lambda: "train.py" in os.popen(f"pgrep -g {pgid} -a").read())
    os.kill(pgid, signal.SIGKILL)  # the wrapper only
    await box.update_all_job_statuses()
    assert job in box.active_jobs
    os.killpg(pgid, signal.SIGKILL)
    await _until(lambda: not _group_alive(pgid))
    assert await box.wait_for_all(poll_interval=0.05) == {job: "FAILED"}


def _reach_local_box(box, monkeypatch) -> None:
    """A source re-attached from the manifest (collect, cancel) reaches the same local box."""

    async def open_local(self):
        return BashConn(box.home)

    monkeypatch.setattr(SSHComputeSource, "_open_connection", open_local)
    monkeypatch.setattr(SSHComputeSource, "_run_rsync", LocalBox._run_rsync)


async def _collect(box, tmp_path, monkeypatch) -> str:
    """Run `hsm sweep collect`'s ssh path from the manifest, against the same local box."""
    _reach_local_box(box, monkeypatch)
    manifest = json.loads((tmp_path / "sweep" / ".hsm_manifest.json").read_text())
    out = io.StringIO()
    await _collect_ssh(tmp_path / "sweep", manifest, Console(file=out, width=300))
    return out.getvalue()


async def test_collect_re_attaches_after_the_launcher_is_gone(box, tmp_path, monkeypatch):
    assert await box.setup(tmp_path / "sweep", "s1")
    await box.submit_batch([{"code": 0}, {"code": 5}, {"sleep": 30}], "s1")
    rcs = [_remote_task(box, f"task_00{i}") / ".hsm_rc" for i in (1, 2)]
    await box.cleanup()  # the launcher dies; its tasks don't
    await _until(lambda: all(rc.exists() for rc in rcs))
    out = await _collect(box, tmp_path, monkeypatch)
    assert "2 task(s) ended (1 not COMPLETED), 1 still running" in out
    assert "trained 5" in (tmp_path / "sweep" / "tasks" / "task_002" / "hsm.log").read_text()
    assert Path(box._remote_sweep_dir).is_dir()  # kept: a task still runs there


async def test_a_finished_sweep_is_collected_and_cleaned_once(box, tmp_path, monkeypatch):
    assert await box.setup(tmp_path / "sweep", "s1")
    await box.submit_batch([{"code": 0}, {"code": 0}], "s1")
    await box.cleanup()
    await _until(lambda: (_remote_task(box, "task_002") / ".hsm_rc").exists())
    assert "2 task(s) ended (0 not COMPLETED), 0 still running" in await _collect(
        box, tmp_path, monkeypatch
    )
    assert not Path(box._remote_sweep_dir).exists()
    assert "Nothing left to collect" in await _collect(box, tmp_path, monkeypatch)


def _drop_pid(tmp_path, task: str, *, dir_too: bool = False) -> None:
    """Rewrite the manifest as the launcher left it had it died mid-launch: no pid yet."""
    path = tmp_path / "sweep" / ".hsm_manifest.json"
    manifest = json.loads(path.read_text())
    for info in manifest["tasks"].values():
        if info["name"].endswith(task):
            info["pid"] = None
            if dir_too:
                info["dir"] += "_never_started"
    path.write_text(json.dumps(manifest))


async def test_a_task_listed_without_its_pid_keeps_the_dir(box, tmp_path, monkeypatch):
    assert await box.setup(tmp_path / "sweep", "s1")
    await box.submit_batch([{"code": 0}, {"sleep": 30}], "s1")
    await box.cleanup()
    _drop_pid(tmp_path, "task_002")  # the launcher died before the reply came back
    await _until((_remote_task(box, "task_001") / ".hsm_rc").exists)
    out = await _collect(box, tmp_path, monkeypatch)
    assert "1 task(s) ended (0 not COMPLETED), 1 still running" in out  # its pid was read back
    assert Path(box._remote_sweep_dir).is_dir()


async def test_a_listed_task_that_never_started_is_failed_and_keeps_the_dir(
    box, tmp_path, monkeypatch
):
    assert await box.setup(tmp_path / "sweep", "s1")
    await box.submit_batch([{"code": 0}, {"code": 0}], "s1")
    await box.cleanup()
    _drop_pid(tmp_path, "task_002", dir_too=True)
    await _until((_remote_task(box, "task_001") / ".hsm_rc").exists)
    assert "2 task(s) ended (1 not COMPLETED)" in await _collect(box, tmp_path, monkeypatch)
    assert Path(box._remote_sweep_dir).is_dir()


async def test_no_collect_while_the_launcher_runs(box, tmp_path, monkeypatch):
    # It may start a task during the collect, whose rm -rf would remove it.
    assert await box.setup(tmp_path / "sweep", "s1")
    await box.submit_batch([{"code": 0}], "s1")  # holds the launcher lock until cleanup()
    await _until((_remote_task(box, "task_001") / ".hsm_rc").exists)
    assert "launcher is still running" in await _collect(box, tmp_path, monkeypatch)
    assert Path(box._remote_sweep_dir).is_dir()
    await box.cleanup()


async def test_a_task_not_yet_its_own_group_is_running(tmp_path):
    # Just after launch the task has a pid but hasn't called setsid: no group by that id yet.
    # A poll then must say "run", not "gone" (a CI run caught that race).
    import subprocess

    from hpc_sweep_manager.core.remote.ssh_compute_source import _POLL_FN

    proc = subprocess.Popen(["sleep", "30"])  # pytest's group, as a task is before setsid
    try:
        poll = subprocess.run(
            ["bash", "-c", f"{_POLL_FN}; p {tmp_path} {proc.pid} j"],
            capture_output=True,
            text=True,
        )
        assert poll.stdout.split() == ["j", "run"]
    finally:
        proc.kill()
        proc.wait()


async def test_cancel_ends_the_running_tasks_and_collect_keeps_the_dir(box, tmp_path, monkeypatch):
    from hpc_sweep_manager.cli.sweep import _cancel_ssh

    assert await box.setup(tmp_path / "sweep", "s1")
    await box.submit_batch([{"code": 0}, {"sleep": 30}], "s1")
    rc = _remote_task(box, "task_001") / ".hsm_rc"
    await _until(rc.exists)
    out = io.StringIO()
    manifest = json.loads((tmp_path / "sweep" / ".hsm_manifest.json").read_text())
    assert await _cancel_ssh(tmp_path / "sweep", manifest, Console(file=out)) == 1  # launcher on
    assert "launcher is still running" in out.getvalue()
    await box.cleanup()  # the launcher stops; its task runs on until cancelled
    _reach_local_box(box, monkeypatch)
    assert await _cancel_ssh(tmp_path / "sweep", manifest, Console(file=out)) == 0
    assert "Sent TERM to 1 of 1 running task(s)" in out.getvalue()
    killed = _remote_task(box, "task_002") / ".hsm_rc"
    await _until(killed.exists)
    assert killed.read_text().strip() == "143"
    assert "2 task(s) ended (1 not COMPLETED)" in await _collect(box, tmp_path, monkeypatch)
    assert Path(box._remote_sweep_dir).is_dir()  # a cancelled task is no success: kept
