"""Detached ssh tasks, run for real: the remote side is a local bash (tracker X1, R12).

The connection runs each command in ``bash -c`` under a temp ``$HOME``; rsync is a copy. So the
wrapper's traps, ``setsid nohup``, the one-command poll and the process-group kill all run as on
a remote, with a stub training script that sleeps, prints and exits with a chosen code.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import signal
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

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

    async def run(self, cmd, *, input=None, check=False):
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
    assert box.completed_jobs[job].status == "CANCELLED"


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


async def test_a_repeated_launch_finds_the_running_task(box, tmp_path):
    # A launch whose reply was lost is retried: it must not start a second copy.
    assert await box.setup(tmp_path / "sweep", "s1")
    job = await box.submit_job({"sleep": 30}, "s1_task_001", "s1")
    await _until(lambda: (_remote_task(box) / ".hsm_pid").exists())
    launch, script = next(c for c in box._conn.calls if "setsid nohup" in c[0])
    again = await box._conn.run(launch, input=script)
    assert again.stdout.strip() == str(box._pids[job])
