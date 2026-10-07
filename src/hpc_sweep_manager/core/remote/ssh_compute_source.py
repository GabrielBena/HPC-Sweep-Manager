"""Push-model SSH compute source.

Mirrors :class:`LocalComputeSource` shape — one persistent asyncssh
connection, a slot ``asyncio.Queue`` for back-pressure, a per-job monitor
coroutine — but ships the work to a remote box. The model:

    1. setup(): open ssh; rsync the local project up to this sweep's code
       snapshot (``~/.hsm/runs/<project>/snapshots/<sweep_id>/``); probe
       ``nvidia-smi`` and partition its GPUs into slots; create a per-sweep dir
       on the remote.
    2. submit_job(): take a free slot, render the wrapper template, and in one short
       command write it and start it detached (``setsid nohup``), getting its pid. The
       task holds no channel and outlives the launcher; its output goes to
       ``tasks/<task>/hsm.log``.
    3. update_all_job_statuses(): ONE command per poll for every running task reads its
       ``.hsm_rc`` (or sees it still running, or gone); a finished task frees its slot.
       A dropped connection is reopened once and never changes a status.
    4. collect_results(): rsync ``sweeps/<id>/tasks/`` back; on full success
       ``rm -rf`` the per-sweep remote dir and its code snapshot.
    5. cleanup(): close the connection. Tasks still running keep running.

The class delegates command-shape decisions to pure helpers in
:mod:`push_exec` and parses GPU output via :mod:`gpu_probe`, so it stays
unit-testable by overriding the two narrow I/O seams
:meth:`_open_connection` and :meth:`_run_rsync`.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import re
import shlex
import time
from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import asyncssh

from ..common.compute_source import ComputeSource, JobInfo
from ..common.resource_spec import ResourceSpec
from ..common.templating import params_to_hydra_args, params_to_yaml, render_template
from .discovery import LINK_GIVE_UP_S, agent_stalled
from .gpu_probe import NVIDIA_SMI_QUERY, parse_nvidia_smi_csv
from .push_exec import (
    DEFAULT_RSYNC_EXCLUDES,
    build_rsync_pull_cmd,
    build_rsync_push_cmd,
    check_gpu_slots,
    cpu_only,
    normalize_gpu_allowlist,
    own_snapshot,
    partition_gpu_slots,
    pin_code_refs,
    remote_interpreter,
    resolve_run_prefix,
    run_rsync,
    snapshot_prepare_cmd,
)

logger = logging.getLogger(__name__)

LAUNCH_TRIES = 3  # a task whose launch fails this often is FAILED
RUN_TIMEOUT_S = 300  # a remote command that takes longer counts as a dropped link

# One poll for every running task: `p <task dir> <pid> <job id>` prints "<job> rc <code>",
# "<job> run" (its process group is alive, or its pid: just after launch, before setsid has
# made it a group) or "<job> gone" (dead without an exit code: killed hard, or the host
# rebooted). The second rc check closes the race with a task that ends between the first and
# `kill -0`; `-s` reads a half-written rc file as still running.
_POLL_FN = (
    'p() { if [ -s "$1/.hsm_rc" ]; then echo "$3 rc $(cat "$1/.hsm_rc")"; '
    'elif kill -0 -- -"$2" 2>/dev/null || kill -0 "$2" 2>/dev/null; then echo "$3 run"; '
    'elif [ -s "$1/.hsm_rc" ]; then echo "$3 rc $(cat "$1/.hsm_rc")"; '
    'else echo "$3 gone"; fi; }'
)


_KILL_USER_PROCESSES = (
    "busctl get-property org.freedesktop.login1 /org/freedesktop/login1 "
    "org.freedesktop.login1.Manager KillUserProcesses 2>/dev/null"
)


def launcher_lock(sweep_dir: Path) -> Any:
    """Take ``<sweep_dir>/.hsm_launcher.lock`` (an open file; closing it releases the lock,
    and so does the process ending, even killed), or None when another process holds it.
    A launcher holds it while it drives the sweep; ``hsm sweep collect`` refuses without it.
    A resumable chain's driver (its launcher, or ``hsm sweep advance``) holds it too."""
    lock = open(sweep_dir / ".hsm_launcher.lock", "a")  # noqa: SIM115 — held past this call
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock.close()
        return None
    return lock


class SSHComputeSource(ComputeSource):
    """Self-contained push-model SSH compute source.

    The remote needs only ``bash`` + ``rsync`` (over the system ssh) +
    optionally ``nvidia-smi``; HSM doesn't have to be installed there. All
    paths in ``project_dir`` / ``script_path`` are LOCAL — the remote sees a
    rsync'd mirror under ``remote_root``.
    """

    def __init__(
        self,
        name: str,
        host: str | None = None,
        ssh_key: str | None = None,
        ssh_port: int | None = None,
        conda_env: str | None = None,
        python_path: str | None = None,
        project_dir: str = ".",
        script_path: str = "",
        remote_root: str = "~/.hsm/runs",
        max_parallel_jobs: int = 1,
        gpus: None | int | Sequence[int] = None,
        default_spec: ResourceSpec | None = None,
        rsync_excludes: Sequence[str] | None = None,
        keep_remote_on_success: bool = False,
    ):
        super().__init__(name, "ssh_remote", max(max_parallel_jobs, 1))
        # host defaults to the source name (which doubles as the ssh-config alias)
        self.host = host or name
        self.ssh_key = ssh_key
        self.ssh_port = ssh_port
        self.conda_env = conda_env
        self.python_path = python_path
        self.project_dir = str(Path(project_dir).resolve())
        # The rendered remote script does `cd <remote_code_dir>` (the rsync'd
        # mirror) then runs `python <script_path>`. If the caller passed an
        # absolute LOCAL path inside project_dir, convert it to relative so it
        # resolves against the remote mirror; otherwise keep as-is.
        if script_path and Path(script_path).is_absolute():
            try:
                script_path = str(Path(script_path).relative_to(self.project_dir))
            except ValueError:
                logger.warning(
                    f"SSHComputeSource {name!r}: script_path {script_path!r} is "
                    f"absolute and outside project_dir {self.project_dir!r}; "
                    f"the rendered remote command will reference this LOCAL path."
                )
        self.script_path = script_path
        self.remote_root = remote_root.rstrip("/")
        self._gpus_config = gpus
        self.default_spec = default_spec or ResourceSpec()
        self.rsync_excludes = (
            # Extend the defaults (dedup, order-preserving) rather than replace —
            # so a user adding `outputs/` doesn't silently start pushing `.git`.
            tuple(dict.fromkeys((*DEFAULT_RSYNC_EXCLUDES, *rsync_excludes)))
            if rsync_excludes is not None
            else DEFAULT_RSYNC_EXCLUDES
        )
        self.keep_remote_on_success = keep_remote_on_success

        # Populated in setup()
        self._conn: Any = None
        self._project_name = Path(self.project_dir).name or "project"
        self._remote_code_dir: str | None = None
        self._remote_sweep_dir: str | None = None
        self.sweep_dir: Path | None = None
        self.sweep_id: str | None = None
        self._gpu_indices: list[int] = []
        self._slot_queue: asyncio.Queue | None = None
        self._slot_count: int = max_parallel_jobs
        self._run_prefix: str = "python"

        # Job bookkeeping: each running task's pid, remote task dir and slot
        self._pids: dict[str, int] = {}
        self._task_dirs: dict[str, str] = {}
        self._slots: dict[str, Any] = {}
        self.slot_poll_s = 10.0  # how often a submit waiting for a slot polls
        self._manifest = False  # keep .hsm_manifest.json current (only as the sweep's own source)
        self._lock: Any = None  # the sweep's launcher lock, held from submit_batch to cleanup()
        self._cancelled: set[str] = set()  # TERM sent, not yet ended
        self._down_since: float | None = None  # when the link went down (None: up)
        self._warned = False
        self._job_counter: int = 0
        self._counter_lock: asyncio.Lock | None = None

    # ------------------------------------------------------------- I/O seams
    async def _open_connection(self) -> Any:
        """Open the persistent asyncssh connection. Overridden in tests.

        With a keepalive: a dead link closes within ~90 s, and :meth:`_run` reconnects."""
        from .discovery import create_ssh_connection

        return await create_ssh_connection(
            self.host, self.ssh_key, self.ssh_port, keepalive_interval=30
        )

    async def _run_rsync(self, cmd: list[str]) -> int:
        """Run an rsync command (a dropped link is retried); return its exit code. Overridden in
        tests."""
        return await run_rsync(cmd, self.host)

    async def _resolve_remote_path(self, path: str) -> str:
        """Expand ~ / $USER / $HOME on the remote, once at setup.

        The rsync destination (built locally) and ``output.dir=<path>`` (passed
        inside a quoted COMMAND) get a literal path with no shell to expand env
        vars — so ``remote_root: /scratch/$USER/...`` would otherwise create a
        literal ``$USER`` dir. A remote-shell ``echo`` (unquoted) expands both
        ``~`` and ``$VAR`` in one shot.
        """
        if not path.startswith(("/", "~", "$")):
            path = f"~/{path}"  # relative to the login dir, as ssh reads it
        if "~" in path or "$" in path:
            result = await self._conn.run(f"echo {path}", check=False)
            lines = (result.stdout or "").strip().splitlines()
            path = lines[-1].strip() if lines else ""  # the last line: rc-file noise comes first
        if not path.startswith("/") or any(c.isspace() for c in path):
            raise RuntimeError(
                f"{self.host}: the remote root must resolve to an absolute path without "
                f"spaces, got {path!r}"
            )
        return path

    # ------------------------------------------------------------------ setup
    async def setup(self, sweep_dir: Path, sweep_id: str) -> bool:
        sweep_dir.mkdir(parents=True, exist_ok=True)
        for sub in ("logs", "scripts", "tasks"):
            (sweep_dir / sub).mkdir(parents=True, exist_ok=True)
        self.sweep_dir = sweep_dir
        self.sweep_id = sweep_id
        self._counter_lock = asyncio.Lock()

        try:
            self._conn = await self._open_connection()
        except Exception as e:  # noqa: BLE001 — surface as setup failure
            logger.error(f"SSH connection to {self.host} failed: {e}")
            self.stats.health_status = "unhealthy"
            return False

        # GPU probe, checked before anything is written there. A box with no nvidia-smi gives []
        # (CPU slots); a probe with no answer (a dropped link, a hung driver) can't say the box
        # has no GPU, so a GPU job stops here rather than run on CPU.
        gpus, answered = [], False
        try:
            result = await self._conn.run(NVIDIA_SMI_QUERY, check=False, timeout=RUN_TIMEOUT_S)
            answered = result.returncode is not None
            if result.returncode == 0:
                gpus = parse_nvidia_smi_csv(result.stdout or "")
        except Exception as e:  # noqa: BLE001
            logger.debug(f"GPU probe on {self.host} failed: {e}")
        if not answered and self.default_spec.gpus:
            logger.error(f"{self.name}@{self.host}: the GPU probe (nvidia-smi) gave no answer")
            self.stats.health_status = "unhealthy"
            return False
        gpu_indices = self._gpu_indices = [g.index for g in gpus]
        busy = [g.index for g in gpus if not g.is_free]

        allowed = normalize_gpu_allowlist(self._gpus_config, gpu_indices, busy)
        gpus_per_job = self.default_spec.gpus or 0
        cpu = cpu_only(self._gpus_config)
        slots = partition_gpu_slots(allowed, gpus_per_job, self.max_parallel_jobs, cpu)
        if not check_gpu_slots(f"{self.name}@{self.host}", slots, gpus_per_job, gpu_indices, busy):
            self.stats.health_status = "unhealthy"
            return False

        # Resolve ~ / $USER / $HOME in remote_root to an absolute path.
        # `cd ~/path` and `mkdir -p ~/path` expand tilde, but values like
        # `output.dir=~/path` (or `/scratch/$USER/...`) passed to python inside
        # quoted COMMAND strings do NOT — nor does the locally-run rsync. One
        # remote-shell echo at setup gives a single absolute path used everywhere.
        try:
            resolved_root = await self._resolve_remote_path(self.remote_root)
        except RuntimeError as e:  # every remote command below needs an absolute path
            logger.error(f"SSHComputeSource {self.name}: {e}")
            self.stats.health_status = "unhealthy"
            return False
        project_root = f"{resolved_root}/{self._project_name}"
        self._remote_code_dir = f"{project_root}/snapshots/{sweep_id}"
        self._remote_sweep_dir = f"{project_root}/sweeps/{sweep_id}"

        # This sweep's code snapshot and dirs, hard-linked against the newest snapshot (S4).
        sweep_dirs = [f"{self._remote_sweep_dir}/{d}" for d in ("tasks", "logs", "scripts")]
        prep = await self._conn.run(
            snapshot_prepare_cmd(project_root, sweep_id, sweep_dirs), check=False
        )
        push_cmd = build_rsync_push_cmd(
            local_dir=self.project_dir,
            host=self.host,
            remote_dir=self._remote_code_dir,
            excludes=self.rsync_excludes,
            agentless=agent_stalled(self.host),
            link_dest=(prep.stdout or "").strip().rstrip("/") or None,
        )
        logger.info(f"rsync push to {self.host}:{self._remote_code_dir}")
        rc = await self._run_rsync(push_cmd)
        if rc != 0:
            self.stats.health_status = "unhealthy"
            return False
        pre_script = pin_code_refs(self.default_spec.pre_script, self._project_name)
        self.default_spec = replace(self.default_spec, pre_script=pre_script)

        # A host whose logind kills a user's processes at logout ends detached tasks with the
        # launcher's session.
        logind = await self._conn.run(_KILL_USER_PROCESSES, check=False)
        if "true" in (logind.stdout or ""):
            logger.warning(
                f"{self.host} kills a user's processes at logout (logind KillUserProcesses): "
                f"tasks end with this launcher's ssh session; ask its admin to exempt you"
            )

        # At most one task per slot, so a distributed dispatcher never queues one behind a slot.
        self._slot_count = len(slots)
        self.max_parallel_jobs = min(self.max_parallel_jobs, self._slot_count)
        self._slot_queue = asyncio.Queue()
        for s in slots:
            self._slot_queue.put_nowait(s)

        self._run_prefix = resolve_run_prefix(self.conda_env, self.python_path)

        slot_desc = (
            f"{self._slot_count} GPU slot(s) ({gpus_per_job}/slot, allowed={allowed})"
            if slots[0]
            else f"{self._slot_count} CPU slot(s)"
        )
        logger.info(
            f"SSHComputeSource {self.name}@{self.host}: {slot_desc}, "
            f"run_prefix={self._run_prefix!r}, "
            f"detected_gpus={gpu_indices}"
            + (f", busy GPUs skipped: {busy}" if self._gpus_config != "all" and busy else "")
        )

        self.stats.health_status = "healthy"
        self.stats.last_health_check = datetime.now()
        return True

    # ----------------------------------------------------------------- submit
    async def _next_job_id(self) -> str:
        assert self._counter_lock is not None
        async with self._counter_lock:
            self._job_counter += 1
            return f"ssh_{self.name}_{self._job_counter}"

    def _local_task_dir_for(self, job_name: str) -> Path:
        assert self.sweep_dir is not None
        m = re.search(r"task_(\d+)", job_name)
        task_name = f"task_{m.group(1)}" if m else job_name
        return self.sweep_dir / "tasks" / task_name

    async def submit_job(
        self,
        params: dict[str, Any],
        job_name: str,
        sweep_id: str,
        wandb_group: str | None = None,
        spec: ResourceSpec | None = None,
    ) -> str:
        if self._conn is None or self._slot_queue is None:
            raise RuntimeError(f"SSHComputeSource {self.name!r} not set up; call setup() first")

        effective_spec = self.default_spec.merge(spec)
        job_id = await self._next_job_id()
        local_task_dir = self._local_task_dir_for(job_name)
        local_task_dir.mkdir(parents=True, exist_ok=True)
        remote_task_dir = f"{self._remote_sweep_dir}/tasks/{local_task_dir.name}"

        slot = await self._acquire_slot()
        cuda_visible = None if slot is None else ",".join(str(i) for i in slot)

        script_content = render_template(
            "ssh_compute_source.sh.j2",
            job_name=job_name,
            job_id=job_id,
            params_hydra=params_to_hydra_args(params),
            params_yaml=params_to_yaml(params),
            wandb_group=wandb_group or sweep_id,
            hydra_overrides=self.hydra_overrides,
            cuda_visible_devices=cuda_visible,
            modules=list(effective_spec.modules),
            pre_script=list(effective_spec.pre_script),
            remote_code_dir=self._remote_code_dir,
            remote_task_dir=remote_task_dir,
            run_prefix=self._run_prefix,
            script_path=self.script_path,
            uses_conda=bool(self.conda_env),
            conda_env=self.conda_env,
        )
        script = shlex.quote(f"{self._remote_sweep_dir}/scripts/{job_name}.sh")
        task = shlex.quote(remote_task_dir)
        # Idempotent: the pid file is written before the reply, so a retry after a lost reply
        # finds the task (running or done) and never starts it twice.
        launch = (
            f"if [ -f {task}/.hsm_pid ]; then cat {task}/.hsm_pid; "
            f"else mkdir -p {task} && cat > {script} && "
            f"{{ setsid nohup bash {script} > {task}/hsm.log 2>&1 < /dev/null & "
            f"echo $! > {task}/.hsm_pid; }} && cat {task}/.hsm_pid; fi"
        )
        now = datetime.now()
        self.active_jobs[job_id] = JobInfo(
            job_id=job_id,
            job_name=job_name,
            params=params,
            source_name=self.name,
            status="RUNNING",
            submit_time=now,
            start_time=now,
            task_dir=str(local_task_dir),
        )
        self._slots[job_id], self._task_dirs[job_id] = slot, remote_task_dir
        self.stats.total_submitted += 1
        if self._manifest:  # listed before it starts: a collect never removes it unknowingly
            self._write_manifest()
        for attempt in range(1, LAUNCH_TRIES + 1):
            try:
                out = ((await self._run(launch, input=script_content)).stdout or "").strip()
                out = out.splitlines()[-1] if out else out  # rc-file noise comes first
            except (OSError, asyncssh.Error) as e:
                out = repr(e)
            if out.isdigit():
                self._pids[job_id] = int(out)
                gpu_msg = f" on GPU(s) {cuda_visible}" if cuda_visible else ""
                logger.info(f"Started {job_name} ({job_id}) on {self.host}{gpu_msg}, pid {out}")
                if self._manifest:
                    self._write_manifest()
                return job_id
            logger.warning(f"{job_name}: launch {attempt}/{LAUNCH_TRIES} on {self.host}: {out!r}")
        self._finish(job_id, "FAILED")
        raise ConnectionError(f"could not start {job_name} on {self.host}: {out}")

    async def submit_batch(self, *args: Any, **kwargs: Any) -> list[str]:
        """The base batch, with ``.hsm_manifest.json`` rewritten as each task starts, so that
        ``hsm sweep collect`` can re-attach (not as a distributed child: siblings share the dir)."""
        self._manifest = True
        self._lock = self.sweep_dir and launcher_lock(self.sweep_dir)
        if self.sweep_dir and not self._lock:
            logger.warning(f"{self.sweep_dir}: another process holds its launcher lock")
        try:
            return await super().submit_batch(*args, **kwargs)
        except BaseException:  # Ctrl-C, or a launch that kept failing
            self._warn_running()
            raise
        finally:
            self._write_manifest()

    def _write_manifest(self) -> None:
        """Where each task runs: enough to re-attach (statuses come from the remote)."""
        if self.sweep_dir is None:
            return
        jobs = {**self.completed_jobs, **self.active_jobs}
        manifest = {
            "sweep_id": self.sweep_id,
            "backend": "ssh",
            "name": self.name,
            "host": self.host,
            "ssh_key": self.ssh_key,
            "ssh_port": self.ssh_port,
            "project_dir": self.project_dir,
            "keep_remote_on_success": self.keep_remote_on_success,
            "remote_sweep_dir": self._remote_sweep_dir,
            "remote_code_dir": self._remote_code_dir,
            "tasks": {
                j: {"name": info.job_name, "pid": self._pids.get(j), "dir": self._task_dirs.get(j)}
                for j, info in jobs.items()
            },
        }
        path = self.sweep_dir / ".hsm_manifest.json"
        try:
            path.with_suffix(".tmp").write_text(json.dumps(manifest, indent=2))
            path.with_suffix(".tmp").replace(path)  # never a half-written manifest
        except OSError as e:
            logger.warning(f"could not write {path}: {e}")

    @classmethod
    def from_manifest(cls, manifest: dict[str, Any], sweep_dir: Path) -> SSHComputeSource:
        """A source re-attached to a launched sweep (``hsm sweep collect``): no push, no setup.
        Statuses come from the remote: :meth:`recover_pids`, then a poll."""
        src = cls(
            name=manifest["name"],
            host=manifest["host"],
            ssh_key=manifest.get("ssh_key"),
            ssh_port=manifest.get("ssh_port"),
            project_dir=manifest.get("project_dir", "."),
            keep_remote_on_success=manifest.get("keep_remote_on_success", False),
        )
        src.sweep_dir, src.sweep_id = sweep_dir, manifest["sweep_id"]
        src._remote_sweep_dir = manifest["remote_sweep_dir"]
        src._remote_code_dir = manifest.get("remote_code_dir")
        for job, task in manifest["tasks"].items():
            src.active_jobs[job] = JobInfo(job, task["name"], {}, src.name, "RUNNING")
            src._task_dirs[job] = task["dir"]
            if task.get("pid"):
                src._pids[job] = task["pid"]
        return src

    async def recover_pids(self) -> None:
        """Tasks listed without a pid (the launcher died mid-launch): read their ``.hsm_pid``.
        One that has none never started (FAILED). Raises when the remote can't be read."""
        if not (lost := [j for j in self.active_jobs if j not in self._pids]):
            return
        q = shlex.quote
        reads = "; ".join(f"echo {q(j)} $(cat {q(self._task_dirs[j])}/.hsm_pid)" for j in lost)
        result = await self._run(f"{{ {reads}; }} 2>/dev/null")
        if result.returncode != 0:
            raise ConnectionError(f"could not read the task pids on {self.host}")
        for job, *pid in (line.split() for line in (result.stdout or "").splitlines()):
            if job in lost and pid and pid[0].isdigit():
                self._pids[job] = int(pid[0])
        for job in lost:
            if job not in self._pids:
                self.update_job_status(job, "FAILED")

    async def _acquire_slot(self) -> Any:
        """A free slot; while none is, poll, so that finished tasks free theirs."""
        assert self._slot_queue is not None
        while self._slot_queue.empty():
            await asyncio.sleep(self.slot_poll_s)
            await self.update_all_job_statuses()
        return self._slot_queue.get_nowait()

    def _finish(self, job_id: str, status: str) -> None:
        """Mark a job terminal and free its slot."""
        self.update_job_status(job_id, status)
        if job_id in self._slots:
            assert self._slot_queue is not None
            self._slot_queue.put_nowait(self._slots.pop(job_id))

    async def _run(self, cmd: str, input: str | None = None) -> Any:
        """One remote command, in bash whatever the login shell (dash's ``kill`` has no ``--``;
        fish can't parse ``if``). A dropped connection is reopened once; no status changes."""
        cmd, kw = f"bash -c {shlex.quote(cmd)}", {"check": False, "timeout": RUN_TIMEOUT_S}
        try:
            return await self._conn.run(cmd, input=input, **kw)
        except (OSError, asyncssh.Error) as e:
            logger.warning(f"{self.host}: connection lost ({e!r}); reconnecting")
            self._conn.close()
            self._conn = await self._open_connection()
            return await self._conn.run(cmd, input=input, **kw)

    # ----------------------------------------------------------------- status
    async def update_all_job_statuses(self) -> None:
        """One remote command reads every running task's state (see :data:`_POLL_FN`)."""
        live = [job for job in self.active_jobs if job in self._pids]
        if not live:
            return
        calls = "; ".join(
            f"p {shlex.quote(self._task_dirs[j])} {self._pids[j]} {shlex.quote(j)}" for j in live
        )
        try:
            result = await self._run(f"{_POLL_FN}; {calls}")
            if result.returncode != 0:  # None: the link died mid-command (asyncssh doesn't raise)
                raise ConnectionError(f"the poll ended with rc {result.returncode}")
        except (OSError, asyncssh.Error) as e:
            self._down_since = self._down_since or time.monotonic()
            if time.monotonic() - self._down_since > LINK_GIVE_UP_S:
                self._warn_running()
                raise ConnectionError(
                    f"{self.host} unreachable for {LINK_GIVE_UP_S // 60} min; its tasks keep "
                    f"running (`hsm sweep collect {self.sweep_id}` re-attaches)"
                ) from e
            logger.warning(f"{self.host}: status poll failed ({e!r}); job states kept")
            return
        self._down_since, out = None, result.stdout or ""
        for job, state, *rc in (
            fields for line in out.splitlines() if len(fields := line.split()) >= 2
        ):
            if job not in self.active_jobs or state == "run":
                continue
            if state == "gone":
                logger.warning(f"{job}: its process is gone without an exit code (killed?)")
            ok = "COMPLETED" if rc == ["0"] else "FAILED"
            self._finish(job, "CANCELLED" if job in self._cancelled else ok)

    async def get_job_status(self, job_id: str) -> str:
        """The state the last poll saw."""
        info = self.active_jobs.get(job_id) or self.completed_jobs.get(job_id)
        return info.status if info else "UNKNOWN"

    async def cancel_job(self, job_id: str) -> bool:
        """TERM the task's whole process group; it is CANCELLED (and frees its slot) once the
        poll sees it end, as it may checkpoint on TERM first."""
        pid = self._pids.get(job_id)
        if pid is None or job_id not in self.active_jobs or job_id in self._cancelled:
            return False
        stamp = shlex.quote(f"{self._task_dirs[job_id]}/.hsm_pid")
        try:
            # A live pid younger than its .hsm_pid is another process's (a reused pid): exit 3,
            # nothing sent. The group exists once the task has called setsid; before, the pid.
            sent = await self._run(
                f"if a=$(ps -o etimes= -p {pid}); then "
                f"[ $(( $(date +%s) - $(stat -c %Y {stamp}) )) -le $(( a + 2 )) ] || exit 3; fi; "
                f"kill -TERM -- -{pid} 2>/dev/null || kill -TERM {pid}"
            )
        except (OSError, asyncssh.Error) as e:
            sent = SimpleNamespace(returncode=repr(e))
        if sent.returncode != 0:  # already ended, or the link dropped
            logger.warning(f"Cancelling {job_id} on {self.host} failed ({sent.returncode})")
            return False
        self._cancelled.add(job_id)
        return True

    # ----------------------------------------------------------- collection
    async def collect_results(
        self, job_ids: list[str] | None = None, *, defer_cleanup: bool = False
    ) -> bool:
        # defer_cleanup is a resumable-chain no-op here (bash-over-SSH is not a
        # chain backend — chains require Slurm dependencies/signals).
        if self._remote_sweep_dir is None or self.sweep_dir is None:
            logger.warning(f"collect_results called before setup on {self.name}")
            return False
        remote_tasks = f"{self._remote_sweep_dir}/tasks"
        local_tasks = str(self.sweep_dir / "tasks")
        pull_cmd = build_rsync_pull_cmd(
            self.host, remote_tasks, local_tasks, agentless=agent_stalled(self.host)
        )
        logger.info(f"rsync pull from {self.host}:{remote_tasks}")
        rc = await self._run_rsync(pull_cmd)
        if rc != 0:
            return False

        # Anything short of COMPLETED (FAILED, CANCELLED) keeps the remote dir and isn't archived
        # as a success.
        any_failed = any(j.status != "COMPLETED" for j in self.completed_jobs.values())
        if not any_failed and not self.keep_remote_on_success and not self.active_jobs:
            try:
                dirs = [self._remote_sweep_dir, own_snapshot(self._remote_code_dir, self.sweep_id)]
                rm = await self._run("rm -rf " + " ".join(shlex.quote(d) for d in dirs if d))
                if rm.returncode != 0:
                    raise OSError(f"rm ended with rc {rm.returncode}")
                logger.info(f"Cleaned remote sweep dir {self._remote_sweep_dir} on {self.host}")
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Failed to clean remote sweep dir: {e}")
        elif any_failed:
            logger.info(
                f"Keeping {self._remote_sweep_dir} on {self.host} for inspection "
                f"(a job not COMPLETED)"
            )
        return True

    # -------------------------------------------------------------- health
    async def health_check(self) -> dict[str, Any]:
        info: dict[str, Any] = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "host": self.host,
            "active_jobs": len(self.active_jobs),
            "max_jobs": self.max_parallel_jobs,
            "utilization": f"{self.utilization:.1%}",
            "gpus_detected": list(self._gpu_indices),
            "slot_count": self._slot_count,
        }
        if self._conn is not None:
            try:
                result = await self._conn.run("date", check=False)
                info["connection"] = "ok"
                info["remote_time"] = (result.stdout or "").strip()
            except Exception as e:  # noqa: BLE001
                info["connection"] = "failed"
                info["error"] = str(e)
                info["status"] = "unhealthy"
        else:
            info["status"] = "unhealthy"
            info["connection"] = "not_connected"

        self.stats.health_status = info["status"]
        self.stats.last_health_check = datetime.now()
        return info

    # --------------------------------------------------------------- cleanup
    async def wait_for_all(self, *args: Any, **kwargs: Any) -> dict[str, str]:
        try:
            return await super().wait_for_all(*args, **kwargs)
        except BaseException:
            self._warn_running()
            raise

    def _warn_running(self) -> None:
        """Say once which tasks keep running (they are detached) and how to stop them."""
        if self._warned or not (
            running := [self._pids[j] for j in self.active_jobs if j in self._pids]
        ):
            return
        self._warned = True
        groups = " ".join(f"-{pid}" for pid in running)
        later = f"`hsm sweep collect {self.sweep_id}` pulls them later; "
        logger.warning(
            f"{len(running)} task(s) keep running on {self.host}, detached; "
            f"{later if self._manifest else ''}to stop them: ssh {self.host} kill -TERM {groups}"
        )

    async def cleanup(self) -> None:
        """Close the connection (and the launcher lock). Tasks still running keep running."""
        self._warn_running()
        if self._lock is not None:
            self._lock.close()
            self._lock = None
        if self._conn is not None:
            try:
                self._conn.close()
                waiter = getattr(self._conn, "wait_closed", None)
                if waiter is not None:
                    await waiter()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None

    def __str__(self) -> str:
        return (
            f"SSH:{self.name} ({self.host}): "
            f"{self.current_job_count}/{self._slot_count} jobs "
            f"(gpus={self._gpu_indices or 'none'})"
        )


# ---------------------------------------------------------- config factory


def parse_gpus_arg(arg: str | None) -> None | str | int | list[int]:
    """Parse a ``--gpus`` CLI value into the shape :func:`normalize_gpu_allowlist` expects.

    Accepts (case-insensitive); indices are nvidia-smi's (PCI order):

    - ``None`` / ``""`` → ``None`` (no allowlist: every free GPU)
    - ``"all"`` → ``"all"`` (every detected GPU, busy or not)
    - ``"cpu"`` → ``0`` (CPU-only)
    - ``"N"`` (single int) → ``N`` (take the first N free GPUs)
    - ``"i,j,k"`` (any comma) → ``[i, j, k]`` (explicit allowlist)

    A single ``"0"`` is treated as the int form (CPU-only) — that's the only
    way the two shapes overlap, and CPU-only is the more useful reading.
    """
    if arg is None:
        return None
    s = arg.strip()
    if not s:
        return None
    if s.lower() == "all":
        return "all"
    if s.lower() == "cpu":
        return 0
    if "," in s:
        try:
            return [int(x.strip()) for x in s.split(",") if x.strip() != ""]
        except ValueError as e:
            raise ValueError(f"--gpus list must be comma-separated integers, got {arg!r}") from e
    try:
        return int(s)
    except ValueError as e:
        raise ValueError(
            f"--gpus must be 'all', 'cpu', a single int N, or a comma-separated "
            f"list of indices; got {arg!r}"
        ) from e


def build_ssh_source(
    *,
    name: str,
    remote_cfg: dict[str, Any] | None = None,
    distributed_cfg: dict[str, Any] | None = None,
    project_dir: str,
    script_path: str,
    default_spec: ResourceSpec | None = None,
    gpus_override: None | int | Sequence[int] = None,
    conda_env_override: str | None = None,
    project_conda_env: str | None = None,
) -> SSHComputeSource:
    """Build a push-model :class:`SSHComputeSource` from local hsm_config.

    Resolves precedence per field:

    - explicit ``*_override`` argument > per-remote ``remote_cfg`` >
      global ``distributed_cfg`` > hardcoded default.

    ``remote_cfg`` is the entry under ``distributed.remotes[name]``; an empty
    dict means "bare ssh-config alias" (host defaults to ``name``).
    ``distributed_cfg`` is the whole ``distributed:`` block, for global
    defaults (``remote_root``, ``conda_env``, ``rsync_excludes``,
    ``keep_remote_on_success``).

    ``remote_cfg["spec"]`` (optional) holds a typed default :class:`ResourceSpec`
    for this remote (``walltime``/``cpus_per_task``/``mem``/``gpus``/
    ``pre_script``/``modules``/...). It's the no-bleed home for per-remote
    defaults — the global ``slurm:`` / ``local:`` blocks are deliberately
    *not* read for remote/distributed modes. CLI flags (``--walltime``,
    ``--resources``) still override per-remote fields.

    Used by both single-remote (``--mode remote``) and distributed (the
    multi-remote child builder).
    """
    remote_cfg = dict(remote_cfg or {})
    distributed_cfg = dict(distributed_cfg or {})

    host = remote_cfg.get("host") or name
    ssh_key = remote_cfg.get("ssh_key")
    ssh_port = remote_cfg.get("ssh_port")
    max_parallel_jobs = remote_cfg.get("max_parallel_jobs") or 1

    # Per-remote `spec:` sub-block — the no-bleed home for this remote's
    # default ResourceSpec fields (walltime / cpus / mem / gpus / pre_script /
    # modules / ...). Layered UNDER the caller-supplied `default_spec` so CLI
    # flags (--walltime, --resources) still override.
    remote_spec_dict = remote_cfg.get("spec")
    if isinstance(remote_spec_dict, dict) and remote_spec_dict:
        per_remote_spec = ResourceSpec.from_dict(remote_spec_dict, where=f"remote {name!r} spec")
        default_spec = per_remote_spec.merge(default_spec or ResourceSpec())

    conda_env, python_path = remote_interpreter(
        remote_cfg, distributed_cfg, project_conda_env, conda_env_override
    )

    if gpus_override is not None:
        gpus_value: None | int | Sequence[int] = gpus_override
    else:
        gpus_value = remote_cfg.get("gpus")
        if isinstance(gpus_value, str):  # YAML `gpus: "1,2"` / `ALL` / `cpu`, never char by char
            gpus_value = parse_gpus_arg(gpus_value)

    remote_root = remote_cfg.get("remote_root", distributed_cfg.get("remote_root", "~/.hsm/runs"))
    rsync_excludes = remote_cfg.get("rsync_excludes", distributed_cfg.get("rsync_excludes"))
    keep_remote_on_success = bool(
        remote_cfg.get(
            "keep_remote_on_success",
            distributed_cfg.get("keep_remote_on_success", False),
        )
    )

    return SSHComputeSource(
        name=name,
        host=host,
        ssh_key=ssh_key,
        ssh_port=ssh_port,
        conda_env=conda_env,
        python_path=python_path,
        project_dir=project_dir,
        script_path=script_path,
        remote_root=remote_root,
        max_parallel_jobs=max_parallel_jobs,
        gpus=gpus_value,
        default_spec=default_spec,
        rsync_excludes=rsync_excludes,
        keep_remote_on_success=keep_remote_on_success,
    )
