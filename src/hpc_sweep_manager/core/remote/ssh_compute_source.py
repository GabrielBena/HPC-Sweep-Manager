"""Push-model SSH compute source.

Mirrors :class:`LocalComputeSource` shape — one persistent asyncssh
connection, a slot ``asyncio.Queue`` for back-pressure, a per-job monitor
coroutine — but ships the work to a remote box. The model:

    1. setup(): open ssh; rsync the local project up to a rolling code dir
       (``~/.hsm/runs/<project>/code/``); probe ``nvidia-smi`` and partition
       its GPUs into slots; create a per-sweep dir on the remote.
    2. submit_job(): acquire a slot, render the wrapper template, write it to
       the remote via ``cat >``, ``create_process(bash <path>)``, spawn a
       monitor coro that releases the slot when the channel exits.
    3. collect_results(): rsync ``sweeps/<id>/tasks/`` back; on full success
       ``rm -rf`` the per-sweep remote dir (code cache persists).
    4. cleanup(): cancel any leftover processes, close the connection.

The class delegates command-shape decisions to pure helpers in
:mod:`push_exec` and parses GPU output via :mod:`gpu_probe`, so it stays
unit-testable by overriding the two narrow I/O seams
:meth:`_open_connection` and :meth:`_run_rsync`.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
import logging
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Sequence, Union

from ..common.compute_source import ComputeSource, JobInfo
from ..common.resource_spec import ResourceSpec
from ..common.templating import params_to_hydra_args, render_template
from .gpu_probe import NVIDIA_SMI_QUERY, parse_nvidia_smi_csv
from .push_exec import (
    DEFAULT_RSYNC_EXCLUDES,
    build_rsync_pull_cmd,
    build_rsync_push_cmd,
    normalize_gpu_allowlist,
    partition_gpu_slots,
    resolve_run_prefix,
)

logger = logging.getLogger(__name__)


class SSHComputeSource(ComputeSource):
    """Self-contained push-model SSH compute source.

    The remote needs only ``bash`` + ``rsync`` (over the system ssh) +
    optionally ``nvidia-smi``; HSM doesn't have to be installed there. All
    paths in ``project_dir`` / ``script_path`` are LOCAL — the remote sees a
    rsync'd mirror under ``remote_root``.
    """

    # Sentinel for legacy callsites that still `isinstance + .remote_manager`.
    # Keeping it as a class attribute means `not source.remote_manager`
    # short-circuits without AttributeError on the dead paths.
    remote_manager = None

    def __init__(
        self,
        name: str,
        host: Optional[str] = None,
        ssh_key: Optional[str] = None,
        ssh_port: Optional[int] = None,
        conda_env: Optional[str] = None,
        python_path: Optional[str] = None,
        project_dir: str = ".",
        script_path: str = "",
        remote_root: str = "~/.hsm/runs",
        max_parallel_jobs: int = 1,
        gpus: Union[None, int, Sequence[int]] = None,
        default_spec: Optional[ResourceSpec] = None,
        rsync_excludes: Optional[Sequence[str]] = None,
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
            tuple(rsync_excludes)
            if rsync_excludes is not None
            else DEFAULT_RSYNC_EXCLUDES
        )
        self.keep_remote_on_success = keep_remote_on_success

        # Populated in setup()
        self._conn: Any = None
        self._project_name = Path(self.project_dir).name or "project"
        self._remote_code_dir: Optional[str] = None
        self._remote_sweep_dir: Optional[str] = None
        self.sweep_dir: Optional[Path] = None
        self.sweep_id: Optional[str] = None
        self._gpu_indices: List[int] = []
        self._slot_queue: Optional[asyncio.Queue] = None
        self._slot_count: int = max_parallel_jobs
        self._run_prefix: str = "python"

        # Job bookkeeping
        self._procs: Dict[str, Any] = {}
        self._monitors: Dict[str, asyncio.Task] = {}
        self._job_counter: int = 0
        self._counter_lock: Optional[asyncio.Lock] = None

    # ------------------------------------------------- transitional adapter
    @classmethod
    def from_remote_config(
        cls,
        name: str,
        remote_config: Any,
        *,
        project_dir: str = ".",
        script_path: str = "",
        conda_env: Optional[str] = None,
        default_spec: Optional[ResourceSpec] = None,
        remote_root: str = "~/.hsm/runs",
        rsync_excludes: Optional[Sequence[str]] = None,
        keep_remote_on_success: bool = False,
        gpus: Union[None, int, Sequence[int]] = None,
    ) -> "SSHComputeSource":
        """Build a push source from the legacy ``RemoteConfig`` shape.

        Used by callsites that still go through ``discover_remote_config``
        until B2-part-2 rewires them. Host / ssh creds / parallel_jobs come
        from the (discovered) ``RemoteConfig``; ``project_dir`` and
        ``script_path`` are LOCAL — discovery does not influence what gets
        pushed.
        """
        return cls(
            name=name,
            host=remote_config.host,
            ssh_key=remote_config.ssh_key,
            ssh_port=remote_config.ssh_port,
            python_path=remote_config.python_interpreter,
            max_parallel_jobs=remote_config.max_parallel_jobs or 1,
            conda_env=conda_env,
            project_dir=project_dir,
            script_path=script_path,
            default_spec=default_spec,
            remote_root=remote_root,
            rsync_excludes=rsync_excludes,
            keep_remote_on_success=keep_remote_on_success,
            gpus=gpus,
        )

    # ------------------------------------------------------------- I/O seams
    async def _open_connection(self) -> Any:
        """Open the persistent asyncssh connection. Overridden in tests."""
        from .discovery import create_ssh_connection

        return await create_ssh_connection(self.host, self.ssh_key, self.ssh_port)

    async def _run_rsync(self, cmd: List[str]) -> int:
        """Run an rsync command and return its exit code. Overridden in tests."""
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, err = await proc.communicate()
        if proc.returncode != 0:
            logger.error(
                f"rsync ({cmd[0]} {self.host}): rc={proc.returncode}\n"
                f"stderr={(err or b'').decode('utf-8', errors='replace')}"
            )
        return proc.returncode or 0

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

        # Resolve any leading ~ in remote_root to an absolute path.
        # `cd ~/path` and `mkdir -p ~/path` expand tilde, but values like
        # `output.dir=~/path` passed to python (inside quoted COMMAND strings)
        # do NOT — python's Path() doesn't expand tilde either. Probing $HOME
        # once at setup gives us a single absolute path used everywhere.
        resolved_root = self.remote_root
        if resolved_root.startswith("~"):
            home_result = await self._conn.run("echo $HOME", check=False)
            home = (home_result.stdout or "").strip()
            if home:
                resolved_root = home + resolved_root[1:]
            else:
                logger.warning(
                    f"Could not resolve $HOME on {self.host}; leaving remote_root as {resolved_root!r}"
                )
        self._remote_code_dir = f"{resolved_root}/{self._project_name}/code"
        self._remote_sweep_dir = (
            f"{resolved_root}/{self._project_name}/sweeps/{sweep_id}"
        )

        # Build the remote layout up front so rsync push + per-task writes
        # don't have to worry about missing directories.
        await self._conn.run(
            f"mkdir -p {self._remote_code_dir} "
            f"{self._remote_sweep_dir}/tasks "
            f"{self._remote_sweep_dir}/logs "
            f"{self._remote_sweep_dir}/scripts",
            check=False,
        )

        push_cmd = build_rsync_push_cmd(
            local_dir=self.project_dir,
            host=self.host,
            remote_dir=self._remote_code_dir,
            excludes=self.rsync_excludes,
        )
        logger.info(f"rsync push to {self.host}:{self._remote_code_dir}")
        rc = await self._run_rsync(push_cmd)
        if rc != 0:
            self.stats.health_status = "unhealthy"
            return False

        # GPU probe — best effort. A box with no nvidia-smi just gives []
        # which falls back to CPU slots downstream.
        gpu_indices: List[int] = []
        try:
            result = await self._conn.run(NVIDIA_SMI_QUERY, check=False)
            if (result.returncode or 0) == 0:
                gpu_indices = [g.index for g in parse_nvidia_smi_csv(result.stdout or "")]
        except Exception as e:  # noqa: BLE001
            logger.debug(f"GPU probe on {self.host} failed: {e}")
        self._gpu_indices = gpu_indices

        allowed = normalize_gpu_allowlist(self._gpus_config, gpu_indices)
        gpus_per_job = self.default_spec.gpus or 0
        slots = partition_gpu_slots(
            allowed, gpus_per_job, cpu_slots=self.max_parallel_jobs
        )
        self._slot_count = len(slots)
        self._slot_queue = asyncio.Queue()
        for s in slots:
            self._slot_queue.put_nowait(s)

        self._run_prefix = resolve_run_prefix(self.conda_env, self.python_path)

        slot_desc = (
            f"{self._slot_count} GPU slot(s) ({gpus_per_job}/slot, allowed={allowed})"
            if slots and slots[0] is not None
            else f"{self._slot_count} CPU slot(s)"
        )
        logger.info(
            f"SSHComputeSource {self.name}@{self.host}: {slot_desc}, "
            f"run_prefix={self._run_prefix!r}, "
            f"detected_gpus={gpu_indices}"
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
        params: Dict[str, Any],
        job_name: str,
        sweep_id: str,
        wandb_group: Optional[str] = None,
        spec: Optional[ResourceSpec] = None,
    ) -> str:
        if self._conn is None or self._slot_queue is None:
            raise RuntimeError(
                f"SSHComputeSource {self.name!r} not set up; call setup() first"
            )

        effective_spec = self.default_spec.merge(spec)
        job_id = await self._next_job_id()
        local_task_dir = self._local_task_dir_for(job_name)
        local_task_dir.mkdir(parents=True, exist_ok=True)
        remote_task_dir = (
            f"{self._remote_sweep_dir}/tasks/{local_task_dir.name}"
        )

        # Block here when all slots are busy — natural back-pressure.
        slot = await self._slot_queue.get()
        cuda_visible = ",".join(str(i) for i in slot) if slot else None

        script_content = render_template(
            "ssh_compute_source.sh.j2",
            job_name=job_name,
            job_id=job_id,
            params_hydra=params_to_hydra_args(params),
            wandb_group=wandb_group or sweep_id,
            cuda_visible_devices=cuda_visible,
            modules=list(effective_spec.modules),
            pre_script=list(effective_spec.pre_script),
            remote_code_dir=self._remote_code_dir,
            remote_task_dir=remote_task_dir,
            run_prefix=self._run_prefix,
            script_path=self.script_path,
            uses_conda=bool(self.conda_env),
        )
        remote_script_path = f"{self._remote_sweep_dir}/scripts/{job_name}.sh"

        try:
            await self._conn.run(
                f"cat > {remote_script_path} && chmod +x {remote_script_path}",
                input=script_content,
                check=False,
            )
            proc = await self._conn.create_process(f"bash {remote_script_path}")
        except BaseException:
            self._slot_queue.put_nowait(slot)
            raise

        self._procs[job_id] = proc
        self.active_jobs[job_id] = JobInfo(
            job_id=job_id,
            job_name=job_name,
            params=params,
            source_name=self.name,
            status="RUNNING",
            submit_time=datetime.now(),
            start_time=datetime.now(),
            task_dir=str(local_task_dir),
        )
        self.stats.total_submitted += 1
        self._monitors[job_id] = asyncio.create_task(
            self._monitor(job_id, proc, slot)
        )
        gpu_msg = f" on GPU(s) {cuda_visible}" if cuda_visible else ""
        logger.info(f"Submitted ssh job {job_id} ({job_name}) to {self.host}{gpu_msg}")
        return job_id

    async def _monitor(self, job_id: str, proc: Any, slot) -> None:
        try:
            result = await proc.wait()
            exit_status = getattr(result, "exit_status", None)
            if exit_status is None:
                # killed by signal / channel torn down → treat as failure
                exit_status = -1
            status = "COMPLETED" if exit_status == 0 else "FAILED"
            existing = (
                self.active_jobs.get(job_id) or self.completed_jobs.get(job_id)
            )
            if existing and existing.status == "CANCELLED":
                status = "CANCELLED"
            if job_id in self.active_jobs:
                self.update_job_status(job_id, status)
            logger.info(
                f"SSH job {job_id} finished status={status} exit_status={exit_status}"
            )
        finally:
            self._procs.pop(job_id, None)
            assert self._slot_queue is not None
            self._slot_queue.put_nowait(slot)

    # ----------------------------------------------------------------- status
    async def get_job_status(self, job_id: str) -> str:
        if job_id in self.active_jobs:
            proc = self._procs.get(job_id)
            if proc is None:
                pass
            elif getattr(proc, "exit_status", None) is None:
                return "RUNNING"
            else:
                status = (
                    "COMPLETED"
                    if proc.exit_status == 0
                    else "FAILED"
                )
                self.update_job_status(job_id, status)
                return status
        if job_id in self.completed_jobs:
            return self.completed_jobs[job_id].status
        return "UNKNOWN"

    async def cancel_job(self, job_id: str) -> bool:
        proc = self._procs.get(job_id)
        if proc is None:
            return False
        try:
            proc.terminate()
        except Exception as e:  # noqa: BLE001
            logger.warning(f"terminate() on ssh job {job_id} raised: {e}")
            return False

        if job_id in self.active_jobs:
            self.update_job_status(job_id, "CANCELLED")

        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:  # noqa: BLE001
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except asyncio.TimeoutError:  # pragma: no cover
                logger.warning(f"SSH job {job_id} did not exit after kill()")
        return True

    async def wait_for_all(
        self,
        poll_interval: float = 5.0,
        on_progress=None,
    ) -> Dict[str, str]:
        """Await per-job monitor tasks directly.

        Same pattern as :class:`LocalComputeSource` — the monitor coroutines
        are a stronger signal than polling :meth:`get_job_status`, because
        each monitor is what actually transitions a job out of
        ``active_jobs``.
        """
        pending_ids = list(self.active_jobs.keys())
        if not pending_ids:
            final = {jid: info.status for jid, info in self.completed_jobs.items()}
            if on_progress is not None:
                on_progress(len(final), max(len(final), 1))
            return final

        monitors = [
            self._monitors[jid] for jid in pending_ids if jid in self._monitors
        ]
        total = len(pending_ids) + len(self.completed_jobs)
        if on_progress is not None:
            on_progress(len(self.completed_jobs), max(total, 1))

        for fut in asyncio.as_completed(monitors):
            await fut
            if on_progress is not None:
                on_progress(len(self.completed_jobs), max(total, 1))
        return {jid: info.status for jid, info in self.completed_jobs.items()}

    # ----------------------------------------------------------- collection
    async def collect_results(self, job_ids: Optional[List[str]] = None) -> bool:
        if self._remote_sweep_dir is None or self.sweep_dir is None:
            logger.warning(f"collect_results called before setup on {self.name}")
            return False
        remote_tasks = f"{self._remote_sweep_dir}/tasks"
        local_tasks = str(self.sweep_dir / "tasks")
        pull_cmd = build_rsync_pull_cmd(self.host, remote_tasks, local_tasks)
        logger.info(f"rsync pull from {self.host}:{remote_tasks}")
        rc = await self._run_rsync(pull_cmd)
        if rc != 0:
            return False

        any_failed = any(
            j.status == "FAILED" for j in self.completed_jobs.values()
        )
        if not any_failed and not self.keep_remote_on_success:
            try:
                await self._conn.run(
                    f"rm -rf {self._remote_sweep_dir}", check=False
                )
                logger.info(
                    f"Cleaned remote sweep dir {self._remote_sweep_dir} on {self.host}"
                )
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Failed to clean remote sweep dir: {e}")
        elif any_failed:
            logger.info(
                f"Keeping {self._remote_sweep_dir} on {self.host} for inspection "
                f"(at least one FAILED job)"
            )
        return True

    # -------------------------------------------------------------- health
    async def health_check(self) -> Dict[str, Any]:
        info: Dict[str, Any] = {
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
    async def cleanup(self) -> None:
        for job_id in list(self._procs.keys()):
            await self.cancel_job(job_id)
        if self._monitors:
            await asyncio.gather(*self._monitors.values(), return_exceptions=True)
        self._monitors.clear()
        if self._conn is not None:
            try:
                self._conn.close()
                waiter = getattr(self._conn, "wait_closed", None)
                if waiter is not None:
                    await waiter()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None

    async def update_all_job_statuses(self) -> None:
        for job_id in list(self.active_jobs.keys()):
            try:
                await self.get_job_status(job_id)
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Failed to update status for {job_id}: {e}")

    def __str__(self) -> str:
        return (
            f"SSH:{self.name} ({self.host}): "
            f"{self.current_job_count}/{self._slot_count} jobs "
            f"(gpus={self._gpu_indices or 'none'})"
        )


# ---------------------------------------------------------- config factory


def parse_gpus_arg(arg: Optional[str]) -> Union[None, int, List[int]]:
    """Parse a ``--gpus`` CLI value into the shape :func:`normalize_gpu_allowlist` expects.

    Accepts (case-insensitive):

    - ``None`` / ``""`` / ``"all"`` → ``None`` (use every detected GPU)
    - ``"cpu"`` → ``0`` (CPU-only)
    - ``"N"`` (single int) → ``N`` (take the first N detected GPUs)
    - ``"i,j,k"`` (any comma) → ``[i, j, k]`` (explicit allowlist)

    A single ``"0"`` is treated as the int form (CPU-only) — that's the only
    way the two shapes overlap, and CPU-only is the more useful reading.
    """
    if arg is None:
        return None
    s = arg.strip()
    if not s or s.lower() == "all":
        return None
    if s.lower() == "cpu":
        return 0
    if "," in s:
        try:
            return [int(x.strip()) for x in s.split(",") if x.strip() != ""]
        except ValueError as e:
            raise ValueError(
                f"--gpus list must be comma-separated integers, got {arg!r}"
            ) from e
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
    remote_cfg: Optional[Dict[str, Any]] = None,
    distributed_cfg: Optional[Dict[str, Any]] = None,
    project_dir: str,
    script_path: str,
    default_spec: Optional[ResourceSpec] = None,
    gpus_override: Union[None, int, Sequence[int]] = None,
    conda_env_override: Optional[str] = None,
) -> "SSHComputeSource":
    """Build a push-model :class:`SSHComputeSource` from local hsm_config.

    Resolves precedence per field:

    - explicit ``*_override`` argument > per-remote ``remote_cfg`` >
      global ``distributed_cfg`` > hardcoded default.

    ``remote_cfg`` is the entry under ``distributed.remotes[name]``; an empty
    dict means "bare ssh-config alias" (host defaults to ``name``).
    ``distributed_cfg`` is the whole ``distributed:`` block, for global
    defaults (``remote_root``, ``conda_env``, ``rsync_excludes``,
    ``keep_remote_on_success``).

    Used by both single-remote (``--mode remote``) and distributed (the
    multi-remote child builder).
    """
    remote_cfg = dict(remote_cfg or {})
    distributed_cfg = dict(distributed_cfg or {})

    host = remote_cfg.get("host") or name
    ssh_key = remote_cfg.get("ssh_key")
    ssh_port = remote_cfg.get("ssh_port")
    max_parallel_jobs = remote_cfg.get("max_parallel_jobs") or 1

    conda_env = (
        conda_env_override
        if conda_env_override is not None
        else remote_cfg.get("conda_env", distributed_cfg.get("conda_env"))
    )
    python_path = remote_cfg.get(
        "python_path", distributed_cfg.get("python_path")
    )

    if gpus_override is not None:
        gpus_value: Union[None, int, Sequence[int]] = gpus_override
    else:
        gpus_value = remote_cfg.get("gpus")

    remote_root = remote_cfg.get(
        "remote_root", distributed_cfg.get("remote_root", "~/.hsm/runs")
    )
    rsync_excludes = remote_cfg.get(
        "rsync_excludes", distributed_cfg.get("rsync_excludes")
    )
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
