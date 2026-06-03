"""SSH-driven Slurm compute source — fills the gap between SSH-bash and local Slurm.

Today you can either:

- ``--mode array | individual`` — submit Slurm jobs *from the cluster itself*
  (requires ``sbatch`` on local PATH), or
- ``--mode remote`` — SSH push then run *bash* on the remote login node.

Neither lets you drive a remote cluster's batch scheduler from a workstation
that doesn't itself have Slurm installed. This module does:

    1. ``setup()``: open an asyncssh connection, rsync the local project up
       to a per-project remote code dir, verify ``sbatch`` lives on the
       remote's PATH, ``mkdir`` the per-sweep layout on the remote.
    2. ``submit_job()`` / ``_submit_array()``: render the existing
       :mod:`slurm_single` / :mod:`slurm_array` Jinja templates with the
       REMOTE paths baked in, pipe the script to the remote via ``cat >``,
       then ``ssh host "cd <sweep_dir> && sbatch <script>"`` — capturing
       the job id from sbatch's stdout.
    3. ``get_job_status`` / ``update_all_job_statuses``: ``ssh host
       "squeue -j <ids> -h -o '%i %T'"`` — batched across all live jobs
       so each poll cycle is one round-trip, not N.
    4. ``collect_results()``: rsync the remote ``tasks/`` tree back to
       the local sweep dir; on full-success, ``rm -rf`` the remote sweep
       dir (the per-project code mirror persists for the next sweep).
    5. ``cleanup()``: close the ssh connection.

Reuses pure helpers:

- :mod:`push_exec` — rsync arg shape + tilde-expansion / GPU helpers.
- :mod:`slurm_protocol` — ``#SBATCH`` directive rendering + the raw-Slurm
  state map + ``sbatch`` stdout parsing.

The class is unit-testable: the only I/O seams are :meth:`_open_connection`
and :meth:`_run_rsync` — override both in tests with a fake asyncssh
connection and a recording rsync, and the entire flow is exercisable
without a real cluster.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
import json
import logging
from pathlib import Path
import shlex
from typing import Any, Dict, List, Optional, Sequence

from ..common.compute_source import (
    TERMINAL_STATES,
    ComputeSource,
    JobInfo,
    ProgressCallback,
    SubmissionMode,
)
from ..common.resource_spec import ResourceSpec
from ..common.templating import params_to_hydra_args, params_to_yaml, render_template
from ..hpc.scheduler_queue import parse_reservations_output
from ..hpc.slurm_protocol import (
    SLURM_STATE_MAP,
    parse_sacct_state,
    parse_sbatch_job_id,
    render_sbatch_directives,
)
from .push_exec import (
    DEFAULT_RSYNC_EXCLUDES,
    build_rsync_pull_cmd,
    build_rsync_push_cmd,
    resolve_run_prefix,
)

logger = logging.getLogger(__name__)


class SSHSlurmComputeSource(ComputeSource):
    """Push-model Slurm-over-SSH compute source.

    The remote needs ``bash`` + ``rsync`` + ``sbatch``/``squeue``/``scancel``
    on the user's PATH. HSM does NOT have to be installed on the remote —
    only the rsync'd code mirror runs there.
    """

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
        workdir: Optional[str] = None,
        archive_dir: Optional[str] = None,
        archive_on: str = "completed",
        max_parallel_jobs: int = 0,
        default_spec: Optional[ResourceSpec] = None,
        rsync_excludes: Optional[Sequence[str]] = None,
        keep_remote_on_success: bool = False,
        qos_whitelist: Optional[frozenset[str]] = None,
    ):
        # max_parallel_jobs=0 -> "no client-side cap" (Slurm's own scheduler
        # decides). Matches SlurmComputeSource's convention.
        super().__init__(name, "ssh_slurm_remote", max_parallel_jobs or 10_000)
        self.host = host or name
        self.ssh_key = ssh_key
        self.ssh_port = ssh_port
        self.conda_env = conda_env
        self.python_path = python_path or "python"
        self.project_dir = str(Path(project_dir).resolve())
        # Templates `cd <project_dir>` inside the rendered script — for
        # SSH-driven Slurm that's the REMOTE code dir, not the local one.
        # Strip leading slash from script_path if it's an absolute LOCAL
        # path inside project_dir, the same way SSHComputeSource does.
        if script_path and Path(script_path).is_absolute():
            try:
                script_path = str(Path(script_path).relative_to(self.project_dir))
            except ValueError:
                logger.warning(
                    f"SSHSlurmComputeSource {name!r}: script_path {script_path!r} "
                    f"is absolute and outside project_dir {self.project_dir!r}; "
                    f"the rendered remote command will reference this LOCAL path."
                )
        self.script_path = script_path
        self.remote_root = remote_root.rstrip("/")
        # `workdir` overrides `remote_root` when set — naming is intentional:
        # `remote_root` says "permanent home for code+sweeps" (the SSH-bash
        # default); `workdir` says "transient scratch for the active run"
        # (the cluster idiom — pair with `archive_dir`).
        self.workdir = workdir.rstrip("/") if workdir else None
        self.archive_dir = archive_dir.rstrip("/") if archive_dir else None
        if archive_on not in ("completed", "always", "never"):
            raise ValueError(
                f"archive_on must be 'completed', 'always', or 'never'; "
                f"got {archive_on!r}"
            )
        self.archive_on = archive_on
        self.default_spec = default_spec or ResourceSpec()
        self.rsync_excludes = (
            tuple(rsync_excludes) if rsync_excludes is not None else DEFAULT_RSYNC_EXCLUDES
        )
        self.keep_remote_on_success = keep_remote_on_success
        self.qos_whitelist = qos_whitelist

        # Populated by setup()
        self._conn: Any = None
        self._project_name = Path(self.project_dir).name or "project"
        self._remote_code_dir: Optional[str] = None
        self._remote_sweep_dir: Optional[str] = None
        self._remote_tasks_dir: Optional[str] = None
        self._remote_logs_dir: Optional[str] = None
        self._remote_scripts_dir: Optional[str] = None
        # archive_dir with $USER/$HOME/~ expanded on the remote (set in setup()).
        self._resolved_archive_dir: Optional[str] = None
        self.sweep_dir: Optional[Path] = None
        self.sweep_id: Optional[str] = None
        self._run_prefix: str = "python"

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

    # ---------------------------------------------------------------- helpers
    def _effective_spec(self, spec: Optional[ResourceSpec]) -> ResourceSpec:
        merged = self.default_spec.merge(spec)
        if (
            merged.qos is not None
            and self.qos_whitelist is not None
            and merged.qos not in self.qos_whitelist
        ):
            raise ValueError(
                f"qos={merged.qos!r} is not in the whitelist "
                f"{sorted(self.qos_whitelist)}"
            )
        return merged

    async def _ssh_run(self, cmd: str, *, check: bool = False) -> Any:
        if self._conn is None:
            raise RuntimeError(
                f"SSHSlurmComputeSource {self.name!r} not connected; call setup() first"
            )
        return await self._conn.run(cmd, check=check)

    async def _resolve_remote_path(self, path: str) -> str:
        """Expand ``~`` / ``$USER`` / ``$HOME`` / ``$SCRATCH`` etc. on the remote.

        The rsync *destination* is handed to a LOCAL rsync process (no remote
        shell), and the server-side archive rsync ``shlex.quote``s its path — so
        neither expands env vars the way the docs promise (``$USER`` lands as a
        literal directory name). We resolve once here via a remote-shell
        ``echo`` (unquoted, so the remote shell expands both ``~`` and ``$VAR``)
        and use the literal result everywhere downstream. Safe for normal HPC
        paths; paths containing spaces or glob metacharacters are unsupported
        (and don't occur in practice).
        """
        result = await self._ssh_run(f"echo {path}", check=False)
        lines = (result.stdout or "").strip().splitlines()
        first = lines[0].strip() if lines else ""
        return first or path

    async def _write_remote_file(self, remote_path: str, content: str) -> None:
        # ``cat > path`` is sufficient since asyncssh's ``.run(..., input=...)``
        # pipes content over the channel. Avoids needing scp or sftp.
        if self._conn is None:
            raise RuntimeError(
                f"SSHSlurmComputeSource {self.name!r} not connected; call setup() first"
            )
        await self._conn.run(
            f"cat > {shlex.quote(remote_path)}",
            input=content,
            check=False,
        )

    # ------------------------------------------------------------------ setup
    async def setup(self, sweep_dir: Path, sweep_id: str) -> bool:
        sweep_dir.mkdir(parents=True, exist_ok=True)
        for sub in ("logs", "scripts", "tasks"):
            (sweep_dir / sub).mkdir(parents=True, exist_ok=True)
        self.sweep_dir = sweep_dir
        self.sweep_id = sweep_id

        try:
            self._conn = await self._open_connection()
        except Exception as e:  # noqa: BLE001 — surface as setup failure
            logger.error(f"SSH connection to {self.host} failed: {e}")
            self.stats.health_status = "unhealthy"
            return False

        # Verify the Slurm tools we need are on the remote PATH. Bail
        # early with a useful message rather than failing at sbatch time.
        check = await self._ssh_run(
            "command -v sbatch && command -v squeue && command -v scancel",
            check=False,
        )
        if (check.returncode or 0) != 0:
            logger.error(
                f"Slurm tools (sbatch/squeue/scancel) not found on PATH on "
                f"{self.host}. If they live in a flavour module or conda env, "
                f"add the activation to the remote's `pre_script:` or "
                f"~/.bashrc (note: non-interactive SSH skips ~/.bashrc — "
                f"flavour modules belong in `pre_script:`)."
            )
            self.stats.health_status = "unhealthy"
            return False

        # `workdir` overrides `remote_root` for THIS run when set — that's
        # the S3IT-style pattern where the active sweep lives on /scratch
        # (ephemeral) and gets archived to /shares (permanent) afterward.
        # When unset, fall back to the (persistent) `remote_root`.
        active_root = self.workdir or self.remote_root
        # Expand ~ / $USER / $HOME on the remote so the rsync destination (built
        # locally) and the archive target (shlex-quoted) point at real paths —
        # the docs promise this expansion (HPC_EXECUTION.md).
        resolved_root = await self._resolve_remote_path(active_root)
        self._resolved_archive_dir = (
            await self._resolve_remote_path(self.archive_dir)
            if self.archive_dir
            else None
        )
        self._remote_code_dir = f"{resolved_root}/{self._project_name}/code"
        self._remote_sweep_dir = (
            f"{resolved_root}/{self._project_name}/sweeps/{sweep_id}"
        )
        self._remote_tasks_dir = f"{self._remote_sweep_dir}/tasks"
        self._remote_logs_dir = f"{self._remote_sweep_dir}/logs"
        self._remote_scripts_dir = f"{self._remote_sweep_dir}/scripts"

        await self._ssh_run(
            f"mkdir -p {self._remote_code_dir} "
            f"{self._remote_tasks_dir} {self._remote_logs_dir} "
            f"{self._remote_scripts_dir}",
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

        self._run_prefix = resolve_run_prefix(self.conda_env, self.python_path)
        self.stats.health_status = "healthy"
        self.stats.last_health_check = datetime.now()
        logger.info(
            f"SSHSlurmComputeSource {self.name}@{self.host}: ready "
            f"(remote_sweep_dir={self._remote_sweep_dir}, "
            f"run_prefix={self._run_prefix!r})"
        )
        await self._check_reservations()
        return True

    async def _check_reservations(self) -> None:
        """Warn at submit if the cluster has any Slurm reservation.

        The field-report trap: a task held behind a maintenance reservation
        (e.g. 06:00–18:00) outlived the launcher and stranded the sweep. Best-
        effort — never fails setup; just surfaces the window + the recovery path
        (``hsm sweep collect``, now that T0/T1 keep partial progress).
        """
        try:
            result = await self._ssh_run(
                "scontrol show reservations", check=False
            )
        except Exception:  # noqa: BLE001
            return
        if (result.returncode or 0) != 0:
            return
        reservations = parse_reservations_output(result.stdout or "")
        if not reservations:
            return
        names = "; ".join(
            f"{r.name} ({r.start_time}→{r.end_time})" for r in reservations[:3]
        )
        more = "" if len(reservations) <= 3 else f" (+{len(reservations) - 3} more)"
        logger.warning(
            f"{len(reservations)} Slurm reservation(s) on {self.host}: {names}"
            f"{more}. A task held behind a reservation can outlive this "
            f"launcher; if `hsm sweep run` exits before everything finishes, "
            f"run `hsm sweep collect {self.sweep_id}` later to pull/archive "
            f"the rest."
        )

    # ----------------------------------------------------------------- submit
    async def submit_job(
        self,
        params: Dict[str, Any],
        job_name: str,
        sweep_id: str,
        wandb_group: Optional[str] = None,
        spec: Optional[ResourceSpec] = None,
    ) -> str:
        if self._conn is None or self._remote_sweep_dir is None:
            raise RuntimeError(
                f"SSHSlurmComputeSource {self.name!r} not set up; call setup() first"
            )
        effective = self._effective_spec(spec)
        directives = render_sbatch_directives(effective)
        remote_task_dir = f"{self._remote_tasks_dir}/{job_name}"

        # The slurm_single template's `cd {{ project_dir }}` is what makes
        # the wrapper land in the right place on the compute node — for
        # SSH-driven Slurm that's the REMOTE code mirror, not the LOCAL
        # project dir.
        script_content = render_template(
            "slurm_single.sh.j2",
            job_name=job_name,
            sweep_id=sweep_id,
            logs_dir=self._remote_logs_dir,
            task_dir=remote_task_dir,
            sbatch_directives=directives,
            modules=list(effective.modules),
            pre_script=list(effective.pre_script),
            project_dir=self._remote_code_dir,
            python_path=self._run_prefix,
            script_path=self.script_path,
            params_hydra=params_to_hydra_args(params),
            params_yaml=params_to_yaml(params),
            wandb_group=wandb_group,
            uses_conda=bool(self.conda_env),
        )
        remote_script_path = f"{self._remote_scripts_dir}/{job_name}.slurm"
        await self._write_remote_file(remote_script_path, script_content)

        result = await self._ssh_run(
            f"sbatch {shlex.quote(remote_script_path)}", check=False
        )
        if (result.returncode or 0) != 0:
            stderr = (result.stderr or "").strip() or "no stderr"
            raise RuntimeError(
                f"sbatch failed for {job_name} on {self.host}: {stderr}"
            )
        job_id = parse_sbatch_job_id(result.stdout or "")

        # Local mirror task dir so collect_results() can write into it.
        local_task_dir = self.sweep_dir / "tasks" / job_name  # type: ignore[union-attr]
        local_task_dir.mkdir(parents=True, exist_ok=True)

        self.active_jobs[job_id] = JobInfo(
            job_id=job_id,
            job_name=job_name,
            params=params,
            source_name=self.name,
            status="PENDING",
            submit_time=datetime.now(),
            task_dir=str(local_task_dir),
        )
        self.stats.total_submitted += 1
        logger.info(
            f"Submitted Slurm job {job_id} ({job_name}) on {self.host} "
            f"via SSH"
        )
        return job_id

    async def submit_batch(
        self,
        params_list: List[Dict[str, Any]],
        sweep_id: str,
        mode: SubmissionMode = "individual",
        spec: Optional[ResourceSpec] = None,
        wandb_group: Optional[str] = None,
        job_name_prefix: Optional[str] = None,
    ) -> List[str]:
        if mode == "array":
            job_ids = [
                await self._submit_array(
                    params_list, sweep_id, spec, wandb_group, job_name_prefix
                )
            ]
        else:
            job_ids = await super().submit_batch(
                params_list, sweep_id, mode, spec, wandb_group, job_name_prefix
            )
        # Drop a re-attach manifest (local + remote) so `hsm sweep collect <id>`
        # can pull/archive after the launching process dies (T0).
        await self._write_manifest(job_ids, mode, len(params_list))
        return job_ids

    async def _write_manifest(
        self, job_ids: List[str], submission_mode: str, num_tasks: int
    ) -> None:
        """Persist everything a fresh client needs to re-attach this sweep.

        Written to BOTH the local sweep dir and the remote sweep dir. Holds the
        source-reconstruction fields + resolved remote paths + job ids, so
        ``hsm sweep collect <id>`` works with no dependence on the original
        process's in-memory state (or even the current ``.hsm/config.yaml``).
        """
        manifest = {
            "sweep_id": self.sweep_id,
            "backend": "slurm",
            "name": self.name,
            "host": self.host,
            "ssh_key": self.ssh_key,
            "ssh_port": self.ssh_port,
            "conda_env": self.conda_env,
            "python_path": self.python_path,
            "project_dir": self.project_dir,
            "remote_root": self.remote_root,
            "workdir": self.workdir,
            "archive_dir": self.archive_dir,
            "resolved_archive_dir": self._resolved_archive_dir,
            "archive_on": self.archive_on,
            "keep_remote_on_success": self.keep_remote_on_success,
            "remote_sweep_dir": self._remote_sweep_dir,
            "remote_tasks_dir": self._remote_tasks_dir,
            "submission_mode": submission_mode,
            "job_ids": list(job_ids),
            "num_tasks": num_tasks,
            "submitted_at": datetime.now().isoformat(),
        }
        content = json.dumps(manifest, indent=2, default=str)
        if self.sweep_dir is not None:
            try:
                (self.sweep_dir / ".hsm_manifest.json").write_text(content)
            except OSError as e:  # noqa: BLE001
                logger.warning(f"could not write local manifest: {e}")
        if self._remote_sweep_dir is not None:
            try:
                await self._write_remote_file(
                    f"{self._remote_sweep_dir}/.hsm_manifest.json", content
                )
            except Exception as e:  # noqa: BLE001
                logger.warning(f"could not write remote manifest: {e}")

    async def reattach(self, sweep_dir: Path, sweep_id: str, manifest: Dict[str, Any]) -> bool:
        """Reconnect to an already-submitted sweep WITHOUT re-pushing code.

        Used by ``hsm sweep collect`` after the launcher died: trusts the
        manifest's resolved remote paths instead of re-deriving (and crucially
        skips the rsync push that :meth:`setup` does). Returns False if the SSH
        connection can't be opened.
        """
        sweep_dir.mkdir(parents=True, exist_ok=True)
        (sweep_dir / "tasks").mkdir(parents=True, exist_ok=True)
        self.sweep_dir = sweep_dir
        self.sweep_id = sweep_id
        try:
            self._conn = await self._open_connection()
        except Exception as e:  # noqa: BLE001
            logger.error(f"SSH connection to {self.host} failed: {e}")
            return False
        self._remote_sweep_dir = manifest["remote_sweep_dir"]
        self._remote_tasks_dir = manifest.get(
            "remote_tasks_dir", f"{self._remote_sweep_dir}/tasks"
        )
        self._resolved_archive_dir = manifest.get("resolved_archive_dir")
        return True

    @classmethod
    def from_manifest(cls, manifest: Dict[str, Any]) -> "SSHSlurmComputeSource":
        """Reconstruct a source from a ``.hsm_manifest.json`` for re-attach.

        Carries no dependence on the current ``.hsm/config.yaml`` — the manifest
        captured everything at submit time. ``script_path`` is irrelevant for
        collection (we never re-submit), so it's left empty.
        """
        return cls(
            name=manifest.get("name") or manifest.get("host") or "remote",
            host=manifest.get("host"),
            ssh_key=manifest.get("ssh_key"),
            ssh_port=manifest.get("ssh_port"),
            conda_env=manifest.get("conda_env"),
            python_path=manifest.get("python_path"),
            project_dir=manifest.get("project_dir", "."),
            script_path="",
            remote_root=manifest.get("remote_root", "~/.hsm/runs"),
            workdir=manifest.get("workdir"),
            archive_dir=manifest.get("archive_dir"),
            archive_on=manifest.get("archive_on", "completed"),
            keep_remote_on_success=manifest.get("keep_remote_on_success", False),
        )

    async def _submit_array(
        self,
        params_list: List[Dict[str, Any]],
        sweep_id: str,
        spec: Optional[ResourceSpec],
        wandb_group: Optional[str],
        job_name_prefix: Optional[str],
    ) -> str:
        if not params_list:
            raise ValueError("Cannot submit an empty array")
        if self._conn is None or self._remote_sweep_dir is None:
            raise RuntimeError(
                f"SSHSlurmComputeSource {self.name!r} not set up; call setup() first"
            )
        effective = self._effective_spec(spec)
        directives = render_sbatch_directives(effective)
        prefix = job_name_prefix or sweep_id
        job_name = f"{prefix}_array"

        # parameter_combinations.json — written to the remote sweep dir
        # so the array template's $SLURM_ARRAY_TASK_ID python helper can
        # find it. The local mirror is created on collect_results().
        indexed = [
            {"index": i + 1, "global_index": i + 1, "params": p}
            for i, p in enumerate(params_list)
        ]
        remote_params_file = f"{self._remote_sweep_dir}/parameter_combinations.json"
        await self._write_remote_file(
            remote_params_file, json.dumps(indexed, indent=2)
        )

        script_content = render_template(
            "slurm_array.sh.j2",
            job_name=job_name,
            sweep_id=sweep_id,
            num_jobs=len(params_list),
            logs_dir=self._remote_logs_dir,
            tasks_dir=self._remote_tasks_dir,
            params_file=remote_params_file,
            sbatch_directives=directives,
            modules=list(effective.modules),
            pre_script=list(effective.pre_script),
            project_dir=self._remote_code_dir,
            python_path=self._run_prefix,
            script_path=self.script_path,
            wandb_group=wandb_group,
            uses_conda=bool(self.conda_env),
        )
        remote_script_path = f"{self._remote_scripts_dir}/{job_name}.slurm"
        await self._write_remote_file(remote_script_path, script_content)

        result = await self._ssh_run(
            f"sbatch {shlex.quote(remote_script_path)}", check=False
        )
        if (result.returncode or 0) != 0:
            stderr = (result.stderr or "").strip() or "no stderr"
            raise RuntimeError(
                f"sbatch (array) failed on {self.host}: {stderr}"
            )
        job_id = parse_sbatch_job_id(result.stdout or "")

        local_tasks_dir = self.sweep_dir / "tasks"  # type: ignore[union-attr]
        local_tasks_dir.mkdir(parents=True, exist_ok=True)

        self.active_jobs[job_id] = JobInfo(
            job_id=job_id,
            job_name=job_name,
            params={"_array_size": len(params_list)},
            source_name=self.name,
            status="PENDING",
            submit_time=datetime.now(),
            task_dir=str(local_tasks_dir),
        )
        self.stats.total_submitted += len(params_list)
        logger.info(
            f"Submitted Slurm array job {job_id} ({job_name}, "
            f"{len(params_list)} tasks) on {self.host} via SSH"
        )
        return job_id

    # ----------------------------------------------------------------- status
    async def _terminal_state_via_sacct(self, job_id: str) -> str:
        """Classify a job that's no longer in ``squeue`` via ``sacct``.

        Leaving the queue is NOT success — query the accounting DB for the real
        terminal state (COMPLETED vs FAILED/TIMEOUT/OOM/CANCELLED). Falls back
        to ``"COMPLETED"`` only when sacct returns nothing (accounting disabled
        or job unknown), preserving the old optimistic behavior on clusters
        that genuinely can't tell us better.
        """
        result = await self._ssh_run(
            f"sacct -j {shlex.quote(job_id)} -n -X -o State", check=False
        )
        state = None
        if (result.returncode or 0) == 0:
            state = parse_sacct_state(result.stdout or "")
        if state is None:
            logger.debug(
                f"sacct returned no state for job {job_id} on {self.host}; "
                f"assuming COMPLETED (accounting may be disabled)"
            )
            return "COMPLETED"
        return state

    async def get_job_status(self, job_id: str) -> str:
        result = await self._ssh_run(
            f"squeue -j {shlex.quote(job_id)} -h -o '%T'", check=False
        )
        if (result.returncode or 0) != 0 or not (result.stdout or "").strip():
            # Gone from the queue — ask sacct for the actual terminal state
            # rather than assuming success (queue-absence ≠ completion).
            status = await self._terminal_state_via_sacct(job_id)
        else:
            raw = (result.stdout or "").strip().splitlines()[0].strip()
            status = SLURM_STATE_MAP.get(raw, "RUNNING")
        if job_id in self.active_jobs:
            self.update_job_status(job_id, status)
        return status

    async def update_all_job_statuses(self) -> None:
        """Refresh every live job's status in a single ``squeue`` call.

        N round-trips → 1 round-trip per poll cycle for jobs still queued.
        Jobs absent from the response have left the queue; we then ask
        ``sacct`` for each one's terminal state — queue-absence is not a
        completion signal. Array parents whose tasks are still queued (squeue
        reports ``<id>_<task>`` rows) are recognized as still RUNNING so we
        don't hit sacct every poll while they run.
        """
        live = list(self.active_jobs.keys())
        if not live:
            return
        joined = ",".join(shlex.quote(j) for j in live)
        result = await self._ssh_run(
            f"squeue -j {joined} -h -o '%i %T'", check=False
        )
        seen: Dict[str, str] = {}
        for line in (result.stdout or "").splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            seen[parts[0]] = SLURM_STATE_MAP.get(parts[1], "RUNNING")
        for jid in live:
            if jid in seen:
                status = seen[jid]
            elif any(k.startswith(f"{jid}_") for k in seen):
                status = "RUNNING"  # array parent: some tasks still queued
            else:
                status = await self._terminal_state_via_sacct(jid)
            if jid in self.active_jobs:
                self.update_job_status(jid, status)

    async def cancel_job(self, job_id: str) -> bool:
        result = await self._ssh_run(
            f"scancel {shlex.quote(job_id)}", check=False
        )
        success = (result.returncode or 0) == 0
        if success and job_id in self.active_jobs:
            self.update_job_status(job_id, "CANCELLED")
        return success

    # ----------------------------------------------------------- collection
    async def _pull_tasks(self) -> int:
        """rsync-pull the remote ``tasks/`` dir down (additive, no ``--delete``).

        Idempotent and cheap to call repeatedly: rsync only transfers new /
        changed task dirs. Used both for the final pull in ``collect_results``
        and for the mid-flight incremental pulls in ``wait_for_all`` (T1), so a
        stuck task or a dead launcher can't strand the tasks that DID finish.
        """
        remote_tasks = f"{self._remote_sweep_dir}/tasks"
        local_tasks = str(self.sweep_dir / "tasks")
        pull_cmd = build_rsync_pull_cmd(self.host, remote_tasks, local_tasks)
        logger.info(f"rsync pull from {self.host}:{remote_tasks}")
        return await self._run_rsync(pull_cmd)

    async def wait_for_all(
        self,
        poll_interval: float = 5.0,
        on_progress: Optional[ProgressCallback] = None,
    ) -> Dict[str, str]:
        """Poll like the base loop, but rsync-pull ``tasks/`` whenever a job
        newly reaches a terminal state (T1).

        Partial progress then survives a single stuck task (a 12h maintenance
        hold no longer holds the other N-1 hostage) and a launcher death loses
        at most the last poll interval. The final ``collect_results()`` still
        runs the authoritative archive → pull → cleanup (CLAUDE.md gotcha 4/4b)
        — these mid-flight pulls are additive and idempotent, never a
        replacement. Mirrors :meth:`ComputeSource.wait_for_all`; kept in sync.
        """
        final_statuses: Dict[str, str] = {}
        for job_id, info in list(self.completed_jobs.items()):
            final_statuses[job_id] = info.status

        total = len(self.active_jobs) + len(final_statuses)
        if on_progress is not None:
            on_progress(len(final_statuses), max(total, 1))

        pulled_through = len(final_statuses)
        while self.active_jobs:
            for job_id in list(self.active_jobs.keys()):
                status = await self.get_job_status(job_id)
                if status in TERMINAL_STATES and job_id not in final_statuses:
                    final_statuses[job_id] = status
            # New completions this cycle → pull their task dirs now.
            if len(final_statuses) > pulled_through and self._remote_sweep_dir:
                pulled_through = len(final_statuses)
                try:
                    await self._pull_tasks()
                except Exception as e:  # noqa: BLE001 — best-effort; retried at end
                    logger.warning(
                        f"incremental tasks/ pull on {self.host} failed "
                        f"(will retry in collect_results): {e}"
                    )
            if on_progress is not None:
                total = len(self.active_jobs) + len(final_statuses)
                on_progress(len(final_statuses), max(total, 1))
            if self.active_jobs:
                await asyncio.sleep(poll_interval)
        return final_statuses

    async def collect_results(self, job_ids: Optional[List[str]] = None) -> bool:
        if self._remote_sweep_dir is None or self.sweep_dir is None:
            logger.warning(
                f"collect_results called before setup on {self.name}"
            )
            return False

        any_failed = any(
            j.status == "FAILED" for j in self.completed_jobs.values()
        )

        # Archive FIRST (server-side rsync /scratch → /shares — the durable
        # safety net) then pull tasks/ back to anahita. The archive uses
        # the cluster's internal network, which is much faster than
        # routing everything through our anahita ↔ S3IT link.
        if self._should_archive(any_failed):
            await self._archive_remote(any_failed)

        rc = await self._pull_tasks()
        if rc != 0:
            return False

        if not any_failed and not self.keep_remote_on_success:
            try:
                await self._ssh_run(
                    f"rm -rf {shlex.quote(self._remote_sweep_dir)}",
                    check=False,
                )
                logger.info(
                    f"Cleaned remote sweep dir {self._remote_sweep_dir} on "
                    f"{self.host}"
                )
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Failed to clean remote sweep dir: {e}")
        elif any_failed:
            logger.info(
                f"Keeping {self._remote_sweep_dir} on {self.host} for "
                f"inspection (at least one FAILED job)"
            )
        return True

    def _should_archive(self, any_failed: bool) -> bool:
        """Decide whether to run the server-side archive step.

        - ``archive_dir`` unset → never (no place to archive to)
        - ``archive_on == "never"`` → never (explicit opt-out)
        - ``archive_on == "always"`` → archive regardless of status
        - ``archive_on == "completed"`` (default) → archive only on
          full success (no FAILED jobs). Sweeps with failures stay
          on /scratch for inspection — the user re-runs after fixing.
        """
        if not self.archive_dir:
            return False
        if self.archive_on == "never":
            return False
        if self.archive_on == "always":
            return True
        return not any_failed  # "completed"

    async def _archive_remote(self, any_failed: bool) -> None:
        """Server-side rsync from ``_remote_sweep_dir`` to ``archive_dir/<sweep_id>``.

        Runs entirely on the remote — no data flows through anahita. On
        success drops a ``.archived`` sentinel containing timestamp + the
        source path, so a later inspection can tell "this is a frozen
        snapshot, not the live sweep dir."
        """
        assert self.archive_dir is not None  # _should_archive gates this
        # Use the remote-expanded archive_dir ($USER/~ resolved in setup()).
        archive_base = self._resolved_archive_dir or self.archive_dir
        archive_target = f"{archive_base}/{self.sweep_id}"
        cmd = (
            f"mkdir -p {shlex.quote(archive_target)} && "
            f"rsync -a {shlex.quote(self._remote_sweep_dir + '/')} "
            f"{shlex.quote(archive_target + '/')}"
        )
        logger.info(
            f"Archiving sweep on {self.host}: "
            f"{self._remote_sweep_dir} -> {archive_target}"
        )
        result = await self._ssh_run(cmd, check=False)
        if (result.returncode or 0) != 0:
            stderr = (result.stderr or "").strip() or "no stderr"
            logger.warning(
                f"Server-side archive on {self.host} failed (rc="
                f"{result.returncode}): {stderr}. The /scratch copy is "
                f"intact; you can retry by hand."
            )
            return

        sentinel_lines = [
            f"archived_at: {datetime.now().isoformat()}",
            f"source: {self._remote_sweep_dir}",
            f"sweep_id: {self.sweep_id}",
            f"any_failed: {any_failed}",
        ]
        sentinel_path = f"{archive_target}/.archived"
        await self._write_remote_file(sentinel_path, "\n".join(sentinel_lines) + "\n")
        logger.info(f"Archive sentinel written: {sentinel_path}")

    # -------------------------------------------------------------- health
    async def health_check(self) -> Dict[str, Any]:
        info: Dict[str, Any] = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "host": self.host,
            "active_jobs": len(self.active_jobs),
            "max_jobs": self.max_parallel_jobs,
            "utilization": f"{self.utilization:.1%}",
        }
        if self._conn is not None:
            try:
                result = await self._ssh_run(
                    "sinfo -h -o '%P %a %D' 2>/dev/null | head -5",
                    check=False,
                )
                if (result.returncode or 0) == 0:
                    info["connection"] = "ok"
                    info["partitions"] = (result.stdout or "").strip()
                else:
                    info["connection"] = "ok_but_no_sinfo"
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
            f"SSHSlurm:{self.name} ({self.host}): "
            f"{self.current_job_count} active jobs"
        )


# ---------------------------------------------------------- config factory


def build_ssh_slurm_source(
    *,
    name: str,
    remote_cfg: Optional[Dict[str, Any]] = None,
    distributed_cfg: Optional[Dict[str, Any]] = None,
    project_dir: str,
    script_path: str,
    default_spec: Optional[ResourceSpec] = None,
    conda_env_override: Optional[str] = None,
) -> "SSHSlurmComputeSource":
    """Build an :class:`SSHSlurmComputeSource` from local ``.hsm/config.yaml``.

    Resolves precedence per field:

    - explicit ``*_override`` argument > per-remote ``remote_cfg`` >
      global ``distributed_cfg`` > hardcoded default.

    ``remote_cfg`` is the entry under ``distributed.remotes[name]``. For
    SSH-driven Slurm sources, the relevant fields are:

    - ``host`` — ssh-config alias (defaults to ``name``)
    - ``backend: slurm`` — required at this level; how the orchestrator
      decided to call *this* factory rather than :func:`build_ssh_source`
    - ``conda_env`` — remote conda env to activate (sourced from common
      install locations by the rendered wrapper)
    - ``remote_root`` — where the per-project layout lives on the remote
      (default ``~/.hsm/runs``; override e.g. to ``/scratch/$USER/hsm-runs``
      on a cluster with ephemeral home — Phase 3 will let you point
      ``workdir`` there directly)
    - ``spec:`` — per-remote default :class:`ResourceSpec` (walltime,
      gpus, gpu_type, modules, qos, account, ...)
    - ``qos_whitelist`` — optional list of allowed QoS names; enforced
      at submit time
    """
    remote_cfg = dict(remote_cfg or {})
    distributed_cfg = dict(distributed_cfg or {})

    host = remote_cfg.get("host") or name
    ssh_key = remote_cfg.get("ssh_key")
    ssh_port = remote_cfg.get("ssh_port")
    max_parallel_jobs = remote_cfg.get("max_parallel_jobs") or 0  # 0 = no cap

    remote_spec_dict = remote_cfg.get("spec")
    if isinstance(remote_spec_dict, dict) and remote_spec_dict:
        try:
            per_remote_spec = ResourceSpec.from_dict(remote_spec_dict)
        except (TypeError, ValueError) as e:
            logger.warning(
                f"Invalid `spec:` block in remote {name!r}: {e}. Ignoring."
            )
            per_remote_spec = None
    else:
        per_remote_spec = None

    if per_remote_spec is not None:
        default_spec = per_remote_spec.merge(default_spec or ResourceSpec())

    conda_env = (
        conda_env_override
        if conda_env_override is not None
        else remote_cfg.get("conda_env", distributed_cfg.get("conda_env"))
    )
    python_path = remote_cfg.get(
        "python_path", distributed_cfg.get("python_path", "python")
    )

    remote_root = remote_cfg.get(
        "remote_root", distributed_cfg.get("remote_root", "~/.hsm/runs")
    )
    # Storage-tier awareness: workdir overrides remote_root for the active
    # run; archive_dir is the durable target on completion. Both opt-in.
    workdir = remote_cfg.get("workdir")
    archive_dir = remote_cfg.get("archive_dir")
    archive_on = str(remote_cfg.get("archive_on", "completed"))
    rsync_excludes = remote_cfg.get(
        "rsync_excludes", distributed_cfg.get("rsync_excludes")
    )
    keep_remote_on_success = bool(
        remote_cfg.get(
            "keep_remote_on_success",
            distributed_cfg.get("keep_remote_on_success", False),
        )
    )

    qos_whitelist_raw = remote_cfg.get("qos_whitelist")
    qos_whitelist: Optional[frozenset[str]]
    if qos_whitelist_raw and isinstance(qos_whitelist_raw, (list, tuple, set)):
        qos_whitelist = frozenset(str(q) for q in qos_whitelist_raw)
    else:
        qos_whitelist = None

    return SSHSlurmComputeSource(
        name=name,
        host=host,
        ssh_key=ssh_key,
        ssh_port=ssh_port,
        conda_env=conda_env,
        python_path=python_path,
        project_dir=project_dir,
        script_path=script_path,
        remote_root=remote_root,
        workdir=workdir,
        archive_dir=archive_dir,
        archive_on=archive_on,
        max_parallel_jobs=max_parallel_jobs,
        default_spec=default_spec,
        rsync_excludes=rsync_excludes,
        keep_remote_on_success=keep_remote_on_success,
        qos_whitelist=qos_whitelist,
    )
