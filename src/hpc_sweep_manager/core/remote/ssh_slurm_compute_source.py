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
       REMOTE paths baked in; write and submit each script over one channel
       (``cat > <script> && sbatch <script>``) and read the job id from
       sbatch's stdout. Slurm remotes default to one job array per sweep.
    3. Status: :class:`SlurmBase` (shared with the native source) asks one
       ``squeue -u <user>`` and one ``sacct`` per poll, whatever the job count, and
       never reads a failed call as a verdict. A dropped ssh link is reopened once per
       command (:meth:`_ssh_run`; a 30 s keepalive notices it); while it stays down the
       polls fail and every job keeps its state.
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
import contextlib
import getpass
import json
import logging
import re
import shlex
import time
from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncssh

from ..common.chain import ChainState
from ..common.compute_source import (
    TERMINAL_STATES,
    JobInfo,
    SubmissionMode,
)
from ..common.resource_spec import ResourceSpec
from ..common.resumable import ChunkProgress, ResumableConfig, ResumableContext
from ..common.templating import params_to_hydra_args, params_to_yaml, render_template
from ..hpc.gpu_planner import (
    SubArraySubmission,
    build_array_submissions,
    jobs_manifest_entries,
    normalize_speed_factors,
    replace_sub_walltime,
)
from ..hpc.scheduler_queue import strip_array_suffix
from ..hpc.slurm_base import SlurmBase
from ..hpc.slurm_protocol import (
    SLURM_STATE_MAP,
    format_signal,
    parse_sbatch_job_id,
    render_sbatch_directives,
)
from .discovery import LINK_GIVE_UP_S, agent_stalled
from .push_exec import (
    DEFAULT_RSYNC_EXCLUDES,
    build_rsync_pull_cmd,
    build_rsync_push_cmd,
    own_snapshot,
    pin_code_refs,
    resolve_run_prefix,
    snapshot_prepare_cmd,
)

logger = logging.getLogger(__name__)

SSH_TIMEOUT_S = 300  # a command that hangs this long counts as a dropped link (bulk ones: none)


class SSHSlurmComputeSource(SlurmBase):
    """Push-model Slurm-over-SSH compute source.

    The remote needs ``bash`` + ``rsync`` + ``sbatch``/``squeue``/``scancel``
    on the user's PATH. HSM does NOT have to be installed on the remote —
    only the rsync'd code mirror runs there.
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
        workdir: str | None = None,
        archive_dir: str | None = None,
        archive_on: str = "completed",
        max_parallel_jobs: int = 0,
        default_spec: ResourceSpec | None = None,
        rsync_excludes: Sequence[str] | None = None,
        keep_remote_on_success: bool = False,
        qos_whitelist: frozenset[str] | None = None,
        speed_factors: dict[str, float] | None = None,
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
                f"archive_on must be 'completed', 'always', or 'never'; got {archive_on!r}"
            )
        self.archive_on = archive_on
        self.default_spec = default_spec or ResourceSpec()
        self.rsync_excludes = (
            # Extend the defaults (dedup, order-preserving) rather than replace —
            # so a user adding `outputs/` doesn't silently start pushing `.git`.
            tuple(dict.fromkeys((*DEFAULT_RSYNC_EXCLUDES, *rsync_excludes)))
            if rsync_excludes is not None
            else DEFAULT_RSYNC_EXCLUDES
        )
        self.keep_remote_on_success = keep_remote_on_success
        self.qos_whitelist = qos_whitelist
        # GPU type → relative runtime multiplier; parameterizes the
        # multi-gpu_type planner (core/hpc/gpu_planner). None = all 1.0.
        self.speed_factors = dict(speed_factors) if speed_factors else None

        # Populated by setup()
        self._conn: Any = None
        self._project_name = Path(self.project_dir).name or "project"
        self._remote_code_dir: str | None = None
        self._remote_sweep_dir: str | None = None
        self._remote_tasks_dir: str | None = None
        self._remote_logs_dir: str | None = None
        self._remote_scripts_dir: str | None = None
        # archive_dir with $USER/$HOME/~ expanded on the remote (set in setup()).
        self._resolved_archive_dir: str | None = None
        self.sweep_dir: Path | None = None
        self.sweep_id: str | None = None
        self._run_prefix: str = "python"
        # Resumable chains (issue #12): the driver sets _pull_excludes so the
        # incremental + final tasks/ pulls skip the heavy checkpoint subdir
        # (it rides the cheap server-side archive instead of the WAN). The
        # config/state are restored by from_manifest for `hsm sweep advance`.
        self._pull_excludes: tuple[str, ...] = ()
        self._resumable_config: ResumableConfig | None = None
        self._chain_state: ChainState | None = None
        self._down_since: float | None = None  # when the ssh link went down (None: up)

    # ------------------------------------------------------------- I/O seams
    async def _open_connection(self) -> Any:
        """Open the persistent asyncssh connection. Overridden in tests.

        With a keepalive: a dead link closes within ~90 s, and :meth:`_ssh_run` reconnects."""
        from .discovery import create_ssh_connection

        return await create_ssh_connection(
            self.host, self.ssh_key, self.ssh_port, keepalive_interval=30
        )

    async def _run_rsync(self, cmd: list[str]) -> int:
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
    def _effective_spec(self, spec: ResourceSpec | None) -> ResourceSpec:
        merged = self.default_spec.merge(spec)
        if (
            merged.qos is not None
            and self.qos_whitelist is not None
            and merged.qos not in self.qos_whitelist
        ):
            raise ValueError(
                f"qos={merged.qos!r} is not in the whitelist {sorted(self.qos_whitelist)}"
            )
        return merged

    async def _ssh_run(
        self,
        cmd: str,
        *,
        input: str | None = None,
        timeout: float | None = SSH_TIMEOUT_S,
        resend: bool = True,
    ) -> Any:
        """One remote command; a ``returncode`` of None means unknown (the link died as it ran).

        A failed connection is reopened and the command sent once more; with ``resend=False``
        only when it never started (its channel didn't open), else the error is raised."""
        if self._conn is None:
            raise RuntimeError(
                f"SSHSlurmComputeSource {self.name!r} not connected; call setup() first"
            )
        try:
            return await self._conn.run(cmd, input=input, check=False, timeout=timeout)
        except (OSError, asyncssh.Error) as e:
            logger.warning(f"{self.host}: connection lost ({e!r}); reconnecting")
            with contextlib.suppress(Exception):
                self._conn.close()
            self._conn = await self._open_connection()
            never_ran = isinstance(e, asyncssh.ChannelOpenError) and (
                e.code != asyncssh.OPEN_REQUEST_SESSION_FAILED  # an unanswered exec may have run
            )
            if not (resend or never_ran):
                raise
            return await self._conn.run(cmd, input=input, check=False, timeout=timeout)

    async def _sbatch(self, job_name: str, script: str) -> str:
        """Write ``scripts/<job_name>.slurm``, then submit it; return the job id.

        The write is confirmed first, so sbatch never waits on stdin. sbatch has no time bound
        (a busy controller can take minutes and still queue the job; the keepalive ends a dead
        link), and a lost reply is never answered with a second sbatch, which could queue the
        job twice: the job is looked up instead (:meth:`_queued_id`)."""
        path = f"{self._remote_scripts_dir}/{job_name}.slurm"
        await self._write_remote_file(path, script)
        lost = "no exit status"
        try:
            result = await self._ssh_run(f"sbatch {shlex.quote(path)}", resend=False, timeout=None)
        except (OSError, asyncssh.Error) as e:
            result, lost = None, repr(e)
        if result is None or result.returncode is None:
            if job_id := await self._queued_id(job_name, path):
                logger.warning(f"sbatch {path}: the reply was lost ({lost}); queued as {job_id}")
                return job_id
            raise RuntimeError(
                f"sbatch {path}: the reply from {self.host} was lost ({lost}); the job may be "
                f"queued. Check `squeue -n {job_name}` (`sacct -X --name {job_name}` once it "
                f"left the queue) and `scancel -n {job_name}` before submitting again"
            )
        if result.returncode != 0:
            stderr = (result.stderr or "").strip() or "no stderr"
            if "array" in stderr:  # e.g. above the cluster's MaxArraySize
                stderr += " (too many tasks for one job array? try --mode individual)"
            raise RuntimeError(f"sbatch {path} failed on {self.host}: {stderr}")
        return parse_sbatch_job_id(result.stdout or "")

    async def _queued_id(self, job_name: str, script: str) -> str | None:
        """The one live job named ``job_name`` that runs ``script`` (a path unique to the
        sweep; a chain's finished chunks share both, hence live only), or None."""
        user = self.slurm_user or getpass.getuser()
        argv = ["squeue", "-h", "-u", user, "-n", job_name, "-o", "%i %T %o"]
        rc, out, _ = await self._sh(argv)
        rows = [line.split() for line in out.splitlines()] if rc == 0 else []
        ids = {
            strip_array_suffix(row[0])
            for row in rows
            if row[2:] == [script] and SLURM_STATE_MAP.get(row[1]) not in TERMINAL_STATES
        }
        return ids.pop() if len(ids) == 1 else None

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
        # Only round-trip when there's something a shell would expand. Plain
        # absolute paths pass through (no wasted SSH call; a literal glob isn't
        # silently multi-expanded into a corrupted dest).
        if "~" not in path and "$" not in path:
            return path
        result = await self._ssh_run(f"echo {path}")
        if result.returncode != 0:  # None: unknown; a literal `$USER` would break every squeue
            raise RuntimeError(f"could not expand {path!r} on {self.host}: {result.stderr!r}")
        lines = (result.stdout or "").strip().splitlines()
        first = lines[0].strip() if lines else ""
        return first or path

    async def _write_remote_file(self, remote_path: str, content: str) -> None:
        """``cat >`` the content (asyncssh pipes ``input`` over the channel; no scp or sftp)."""
        result = await self._ssh_run(f"cat > {shlex.quote(remote_path)}", input=content)
        if result.returncode != 0:  # None: unknown
            err = (result.stderr or "").strip() or f"rc={result.returncode}"
            raise RuntimeError(f"writing {remote_path} on {self.host} failed: {err}")

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
        check = await self._ssh_run("command -v sbatch && command -v squeue && command -v scancel")
        if check.returncode != 0:
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
        self.slurm_user = await self._resolve_remote_path("$USER")  # whose squeue to read
        self._resolved_archive_dir = (
            await self._resolve_remote_path(self.archive_dir) if self.archive_dir else None
        )
        project_root = f"{resolved_root}/{self._project_name}"
        self._remote_code_dir = f"{project_root}/snapshots/{sweep_id}"
        self._remote_sweep_dir = f"{project_root}/sweeps/{sweep_id}"
        self._remote_tasks_dir = f"{self._remote_sweep_dir}/tasks"
        self._remote_logs_dir = f"{self._remote_sweep_dir}/logs"
        self._remote_scripts_dir = f"{self._remote_sweep_dir}/scripts"

        # This sweep's code snapshot and dirs, hard-linked against the newest snapshot (S4).
        sweep_dirs = [self._remote_tasks_dir, self._remote_logs_dir, self._remote_scripts_dir]
        prep = await self._ssh_run(snapshot_prepare_cmd(project_root, sweep_id, sweep_dirs))
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

        self._run_prefix = resolve_run_prefix(self.conda_env, self.python_path)
        self.stats.health_status = "healthy"
        self.stats.last_health_check = datetime.now()
        logger.info(
            f"SSHSlurmComputeSource {self.name}@{self.host}: ready "
            f"(remote_sweep_dir={self._remote_sweep_dir}, "
            f"run_prefix={self._run_prefix!r})"
        )
        return True

    # ----------------------------------------------------------------- submit
    async def submit_job(
        self,
        params: dict[str, Any],
        job_name: str,
        sweep_id: str,
        wandb_group: str | None = None,
        spec: ResourceSpec | None = None,
    ) -> str:
        if self._conn is None or self._remote_sweep_dir is None:
            raise RuntimeError(
                f"SSHSlurmComputeSource {self.name!r} not set up; call setup() first"
            )
        effective = await self._off_gpu_nodes(self._effective_spec(spec))
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
        job_id = await self._sbatch(job_name, script_content)

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
        logger.info(f"Submitted Slurm job {job_id} ({job_name}) on {self.host} via SSH")
        return job_id

    async def submit_batch(
        self,
        params_list: list[dict[str, Any]],
        sweep_id: str,
        mode: SubmissionMode = "individual",
        spec: ResourceSpec | None = None,
        wandb_group: str | None = None,
        job_name_prefix: str | None = None,
        costs: Sequence[float] | None = None,
        *,
        dependency: str | None = None,
        resumable: ResumableContext | None = None,
    ) -> list[str]:
        cap = resumable.config.chunk_walltime if resumable else None
        await self._warn_reservations(cap or self._effective_spec(spec).walltime)
        if resumable is not None and mode != "array":
            raise ValueError(
                f"resumable chains use array mode (one chunk = one Slurm array); got mode={mode!r}"
            )
        try:
            if mode == "array":
                job_ids = await self._submit_array(
                    params_list,
                    sweep_id,
                    spec,
                    wandb_group,
                    job_name_prefix,
                    costs,
                    dependency=dependency,
                    resumable=resumable,
                )
            elif isinstance(self._effective_spec(spec).gpu_type, tuple):
                raise ValueError(
                    "Multi-type gpu_type lists are supported in array mode "
                    "only — use `--mode array` (one Slurm array per GPU type)."
                )
            else:
                job_ids = await super().submit_batch(
                    params_list, sweep_id, mode, spec, wandb_group, job_name_prefix
                )
        except BaseException:
            # Submission is a loop of sbatch calls (one per task, or one per GPU type): a failure
            # or a Ctrl-C partway leaves earlier jobs live on the cluster. Persist them before
            # re-raising, so `hsm sweep collect` can re-attach (field report #8, tracker S3) —
            # except in a resumable chain, whose manifest the driver owns: overwriting it would
            # stop `advance` recognising the chain and let `collect` clean it up.
            if self.active_jobs:
                live = list(self.active_jobs)
                ids = " ".join(live) if len(live) <= 10 else "<the job_ids in .hsm_manifest.json>"
                if resumable is not None:
                    logger.error(
                        f"chunk submission stopped partway: cancel {ids} (ssh {self.host} "
                        f"scancel {ids}) before `hsm sweep advance {sweep_id}`, whose manifest "
                        f"still names the previous chunk."
                    )
                    raise
                logger.error(
                    f"submission stopped partway: {len(live)} job(s) already live on {self.host}. "
                    f"Writing the manifest so `hsm sweep collect {sweep_id}` can re-attach; "
                    f"to abort instead: ssh {self.host} scancel {ids}"
                )
                await self._write_manifest(live, mode, len(params_list))
            raise
        # Drop a re-attach manifest (local + remote) so `hsm sweep collect <id>`
        # can pull/archive after the launching process dies (T0). In resumable
        # mode the chain DRIVER owns the manifest (it carries the chain state +
        # per-chunk job ids via persist_chain_manifest), so don't double-write.
        if resumable is None:
            await self._write_manifest(job_ids, mode, len(params_list))
        return job_ids

    async def persist_chain_manifest(
        self,
        *,
        resumable: dict[str, Any],
        chain: dict[str, Any],
        job_ids: list[str],
        num_tasks: int,
    ) -> None:
        """Re-write the manifest with the resumable config + chain state so a
        detached ``hsm sweep advance`` can reconstruct and keep driving."""
        await self._write_manifest(
            job_ids, "array", num_tasks, resumable_manifest=resumable, chain=chain
        )

    async def _write_manifest(
        self,
        job_ids: list[str],
        submission_mode: str,
        num_tasks: int,
        *,
        resumable_manifest: dict[str, Any] | None = None,
        chain: dict[str, Any] | None = None,
    ) -> None:
        """Persist everything a fresh client needs to re-attach this sweep.

        Written to BOTH the local sweep dir and the remote sweep dir. Holds the
        source-reconstruction fields + resolved remote paths + job ids, so
        ``hsm sweep collect <id>`` works with no dependence on the original
        process's in-memory state (or even the current ``.hsm/config.yaml``).
        ``resumable_manifest``/``chain`` are the resumable-chain additions
        (issue #12), omitted for ordinary sweeps.
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
            "remote_code_dir": self._remote_code_dir,  # this sweep's snapshot (advance re-uses it)
            "submission_mode": submission_mode,
            "job_ids": list(job_ids),
            "num_tasks": num_tasks,
            # Per-job detail (gpu_type, task count) — lets consumers stop
            # guessing per-job totals from len(job_ids)==1 (multi-type
            # sweeps legitimately have several arrays).
            "jobs": jobs_manifest_entries(
                job_ids,
                {jid: (info.params or {}) for jid, info in self.active_jobs.items()},
            ),
            "submitted_at": datetime.now().isoformat(),
        }
        if resumable_manifest is not None:
            manifest["resumable"] = resumable_manifest
            # Store everything `hsm sweep advance` needs to re-submit the next
            # chunk without re-reading a possibly-changed .hsm/config.yaml: the
            # effective spec (the per-chunk walltime cap is reapplied at submit,
            # so the FULL walltime here is correct) and the train script; the
            # sweep's code snapshot (``remote_code_dir``) persists between chunks.
            manifest["spec"] = self.default_spec.to_dict()
            manifest["script_path"] = self.script_path
        if chain is not None:
            manifest["chain"] = chain
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

    async def reattach(self, sweep_dir: Path, sweep_id: str, manifest: dict[str, Any]) -> bool:
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
            self.slurm_user = await self._resolve_remote_path("$USER")
        except Exception as e:  # noqa: BLE001
            logger.error(f"SSH connection to {self.host} failed: {e}")
            return False
        self._remote_sweep_dir = manifest["remote_sweep_dir"]
        self._remote_tasks_dir = manifest.get("remote_tasks_dir", f"{self._remote_sweep_dir}/tasks")
        self._resolved_archive_dir = manifest.get("resolved_archive_dir")
        self._remote_code_dir = manifest.get("remote_code_dir")
        return True

    @classmethod
    def from_manifest(cls, manifest: dict[str, Any]) -> SSHSlurmComputeSource:
        """Reconstruct a source from a ``.hsm_manifest.json`` for re-attach.

        Carries no dependence on the current ``.hsm/config.yaml`` — the manifest
        captured everything at submit time. ``script_path`` is irrelevant for
        collection (we never re-submit), so it's left empty.
        """
        # For a resumable chain, `advance` re-submits — so restore the script
        # path + effective spec (ordinary collect never re-submits, leaves them
        # empty/default).
        is_chain = bool(manifest.get("resumable"))
        spec_block = manifest.get("spec")
        inst = cls(
            name=manifest.get("name") or manifest.get("host") or "remote",
            host=manifest.get("host"),
            ssh_key=manifest.get("ssh_key"),
            ssh_port=manifest.get("ssh_port"),
            conda_env=manifest.get("conda_env"),
            python_path=manifest.get("python_path"),
            project_dir=manifest.get("project_dir", "."),
            script_path=manifest.get("script_path", "") if is_chain else "",
            remote_root=manifest.get("remote_root", "~/.hsm/runs"),
            workdir=manifest.get("workdir"),
            archive_dir=manifest.get("archive_dir"),
            archive_on=manifest.get("archive_on", "completed"),
            keep_remote_on_success=manifest.get("keep_remote_on_success", False),
            default_spec=ResourceSpec.from_dict(spec_block) if spec_block else None,
        )
        # Resumable chains (issue #12): restore the config + last chain state so
        # `hsm sweep advance` can keep driving a detached chain.
        rblock = manifest.get("resumable")
        if rblock:
            inst._resumable_config = ResumableConfig.from_manifest(rblock)
            inst._chain_state = ChainState.from_dict((manifest.get("chain") or {}).get("state"))
            if inst._should_archive(False):  # the checkpoints ride the archive, not the WAN
                inst._pull_excludes = (f"*/{inst._resumable_config.checkpoint_subdir}/",)
        return inst

    async def _submit_array(
        self,
        params_list: list[dict[str, Any]],
        sweep_id: str,
        spec: ResourceSpec | None,
        wandb_group: str | None,
        job_name_prefix: str | None,
        costs: Sequence[float] | None = None,
        *,
        dependency: str | None = None,
        resumable: ResumableContext | None = None,
    ) -> list[str]:
        """Submit the sweep as 1..K Slurm arrays.

        Single-type specs submit exactly one array (today's behavior).
        Multi-type specs (``gpu_type`` tuple) are planned into one
        sub-array per GPU type via :func:`gpu_planner.build_array_submissions`
        — LPT task assignment by relative ``costs``, per-type walltimes from
        ``speed_factors``.

        In a resumable chunk (issue #12) every sub-array's walltime is CAPPED at
        ``chunk_walltime`` (chunking caps, it doesn't scale): #7's cost-based
        placement still runs, but the cost-scaled per-type walltime is flattened
        to the cap. The same ``dependency`` token (``afterany:<prev parents>``)
        is set on every sub-array so chunk k+1 starts only after the whole
        previous chunk cleared.
        """
        if not params_list:
            raise ValueError("Cannot submit an empty array")
        if self._conn is None or self._remote_sweep_dir is None:
            raise RuntimeError(
                f"SSHSlurmComputeSource {self.name!r} not set up; call setup() first"
            )
        effective = await self._off_gpu_nodes(self._effective_spec(spec))
        submissions = build_array_submissions(
            params_list=params_list,
            effective_spec=effective,
            prefix=job_name_prefix or sweep_id,
            speed_factors=self.speed_factors,
            costs=costs,
        )
        if resumable is not None:
            cap = resumable.config.chunk_walltime
            submissions = [replace_sub_walltime(sub, cap) for sub in submissions]
        return [
            await self._submit_one_array(
                sub, sweep_id, wandb_group, dependency=dependency, resumable=resumable
            )
            for sub in submissions
        ]

    async def _submit_one_array(
        self,
        sub: SubArraySubmission,
        sweep_id: str,
        wandb_group: str | None,
        *,
        dependency: str | None = None,
        resumable: ResumableContext | None = None,
    ) -> str:
        signal = format_signal(resumable.config.signal_grace) if resumable else None
        directives = render_sbatch_directives(sub.spec, dependency=dependency, signal=signal)

        # Per-(sub-)array params file — written to the remote sweep dir so
        # the array template's $SLURM_ARRAY_TASK_ID python helper can find
        # it ("index" is array-local; "global_index" keeps the task's
        # original 1..N position so tasks/task_%04d stays globally
        # numbered). The local mirror is created on collect_results().
        remote_params_file = f"{self._remote_sweep_dir}/{sub.params_filename}"
        await self._write_remote_file(remote_params_file, json.dumps(list(sub.entries), indent=2))

        rcfg = resumable.config if resumable else None
        script_content = render_template(
            "slurm_array.sh.j2",
            job_name=sub.job_name,
            sweep_id=sweep_id,
            num_jobs=len(sub.entries),
            array_throttle=sub.spec.array_throttle,
            logs_dir=self._remote_logs_dir,
            tasks_dir=self._remote_tasks_dir,
            params_file=remote_params_file,
            sbatch_directives=directives,
            modules=list(sub.spec.modules),
            pre_script=list(sub.spec.pre_script),
            project_dir=self._remote_code_dir,
            python_path=self._run_prefix,
            script_path=self.script_path,
            wandb_group=wandb_group,
            uses_conda=bool(self.conda_env),
            gpu_type=sub.gpu_type,
            resumable=resumable is not None,
            resume_from_present=(resumable.resume_from_present if resumable else False),
            resume_arg=(rcfg.resume_arg if rcfg else None),
            done_sentinel=(rcfg.done_sentinel if rcfg else ".hsm_done"),
            checkpoint_subdir=(rcfg.checkpoint_subdir if rcfg else "resume"),
        )
        job_id = await self._sbatch(sub.job_name, script_content)

        local_tasks_dir = self.sweep_dir / "tasks"  # type: ignore[union-attr]
        local_tasks_dir.mkdir(parents=True, exist_ok=True)

        params: dict[str, Any] = {"_array_size": len(sub.entries)}
        if sub.gpu_type:
            params["_gpu_type"] = sub.gpu_type
        self.active_jobs[job_id] = JobInfo(
            job_id=job_id,
            job_name=sub.job_name,
            params=params,
            source_name=self.name,
            status="PENDING",
            submit_time=datetime.now(),
            task_dir=str(local_tasks_dir),
        )
        self.stats.total_submitted += len(sub.entries)
        gpu_note = f", gpu_type={sub.gpu_type}" if sub.gpu_type else ""
        logger.info(
            f"Submitted Slurm array job {job_id} ({sub.job_name}, "
            f"{len(sub.entries)} tasks{gpu_note}) on {self.host} via SSH"
        )
        return job_id

    # ----------------------------------------------------------------- status
    async def _sh(self, argv: Sequence[str]) -> tuple[int, str, str]:
        try:
            result = await self._ssh_run(shlex.join(argv))
        except (OSError, asyncssh.Error) as e:  # still unreachable: a failed call, no verdict
            self._down_since = self._down_since or time.monotonic()
            if time.monotonic() - self._down_since > LINK_GIVE_UP_S:
                raise ConnectionError(
                    f"{self.host} unreachable for {LINK_GIVE_UP_S // 60} min; its jobs stay in "
                    f"Slurm (`hsm queue mine --remote {self.name}` lists them; `hsm sweep collect "
                    f"{self.sweep_id}`, or `advance` for a chain, re-attaches a sweep it ran alone)"
                ) from e
            return 255, "", f"ssh to {self.host}: {e!r}"
        if result.returncode is None:  # unknown: the link died as it ran
            return 255, result.stdout or "", "no exit status"
        self._down_since = None
        return result.returncode, result.stdout or "", result.stderr or ""

    async def cancel_job(self, job_id: str) -> bool:
        result = await self._ssh_run(f"scancel {shlex.quote(job_id)}")
        success = result.returncode == 0
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
        # _pull_excludes is set by the resumable driver to skip the heavy
        # checkpoint subdir (issue #12); empty for ordinary sweeps.
        pull_cmd = build_rsync_pull_cmd(
            self.host,
            remote_tasks,
            local_tasks,
            excludes=self._pull_excludes,
            agentless=agent_stalled(self.host),
        )
        logger.info(f"rsync pull from {self.host}:{remote_tasks}")
        return await self._run_rsync(pull_cmd)

    async def chunk_progress(
        self, num_tasks: int, *, done_sentinel: str, checkpoint_subdir: str
    ) -> ChunkProgress | None:
        """Probe done-sentinels + newest checkpoint mtime in ONE ssh round-trip.

        HSM stats only paths it itself provided — it never reads checkpoint
        CONTENTS (the generality guardrail). The remote ``find`` emits the
        sentinel paths (one per done task) and the single max checkpoint mtime;
        the driver diffs successive observations into the ``progressed`` bool.
        None when the probe failed: an empty one would count as a chunk without progress.
        """
        if self._conn is None or self._remote_tasks_dir is None:
            return ChunkProgress(done_indices=frozenset(), checkpoint_mtime=None)
        tasks = shlex.quote(self._remote_tasks_dir)
        sent = shlex.quote(done_sentinel)
        ckpt_glob = shlex.quote(f"*/{checkpoint_subdir}/*")
        # Two finds joined by a sentinel line: the first lists done-sentinel
        # paths (bounded by -maxdepth 2 = tasks/task_N/.hsm_done); the second
        # reduces every checkpoint file's mtime to a single max so the output
        # stays tiny no matter how many checkpoint files exist.
        cmd = (
            f"find {tasks} -maxdepth 2 -name {sent} -type f 2>/dev/null; "
            f"echo HSM_SEP; "
            f"find {tasks} -path {ckpt_glob} -type f -printf '%T@\\n' 2>/dev/null "
            f"| sort -n | tail -1"
        )
        rc, out, err = await self._sh(["bash", "-c", cmd])
        if rc != 0:
            logger.warning(f"progress probe on {self.host} failed (rc={rc}): {err.strip()}")
            return None
        before, _, after = out.partition("HSM_SEP")
        done: set[int] = set()
        for line in before.splitlines():
            # Anchor to the sentinel's PARENT (`.../task_<N>/<sentinel>` at the
            # end), not the first `/task_N/` — a workdir prefix could itself
            # contain a `/task_<digit>/` component and mis-parse the index.
            m = re.search(r"task_(\d+)/[^/]+$", line.strip())
            if m:
                done.add(int(m.group(1)))
        mtime: float | None = None
        tail = after.strip().splitlines()
        if tail:
            try:
                mtime = float(tail[-1].strip())
            except ValueError:
                mtime = None
        return ChunkProgress(done_indices=frozenset(done), checkpoint_mtime=mtime)

    async def _after_poll(self, newly_done: int) -> None:
        """Pull ``tasks/`` whenever a job newly finishes (T1), so a stuck task or a dead launcher
        strands nothing that is done. Additive and idempotent: the final
        :meth:`collect_results` still runs archive → pull → cleanup (gotchas 4/4b)."""
        if newly_done and self._remote_sweep_dir:
            try:
                await self._pull_tasks()
            except Exception as e:  # noqa: BLE001 — best-effort; retried in collect_results
                logger.warning(f"incremental tasks/ pull on {self.host} failed: {e}")

    async def collect_results(
        self, job_ids: list[str] | None = None, *, defer_cleanup: bool = False
    ) -> bool:
        if self._remote_sweep_dir is None or self.sweep_dir is None:
            logger.warning(f"collect_results called before setup on {self.name}")
            return False

        # Resumable chains (issue #12): between chunks pull partial progress but
        # do NOT archive or rm -rf — the next chunk's checkpoints live in the
        # remote sweep dir. Only the terminal DONE/FAILED call cleans up.
        if defer_cleanup:
            rc = await self._pull_tasks()
            return rc == 0

        any_failed = any(j.status == "FAILED" for j in self.completed_jobs.values())

        # Archive FIRST (server-side rsync /scratch → /shares — the durable
        # safety net) then pull tasks/ back to anahita. The archive uses
        # the cluster's internal network, which is much faster than
        # routing everything through our anahita ↔ S3IT link.
        if self._should_archive(any_failed) and not await self._archive_remote(any_failed):
            return False  # no pull, no rm -rf: the remote dir stays for a later collect

        rc = await self._pull_tasks()
        if rc != 0:
            return False

        if not any_failed and not self.keep_remote_on_success:
            try:
                dirs = (self._remote_sweep_dir, own_snapshot(self._remote_code_dir, self.sweep_id))
                rm = "rm -rf " + " ".join(shlex.quote(d) for d in dirs if d)
                await self._ssh_run(rm, timeout=None)
                logger.info(f"Cleaned remote sweep dir {self._remote_sweep_dir} on {self.host}")
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

    async def _archive_remote(self, any_failed: bool) -> bool:
        """Server-side rsync from ``_remote_sweep_dir`` to ``archive_dir/<sweep_id>``.

        Runs entirely on the remote — no data flows through anahita. On
        success drops a ``.archived`` sentinel containing timestamp + the
        source path, so a later inspection can tell "this is a frozen
        snapshot, not the live sweep dir." False when the rsync failed.
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
        if snapshot := own_snapshot(self._remote_code_dir, self.sweep_id):
            # The code the sweep ran goes with its results (a reproducibility record).
            # --link-dest against the newest archived code: unchanged files cost nothing.
            prev = f"$(ls -1d {shlex.quote(archive_base)}/*/code/ 2>/dev/null | tail -1)"
            code_target = shlex.quote(f"{archive_target}/code/")
            cmd += (
                f' && p="{prev}" && rsync -a ${{p:+--link-dest="$p"}} '
                f"{shlex.quote(snapshot + '/')} {code_target}"
            )
        logger.info(f"Archiving sweep on {self.host}: {self._remote_sweep_dir} -> {archive_target}")
        result = await self._ssh_run(cmd, timeout=None)
        if result.returncode != 0:  # None: unknown, e.g. the link died during the rsync
            stderr = (result.stderr or "").strip() or "no stderr"
            logger.warning(
                f"Server-side archive on {self.host} failed (rc="
                f"{result.returncode}): {stderr}. The /scratch copy is kept; "
                f"`hsm sweep collect {self.sweep_id}` archives it again."
            )
            return False

        sentinel_lines = [
            f"archived_at: {datetime.now().isoformat()}",
            f"source: {self._remote_sweep_dir}",
            f"sweep_id: {self.sweep_id}",
            f"any_failed: {any_failed}",
        ]
        sentinel_path = f"{archive_target}/.archived"
        await self._write_remote_file(sentinel_path, "\n".join(sentinel_lines) + "\n")
        logger.info(f"Archive sentinel written: {sentinel_path}")
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
        }
        if self._conn is not None:
            try:
                result = await self._ssh_run("sinfo -h -o '%P %a %D' 2>/dev/null | head -5")
                if result.returncode == 0:
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
        return f"SSHSlurm:{self.name} ({self.host}): {self.current_job_count} active jobs"


# ---------------------------------------------------------- config factory


def build_ssh_slurm_source(
    *,
    name: str,
    remote_cfg: dict[str, Any] | None = None,
    distributed_cfg: dict[str, Any] | None = None,
    project_dir: str,
    script_path: str,
    default_spec: ResourceSpec | None = None,
    conda_env_override: str | None = None,
) -> SSHSlurmComputeSource:
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
    max_parallel_jobs = remote_cfg.get("max_parallel_jobs")  # None or 0 = no cap, as before
    if max_parallel_jobs is not None and (
        isinstance(max_parallel_jobs, bool)
        or not isinstance(max_parallel_jobs, int)
        or max_parallel_jobs < 0
    ):
        raise ValueError(f"remote {name!r}: max_parallel_jobs must be a whole number (0: no cap)")

    remote_spec_dict = remote_cfg.get("spec")
    if isinstance(remote_spec_dict, dict) and remote_spec_dict:
        if "speed_factors" in remote_spec_dict:
            # Plausible misplacement: it belongs BESIDE spec:, not inside it
            # (per-source planner knob, not a per-job resource). Filtered here
            # so this hint replaces from_dict's generic unknown-key warning.
            logger.warning(
                f"remote {name!r}: `speed_factors` belongs at the remote "
                f"level (sibling of `spec:`), not inside it — ignoring the "
                f"misplaced entry. Move it up one level."
            )
            remote_spec_dict = {k: v for k, v in remote_spec_dict.items() if k != "speed_factors"}
        per_remote_spec = ResourceSpec.from_dict(remote_spec_dict, where=f"remote {name!r} spec")
        default_spec = per_remote_spec.merge(default_spec or ResourceSpec())
    # The remote's max_parallel_jobs caps its arrays (S5); before, Slurm never saw it.
    if max_parallel_jobs and (default_spec is None or default_spec.array_throttle is None):
        default_spec = replace(default_spec or ResourceSpec(), array_throttle=max_parallel_jobs)

    conda_env = (
        conda_env_override
        if conda_env_override is not None
        else remote_cfg.get("conda_env", distributed_cfg.get("conda_env"))
    )
    python_path = remote_cfg.get("python_path", distributed_cfg.get("python_path", "python"))

    remote_root = remote_cfg.get("remote_root", distributed_cfg.get("remote_root", "~/.hsm/runs"))
    # Storage-tier awareness: workdir overrides remote_root for the active
    # run; archive_dir is the durable target on completion. Both opt-in.
    workdir = remote_cfg.get("workdir")
    archive_dir = remote_cfg.get("archive_dir")
    archive_on = str(remote_cfg.get("archive_on", "completed"))
    rsync_excludes = remote_cfg.get("rsync_excludes", distributed_cfg.get("rsync_excludes"))
    keep_remote_on_success = bool(
        remote_cfg.get(
            "keep_remote_on_success",
            distributed_cfg.get("keep_remote_on_success", False),
        )
    )

    qos_whitelist_raw = remote_cfg.get("qos_whitelist")
    qos_whitelist: frozenset[str] | None
    if qos_whitelist_raw and isinstance(qos_whitelist_raw, (list, tuple, set)):
        qos_whitelist = frozenset(str(q) for q in qos_whitelist_raw)
    else:
        qos_whitelist = None

    # GPU type → relative runtime multiplier for multi-gpu_type planning
    # (qos_whitelist pattern: per-remote key beside spec:, not inside it).
    speed_factors = normalize_speed_factors(
        remote_cfg.get("speed_factors"),
        warn_context=f"remote {name!r}: speed_factors",
    )

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
        max_parallel_jobs=max_parallel_jobs or 0,
        default_spec=default_spec,
        rsync_excludes=rsync_excludes,
        keep_remote_on_success=keep_remote_on_success,
        qos_whitelist=qos_whitelist,
        speed_factors=speed_factors,
    )
