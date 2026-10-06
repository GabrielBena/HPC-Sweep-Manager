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
import logging
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any

import asyncssh
import pytest

from hpc_sweep_manager.core.common.compute_source import JobInfo
from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
from hpc_sweep_manager.core.common.resumable import ResumableConfig, ResumableContext
from hpc_sweep_manager.core.remote import ssh_slurm_compute_source
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
        exit_status: int | None = None,
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
    responses to repeated commands (e.g. two sbatch submissions). An
    exception entry is raised (a dropped link). Falls back to
    ``_Result(returncode=0, stdout="")`` when no entry matches.
    """

    def __init__(
        self,
        responder: list[tuple[str, _Result]] | None = None,
        home: str = "/u/home/gbena",
        user: str | None = None,
    ):
        self.run_calls: list[dict[str, Any]] = []
        self.closed = False
        self._responder = responder or []
        # Used to simulate remote-shell expansion of `echo <path>` (the seam
        # _resolve_remote_path uses for ~ / $USER / $HOME). Unexplicit `echo`
        # commands get expanded the way a real shell would.
        self._home = home.rstrip("/")
        self._user = user or (self._home.split("/")[-1] or "gbena")

    def add(self, substring: str, result: _Result) -> None:
        self._responder.append((substring, result))

    def _expand_echo(self, cmd: str) -> str:
        arg = cmd[len("echo ") :].strip().strip('"').strip("'")
        if arg.startswith("~"):
            arg = self._home + arg[1:]
        arg = arg.replace("${HOME}", self._home).replace("$HOME", self._home)
        arg = arg.replace("${USER}", self._user).replace("$USER", self._user)
        return arg

    async def run(
        self,
        cmd: str,
        *,
        input: str | None = None,
        check: bool = False,
        timeout: float | None = None,
    ) -> _Result:
        self.run_calls.append({"cmd": cmd, "input": input, "check": check, "timeout": timeout})
        for i, (sub, res) in enumerate(self._responder):
            if sub in cmd:
                del self._responder[i]
                if isinstance(res, Exception):
                    raise res
                return res
        # Simulate the remote shell expanding `echo <path>` (~, $USER, $HOME).
        if cmd.startswith("echo "):
            return _Result(returncode=0, stdout=self._expand_echo(cmd) + "\n")
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
        self._rsync_calls: list[list[str]] = []
        self._rsync_rc = rsync_rc

    async def _open_connection(self):
        return self._fake_conn

    async def _run_rsync(self, cmd: list[str]) -> int:
        self._rsync_calls.append(cmd)
        return self._rsync_rc


def _setup_ok_responder(home: str = "/u/home/gbena") -> list[tuple[str, _Result]]:
    """Default responder for a successful setup() lifecycle.

    Path expansion (`echo <path>`) is handled generically by FakeConn now —
    set the home/user there. ``home`` is accepted for call-site compatibility
    but no longer drives an explicit ``echo $HOME`` response.
    """
    return [
        ("command -v sbatch", _Result(0, stdout="/usr/bin/sbatch\n")),
        ("mkdir -p", _Result(0)),
    ]


# ---------------------------------------------------------------------- setup


class TestSetup:
    @pytest.mark.asyncio
    async def test_happy_path(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder(), home="/home/gbena")
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
        assert src._remote_code_dir == f"/home/gbena/.hsm/runs/{project_name}/snapshots/sweep_x"
        assert src._remote_sweep_dir == f"/home/gbena/.hsm/runs/{project_name}/sweeps/sweep_x"
        # mkdir issued once with all four dirs.
        mkdir_calls = [c for c in conn.run_calls if "mkdir -p" in c["cmd"]]
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
    @pytest.mark.parametrize("rc", [127, None])  # None: unknown, never a success
    async def test_fails_when_sbatch_not_on_remote(self, tmp_path, rc):
        conn = FakeConn(responder=[("command -v sbatch", _Result(rc, stderr="not found"))])
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
        jid = await src.submit_job(params={"seed": 0}, job_name="task_0", sweep_id="sweep_1")
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
        # A confirmed write, then sbatch with no time bound: sbatch never waits on stdin (#38).
        script = f"{src._remote_scripts_dir}/task_0.slurm"
        assert cat_calls[0]["cmd"] == f"cat > {script}"
        sbatch = [(c["cmd"], c["timeout"]) for c in conn.run_calls if c["cmd"].startswith("sbatch")]
        assert sbatch == [(f"sbatch {script}", None)]

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
        with pytest.raises(RuntimeError, match="failed on uzh: error: invalid partition"):
            await src.submit_job(params={"seed": 0}, job_name="task_0", sweep_id="sweep_1")

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
            c
            for c in conn.run_calls
            if c["cmd"].startswith("cat > ") and "parameter_combinations" in c["cmd"]
        ]
        assert len(cat_params) == 1
        loaded = json.loads(cat_params[0]["input"])
        assert len(loaded) == 3
        assert loaded[0] == {"index": 1, "global_index": 1, "params": {"seed": 0}}

    @pytest.mark.asyncio
    async def test_submit_array_multi_gpu_type_splits(self, tmp_path):
        """Issue #7: gpu_type tuple → one sub-array per type, partitioned
        params files (array-local index, original global_index), per-type
        walltimes, and a manifest with per-job detail."""
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 111\n"))
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 222\n"))
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="train.py",
            default_spec=ResourceSpec(walltime="10:00:00", gpus=1, gpu_type=("A100", "H200")),
            speed_factors={"a100": 1.0, "h200": 0.5},
            fake_conn=conn,
        )
        sweep_dir = tmp_path / "sweeps" / "outputs" / "sweep_1"
        await src.setup(sweep_dir, "sweep_1")
        ids = await src.submit_batch(
            params_list=[{"seed": i} for i in range(4)],
            sweep_id="sweep_1",
            mode="array",
            job_name_prefix="sweep_1",
        )
        assert ids == ["111", "222"]

        # Two params files, one per type, with array-local `index` and
        # original `global_index` (union covers every task exactly once).
        cat_params = [
            c
            for c in conn.run_calls
            if c["cmd"].startswith("cat > ") and "parameter_combinations_" in c["cmd"]
        ]
        assert len(cat_params) == 2
        all_globals = []
        for c in cat_params:
            entries = json.loads(c["input"])
            assert [e["index"] for e in entries] == list(range(1, len(entries) + 1))
            all_globals.extend(e["global_index"] for e in entries)
        assert sorted(all_globals) == [1, 2, 3, 4]

        # Rendered scripts carry per-type --gres and SCALED walltime.
        cat_scripts = [
            c
            for c in conn.run_calls
            if c["cmd"].startswith("cat > ") and c["cmd"].rstrip("'\"").endswith(".slurm")
        ]
        assert len(cat_scripts) == 2
        rendered = "\n".join(c["input"] for c in cat_scripts)
        assert "--gres=gpu:A100:1" in rendered
        assert "--gres=gpu:H200:1" in rendered
        assert "#SBATCH --time=10:00:00" in rendered  # a100 (factor 1.0)
        assert "#SBATCH --time=05:00:00" in rendered  # h200 (factor 0.5)
        assert 'echo "GPU Type: A100"' in rendered
        assert 'echo "GPU Type: H200"' in rendered

        # JobInfo per sub-array records its type + size.
        types = {src.active_jobs[j].params.get("_gpu_type") for j in ids}
        assert types == {"A100", "H200"}

        # Manifest gains per-job detail (and keeps the legacy fields).
        manifest = json.loads((sweep_dir / ".hsm_manifest.json").read_text())
        assert manifest["job_ids"] == ["111", "222"]
        assert manifest["num_tasks"] == 4
        jobs = {j["job_id"]: j for j in manifest["jobs"]}
        assert jobs["111"]["gpu_type"] in ("A100", "H200")
        assert sum(j["num_tasks"] for j in manifest["jobs"]) == 4

    @pytest.mark.asyncio
    async def test_partial_submission_failure_still_writes_manifest(self, tmp_path):
        """Review finding: sub-array 1 live + sub-array 2's sbatch failing
        used to leave NO manifest → `hsm sweep collect` impossible, orphaned
        jobs invisible. The error path must persist what DID submit."""
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 111\n"))
        conn.add("sbatch", _Result(1, "", "sbatch: error: budget exceeded"))
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="train.py",
            default_spec=ResourceSpec(walltime="10:00:00", gpus=1, gpu_type=("A100", "H200")),
            speed_factors={"a100": 1.0, "h200": 0.5},
            fake_conn=conn,
        )
        sweep_dir = tmp_path / "sweeps" / "outputs" / "sweep_1"
        await src.setup(sweep_dir, "sweep_1")
        with pytest.raises(RuntimeError, match="sbatch"):
            await src.submit_batch(
                params_list=[{"seed": i} for i in range(4)],
                sweep_id="sweep_1",
                mode="array",
                job_name_prefix="sweep_1",
            )
        # The recovery anchor exists and names the LIVE sub-array.
        manifest = json.loads((sweep_dir / ".hsm_manifest.json").read_text())
        assert manifest["job_ids"] == ["111"]
        assert manifest["jobs"][0]["job_id"] == "111"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("interrupt", [None, asyncio.CancelledError, KeyboardInterrupt])
    async def test_individual_submission_stopped_partway_writes_manifest(self, tmp_path, interrupt):
        """Tracker S3: a loop of individual sbatch calls that fails, or is Ctrl-C'd, partway
        used to leave its live jobs untracked (the manifest came only after the loop)."""
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 111\n"))
        conn.add("sbatch", _Result(1, "", "sbatch: error: QOSMaxSubmitJobPerUserLimit"))
        src = _StubSrc(name="uzh", host="uzh", project_dir=str(tmp_path), fake_conn=conn)
        sweep_dir = tmp_path / "sweeps" / "outputs" / "sweep_1"
        await src.setup(sweep_dir, "sweep_1")
        if interrupt is not None:
            real_sbatch = src._sbatch

            async def sbatch_then_interrupt(path, script):
                if src.active_jobs:
                    raise interrupt()
                return await real_sbatch(path, script)

            src._sbatch = sbatch_then_interrupt
        with pytest.raises(interrupt or RuntimeError):
            await src.submit_batch([{"seed": i} for i in range(3)], "sweep_1", mode="individual")
        manifest = json.loads((sweep_dir / ".hsm_manifest.json").read_text())
        assert manifest["job_ids"] == ["111"]

    @pytest.mark.asyncio
    async def test_a_partial_chunk_keeps_the_chain_manifest(self, tmp_path):
        """Review of #26: a partial submission inside a resumable chain must not overwrite the
        driver's chain manifest (advance would stop recognising the chain; collect would then
        archive and clean it)."""
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 111\n"))
        conn.add("sbatch", _Result(1, "", "sbatch: error: QOSMaxSubmitJobPerUserLimit"))
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            default_spec=ResourceSpec(walltime="10:00:00", gpus=1, gpu_type=("A100", "H200")),
            speed_factors={"a100": 1.0, "h200": 0.5},
            fake_conn=conn,
        )
        sweep_dir = tmp_path / "sweeps" / "outputs" / "sweep_1"
        await src.setup(sweep_dir, "sweep_1")
        chain_manifest = '{"resumable": {"enabled": true}, "chain": {}, "job_ids": ["99"]}'
        (sweep_dir / ".hsm_manifest.json").write_text(chain_manifest)
        ctx = ResumableContext(
            chunk_index=1, config=ResumableConfig(enabled=True, chunk_walltime="04:00:00")
        )
        with pytest.raises(RuntimeError, match="QOSMaxSubmitJobPerUserLimit"):
            await src.submit_batch(
                [{"seed": i} for i in range(4)], "sweep_1", mode="array", resumable=ctx
            )
        assert (sweep_dir / ".hsm_manifest.json").read_text() == chain_manifest

    @pytest.mark.asyncio
    async def test_an_array_rejection_suggests_individual_mode(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(1, "", "sbatch: error: Invalid job array specification"))
        src = _StubSrc(name="uzh", host="uzh", project_dir=str(tmp_path), fake_conn=conn)
        await src.setup(tmp_path / "sw", "sw")
        with pytest.raises(RuntimeError, match="try --mode individual"):
            await src.submit_batch([{"seed": 0}], "sw", mode="array")

    @pytest.mark.asyncio
    async def test_multi_gpu_type_individual_mode_rejected(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="train.py",
            default_spec=ResourceSpec(gpus=1, gpu_type=("A100", "H200")),
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        with pytest.raises(ValueError, match="array mode"):
            await src.submit_batch(params_list=[{"seed": 0}], sweep_id="sweep_1", mode="individual")

    @pytest.mark.asyncio
    async def test_submit_array_rejects_empty(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="t.py",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        with pytest.raises(ValueError, match="empty array"):
            await src.submit_batch(
                params_list=[],
                sweep_id="sweep_1",
                mode="array",
            )


# --------------------------------------------------------------------- status


class TestArrayThrottle:
    """Tracker S5: a slurm remote's max_parallel_jobs was a client-side count Slurm never saw."""

    def test_max_parallel_jobs_becomes_the_array_throttle(self, tmp_path):
        src = build_ssh_slurm_source(
            name="uzh",
            remote_cfg={"max_parallel_jobs": 350},
            project_dir=str(tmp_path),
            script_path="t.py",
        )
        assert src.default_spec.array_throttle == 350

    @pytest.mark.parametrize("value", ["350", True, 0.5])
    def test_a_bad_max_parallel_jobs_is_named(self, tmp_path, value):
        with pytest.raises(ValueError, match="max_parallel_jobs must be a whole number"):
            build_ssh_slurm_source(
                name="uzh",
                remote_cfg={"max_parallel_jobs": value},
                project_dir=str(tmp_path),
                script_path="t.py",
            )

    @pytest.mark.parametrize("value", ["350", -1, True])
    def test_a_bad_max_parallel_jobs_is_named_beside_a_throttle(self, tmp_path, value):
        cfg = {"max_parallel_jobs": value, "spec": {"array_throttle": 50}}
        with pytest.raises(ValueError, match="max_parallel_jobs must be a whole number"):
            build_ssh_slurm_source(
                name="uzh", remote_cfg=cfg, project_dir=str(tmp_path), script_path="t.py"
            )

    def test_zero_max_parallel_jobs_still_means_no_cap(self, tmp_path):
        cfg = {"max_parallel_jobs": 0}
        src = build_ssh_slurm_source(
            name="uzh", remote_cfg=cfg, project_dir=str(tmp_path), script_path="t.py"
        )
        assert src.default_spec.array_throttle is None and src.max_parallel_jobs == 10_000

    def test_an_explicit_throttle_wins(self, tmp_path):
        cfg = {"max_parallel_jobs": 350, "spec": {"array_throttle": 50}}
        src = build_ssh_slurm_source(
            name="uzh", remote_cfg=cfg, project_dir=str(tmp_path), script_path="t.py"
        )
        assert src.default_spec.array_throttle == 50


class TestStatus:
    """The SSH seam of SlurmBase's refresh (the semantics: test_slurm_base.py)."""

    @staticmethod
    async def _tracking(tmp_path, conn, *jobs, replies=()):
        """Set up a source tracking ``jobs``; ``replies`` are scripted after setup, so no setup
        command (whose paths contain the test's name) can consume them."""
        src = _StubSrc(
            name="uzh", host="uzh", project_dir=str(tmp_path), script_path="t.py", fake_conn=conn
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        for reply in replies:
            conn.add(*reply)
        for job in jobs:
            src.active_jobs[job] = JobInfo(job, job, {}, src.name)
        return src

    @pytest.mark.asyncio
    async def test_one_squeue_and_one_sacct_on_the_wire(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        replies = [
            ("squeue -u", _Result(0, stdout="100_[3-9] PENDING\n100_2 RUNNING\n")),
            ("sacct", _Result(0, stdout="101|FAILED\n102|COMPLETED\n")),
        ]
        src = await self._tracking(tmp_path, conn, "100", "101", "102", replies=replies)
        await src.update_all_job_statuses()
        wire = [c["cmd"] for c in conn.run_calls if c["cmd"].startswith(("squeue", "sacct"))]
        assert wire == ["squeue -u gbena -h -o '%i %T'", "sacct -j 101,102 -n -X -P -o JobID,State"]
        assert src.active_jobs["100"].status == "RUNNING"
        assert src.completed_jobs["101"].status == "FAILED"
        assert src.completed_jobs["102"].status == "COMPLETED"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("failing", ["squeue -u", "sacct"])
    async def test_an_outage_never_ends_the_wait(self, tmp_path, failing):
        # Tracker S1: squeue (slurmctld) or sacct (slurmdbd) failing read as COMPLETED, so the
        # launcher went on to collect, archive and rm -rf a live sweep dir.
        conn = FakeConn(responder=_setup_ok_responder())
        outage = _Result(1, stderr="Unable to contact slurm controller/database")
        src = await self._tracking(tmp_path, conn, "777", replies=[(failing, outage)] * 200)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(src.wait_for_all(poll_interval=0.001), timeout=0.2)
        assert "777" in src.active_jobs

    @pytest.mark.asyncio
    async def test_a_signal_killed_command_is_a_failure(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        src = await self._tracking(tmp_path, conn, replies=[("squeue -u", _Result(None))])
        assert (await src._sh(["squeue", "-u", "gbena"]))[0] == 255

    def test_slurm_sources_poll_every_minute_the_others_every_10_s(self):
        # Tracker R9: a 10 s poll for jobs that run 47 h.
        from hpc_sweep_manager.core.hpc.slurm_compute_source import SlurmComputeSource
        from hpc_sweep_manager.core.local.local_compute_source import LocalComputeSource
        from hpc_sweep_manager.core.remote.ssh_compute_source import SSHComputeSource

        assert SSHSlurmComputeSource.poll_interval == SlurmComputeSource.poll_interval == 60
        assert LocalComputeSource.poll_interval == SSHComputeSource.poll_interval == 10


class _DeadConn(FakeConn):
    async def run(self, cmd: str, **kw) -> _Result:
        raise asyncssh.ConnectionLost("link down")


class TestReconnect:
    """Tracker S11: a login-node blip used to end a multi-day launcher."""

    @pytest.mark.asyncio
    async def test_a_dropped_link_is_reopened_once_and_changes_no_status(self, tmp_path, caplog):
        conn = FakeConn(responder=_setup_ok_responder())
        blip = ("squeue -u", asyncssh.ConnectionLost("blip"))
        src = await TestStatus._tracking(tmp_path, conn, "777", replies=[blip])
        src.active_jobs["777"].status = "RUNNING"
        src._fake_conn = fresh = FakeConn(responder=[("squeue -u", _Result(0, "777 RUNNING\n"))])
        with caplog.at_level(logging.WARNING):
            await src.update_all_job_statuses()
        assert src._conn is fresh and conn.closed and src.active_jobs["777"].status == "RUNNING"
        assert [r.message for r in caplog.records if r.levelno >= logging.WARNING] == [
            "uzh: connection lost (ConnectionLost('blip')); reconnecting"
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("reopen", ["dead", "unreachable"])
    async def test_a_link_that_stays_down_keeps_every_state(self, tmp_path, reopen):
        conn = FakeConn(responder=_setup_ok_responder())
        src = await TestStatus._tracking(tmp_path, conn, "777")
        src._conn = src._fake_conn = _DeadConn()
        if reopen == "unreachable":

            async def unreachable():
                raise ConnectionError("Could not reach uzh within 60 s")

            src._open_connection = unreachable
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(src.wait_for_all(poll_interval=0.001), timeout=0.2)
        assert src.active_jobs["777"].status == "PENDING" and not src.completed_jobs

    @pytest.mark.asyncio
    async def test_a_link_down_for_good_ends_the_wait_with_the_re_attach_command(
        self, tmp_path, monkeypatch
    ):
        # An endless wait would block a distributed sweep's other children.
        from hpc_sweep_manager.core.remote import ssh_slurm_compute_source as mod

        monkeypatch.setattr(mod, "LINK_GIVE_UP_S", 0)
        conn = FakeConn(responder=_setup_ok_responder())
        src = await TestStatus._tracking(tmp_path, conn, "777")
        src._conn = src._fake_conn = _DeadConn()
        with pytest.raises(ConnectionError, match="hsm sweep collect"):
            for _ in range(3):
                await src.update_all_job_statuses()
        assert src.active_jobs["777"].status == "PENDING"  # still no verdict

    @pytest.mark.asyncio
    async def test_a_reply_without_exit_status_is_unknown(self, tmp_path):
        # asyncssh reports a link that died mid-command as returncode None, not as an error.
        conn = FakeConn(responder=_setup_ok_responder())
        src = await TestStatus._tracking(tmp_path, conn, replies=[("squeue", _Result(None))])
        src._down_since = 1.0
        assert (await src._sh(["squeue"]))[0] == 255 and src._down_since == 1.0
        assert (await src._sh(["squeue"]))[0] == 0 and src._down_since is None
        assert {c["timeout"] for c in conn.run_calls} == {300}  # a hung command is bounded

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "reply",
        [
            asyncssh.ConnectionLost("blip"),
            asyncssh.ChannelOpenError(asyncssh.OPEN_REQUEST_SESSION_FAILED, "exec reply lost"),
            _Result(None),
        ],
        ids=["lost", "exec-unanswered", "no-exit-status"],
    )
    @pytest.mark.parametrize(
        "rc, queued, found",
        [
            (0, "", None),
            # 444 is the chain's finished chunk (same name and script); 556 another sweep's job.
            (
                0,
                "444 COMPLETED {s}\n555_[2-3] PENDING {s}\n555_1 RUNNING {s}\n556 PENDING /x",
                "555",
            ),
            (0, "555 PENDING {s}\n556 RUNNING {s}\n", None),
            (1, "555 PENDING {s}\n", None),
        ],
        ids=["not-queued", "queued", "ambiguous", "squeue-failed"],
    )
    async def test_a_lost_sbatch_reply_is_looked_up_never_resent(
        self, tmp_path, reply, rc, queued, found
    ):
        # A second sbatch could queue the job twice.
        conn = FakeConn(responder=_setup_ok_responder())
        src = await TestStatus._tracking(tmp_path, conn, replies=[("sbatch /", reply)])
        script = f"{src._remote_scripts_dir}/sweep_1_array.slurm"
        lookup = "squeue -h -u gbena -n sweep_1_array -o '%i %T %o'"
        conn.add(lookup, _Result(rc, queued.format(s=script)))
        with nullcontext() if found else pytest.raises(RuntimeError, match="scancel -n sweep_1"):
            assert await src.submit_batch([{"s": 0}], "sweep_1", mode="array") == [found]
        assert sum(c["cmd"].startswith("sbatch") for c in conn.run_calls) == 1
        assert any(c["cmd"] == lookup for c in conn.run_calls)

    @pytest.mark.asyncio
    async def test_an_sbatch_that_never_started_is_sent_again(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        unsent = asyncssh.ChannelOpenError(asyncssh.OPEN_CONNECT_FAILED, "SSH connection closed")
        ok = _Result(0, stdout="Submitted batch job 7\n")
        replies = [("sbatch /", unsent), ("sbatch /", ok)]
        src = await TestStatus._tracking(tmp_path, conn, replies=replies)
        assert await src.submit_batch([{"s": 0}], "sweep_1", mode="array") == ["7"]
        assert sum(c["cmd"].startswith("sbatch") for c in conn.run_calls) == 2

    @pytest.mark.asyncio
    async def test_no_exit_status_is_never_a_success(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        replies = [("scancel", _Result(None)), ("echo $USER", _Result(None))]
        src = await TestStatus._tracking(tmp_path, conn, "777", replies=replies)
        assert await src.cancel_job("777") is False and "777" in src.active_jobs
        with pytest.raises(RuntimeError, match="could not expand"):  # a literal $USER: no squeue
            await src._resolve_remote_path("$USER")
        conn.add("find", _Result(None))  # an empty probe would count as a chunk without progress
        assert await src.chunk_progress(1, done_sentinel=".d", checkpoint_subdir="r") is None
        conn.add("sinfo -h", _Result(None))
        assert (await src.health_check())["connection"] == "ok_but_no_sinfo"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("rc", [1, None])
    async def test_a_failed_write_raises_naming_the_path(self, tmp_path, rc):
        conn = FakeConn(responder=_setup_ok_responder())
        full = ("cat > /x/f.json", _Result(rc, stderr="No space left on device"))
        src = await TestStatus._tracking(tmp_path, conn, replies=[full])
        with pytest.raises(RuntimeError, match="writing /x/f.json on uzh failed: No space"):
            await src._write_remote_file("/x/f.json", "{}")


class TestReservationWarning:
    """Tracker S7: warn when a maintenance window starts before a job of this walltime ends."""

    MAINT = (
        "ReservationName=maint StartTime=2026-10-07T06:00:00 EndTime=2026-10-07T18:00:00 "
        "Duration=12:00:00 Nodes=ALL NodeCnt=400 Flags=MAINT,SPEC_NODES\n"
    )

    async def _submit(self, tmp_path, caplog, now, walltime):
        import logging

        conn = FakeConn(responder=_setup_ok_responder())
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="t.py",
            fake_conn=conn,
            default_spec=ResourceSpec(walltime=walltime),
        )
        await src.setup(tmp_path / "sweep", "sw1")
        conn.add("scontrol show reservations", _Result(0, stdout=f"{now}\n{self.MAINT}"))
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 1\n"))
        with caplog.at_level(logging.WARNING):
            await src.submit_batch([{"s": 0}], "sw1", mode="array")
        return [r.message for r in caplog.records if "Reservation" in r.message]

    @pytest.mark.asyncio
    async def test_a_walltime_crossing_maintenance_is_warned(self, tmp_path, caplog):
        msgs = await self._submit(tmp_path, caplog, "2026-10-06T10:00:00", "47:30:00")
        assert len(msgs) == 1
        assert "won't start before 2026-10-07T18:00:00" in msgs[0]
        assert "a walltime ≤ 20:00:00 would start now" in msgs[0]

    @pytest.mark.asyncio
    async def test_a_job_ending_before_maintenance_is_not_warned(self, tmp_path, caplog):
        assert not await self._submit(tmp_path, caplog, "2026-10-06T10:00:00", "04:00:00")


class TestManifest:
    @pytest.mark.asyncio
    async def test_submit_batch_writes_local_and_remote_manifest(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder(), home="/u/home/gbena", user="gbena")
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 5\n"))
        sweep_dir = tmp_path / "sweeps" / "outputs" / "sw1"
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="t.py",
            workdir="/scratch/$USER/hsm-runs",
            archive_dir="/shares/$USER/arch",
            fake_conn=conn,
        )
        await src.setup(sweep_dir, "sw1")
        await src.submit_batch([{"s": 0}], "sw1", mode="individual", job_name_prefix="sw1")
        # Local manifest written with the re-attach fields.
        local = sweep_dir / ".hsm_manifest.json"
        assert local.exists()
        m = json.loads(local.read_text())
        assert m["sweep_id"] == "sw1"
        assert m["backend"] == "slurm"
        assert m["host"] == "uzh"
        assert m["job_ids"] == ["5"]
        assert "/scratch/gbena/hsm-runs" in m["remote_sweep_dir"]
        assert m["resolved_archive_dir"] == "/shares/gbena/arch"
        assert m["remote_code_dir"].endswith("/snapshots/sw1")  # this sweep's code (S4)
        # Remote manifest cat-piped too.
        remote_manifest = [
            c
            for c in conn.run_calls
            if c["cmd"].startswith("cat > ") and ".hsm_manifest.json" in c["cmd"]
        ]
        assert len(remote_manifest) == 1

    def test_from_manifest_reconstructs_source(self, tmp_path):
        m = {
            "name": "uzh",
            "host": "uzh",
            "ssh_key": "/k",
            "ssh_port": 2222,
            "conda_env": "cpvr",
            "project_dir": str(tmp_path),
            "remote_root": "~/.hsm/runs",
            "workdir": "/scratch/gbena/hsm-runs",
            "archive_dir": "/shares/gbena/arch",
            "archive_on": "always",
            "keep_remote_on_success": True,
        }
        src = SSHSlurmComputeSource.from_manifest(m)
        assert src.host == "uzh"
        assert src.conda_env == "cpvr"
        assert src.workdir == "/scratch/gbena/hsm-runs"
        assert src.archive_on == "always"
        assert src.keep_remote_on_success is True

    @pytest.mark.asyncio
    async def test_reattach_sets_paths_without_pushing(self, tmp_path):
        conn = FakeConn(responder=[])
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="",
            fake_conn=conn,
        )
        m = {
            "remote_sweep_dir": "/scratch/gbena/hsm-runs/proj/sweeps/sw1",
            "remote_tasks_dir": "/scratch/gbena/hsm-runs/proj/sweeps/sw1/tasks",
            "resolved_archive_dir": "/shares/gbena/arch",
        }
        ok = await src.reattach(tmp_path / "sweeps" / "outputs" / "sw1", "sw1", m)
        assert ok is True
        assert src._remote_sweep_dir == m["remote_sweep_dir"]
        assert src._resolved_archive_dir == "/shares/gbena/arch"
        # reattach must NOT rsync-push the code mirror.
        assert src._rsync_calls == []


class TestPeriodicPull:
    """R7: while jobs are live, ``tasks/`` is pulled every 10 min. An array is one job, so the
    old pull per finished job came only at its end; the final pull is collect_results'."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("outcome", "takes", "times"),
        [
            (0, 0, [600, 1200]),
            (23, 0, [600, 1200]),
            (OSError("link down"), 0, [600, 1200]),
            (0, 700, [600, 1900]),  # a pull longer than the interval: next one 10 min after it
        ],
    )
    async def test_every_ten_minutes_while_jobs_run(
        self, tmp_path, monkeypatch, caplog, outcome, takes, times
    ):
        clock = [0.0]  # a fake time.monotonic, a minute on at each poll
        monkeypatch.setattr(
            ssh_slurm_compute_source, "time", SimpleNamespace(monotonic=lambda: clock[0])
        )
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 1\n"))
        src = _StubSrc(
            name="uzh", host="uzh", project_dir=str(tmp_path), script_path="t.py", fake_conn=conn
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        await src.submit_job({"s": 0}, "task_0", "sweep_1")
        for _ in range(25):  # the array runs 25 polls, then is gone and done
            conn.add("squeue -u", _Result(0, stdout="1_[3-9] PENDING\n1_2 RUNNING\n"))
        conn.add("squeue -u", _Result(0, stdout=""))
        conn.add("sacct", _Result(0, stdout="1|COMPLETED\n"))
        pulls = []

        async def refresh():
            clock[0] += 60
            await SSHSlurmComputeSource.update_all_job_statuses(src)

        async def rsync(cmd):
            pulls.append((clock[0], cmd))
            clock[0] += takes
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        src.update_all_job_statuses, src._run_rsync = refresh, rsync
        assert await src.wait_for_all(poll_interval=0) == {"1": "COMPLETED"}
        # Every 10 min, never more often (a failed pull too), none once the job is done.
        assert [t for t, _ in pulls] == times
        assert all("--exclude=*.pt" in cmd and cmd[-2].endswith("/tasks/") for _, cmd in pulls)
        failed = caplog.text.count("periodic tasks/ pull from uzh failed")
        assert failed == (2 if outcome else 0)  # a failure is a warning; the wait went on


# --------------------------------------------------------------------- cancel


class TestCancel:
    @pytest.mark.asyncio
    async def test_cancel_marks_cancelled(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 42\n"))
        conn.add("scancel", _Result(0))
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="t.py",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        jid = await src.submit_job({"s": 0}, "task_0", "sweep_1")
        assert await src.cancel_job(jid) is True
        assert src.completed_jobs[jid].status == "CANCELLED"


# ---------------------------------------------------------------- collect_results


class TestCollectResults:
    @pytest.mark.asyncio
    async def test_rsync_skips_an_agent_that_stalled(self, tmp_path, monkeypatch):
        from hpc_sweep_manager.core.remote import discovery

        monkeypatch.setattr(discovery, "_AGENT_STALLED", {"uzh"})
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 7\n"))
        conn.add("rm -rf", _Result(0))
        src = _StubSrc(
            name="uzh", host="uzh", project_dir=str(tmp_path), script_path="t.py", fake_conn=conn
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        src.update_job_status(await src.submit_job({"s": 0}, "task_0", "sweep_1"), "COMPLETED")
        await src.collect_results()
        push, pull = src._rsync_calls
        assert all("-o IdentityAgent=none" in cmd[cmd.index("-e") + 1] for cmd in (push, pull))

    @pytest.mark.asyncio
    async def test_pull_then_cleanup_on_success(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 7\n"))
        conn.add("rm -rf", _Result(0))
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="t.py",
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
        # Remote dir was cleaned, with the sweep's own code snapshot (S4).
        rm_calls = [c for c in conn.run_calls if c["cmd"].startswith("rm -rf")]
        assert len(rm_calls) == 1
        assert src._remote_sweep_dir in rm_calls[0]["cmd"]
        assert rm_calls[0]["cmd"].endswith("/snapshots/sweep_1")
        assert rm_calls[0]["timeout"] is None  # a big tree may take longer than the 300 s bound

    @pytest.mark.asyncio
    async def test_task_states_land_in_the_local_sweep_dir(self, tmp_path):
        """S8: one sacct at collect writes tasks_state.json into the LOCAL sweep dir, never the
        remote one the cleanup deletes, and before the archive -> pull -> rm -rf sequence."""
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 7\n"))
        rows = "7_1|COMPLETED|0:0|n1|50\n7_2|TIMEOUT|0:15|n2|86400\n"
        conn.add("sacct -j 7 -P", _Result(0, stdout=rows))
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="t.py",
            archive_dir="/shares/a",
            archive_on="always",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        (jid,) = await src.submit_batch([{"s": 0}, {"s": 1}], "sweep_1", mode="array")
        src.update_job_status(jid, "FAILED")
        assert await src.collect_results() is True
        states = json.loads((tmp_path / "sweep" / "tasks_state.json").read_text())
        assert {t: s["state"] for t, s in states.items()} == {
            "task_1": "COMPLETED",
            "task_2": "TIMEOUT",
        }
        cmds = [c["cmd"] for c in conn.run_calls]
        assert not any("tasks_state" in c for c in cmds)
        sacct = next(i for i, c in enumerate(cmds) if c.startswith("sacct -j 7 -P"))
        assert sacct < next(i for i, c in enumerate(cmds) if "rsync -a" in c)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["FAILED", "CANCELLED"])  # `hsm sweep cancel`, then collect
    async def test_no_cleanup_on_failure(self, tmp_path, status):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 7\n"))
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="t.py",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        jid = await src.submit_job({"s": 0}, "task_0", "sweep_1")
        src.update_job_status(jid, status)
        ok = await src.collect_results()
        assert ok is True
        rm_calls = [c for c in conn.run_calls if c["cmd"].startswith("rm -rf")]
        assert rm_calls == []

    @pytest.mark.asyncio
    async def test_keep_remote_overrides_cleanup(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 7\n"))
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="t.py",
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


class TestLegacyCodeDir:
    @pytest.mark.asyncio
    async def test_a_reattached_old_manifest_never_cleans_the_shared_code_dir(self, tmp_path):
        # An old chain's manifest names the shared .../code dir: tasks of other sweeps use it.
        conn = FakeConn()
        src = _StubSrc(name="uzh", host="uzh", project_dir=str(tmp_path), fake_conn=conn)
        manifest = {"remote_sweep_dir": "/r/proj/sweeps/sw1", "remote_code_dir": "/r/proj/code"}
        await src.reattach(tmp_path / "sw1", "sw1", manifest)
        await src.collect_results()
        rm_calls = [c["cmd"] for c in conn.run_calls if c["cmd"].startswith("rm -rf")]
        assert rm_calls == ["rm -rf /r/proj/sweeps/sw1"]


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
        assert src._remote_sweep_dir == f"/scratch/gbena/hsm-runs/{project_name}/sweeps/sweep_w"

    @pytest.mark.asyncio
    async def test_workdir_user_var_expanded(self, tmp_path):
        # #1 fix: $USER in workdir must expand on the remote, not land as a
        # literal "$USER" directory in the rsync destination.
        conn = FakeConn(responder=_setup_ok_responder(), home="/u/home/gbena", user="gbena")
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="train.py",
            workdir="/scratch/$USER/hsm-runs",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_w")
        project_name = tmp_path.name
        assert src._remote_sweep_dir == f"/scratch/gbena/hsm-runs/{project_name}/sweeps/sweep_w"
        # The rsync push destination is the expanded path (no literal $USER).
        push = src._rsync_calls[0]
        assert any("/scratch/gbena/hsm-runs" in a for a in push)
        assert not any("$USER" in a for a in push)

    @pytest.mark.asyncio
    async def test_archive_dir_user_var_expanded(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder(), home="/u/home/gbena", user="gbena")
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 1\n"))
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="t.py",
            workdir="/scratch/$USER/hsm-runs",
            archive_dir="/shares/$USER/hsm-archive",
            archive_on="always",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sw1")
        assert src._resolved_archive_dir == "/shares/gbena/hsm-archive"
        jid = await src.submit_job({"s": 0}, "task_0", "sw1")
        src.update_job_status(jid, "COMPLETED")
        await src.collect_results()
        # The archive command targets the expanded path, never literal $USER.
        arch = [
            c
            for c in conn.run_calls
            if "rsync -a" in c["cmd"] and "/shares/gbena/hsm-archive/sw1" in c["cmd"]
        ]
        assert len(arch) == 1
        assert not any("$USER" in c["cmd"] for c in conn.run_calls if "rsync -a" in c["cmd"])

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
            src._remote_sweep_dir == f"/u/home/gbena/scratch/hsm-runs/{project_name}/sweeps/sweep_w"
        )

    @pytest.mark.asyncio
    async def test_archive_on_completed_runs_when_clean(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 1\n"))
        # The archive cmd issues "mkdir -p <archive>/<id> && rsync ..." —
        # match the leading "mkdir -p" + the "rsync" parts of it.
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="t.py",
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
            c
            for c in conn.run_calls
            if "rsync -a" in c["cmd"] and "/shares/payvand/hsm-archive/sw1" in c["cmd"]
        ]
        assert len(arch_calls) == 1
        assert "/snapshots/sw1/ /shares/payvand/hsm-archive/sw1/code/" in arch_calls[0]["cmd"]
        # Sentinel was written.
        sentinel_calls = [
            c for c in conn.run_calls if c["cmd"].startswith("cat > ") and ".archived" in c["cmd"]
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
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="t.py",
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
            c for c in conn.run_calls if "rsync -a" in c["cmd"] and "/shares/payvand" in c["cmd"]
        ]
        assert arch_calls == []

    @pytest.mark.asyncio
    async def test_archive_on_always_runs_even_on_failure(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 1\n"))
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="t.py",
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
            c for c in conn.run_calls if "rsync -a" in c["cmd"] and "/shares/payvand" in c["cmd"]
        ]
        assert len(arch_calls) == 1
        # Sentinel records the failure.
        sentinel = [
            c for c in conn.run_calls if c["cmd"].startswith("cat > ") and ".archived" in c["cmd"]
        ][0]
        assert "any_failed: True" in sentinel["input"]

    @pytest.mark.asyncio
    async def test_archive_on_never_disables_archive(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 1\n"))
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="t.py",
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
            c for c in conn.run_calls if "rsync -a" in c["cmd"] and "/shares/payvand" in c["cmd"]
        ]
        assert arch_calls == []

    @pytest.mark.asyncio
    async def test_no_archive_dir_means_no_archive(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 1\n"))
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="t.py",
            workdir="/scratch/gbena/hsm-runs",
            # archive_dir omitted
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sw1")
        jid = await src.submit_job({"s": 0}, "task_0", "sw1")
        src.update_job_status(jid, "COMPLETED")
        await src.collect_results()
        arch_calls = [
            c for c in conn.run_calls if c["cmd"].startswith("mkdir -p") and "rsync -a" in c["cmd"]
        ]
        assert arch_calls == []

    @pytest.mark.asyncio
    async def test_an_archive_cut_short_keeps_the_remote_dir(self, tmp_path):
        # Tracker S11 review: rc None (the link died during the rsync) read as success, so the
        # launcher pulled and rm -rf'd the scratch copy behind a partial archive.
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 1\n"))
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="t.py",
            workdir="/scratch/gbena/hsm-runs",
            archive_dir="/shares/payvand/hsm-archive",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sw1")
        src.update_job_status(await src.submit_job({"s": 0}, "task_0", "sw1"), "COMPLETED")
        conn.add("rsync -a", _Result(None))
        assert await src.collect_results() is False
        assert len(src._rsync_calls) == 1  # the setup push only: no pull
        assert not any(
            c["cmd"].startswith("rm -rf") or ".archived" in c["cmd"] for c in conn.run_calls
        )
        assert [c["timeout"] for c in conn.run_calls if "rsync -a" in c["cmd"]] == [None]

    @pytest.mark.asyncio
    async def test_archive_runs_before_pull(self, tmp_path):
        """Archive happens server-side first; THEN we pull tasks/ back."""
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 1\n"))
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="t.py",
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
                name="x",
                host="x",
                project_dir=str(tmp_path),
                script_path="t.py",
                archive_on="sometimes",
            )


# ---------------------------------------------------------------- qos_whitelist


class TestQosWhitelist:
    @pytest.mark.asyncio
    async def test_disallowed_qos_raises(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="t.py",
            default_spec=ResourceSpec(walltime="1:00:00"),
            qos_whitelist=frozenset({"normal", "medium"}),
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        with pytest.raises(ValueError, match="qos="):
            await src.submit_job(
                {"s": 0},
                "task_0",
                "sweep_1",
                spec=ResourceSpec(qos="lowprio"),
            )

    @pytest.mark.asyncio
    async def test_allowed_qos_succeeds(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 1\n"))
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="t.py",
            default_spec=ResourceSpec(walltime="1:00:00"),
            qos_whitelist=frozenset({"normal", "medium"}),
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweep", "sweep_1")
        jid = await src.submit_job(
            {"s": 0},
            "task_0",
            "sweep_1",
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

    def test_spec_typo_keeps_account_qos_exclude(self, tmp_path, caplog):
        # C2: one unknown key used to drop the whole spec — account, qos and
        # the --exclude that keeps CPU jobs off GPU nodes went with it.
        spec = {
            "account": "lab",
            "qos": "medium",
            "extra_directives": {"--exclude": "gpu[01-02]"},
            "cpus": 4,  # typo of cpus_per_task
            "speed_factors": {"a100": 1.0},  # misplaced: belongs beside spec:
        }
        with caplog.at_level("WARNING"):
            src = build_ssh_slurm_source(
                name="uzh",
                remote_cfg={"host": "uzh", "spec": spec},
                distributed_cfg={},
                project_dir=str(tmp_path),
                script_path="train.py",
            )
        assert (src.default_spec.account, src.default_spec.qos) == ("lab", "medium")
        assert dict(src.default_spec.extra_directives) == {"--exclude": "gpu[01-02]"}
        messages = [r.message for r in caplog.records]
        assert any("['cpus']" in m for m in messages)
        # Only the "move it up" hint mentions speed_factors — no generic duplicate.
        assert [m for m in messages if "speed_factors" in m] == [
            m for m in messages if "up one level" in m
        ]
        assert len([m for m in messages if "up one level" in m]) == 1

    def test_spec_invalid_value_raises(self, tmp_path):
        with pytest.raises(ValueError, match="remote 'uzh' spec: .*gpu_type requires gpus"):
            build_ssh_slurm_source(
                name="uzh",
                remote_cfg={"host": "uzh", "spec": {"account": "lab", "gpu_type": "H100"}},
                distributed_cfg={},
                project_dir=str(tmp_path),
                script_path="train.py",
            )

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


# ----------------------------------------------------------- resumable chains


class TestResumableSubmit:
    """Resumable chains (issue #12): chunk submissions carry --signal, a
    dependency on chunk >=2, the resume env/arg, the sentinel skip-check, and
    cap every sub-array's walltime at chunk_walltime."""

    def _ctx(self, chunk_index, **over):
        from hpc_sweep_manager.core.common.resumable import (
            ResumableConfig,
            ResumableContext,
        )

        opts = dict(
            enabled=True,
            chunk_walltime="23:00:00",
            signal_grace=120,
            resume_arg="training.resume_from",  # opt into the hydra CLI override
        )
        opts.update(over)
        return ResumableContext(chunk_index=chunk_index, config=ResumableConfig(**opts))

    async def _make_src(self, tmp_path, conn, **spec_over):
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="train.py",
            default_spec=ResourceSpec(walltime="48:00:00", **spec_over),
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweeps" / "outputs" / "sw", "sw")
        return src

    def _rendered(self, conn):
        scripts = [
            c
            for c in conn.run_calls
            if c["cmd"].startswith("cat > ") and c["cmd"].rstrip("'\"").endswith(".slurm")
        ]
        return "\n".join(c["input"] for c in scripts)

    @pytest.mark.asyncio
    async def test_chunk0_render(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 100\n"))
        src = await self._make_src(tmp_path, conn)
        ids = await src.submit_batch(
            params_list=[{"seed": 0}, {"seed": 1}],
            sweep_id="sw",
            mode="array",
            job_name_prefix="sw",
            dependency=None,
            resumable=self._ctx(0),
        )
        assert ids == ["100"]
        body = self._rendered(conn)
        # Signal present; NO dependency on chunk 0; walltime capped at the chunk.
        assert "#SBATCH --signal=B:TERM@120" in body
        assert "--dependency" not in body
        assert "#SBATCH --time=23:00:00" in body
        assert "#SBATCH --time=48:00:00" not in body  # the full budget is NOT used
        # Fresh start: empty resume pointer, no resume arg in the command.
        assert 'export HSM_RESUME_FROM=""' in body
        assert "training.resume_from=" not in body
        # Sentinel skip-check, not the Status: grep.
        assert 'if [[ -f "$HSM_DONE_SENTINEL" ]]; then' in body
        # SIGTERM-forwarding run block.
        assert 'eval "$COMMAND" &' in body
        assert "set +e" in body
        assert "Status: CHUNK_INCOMPLETE" in body

    @pytest.mark.asyncio
    async def test_chunk1_render_has_dependency_and_resume(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 101\n"))
        src = await self._make_src(tmp_path, conn)
        await src.submit_batch(
            params_list=[{"seed": 0}, {"seed": 1}],
            sweep_id="sw",
            mode="array",
            job_name_prefix="sw",
            dependency="afterany:100",
            resumable=self._ctx(1),
        )
        body = self._rendered(conn)
        assert "#SBATCH --dependency=afterany:100" in body
        assert 'export HSM_RESUME_FROM="$HSM_RESUME_TO"' in body
        assert "training.resume_from=$HSM_RESUME_FROM" in body

    @pytest.mark.asyncio
    async def test_budget_threading_identical_overrides(self, tmp_path):
        """Same hydra overrides + output.dir/wandb.group every chunk; only the
        resume pointer differs (faithful-budget contract)."""
        conn0 = FakeConn(responder=_setup_ok_responder())
        conn0.add("sbatch", _Result(0, stdout="Submitted batch job 100\n"))
        s0 = await self._make_src(tmp_path / "a", conn0)
        await s0.submit_batch(
            params_list=[{"seed": 0}],
            sweep_id="sw",
            mode="array",
            job_name_prefix="sw",
            resumable=self._ctx(0),
        )
        conn1 = FakeConn(responder=_setup_ok_responder())
        conn1.add("sbatch", _Result(0, stdout="Submitted batch job 101\n"))
        s1 = await self._make_src(tmp_path / "b", conn1)
        await s1.submit_batch(
            params_list=[{"seed": 0}],
            sweep_id="sw",
            mode="array",
            job_name_prefix="sw",
            dependency="afterany:100",
            resumable=self._ctx(1),
        )

        # The params file (hydra overrides) is byte-identical across chunks.
        def _params(conn):
            return [
                c["input"]
                for c in conn.run_calls
                if c["cmd"].startswith("cat > ") and "parameter_combinations" in c["cmd"]
            ][0]

        assert _params(conn0) == _params(conn1)

    @pytest.mark.asyncio
    async def test_multi_type_resumable_caps_every_subarray(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 111\n"))
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 222\n"))
        src = await self._make_src(tmp_path, conn, gpus=1, gpu_type=("A100", "H200"))
        src.speed_factors = {"a100": 1.0, "h200": 0.5}
        ids = await src.submit_batch(
            params_list=[{"seed": i} for i in range(4)],
            sweep_id="sw",
            mode="array",
            job_name_prefix="sw",
            dependency="afterany:90:91",
            resumable=self._ctx(1),
        )
        assert ids == ["111", "222"]
        body = self._rendered(conn)
        # BOTH sub-arrays capped at chunk_walltime (NOT the cost-scaled 23/11.5h).
        assert body.count("#SBATCH --time=23:00:00") == 2
        assert "11:30:00" not in body
        # Both carry the signal + the same dependency on the previous chunk.
        assert body.count("#SBATCH --signal=B:TERM@120") == 2
        assert body.count("#SBATCH --dependency=afterany:90:91") == 2
        # Both still get their own --gres type.
        assert "--gres=gpu:A100:1" in body and "--gres=gpu:H200:1" in body

    @pytest.mark.asyncio
    async def test_non_array_resumable_rejected(self, tmp_path):
        conn = FakeConn(responder=_setup_ok_responder())
        src = await self._make_src(tmp_path, conn)
        with pytest.raises(ValueError, match="array mode"):
            await src.submit_batch(
                params_list=[{"seed": 0}],
                sweep_id="sw",
                mode="individual",
                resumable=self._ctx(0),
            )

    @pytest.mark.asyncio
    async def test_resumable_submit_skips_base_manifest(self, tmp_path):
        """In resumable mode the chain driver owns the manifest, so submit_batch
        does NOT write one itself."""
        conn = FakeConn(responder=_setup_ok_responder())
        conn.add("sbatch", _Result(0, stdout="Submitted batch job 100\n"))
        src = await self._make_src(tmp_path, conn)
        await src.submit_batch(
            params_list=[{"seed": 0}],
            sweep_id="sw",
            mode="array",
            job_name_prefix="sw",
            resumable=self._ctx(0),
        )
        assert not (src.sweep_dir / ".hsm_manifest.json").exists()
        # ...but persist_chain_manifest writes it (with the chain block).
        await src.persist_chain_manifest(
            resumable=self._ctx(0).config.to_manifest(),
            chain={
                "state": {"chunk_index": 0},
                "chunks": [{"index": 0, "job_ids": ["100"]}],
                "num_tasks": 1,
            },
            job_ids=["100"],
            num_tasks=1,
        )
        manifest = json.loads((src.sweep_dir / ".hsm_manifest.json").read_text())
        assert manifest["resumable"]["chunk_walltime"] == "23:00:00"
        assert manifest["chain"]["chunks"][0]["job_ids"] == ["100"]


class TestChunkProgress:
    """The one-find sentinel + checkpoint-mtime probe (issue #12)."""

    async def _src(self, tmp_path, stdout):
        conn = FakeConn(responder=_setup_ok_responder())
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="train.py",
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sw", "sw")
        conn.add("find", _Result(0, stdout=stdout))  # after setup: its snapshot GC runs a find
        return src

    @pytest.mark.asyncio
    async def test_parses_done_indices_and_mtime(self, tmp_path):
        out = "/r/sw/tasks/task_1/.hsm_done\n/r/sw/tasks/task_3/.hsm_done\nHSM_SEP\n1700000100.2\n"
        src = await self._src(tmp_path, out)
        prog = await src.chunk_progress(3, done_sentinel=".hsm_done", checkpoint_subdir="resume")
        assert prog.done_indices == frozenset({1, 3})
        assert prog.checkpoint_mtime == 1700000100.2

    @pytest.mark.asyncio
    async def test_pad_agnostic_task_parse(self, tmp_path):
        out = "/r/sw/tasks/task_07/.hsm_done\nHSM_SEP\n\n"
        src = await self._src(tmp_path, out)
        prog = await src.chunk_progress(8, done_sentinel=".hsm_done", checkpoint_subdir="resume")
        assert prog.done_indices == frozenset({7})
        assert prog.checkpoint_mtime is None

    @pytest.mark.asyncio
    async def test_prefix_with_task_dir_not_mis_parsed(self, tmp_path):
        # A workdir prefix containing a `/task_3/` component must NOT shadow the
        # real (trailing) task index — anchor on the sentinel's parent.
        out = "/scratch/task_3/runs/sw/tasks/task_9/.hsm_done\nHSM_SEP\n"
        src = await self._src(tmp_path, out)
        prog = await src.chunk_progress(9, done_sentinel=".hsm_done", checkpoint_subdir="resume")
        assert prog.done_indices == frozenset({9})

    @pytest.mark.asyncio
    async def test_empty_output(self, tmp_path):
        src = await self._src(tmp_path, "HSM_SEP\n")
        prog = await src.chunk_progress(2, done_sentinel=".hsm_done", checkpoint_subdir="resume")
        assert prog.done_indices == frozenset()
        assert prog.checkpoint_mtime is None


class TestChainManifestRoundTrip:
    """The re-submit-critical fields (spec/script_path/remote_code_dir/chain
    state) must survive persist -> from_manifest, or `advance` would re-submit
    chunks with empty #SBATCH directives (issue #12)."""

    @pytest.mark.asyncio
    async def test_spec_and_chain_survive(self, tmp_path):
        from hpc_sweep_manager.core.common.resumable import ResumableConfig
        from hpc_sweep_manager.core.remote.ssh_slurm_compute_source import (
            SSHSlurmComputeSource,
        )

        conn = FakeConn(responder=_setup_ok_responder())
        src = _StubSrc(
            name="uzh",
            host="uzh",
            project_dir=str(tmp_path),
            script_path="train.py",
            default_spec=ResourceSpec(
                walltime="48:00:00",
                gpus=1,
                gpu_type="V100",
                qos="normal",
                partition="lowprio",
            ),
            fake_conn=conn,
        )
        await src.setup(tmp_path / "sweeps" / "outputs" / "sw", "sw")
        await src.persist_chain_manifest(
            resumable=ResumableConfig(enabled=True, chunk_walltime="23:00:00").to_manifest(),
            chain={
                "state": {"chunk_index": 1},
                "chunks": [{"index": 0, "job_ids": ["100"]}],
                "num_tasks": 2,
                "last_done_count": 1,
                "last_checkpoint_mtime": 9.0,
                "wandb_group": "grp",
            },
            job_ids=["100"],
            num_tasks=2,
        )
        manifest = json.loads((src.sweep_dir / ".hsm_manifest.json").read_text())
        assert manifest["spec"]["gpu_type"] == "V100"
        assert manifest["script_path"] == "train.py"
        assert manifest["remote_code_dir"]

        restored = SSHSlurmComputeSource.from_manifest(manifest)
        assert restored.default_spec.gpu_type == "V100"
        assert restored.default_spec.partition == "lowprio"
        assert restored.default_spec.walltime == "48:00:00"
        assert restored.script_path == "train.py"
        assert restored._chain_state.chunk_index == 1
        assert restored._resumable_config.chunk_walltime == "23:00:00"
