"""Shared test doubles (chore-5): an asyncssh connection, and a runner for rendered templates.
Import as ``from fakes import ...``; ``tests/`` is on ``sys.path`` (its conftest.py is there)."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


class Result:
    """Enough of asyncssh's ``SSHCompletedProcess``. ``returncode=None`` is a dropped link."""

    def __init__(
        self,
        returncode: int | None = 0,
        stdout: str = "",
        stderr: str = "",
        exit_status: int | None = None,
    ):
        self.returncode, self.stdout, self.stderr = returncode, stdout, stderr
        self.exit_status = exit_status if exit_status is not None else returncode


class FakeConn:
    """An asyncssh connection stand-in.

    ``add(substring, result)`` scripts a reply. Each ``run()`` takes the first entry whose
    substring is in the command and **pops** it, so repeated entries script repeated calls; an
    exception entry is raised (a dropped link). An unmatched command succeeds silently, except
    ``echo <path>``, which expands ``~``, ``$HOME`` and ``$USER`` as a remote shell would.
    Every call is recorded in ``run_calls``; ``cmds`` lists only the command strings.
    """

    def __init__(
        self,
        responder: list[tuple[str, Any]] | None = None,
        home: str = "/u/home/gbena",
        user: str | None = None,
    ):
        self.run_calls: list[dict[str, Any]] = []
        self.closed = False
        self.run_delay_s = 0.0
        self._responder = responder or []
        self._home = home.rstrip("/")
        self._user = user or (self._home.split("/")[-1] or "gbena")

    @property
    def cmds(self) -> list[str]:
        return [c["cmd"] for c in self.run_calls]

    def add(self, substring: str, result: Any) -> None:
        self._responder.append((substring, result))

    def _expand_echo(self, cmd: str) -> str:
        arg = cmd[len("echo ") :].strip().strip('"').strip("'")
        if arg.startswith("~"):
            arg = self._home + arg[1:]
        arg = arg.replace("${HOME}", self._home).replace("$HOME", self._home)
        return arg.replace("${USER}", self._user).replace("$USER", self._user)

    async def run(
        self, cmd: str, *, input: str | None = None, check: bool = False, timeout=None
    ) -> Result:
        self.run_calls.append({"cmd": cmd, "input": input, "check": check, "timeout": timeout})
        if self.run_delay_s:
            await asyncio.sleep(self.run_delay_s)
        for i, (sub, res) in enumerate(self._responder):
            if sub in cmd:
                del self._responder[i]
                if isinstance(res, Exception):
                    raise res
                return res
        if cmd.startswith("echo "):
            return Result(stdout=self._expand_echo(cmd) + "\n")
        return Result()

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        pass


# The stub trainer `run_template` runs: it records its argv and cwd, then exits with
# $HSM_TEST_RC.
STUB_TRAINER = """import json, os, sys
json.dump({"argv": sys.argv[1:], "cwd": os.getcwd()}, open(os.environ["HSM_TEST_OUT"], "w"))
sys.exit(int(os.environ.get("HSM_TEST_RC", "0")))
"""


def run_template(name: str, tmp_path: Path, params: dict, rc: int = 0, **ctx: Any):
    """Render template ``name`` for one task (``task_1`` under ``tmp_path/tasks``) and run it
    under bash, with the stub trainer as its script. Returns the finished process and what
    the trainer saw (``None`` when it never ran). ``ctx`` overrides any template variable."""
    from hpc_sweep_manager.core.common.templating import (
        params_to_hydra_args,
        params_to_yaml,
        render_template,
    )

    proj, tasks = tmp_path / "proj", tmp_path / "tasks"
    proj.mkdir(exist_ok=True)
    (proj / "train.py").write_text(STUB_TRAINER)
    params_file = tmp_path / "params.json"  # the array's: task 1 is global index 1
    params_file.write_text(json.dumps([{"index": 1, "global_index": 1, "params": params}]))
    task, py = str(tasks / "task_1"), sys.executable
    kw = dict(job_name="task_1", job_id="1", sweep_id="sw", wandb_group="g", num_jobs=1)
    kw |= dict(task_dir=task, remote_task_dir=task, tasks_dir=str(tasks), logs_dir=str(tmp_path))
    kw |= dict(project_dir=str(proj), remote_code_dir=str(proj), params_file=str(params_file))
    kw |= dict(python_path=py, run_prefix=py, script_path="train.py", sbatch_directives="")
    kw |= dict(params_hydra=params_to_hydra_args(params), params_yaml=params_to_yaml(params))
    kw |= dict(modules=[], pre_script=[], uses_conda=False, cuda_visible_devices=None)
    script = tmp_path / f"{name}.sh"
    script.write_text(render_template(name, **(kw | ctx)))
    out = tmp_path / "trainer.json"
    env = os.environ | {
        "SLURM_ARRAY_TASK_ID": "1",
        "HSM_TEST_OUT": str(out),
        "HSM_TEST_RC": str(rc),
    }
    proc = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True)
    return proc, json.loads(out.read_text()) if out.exists() else None
