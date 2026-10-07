"""Shared test doubles (chore-5). Import as ``from fakes import FakeConn, Result``:
``tests/`` is on ``sys.path`` because its ``conftest.py`` lives there."""

from __future__ import annotations

import asyncio
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
