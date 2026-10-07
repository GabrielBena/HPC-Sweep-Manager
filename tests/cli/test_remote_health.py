"""`hsm remote health --watch` (G1): it crashed (`datetime.now()` on a module)."""

from __future__ import annotations

import time

from click.testing import CliRunner

from hpc_sweep_manager.cli import remote as remote_cli


def test_watch_redraws_until_interrupted(monkeypatch):
    async def ping(name, cfg):
        return {"name": name, "host": name, "status": "healthy", "uptime": "up 1 day"}

    def stop(_):
        raise KeyboardInterrupt

    monkeypatch.setattr(remote_cli, "_resolve_remotes_for_action", lambda *a: ({"box": {}}, None))
    monkeypatch.setattr(remote_cli, "_ping_remote", ping)
    monkeypatch.setattr(time, "sleep", stop)
    res = CliRunner().invoke(remote_cli.remote, ["health", "box", "--watch"])
    assert res.exit_code == 0, res.output
    assert "Remote Health Monitor — 20" in res.output and "monitoring stopped" in res.output
