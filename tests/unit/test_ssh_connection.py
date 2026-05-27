"""Unit tests for create_ssh_connection's ~/.ssh/config reuse + override logic.

These never open a real socket — asyncssh.connect is monkeypatched to capture
the kwargs HSM would hand it.
"""

from __future__ import annotations

import pytest

from hpc_sweep_manager.core.remote import discovery


pytestmark = pytest.mark.asyncio


@pytest.fixture
def captured_connect(monkeypatch):
    """Capture the kwargs passed to asyncssh.connect; return a dummy conn."""
    captured: dict = {}

    async def fake_connect(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(discovery.asyncssh, "connect", fake_connect)
    return captured


@pytest.fixture
def ssh_config_present(monkeypatch, tmp_path):
    """Pretend ~/.ssh/config exists; leave other path expansions intact."""
    cfg = tmp_path / "config"
    cfg.write_text("Host gpubox\n    HostName 10.0.0.5\n    User gbena\n")
    real_expanduser = discovery.os.path.expanduser

    def fake_expanduser(p):
        return str(cfg) if p == "~/.ssh/config" else real_expanduser(p)

    monkeypatch.setattr(discovery.os.path, "expanduser", fake_expanduser)
    return cfg


async def test_passes_ssh_config_when_present(captured_connect, ssh_config_present):
    await discovery.create_ssh_connection("gpubox")
    assert captured_connect["config"] == [str(ssh_config_present)]
    assert captured_connect["host"] == "gpubox"


async def test_never_disables_host_key_checking(captured_connect, ssh_config_present):
    await discovery.create_ssh_connection("gpubox")
    # known_hosts must NOT be forced to None (that would disable verification).
    assert "known_hosts" not in captured_connect


async def test_no_username_forced(captured_connect, ssh_config_present):
    # The ssh-config User directive must win — HSM must not inject username.
    await discovery.create_ssh_connection("gpubox")
    assert "username" not in captured_connect


async def test_port_only_set_when_explicit(captured_connect, ssh_config_present):
    await discovery.create_ssh_connection("gpubox")
    assert "port" not in captured_connect

    captured_connect.clear()
    await discovery.create_ssh_connection("gpubox", ssh_port=2222)
    assert captured_connect["port"] == 2222


async def test_explicit_key_sets_client_keys(captured_connect, ssh_config_present, tmp_path):
    from pathlib import Path

    key = tmp_path / "id_test"
    key.write_text("dummy")
    await discovery.create_ssh_connection("gpubox", ssh_key=str(key))
    assert captured_connect["client_keys"] == [str(Path(key).resolve())]


async def test_no_ssh_config_file_omits_config(captured_connect, monkeypatch, tmp_path):
    # expanduser maps the config path to a non-existent file.
    real_expanduser = discovery.os.path.expanduser

    def fake_expanduser(p):
        return str(tmp_path / "nope" / "config") if p == "~/.ssh/config" else real_expanduser(p)

    monkeypatch.setattr(discovery.os.path, "expanduser", fake_expanduser)
    await discovery.create_ssh_connection("plainhost")
    assert "config" not in captured_connect
    assert captured_connect["host"] == "plainhost"
