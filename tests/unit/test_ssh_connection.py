"""Unit tests for create_ssh_connection's ~/.ssh/config reuse + override logic.

These never open a real socket — asyncssh.connect is monkeypatched to capture
the kwargs HSM would hand it.
"""

from __future__ import annotations

import os

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


async def test_login_and_connect_are_bounded(captured_connect, ssh_config_present):
    await discovery.create_ssh_connection("gpubox")
    assert captured_connect["login_timeout"] == 30
    assert captured_connect["connect_timeout"] == 60
    # No keepalive by default: without a reconnect it would turn a network stall into a dead
    # launcher, and it would override the user's ServerAliveInterval.
    assert "keepalive_interval" not in captured_connect


async def test_a_caller_that_reconnects_asks_for_a_keepalive(captured_connect, ssh_config_present):
    await discovery.create_ssh_connection("gpubox", keepalive_interval=30)
    assert captured_connect["keepalive_interval"] == 30


# --- stale SSH agent (field report 2026-09-29): login stalls until the server resets


@pytest.fixture(autouse=True)
def no_stalled_agents(monkeypatch):
    """Each test starts with no host remembered as agent-stalled."""
    monkeypatch.setattr(discovery, "_AGENT_STALLED", set())


def _failing_connect(monkeypatch, tmp_path, exc_factory):
    """asyncssh.connect raises ``exc_factory()`` unless the agent is disabled; record each call."""
    calls: list[dict] = []

    async def fake_connect(**kwargs):
        calls.append(kwargs)
        if "agent_path" not in kwargs:
            raise exc_factory()
        return object()

    monkeypatch.setattr(discovery.asyncssh, "connect", fake_connect)
    monkeypatch.setenv("HOME", str(tmp_path))  # asyncssh's default-key lookup stays hermetic
    return calls


@pytest.fixture(
    params=[
        lambda: ConnectionResetError(104, "Connection reset by peer"),  # sshd's LoginGraceTime
        lambda: discovery.asyncssh.ConnectionLost("Login timeout expired"),  # our login_timeout
    ],
    ids=["reset", "login-timeout"],
)
def stalls_on_agent(request, monkeypatch, tmp_path):
    return _failing_connect(monkeypatch, tmp_path, request.param)


async def test_stale_agent_is_retried_without_it(
    stalls_on_agent, ssh_config_present, monkeypatch, caplog
):
    monkeypatch.setenv("SSH_AUTH_SOCK", "/tmp/stale-agent.sock")
    assert await discovery.create_ssh_connection("gpubox") is not None
    assert len(stalls_on_agent) == 2
    assert stalls_on_agent[1]["agent_path"] is None
    assert "/tmp/stale-agent.sock" in caplog.text and "IdentityAgent none" in caplog.text
    assert discovery.agent_stalled("gpubox") and not discovery.agent_stalled("other")
    assert os.environ["SSH_AUTH_SOCK"] == "/tmp/stale-agent.sock"  # the process env is untouched

    # The next login to that host skips the agent from the start.
    await discovery.create_ssh_connection("gpubox")
    assert len(stalls_on_agent) == 3 and stalls_on_agent[2]["agent_path"] is None


async def test_no_agent_means_no_retry_and_a_clear_error(
    stalls_on_agent, ssh_config_present, monkeypatch
):
    monkeypatch.delenv("SSH_AUTH_SOCK", raising=False)
    with pytest.raises(ConnectionError, match="SSH login to gpubox timed out or was reset"):
        await discovery.create_ssh_connection("gpubox")
    assert len(stalls_on_agent) == 1


async def test_identity_agent_none_counts_as_no_agent(
    stalls_on_agent, ssh_config_present, monkeypatch
):
    ssh_config_present.write_text("Host gpubox\n    IdentityAgent none\n")
    monkeypatch.setenv("SSH_AUTH_SOCK", "/tmp/stale-agent.sock")
    with pytest.raises(ConnectionError, match="timed out or was reset"):
        await discovery.create_ssh_connection("gpubox")
    assert len(stalls_on_agent) == 1


async def test_failed_retry_is_a_clear_error(ssh_config_present, monkeypatch, tmp_path):
    calls = []

    async def fake_connect(**kwargs):
        calls.append(kwargs)
        raise discovery.asyncssh.ConnectionLost("Login timeout expired")

    monkeypatch.setattr(discovery.asyncssh, "connect", fake_connect)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("SSH_AUTH_SOCK", "/tmp/stale-agent.sock")
    with pytest.raises(ConnectionError, match="SSH login to gpubox timed out or was reset"):
        await discovery.create_ssh_connection("gpubox")
    assert len(calls) == 2
    assert not discovery.agent_stalled("gpubox")  # the agent wasn't the cause


async def test_unreachable_host_is_not_blamed_on_the_agent(
    ssh_config_present, monkeypatch, tmp_path, caplog
):
    # connect_timeout fires (asyncio.wait_for → TimeoutError): no retry, no agent warning.
    calls = _failing_connect(monkeypatch, tmp_path, TimeoutError)
    monkeypatch.setenv("SSH_AUTH_SOCK", "/tmp/stale-agent.sock")
    with pytest.raises(ConnectionError, match="Could not reach gpubox within 60 s"):
        await discovery.create_ssh_connection("gpubox")
    assert len(calls) == 1
    assert "stalled" not in caplog.text


async def test_other_connection_loss_is_not_retried(ssh_config_present, monkeypatch, tmp_path):
    lost = discovery.asyncssh.ConnectionLost
    calls = _failing_connect(monkeypatch, tmp_path, lambda: lost("Connection lost"))
    monkeypatch.setenv("SSH_AUTH_SOCK", "/tmp/stale-agent.sock")
    with pytest.raises(lost):
        await discovery.create_ssh_connection("gpubox")
    assert len(calls) == 1
