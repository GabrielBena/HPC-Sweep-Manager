"""SSH connection primitives.

Historically this module also held remote-side configuration discovery
(``RemoteDiscovery`` / ``RemoteValidator``) used by the legacy pull-model
:class:`RemoteJobManager`. Push-model :class:`SSHComputeSource` doesn't
discover anything — every field comes from local ``hsm_config`` — so all
that's left here is the ssh-config-aware connection factory shared by
``gpu_probe``, ``SSHComputeSource``, and ``hsm remote test|health|clean``.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

try:
    import asyncssh

    ASYNCSSH_AVAILABLE = True
except ImportError:  # pragma: no cover - asyncssh is a hard runtime dep
    ASYNCSSH_AVAILABLE = False

logger = logging.getLogger(__name__)

# Hosts whose login stalled on the SSH agent and then worked without it, this run.
_AGENT_STALLED: set[str] = set()


def agent_stalled(host: str) -> bool:
    """True once a login to ``host`` stalled on the SSH agent: later logins and rsync skip it."""
    return host in _AGENT_STALLED


def expand_ssh_key_path(ssh_key_path: str) -> str | None:
    """Expand ``~`` / env vars in an ssh key path; return absolute or None if missing."""
    if not ssh_key_path:
        return None
    expanded_path = Path(os.path.expanduser(os.path.expandvars(ssh_key_path))).resolve()
    if expanded_path.exists():
        return str(expanded_path)
    logger.debug(f"SSH key not found at: {expanded_path}")
    return None


async def create_ssh_connection(
    host: str,
    ssh_key: str | None = None,
    ssh_port: int | None = None,
    keepalive_interval: int | None = None,
):
    """Open an SSH connection, reusing the user's ``~/.ssh/config`` when present.

    ``host`` may be a plain hostname, a ``user@host`` string, or an alias
    defined in ``~/.ssh/config`` — asyncssh resolves the alias's ``HostName``,
    ``User``, ``Port``, ``IdentityFile``, ``ProxyJump`` etc. Explicit
    ``ssh_key`` / ``ssh_port`` arguments (from ``hsm_config.yaml``) override
    the corresponding ssh-config directives. Host keys are verified against
    ``~/.ssh/known_hosts`` (and any ``UserKnownHostsFile`` /
    ``StrictHostKeyChecking`` the config specifies). A login that stalls on a
    stale SSH agent is retried once without it (:func:`_connect`).
    """
    logger.debug(f"Attempting SSH connection to {host}")

    # End a stuck login before sshd's LoginGraceTime does (asyncssh waits 120 s), and
    # bound the whole connect, ProxyJump hops included (asyncssh sets no bound).
    connection_kwargs: dict[str, Any] = {"host": host, "login_timeout": 30, "connect_timeout": 60}
    if keepalive_interval:  # only for a caller that reconnects: it ends a stalled link
        connection_kwargs["keepalive_interval"] = keepalive_interval
    if agent_stalled(host):
        connection_kwargs["agent_path"] = None

    # Hand asyncssh the user's ssh config so aliases resolve like `ssh <alias>`.
    ssh_config_path = os.path.expanduser("~/.ssh/config")
    if os.path.exists(ssh_config_path):
        connection_kwargs["config"] = [ssh_config_path]
        logger.debug(f"Using SSH config: {ssh_config_path}")

    # Explicit overrides win over the ssh-config entry. Leaving these unset lets
    # the config (or asyncssh defaults / SSH agent) supply them.
    if ssh_port:
        connection_kwargs["port"] = ssh_port
    if ssh_key:
        expanded_key = expand_ssh_key_path(ssh_key)
        if expanded_key:
            connection_kwargs["client_keys"] = [expanded_key]
        else:
            logger.warning(
                f"Specified SSH key not found: {ssh_key}; falling back to ssh config / agent"
            )

    # known_hosts is intentionally NOT set to None → asyncssh verifies against
    # ~/.ssh/known_hosts and honors the config's host-key directives.
    try:
        conn = await _connect(host, connection_kwargs)
        logger.debug(f"✓ SSH connection established to {host}")
        return conn
    except asyncssh.PermissionDenied as e:
        logger.error(f"SSH permission denied to {host}: {e}")
        logger.error(f"Check your key / ~/.ssh/config entry, or run: ssh-copy-id {host}")
        raise
    except asyncssh.HostKeyNotVerifiable as e:
        logger.error(f"Host key for {host} is not in known_hosts: {e}")
        logger.error(f"Connect once interactively to record it: ssh {host}")
        raise
    except Exception as e:
        logger.error(f"SSH connection to {host} failed: {type(e).__name__}: {e}")
        raise


async def _connect(host: str, kwargs: dict[str, Any]):
    """``asyncssh.connect`` with clear errors; a login stalled on an SSH agent is retried once
    without it.

    asyncssh asks the agent before the key files, so a stale agent (a forwarded
    ``SSH_AUTH_SOCK`` whose session is gone) stalls auth until our login_timeout
    or the server's LoginGraceTime resets the connection (field report 2026-09-29).
    """
    try:
        try:
            return await asyncssh.connect(**kwargs)
        except (asyncssh.ConnectionLost, ConnectionResetError) as e:
            # The agent asyncssh used: '' if SSH_AUTH_SOCK is unset or `IdentityAgent none`.
            agent = _login_stalled(e) and asyncssh.SSHClientConnectionOptions(**kwargs).agent_path
            if not agent:
                raise
            logger.warning(
                f"SSH login to {host} stalled ({e!r}) on the agent at {agent}, usually a stale "
                f"(e.g. forwarded) SSH_AUTH_SOCK. Retrying, and skipping the agent for {host} "
                f"for the rest of this run; to skip it for good, set `IdentityAgent none` for "
                f"{host} in ~/.ssh/config."
            )
        conn = await asyncssh.connect(**kwargs, agent_path=None)
    except TimeoutError as e:  # connect_timeout: TCP connect, handshake, ProxyJump hops
        raise ConnectionError(
            f"Could not reach {host} within {kwargs['connect_timeout']} s: host down, "
            f"network/VPN off, or a ProxyJump hop stuck (e.g. on a stale SSH agent)"
        ) from e
    except (asyncssh.ConnectionLost, ConnectionResetError) as e:
        if not _login_stalled(e):
            raise
        raise ConnectionError(f"SSH login to {host} timed out or was reset ({e!r})") from e
    _AGENT_STALLED.add(host)
    return conn


def _login_stalled(e: Exception) -> bool:
    """Our login_timeout fired, or the server reset the connection (its LoginGraceTime)."""
    return isinstance(e, ConnectionResetError) or "Login timeout" in str(e)
