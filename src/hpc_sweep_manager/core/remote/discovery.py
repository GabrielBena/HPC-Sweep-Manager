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
from typing import Any, Dict, Optional

try:
    import asyncssh

    ASYNCSSH_AVAILABLE = True
except ImportError:  # pragma: no cover - asyncssh is a hard runtime dep
    ASYNCSSH_AVAILABLE = False

logger = logging.getLogger(__name__)


def expand_ssh_key_path(ssh_key_path: str) -> Optional[str]:
    """Expand ``~`` / env vars in an ssh key path; return absolute or None if missing."""
    if not ssh_key_path:
        return None
    expanded_path = Path(os.path.expanduser(os.path.expandvars(ssh_key_path))).resolve()
    if expanded_path.exists():
        return str(expanded_path)
    logger.debug(f"SSH key not found at: {expanded_path}")
    return None


async def create_ssh_connection(
    host: str, ssh_key: Optional[str] = None, ssh_port: Optional[int] = None
):
    """Open an SSH connection, reusing the user's ``~/.ssh/config`` when present.

    ``host`` may be a plain hostname, a ``user@host`` string, or an alias
    defined in ``~/.ssh/config`` — asyncssh resolves the alias's ``HostName``,
    ``User``, ``Port``, ``IdentityFile``, ``ProxyJump`` etc. Explicit
    ``ssh_key`` / ``ssh_port`` arguments (from ``hsm_config.yaml``) override
    the corresponding ssh-config directives. Host keys are verified against
    ``~/.ssh/known_hosts`` (and any ``UserKnownHostsFile`` /
    ``StrictHostKeyChecking`` the config specifies).
    """
    logger.debug(f"Attempting SSH connection to {host}")

    connection_kwargs: Dict[str, Any] = {"host": host}

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
        conn = await asyncssh.connect(**connection_kwargs)
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
