"""CLI commands for remote machine management."""

import asyncio
import datetime
import logging
import posixpath
import re
import shlex
from pathlib import Path, PurePosixPath

import click
import yaml
from rich.console import Console
from rich.table import Table
from rich.tree import Tree

from ..core.common.config import HSMConfig
from ..core.remote.discovery import create_ssh_connection
from ..core.remote.gpu_probe import probe_gpus

# Set up more detailed logging for debugging
# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@click.group()
def remote():
    """Manage remote machines for distributed sweeps."""
    pass


def _config_write_path() -> Path:
    """Pick where to persist remote config edits.

    Prefers an existing config file in the standard search order; otherwise
    bootstraps a new ``.hsm/config.yaml`` (the primary location).
    """
    candidates = [
        Path.cwd() / ".hsm" / "config.yaml",
        Path.cwd() / "sweeps" / "hsm_config.yaml",
        Path.cwd() / "hsm_config.yaml",
    ]
    for path in candidates:
        if path.exists():
            return path
    return candidates[0]  # bootstrap a fresh .hsm/config.yaml


def _read_project_config() -> tuple[Path, str, dict]:
    """The PROJECT config file's path, text and data — never the machine-merged view."""
    path = _config_write_path()
    text = path.read_text() if path.exists() else ""
    data = yaml.safe_load(text) or {}
    if not isinstance(data, dict):
        raise click.ClickException(f"{path} is not a YAML mapping")
    return path, text, data


def _write_project_config(path: Path, text: str, data: dict, hint: str, entry: dict) -> None:
    """Rewrite the project config — or, if it has comments, print ``entry`` and exit 1."""
    if re.search(r"(^|\s)#", text, re.MULTILINE):  # a rewrite would strip them
        click.echo(yaml.safe_dump({"distributed": {"remotes": entry}}, sort_keys=False))
        raise click.ClickException(f"{path} has comments, so it is unchanged. {hint} by hand.")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(data, default_flow_style=False, indent=2))


def _unsafe_root(root: str, home: str) -> bool:
    """True when ``rm -rf`` at or under ``root`` could hit ``/``, ``$HOME`` or a top-level dir."""
    path, home_dir = (PurePosixPath(posixpath.normpath(p.strip() or ".")) for p in (root, home))
    return not path.is_absolute() or len(path.parts) < 3 or path in (home_dir, *home_dir.parents)


def _resolve_remotes_for_action(names: tuple, all_flag: bool, console: Console):
    """Resolve which remotes to act on, supporting bare ~/.ssh/config aliases.

    Registered remotes come from hsm_config's ``distributed.remotes``; a name
    that isn't registered is treated as a bare ssh-config alias (empty config →
    host defaults to the alias name). ``--all`` only spans registered remotes.

    Returns ``(remotes_dict, config_data)`` or ``(None, None)`` on error/empty.
    """
    hsm_config = HSMConfig.load()
    config_data = hsm_config.config_data if hsm_config else {}
    registered = config_data.get("distributed", {}).get("remotes", {})

    if all_flag:
        if not registered:
            console.print(
                "[yellow]No remotes registered. Add one with 'hsm remote add', "
                "or name a ~/.ssh/config alias directly.[/yellow]"
            )
            return None, None
        return {name: dict(cfg) for name, cfg in registered.items()}, config_data

    if not names:
        console.print("[red]Specify remote name(s) or use --all.[/red]")
        return None, None

    selected: dict = {}
    for name in names:
        if name in registered:
            selected[name] = dict(registered[name])
        else:
            console.print(
                f"[dim]{name}: not in hsm_config — treating as a ~/.ssh/config alias[/dim]"
            )
            selected[name] = {}
    return selected, config_data


@remote.command()
@click.argument("name")
@click.argument("host", required=False)
@click.option("--key", help="SSH key path (overrides ~/.ssh/config)")
@click.option("--port", type=int, default=None, help="SSH port (overrides ~/.ssh/config)")
@click.option("--max-jobs", type=int, help="Max parallel jobs (overrides remote default)")
@click.option("--enabled/--disabled", default=None, help="Enable/disable this remote")
def add(name: str, host: str, key: str, port: int, max_jobs: int, enabled: bool | None):
    """Add a new remote machine configuration.

    HOST is optional: if omitted, NAME is treated as a ~/.ssh/config alias and
    all connection details (hostname, user, port, key, proxy) come from there.
    Provide HOST / --key / --port only to override the ssh-config entry.
    Re-adding a registered remote updates only the fields you pass.
    """
    console = Console()

    # Load the project file (never the machine-merged view), or bootstrap a fresh one —
    # adding a remote is exactly the moment to create the file, so don't demand 'hsm init'.
    config_path, text, config_data = _read_project_config()

    distributed = config_data.setdefault(
        "distributed",
        {"enabled": False, "strategy": "round_robin", "sync_method": "rsync"},
    )
    remotes = distributed["remotes"] = distributed.get("remotes") or {}

    # Only persist connection fields that were explicitly given — a bare entry
    # resolves entirely from ~/.ssh/config via the alias.
    remote_config = {} if enabled is None else {"enabled": enabled}
    if host:
        remote_config["host"] = host
    if port:
        remote_config["ssh_port"] = port
    if key:
        remote_config["ssh_key"] = key
    if max_jobs:
        remote_config["max_parallel_jobs"] = max_jobs

    # Merge, never replace: an existing entry's backend/workdir/spec must survive.
    entry = remotes[name] = {**(remotes.get(name) or {}), **remote_config}
    hint = "Paste this entry under `distributed.remotes`"
    try:
        _write_project_config(config_path, text, config_data, hint, {name: entry})
        where = host or f"{name} (via ~/.ssh/config)"
        console.print(f"[green]✓ Added remote '{name}' ({where})[/green]")
        console.print(f"Configuration saved to: {config_path}")
        console.print(f"Run 'hsm remote test {name}' to verify the connection.")

    except OSError as e:
        console.print(f"[red]Failed to save configuration: {e}[/red]")


@remote.command()
def list():
    """List all configured remote machines."""
    console = Console()

    hsm_config = HSMConfig.load()
    remotes = hsm_config.config_data.get("distributed", {}).get("remotes", {}) if hsm_config else {}

    if not remotes:
        console.print("[yellow]No remotes registered yet.[/yellow]")
        console.print(
            "Register one with 'hsm remote add <name>' (uses your ~/.ssh/config alias), "
            "or ping an alias directly with 'hsm remote test <alias>'."
        )
        return

    table = Table(title="Configured Remote Machines")
    table.add_column("Name", style="cyan")
    table.add_column("Host", style="green")
    table.add_column("Port", style="magenta")
    table.add_column("SSH Key", style="blue")
    table.add_column("Max Jobs", style="yellow")
    table.add_column("Status", style="red")

    for name, config in remotes.items():
        status = "✓ Enabled" if config.get("enabled", True) else "✗ Disabled"

        # A bare entry resolves from ~/.ssh/config; show the alias + that hint.
        table.add_row(
            name,
            config.get("host", f"{name} (ssh config)"),
            str(config.get("ssh_port", "ssh config")),
            config.get("ssh_key", "ssh config") or "ssh config",
            str(config.get("max_parallel_jobs", "auto")),
            status,
        )

    console.print(table)


@remote.command()
@click.argument("names", nargs=-1)
@click.option("--all", is_flag=True, help="Probe all registered remotes")
def gpus(names: tuple, all: bool):
    """Show GPU availability on remote machines (via nvidia-smi).

    NAMES may be registered remotes or bare ~/.ssh/config aliases. Needs
    nothing on the remote except nvidia-smi — handy for deciding where to send
    a sweep. Example: ``hsm remote gpus anahita`` or ``hsm remote gpus --all``.
    """
    console = Console()

    targets, _ = _resolve_remotes_for_action(names, all, console)
    if not targets:
        return

    async def probe_all():
        results = {}
        for name, config in targets.items():
            host = config.get("host", name)
            try:
                results[name] = await probe_gpus(
                    host, config.get("ssh_key"), config.get("ssh_port")
                )
            except Exception as e:  # noqa: BLE001 - report unreachable per host
                results[name] = e
        return results

    try:
        results = asyncio.run(probe_all())
    except Exception as e:
        console.print(f"[red]Error probing GPUs: {e}[/red]")
        return

    table = Table(title="Remote GPU Availability")
    table.add_column("Machine", style="cyan")
    table.add_column("GPU", style="magenta", justify="right")
    table.add_column("Name", style="green")
    table.add_column("Memory", style="blue")
    table.add_column("Util", style="yellow", justify="right")
    table.add_column("State")

    for name, result in results.items():
        if isinstance(result, Exception):
            table.add_row(name, "-", f"[red]unreachable: {result}[/red]", "-", "-", "")
            continue
        if not result:
            table.add_row(name, "-", "[dim]no GPUs / nvidia-smi not found[/dim]", "-", "-", "")
            continue
        for i, gpu in enumerate(result):
            state = "[green]○ free[/green]" if gpu.is_free else "[red]● busy[/red]"
            table.add_row(
                name if i == 0 else "",
                str(gpu.index),
                gpu.name,
                f"{gpu.mem_used_gb:.1f}/{gpu.mem_total_gb:.0f} GB",
                f"{gpu.util_pct:.0f}%",
                state,
            )

    console.print(table)

    # Quick summary of free capacity to aid targeting decisions.
    free_by_host = {
        name: sum(1 for g in res if g.is_free)
        for name, res in results.items()
        if not isinstance(res, Exception) and res
    }
    if free_by_host:
        summary = ", ".join(f"{name}: {n} free" for name, n in free_by_host.items())
        console.print(f"\n[bold]Free GPUs[/bold] — {summary}")


async def _ping_remote(name: str, cfg: dict) -> dict:
    """Open ssh, run a few quick read-only commands, return what we learned.

    Per-field success is reported independently so a remote with `python`
    missing still reports ssh OK / uptime OK.
    """
    info: dict = {"name": name, "host": cfg.get("host", name)}
    try:
        async with await create_ssh_connection(
            cfg.get("host", name), cfg.get("ssh_key"), cfg.get("ssh_port")
        ) as conn:
            info["connection"] = "✓"
            for label, cmd in (
                ("date", "date"),
                ("uptime", "uptime"),
                ("disk", "df -h ~ | tail -1"),
                ("python", "python --version 2>&1 || python3 --version"),
            ):
                try:
                    r = await conn.run(cmd, check=False)
                    info[label] = (r.stdout or "").strip()
                except Exception as e:  # noqa: BLE001
                    info[label] = f"<error: {e}>"
        info["status"] = "healthy"
    except Exception as e:  # noqa: BLE001
        info["status"] = "unhealthy"
        info["connection"] = "✗"
        info["error"] = str(e)
    return info


@remote.command()
@click.argument("names", nargs=-1)
@click.option("--all", is_flag=True, help="Test all remotes")
def test(names: tuple, all: bool):
    """Quick SSH ping — open a connection and run a couple of read-only commands."""
    console = Console()

    test_remotes, _ = _resolve_remotes_for_action(names, all, console)
    if not test_remotes:
        return

    console.print(f"[bold]Testing {len(test_remotes)} remote(s)...[/bold]")

    async def run_all():
        return [await _ping_remote(name, cfg) for name, cfg in test_remotes.items()]

    try:
        results = asyncio.run(run_all())
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        return

    for info in results:
        if info["status"] == "healthy":
            tree = Tree(f"[green]✓ {info['name']}[/green] ({info['host']})")
            tree.add(f"Date: {info.get('date', 'N/A')}")
            tree.add(f"Python: {info.get('python', 'N/A')}")
            tree.add(f"Uptime: {info.get('uptime', 'N/A')}")
            console.print(tree)
        else:
            console.print(
                f"[red]✗ {info['name']} ({info['host']}): {info.get('error', 'unknown')}[/red]"
            )

    healthy = sum(1 for i in results if i["status"] == "healthy")
    console.print(f"\n[bold]{healthy}/{len(results)} remote(s) reachable.[/bold]")


@remote.command()
@click.argument("names", nargs=-1)
@click.option("--all", is_flag=True, help="Check health of all remotes")
@click.option("--watch", is_flag=True, help="Continuous monitoring mode")
@click.option("--refresh", default=30, help="Refresh interval in seconds for watch mode")
def health(names: tuple, all: bool, watch: bool, refresh: int):
    """Health check — ssh ping with load + disk + python info."""
    console = Console()

    check_remotes, _ = _resolve_remotes_for_action(names, all, console)
    if not check_remotes:
        return

    async def run_all():
        return [await _ping_remote(name, cfg) for name, cfg in check_remotes.items()]

    def show(results):
        table = Table(title="Remote Machine Health")
        table.add_column("Machine", style="cyan")
        table.add_column("Status", style="green")
        table.add_column("Uptime", style="yellow")
        table.add_column("Disk (home)", style="blue")
        table.add_column("Python", style="magenta")
        for info in results:
            status_color = "green" if info["status"] == "healthy" else "red"
            uptime = info.get("uptime", "N/A")
            if len(uptime) > 40:
                uptime = uptime[:37] + "..."
            table.add_row(
                info["name"],
                f"[{status_color}]{info['status']}[/{status_color}]",
                uptime,
                info.get("disk", "N/A"),
                info.get("python", "N/A"),
            )
        console.print(table)
        for info in results:
            if info["status"] != "healthy":
                console.print(f"[red]✗ {info['name']}: {info.get('error', 'unknown')}[/red]")

    if watch:
        console.print(f"[bold]Monitoring every {refresh}s (Ctrl+C to stop)...[/bold]")
        try:
            while True:
                console.clear()
                console.print(
                    f"[bold]Remote Health Monitor — "
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/bold]\n"
                )
                try:
                    show(asyncio.run(run_all()))
                except Exception as e:
                    console.print(f"[red]Error: {e}[/red]")
                import time

                time.sleep(refresh)
        except KeyboardInterrupt:
            console.print("\n[yellow]Health monitoring stopped.[/yellow]")
    else:
        try:
            show(asyncio.run(run_all()))
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")


@remote.command()
@click.argument("name")
@click.option(
    "--all-projects",
    is_flag=True,
    help="Remove the whole HSM root on the remote (every project's code + sweeps).",
)
@click.option("-y", "--yes", is_flag=True, help="Skip confirmation prompt.")
def clean(name: str, all_projects: bool, yes: bool):
    """Delete the HSM scratch directory for this project on a remote.

    By default removes ``{remote_root}/{project-root-dir-name}/`` — i.e. the
    rsync'd code cache + any per-sweep dirs that survived a failed run. Use
    ``--all-projects`` to wipe ``{remote_root}/`` itself.

    Resolves the remote_root as the sources do: a ``backend: slurm`` remote's
    ``workdir``, the registered remote's ``remote_root`` override,
    ``distributed.remote_root``, then ``~/.hsm/runs`` — expanded on the remote,
    refusing ``/``, ``$HOME`` and one-level paths. NAME may be a registered
    remote or a bare ssh-config alias.
    """
    from ..core.remote.discovery import create_ssh_connection

    console = Console()

    hsm_config = HSMConfig.load()
    config_data = hsm_config.config_data if hsm_config else {}
    distributed_cfg = config_data.get("distributed", {})
    registered = distributed_cfg.get("remotes", {})
    remote_cfg = dict(registered.get(name, {}))
    if name not in registered:
        console.print(f"[dim]{name}: not in hsm_config — treating as a ~/.ssh/config alias[/dim]")

    host = remote_cfg.get("host") or name
    ssh_key = remote_cfg.get("ssh_key")
    ssh_port = remote_cfg.get("ssh_port")
    remote_root = remote_cfg.get("remote_root", distributed_cfg.get("remote_root", "~/.hsm/runs"))
    if remote_cfg.get("backend") == "slurm" and remote_cfg.get("workdir"):
        remote_root = remote_cfg["workdir"]
    remote_root = remote_root.rstrip("/")

    if all_projects:
        target = remote_root
        scope_msg = f"the entire HSM root ({target}) on {host}"
    else:
        project_name = Path((hsm_config and hsm_config.get_project_root()) or Path.cwd()).name
        if project_name in ("", ".."):
            raise click.ClickException(f"Refusing to clean: unsafe project name {project_name!r}")
        target = f"{remote_root}/{project_name}"
        scope_msg = f"project '{project_name}' at {target} on {host}"

    if not yes:
        console.print(f"[yellow]About to remove {scope_msg}.[/yellow]")
        if not click.confirm("Proceed?", default=False):
            console.print("Cancelled.")
            return

    async def do_clean():
        async with await create_ssh_connection(host, ssh_key, ssh_port) as conn:
            # Expand ~/$VAR on the remote (unquoted echo, as the sources do), then guard.
            out = (await conn.run(f'echo {remote_root}; echo "$HOME"', check=False)).stdout
            root, home = ((out or "").splitlines() + ["", ""])[:2]
            if _unsafe_root(root, home):
                raise click.ClickException(
                    f"Refusing to clean under {root!r} on {host}: it is /, $HOME (or above "
                    f"it) or a top-level dir. Set a deeper `remote_root`/`workdir`."
                )
            path = root if all_projects else f"{root}/{project_name}"
            # `rm -rf` of a non-existent path is a no-op success → idempotent.
            result = await conn.run(f"rm -rf {shlex.quote(path)}", check=False)
            return result.returncode or 0

    try:
        rc = asyncio.run(do_clean())
    except click.ClickException:
        raise
    except Exception as e:
        console.print(f"[red]Failed to connect to {host}: {e}[/red]")
        return

    if rc == 0:
        console.print(f"[green]✓ Cleaned {target} on {host}[/green]")
    else:
        console.print(f"[red]rm exited with rc={rc} on {host}[/red]")


@remote.command()
@click.argument("name")
@click.confirmation_option(prompt="Are you sure you want to remove this remote?")
def remove(name: str):
    """Remove a remote machine configuration (from the project config file only)."""
    console = Console()

    config_path, text, config_data = _read_project_config()
    remotes = (config_data.get("distributed") or {}).get("remotes") or {}

    if name not in remotes:
        console.print(f"[red]Remote '{name}' not found in hsm_config.[/red]")
        return

    entry = remotes.pop(name)
    hint = "Delete this entry from `distributed.remotes`"
    try:
        _write_project_config(config_path, text, config_data, hint, {name: entry})
        console.print(f"[green]✓ Removed remote '{name}'[/green]")

    except OSError as e:
        console.print(f"[red]Failed to save configuration: {e}[/red]")


if __name__ == "__main__":
    remote()
