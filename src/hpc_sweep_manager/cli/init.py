"""Project initialization CLI commands."""

from datetime import datetime
import logging
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table
import yaml

from ..core.common.config import MACHINE_CONFIG_PATH
from ..core.common.path_detector import PathDetector
from .common import common_options


_MIN_DISK_FREE_BYTES = 50 * 1024**3  # 50 GB — anything smaller isn't worth redirecting to.


def _detect_local_gpus() -> List[int]:
    """Return GPU indices reported by ``nvidia-smi -L``, or [] if unavailable.

    Sync wrapper used at init time to seed the ``local:`` block — keeps init
    free of asyncio bootstrap. See :func:`core.local.local_compute_source._detect_gpus`
    for the runtime async sibling.
    """
    try:
        proc = subprocess.run(
            ["nvidia-smi", "-L"], capture_output=True, text=True, timeout=5
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return []
    if proc.returncode != 0:
        return []
    return [int(m.group(1)) for m in re.finditer(r"^GPU\s+(\d+):", proc.stdout, re.M)]


def _detect_candidate_sweeps_roots() -> List[Tuple[Path, int]]:
    """Find writable directories with > 50 GB free under ``/mnt``, ``/data``, ``/scratch``.

    The common workstation pattern: a system disk that fills up under
    ``/`` plus a fat secondary mount somewhere predictable (the user's
    8 TB HDD, a /scratch share, etc.). For each parent we check every
    immediate child:

    - If the child itself is writable (e.g. ``/scratch/$USER``), use it.
    - Else if ``<child>/<USER>`` is a writable subdirectory (the common
      ``/mnt/8TB_HDD/$USER`` pattern where the mount root is owned by
      root), use that subdir instead.

    Returns ``(path, free_bytes)`` sorted by free space descending; empty
    list when nothing qualifies. Read-only mounts, tiny disks, and the
    current ``$HOME`` are all skipped.
    """
    candidates: List[Tuple[Path, int]] = []
    user = os.environ.get("USER", "")
    home = Path.home()

    for parent in (Path("/mnt"), Path("/data"), Path("/scratch")):
        if not parent.is_dir():
            continue
        try:
            children = sorted(parent.iterdir())
        except (PermissionError, OSError):
            continue
        for child in children:
            if not child.is_dir():
                continue
            # Resolve to a writable path: the child itself, or <child>/<user>.
            writable: Optional[Path] = None
            try:
                if os.access(child, os.W_OK):
                    writable = child
                elif user:
                    user_subdir = child / user
                    if user_subdir.is_dir() and os.access(user_subdir, os.W_OK):
                        writable = user_subdir
            except OSError:
                continue
            if writable is None:
                continue
            # Skip if it's actually $HOME (or under it) — redirecting back to
            # home defeats the purpose of the prompt.
            try:
                writable.relative_to(home)
                continue
            except ValueError:
                pass
            try:
                usage = shutil.disk_usage(writable)
            except OSError:
                continue
            if usage.free < _MIN_DISK_FREE_BYTES:
                continue
            candidates.append((writable, usage.free))
    candidates.sort(key=lambda item: -item[1])
    return candidates


def _format_size(n_bytes: int) -> str:
    """Render a byte count as the largest sensible TB/GB/MB unit."""
    for unit, divisor in (("TB", 1024**4), ("GB", 1024**3), ("MB", 1024**2)):
        if n_bytes >= divisor:
            return f"{n_bytes / divisor:.1f} {unit}"
    return f"{n_bytes} B"


def _prompt_sweeps_root(
    candidates: List[Tuple[Path, int]], console: Console
) -> Optional[str]:
    """Interactive picker for ``local.sweeps_root``. Returns chosen path or ``None``.

    ``None`` means "skip the redirect" — caller writes the commented stub.
    The chosen path has ``/hsm-sweeps`` appended automatically (we don't
    let the user type a full path on the standard options to avoid the
    "wait, where did it land?" surprise).
    """
    console.print(
        "\n[bold]Where should HSM store sweep outputs on this machine?[/bold]"
    )
    console.print(
        "[dim]HSM creates a discovery symlink at "
        "[cyan]<project>/sweeps/outputs/<sweep_id>[/cyan] so existing "
        "tooling (status/report) keeps finding sweeps where it expects.[/dim]\n"
    )

    n_other = len(candidates) + 1
    n_skip = len(candidates) + 2

    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column(style="cyan", justify="right")
    table.add_column()
    table.add_column(style="dim")
    for i, (path, free) in enumerate(candidates, 1):
        proposed = path / "hsm-sweeps"
        table.add_row(f"{i}.", f"[green]{proposed}[/green]", f"({_format_size(free)} free on {path})")
    table.add_row(f"{n_other}.", "Other (enter path)", "")
    table.add_row(f"{n_skip}.", "[dim]Don't redirect — sweeps stay under each project[/dim]", "")
    console.print(table)
    console.print()

    choice = Prompt.ask(
        "Choice",
        choices=[str(i) for i in range(1, n_skip + 1)],
        default="1",
    )
    idx = int(choice)
    if idx == n_skip:
        return None
    if idx == n_other:
        custom = Prompt.ask("Enter sweeps_root path").strip()
        return custom or None
    path, _ = candidates[idx - 1]
    return str(path / "hsm-sweeps")


def _render_machine_config(sweeps_root: Optional[str]) -> str:
    """Render ``~/.hsm/config.yaml`` content. Active if ``sweeps_root`` given, else commented stub."""
    header = (
        "# ~/.hsm/config.yaml — machine-wide HSM defaults\n"
        "#\n"
        "# This file holds settings that vary by MACHINE, not by project.\n"
        "# It is loaded by every HSM invocation on this user account; values\n"
        "# here are overridden by any matching field in a project's\n"
        "# .hsm/config.yaml. Only the `local:` block is honored here\n"
        "# (project-scoped blocks like `distributed:` or `slurm:` would\n"
        "# silently affect every project — confusing — so they're dropped\n"
        "# with a warning at load time).\n"
    )
    if sweeps_root:
        return header + (
            "\n"
            "local:\n"
            "  # Where sweep output dirs live on this machine. HSM creates a\n"
            "  # discovery symlink at <project>/sweeps/outputs/<sweep_id>\n"
            "  # pointing here so status/report tooling keeps working.\n"
            f"  sweeps_root: {sweeps_root}\n"
            "\n"
            "  # Optional: which GPU indices --mode local may use.\n"
            "  # Useful on shared boxes where (e.g.) GPU:0 is reserved.\n"
            "  # visible_gpus: [1, 2, 3]\n"
            "\n"
            "  # Optional: default python interpreter for --mode local.\n"
            "  # python_path: ~/miniconda3/envs/<env>/bin/python\n"
        )
    return header + (
        "#\n"
        "# Uncomment + edit what you want.\n"
        "\n"
        "# local:\n"
        "#   # Where sweep output dirs live on this machine. HSM creates a\n"
        "#   # discovery symlink at <project>/sweeps/outputs/<sweep_id>\n"
        "#   # pointing here so status/report tooling keeps working.\n"
        "#   sweeps_root: /mnt/big-drive/<user>/hsm-sweeps\n"
        "#\n"
        "#   # Which GPU indices --mode local may use.\n"
        "#   visible_gpus: [1, 2, 3]\n"
        "#\n"
        "#   # Default python interpreter for --mode local.\n"
        "#   python_path: ~/miniconda3/envs/<env>/bin/python\n"
    )


def _ensure_machine_config(console: Console) -> None:
    """Create or acknowledge ``~/.hsm/config.yaml`` on this machine.

    Side effects + user-facing prints — both branches are LOUD on purpose
    so the user knows this file is load-bearing.
    """
    machine_path = MACHINE_CONFIG_PATH

    if machine_path.exists():
        console.print(
            f"\n[green]✓[/green] Using existing machine config: "
            f"[cyan]{machine_path}[/cyan]"
        )
        return

    console.print("\n[bold cyan]Machine-wide HSM config setup[/bold cyan]")
    console.print(
        f"[dim]No [cyan]{machine_path}[/cyan] found — creating one. "
        f"This file holds defaults that vary by machine (sweeps_root, "
        f"visible_gpus, ...) and is shared across every project on this "
        f"account.[/dim]"
    )

    sweeps_root: Optional[str] = None
    if sys.stdin.isatty():
        candidates = _detect_candidate_sweeps_roots()
        if candidates:
            sweeps_root = _prompt_sweeps_root(candidates, console)

    machine_path.parent.mkdir(parents=True, exist_ok=True)
    machine_path.write_text(_render_machine_config(sweeps_root))

    console.print(
        f"\n[green]✓[/green] Created machine config: [cyan]{machine_path}[/cyan]"
    )

    if sweeps_root:
        # Pre-create the sweeps_root so the first sweep doesn't hit
        # resolve_sweep_dir's hard-error.
        target = Path(os.path.expandvars(os.path.expanduser(sweeps_root)))
        try:
            target.mkdir(parents=True, exist_ok=True)
            console.print(
                f"  [dim]sweeps_root = {sweeps_root} (created)[/dim]"
            )
        except OSError as e:
            console.print(
                f"  [yellow]⚠[/yellow]  sweeps_root = {sweeps_root} "
                f"(could not create: {e})"
            )
            console.print(
                f"     Create it before your first sweep, or edit "
                f"[cyan]{machine_path}[/cyan] to disable the redirect."
            )
    else:
        console.print(
            "  [dim]Stub only — sweeps will land in each project's "
            "sweeps/outputs/. Edit the file to set sweeps_root if you "
            "want a single landing zone.[/dim]"
        )


def _render_typed_config_scaffold(gpu_count: int) -> str:
    """Render the appended-to-config.yaml scaffold.

    Three independent, mode-scoped blocks (no field bleed between them):

    - ``local:`` — read only by ``--mode local``. If GPUs are detected on the
      init machine, the block is emitted **active** with ``gpus: 1`` so
      ``hsm sweep run --mode local`` uses them immediately (one task per GPU).
      Toggle off by editing ``gpus: 0`` or commenting the block. With no GPUs
      detected, the whole block stays commented as a discoverable scaffold.
    - ``slurm:`` — read only by ``--mode array|individual``.
    - ``distributed:`` — populated by ``hsm remote add``. Per-remote ``spec:``
      sub-block is the no-bleed home for that remote's ResourceSpec defaults.
    """
    if gpu_count > 0:
        # When >1 GPU is present, surface visible_gpus as a discoverable hint
        # (shared boxes often reserve GPU:0 for interactive work) — but keep it
        # commented so the default behavior is "use every GPU."
        if gpu_count > 1:
            allowlist_example = list(range(1, gpu_count))  # e.g. 4 GPUs -> [1, 2, 3]
            visible_hint = (
                f"# Allowlist (optional): restrict which GPU indices this box uses.\n"
                f"# CLI `--gpus 0,1,3` overrides. Useful on shared boxes where (e.g.)\n"
                f"# GPU:0 is reserved for interactive work.\n"
                f"#   visible_gpus: {allowlist_example}    # would exclude GPU:0\n"
            )
        else:
            visible_hint = ""
        local_block = (
            "# Auto-detected {n} GPU(s) on this box via `nvidia-smi -L` at init time.\n"
            "# To toggle off: set `gpus: 0` or comment the block out.\n"
            "# Other fields (walltime/cpus_per_task/mem/pre_script) are optional — see\n"
            "# docs/user_guide/HPC_EXECUTION.md for the full schema.\n"
            "local:\n"
            "  gpus: 1                  # per-task GPU count; LocalComputeSource partitions\n"
            "                           #   the {n} detected GPU(s) into slots of this size\n"
            "{visible_hint}"
            "# Optional reach fields (commented — uncomment to use):\n"
            "#   walltime: \"04:00:00\"\n"
            "#   cpus_per_task: 4\n"
            "#   mem: \"16gb\"\n"
            "#   pre_script:\n"
            "#     - \"conda activate my-env\"\n"
            "# Optional: redirect sweep dirs to a different filesystem (e.g. a big HDD\n"
            "# mount on a workstation whose system disk is tight). The data lives at\n"
            "# `<sweeps_root>/<sweep_id>/`; a symlink at `<project>/sweeps/outputs/<sweep_id>`\n"
            "# keeps `hsm sweep status`/`report` working transparently.\n"
            "#   sweeps_root: \"/mnt/big-disk/$USER/hsm-sweeps\"\n"
        ).format(n=gpu_count, visible_hint=visible_hint)
    else:
        local_block = (
            "# No GPUs detected at init time. Uncomment + edit to use --mode local\n"
            "# with explicit per-task resources.\n"
            "# local:\n"
            "#   gpus: 1                # per-task GPU count (needs nvidia-smi on this box)\n"
            "#   visible_gpus: [1, 2, 3]  # optional allowlist; CLI --gpus overrides\n"
            "#   walltime: \"04:00:00\"\n"
            "#   cpus_per_task: 4\n"
            "#   mem: \"16gb\"\n"
            "#   pre_script:\n"
            "#     - \"conda activate my-env\"\n"
            "#   sweeps_root: \"/mnt/big-disk/$USER/hsm-sweeps\"  # redirect to a larger filesystem\n"
        )

    return f"""
# --- Defaults for `--mode local` (LocalComputeSource) -------------------------
# Read ONLY when --mode is local. Slurm-only fields (gpu_type / modules /
# qos / account / extra_directives) belong in `slurm:` below, not here.
{local_block}

# --- Optional: defaults for `--mode array|individual` (Slurm) -----------------
# Read ONLY when --mode is array or individual. Reaches fields the opaque
# --resources CLI string can't express (gpu_type / modules / qos / account).
# See docs/user_guide/HPC_EXECUTION.md#the-typed-slurm-block
# slurm:
#   walltime: "01:00:00"
#   cpus_per_task: 4
#   mem: "16gb"
#   gpus: 1
#   gpu_type: "H100"          # -> #SBATCH --gres=gpu:H100:1
#                             #   GRES names are CASE-SENSITIVE — check `sinfo -o "%P %G"`
#                             #   on your cluster (S3IT uses uppercase: H100/L4/A100/H200).
#   qos: "normal"
#   account: "my-project"
#   pre_script:
#     - "conda activate my-env"
#   modules:                  # use ONLY for non-flavour modules (matlab, openmpi, ...).
#     - matlab                #   GPU flavour modules (h100/l4) should be loaded on the
#                             #   command line BEFORE `hsm sweep run`, not in the script —
#                             #   they set Slurm constraints that conflict with directives.
#   max_array_size: 10000     # cluster's Slurm-array ceiling (Slurm default)

# --- Optional: SSH remotes (populated by `hsm remote add <alias>`) ------------
# Per-remote `spec:` sub-block is the no-bleed home for that remote's
# default ResourceSpec — the `local:` / `slurm:` blocks above are deliberately
# NOT read for --mode remote or --mode distributed.
#
# `backend:` selects how this remote is driven:
#   - "ssh"   (default) — push code via rsync, run wrapped bash directly
#   - "slurm" — push code via rsync, then sbatch jobs over SSH; pair with
#               `workdir:` (e.g. /scratch/$USER/...) and `archive_dir:`
#               (e.g. /shares/<project>/...) for a survive-the-30-day-purge
#               flow on clusters that auto-clean scratch.
# distributed:
#   enabled: false
#   strategy: round_robin
#   sync_method: rsync
#   remotes:
#     my-box:                      # ~/.ssh/config alias (host defaults to alias)
#       max_parallel_jobs: 4
#       gpus: all                  # CLI --gpus default for this remote
#       conda_env: my-env          # remote conda env to activate
#       spec:                      # default ResourceSpec for this remote
#         walltime: "04:00:00"
#         cpus_per_task: 4
#         mem: "16gb"
#         gpus: 1                  # per-task GPU count
#     uzh:                         # example: S3IT Slurm cluster via SSH
#       backend: slurm
#       host: uzh
#       conda_env: my-env
#       workdir: "/scratch/$USER/hsm-runs"        # ephemeral; cleaned at 30d
#       archive_dir: "/shares/<project>/hsm-archive"   # durable safety net
#       archive_on: completed                     # completed | always | never
#       qos_whitelist: [normal, medium]
#       spec:
#         walltime: "06:00:00"
#         cpus_per_task: 4
#         mem: "32G"
#         gpus: 1
#         gpu_type: H100           # GRES casing is case-sensitive on S3IT
"""


def init_project(project_path: Path, interactive: bool, console: Console, logger: logging.Logger):
    """Initialize sweep infrastructure in a project."""
    console.print(
        Panel.fit(
            "[bold blue]HPC Sweep Manager - Project Initialization[/bold blue]",
            border_style="blue",
        )
    )

    project_path = project_path.resolve()
    console.print(f"[green]Initializing project at: {project_path}[/green]")

    # Check for existing old-style config (migration)
    old_config_path = project_path / "sweeps" / "hsm_config.yaml"
    migrating = False

    if old_config_path.exists():
        console.print("\n[yellow]📦 Found existing config at sweeps/hsm_config.yaml[/yellow]")
        console.print("[cyan]Migrating to new .hsm/ structure...[/cyan]\n")

        try:
            # Load existing config to preserve settings
            with open(old_config_path) as f:
                existing_config = yaml.safe_load(f)

            config = _extract_config_from_existing(existing_config, project_path)
            migrating = True

        except Exception as e:
            console.print(f"[yellow]Warning: Could not read old config: {e}[/yellow]")
            console.print("[yellow]Proceeding with fresh initialization...[/yellow]\n")
            migrating = False

    if not migrating:
        # Initialize path detector
        detector = PathDetector(project_path)

        # Get project information
        console.print("\n[yellow]Scanning project structure...[/yellow]")
        project_info = detector.get_project_info()
        issues = detector.validate_paths()

        # Display detected information
        _display_project_info(project_info, console)

        if issues:
            console.print("\n[red]⚠️  Issues detected:[/red]")
            for issue in issues:
                console.print(f"  - {issue}")

        # Interactive configuration if requested
        config = {}
        if interactive:
            console.print("\n[bold]Interactive Configuration[/bold]")
            config = _interactive_configuration(project_info, console)
        else:
            config = _auto_configuration(project_info)

    # Make sure the machine-wide ~/.hsm/config.yaml exists (creates or
    # acknowledges; prompts for sweeps_root only on a TTY with candidates).
    # Skipped when the project is $HOME itself (would collide path-wise).
    if project_path.resolve() != Path.home().resolve():
        _ensure_machine_config(console)
    else:
        console.print(
            "\n[yellow]Project path is $HOME; skipping machine config setup "
            "(would collide with the project's .hsm/config.yaml).[/yellow]"
        )

    # Create sweep infrastructure
    console.print("\n[yellow]Creating sweep infrastructure...[/yellow]")
    success = _create_sweep_infrastructure(project_path, config, console, logger)

    if success:
        # If we migrated, offer to clean up old file
        if migrating and old_config_path.exists():
            new_config_path = project_path / ".hsm" / "config.yaml"
            if new_config_path.exists():
                console.print(
                    "\n[green]✅ Successfully migrated config to .hsm/config.yaml[/green]"
                )
                if Confirm.ask("Remove old config at sweeps/hsm_config.yaml?", default=False):
                    old_config_path.unlink()
                    console.print("  ✅ Removed old config file")
                else:
                    console.print(
                        "  [dim]Kept old config (you can safely delete it manually)[/dim]"
                    )

        console.print("\n[green]✅ Project initialization completed successfully![/green]")
        _display_next_steps(console)
    else:
        console.print("\n[red]❌ Project initialization failed![/red]")


def _extract_config_from_existing(
    existing_config: Dict[str, Any], project_path: Path
) -> Dict[str, Any]:
    """Extract configuration from an existing ``sweeps/hsm_config.yaml`` for migration.

    Preserves project / paths / wandb settings. The old ``hpc:`` block is
    intentionally dropped — its ``default_walltime`` / ``default_resources``
    fields silently overrode the typed ``slurm:`` / ``local:`` blocks (see
    CLAUDE.md gotcha #5b). Users should re-express any such defaults in
    the typed scaffolds at the bottom of the new ``.hsm/config.yaml``.
    """
    config = {}

    project_section = existing_config.get("project", {})
    config["project_name"] = project_section.get("name", project_path.name)

    paths_section = existing_config.get("paths", {})
    config["python_path"] = paths_section.get("python_interpreter", "python")
    config["train_script"] = paths_section.get("train_script", "scripts/train.py")
    config["config_dir"] = paths_section.get("config_dir", "configs")

    wandb_section = existing_config.get("wandb", {})
    if wandb_section:
        config["wandb_project"] = wandb_section.get("project", config["project_name"])
        config["wandb_entity"] = wandb_section.get("entity", "")

    return config


def _display_project_info(info: Dict[str, Any], console: Console):
    """Display detected project information."""
    table = Table(title="Detected Project Information")
    table.add_column("Component", style="cyan")
    table.add_column("Status", style="magenta")
    table.add_column("Path/Value", style="green")

    # Core components
    table.add_row(
        "Config Directory",
        "✅ Found" if info["config_dir"] else "❌ Not found",
        str(info["config_dir"]) if info["config_dir"] else "None",
    )

    table.add_row(
        "Training Script",
        "✅ Found" if info["train_script"] else "❌ Not found",
        str(info["train_script"]) if info["train_script"] else "None",
    )

    table.add_row(
        "Python Interpreter",
        "✅ Found" if info["python_path"] else "❌ Not found",
        str(info["python_path"]) if info["python_path"] else "None",
    )

    table.add_row("HPC System", "✅ Detected", info["hpc_system"].upper())
    table.add_row("Output Directory", "📁 Available", str(info["output_dir"]))

    # Project features
    table.add_row("Git Repository", "✅ Yes" if info["has_git"] else "❌ No", "")
    table.add_row(
        "Dependencies",
        "✅ Yes"
        if any([info["has_requirements"], info["has_pyproject"], info["has_conda_env"]])
        else "❌ No",
        "",
    )

    console.print(table)


def _interactive_configuration(project_info: Dict[str, Any], console: Console) -> Dict[str, Any]:
    """Interactive configuration prompts."""
    config = {}

    # Project name
    default_name = project_info["project_root"].name
    config["project_name"] = Prompt.ask("Project name", default=default_name)

    # Python interpreter
    if project_info["python_path"]:
        use_detected = Confirm.ask(
            f"Use detected Python interpreter: {project_info['python_path']}?",
            default=True,
        )
        if use_detected:
            config["python_path"] = str(project_info["python_path"])
        else:
            config["python_path"] = Prompt.ask("Python interpreter path")
    else:
        config["python_path"] = Prompt.ask("Python interpreter path", default="python")

    # Training script
    if project_info["train_script"]:
        use_detected = Confirm.ask(
            f"Use detected training script: {project_info['train_script']}?",
            default=True,
        )
        if use_detected:
            config["train_script"] = str(project_info["train_script"])
        else:
            config["train_script"] = Prompt.ask("Training script path")
    else:
        config["train_script"] = Prompt.ask("Training script path", default="scripts/train.py")

    # Config directory
    if project_info["config_dir"]:
        config["config_dir"] = str(project_info["config_dir"])
    else:
        config["config_dir"] = Prompt.ask("Hydra config directory", default="configs")

    # No HPC/resource prompts: defaults live in the typed `local:` / `slurm:`
    # scaffolds appended to .hsm/config.yaml. Users edit those blocks
    # post-init for the modes they actually use.

    # W&B settings
    if Confirm.ask("Configure Weights & Biases integration?", default=True):
        config["wandb_project"] = Prompt.ask("W&B project name", default=config["project_name"])
        config["wandb_entity"] = Prompt.ask("W&B entity (optional)", default="")

    return config


def _auto_configuration(project_info: Dict[str, Any]) -> Dict[str, Any]:
    """Automatic configuration based on detected information.

    No HPC/resource defaults — the typed ``local:`` / ``slurm:`` scaffolds
    appended to ``.hsm/config.yaml`` are the canonical home for those.
    """
    return {
        "project_name": project_info["project_root"].name,
        "python_path": str(project_info["python_path"])
        if project_info["python_path"]
        else "python",
        "train_script": str(project_info["train_script"])
        if project_info["train_script"]
        else "scripts/train.py",
        "config_dir": str(project_info["config_dir"]) if project_info["config_dir"] else "configs",
        "wandb_project": project_info["project_root"].name,
    }


def _create_sweep_infrastructure(
    project_path: Path, config: Dict[str, Any], console: Console, logger: logging.Logger
) -> bool:
    """Create sweep directories and configuration files."""
    try:
        # Create .hsm directory (just the config home — no cache/logs subdirs;
        # nothing in HSM writes to them).
        hsm_dir = project_path / ".hsm"
        hsm_dir.mkdir(exist_ok=True)

        console.print("  ✅ Created .hsm/ directory")

        # Create sweep directory structure
        sweeps_dir = project_path / "sweeps"
        sweeps_dir.mkdir(exist_ok=True)

        outputs_dir = sweeps_dir / "outputs"
        outputs_dir.mkdir(exist_ok=True)

        sweep_logs_dir = sweeps_dir / "logs"
        sweep_logs_dir.mkdir(exist_ok=True)

        console.print("  ✅ Created sweeps/ directory structure")

        # Create HSM configuration file (in new location).
        # No `hpc:` block — defaults live in the typed `local:` / `slurm:` /
        # per-remote `spec:` scaffolds appended below. Each --mode reads only
        # its own block (see CLAUDE.md gotcha #5b).
        hsm_config_path = hsm_dir / "config.yaml"
        hsm_config = {
            "project": {
                "name": config["project_name"],
                "root": str(project_path),
            },
            "paths": {
                "python_interpreter": config["python_path"],
                "train_script": config["train_script"],
                "config_dir": config["config_dir"],
                "output_dir": "outputs",
            },
            "metadata": {
                "created_by": "HSM (HPC Sweep Manager)",
                "created_at": datetime.now().isoformat(),
                "version": "1.0.0",
            },
        }

        # Add W&B config if specified
        if config.get("wandb_project"):
            hsm_config["wandb"] = {
                "project": config["wandb_project"],
                "entity": config.get("wandb_entity", ""),
            }

        gpu_indices = _detect_local_gpus()
        with open(hsm_config_path, "w") as f:
            yaml.dump(hsm_config, f, default_flow_style=False, indent=2)
            f.write(_render_typed_config_scaffold(len(gpu_indices)))

        console.print(
            f"  ✅ Created HSM configuration file: {hsm_config_path.relative_to(project_path)}"
        )
        if gpu_indices:
            console.print(
                f"  🎯 Detected {len(gpu_indices)} GPU(s) — `local:` scaffold pre-seeded with `gpus: 1`"
            )

        # Create example sweep configuration
        example_sweep_path = sweeps_dir / "example_sweep.yaml"
        example_sweep = {
            "defaults": ["override hydra/launcher: basic"],
            "sweep": {
                "grid": {
                    "seed": [1, 2, 3, 4, 5],
                    "model.lr": [0.001, 0.01, 0.1],
                    "data.batch_size": [32, 64],
                }
            },
            "metadata": {
                "description": "Example hyperparameter sweep",
                "wandb_project": config.get("wandb_project", config["project_name"]),
                "tags": ["example", "baseline"],
            },
        }

        with open(example_sweep_path, "w") as f:
            yaml.dump(example_sweep, f, default_flow_style=False, indent=2)

        console.print("  ✅ Created example sweep configuration")

        # Create README
        readme_path = sweeps_dir / "README.md"
        project_name = config["project_name"]
        readme_content = f"""# {project_name} - HPC Sweeps

This directory contains HPC sweep configurations and outputs for the {project_name} project.

## Directory Structure

```
.hsm/
└── config.yaml          # HSM project configuration

sweeps/
├── example_sweep.yaml   # Example sweep configuration
├── outputs/             # Sweep results and logs
└── logs/                # HPC job logs
```

## Usage

```bash
# Count combinations (no submission)
hsm sweep run --config sweeps/example_sweep.yaml --count-only

# Preview without submitting
hsm sweep run --config sweeps/example_sweep.yaml --dry-run

# Run locally with N parallel processes
hsm sweep run --config sweeps/example_sweep.yaml --mode local

# Submit as a single Slurm array job
hsm sweep run --config sweeps/example_sweep.yaml --mode array

# Push to an SSH remote (rsync up, run, rsync results back)
hsm sweep run --config sweeps/example_sweep.yaml --remote my-box
```

## Configuration

- **HSM config**: `.hsm/config.yaml` — paths, defaults, optional typed
  `slurm:` block (for `gpu_type`, `modules`, `qos`, `account`, etc.) and
  `distributed:` block (SSH remotes, populated by `hsm remote add`).
- **Sweep configs**: YAML files in `sweeps/` (grid + paired axes,
  metadata, tags).

See the HSM documentation for details.

Generated by HSM on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
"""

        with open(readme_path, "w") as f:
            f.write(readme_content)

        console.print("  ✅ Created README documentation")

        logger.info(f"Successfully initialized sweep infrastructure at {project_path}")
        return True

    except Exception as e:
        console.print(f"  ❌ Error creating infrastructure: {e}")
        logger.error(f"Failed to create sweep infrastructure: {e}")
        return False


def _display_next_steps(console: Console):
    """Display next steps after initialization."""
    next_steps = """
[bold]Next Steps:[/bold]

1. **Review HSM configuration**: Edit `.hsm/config.yaml`. For Slurm reach
   fields (`gpu_type`, `modules`, `qos`, `account`, `pre_script`), uncomment
   the typed `slurm:` scaffold at the bottom of the file — see
   `docs/user_guide/HPC_EXECUTION.md`.
2. **(Optional) Register an SSH remote**: `hsm remote add <alias>` — uses
   your `~/.ssh/config` alias; nothing needs installing on the remote.
   For driving a Slurm cluster (e.g., S3IT) over SSH from off-cluster,
   set `backend: slurm` + `workdir` + `archive_dir` per-remote — see
   `docs/user_guide/SSH_EXECUTION.md#driving-slurm-over-ssh-backend-slurm`.
   For fanning a sweep across local + SSH workstation + SSH-Slurm cluster
   in one run, see `docs/user_guide/MULTI_CLUSTER.md`.
3. **Create sweep config**: Run `hsm setup configure` or edit
   `sweeps/example_sweep.yaml` directly.
4. **Dry-run**: `hsm sweep run --config sweeps/example_sweep.yaml --dry-run`
5. **Submit**: `hsm sweep run --config sweeps/example_sweep.yaml --mode array`
   (or `--mode local`, `--remote <alias>`, `--mode distributed`)
6. **Inspect**: `hsm sweep status <sweep-id>` and `hsm sweep report <sweep-id>`

[bold]Useful Commands:[/bold]
- `hsm sweep --help` - Sweep lifecycle (run/status/report/errors/watch/...)
- `hsm remote --help` - Manage SSH remotes (add/list/test/health/gpus/clean)
- `hsm sweep watch <sweep-id>` - Live progress for an active sweep
- `hsm setup configure` - Interactive sweep configuration builder
- `hsm --help` - All available commands
"""

    console.print(Panel(next_steps, title="Getting Started", border_style="green"))


# CLI command group structure
@click.group()
def setup():
    """Setup and configure HSM projects."""
    pass


@setup.command("init")
@click.option("--interactive", "-i", is_flag=True, help="Interactive setup mode")
@click.option("--project-root", type=click.Path(exists=True), help="Project root directory")
@common_options
@click.pass_context
def init_cmd(ctx, interactive: bool, project_root: str, verbose: bool, quiet: bool):
    """Initialize sweep infrastructure in a project."""
    project_path = Path(project_root) if project_root else Path.cwd()
    init_project(project_path, interactive, ctx.obj["console"], ctx.obj["logger"])


@setup.command("configure")
@click.option(
    "--from-file",
    type=click.Path(exists=True),
    help="Build from existing Hydra config file",
)
@click.option("--output", "-o", type=click.Path(), help="Output sweep config file")
@common_options
@click.pass_context
def configure_cmd(ctx, from_file: str, output: str, verbose: bool, quiet: bool):
    """Interactive sweep configuration builder."""
    from .configure import configure_sweep

    config_file = Path(from_file) if from_file else None
    output_file = Path(output) if output else None

    configure_sweep(config_file, output_file, ctx.obj["console"], ctx.obj["logger"])


if __name__ == "__main__":
    setup()
