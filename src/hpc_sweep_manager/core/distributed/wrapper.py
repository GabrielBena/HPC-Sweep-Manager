"""Distributed sweep wrapper for integration with existing sweep interface."""

import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..common.path_detector import PathDetector
from ..local.local_compute_source import LocalComputeSource
from ..remote.discovery import RemoteDiscovery
from ..remote.ssh_compute_source import SSHComputeSource
from .manager import (
    DistributedJobManager,
    DistributedSweepConfig,
    DistributionStrategy,
)

logger = logging.getLogger(__name__)


class DistributedSweepWrapper:
    """Wrapper to make DistributedJobManager compatible with sync sweep interface."""

    def __init__(self, hsm_config, console, logger, sweep_dir: Path, show_progress: bool = True):
        self.hsm_config = hsm_config
        self.console = console
        self.logger = logger
        self.sweep_dir = sweep_dir
        self.show_progress = show_progress
        self.system_type = "distributed"
        self.distributed_manager = None

    def submit_sweep(
        self,
        param_combinations: List[Dict[str, Any]],
        mode: str,
        sweep_dir: Path,
        sweep_id: str,
        wandb_group: Optional[str] = None,
        **kwargs,
    ) -> List[str]:
        """Submit a distributed sweep (sync interface)."""
        return asyncio.run(
            self._async_submit_sweep(param_combinations, mode, sweep_dir, sweep_id, wandb_group)
        )

    async def _async_submit_sweep(
        self,
        param_combinations: List[Dict[str, Any]],
        mode: str,
        sweep_dir: Path,
        sweep_id: str,
        wandb_group: Optional[str] = None,
    ) -> List[str]:
        """Async implementation of distributed sweep submission."""
        try:
            self.console.print("[cyan]Setting up distributed computing environment...[/cyan]")

            # Create distributed job manager
            config = self._create_distributed_config()
            self.distributed_manager = DistributedJobManager(
                sweep_dir=sweep_dir, config=config, show_progress=self.show_progress
            )

            # Create and add compute sources
            await self._setup_compute_sources()

            # Setup all sources
            self.console.print(
                f"[cyan]Initializing {len(self.distributed_manager.sources)} compute sources...[/cyan]"
            )
            setup_success = await self.distributed_manager.setup_all_sources(sweep_id)

            if not setup_success:
                raise Exception("Failed to setup compute sources for distributed execution")

            self.console.print("[green]✓ Distributed environment ready[/green]")

            # Submit distributed sweep
            self.console.print(
                f"[bold]Starting distributed execution across {len(self.distributed_manager.sources)} sources...[/bold]"
            )
            job_ids = await self.distributed_manager.submit_distributed_sweep(
                param_combinations=param_combinations,
                sweep_id=sweep_id,
                wandb_group=wandb_group,
            )

            self.console.print("[green]✓ Distributed sweep completed successfully![/green]")
            return job_ids

        except Exception as e:
            self.logger.error(f"Distributed sweep failed: {e}")
            self.console.print(f"[red]Distributed sweep failed: {e}[/red]")
            raise
        finally:
            # Cleanup
            if self.distributed_manager:
                await self.distributed_manager.cleanup()

    def _create_distributed_config(self) -> DistributedSweepConfig:
        """Create distributed sweep configuration from HSM config."""
        distributed_config = self.hsm_config.config_data.get("distributed", {})

        # Parse strategy
        strategy_str = distributed_config.get("strategy", "round_robin")
        try:
            strategy = DistributionStrategy(strategy_str)
        except ValueError:
            strategy = DistributionStrategy.ROUND_ROBIN
            self.logger.warning(f"Unknown strategy '{strategy_str}', using round_robin")

        config = DistributedSweepConfig(
            strategy=strategy,
            collect_interval=distributed_config.get(
                "collect_interval", 30
            ),  # More frequent collection
            health_check_interval=distributed_config.get("health_check_interval", 60),
            max_retries=distributed_config.get("max_retries", 3),
            enable_auto_sync=distributed_config.get("enable_auto_sync", False),
            enable_interactive_sync=distributed_config.get("enable_interactive_sync", True),
        )

        return config

    async def _setup_compute_sources(self):
        """Setup all configured compute sources."""
        distributed_config = self.hsm_config.config_data.get("distributed", {})

        # Add local compute source
        await self._add_local_compute_source(distributed_config)

        # Add SSH remote compute sources
        remotes = distributed_config.get("remotes", {})
        if remotes:
            await self._add_ssh_compute_sources(remotes)

        if len(self.distributed_manager.sources) == 0:
            raise Exception("No compute sources configured for distributed execution")

        self.logger.info(f"Configured {len(self.distributed_manager.sources)} compute sources")

    async def _add_local_compute_source(self, distributed_config: Dict[str, Any]):
        """Add local compute source."""
        try:
            # Get paths from HSM config or auto-detect
            detector = PathDetector()
            python_path = self.hsm_config.get_default_python_path() or detector.detect_python_path()
            script_path = (
                self.hsm_config.get_default_script_path() or detector.detect_train_script()
            )
            project_dir = self.hsm_config.get_project_root() or str(Path.cwd())

            # Get max parallel jobs for local
            local_max_jobs = distributed_config.get("local_max_jobs", 4)
            if self.hsm_config:
                local_max_jobs = min(local_max_jobs, self.hsm_config.get_max_array_size() or 4)

            local_source = LocalComputeSource(
                name="local",
                max_parallel_jobs=local_max_jobs,
                python_path=python_path,
                script_path=script_path,
                project_dir=project_dir,
            )

            self.distributed_manager.add_compute_source(local_source)
            self.console.print(
                f"[green]✓ Added local compute source ({local_max_jobs} max jobs)[/green]"
            )

        except Exception as e:
            self.logger.warning(f"Failed to add local compute source: {e}")
            self.console.print(f"[yellow]⚠ Could not add local compute source: {e}[/yellow]")

    async def _add_ssh_compute_sources(self, remotes: Dict[str, Any]):
        """Add SSH remote compute sources."""
        discovery = RemoteDiscovery(self.hsm_config.config_data)

        for remote_name, remote_config in remotes.items():
            if not remote_config.get("enabled", True):
                self.console.print(f"[dim]- Skipping disabled source: {remote_name}[/dim]")
                continue

            try:
                self.console.print(f"[cyan]Discovering SSH source: {remote_name}...[/cyan]")

                # Prepare remote info for discovery
                remote_info = remote_config.copy()
                remote_info["name"] = remote_name

                # Discover remote configuration
                discovered_config = await discovery.discover_remote_config(remote_info)

                if discovered_config:
                    ssh_source = SSHComputeSource(name=remote_name, remote_config=discovered_config)

                    self.distributed_manager.add_compute_source(ssh_source)
                    self.console.print(
                        f"[green]✓ Added SSH source: {remote_name} ({discovered_config.max_parallel_jobs} max jobs)[/green]"
                    )

                else:
                    self.logger.warning(f"Failed to discover configuration for {remote_name}")
                    self.console.print(
                        f"[yellow]⚠ Could not configure SSH source: {remote_name}[/yellow]"
                    )

            except Exception as e:
                self.logger.warning(f"Failed to add SSH source {remote_name}: {e}")
                self.console.print(
                    f"[yellow]⚠ Could not add SSH source {remote_name}: {e}[/yellow]"
                )

    def get_job_status(self, job_id: str) -> str:
        """Get job status (sync wrapper)."""
        if not self.distributed_manager:
            return "UNKNOWN"

        # Find the source for this job
        source_name = self.distributed_manager.job_to_source.get(job_id)
        if not source_name:
            return "UNKNOWN"

        source = self.distributed_manager.source_by_name.get(source_name)
        if not source:
            return "UNKNOWN"

        return asyncio.run(source.get_job_status(job_id))

    def collect_results(self) -> bool:
        """Collect results from all sources."""
        if not self.distributed_manager:
            return False

        return asyncio.run(self._async_collect_results())

    async def _async_collect_results(self) -> bool:
        """Async result collection."""
        try:
            # Collect results from all SSH sources
            ssh_sources = [
                s for s in self.distributed_manager.sources if isinstance(s, SSHComputeSource)
            ]

            if ssh_sources:
                self.console.print(
                    f"[cyan]Collecting results from {len(ssh_sources)} SSH sources...[/cyan]"
                )

                collection_tasks = []
                for source in ssh_sources:
                    collection_tasks.append(source.collect_results())

                results = await asyncio.gather(*collection_tasks, return_exceptions=True)

                success_count = sum(1 for r in results if r is True)
                self.console.print(
                    f"[green]✓ Results collected from {success_count}/{len(ssh_sources)} SSH sources[/green]"
                )

                return success_count > 0
            else:
                # Only local source, results are already local
                self.console.print("[green]✓ Results are local (no remote sources)[/green]")
                return True

        except Exception as e:
            self.logger.error(f"Error collecting distributed results: {e}")
            return False

    def _params_to_string(self, params: Dict[str, Any]) -> str:
        """Convert parameters to string format."""
        # Use the same parameter conversion as RemoteJobManager
        param_strs = []
        for key, value in params.items():
            if isinstance(value, (list, tuple)):
                value_str = str(list(value))
                param_strs.append(f'"{key}={value_str}"')
            elif value is None:
                param_strs.append(f'"{key}=null"')
            elif isinstance(value, bool):
                param_strs.append(f'"{key}={str(value).lower()}"')
            elif isinstance(value, str) and (" " in value or "," in value):
                param_strs.append(f'"{key}={value}"')
            else:
                param_strs.append(f'"{key}={value}"')
        return " ".join(param_strs)


def create_distributed_sweep_wrapper(
    hsm_config, console, logger, sweep_dir: Path, show_progress: bool = True
) -> DistributedSweepWrapper:
    """Create a distributed sweep wrapper."""
    if not hsm_config:
        raise Exception("HSM config required for distributed mode")

    distributed_config = hsm_config.config_data.get("distributed", {})
    if not distributed_config.get("enabled", False):
        raise Exception("Distributed computing is not enabled in hsm_config.yaml")

    if not distributed_config.get("remotes") and not distributed_config.get("local_max_jobs"):
        raise Exception("No compute sources configured for distributed execution")

    return DistributedSweepWrapper(
        hsm_config=hsm_config,
        console=console,
        logger=logger,
        sweep_dir=sweep_dir,
        show_progress=show_progress,
    )
