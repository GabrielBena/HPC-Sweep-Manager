"""Distributed sweep wrapper for integration with existing sweep interface."""

import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..common.path_detector import PathDetector
from ..local.local_compute_source import LocalComputeSource
from ..remote.discovery import RemoteDiscovery
from ..remote.ssh_compute_source import SSHComputeSource
from .base_distributed_manager import DistributedSweepConfig, DistributionStrategy
from .distributed_job_manager import DistributedJobManager

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
            # Create distributed job manager
            config = self._create_distributed_config()
            self.distributed_manager = DistributedJobManager(
                sweep_dir=sweep_dir, config=config, show_progress=self.show_progress
            )

            # Create and add compute sources quietly
            await self._setup_compute_sources()

            # Setup all sources
            setup_success = await self.distributed_manager.setup_all_sources(sweep_id)

            if not setup_success:
                raise Exception("Failed to setup compute sources for distributed execution")

            self.console.print(
                f"[green]✓ Distributed environment ready with {len(self.distributed_manager.sources)} sources[/green]"
            )
            self.console.print(
                "[green]✓ All compute sources synchronized - consistent config state across all GPUs[/green]"
            )

            # Submit distributed sweep
            job_ids = await self.distributed_manager.submit_distributed_sweep(
                param_combinations=param_combinations,
                sweep_id=sweep_id,
                wandb_group=wandb_group,
            )

            self.console.print("[green]✓ Distributed sweep completed successfully![/green]")
            return job_ids

        except asyncio.CancelledError:
            self.logger.warning("Distributed sweep was cancelled")
            self.console.print("[yellow]⚠ Distributed sweep cancelled[/yellow]")
            raise
        except Exception as e:
            self.logger.error(f"Distributed sweep failed: {e}")
            self.console.print(f"[red]Distributed sweep failed: {e}[/red]")
            raise
        finally:
            # Cleanup
            if self.distributed_manager:
                try:
                    await self.distributed_manager.cleanup()
                except Exception as cleanup_error:
                    self.logger.error(f"Error during cleanup: {cleanup_error}")

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

        # Count potential sources for summary
        local_enabled = distributed_config.get("local_max_jobs", 1) > 0
        remotes = {
            k: v for k, v in distributed_config.get("remotes", {}).items() if v.get("enabled", True)
        }

        self.console.print(
            f"[cyan]Discovering compute sources... (up to {1 if local_enabled else 0} local + {len(remotes)} remote)[/cyan]"
        )

        try:
            # Add local compute source
            await self._add_local_compute_source(distributed_config)

            # Add SSH remote compute sources
            if remotes:
                await self._add_ssh_compute_sources(remotes)

            if len(self.distributed_manager.sources) == 0:
                raise Exception("No compute sources configured for distributed execution")

            self.logger.info(f"Configured {len(self.distributed_manager.sources)} compute sources")

        except Exception as e:
            self.logger.error(f"Error setting up compute sources: {e}")
            raise Exception(f"Failed to setup compute sources: {e}")

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
            local_max_jobs = distributed_config.get("local_max_jobs", 1)
            if self.hsm_config:
                local_max_jobs = min(local_max_jobs, self.hsm_config.get_max_array_size() or 1)

            local_source = LocalComputeSource(
                name="local",
                max_parallel_jobs=local_max_jobs,
                python_path=python_path,
                script_path=script_path,
                project_dir=project_dir,
            )

            self.distributed_manager.add_compute_source(local_source)
            self.console.print(f"[green]✓ Local source ready ({local_max_jobs} max jobs)[/green]")

        except Exception as e:
            self.logger.warning(f"Failed to add local compute source: {e}")
            self.console.print(f"[yellow]⚠ Local source unavailable: {e}[/yellow]")
            # Don't raise - this isn't critical if we have remote sources

    async def _add_ssh_compute_sources(self, remotes: Dict[str, Any]):
        """Add SSH remote compute sources."""
        discovery = RemoteDiscovery(self.hsm_config.config_data)

        successful_remotes = []
        failed_remotes = []

        for remote_name, remote_config in remotes.items():
            try:
                # Prepare remote info for discovery
                remote_info = remote_config.copy()
                remote_info["name"] = remote_name

                # Discover remote configuration
                discovered_config = await discovery.discover_remote_config(remote_info)

                if discovered_config:
                    ssh_source = SSHComputeSource(name=remote_name, remote_config=discovered_config)

                    self.distributed_manager.add_compute_source(ssh_source)
                    successful_remotes.append(
                        f"{remote_name} ({discovered_config.max_parallel_jobs} jobs)"
                    )

                else:
                    self.logger.warning(f"Failed to discover configuration for {remote_name}")
                    failed_remotes.append(remote_name)

            except Exception as e:
                self.logger.warning(f"Failed to add SSH source {remote_name}: {e}")
                failed_remotes.append(remote_name)

        # Show summary instead of individual messages
        if successful_remotes:
            self.console.print(
                f"[green]✓ Remote sources ready: {', '.join(successful_remotes)}[/green]"
            )

        if failed_remotes:
            self.console.print(
                f"[yellow]⚠ Unavailable remotes: {', '.join(failed_remotes)}[/yellow]"
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

        try:
            return asyncio.run(self._async_collect_results())
        except Exception as e:
            self.logger.error(f"Error collecting distributed results: {e}")
            return False

    async def _async_collect_results(self) -> bool:
        """Async result collection."""
        try:
            # Results should already be normalized by the distributed manager
            # during sweep execution, so this is just a verification

            tasks_dir = self.sweep_dir / "tasks"
            if tasks_dir.exists():
                task_count = len(
                    [d for d in tasks_dir.iterdir() if d.is_dir() and d.name.startswith("task_")]
                )
                self.console.print(f"[green]✓ {task_count} task results available locally[/green]")
                return True
            else:
                self.console.print("[yellow]⚠ No task results directory found[/yellow]")
                return False

        except Exception as e:
            self.logger.error(f"Error in result collection verification: {e}")
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
