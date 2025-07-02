"""Base distributed manager that properly inherits from BaseJobManager."""

import asyncio
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import logging
from pathlib import Path
import signal
import sys
from typing import Any, Dict, List, Optional, Set

from ..common.base_manager import BaseJobManager
from ..common.compute_source import ComputeSource, JobInfo

logger = logging.getLogger(__name__)


class DistributionStrategy(Enum):
    """Job distribution strategies."""

    ROUND_ROBIN = "round_robin"
    LEAST_LOADED = "least_loaded"
    CAPABILITY_BASED = "capability_based"


@dataclass
class DistributedSweepConfig:
    """Configuration for distributed sweep execution."""

    strategy: DistributionStrategy = DistributionStrategy.ROUND_ROBIN
    collect_interval: int = 300  # seconds
    health_check_interval: int = 60  # seconds
    max_retries: int = 3
    enable_auto_sync: bool = False
    enable_interactive_sync: bool = True
    # Failsafe configuration
    enable_source_failsafe: bool = True
    source_failure_threshold: float = 0.4  # Disable source if 40% of jobs fail
    min_jobs_for_failsafe: int = 5  # Need at least 5 jobs before considering failsafe
    auto_disable_unhealthy_sources: bool = True
    health_check_failure_threshold: int = 3  # Disable after 3 consecutive health check failures


class BaseDistributedManager(BaseJobManager):
    """Base class for distributed job managers with proper inheritance."""

    def __init__(
        self,
        sweep_dir: Path,
        config: DistributedSweepConfig = None,
        show_progress: bool = True,
        max_parallel_jobs: int = None,
    ):
        # Calculate total max parallel jobs from all sources
        total_max_jobs = max_parallel_jobs or 100  # Reasonable default
        super().__init__(total_max_jobs, show_progress)

        self.sweep_dir = sweep_dir
        self.config = config or DistributedSweepConfig()
        self.system_type = "distributed"

        # Job tracking - extending base class attributes
        self.jobs_submitted = 0
        self.jobs_failed = 0
        self.jobs_cancelled = 0

        # Compute sources
        self.sources: List[ComputeSource] = []
        self.source_by_name: Dict[str, ComputeSource] = {}

        # Job tracking - extending base class
        self.all_jobs: Dict[str, JobInfo] = {}  # job_id -> job_info
        self.job_to_source: Dict[str, str] = {}  # job_id -> source_name
        self.task_to_source: Dict[str, str] = {}  # task_name -> source_name
        self.failed_jobs: Dict[str, int] = {}  # job_id -> retry_count

        # Source failure tracking
        self.source_failure_counts = {}  # source_name -> failure_count
        self.source_job_counts = {}  # source_name -> total_job_count
        self.source_health_failures = {}  # source_name -> consecutive_health_failures
        self.disabled_sources = set()  # Set of disabled source names

        # Control flags
        self._cancelled = False
        self._cleanup_in_progress = False

        # Round-robin state
        self._round_robin_index = 0

        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Setup signal handlers to catch interrupts and preserve source mapping."""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        import atexit

        atexit.register(self._cleanup_on_exit)

        logger.debug("Signal handlers registered for distributed job manager")

    def _signal_handler(self, signum, frame):
        """Handle signals for graceful shutdown and source mapping preservation."""
        if hasattr(self, "_signal_received") and self._signal_received:
            logger.warning(f"Received signal {signum} again, forcing immediate exit...")
            sys.exit(1)

        self._signal_received = True
        logger.info(f"🛑 Signal {signum} received - preserving source mapping and cleaning up...")

        try:
            # Preserve source mapping before cleanup
            self._preserve_source_mapping_sync()

            # Run async cleanup
            import threading

            cleanup_thread = threading.Thread(target=self._threaded_cleanup)
            cleanup_thread.daemon = True
            cleanup_thread.start()
            cleanup_thread.join(timeout=30)

            if cleanup_thread.is_alive():
                logger.warning("Cleanup thread still running after timeout, forcing exit...")
                sys.exit(1)
            else:
                logger.info("✅ Distributed job cleanup completed successfully")
                sys.exit(0)

        except Exception as e:
            logger.error(f"Error during signal cleanup: {e}")
            sys.exit(1)

    def _threaded_cleanup(self):
        """Run async cleanup in a thread (called from signal handler)."""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                loop.run_until_complete(
                    asyncio.wait_for(self._async_cleanup_on_signal(), timeout=25.0)
                )
                logger.info("Signal cleanup completed successfully")
            except asyncio.TimeoutError:
                logger.warning("Signal cleanup timed out after 25 seconds")
            except Exception as e:
                logger.error(f"Error during async signal cleanup: {e}")

        except Exception as e:
            logger.error(f"Error in threaded cleanup: {e}")
        finally:
            try:
                if loop and not loop.is_closed():
                    loop.close()
            except Exception:
                pass

    async def _async_cleanup_on_signal(self):
        """Async cleanup method called when signal is received."""
        if self._cleanup_in_progress:
            return

        self._cleanup_in_progress = True
        self._cancelled = True

        try:
            logger.info("Starting cancellation of distributed jobs...")

            # Cancel all running jobs
            try:
                results = await self.cancel_all_jobs()
                logger.info(f"Job cancellation results: {results}")
            except Exception as e:
                logger.error(f"Error during job cancellation: {e}")

            # Cleanup compute sources
            logger.info("Cleaning up compute sources...")
            try:
                await asyncio.wait_for(self.cleanup(), timeout=10.0)
                logger.info("Compute source cleanup completed")
            except asyncio.TimeoutError:
                logger.warning("Compute source cleanup timed out")
            except Exception as e:
                logger.warning(f"Compute source cleanup failed: {e}")

        except Exception as e:
            logger.error(f"Error during async cleanup: {e}")
        finally:
            self._cleanup_in_progress = False

    def _cleanup_on_exit(self):
        """Clean up resources on program exit (called by atexit)."""
        if (
            not hasattr(self, "_signal_received")
            and hasattr(self, "running_jobs")
            and self.running_jobs
        ):
            logger.info("Program exiting, preserving source mapping...")
            self._preserve_source_mapping_sync()

    def _preserve_source_mapping_sync(self):
        """Synchronously preserve source mapping before cleanup."""
        try:
            mapping_file = self.sweep_dir / "source_mapping.yaml"
            if mapping_file.exists():
                backup_file = mapping_file.with_suffix(".yaml.signal_backup")
                import shutil

                shutil.copy2(mapping_file, backup_file)
                logger.info(f"Source mapping backed up to: {backup_file}")

            # Save current state
            asyncio.run(self._save_source_mapping())
            logger.info("Current source mapping state preserved")

        except Exception as e:
            logger.error(f"Error preserving source mapping: {e}")

    def add_compute_source(self, source: ComputeSource):
        """Add a compute source to the distributed manager."""
        self.sources.append(source)
        self.source_by_name[source.name] = source
        logger.debug(f"Added compute source: {source}")

    async def setup_all_sources(self, sweep_id: str) -> bool:
        """Setup all compute sources for job execution with strict sync requirements."""
        logger.info(f"Setting up {len(self.sources)} compute sources for distributed execution")
        logger.info(
            "🔒 Distributed mode requires strict project synchronization across all sources"
        )

        setup_tasks = []
        for source in self.sources:
            setup_tasks.append(source.setup(self.sweep_dir, sweep_id))

        results = await asyncio.gather(*setup_tasks, return_exceptions=True)

        successful_sources = []
        failed_sources = []
        sync_failed_sources = []

        for i, result in enumerate(results):
            source = self.sources[i]
            if isinstance(result, Exception):
                logger.error(f"Setup failed for {source.name}: {result}")
                failed_sources.append(source.name)
            elif result is True:
                logger.debug(f"✓ Setup successful for {source.name}")
                successful_sources.append(source.name)
            else:
                logger.error(f"Setup failed for {source.name} (likely sync enforcement failure)")
                if hasattr(source, "remote_manager"):
                    sync_failed_sources.append(source.name)
                else:
                    failed_sources.append(source.name)

        # For distributed execution, we need ALL sources to be properly synced
        if sync_failed_sources:
            logger.error("🚨 CRITICAL: Distributed execution cannot proceed!")
            logger.error(f"   Project sync enforcement failed for: {sync_failed_sources}")
            logger.error(
                "   Distributed sweeps require identical configs across ALL compute sources"
            )

            self.sources = []
            self.source_by_name = {}
            return False

        if not successful_sources:
            logger.error("No compute sources successfully set up")
            self.sources = []
            self.source_by_name = {}
            return False

        if failed_sources:
            logger.warning(f"Some sources failed setup (non-sync issues): {failed_sources}")
            # Remove failed sources
            self.sources = [s for s in self.sources if s.name in successful_sources]
            self.source_by_name = {
                name: s for name, s in self.source_by_name.items() if name in successful_sources
            }

        logger.info(f"✅ {len(successful_sources)} compute sources ready for distributed execution")
        logger.info("🔒 All sources have verified identical project configurations")
        return True

    async def cleanup(self):
        """Cleanup all resources."""
        logger.debug("Cleaning up distributed job manager")

        cleanup_tasks = []
        for source in self.sources:
            cleanup_tasks.append(source.cleanup())

        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)

        logger.debug("Distributed job manager cleanup completed")

    # Abstract methods from BaseJobManager that subclasses must implement
    def submit_single_job(
        self, params: Dict[str, Any], job_name: str, sweep_id: str, **kwargs
    ) -> str:
        """Submit a single job - to be implemented by concrete subclasses."""
        raise NotImplementedError("Subclasses must implement submit_single_job")

    def get_job_status(self, job_id: str) -> str:
        """Get status of a job."""
        if job_id in self.all_jobs:
            return self.all_jobs[job_id].status
        return "UNKNOWN"

    def cancel_job(self, job_id: str, timeout: int = 10) -> bool:
        """Cancel a specific job - to be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement cancel_job")

    async def cancel_all_jobs(self, timeout: int = 10) -> dict:
        """Cancel all running jobs - to be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement cancel_all_jobs")

    def wait_for_all_jobs(self, use_progress_bar: bool = False):
        """Wait for all jobs to complete - to be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement wait_for_all_jobs")

    async def _save_source_mapping(self):
        """Save source mapping - to be implemented by subclasses."""
        pass
