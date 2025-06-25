#!/usr/bin/env python3
"""
Debug script for remote job cancellation issues.

This script helps troubleshoot and test remote job cancellation functionality.

Usage:
    python debug_remote_cancellation.py <remote_name> [--test-signal]

Example:
    python debug_remote_cancellation.py blossom --test-signal
"""

import asyncio
import argparse
import signal
import sys
import time
import threading
from pathlib import Path

# Add the src directory to the path so we can import HSM modules
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from hpc_sweep_manager.core.hsm_config import HSMConfig
    from hpc_sweep_manager.core.remote_discovery import RemoteDiscovery
    from hpc_sweep_manager.core.remote_job_manager import RemoteJobManager
except ImportError as e:
    print(f"Error importing HSM modules: {e}")
    print("Make sure you're running this script from the HPC-Sweep-Manager directory")
    sys.exit(1)


class CancellationTester:
    """Test remote job cancellation functionality."""

    def __init__(self, remote_name: str):
        self.remote_name = remote_name
        self.remote_manager = None
        self.test_jobs = []

    async def setup(self):
        """Setup remote connection and job manager."""
        print(f"🔧 Setting up connection to {self.remote_name}...")

        # Load HSM config
        hsm_config = HSMConfig.load()
        if not hsm_config:
            print("❌ No hsm_config.yaml found")
            return False

        # Get remote configuration
        distributed_config = hsm_config.config_data.get("distributed", {})
        remotes = distributed_config.get("remotes", {})

        if self.remote_name not in remotes:
            print(f"❌ Remote '{self.remote_name}' not found in config")
            print("Available remotes:", list(remotes.keys()))
            return False

        # Discover remote configuration
        discovery = RemoteDiscovery(hsm_config.config_data)
        remote_info = remotes[self.remote_name].copy()
        remote_info["name"] = self.remote_name

        remote_config = await discovery.discover_remote_config(remote_info)
        if not remote_config:
            print(f"❌ Failed to discover remote configuration for {self.remote_name}")
            return False

        print(f"✅ Remote configuration discovered")

        # Create remote job manager
        test_sweep_dir = Path(f"debug_cancellation_{int(time.time())}")
        self.remote_manager = RemoteJobManager(
            remote_config, test_sweep_dir, max_parallel_jobs=2, show_progress=True
        )

        return True

    async def test_basic_cancellation(self):
        """Test basic job cancellation functionality."""
        print("\n🧪 Testing basic job cancellation...")

        # Setup remote environment
        setup_success = await self.remote_manager.setup_remote_environment(
            verify_sync=False, auto_sync=False
        )
        if not setup_success:
            print("❌ Failed to setup remote environment")
            return False

        # Submit a test job (long-running sleep command)
        test_params = {"test": "cancellation_debug"}
        job_id = await self.remote_manager.submit_single_job(
            test_params, "cancellation_test", "debug_sweep", "debug_group"
        )

        if not job_id:
            print("❌ Failed to submit test job")
            return False

        print(f"✅ Submitted test job: {job_id}")
        self.test_jobs.append(job_id)

        # Wait a bit for job to start
        await asyncio.sleep(3)

        # Check job status
        status = await self.remote_manager.get_job_status(job_id)
        print(f"📊 Job status: {status}")

        # Test cancellation
        print("🛑 Testing job cancellation...")
        cancel_success = await self.remote_manager.cancel_job(job_id)

        if cancel_success:
            print("✅ Job cancellation successful")
        else:
            print("❌ Job cancellation failed")

        # Verify job is no longer running
        await asyncio.sleep(2)
        final_status = await self.remote_manager.get_job_status(job_id)
        print(f"📊 Final job status: {final_status}")

        return cancel_success

    async def test_signal_handling(self):
        """Test signal-based cancellation."""
        print("\n🧪 Testing signal handling cancellation...")

        # Submit multiple test jobs
        for i in range(2):
            test_params = {"test": f"signal_test_{i}"}
            job_id = await self.remote_manager.submit_single_job(
                test_params, f"signal_test_{i}", "debug_sweep", "debug_group"
            )
            if job_id:
                self.test_jobs.append(job_id)
                print(f"✅ Submitted test job {i + 1}: {job_id}")

        await asyncio.sleep(3)

        print("📊 Current running jobs:")
        for job_id in self.test_jobs:
            status = await self.remote_manager.get_job_status(job_id)
            print(f"  - {job_id}: {status}")

        # Test signal handling by calling the cleanup method directly
        print("🛑 Testing signal-based cleanup...")
        try:
            await self.remote_manager._async_cleanup_on_signal()
            print("✅ Signal cleanup completed")
        except Exception as e:
            print(f"❌ Signal cleanup failed: {e}")

        return True

    async def cleanup(self):
        """Clean up any remaining test artifacts."""
        print("\n🧹 Cleaning up test artifacts...")

        if self.remote_manager:
            try:
                # Cancel any remaining jobs
                for job_id in self.test_jobs:
                    if job_id in self.remote_manager.running_jobs:
                        await self.remote_manager.cancel_job(job_id)

                # Cleanup remote environment
                await self.remote_manager.cleanup_remote_environment()
                print("✅ Cleanup completed")
            except Exception as e:
                print(f"⚠️ Cleanup warning: {e}")


async def main():
    """Main test function."""
    parser = argparse.ArgumentParser(description="Debug remote job cancellation")
    parser.add_argument("remote_name", help="Name of remote machine to test")
    parser.add_argument(
        "--test-signal", action="store_true", help="Test signal handling cancellation"
    )
    parser.add_argument(
        "--basic-only", action="store_true", help="Only run basic cancellation test"
    )

    args = parser.parse_args()

    print("🔍 Remote Job Cancellation Debug Tool")
    print("=" * 50)

    tester = CancellationTester(args.remote_name)

    try:
        # Setup
        setup_success = await tester.setup()
        if not setup_success:
            return 1

        # Run tests
        tests_passed = 0
        total_tests = 0

        # Basic cancellation test
        total_tests += 1
        basic_success = await tester.test_basic_cancellation()
        if basic_success:
            tests_passed += 1

        # Signal handling test (if requested and not basic-only)
        if args.test_signal and not args.basic_only:
            total_tests += 1
            signal_success = await tester.test_signal_handling()
            if signal_success:
                tests_passed += 1

        # Results
        print(f"\n📊 Test Results: {tests_passed}/{total_tests} passed")

        if tests_passed == total_tests:
            print("✅ All tests passed - cancellation functionality is working")
            return 0
        else:
            print("❌ Some tests failed - check error messages above")
            return 1

    except KeyboardInterrupt:
        print("\n🛑 Test interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        return 1
    finally:
        # Always cleanup
        await tester.cleanup()


if __name__ == "__main__":
    # Handle the fact that we might be in a different event loop context
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n🛑 Interrupted")
        sys.exit(1)
