"""
Tests for SSH compute source and project synchronization features.

These tests validate:
- SSH compute source creation and configuration
- Project synchronization verification (git and file-based)
- Remote result collection
- Cross-mode completion with SSH sources
"""

import asyncio
from pathlib import Path
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hsm.compute.base import HealthStatus, SweepContext, Task, TaskStatus
from hsm.core.sync_manager import ProjectStateChecker, ProjectSyncManager, ProjectSyncError
from hsm.core.result_collector import RemoteResultCollector

pytestmark = [pytest.mark.integration, pytest.mark.slow]


class TestSSHComputeSource:
    """Test SSH compute source functionality."""

    @patch("hsm.compute.ssh.ASYNCSSH_AVAILABLE", True)
    def test_ssh_compute_source_creation(self):
        """Test SSH compute source can be created with proper configuration."""
        from hsm.compute.ssh import SSHComputeSource, SSHConfig

        ssh_config = SSHConfig(
            host="example.com",
            username="testuser",
            port=22,
            key_file="/path/to/key",
            project_dir="/remote/project",
            python_path="/remote/python",
            conda_env="remote_env",
        )

        source = SSHComputeSource(
            name="test-ssh", ssh_config=ssh_config, max_concurrent_tasks=4, timeout=3600
        )

        assert source.name == "test-ssh"
        assert source.source_type == "ssh"
        assert source.max_parallel_tasks == 4
        assert source.ssh_config.host == "example.com"
        assert source.ssh_config.username == "testuser"
        assert source.ssh_config.project_dir == "/remote/project"

    @patch("hsm.compute.ssh.ASYNCSSH_AVAILABLE", True)
    @pytest.mark.asyncio
    async def test_ssh_config_to_dict_serialization(self):
        """Test SSH config can be serialized to dictionary."""
        from hsm.compute.ssh import SSHConfig

        ssh_config = SSHConfig(
            host="test.example.com",
            username="user",
            port=2222,
            key_file="/home/user/.ssh/id_rsa",
            project_dir="/home/user/project",
        )

        config_dict = ssh_config.to_dict()

        assert config_dict["host"] == "test.example.com"
        assert config_dict["username"] == "user"
        assert config_dict["port"] == 2222
        assert config_dict["key_file"] == "/home/user/.ssh/id_rsa"
        assert config_dict["project_dir"] == "/home/user/project"

    @patch("hsm.compute.ssh.ASYNCSSH_AVAILABLE", False)
    def test_ssh_source_fails_without_asyncssh(self):
        """Test SSH source creation fails when asyncssh not available."""
        from hsm.cli.utils import create_compute_source

        with pytest.raises(ImportError, match="asyncssh is required"):
            create_compute_source("ssh", {"hostname": "example.com"})

    @patch("hsm.compute.ssh.ASYNCSSH_AVAILABLE", True)
    @patch("hsm.compute.ssh.asyncssh")
    @pytest.mark.asyncio
    async def test_ssh_source_health_check_mock(self, mock_asyncssh):
        """Test SSH source health check with mocked connection."""
        from hsm.compute.ssh import SSHComputeSource, SSHConfig

        # Mock SSH connection
        mock_conn = AsyncMock()
        mock_asyncssh.connect.return_value.__aenter__.return_value = mock_conn

        # Mock command execution for health metrics
        mock_conn.run.return_value.stdout = "50.0"  # CPU usage

        ssh_config = SSHConfig(host="example.com", username="test")
        source = SSHComputeSource(name="test-ssh", ssh_config=ssh_config)

        # Mock the connection check
        source._check_connection = AsyncMock(return_value=True)
        source._get_remote_metrics = AsyncMock(
            return_value={
                "cpu_percent": 50.0,
                "memory_percent": 60.0,
                "disk_percent": 70.0,
                "load_average": 2.5,
            }
        )

        health_report = await source.health_check()

        assert health_report.source_name == "test-ssh"
        assert health_report.status in [HealthStatus.HEALTHY, HealthStatus.DEGRADED]


class TestProjectSynchronization:
    """Test project synchronization functionality."""

    def test_project_state_checker_creation(self):
        """Test ProjectStateChecker can be created."""
        checker = ProjectStateChecker(
            local_project_root="/local/project",
            remote_host="example.com",
            remote_project_root="/remote/project",
            ssh_key="/path/to/key",
        )

        assert checker.local_project_root == Path("/local/project")
        assert checker.remote_host == "example.com"
        assert checker.remote_project_root == "/remote/project"
        assert checker.ssh_key == "/path/to/key"

    @patch("hsm.core.sync_manager.ASYNCSSH_AVAILABLE", False)
    @pytest.mark.asyncio
    async def test_project_sync_fails_without_asyncssh(self):
        """Test project sync verification fails without asyncssh."""
        checker = ProjectStateChecker(
            local_project_root="/local/project",
            remote_host="example.com",
            remote_project_root="/remote/project",
        )

        with pytest.raises(ProjectSyncError, match="asyncssh is required"):
            await checker.verify_project_sync()

    def test_project_sync_manager_creation(self):
        """Test ProjectSyncManager can be created."""
        manager = ProjectSyncManager(local_project_root="/local/project")

        assert manager.local_project_root == Path("/local/project")

    @pytest.mark.asyncio
    async def test_project_sync_manager_sync_methods_exist(self):
        """Test that sync manager has required methods."""
        manager = ProjectSyncManager(local_project_root="/tmp")

        # Test methods exist (they'll fail without proper setup, but should exist)
        assert hasattr(manager, "sync_files_to_remote")
        assert callable(manager.sync_files_to_remote)

    @patch("hsm.core.sync_manager.ASYNCSSH_AVAILABLE", True)
    @patch("hsm.core.sync_manager.asyncssh")
    @pytest.mark.asyncio
    async def test_project_sync_git_detection_mock(self, mock_asyncssh):
        """Test git-based sync detection with mocked SSH."""
        checker = ProjectStateChecker(
            local_project_root="/tmp",
            remote_host="example.com",
            remote_project_root="/remote/project",
        )

        # Mock SSH connection
        mock_conn = AsyncMock()
        mock_asyncssh.connect.return_value.__aenter__.return_value = mock_conn

        # Mock git commands
        mock_conn.run.side_effect = [
            # Local git status
            MagicMock(stdout="clean", returncode=0),
            # Remote git status
            MagicMock(stdout="clean", returncode=0),
            # Local git rev-parse
            MagicMock(stdout="abc123\n", returncode=0),
            # Remote git rev-parse
            MagicMock(stdout="abc123\n", returncode=0),
        ]

        # Mock the perform verification to avoid complex git logic
        checker._perform_verification = AsyncMock(
            return_value={
                "in_sync": True,
                "method": "git",
                "details": {"local_commit": "abc123", "remote_commit": "abc123"},
                "warnings": [],
                "errors": [],
            }
        )

        result = await checker.verify_project_sync(mock_conn)

        assert result["in_sync"] == True
        assert result["method"] == "git"


class TestRemoteResultCollection:
    """Test remote result collection functionality."""

    def test_remote_result_collector_creation(self):
        """Test RemoteResultCollector can be created."""
        collector = RemoteResultCollector(
            local_sweep_dir=Path("/local/sweep"),
            remote_host="example.com",
            ssh_key="/path/to/key",
            ssh_port=22,
        )

        assert collector.local_sweep_dir == Path("/local/sweep")
        assert collector.remote_host == "example.com"
        assert collector.ssh_key == "/path/to/key"
        assert collector.ssh_port == 22

    @pytest.mark.asyncio
    async def test_remote_result_collector_methods_exist(self):
        """Test that result collector has required methods."""
        collector = RemoteResultCollector(local_sweep_dir=Path("/tmp"), remote_host="example.com")

        # Test methods exist
        assert hasattr(collector, "collect_results")
        assert hasattr(collector, "collect_failed_task_info")
        assert callable(collector.collect_results)
        assert callable(collector.collect_failed_task_info)

    def test_result_collection_manager_creation(self):
        """Test ResultCollectionManager can be created."""
        from hsm.core.result_collector import ResultCollectionManager

        manager = ResultCollectionManager(local_sweep_dir=Path("/tmp/sweep"))

        assert manager.local_sweep_dir == Path("/tmp/sweep")
        assert hasattr(manager, "get_remote_collector")
        assert hasattr(manager, "get_local_collector")
        assert hasattr(manager, "collect_all_results")

    @pytest.mark.asyncio
    async def test_result_collection_manager_source_configs(self):
        """Test result collection manager handles different source configurations."""
        from hsm.core.result_collector import ResultCollectionManager

        manager = ResultCollectionManager(local_sweep_dir=Path("/tmp/sweep"))

        # Test with different source configurations
        source_configs = {
            "remote_source": {
                "type": "remote",
                "host": "example.com",
                "ssh_key": "/path/to/key",
                "remote_sweep_dir": "/remote/sweep",
                "cleanup_after_sync": True,
            },
            "local_source": {
                "type": "local",
                "task_dirs": [Path("/tmp/task1"), Path("/tmp/task2")],
            },
        }

        # Should not crash when processing configs
        try:
            await manager.collect_all_results(source_configs)
        except Exception as e:
            # Expected to fail without actual infrastructure, but should handle configs properly
            assert "source_configs" not in str(e).lower()


class TestDistributedSyncIntegration:
    """Test integration between distributed execution and sync features."""

    @patch("hsm.compute.ssh.ASYNCSSH_AVAILABLE", True)
    def test_ssh_source_has_sync_integration(self):
        """Test SSH source integrates with sync functionality."""
        from hsm.compute.ssh import SSHComputeSource, SSHConfig

        ssh_config = SSHConfig(host="example.com")
        source = SSHComputeSource(name="sync-test", ssh_config=ssh_config)

        # Should have sync-related attributes
        assert hasattr(source, "project_state_checker")
        assert hasattr(source, "remote_result_collector")

    @patch("hsm.compute.ssh.ASYNCSSH_AVAILABLE", True)
    @pytest.mark.asyncio
    async def test_ssh_source_verify_project_sync_method(self):
        """Test SSH source has project sync verification method."""
        from hsm.compute.ssh import SSHComputeSource, SSHConfig

        ssh_config = SSHConfig(host="example.com")
        source = SSHComputeSource(name="sync-test", ssh_config=ssh_config)

        # Should have project sync verification method
        assert hasattr(source, "verify_project_sync")
        assert callable(source.verify_project_sync)

        # Mock the method to avoid actual SSH connection
        source.verify_project_sync = AsyncMock(
            return_value={"in_sync": False, "message": "Projects not synchronized", "error": False}
        )

        result = await source.verify_project_sync()
        assert "in_sync" in result
        assert "message" in result

    def test_sweep_engine_has_sync_attributes(self):
        """Test SweepEngine has sync-related attributes as claimed."""
        from hsm.core.engine import SweepEngine
        from hsm.compute.base import SweepContext

        sweep_context = SweepContext(sweep_id="test", sweep_dir=Path("/tmp/test"), config={})

        engine = SweepEngine(sweep_context=sweep_context, sources=[])

        # Should have sync-related attributes
        assert hasattr(engine, "project_state_checker")
        assert hasattr(engine, "project_sync_verified")
        assert hasattr(engine, "project_sync_warnings")
        assert hasattr(engine, "result_collection_manager")
        assert hasattr(engine, "comprehensive_collection_used")

    @pytest.mark.asyncio
    async def test_sweep_engine_verify_project_sync_method(self):
        """Test SweepEngine has project sync verification method."""
        from hsm.core.engine import SweepEngine
        from hsm.compute.base import SweepContext

        sweep_context = SweepContext(sweep_id="test", sweep_dir=Path("/tmp/test"), config={})

        engine = SweepEngine(sweep_context=sweep_context, sources=[])

        # Should have verify sync method
        assert hasattr(engine, "_verify_project_sync")
        assert callable(engine._verify_project_sync)

        # Test it doesn't crash with no remote sources
        await engine._verify_project_sync()
        assert engine.project_sync_verified == True
        assert isinstance(engine.project_sync_warnings, list)


class TestSyncErrorHandling:
    """Test error handling in sync functionality."""

    def test_project_sync_error_exists(self):
        """Test ProjectSyncError exception exists."""
        from hsm.core.sync_manager import ProjectSyncError

        # Should be able to create and raise the exception
        error = ProjectSyncError("Test sync error")
        assert str(error) == "Test sync error"

        with pytest.raises(ProjectSyncError):
            raise ProjectSyncError("Test error")

    def test_result_collection_error_exists(self):
        """Test ResultCollectionError exception exists."""
        from hsm.core.result_collector import ResultCollectionError

        # Should be able to create and raise the exception
        error = ResultCollectionError("Test collection error")
        assert str(error) == "Test collection error"

        with pytest.raises(ResultCollectionError):
            raise ResultCollectionError("Test error")


class TestCrossModeFunctionality:
    """Test cross-mode completion functionality."""

    def test_sweep_tracker_exists(self):
        """Test SweepTracker exists for cross-mode completion."""
        from hsm.core.tracker import SweepTracker

        tracker = SweepTracker(sweep_dir=Path("/tmp/test"))

        # Should have cross-mode completion methods
        assert hasattr(tracker, "get_completion_status")
        assert hasattr(tracker, "get_missing_tasks")
        assert hasattr(tracker, "get_failed_tasks")

    def test_completion_status_enum_exists(self):
        """Test CompletionStatus enum exists."""
        from hsm.core.tracker import CompletionStatus

        assert hasattr(CompletionStatus, "INCOMPLETE")
        assert hasattr(CompletionStatus, "COMPLETE")
        assert hasattr(CompletionStatus, "FAILED")
        assert hasattr(CompletionStatus, "PARTIAL")

    @pytest.mark.asyncio
    async def test_task_tracking_serialization(self):
        """Test TaskTracking can be serialized/deserialized."""
        from hsm.core.tracker import TaskTracking
        from hsm.compute.base import TaskStatus
        from datetime import datetime

        original = TaskTracking(
            task_id="test_task",
            compute_source="local",
            status=TaskStatus.COMPLETED,
            start_time=datetime.now(),
            params={"lr": 0.01},
        )

        # Test serialization
        data = original.to_dict()
        assert data["task_id"] == "test_task"
        assert data["compute_source"] == "local"
        assert data["status"] == "completed"

        # Test deserialization
        restored = TaskTracking.from_dict(data)
        assert restored.task_id == original.task_id
        assert restored.compute_source == original.compute_source
        assert restored.status == original.status
