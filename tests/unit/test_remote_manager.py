"""Unit tests for the remote job manager."""

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hpc_sweep_manager.core.remote.remote_manager import ProjectStateChecker, RemoteJobManager


class MockRemoteConfig:
    """Mock remote configuration for testing."""

    def __init__(self, name="test_remote"):
        self.name = name
        self.host = "test.example.com"
        self.ssh_key = "~/.ssh/id_rsa"
        self.ssh_port = 22
        self.project_root = "/home/user/project"
        self.train_script = "train.py"
        self.python_interpreter = "python"
        self.config_dir = "configs"


class MockSSHConnection:
    """Mock SSH connection for testing."""

    def __init__(self, return_codes=None, outputs=None):
        self.return_codes = return_codes or {}
        self.outputs = outputs or {}
        self.commands_run = []

    async def run(self, command, check=True):
        """Mock SSH command execution."""
        self.commands_run.append(command)

        # Create mock result
        result = MagicMock()
        result.stdout = self.outputs.get(command, "")
        result.stderr = ""
        result.returncode = self.return_codes.get(command, 0)

        if check and result.returncode != 0:
            raise Exception(f"Command failed: {command}")

        return result

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class TestRemoteJobManager:
    """Test cases for RemoteJobManager."""

    @pytest.fixture
    def mock_remote_config(self):
        """Create a mock remote configuration."""
        return MockRemoteConfig()

    @pytest.fixture
    def temp_dir(self, tmp_path):
        """Create a temporary directory for tests."""
        return tmp_path

    @pytest.fixture
    def local_sweep_dir(self, temp_dir):
        """Create a local sweep directory."""
        sweep_dir = temp_dir / "local_sweep"
        sweep_dir.mkdir()
        return sweep_dir

    @pytest.fixture
    def remote_manager(self, mock_remote_config, local_sweep_dir):
        """Create a remote job manager for testing."""
        with patch("hpc_sweep_manager.core.remote.remote_manager.ASYNCSSH_AVAILABLE", True):
            manager = RemoteJobManager(
                remote_config=mock_remote_config,
                local_sweep_dir=local_sweep_dir,
                max_parallel_jobs=2,
                show_progress=False,
            )
            # Disable signal handlers for testing
            manager._setup_signal_handlers = MagicMock()
            return manager

    @pytest.fixture
    def sample_params(self):
        """Sample parameters for testing."""
        return [
            {"lr": 0.001, "batch_size": 16},
            {"lr": 0.001, "batch_size": 32},
            {"lr": 0.01, "batch_size": 16},
        ]

    @pytest.mark.unit
    def test_init(self, mock_remote_config, local_sweep_dir):
        """Test remote manager initialization."""
        with patch("hpc_sweep_manager.core.remote.remote_manager.ASYNCSSH_AVAILABLE", True):
            with patch.object(RemoteJobManager, "_setup_signal_handlers"):
                manager = RemoteJobManager(
                    remote_config=mock_remote_config,
                    local_sweep_dir=local_sweep_dir,
                    max_parallel_jobs=4,
                    show_progress=True,
                )

                assert manager.remote_config == mock_remote_config
                assert manager.local_sweep_dir == local_sweep_dir
                assert manager.system_type == "remote"
                assert manager.max_parallel_jobs == 4
                assert manager.show_progress is True
                assert manager.job_counter == 0
                assert len(manager.running_jobs) == 0

    @pytest.mark.unit
    def test_init_no_asyncssh(self, mock_remote_config, local_sweep_dir):
        """Test initialization fails without asyncssh."""
        with patch("hpc_sweep_manager.core.remote.remote_manager.ASYNCSSH_AVAILABLE", False):
            with pytest.raises(ImportError, match="asyncssh is required"):
                RemoteJobManager(
                    remote_config=mock_remote_config,
                    local_sweep_dir=local_sweep_dir,
                )

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_setup_remote_environment_success(self, remote_manager):
        """Test successful remote environment setup."""
        mock_conn = MockSSHConnection(
            return_codes={
                "test -e /home/user/project": 0,
                "test -e /home/user/project/train.py": 0,
                "python --version": 0,
            },
            outputs={
                "python --version": "Python 3.9.0",
            },
        )

        with patch(
            "hpc_sweep_manager.core.remote.remote_manager.create_ssh_connection",
            return_value=mock_conn,
        ):
            result = await remote_manager.setup_remote_environment(verify_sync=False)

        assert result is True
        assert remote_manager.remote_sweep_dir is not None
        assert remote_manager.remote_tasks_dir is not None
        assert "mkdir -p" in str(mock_conn.commands_run)

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_setup_remote_environment_validation_failure(self, remote_manager):
        """Test remote environment setup with validation failure."""
        mock_conn = MockSSHConnection(
            return_codes={
                "test -e /home/user/project": 1,  # Project root doesn't exist
                "test -e /home/user/project/train.py": 1,
                "python --version": 1,
            }
        )

        with patch(
            "hpc_sweep_manager.core.remote.remote_manager.create_ssh_connection",
            return_value=mock_conn,
        ):
            result = await remote_manager.setup_remote_environment(verify_sync=False)

        assert result is False

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_submit_single_job(self, remote_manager):
        """Test submitting a single job."""
        # Setup remote environment first
        remote_manager.remote_sweep_dir = "/remote/sweep"
        remote_manager.remote_tasks_dir = "/remote/sweep/tasks"

        # Use a job name that matches the expected pattern (task_NUMBER)
        job_name = "test_task_1"

        mock_conn = MockSSHConnection(
            outputs={
                "cat /remote/sweep/tasks/task_1/job.pid": "12345",
            }
        )

        params = {"lr": 0.001, "batch_size": 32}
        sweep_id = "test_sweep"

        with patch(
            "hpc_sweep_manager.core.remote.remote_manager.create_ssh_connection",
            return_value=mock_conn,
        ), patch(
            "hpc_sweep_manager.core.remote.remote_manager.asyncssh.scp", new_callable=AsyncMock
        ), patch("tempfile.NamedTemporaryFile") as mock_tempfile, patch("pathlib.Path.unlink"):
            mock_tempfile.return_value.__enter__.return_value.name = "/tmp/test_script.sh"

            job_id = await remote_manager.submit_single_job(
                params=params,
                job_name=job_name,
                sweep_id=sweep_id,
            )

        assert job_id.startswith("remote_test_remote_")
        assert job_id in remote_manager.running_jobs

        job_info = remote_manager.running_jobs[job_id]
        assert job_info["job_name"] == job_name
        assert job_info["params"] == params
        assert job_info["remote_pid"] == "12345"

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_get_job_status_running(self, remote_manager):
        """Test getting status of a running job."""
        # Add a mock running job
        job_id = "remote_test_remote_1"
        remote_manager.running_jobs[job_id] = {
            "job_name": "test_job",
            "task_dir": "/remote/sweep/tasks/task_1",
            "remote_pid": "12345",
        }

        mock_conn = MockSSHConnection(
            return_codes={
                "cat /remote/sweep/tasks/task_1/script.pid 2>/dev/null": 0,
                "cat /remote/sweep/tasks/task_1/python.pid 2>/dev/null": 0,
                "ps -p 12345": 0,  # Process is running
            },
            outputs={
                "cat /remote/sweep/tasks/task_1/script.pid 2>/dev/null": "12345",
                "cat /remote/sweep/tasks/task_1/python.pid 2>/dev/null": "12346",
            },
        )

        with patch(
            "hpc_sweep_manager.core.remote.remote_manager.create_ssh_connection",
            return_value=mock_conn,
        ):
            status = await remote_manager.get_job_status(job_id)

        assert status == "RUNNING"

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_get_job_status_completed(self, remote_manager):
        """Test getting status of a completed job."""
        job_id = "remote_test_remote_1"
        remote_manager.running_jobs[job_id] = {
            "job_name": "test_job",
            "task_dir": "/remote/sweep/tasks/task_1",
            "remote_pid": "12345",
        }

        mock_conn = MockSSHConnection(
            return_codes={
                "cat /remote/sweep/tasks/task_1/script.pid 2>/dev/null": 0,
                "cat /remote/sweep/tasks/task_1/python.pid 2>/dev/null": 0,
                "ps -p 12345": 1,  # Process not running
                "ps -p 12346": 1,  # Process not running
                "grep 'Status:' /remote/sweep/tasks/task_1/task_info.txt | tail -1": 0,
            },
            outputs={
                "cat /remote/sweep/tasks/task_1/script.pid 2>/dev/null": "12345",
                "cat /remote/sweep/tasks/task_1/python.pid 2>/dev/null": "12346",
                "grep 'Status:' /remote/sweep/tasks/task_1/task_info.txt | tail -1": "Status: COMPLETED",
            },
        )

        with patch(
            "hpc_sweep_manager.core.remote.remote_manager.create_ssh_connection",
            return_value=mock_conn,
        ):
            status = await remote_manager.get_job_status(job_id)

        assert status == "COMPLETED"

    @pytest.mark.unit
    def test_get_job_status_unknown(self, remote_manager):
        """Test getting status of unknown job."""
        status = asyncio.run(remote_manager.get_job_status("unknown_job"))
        assert status == "UNKNOWN"

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_cancel_job(self, remote_manager):
        """Test cancelling a job."""
        job_id = "remote_test_remote_1"
        remote_manager.running_jobs[job_id] = {
            "job_name": "test_job",
            "task_dir": "/remote/sweep/tasks/task_1",
            "remote_pid": "12345",
        }

        mock_conn = MockSSHConnection(
            return_codes={
                "cat /remote/sweep/tasks/task_1/script.pid 2>/dev/null": 0,
                "cat /remote/sweep/tasks/task_1/python.pid 2>/dev/null": 0,
                "ps -p 12345": 1,  # Process terminated after signal
                "ps -p 12346": 1,
            },
            outputs={
                "cat /remote/sweep/tasks/task_1/script.pid 2>/dev/null": "12345",
                "cat /remote/sweep/tasks/task_1/python.pid 2>/dev/null": "12346",
            },
        )

        with patch(
            "hpc_sweep_manager.core.remote.remote_manager.create_ssh_connection",
            return_value=mock_conn,
        ):
            result = await remote_manager.cancel_job(job_id)

        assert result is True
        assert job_id not in remote_manager.running_jobs

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_cancel_all_jobs(self, remote_manager):
        """Test cancelling all jobs."""
        # Add some mock jobs
        remote_manager.running_jobs = {
            "job1": {"job_name": "test1", "task_dir": "/remote/tasks/task_1"},
            "job2": {"job_name": "test2", "task_dir": "/remote/tasks/task_2"},
        }

        with patch.object(
            remote_manager, "_cancel_job_with_result", new_callable=AsyncMock
        ) as mock_cancel:
            mock_cancel.return_value = True

            results = await remote_manager.cancel_all_jobs()

        assert results["cancelled"] == 2
        assert results["failed"] == 0
        assert mock_cancel.call_count == 2

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_collect_results(self, remote_manager, local_sweep_dir):
        """Test collecting results from remote."""
        remote_manager.remote_tasks_dir = "/remote/sweep/tasks"

        with patch("subprocess.run") as mock_subprocess:
            mock_subprocess.return_value.returncode = 0

            result = await remote_manager.collect_results()

        assert result is True
        mock_subprocess.assert_called_once()

        # Check that rsync command was constructed correctly
        rsync_cmd = mock_subprocess.call_args[0][0]
        assert "rsync" in rsync_cmd
        assert "/remote/sweep/tasks/" in rsync_cmd

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_cleanup_remote_environment(self, remote_manager):
        """Test cleaning up remote environment."""
        remote_manager.remote_sweep_dir = "/remote/sweep"

        mock_conn = MockSSHConnection()

        with patch(
            "hpc_sweep_manager.core.remote.remote_manager.create_ssh_connection",
            return_value=mock_conn,
        ):
            await remote_manager.cleanup_remote_environment()

        assert "rm -rf /remote/sweep" in mock_conn.commands_run
        assert remote_manager.remote_sweep_dir is None

    @pytest.mark.unit
    def test_params_to_string(self, remote_manager):
        """Test parameter conversion to string."""
        params = {
            "lr": 0.001,
            "batch_size": 32,
            "model_type": "transformer",
            "layers": [128, 256, 128],
            "dropout": None,
            "use_attention": True,
            "config_file": "path with spaces.yaml",
        }

        result = remote_manager._params_to_string(params)

        assert '"lr=0.001"' in result
        assert '"batch_size=32"' in result
        assert '"model_type=transformer"' in result
        assert '"layers=[128, 256, 128]"' in result
        assert '"dropout=null"' in result
        assert '"use_attention=true"' in result
        assert '"config_file=path with spaces.yaml"' in result

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_submit_sweep(self, remote_manager, sample_params):
        """Test submitting a complete sweep."""
        remote_manager.remote_sweep_dir = "/remote/sweep"
        remote_manager.remote_tasks_dir = "/remote/sweep/tasks"

        with patch.object(
            remote_manager, "submit_single_job", new_callable=AsyncMock
        ) as mock_submit, patch.object(remote_manager, "wait_for_all_jobs", new_callable=AsyncMock):
            mock_submit.side_effect = [f"job_{i}" for i in range(len(sample_params))]

            job_ids = await remote_manager.submit_sweep(
                param_combinations=sample_params,
                sweep_id="test_sweep",
            )

        assert len(job_ids) == len(sample_params)
        assert mock_submit.call_count == len(sample_params)
        assert remote_manager.total_jobs_planned == len(sample_params)

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_get_running_jobs_info(self, remote_manager):
        """Test getting running jobs information."""
        # Add some mock jobs
        remote_manager.running_jobs = {
            "job1": {
                "job_name": "test1",
                "start_time": datetime.now(),
            },
            "job2": {
                "job_name": "test2",
                "start_time": datetime.now(),
            },
        }

        with patch.object(remote_manager, "get_job_status", new_callable=AsyncMock) as mock_status:
            mock_status.side_effect = ["RUNNING", "COMPLETED"]

            info = await remote_manager.get_running_jobs_info()

        assert info["remote_name"] == "test_remote"
        assert info["total_jobs"] == 2
        assert info["running_jobs"] == 1
        assert info["completed_jobs"] == 1
        assert len(info["job_details"]) == 2


class TestProjectStateChecker:
    """Test cases for ProjectStateChecker."""

    @pytest.fixture
    def temp_dir(self, tmp_path):
        """Create a temporary directory for tests."""
        return tmp_path

    @pytest.fixture
    def mock_remote_config(self):
        """Create a mock remote configuration."""
        return MockRemoteConfig()

    @pytest.fixture
    def project_checker(self, temp_dir, mock_remote_config):
        """Create a project state checker."""
        return ProjectStateChecker(str(temp_dir), mock_remote_config)

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_verify_project_sync(self, project_checker, temp_dir):
        """Test project sync verification."""
        # Create test files
        (temp_dir / "train.py").write_text("print('training script')")
        (temp_dir / "configs").mkdir()
        (temp_dir / "configs" / "config.yaml").write_text("model: test")

        mock_conn = MockSSHConnection(
            outputs={
                # Mock file existence checks
                f"[ -f '/home/user/project/train.py' ] && echo 'exists' || echo 'missing'": "exists",
                f"[ -f '/home/user/project/configs/config.yaml' ] && echo 'exists' || echo 'missing'": "exists",
            }
        )

        # Mock verify_and_enforce_sync to return in-sync result
        with patch.object(project_checker, "verify_and_enforce_sync") as mock_verify:
            mock_verify.return_value = {
                "in_sync": True,
                "warnings": [],
                "errors": [],
                "files_checked": ["train.py", "configs/config.yaml"],
            }

            result = await project_checker.verify_project_sync(mock_conn)

        assert result["in_sync"] is True
        assert "details" in result

    @pytest.mark.unit
    def test_calculate_file_checksum(self, project_checker, temp_dir):
        """Test file checksum calculation."""
        test_file = temp_dir / "test.txt"
        test_file.write_text("test content")

        checksum = project_checker._calculate_file_checksum(test_file)

        assert isinstance(checksum, str)
        assert len(checksum) == 64  # SHA256 hex digest length
