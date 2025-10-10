"""Unit tests for LocalJobManager class."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from hpc_sweep_manager.core.local.local_manager import LocalJobManager


class TestLocalJobManager:
    """Test LocalJobManager functionality."""

    @pytest.fixture
    def local_manager(self):
        """Create a local manager for testing."""
        return LocalJobManager(max_parallel_jobs=2, show_progress=False, show_output=False)

    def test_init_default(self):
        """Test LocalJobManager initialization with defaults."""
        manager = LocalJobManager()
        assert manager.system_type == "local"
        assert manager.max_parallel_jobs == 1
        assert manager.show_progress is True
        assert manager.show_output is False

    def test_init_with_params(self, temp_dir):
        """Test LocalJobManager initialization with custom parameters."""
        manager = LocalJobManager(
            project_dir=str(temp_dir), max_parallel_jobs=4, show_progress=False, show_output=True
        )
        assert manager.project_dir == str(temp_dir)
        assert manager.max_parallel_jobs == 4
        assert manager.show_progress is False
        assert manager.show_output is True

    @pytest.mark.unit
    def test_params_to_string(self, local_manager):
        """Test parameter string conversion."""
        params = {
            "lr": 0.001,
            "batch_size": 32,
            "model_name": "transformer",
            "use_gpu": True,
            "layers": [128, 256, 128],
            "dropout": None,
        }

        param_str = local_manager._params_to_string(params)

        assert '"lr=0.001"' in param_str
        assert '"batch_size=32"' in param_str
        assert '"model_name=transformer"' in param_str
        assert '"use_gpu=true"' in param_str
        assert '"layers=[128, 256, 128]"' in param_str
        assert '"dropout=null"' in param_str

    @pytest.mark.unit
    def test_submit_single_job(self, local_manager, temp_dir, sample_params):
        """Test submitting a single local job."""
        sweep_dir = temp_dir / "local_sweep"
        sweep_dir.mkdir()

        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.pid = 12345
            mock_process.poll.return_value = None  # Still running
            mock_popen.return_value = mock_process

            job_id = local_manager.submit_single_job(
                params=sample_params[0],
                job_name="test_job",
                sweep_dir=sweep_dir,
                sweep_id="local_test",
            )

            assert job_id.startswith("local_local_test_")
            assert job_id in local_manager.running_processes
            assert local_manager.running_processes[job_id]["job_name"] == "test_job"

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_submit_array_job(self, local_manager, temp_dir, sample_params):
        """Test submitting a local array job."""
        sweep_dir = temp_dir / "local_array_sweep"
        sweep_dir.mkdir()

        local_manager.total_jobs_planned = len(sample_params)

        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.pid = 12345
            mock_process.poll.return_value = 0  # Completed immediately for test
            mock_popen.return_value = mock_process

            # Mock the wait_for_all_jobs method to avoid actually waiting
            with patch.object(local_manager, "wait_for_all_jobs"):
                job_id = await local_manager.submit_array_job(
                    param_combinations=sample_params,
                    sweep_id="local_array_test",
                    sweep_dir=sweep_dir,
                )

                assert job_id == "local_array_local_array_test"

    @pytest.mark.unit
    def test_get_job_status(self, local_manager, temp_dir):
        """Test getting job status."""
        # Test unknown job
        status = local_manager.get_job_status("unknown_job")
        assert status == "UNKNOWN"

        # Test array job
        status = local_manager.get_job_status("local_array_test")
        assert status == "COMPLETED"  # No running processes

        # Test running job
        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.poll.return_value = None  # Still running
            mock_popen.return_value = mock_process

            sweep_dir = temp_dir / "test_sweep"
            sweep_dir.mkdir()

            job_id = local_manager.submit_single_job(
                params={"lr": 0.001},
                job_name="test_job",
                sweep_dir=sweep_dir,
                sweep_id="test",
            )

            status = local_manager.get_job_status(job_id)
            assert status == "RUNNING"

            # Test completed job
            mock_process.poll.return_value = 0  # Process finished
            mock_process.returncode = 0  # Exit code 0 = success
            status = local_manager.get_job_status(job_id)
            assert status == "COMPLETED"

            # Test failed job
            mock_process.poll.return_value = 1  # Process finished
            mock_process.returncode = 1  # Exit code 1 = failure
            status = local_manager.get_job_status(job_id)
            assert status == "FAILED"

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_submit_sweep_individual(self, local_manager, temp_dir, sample_params):
        """Test submitting sweep with individual jobs."""
        sweep_dir = temp_dir / "individual_sweep"
        sweep_dir.mkdir()

        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.pid = 12345
            mock_process.poll.return_value = None
            mock_popen.return_value = mock_process

            job_ids = await local_manager.submit_sweep(
                param_combinations=sample_params,
                mode="individual",
                sweep_dir=sweep_dir,
                sweep_id="individual_test",
            )

            assert len(job_ids) == len(sample_params)
            assert all(job_id.startswith("local_individual_test_") for job_id in job_ids)

    @pytest.mark.unit
    @pytest.mark.anyio
    async def test_submit_sweep_array(self, local_manager, temp_dir, sample_params):
        """Test submitting sweep with array job."""
        sweep_dir = temp_dir / "array_sweep"
        sweep_dir.mkdir()

        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.poll.return_value = 0  # Completed
            mock_popen.return_value = mock_process

            with patch.object(local_manager, "wait_for_all_jobs"):
                job_ids = await local_manager.submit_sweep(
                    param_combinations=sample_params,
                    mode="array",
                    sweep_dir=sweep_dir,
                    sweep_id="array_test",
                )

                assert len(job_ids) == 1
                assert job_ids[0] == "local_array_array_test"

    @pytest.mark.unit
    def test_cancel_job(self, local_manager, temp_dir):
        """Test job cancellation."""
        with patch("subprocess.Popen") as mock_popen, patch("os.killpg") as mock_killpg, patch(
            "os.getpgid"
        ) as mock_getpgid, patch("time.sleep") as mock_sleep:
            mock_process = MagicMock()
            mock_process.pid = 12345
            mock_process.returncode = 0

            # Create a callable that simulates process termination after first call
            poll_count = [0]

            def mock_poll():
                poll_count[0] += 1
                if poll_count[0] == 1:
                    return None  # Running on first check
                else:
                    return 0  # Terminated afterwards

            mock_process.poll = mock_poll
            mock_popen.return_value = mock_process
            mock_getpgid.return_value = 12345

            sweep_dir = temp_dir / "test_sweep"
            sweep_dir.mkdir()

            job_id = local_manager.submit_single_job(
                params={"lr": 0.001},
                job_name="test_job",
                sweep_dir=sweep_dir,
                sweep_id="test",
            )

            result = local_manager.cancel_job(job_id)
            assert result is True
            assert job_id not in local_manager.running_processes

    @pytest.mark.unit
    def test_cancel_all_jobs(self, local_manager, temp_dir, sample_params):
        """Test cancelling all jobs."""
        sweep_dir = temp_dir / "cancel_test_sweep"
        sweep_dir.mkdir()

        with patch("subprocess.Popen") as mock_popen, patch("os.killpg") as mock_killpg, patch(
            "os.getpgid"
        ) as mock_getpgid, patch("time.sleep") as mock_sleep:
            # Create separate mocks for each job
            mock_processes = []
            for i in range(2):
                mock_process = MagicMock()
                mock_process.pid = 12345 + i
                mock_process.returncode = 0

                # Create a stateful poll function for each process
                poll_count = [0]

                def make_poll_func():
                    count = [0]

                    def mock_poll():
                        count[0] += 1
                        if count[0] == 1:
                            return None  # Running on first check
                        else:
                            return 0  # Terminated afterwards

                    return mock_poll

                mock_process.poll = make_poll_func()
                mock_processes.append(mock_process)

            mock_popen.side_effect = mock_processes
            mock_getpgid.side_effect = [12345, 12346]  # Different pgids for each process

            # Submit some jobs
            for i, params in enumerate(sample_params[:2]):
                local_manager.submit_single_job(
                    params=params,
                    job_name=f"test_job_{i}",
                    sweep_dir=sweep_dir,
                    sweep_id="test",
                )

            results = local_manager.cancel_all_jobs()

            assert "cancelled" in results
            assert "failed" in results
            assert len(local_manager.running_processes) == 0

    @pytest.mark.unit
    def test_get_running_process_info(self, local_manager, temp_dir):
        """Test getting running process information."""
        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.pid = 12345
            mock_process.poll.return_value = None
            mock_popen.return_value = mock_process

            sweep_dir = temp_dir / "info_test_sweep"
            sweep_dir.mkdir()

            job_id = local_manager.submit_single_job(
                params={"lr": 0.001},
                job_name="test_job",
                sweep_dir=sweep_dir,
                sweep_id="test",
            )

            info = local_manager.get_running_process_info()

            assert info["total_running"] == 1
            assert info["max_parallel"] == local_manager.max_parallel_jobs
            assert job_id in info["running_jobs"]
            assert info["running_jobs"][job_id]["job_name"] == "test_job"
            assert info["running_jobs"][job_id]["pid"] == 12345

    @pytest.mark.integration
    def test_path_validation(self, temp_dir):
        """Test path validation and fixing."""
        # Test with valid project directory
        manager = LocalJobManager(project_dir=str(temp_dir))
        assert Path(manager.project_dir).exists()

        # Test with invalid project directory
        with patch("builtins.print") as mock_print:
            manager = LocalJobManager(project_dir="/nonexistent/path")
            mock_print.assert_called()
            # Should fallback to current directory
            assert Path(manager.project_dir).exists()
