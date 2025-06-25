"""Unit tests for HPC job managers."""

from unittest.mock import MagicMock, patch

import pytest

from hpc_sweep_manager.core.hpc.hpc_base import HPCJobManager
from hpc_sweep_manager.core.hpc.pbs_manager import PBSJobManager
from hpc_sweep_manager.core.hpc.slurm_manager import SlurmJobManager


class TestHPCJobManagerBase:
    """Test HPCJobManager base class functionality."""

    def test_init_default(self):
        """Test initialization with default parameters."""
        # Can't instantiate abstract class directly, so we'll test via concrete implementations
        pass

    @pytest.mark.unit
    def test_auto_detect_pbs(self, mock_hpc_commands, monkeypatch):
        """Test auto-detection of PBS system."""

        def mock_which(cmd):
            if cmd in ["qsub", "qstat"]:
                return "/usr/bin/" + cmd
            return None

        def mock_run(*args, **kwargs):
            mock = MagicMock()
            mock.returncode = 0
            if "qstat" in str(args[0]) and "--version" in str(args[0]):
                mock.stdout = "qstat version 18.1.4"
            mock.stderr = ""
            return mock

        monkeypatch.setattr("shutil.which", mock_which)
        monkeypatch.setattr("subprocess.run", mock_run)

        manager = HPCJobManager.auto_detect()
        assert isinstance(manager, PBSJobManager)
        assert manager.scheduler_name == "pbs"

    @pytest.mark.unit
    def test_auto_detect_slurm(self, monkeypatch):
        """Test auto-detection of Slurm system."""

        def mock_which(cmd):
            if cmd in ["sbatch", "squeue", "sinfo"]:
                return "/usr/bin/" + cmd
            return None

        def mock_run(*args, **kwargs):
            mock = MagicMock()
            mock.returncode = 0
            if "sinfo" in str(args[0]) and "--version" in str(args[0]):
                mock.stdout = "slurm 20.11.7"
            mock.stderr = ""
            return mock

        monkeypatch.setattr("shutil.which", mock_which)
        monkeypatch.setattr("subprocess.run", mock_run)

        manager = HPCJobManager.auto_detect()
        assert isinstance(manager, SlurmJobManager)
        assert manager.scheduler_name == "slurm"

    @pytest.mark.unit
    def test_auto_detect_no_hpc(self, monkeypatch):
        """Test auto-detection when no HPC system is available."""

        def mock_which(cmd):
            return None

        monkeypatch.setattr("shutil.which", mock_which)

        with pytest.raises(RuntimeError, match="No supported HPC scheduler detected"):
            HPCJobManager.auto_detect()

    @pytest.mark.unit
    def test_auto_detect_with_params(self, mock_hpc_commands):
        """Test auto-detection with custom parameters."""
        manager = HPCJobManager.auto_detect(
            walltime="08:00:00", resources="select=2:ncpus=8:mem=32gb"
        )

        assert manager.walltime == "08:00:00"
        assert manager.resources == "select=2:ncpus=8:mem=32gb"


class TestPBSJobManager:
    """Test PBS job manager functionality."""

    @pytest.fixture
    def pbs_manager(self, mock_hpc_commands):
        """Create a PBS manager for testing."""
        return PBSJobManager(walltime="04:00:00", resources="select=1:ncpus=4:mem=16gb")

    def test_init(self, pbs_manager):
        """Test PBS manager initialization."""
        assert pbs_manager.scheduler_name == "pbs"
        assert pbs_manager.submit_command == "qsub"
        assert pbs_manager.status_command == "qstat"
        assert pbs_manager.walltime == "04:00:00"
        assert pbs_manager.resources == "select=1:ncpus=4:mem=16gb"

    @pytest.mark.unit
    def test_validate_system(self, pbs_manager, mock_hpc_commands):
        """Test PBS system validation."""
        # Should not raise exception with mocked commands
        pbs_manager._validate_system()

    @pytest.mark.unit
    def test_validate_system_missing_commands(self, monkeypatch):
        """Test validation failure when PBS commands are missing."""

        def mock_which(cmd):
            return None

        monkeypatch.setattr("shutil.which", mock_which)

        with pytest.raises(RuntimeError, match="PBS/Torque commands not found"):
            PBSJobManager()

    @pytest.mark.unit
    def test_submit_single_job(self, pbs_manager, temp_dir, sample_params, mock_hpc_commands):
        """Test submitting a single PBS job."""
        sweep_dir = temp_dir / "pbs_sweep"
        sweep_dir.mkdir()

        job_id = pbs_manager.submit_single_job(
            params=sample_params[0],
            job_name="test_job",
            sweep_dir=sweep_dir,
            sweep_id="pbs_test",
        )

        assert job_id == "12345.cluster"  # Mock job ID

    @pytest.mark.unit
    def test_submit_array_job(self, pbs_manager, temp_dir, sample_params, mock_hpc_commands):
        """Test submitting a PBS array job."""
        sweep_dir = temp_dir / "pbs_array_sweep"
        sweep_dir.mkdir()

        job_id = pbs_manager.submit_array_job(
            param_combinations=sample_params,
            sweep_id="pbs_array_test",
            sweep_dir=sweep_dir,
        )

        assert job_id == "12345.cluster"

    @pytest.mark.unit
    def test_get_job_status(self, pbs_manager, mock_hpc_commands):
        """Test getting PBS job status."""
        status = pbs_manager.get_job_status("12345.cluster")
        assert status in ["queued", "running", "completed", "failed", "unknown"]

    @pytest.mark.unit
    def test_submit_sweep_individual(self, pbs_manager, temp_dir, sample_params, mock_hpc_commands):
        """Test submitting sweep with individual jobs."""
        sweep_dir = temp_dir / "pbs_individual_sweep"
        sweep_dir.mkdir()

        job_ids = pbs_manager.submit_sweep(
            param_combinations=sample_params,
            mode="individual",
            sweep_dir=sweep_dir,
            sweep_id="pbs_individual_test",
        )

        assert len(job_ids) == len(sample_params)
        assert all(job_id == "12345.cluster" for job_id in job_ids)

    @pytest.mark.unit
    def test_submit_sweep_array(self, pbs_manager, temp_dir, sample_params, mock_hpc_commands):
        """Test submitting sweep with array job."""
        sweep_dir = temp_dir / "pbs_array_sweep"
        sweep_dir.mkdir()

        job_ids = pbs_manager.submit_sweep(
            param_combinations=sample_params,
            mode="array",
            sweep_dir=sweep_dir,
            sweep_id="pbs_array_test",
        )

        assert len(job_ids) == 1
        assert job_ids[0] == "12345.cluster"

    @pytest.mark.unit
    def test_submit_sweep_invalid_mode(self, pbs_manager, temp_dir, sample_params):
        """Test error handling for invalid submission mode."""
        with pytest.raises(ValueError, match="Unsupported submission mode"):
            pbs_manager.submit_sweep(
                param_combinations=sample_params,
                mode="invalid_mode",
                sweep_dir=temp_dir,
                sweep_id="test",
            )

    @pytest.mark.unit
    def test_params_to_string(self, pbs_manager):
        """Test parameter string conversion."""
        params = {
            "lr": 0.001,
            "batch_size": 32,
            "model_name": "transformer",
            "use_gpu": True,
            "layers": [128, 256, 128],
            "dropout": None,
        }

        param_str = pbs_manager._params_to_string(params)

        assert '"lr=0.001"' in param_str
        assert '"batch_size=32"' in param_str
        assert '"model_name=transformer"' in param_str
        assert '"use_gpu=true"' in param_str
        assert '"layers=[128, 256, 128]"' in param_str
        assert '"dropout=null"' in param_str


class TestSlurmJobManager:
    """Test Slurm job manager functionality."""

    @pytest.fixture
    def slurm_manager(self, monkeypatch):
        """Create a Slurm manager for testing."""

        def mock_which(cmd):
            if cmd in ["sbatch", "squeue"]:
                return "/usr/bin/" + cmd
            return None

        def mock_run(*args, **kwargs):
            mock = MagicMock()
            mock.returncode = 0
            if "sbatch" in str(args[0]):
                mock.stdout = "Submitted batch job 67890"
            elif "squeue" in str(args[0]):
                mock.stdout = "67890 RUNNING"
            mock.stderr = ""
            return mock

        monkeypatch.setattr("shutil.which", mock_which)
        monkeypatch.setattr("subprocess.run", mock_run)

        return SlurmJobManager(
            walltime="04:00:00", resources="--nodes=1 --ntasks-per-node=4 --mem=16G"
        )

    def test_init(self, slurm_manager):
        """Test Slurm manager initialization."""
        assert slurm_manager.scheduler_name == "slurm"
        assert slurm_manager.submit_command == "sbatch"
        assert slurm_manager.status_command == "squeue"
        assert slurm_manager.walltime == "04:00:00"
        assert slurm_manager.resources == "--nodes=1 --ntasks-per-node=4 --mem=16G"

    @pytest.mark.unit
    def test_validate_system(self, slurm_manager):
        """Test Slurm system validation."""
        # Should not raise exception with mocked commands
        slurm_manager._validate_system()

    @pytest.mark.unit
    def test_submit_single_job(self, slurm_manager, temp_dir, sample_params):
        """Test submitting a single Slurm job."""
        sweep_dir = temp_dir / "slurm_sweep"
        sweep_dir.mkdir()

        job_id = slurm_manager.submit_single_job(
            params=sample_params[0],
            job_name="test_job",
            sweep_dir=sweep_dir,
            sweep_id="slurm_test",
        )

        assert job_id == "67890"

    @pytest.mark.unit
    def test_submit_array_job(self, slurm_manager, temp_dir, sample_params):
        """Test submitting a Slurm array job."""
        sweep_dir = temp_dir / "slurm_array_sweep"
        sweep_dir.mkdir()

        job_id = slurm_manager.submit_array_job(
            param_combinations=sample_params,
            sweep_id="slurm_array_test",
            sweep_dir=sweep_dir,
        )

        assert job_id == "67890"

    @pytest.mark.unit
    def test_get_job_status(self, slurm_manager):
        """Test getting Slurm job status."""
        status = slurm_manager.get_job_status("67890")
        assert status in ["queued", "running", "completed", "failed", "unknown"]

    @pytest.mark.unit
    def test_custom_resources(self, monkeypatch):
        """Test Slurm with custom resource specifications."""

        def mock_which(cmd):
            if cmd in ["sbatch", "squeue", "sinfo"]:
                return "/usr/bin/" + cmd
            return None

        def mock_run(*args, **kwargs):
            mock = MagicMock()
            mock.returncode = 0
            if "sinfo" in str(args[0]) and "--version" in str(args[0]):
                mock.stdout = "slurm 20.11.7"
            mock.stderr = ""
            return mock

        monkeypatch.setattr("shutil.which", mock_which)
        monkeypatch.setattr("subprocess.run", mock_run)

        manager = SlurmJobManager(
            walltime="08:00:00", resources="--partition=gpu --nodes=2 --gres=gpu:v100:2"
        )

        assert manager.walltime == "08:00:00"
        assert "--partition=gpu" in manager.resources
        assert "--gres=gpu:v100:2" in manager.resources


class TestHPCIntegration:
    """Integration tests for HPC functionality."""

    @pytest.mark.integration
    def test_pbs_slurm_compatibility(self, mock_hpc_commands):
        """Test that PBS and Slurm managers have compatible interfaces."""
        pbs_manager = PBSJobManager()

        # Mock Slurm commands
        def mock_which_slurm(cmd):
            if cmd in ["sbatch", "squeue"]:
                return "/usr/bin/" + cmd
            return None

        with patch("shutil.which", mock_which_slurm):
            slurm_manager = SlurmJobManager()

        # Both should have the same interface
        assert hasattr(pbs_manager, "submit_single_job")
        assert hasattr(slurm_manager, "submit_single_job")
        assert hasattr(pbs_manager, "submit_array_job")
        assert hasattr(slurm_manager, "submit_array_job")
        assert hasattr(pbs_manager, "get_job_status")
        assert hasattr(slurm_manager, "get_job_status")

    @pytest.mark.integration
    def test_full_hpc_workflow(self, mock_hpc_commands, temp_dir, sample_params):
        """Test complete HPC workflow."""
        manager = HPCJobManager.auto_detect()
        sweep_dir = temp_dir / "hpc_integration_test"
        sweep_dir.mkdir()

        # Submit individual jobs
        individual_job_ids = manager.submit_sweep(
            param_combinations=sample_params[:2],
            mode="individual",
            sweep_dir=sweep_dir,
            sweep_id="hpc_individual_test",
        )

        assert len(individual_job_ids) == 2

        # Submit array job
        array_job_ids = manager.submit_sweep(
            param_combinations=sample_params,
            mode="array",
            sweep_dir=sweep_dir,
            sweep_id="hpc_array_test",
        )

        assert len(array_job_ids) == 1

        # Check job statuses
        for job_id in individual_job_ids + array_job_ids:
            status = manager.get_job_status(job_id)
            assert status in ["queued", "running", "completed", "failed", "unknown"]
