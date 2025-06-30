"""
Unit tests for compute source creation and initialization.

These tests cover the specific issues encountered when running the simple_mlp example
and ensure that compute sources can be created correctly with proper constructor arguments.
"""

import pytest
from unittest.mock import patch, MagicMock

pytestmark = pytest.mark.unit

from hsm.cli.utils import create_compute_source, parse_compute_sources
from hsm.compute.base import ComputeSource, TaskStatus, HealthStatus
from hsm.compute.local import LocalComputeSource


class TestComputeSourceCreation:
    """Test compute source creation through CLI utilities."""

    def test_create_local_compute_source_basic(self):
        """Test creating a basic local compute source."""
        source = create_compute_source("local", {})

        assert isinstance(source, LocalComputeSource)
        assert source.name == "local-default"
        assert source.source_type == "local"
        assert source.max_parallel_tasks == 4  # default value

    def test_create_local_compute_source_with_config(self):
        """Test creating a local compute source with custom configuration."""
        config = {
            "name": "my-local",
            "max_concurrent_tasks": 8,
            "python_path": "/custom/python",
            "conda_env": "test-env",
        }

        source = create_compute_source("local", config)

        assert isinstance(source, LocalComputeSource)
        assert source.name == "my-local"
        assert source.source_type == "local"
        assert source.max_parallel_tasks == 8
        assert source.python_path == "/custom/python"
        assert source.conda_env == "test-env"

    @patch("hsm.compute.ssh.ASYNCSSH_AVAILABLE", True)
    def test_create_ssh_compute_source_basic(self):
        """Test creating a basic SSH compute source."""
        config = {"hostname": "example.com"}

        source = create_compute_source("ssh", config)

        assert source.name == "ssh-example.com"
        assert source.source_type == "ssh"
        assert source.max_concurrent_tasks == 4  # default value
        assert source.ssh_config.host == "example.com"

    @patch("hsm.compute.ssh.ASYNCSSH_AVAILABLE", True)
    def test_create_ssh_compute_source_with_config(self):
        """Test creating SSH compute source with custom configuration."""
        config = {
            "hostname": "cluster.example.com",
            "name": "my-ssh-source",
            "username": "testuser",
            "port": 2222,
            "max_concurrent_tasks": 16,
            "conda_env": "gpu-env",
        }

        source = create_compute_source("ssh", config)

        assert source.name == "my-ssh-source"
        assert source.source_type == "ssh"
        assert source.max_concurrent_tasks == 16
        assert source.ssh_config.host == "cluster.example.com"
        assert source.ssh_config.username == "testuser"
        assert source.ssh_config.port == 2222
        assert source.ssh_config.conda_env == "gpu-env"

    def test_create_ssh_compute_source_missing_hostname(self):
        """Test that SSH compute source creation fails without hostname."""
        with pytest.raises(ValueError, match="SSH compute source requires 'hostname'"):
            create_compute_source("ssh", {})

    @patch("shutil.which")
    def test_create_hpc_compute_source_basic(self, mock_which):
        """Test creating a basic HPC compute source."""
        # Mock slurm availability
        mock_which.side_effect = lambda cmd: cmd in ["sbatch", "squeue"]

        config = {"cluster": "my-cluster"}

        source = create_compute_source("hpc", config)

        assert source.name == "hpc-my-cluster"
        assert source.source_type == "hpc"
        assert source.max_parallel_tasks == 100  # default value
        assert source.cluster == "my-cluster"
        assert source.scheduler == "slurm"

    @patch("shutil.which")
    def test_create_hpc_compute_source_pbs(self, mock_which):
        """Test creating HPC compute source with PBS scheduler."""
        # Mock PBS availability (no slurm)
        mock_which.side_effect = lambda cmd: cmd in ["qsub", "qstat"]

        config = {"cluster": "pbs-cluster", "name": "my-hpc"}

        source = create_compute_source("hpc", config)

        assert source.name == "my-hpc"
        assert source.source_type == "hpc"
        assert source.cluster == "pbs-cluster"
        assert source.scheduler == "pbs"

    def test_create_unknown_compute_source(self):
        """Test that creating unknown compute source type fails."""
        with pytest.raises(ValueError, match="Unknown compute source type: unknown"):
            create_compute_source("unknown", {})


class TestParseComputeSources:
    """Test parsing compute source specifications."""

    def test_parse_local_source(self):
        """Test parsing local compute source."""
        sources = parse_compute_sources("local")

        assert len(sources) == 1
        assert sources[0] == ("local", {})

    def test_parse_ssh_source(self):
        """Test parsing SSH compute source."""
        sources = parse_compute_sources("ssh:example.com")

        assert len(sources) == 1
        assert sources[0] == ("ssh", {"hostname": "example.com"})

    def test_parse_hpc_source(self):
        """Test parsing HPC compute source."""
        sources = parse_compute_sources("hpc:my-cluster")

        assert len(sources) == 1
        assert sources[0] == ("hpc", {"cluster": "my-cluster"})

    def test_parse_multiple_sources(self):
        """Test parsing multiple compute sources."""
        sources = parse_compute_sources("local,ssh:example.com,hpc:cluster")

        assert len(sources) == 3
        assert sources[0] == ("local", {})
        assert sources[1] == ("ssh", {"hostname": "example.com"})
        assert sources[2] == ("hpc", {"cluster": "cluster"})

    def test_parse_unknown_source(self):
        """Test parsing unknown compute source fails."""
        with pytest.raises(ValueError, match="Unknown compute source type: unknown"):
            parse_compute_sources("unknown:config")

    def test_parse_unknown_simple_source(self):
        """Test parsing unknown simple source fails."""
        with pytest.raises(ValueError, match="Unknown compute source: unknown"):
            parse_compute_sources("unknown")


class TestComputeSourceConstructors:
    """Test that compute source constructors work correctly."""

    def test_local_compute_source_constructor(self):
        """Test LocalComputeSource constructor with proper parent initialization."""
        source = LocalComputeSource(
            name="test-local",
            max_concurrent_tasks=8,
            python_path="/usr/bin/python3",
            conda_env="test-env",
        )

        assert source.name == "test-local"
        assert source.source_type == "local"
        assert source.max_parallel_tasks == 8
        assert source.python_path == "/usr/bin/python3"
        assert source.conda_env == "test-env"
        assert source.health_check_interval == 300

    @patch("hsm.compute.ssh.ASYNCSSH_AVAILABLE", True)
    def test_ssh_compute_source_constructor(self):
        """Test SSHComputeSource constructor with proper parent initialization."""
        from hsm.compute.ssh import SSHComputeSource, SSHConfig

        ssh_config = SSHConfig(host="example.com", username="testuser", port=22)

        source = SSHComputeSource(name="test-ssh", ssh_config=ssh_config, max_concurrent_tasks=4)

        assert source.name == "test-ssh"
        assert source.source_type == "ssh"
        assert source.max_parallel_tasks == 4
        assert source.ssh_config.host == "example.com"
        assert source.health_check_interval == 300

    @patch("shutil.which")
    def test_hpc_compute_source_constructor(self, mock_which):
        """Test HPCComputeSource constructor with proper parent initialization."""
        from hsm.compute.hpc import HPCComputeSource

        # Mock slurm availability
        mock_which.side_effect = lambda cmd: cmd in ["sbatch", "squeue"]

        source = HPCComputeSource(cluster="test-cluster", max_concurrent_jobs=50, name="test-hpc")

        assert source.name == "test-hpc"
        assert source.source_type == "hpc"
        assert source.max_parallel_tasks == 50
        assert source.cluster == "test-cluster"
        assert source.scheduler == "slurm"
        assert source.health_check_interval == 300

    @patch("shutil.which")
    def test_hpc_compute_source_auto_name(self, mock_which):
        """Test HPCComputeSource auto-generates name when not provided."""
        from hsm.compute.hpc import HPCComputeSource

        # Mock PBS availability
        mock_which.side_effect = lambda cmd: cmd in ["qsub", "qstat"]

        source = HPCComputeSource(cluster="auto-cluster")

        assert source.name == "hpc-pbs-auto-cluster"
        assert source.source_type == "hpc"
        assert source.cluster == "auto-cluster"
        assert source.scheduler == "pbs"


class TestComputeSourceCreationErrorReproduction:
    """Test cases that reproduce the original error from simple_mlp example."""

    def test_reproduce_original_error_scenario(self):
        """Test reproducing the original 'missing source_type' error scenario."""
        # This test reproduces the error that would have occurred before the fix
        # The error was: "ComputeSource.__init__() missing 1 required positional argument: 'source_type'"

        # Before the fix, this would have failed
        source = create_compute_source("local", {})

        # After the fix, this should work correctly
        assert isinstance(source, LocalComputeSource)
        assert source.source_type == "local"
        assert hasattr(source, "name")
        assert hasattr(source, "max_parallel_tasks")

    def test_local_source_with_default_config(self):
        """Test creating local source with minimal config like in simple_mlp."""
        # This simulates the configuration parsing that happens in the CLI
        sources = parse_compute_sources("local")
        assert len(sources) == 1

        source_type, config = sources[0]
        assert source_type == "local"
        assert config == {}

        # This should not raise the "missing source_type" error
        source = create_compute_source(source_type, config)

        assert isinstance(source, LocalComputeSource)
        assert source.source_type == "local"
        assert source.name == "local-default"

    def test_multiple_source_creation(self):
        """Test creating multiple compute sources as would happen in a real sweep."""
        source_specs = parse_compute_sources("local")
        sources = []

        for source_type, config in source_specs:
            source = create_compute_source(source_type, config)
            sources.append(source)

        assert len(sources) == 1
        assert all(isinstance(s, ComputeSource) for s in sources)
        assert all(hasattr(s, "source_type") for s in sources)
        assert all(hasattr(s, "name") for s in sources)


class TestSimpleMlpScenario:
    """Test scenarios specific to the simple_mlp example."""

    def test_simple_mlp_compute_source_creation(self):
        """Test compute source creation as it would happen in simple_mlp."""
        # Simulate the command: hsm run --sources local
        sources_str = "local"

        # Parse sources
        source_specs = parse_compute_sources(sources_str)
        assert len(source_specs) == 1

        source_type, config = source_specs[0]
        assert source_type == "local"
        assert config == {}

        # Create compute source (this was failing before the fix)
        source = create_compute_source(source_type, config)

        # Verify it's properly configured
        assert isinstance(source, LocalComputeSource)
        assert source.source_type == "local"
        assert source.name.startswith("local-")
        assert source.max_parallel_tasks > 0
        assert source.health_check_interval > 0

    def test_simple_mlp_default_configuration(self):
        """Test that default configuration works for simple_mlp scenario."""
        source = LocalComputeSource()

        # Verify defaults are sensible
        assert source.name == "local"
        assert source.source_type == "local"
        assert source.max_parallel_tasks == 4
        assert source.timeout == 3600
        assert source.health_check_interval == 300

        # Verify the source is in a valid state
        assert not source._setup_complete  # Should start as not setup
        assert source.active_processes == {}
        assert source.task_futures == {}
