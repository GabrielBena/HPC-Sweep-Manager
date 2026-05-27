"""Unit tests for build_ssh_source + parse_gpus_arg (B2-part-2)."""

from __future__ import annotations

import pytest

from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
from hpc_sweep_manager.core.remote.ssh_compute_source import (
    SSHComputeSource,
    build_ssh_source,
    parse_gpus_arg,
)


class TestParseGpusArg:
    def test_none_returns_none(self):
        assert parse_gpus_arg(None) is None

    def test_empty_returns_none(self):
        assert parse_gpus_arg("") is None

    def test_all_returns_none(self):
        assert parse_gpus_arg("all") is None
        assert parse_gpus_arg("ALL") is None

    def test_cpu_returns_zero(self):
        assert parse_gpus_arg("cpu") == 0
        assert parse_gpus_arg("CPU") == 0

    def test_single_int(self):
        assert parse_gpus_arg("4") == 4
        assert parse_gpus_arg("1") == 1

    def test_zero_int_is_cpu(self):
        # int 0 means CPU per normalize_gpu_allowlist; both spellings agree.
        assert parse_gpus_arg("0") == 0

    def test_comma_list(self):
        assert parse_gpus_arg("0,1,3") == [0, 1, 3]

    def test_comma_list_with_spaces(self):
        assert parse_gpus_arg(" 0 , 1 , 3 ") == [0, 1, 3]

    def test_single_with_trailing_comma_is_list(self):
        # Anything containing a comma takes the list path so users have a way
        # to express a single-element explicit allowlist.
        assert parse_gpus_arg("2,") == [2]

    def test_invalid_int_raises(self):
        with pytest.raises(ValueError, match="--gpus"):
            parse_gpus_arg("abc")

    def test_invalid_list_raises(self):
        with pytest.raises(ValueError, match="list"):
            parse_gpus_arg("0,foo,2")


class TestBuildSshSource:
    DEFAULTS = dict(project_dir="/local/proj", script_path="train.py")

    def test_bare_alias_no_config(self):
        src = build_ssh_source(name="anahita", **self.DEFAULTS)
        assert isinstance(src, SSHComputeSource)
        assert src.host == "anahita"  # alias-as-host fallback
        assert src.remote_root == "~/.hsm/runs"
        assert src.conda_env is None
        assert src.python_path is None
        assert src.max_parallel_jobs == 1
        assert src._gpus_config is None
        assert src.keep_remote_on_success is False
        assert src.rsync_excludes  # populated from DEFAULT_RSYNC_EXCLUDES

    def test_per_remote_overrides_global(self):
        distributed = {
            "conda_env": "lab",
            "remote_root": "~/.hsm/global",
            "keep_remote_on_success": False,
        }
        remote = {
            "host": "actual.example.com",
            "ssh_key": "~/.ssh/foo",
            "ssh_port": 2222,
            "max_parallel_jobs": 8,
            "conda_env": "lab-cpu",         # overrides global
            "remote_root": "~/.hsm/anahita",  # overrides global
            "gpus": [0, 2],
            "keep_remote_on_success": True,
        }
        src = build_ssh_source(
            name="anahita",
            remote_cfg=remote,
            distributed_cfg=distributed,
            **self.DEFAULTS,
        )
        assert src.host == "actual.example.com"
        assert src.ssh_key == "~/.ssh/foo"
        assert src.ssh_port == 2222
        assert src.max_parallel_jobs == 8
        assert src.conda_env == "lab-cpu"
        assert src.remote_root == "~/.hsm/anahita"
        assert src._gpus_config == [0, 2]
        assert src.keep_remote_on_success is True

    def test_global_fallback(self):
        distributed = {"conda_env": "lab", "remote_root": "/scratch/hsm"}
        remote = {"max_parallel_jobs": 4}
        src = build_ssh_source(
            name="anahita",
            remote_cfg=remote,
            distributed_cfg=distributed,
            **self.DEFAULTS,
        )
        assert src.conda_env == "lab"
        assert src.remote_root == "/scratch/hsm"

    def test_explicit_overrides_win(self):
        distributed = {"conda_env": "global"}
        remote = {"conda_env": "remote", "gpus": [0, 1]}
        src = build_ssh_source(
            name="anahita",
            remote_cfg=remote,
            distributed_cfg=distributed,
            conda_env_override="cli-env",
            gpus_override=[3],
            **self.DEFAULTS,
        )
        assert src.conda_env == "cli-env"
        assert src._gpus_config == [3]

    def test_rsync_excludes_per_remote(self):
        distributed = {"rsync_excludes": ["global_excl"]}
        remote = {"rsync_excludes": ["per_remote_excl"]}
        src = build_ssh_source(
            name="anahita",
            remote_cfg=remote,
            distributed_cfg=distributed,
            **self.DEFAULTS,
        )
        assert "per_remote_excl" in src.rsync_excludes
        assert "global_excl" not in src.rsync_excludes

    def test_rsync_excludes_global_fallback(self):
        distributed = {"rsync_excludes": ["only_global"]}
        src = build_ssh_source(
            name="anahita",
            distributed_cfg=distributed,
            **self.DEFAULTS,
        )
        assert "only_global" in src.rsync_excludes

    def test_default_spec_passed_through(self):
        spec = ResourceSpec(gpus=2, modules=("foo",))
        src = build_ssh_source(
            name="anahita", default_spec=spec, **self.DEFAULTS
        )
        assert src.default_spec is spec
