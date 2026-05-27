"""Unit tests for the pure push-execution helpers."""

from __future__ import annotations

import pytest

from hpc_sweep_manager.core.remote.push_exec import (
    DEFAULT_RSYNC_EXCLUDES,
    build_rsync_pull_cmd,
    build_rsync_push_cmd,
    normalize_gpu_allowlist,
    partition_gpu_slots,
    resolve_run_prefix,
)


class TestNormalizeGpuAllowlist:
    def test_none_uses_all_detected(self):
        assert normalize_gpu_allowlist(None, [0, 1, 2, 3]) == [0, 1, 2, 3]

    def test_zero_is_cpu(self):
        assert normalize_gpu_allowlist(0, [0, 1, 2, 3]) == []

    def test_int_takes_first_n(self):
        assert normalize_gpu_allowlist(2, [0, 1, 2, 3]) == [0, 1]

    def test_int_larger_than_detected(self):
        assert normalize_gpu_allowlist(9, [0, 1]) == [0, 1]

    def test_explicit_allowlist_preserved_and_intersected(self):
        assert normalize_gpu_allowlist([1, 2], [0, 1, 2, 3]) == [1, 2]
        # stale index 7 not present → dropped
        assert normalize_gpu_allowlist([2, 7], [0, 1, 2, 3]) == [2]

    def test_explicit_order_preserved(self):
        assert normalize_gpu_allowlist([3, 1], [0, 1, 2, 3]) == [3, 1]

    def test_bool_rejected(self):
        with pytest.raises(TypeError):
            normalize_gpu_allowlist(True, [0, 1])


class TestPartitionGpuSlots:
    def test_one_per_job(self):
        assert partition_gpu_slots([1, 2], 1, cpu_slots=4) == [[1], [2]]

    def test_two_per_job(self):
        assert partition_gpu_slots([0, 1, 2, 3], 2, cpu_slots=4) == [[0, 1], [2, 3]]

    def test_drops_remainder(self):
        assert partition_gpu_slots([0, 1, 2], 2, cpu_slots=4) == [[0, 1]]

    def test_request_exceeds_supply_falls_back_to_cpu(self):
        assert partition_gpu_slots([0], 2, cpu_slots=3) == [None, None, None]

    def test_no_gpus_uses_cpu_slots(self):
        assert partition_gpu_slots([], 1, cpu_slots=2) == [None, None]

    def test_gpus_per_job_zero_uses_cpu_slots(self):
        assert partition_gpu_slots([0, 1], 0, cpu_slots=2) == [None, None]

    def test_cpu_slots_floor_of_one(self):
        assert partition_gpu_slots([], 1, cpu_slots=0) == [None]


class TestResolveRunPrefix:
    def test_conda_env_wins(self):
        assert resolve_run_prefix("lab", "/x/python") == "conda run -n lab python"

    def test_python_path_fallback(self):
        assert resolve_run_prefix(None, "/opt/py/bin/python") == "/opt/py/bin/python"

    def test_bare_python_default(self):
        assert resolve_run_prefix(None, None) == "python"


class TestRsyncCommands:
    def test_push_structure(self):
        cmd = build_rsync_push_cmd("/local/proj", "anahita", "~/.hsm/runs/proj/code", [".git"])
        assert cmd[:3] == ["rsync", "-az", "--delete"]
        assert "--exclude=.git" in cmd
        # trailing slashes: contents of local → into remote dir
        assert cmd[-2] == "/local/proj/"
        assert cmd[-1] == "anahita:~/.hsm/runs/proj/code/"

    def test_push_strips_trailing_slashes_before_readding(self):
        cmd = build_rsync_push_cmd("/local/proj/", "h", "/remote/", [])
        assert cmd[-2] == "/local/proj/"
        assert cmd[-1] == "h:/remote/"

    def test_push_default_excludes_applied(self):
        cmd = build_rsync_push_cmd("/l", "h", "/r", DEFAULT_RSYNC_EXCLUDES)
        assert "--exclude=.git" in cmd
        assert "--exclude=sweeps/outputs" in cmd
        assert "--exclude=__pycache__" in cmd

    def test_pull_has_no_delete(self):
        cmd = build_rsync_pull_cmd("anahita", "/remote/tasks", "/local/tasks")
        assert "--delete" not in cmd
        assert cmd[-2] == "anahita:/remote/tasks/"
        assert cmd[-1] == "/local/tasks/"
