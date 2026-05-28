"""Unit tests for the sweep orchestrator factory + spec helpers."""

from __future__ import annotations

import pytest

from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
from hpc_sweep_manager.core.common.sweep_orchestrator import (
    SUPPORTED_MODES,
    build_compute_source,
    spec_from_cli,
)
from hpc_sweep_manager.core.hpc.slurm_compute_source import SlurmComputeSource
from hpc_sweep_manager.core.local.local_compute_source import LocalComputeSource


class TestSpecFromCli:
    def test_empty_inputs_returns_empty_spec(self):
        spec = spec_from_cli(walltime=None, resources=None)
        assert spec == ResourceSpec()

    def test_walltime_only(self):
        spec = spec_from_cli(walltime="01:00:00", resources=None)
        assert spec.walltime == "01:00:00"
        assert spec.cpus_per_task is None

    def test_walltime_overrides_resources(self):
        # walltime arg should win even if --resources also encodes one
        spec = spec_from_cli(
            walltime="02:00:00",
            resources="--time=99:99:99 --cpus-per-task=4",
            scheduler="slurm",
        )
        assert spec.walltime == "02:00:00"
        assert spec.cpus_per_task == 4

    def test_slurm_resources_parsed(self):
        spec = spec_from_cli(
            walltime=None,
            resources="--cpus-per-task=8 --mem-per-cpu=4G --gpus=2",
            scheduler="slurm",
        )
        assert spec.cpus_per_task == 8
        assert spec.mem_per_cpu == "4G"
        assert spec.gpus == 2

    def test_pbs_resources_parsed(self):
        spec = spec_from_cli(
            walltime=None,
            resources="select=1:ncpus=4:mem=16gb:ngpus=1",
            scheduler="pbs",
        )
        assert spec.cpus_per_task == 4
        assert spec.mem == "16gb"
        assert spec.gpus == 1


class TestSpecFromCliSlurmConfigBlock:
    """Precedence: CLI --walltime > CLI --resources > slurm: block > empty."""

    def test_slurm_block_alone_is_returned(self):
        from hpc_sweep_manager.core.common.config import HSMConfig

        cfg = HSMConfig(
            {
                "slurm": {
                    "walltime": "01:00:00",
                    "cpus_per_task": 4,
                    "gpus": 1,
                    "gpu_type": "h100",
                    "modules": ["h100"],
                }
            }
        )
        spec = spec_from_cli(walltime=None, resources=None, hsm_config=cfg)
        # Fields the opaque --resources string can't reach come through here.
        assert spec.gpu_type == "h100"
        assert spec.modules == ("h100",)
        assert spec.cpus_per_task == 4

    def test_resources_layers_on_top_of_block(self):
        from hpc_sweep_manager.core.common.config import HSMConfig

        cfg = HSMConfig(
            {
                "slurm": {
                    "walltime": "01:00:00",
                    "cpus_per_task": 4,
                    "gpus": 1,
                    "gpu_type": "h100",
                    "modules": ["h100"],
                }
            }
        )
        spec = spec_from_cli(
            walltime=None,
            resources="--cpus-per-task=8",  # override cpus
            scheduler="slurm",
            hsm_config=cfg,
        )
        # --resources overrode cpus_per_task; gpu_type + modules preserved.
        assert spec.cpus_per_task == 8
        assert spec.gpu_type == "h100"
        assert spec.modules == ("h100",)

    def test_walltime_overrides_everything(self):
        from hpc_sweep_manager.core.common.config import HSMConfig

        cfg = HSMConfig({"slurm": {"walltime": "01:00:00", "gpus": 1, "gpu_type": "h100"}})
        spec = spec_from_cli(
            walltime="04:00:00",
            resources="--time=02:00:00",
            scheduler="slurm",
            hsm_config=cfg,
        )
        assert spec.walltime == "04:00:00"
        # Slurm block fields still come through.
        assert spec.gpu_type == "h100"

    def test_no_block_no_resources_returns_empty(self):
        from hpc_sweep_manager.core.common.config import HSMConfig

        cfg = HSMConfig({})  # No slurm block.
        assert spec_from_cli(walltime=None, resources=None, hsm_config=cfg) == ResourceSpec()


class TestSpecFromCliModeAware:
    """No-bleed semantics: each mode reads only its own config block.

    Regression guard for the bug where every backend silently read the
    ``slurm:`` block as its default ResourceSpec, so e.g. ``slurm: { gpus: 4 }``
    secretly changed ``--mode local`` behavior. See CLAUDE.md gotcha #5b.
    """

    def _both_blocks_cfg(self):
        from hpc_sweep_manager.core.common.config import HSMConfig

        # local: says gpus=1; slurm: says gpus=4. The mode picks the winner.
        return HSMConfig(
            {
                "local": {"walltime": "02:00:00", "gpus": 1},
                "slurm": {"walltime": "08:00:00", "gpus": 4, "gpu_type": "h100"},
            }
        )

    def test_local_mode_reads_only_local_block(self):
        spec = spec_from_cli(
            walltime=None, resources=None, hsm_config=self._both_blocks_cfg(), mode="local"
        )
        assert spec.gpus == 1            # from local: block
        assert spec.walltime == "02:00:00"
        assert spec.gpu_type is None     # slurm: block must not leak

    def test_array_mode_reads_only_slurm_block(self):
        spec = spec_from_cli(
            walltime=None, resources=None, hsm_config=self._both_blocks_cfg(), mode="array"
        )
        assert spec.gpus == 4            # from slurm: block
        assert spec.walltime == "08:00:00"
        assert spec.gpu_type == "h100"

    def test_individual_mode_reads_only_slurm_block(self):
        spec = spec_from_cli(
            walltime=None, resources=None, hsm_config=self._both_blocks_cfg(), mode="individual"
        )
        assert spec.gpus == 4
        assert spec.gpu_type == "h100"

    def test_remote_mode_reads_neither_block(self):
        # Per-remote spec lives at distributed.remotes.<alias>.spec, not in
        # the global blocks — for --mode remote / --mode distributed both
        # local: and slurm: must be ignored.
        spec = spec_from_cli(
            walltime=None, resources=None, hsm_config=self._both_blocks_cfg(), mode="remote"
        )
        assert spec == ResourceSpec()

    def test_distributed_mode_reads_neither_block(self):
        spec = spec_from_cli(
            walltime=None, resources=None, hsm_config=self._both_blocks_cfg(), mode="distributed"
        )
        assert spec == ResourceSpec()

    def test_cli_flags_still_layer_on_remote_mode(self):
        # CLI overrides MUST still propagate for remote/distributed — only
        # the global blocks are skipped.
        spec = spec_from_cli(
            walltime="04:00:00",
            resources="--gpus=2",
            scheduler="slurm",
            hsm_config=self._both_blocks_cfg(),
            mode="remote",
        )
        assert spec.walltime == "04:00:00"
        assert spec.gpus == 2

    def test_legacy_mode_none_preserves_slurm_bleed(self):
        # Back-compat: callers that don't pass mode= keep the old behavior
        # (slurm: block is read). This lets the pre-refactor tests pass.
        spec = spec_from_cli(
            walltime=None, resources=None, hsm_config=self._both_blocks_cfg(), mode=None
        )
        assert spec.gpus == 4
        assert spec.gpu_type == "h100"


class TestBuildComputeSource:
    BASE_KWARGS = dict(
        python_path="python3",
        script_path="/tmp/train.py",
        project_dir="/tmp",
    )

    def test_unknown_mode_raises(self):
        with pytest.raises(ValueError, match="unsupported mode"):
            build_compute_source(mode="quantum", **self.BASE_KWARGS)

    def test_modes_list_matches_supported(self):
        # Sanity: the supported set should match what the CLI documents.
        assert SUPPORTED_MODES == {
            "local",
            "auto",
            "array",
            "individual",
            "distributed",
            "remote",
        }

    def test_distributed_requires_hsm_config(self):
        with pytest.raises(RuntimeError, match="requires hsm_config"):
            build_compute_source(mode="distributed", hsm_config=None, **self.BASE_KWARGS)

    def test_distributed_requires_enabled_flag(self):
        class FakeConfig:
            config_data = {"distributed": {"enabled": False}}

            def get_max_array_size(self):
                return None

        with pytest.raises(RuntimeError, match="not enabled"):
            build_compute_source(
                mode="distributed", hsm_config=FakeConfig(), **self.BASE_KWARGS
            )

    def test_distributed_requires_sources(self):
        class FakeConfig:
            config_data = {"distributed": {"enabled": True}}

            def get_max_array_size(self):
                return None

        with pytest.raises(RuntimeError, match="No compute sources"):
            build_compute_source(
                mode="distributed", hsm_config=FakeConfig(), **self.BASE_KWARGS
            )

    def test_distributed_builds_source(self):
        from hpc_sweep_manager.core.distributed.distributed_compute_source import (
            DistributedComputeSource,
        )

        class FakeConfig:
            config_data = {
                "distributed": {"enabled": True, "local_max_jobs": 2, "remotes": {}}
            }

            def get_max_array_size(self):
                return None

        source, resolved, sub_mode = build_compute_source(
            mode="distributed", hsm_config=FakeConfig(), **self.BASE_KWARGS
        )
        assert isinstance(source, DistributedComputeSource)
        assert resolved == "distributed"
        assert sub_mode == "individual"

    def test_local_mode_returns_local_source(self, no_gpus):
        source, resolved, sub_mode = build_compute_source(
            mode="local", parallel_jobs=4, **self.BASE_KWARGS
        )
        assert isinstance(source, LocalComputeSource)
        assert resolved == "local"
        assert sub_mode == "individual"
        assert source.max_parallel_jobs == 4

    def test_local_mode_no_parallel_jobs_falls_back_to_one(self, no_gpus):
        # `slurm.max_array_size` is a Slurm-array cap; it must NOT piggyback
        # as the local slot-queue default any more. Pre-cleanup it did, and
        # that was the source of cross-mode bleed for the parallel-jobs knob.
        class FakeConfig:
            def get_max_array_size(self):
                return 100  # should be ignored by local mode

        source, _, _ = build_compute_source(
            mode="local", hsm_config=FakeConfig(), **self.BASE_KWARGS
        )
        assert source.max_parallel_jobs == 1  # CLI --parallel-jobs is the only knob

    def test_local_mode_explicit_parallel_jobs_overrides_config(self, no_gpus):
        class FakeConfig:
            def get_max_array_size(self):
                return 100

        source, _, _ = build_compute_source(
            mode="local",
            hsm_config=FakeConfig(),
            parallel_jobs=2,
            **self.BASE_KWARGS,
        )
        assert source.max_parallel_jobs == 2

    def test_auto_mode_falls_back_to_local_without_slurm(self, no_gpus, monkeypatch):
        monkeypatch.setattr(
            "hpc_sweep_manager.core.common.sweep_orchestrator.shutil.which",
            lambda _: None,
        )
        source, resolved, sub_mode = build_compute_source(mode="auto", **self.BASE_KWARGS)
        assert isinstance(source, LocalComputeSource)
        assert resolved == "local"
        assert sub_mode == "individual"

    def test_auto_mode_picks_slurm_when_sbatch_on_path(self, fake_slurm):
        source, resolved, sub_mode = build_compute_source(mode="auto", **self.BASE_KWARGS)
        assert isinstance(source, SlurmComputeSource)
        assert resolved == "array"
        assert sub_mode == "array"

    def test_array_mode_requires_slurm(self, monkeypatch):
        monkeypatch.setattr(
            "hpc_sweep_manager.core.common.sweep_orchestrator.shutil.which",
            lambda _: None,
        )
        with pytest.raises(RuntimeError, match="no Slurm tools"):
            build_compute_source(mode="array", **self.BASE_KWARGS)

    def test_individual_mode_with_slurm(self, fake_slurm):
        source, resolved, sub_mode = build_compute_source(
            mode="individual", **self.BASE_KWARGS
        )
        assert isinstance(source, SlurmComputeSource)
        assert resolved == "individual"
        assert sub_mode == "individual"

    def test_array_mode_with_slurm(self, fake_slurm):
        source, resolved, sub_mode = build_compute_source(
            mode="array", **self.BASE_KWARGS
        )
        assert isinstance(source, SlurmComputeSource)
        assert resolved == "array"
        assert sub_mode == "array"

    def test_default_spec_is_propagated(self, fake_slurm):
        spec = ResourceSpec(walltime="03:00:00", cpus_per_task=2)
        source, _, _ = build_compute_source(
            mode="array", default_spec=spec, **self.BASE_KWARGS
        )
        assert source.default_spec is spec

    def test_qos_whitelist_is_propagated_to_slurm(self, fake_slurm):
        wl = frozenset({"normal", "long"})
        source, _, _ = build_compute_source(
            mode="array", qos_whitelist=wl, **self.BASE_KWARGS
        )
        assert source.qos_whitelist is wl

    def test_qos_whitelist_from_hsm_config_is_used(self, fake_slurm):
        # When the caller doesn't pass qos_whitelist, fall back to
        # hsm_config's slurm.qos_whitelist block.
        from hpc_sweep_manager.core.common.config import HSMConfig

        cfg = HSMConfig({"slurm": {"qos_whitelist": ["normal", "medium", "long"]}})
        source, _, _ = build_compute_source(
            mode="array", hsm_config=cfg, **self.BASE_KWARGS
        )
        assert source.qos_whitelist == frozenset({"normal", "medium", "long"})

    def test_explicit_qos_whitelist_overrides_hsm_config(self, fake_slurm):
        from hpc_sweep_manager.core.common.config import HSMConfig

        cfg = HSMConfig({"slurm": {"qos_whitelist": ["normal", "medium", "long"]}})
        explicit = frozenset({"short"})
        source, _, _ = build_compute_source(
            mode="array", qos_whitelist=explicit, hsm_config=cfg, **self.BASE_KWARGS
        )
        assert source.qos_whitelist is explicit


class TestRemoteMode:
    """`mode='remote'` builds a single push SSHComputeSource."""

    BASE_KWARGS = dict(
        python_path="python3",
        script_path="train.py",
        project_dir="/local/proj",
    )

    def test_requires_alias(self):
        with pytest.raises(RuntimeError, match="--remote <alias>"):
            build_compute_source(mode="remote", **self.BASE_KWARGS)

    def test_bare_alias_works_without_hsm_config(self):
        from hpc_sweep_manager.core.remote.ssh_compute_source import SSHComputeSource

        source, resolved, sub_mode = build_compute_source(
            mode="remote",
            remote_alias="anahita",
            hsm_config=None,
            **self.BASE_KWARGS,
        )
        assert isinstance(source, SSHComputeSource)
        assert source.host == "anahita"
        assert source.name == "anahita"
        assert resolved == "remote"
        assert sub_mode == "individual"

    def test_registered_remote_supplies_fields(self):
        from hpc_sweep_manager.core.remote.ssh_compute_source import SSHComputeSource

        class FakeConfig:
            config_data = {
                "distributed": {
                    "conda_env": "lab",
                    "remote_root": "/scratch/hsm",
                    "remotes": {
                        "anahita": {
                            "max_parallel_jobs": 4,
                            "gpus": [0, 1],
                            "conda_env": "lab-cpu",
                        }
                    },
                }
            }

            def get_max_array_size(self):
                return None

        source, _, _ = build_compute_source(
            mode="remote",
            remote_alias="anahita",
            hsm_config=FakeConfig(),
            **self.BASE_KWARGS,
        )
        assert isinstance(source, SSHComputeSource)
        assert source.max_parallel_jobs == 4
        assert source.conda_env == "lab-cpu"  # per-remote wins over global
        assert source.remote_root == "/scratch/hsm"
        assert source._gpus_config == [0, 1]

    def test_cli_overrides_beat_config(self):
        class FakeConfig:
            config_data = {
                "distributed": {
                    "conda_env": "config-env",
                    "remotes": {"anahita": {"conda_env": "per-remote", "gpus": [0]}},
                }
            }

            def get_max_array_size(self):
                return None

        source, _, _ = build_compute_source(
            mode="remote",
            remote_alias="anahita",
            hsm_config=FakeConfig(),
            gpus_override=[2, 3],
            conda_env_override="cli-env",
            **self.BASE_KWARGS,
        )
        assert source.conda_env == "cli-env"
        assert source._gpus_config == [2, 3]
