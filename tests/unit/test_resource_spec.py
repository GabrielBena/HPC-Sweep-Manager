"""Unit tests for ResourceSpec."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace

import pytest

from hpc_sweep_manager.core.common.resource_spec import (
    ResourceSpec,
    spec_from_legacy_resources,
)


class TestResourceSpec:
    def test_default_is_all_none(self):
        spec = ResourceSpec()
        assert spec.walltime is None
        assert spec.cpus_per_task is None
        assert spec.mem is None
        assert spec.mem_per_cpu is None
        assert spec.gpus is None
        assert spec.gpu_type is None
        assert spec.partition is None
        assert spec.qos is None
        assert spec.account is None
        assert spec.modules == ()
        assert spec.pre_script == ()
        assert spec.extra_directives == ()

    def test_full_construction(self):
        spec = ResourceSpec(
            walltime="04:00:00",
            cpus_per_task=4,
            mem_per_cpu="4G",
            gpus=1,
            gpu_type="h100",
            qos="normal",
            partition="gpu",
            account="proj-x",
            modules=("h100", "cuda/12.1"),
            pre_script=("source ~/venv/bin/activate",),
            extra_directives=(("--exclusive", ""),),
        )
        assert spec.cpus_per_task == 4
        assert spec.modules == ("h100", "cuda/12.1")

    def test_frozen(self):
        spec = ResourceSpec(walltime="01:00:00")
        with pytest.raises(FrozenInstanceError):
            spec.walltime = "02:00:00"  # type: ignore[misc]

    def test_replace_creates_new_instance(self):
        spec = ResourceSpec(walltime="01:00:00", cpus_per_task=2)
        new_spec = replace(spec, walltime="02:00:00")
        assert new_spec.walltime == "02:00:00"
        assert new_spec.cpus_per_task == 2
        assert spec.walltime == "01:00:00"  # original unchanged


class TestResourceSpecValidation:
    def test_mem_and_mem_per_cpu_mutually_exclusive(self):
        with pytest.raises(ValueError, match="cannot set both"):
            ResourceSpec(mem="16G", mem_per_cpu="4G")

    def test_negative_cpus(self):
        with pytest.raises(ValueError, match="cpus_per_task must be >= 1"):
            ResourceSpec(cpus_per_task=0)

    def test_negative_gpus(self):
        with pytest.raises(ValueError, match="gpus must be >= 0"):
            ResourceSpec(gpus=-1)

    def test_zero_gpus_allowed(self):
        spec = ResourceSpec(gpus=0)
        assert spec.gpus == 0

    def test_gpu_type_without_gpus_count_raises(self):
        with pytest.raises(ValueError, match="gpu_type requires gpus >= 1"):
            ResourceSpec(gpu_type="h100")

    def test_gpu_type_with_zero_gpus_raises(self):
        with pytest.raises(ValueError, match="gpu_type requires gpus >= 1"):
            ResourceSpec(gpus=0, gpu_type="h100")

    def test_empty_module_string_rejected(self):
        with pytest.raises(ValueError, match="modules must be non-empty"):
            ResourceSpec(modules=("",))

    def test_non_string_module_rejected(self):
        with pytest.raises(ValueError, match="modules must be non-empty"):
            ResourceSpec(modules=(123,))  # type: ignore[arg-type]


class TestResourceSpecFromDict:
    def test_empty_dict(self):
        assert ResourceSpec.from_dict({}) == ResourceSpec()
        assert ResourceSpec.from_dict(None) == ResourceSpec()

    def test_full_dict(self):
        spec = ResourceSpec.from_dict(
            {
                "walltime": "12:00:00",
                "cpus_per_task": 8,
                "mem_per_cpu": "2G",
                "gpus": 2,
                "gpu_type": "l4",
                "qos": "medium",
                "modules": ["l4", "cuda/12.0"],
                "pre_script": ["source ~/.bashrc"],
                "extra_directives": {"--exclusive": "", "--nice": "100"},
            }
        )
        assert spec.cpus_per_task == 8
        assert spec.modules == ("l4", "cuda/12.0")
        assert spec.pre_script == ("source ~/.bashrc",)
        assert dict(spec.extra_directives) == {"--exclusive": "", "--nice": "100"}

    def test_string_modules_wrapped(self):
        spec = ResourceSpec.from_dict({"modules": "h100"})
        assert spec.modules == ("h100",)

    def test_extra_directives_from_pairs(self):
        spec = ResourceSpec.from_dict(
            {"extra_directives": [("--exclusive", ""), ("--mail-type", "BEGIN")]}
        )
        assert dict(spec.extra_directives) == {"--exclusive": "", "--mail-type": "BEGIN"}

    def test_none_values_are_skipped(self):
        spec = ResourceSpec.from_dict({"walltime": "04:00:00", "qos": None})
        assert spec.walltime == "04:00:00"
        assert spec.qos is None


class TestResourceSpecMerge:
    def test_merge_none_returns_self(self):
        spec = ResourceSpec(walltime="04:00:00")
        assert spec.merge(None) is spec

    def test_merge_overrides_with_non_none(self):
        base = ResourceSpec(walltime="04:00:00", cpus_per_task=2)
        override = ResourceSpec(walltime="24:00:00", qos="long")
        merged = base.merge(override)
        assert merged.walltime == "24:00:00"  # overridden
        assert merged.cpus_per_task == 2  # preserved
        assert merged.qos == "long"  # added

    def test_merge_skips_none_overrides(self):
        base = ResourceSpec(walltime="04:00:00")
        override = ResourceSpec(qos="medium")  # walltime defaults to None
        merged = base.merge(override)
        assert merged.walltime == "04:00:00"  # preserved
        assert merged.qos == "medium"

    def test_merge_with_dict(self):
        base = ResourceSpec(walltime="04:00:00", cpus_per_task=2)
        merged = base.merge({"qos": "normal", "gpus": 1})
        assert merged.walltime == "04:00:00"
        assert merged.qos == "normal"
        assert merged.gpus == 1

    def test_merge_collections_replace_wholesale(self):
        base = ResourceSpec(modules=("h100",))
        merged = base.merge(ResourceSpec(modules=("l4",)))
        assert merged.modules == ("l4",)

    def test_merge_empty_collection_does_not_clear(self):
        base = ResourceSpec(modules=("h100",))
        merged = base.merge(ResourceSpec(modules=()))
        assert merged.modules == ("h100",)


class TestResourceSpecToDict:
    def test_round_trip(self):
        spec = ResourceSpec(
            walltime="04:00:00",
            modules=("h100",),
            extra_directives=(("--exclusive", ""),),
        )
        d = spec.to_dict()
        assert d["walltime"] == "04:00:00"
        assert d["modules"] == ["h100"]
        assert d["extra_directives"] == {"--exclusive": ""}


class TestLegacyResourceParsing:
    def test_empty_resources(self):
        assert spec_from_legacy_resources(None) == ResourceSpec()
        assert spec_from_legacy_resources("") == ResourceSpec()

    def test_pbs_style(self):
        spec = spec_from_legacy_resources("select=1:ncpus=4:mem=16gb", scheduler="pbs")
        assert spec.cpus_per_task == 4
        assert spec.mem == "16gb"

    def test_pbs_style_inferred(self):
        spec = spec_from_legacy_resources("select=1:ncpus=8:mem=32gb")
        assert spec.cpus_per_task == 8

    def test_slurm_style(self):
        spec = spec_from_legacy_resources(
            "--cpus-per-task=4 --mem=16G --qos=normal --partition=gpu",
            scheduler="slurm",
        )
        assert spec.cpus_per_task == 4
        assert spec.mem == "16G"
        assert spec.qos == "normal"
        assert spec.partition == "gpu"

    def test_slurm_unknown_flag_lands_in_extras(self):
        spec = spec_from_legacy_resources("--cpus-per-task=4 --weird-flag=foo", scheduler="slurm")
        assert spec.cpus_per_task == 4
        assert ("--weird-flag", "foo") in spec.extra_directives
