"""Unit tests for HSMConfig accessors — focuses on the typed ``slurm:`` block.

The rest of HSMConfig is a thin dict-getter; the ``slurm:`` block accessors
are the only part with real logic (filter + validate + frozenset construction).
"""

from __future__ import annotations

import pytest

from hpc_sweep_manager.core.common.config import HSMConfig
from hpc_sweep_manager.core.common.resource_spec import ResourceSpec


# ----------------------------------------------------------------- get_slurm_spec


class TestSlurmSpec:
    def test_no_block_returns_none(self):
        assert HSMConfig({}).get_slurm_spec() is None

    def test_empty_block_returns_none(self):
        assert HSMConfig({"slurm": {}}).get_slurm_spec() is None

    def test_non_dict_block_returns_none(self):
        # Defensive: a typo'd YAML scalar shouldn't crash; we warn + return None.
        assert HSMConfig({"slurm": "oops"}).get_slurm_spec() is None
        assert HSMConfig({"slurm": ["a", "b"]}).get_slurm_spec() is None

    def test_walltime_only(self):
        spec = HSMConfig({"slurm": {"walltime": "02:00:00"}}).get_slurm_spec()
        assert spec is not None
        assert spec.walltime == "02:00:00"
        assert spec.gpus is None

    def test_full_field_set(self):
        spec = HSMConfig(
            {
                "slurm": {
                    "walltime": "01:00:00",
                    "cpus_per_task": 4,
                    "mem": "16gb",
                    "gpus": 1,
                    "gpu_type": "h100",
                    "partition": "gpu",
                    "qos": "normal",
                    "account": "my-project",
                    "modules": ["h100", "cuda/12"],
                    "pre_script": ["source ~/.bashrc", "conda activate my-env"],
                    "extra_directives": {"mail-type": "FAIL", "mail-user": "me@x.com"},
                }
            }
        ).get_slurm_spec()
        assert spec is not None
        assert spec.walltime == "01:00:00"
        assert spec.cpus_per_task == 4
        assert spec.mem == "16gb"
        assert spec.gpus == 1
        assert spec.gpu_type == "h100"
        assert spec.partition == "gpu"
        assert spec.qos == "normal"
        assert spec.account == "my-project"
        assert spec.modules == ("h100", "cuda/12")
        assert spec.pre_script == ("source ~/.bashrc", "conda activate my-env")
        assert dict(spec.extra_directives) == {"mail-type": "FAIL", "mail-user": "me@x.com"}

    def test_qos_whitelist_stripped_before_construction(self):
        # qos_whitelist lives in the slurm: block but is not a ResourceSpec field;
        # we strip it before building the spec.
        spec = HSMConfig(
            {
                "slurm": {
                    "gpus": 1,
                    "qos_whitelist": ["normal", "medium", "long"],
                }
            }
        ).get_slurm_spec()
        assert spec is not None
        assert spec.gpus == 1

    def test_invalid_field_returns_none_and_warns(self, caplog):
        # ResourceSpec's __post_init__ rejects bad values; the accessor catches.
        with caplog.at_level("WARNING"):
            result = HSMConfig({"slurm": {"cpus_per_task": -1}}).get_slurm_spec()
        assert result is None
        assert any("Invalid `slurm:` block" in r.message for r in caplog.records)

    def test_gpu_type_without_gpus_returns_none(self, caplog):
        # ResourceSpec rejects gpu_type without gpus >= 1.
        with caplog.at_level("WARNING"):
            result = HSMConfig({"slurm": {"gpu_type": "h100"}}).get_slurm_spec()
        assert result is None


# ----------------------------------------------------------------- get_local_spec


class TestLocalSpec:
    def test_no_block_returns_none(self):
        assert HSMConfig({}).get_local_spec() is None

    def test_empty_block_returns_none(self):
        assert HSMConfig({"local": {}}).get_local_spec() is None

    def test_non_dict_block_returns_none(self):
        assert HSMConfig({"local": "oops"}).get_local_spec() is None
        assert HSMConfig({"local": ["a", "b"]}).get_local_spec() is None

    def test_allowed_fields_round_trip(self):
        spec = HSMConfig(
            {
                "local": {
                    "walltime": "02:00:00",
                    "cpus_per_task": 4,
                    "mem": "16gb",
                    "gpus": 1,
                    "pre_script": ["conda activate my-env"],
                }
            }
        ).get_local_spec()
        assert spec is not None
        assert spec.walltime == "02:00:00"
        assert spec.cpus_per_task == 4
        assert spec.gpus == 1
        assert spec.pre_script == ("conda activate my-env",)

    def test_slurm_only_fields_dropped_with_warning(self, caplog):
        # Slurm reach fields should NOT silently leak into local mode.
        with caplog.at_level("WARNING"):
            spec = HSMConfig(
                {
                    "local": {
                        "gpus": 1,
                        "gpu_type": "h100",     # should be ignored + warn
                        "modules": ["h100"],     # should be ignored + warn
                        "qos": "normal",         # should be ignored + warn
                        "account": "my-project", # should be ignored + warn
                    }
                }
            ).get_local_spec()
        assert spec is not None
        assert spec.gpus == 1
        assert spec.gpu_type is None
        assert spec.modules == ()
        assert spec.qos is None
        assert spec.account is None
        assert any("Slurm-only" in r.message for r in caplog.records)


# ------------------------------------------------------------ get_local_visible_gpus


class TestLocalVisibleGpus:
    def test_no_block_returns_none(self):
        assert HSMConfig({}).get_local_visible_gpus() is None

    def test_no_field_returns_none(self):
        assert HSMConfig({"local": {"gpus": 1}}).get_local_visible_gpus() is None

    def test_list_returns_list_of_ints(self):
        v = HSMConfig({"local": {"visible_gpus": [1, 2, 3]}}).get_local_visible_gpus()
        assert v == [1, 2, 3]

    def test_string_indices_coerced(self):
        # YAML may yield strings if quoted; coerce to int.
        v = HSMConfig({"local": {"visible_gpus": ["1", "2"]}}).get_local_visible_gpus()
        assert v == [1, 2]

    def test_empty_list_returns_none(self):
        assert (
            HSMConfig({"local": {"visible_gpus": []}}).get_local_visible_gpus()
            is None
        )

    def test_non_list_warns_returns_none(self, caplog):
        # Bare int is the wrong shape — it would be ambiguous with the
        # `first N` CLI semantics. Force the user to write the list form.
        with caplog.at_level("WARNING"):
            result = HSMConfig(
                {"local": {"visible_gpus": 3}}
            ).get_local_visible_gpus()
        assert result is None
        assert any("must be a list" in r.message for r in caplog.records)

    def test_uncoercible_value_returns_none(self, caplog):
        with caplog.at_level("WARNING"):
            result = HSMConfig(
                {"local": {"visible_gpus": ["one", "two"]}}
            ).get_local_visible_gpus()
        assert result is None

    def test_visible_gpus_does_not_leak_into_spec(self):
        # visible_gpus is consumed separately; get_local_spec must NOT carry it.
        cfg = HSMConfig(
            {"local": {"gpus": 1, "visible_gpus": [1, 2, 3]}}
        )
        spec = cfg.get_local_spec()
        assert spec is not None
        assert spec.gpus == 1
        assert not hasattr(spec, "visible_gpus")
        # And it must not be in the rejected-fields warning either —
        # visible_gpus is a known non-spec key, not an unknown one.


# ----------------------------------------------------------- get_slurm_qos_whitelist


class TestSlurmQosWhitelist:
    def test_no_block_returns_none(self):
        assert HSMConfig({}).get_slurm_qos_whitelist() is None

    def test_block_without_whitelist_returns_none(self):
        assert HSMConfig({"slurm": {"gpus": 1}}).get_slurm_qos_whitelist() is None

    def test_list_returns_frozenset(self):
        wl = HSMConfig(
            {"slurm": {"qos_whitelist": ["normal", "medium", "long"]}}
        ).get_slurm_qos_whitelist()
        assert wl == frozenset({"normal", "medium", "long"})

    def test_set_returns_frozenset(self):
        wl = HSMConfig(
            {"slurm": {"qos_whitelist": {"normal", "long"}}}
        ).get_slurm_qos_whitelist()
        assert wl == frozenset({"normal", "long"})

    def test_string_warns_returns_none(self, caplog):
        # A bare string is a YAML mistake (forgetting the list dashes).
        with caplog.at_level("WARNING"):
            result = HSMConfig(
                {"slurm": {"qos_whitelist": "normal"}}
            ).get_slurm_qos_whitelist()
        assert result is None
        assert any("must be a list" in r.message for r in caplog.records)

    def test_empty_list_returns_none(self):
        assert (
            HSMConfig({"slurm": {"qos_whitelist": []}}).get_slurm_qos_whitelist()
            is None
        )


# --------------------------------------------------------- existing-accessors smoke


class TestExistingAccessors:
    """Smoke checks for the other HSMConfig getters — pre-existing behavior."""

    def test_defaults_when_blocks_missing(self):
        cfg = HSMConfig({})
        assert cfg.get_max_array_size() is None
        assert cfg.get_project_root() is None
        assert cfg.get_default_python_path() is None
        assert cfg.get_default_script_path() is None
        assert cfg.get_wandb_config() == {}

    def test_max_array_size_reads_from_slurm_block(self):
        # Moved from `hpc:` to `slurm:` when the legacy hpc: block was deleted.
        cfg = HSMConfig({"slurm": {"max_array_size": 5000, "gpus": 1}})
        assert cfg.get_max_array_size() == 5000
        # And it must NOT leak into the ResourceSpec built from the same block.
        spec = cfg.get_slurm_spec()
        assert spec is not None
        assert spec.gpus == 1
        assert not hasattr(spec, "max_array_size")
