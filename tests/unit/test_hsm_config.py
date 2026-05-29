"""Unit tests for HSMConfig accessors — focuses on the typed ``slurm:`` block.

The rest of HSMConfig is a thin dict-getter; the ``slurm:`` block accessors
are the only part with real logic (filter + validate + frozenset construction).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from hpc_sweep_manager.core.common.config import HSMConfig, resolve_sweep_dir
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


# ----------------------------------------------------------- get_local_sweeps_root


class TestLocalSweepsRoot:
    def test_no_block_returns_none(self):
        assert HSMConfig({}).get_local_sweeps_root() is None

    def test_no_field_returns_none(self):
        assert HSMConfig({"local": {"gpus": 1}}).get_local_sweeps_root() is None

    def test_set_returns_raw_string(self):
        # Accessor returns the raw value; expansion happens in resolve_sweep_dir.
        v = HSMConfig(
            {"local": {"sweeps_root": "/mnt/big-disk/sweeps"}}
        ).get_local_sweeps_root()
        assert v == "/mnt/big-disk/sweeps"

    def test_envvar_pattern_preserved(self):
        # Don't expand at accessor time — resolve_sweep_dir does that.
        v = HSMConfig(
            {"local": {"sweeps_root": "/mnt/big-disk/$USER/sweeps"}}
        ).get_local_sweeps_root()
        assert "$USER" in v

    def test_empty_string_warns_returns_none(self, caplog):
        with caplog.at_level("WARNING"):
            result = HSMConfig(
                {"local": {"sweeps_root": "  "}}
            ).get_local_sweeps_root()
        assert result is None

    def test_non_string_warns_returns_none(self, caplog):
        with caplog.at_level("WARNING"):
            result = HSMConfig(
                {"local": {"sweeps_root": ["a", "b"]}}
            ).get_local_sweeps_root()
        assert result is None
        assert any("non-empty string" in r.message for r in caplog.records)

    def test_sweeps_root_does_not_leak_into_spec(self):
        # Like visible_gpus, sweeps_root is a non-ResourceSpec local field.
        # get_local_spec must NOT carry it and must NOT warn about it.
        cfg = HSMConfig(
            {"local": {"gpus": 1, "sweeps_root": "/mnt/big-disk/sweeps"}}
        )
        spec = cfg.get_local_spec()
        assert spec is not None
        assert spec.gpus == 1
        assert not hasattr(spec, "sweeps_root")


# ----------------------------------------------------------- resolve_sweep_dir


class TestResolveSweepDir:
    def test_default_when_no_config(self, tmp_path):
        # No HSMConfig => default <project>/sweeps/outputs/<sweep_id>.
        result = resolve_sweep_dir(None, "sweep_abc", project_dir=tmp_path)
        assert result == tmp_path / "sweeps" / "outputs" / "sweep_abc"
        assert result.is_dir()

    def test_default_when_no_sweeps_root(self, tmp_path):
        cfg = HSMConfig({"local": {"gpus": 1}})
        result = resolve_sweep_dir(cfg, "sweep_xyz", project_dir=tmp_path)
        assert result == tmp_path / "sweeps" / "outputs" / "sweep_xyz"
        assert result.is_dir()

    def test_redirects_to_sweeps_root(self, tmp_path):
        big_disk = tmp_path / "big-disk"
        big_disk.mkdir()  # caller's responsibility — see hard-error test below
        cfg = HSMConfig({"local": {"sweeps_root": str(big_disk)}})
        result = resolve_sweep_dir(cfg, "sweep_123", project_dir=tmp_path)
        # Result is the absolute target, NOT the symlink path.
        assert result == (big_disk / "sweep_123").resolve()
        assert result.is_dir()

    def test_creates_discovery_symlink(self, tmp_path):
        big_disk = tmp_path / "big-disk"
        big_disk.mkdir()
        cfg = HSMConfig({"local": {"sweeps_root": str(big_disk)}})
        result = resolve_sweep_dir(cfg, "sweep_42", project_dir=tmp_path)
        link = tmp_path / "sweeps" / "outputs" / "sweep_42"
        assert link.is_symlink()
        assert link.resolve() == result

    def test_expands_envvar(self, tmp_path, monkeypatch):
        expanded_root = tmp_path / "expanded" / "sweeps"
        expanded_root.mkdir(parents=True)
        monkeypatch.setenv("HSM_TEST_ROOT", str(tmp_path / "expanded"))
        cfg = HSMConfig(
            {"local": {"sweeps_root": "$HSM_TEST_ROOT/sweeps"}}
        )
        result = resolve_sweep_dir(cfg, "sweep_env", project_dir=tmp_path)
        assert "expanded" in str(result)
        assert result.is_dir()

    def test_expands_tilde(self, tmp_path, monkeypatch):
        # Simulate HOME so ~/... expansion is testable without touching the real one.
        fake_home = tmp_path / "home-stub"
        fake_home.mkdir()
        (fake_home / "hsm-sweeps").mkdir()
        monkeypatch.setenv("HOME", str(fake_home))
        cfg = HSMConfig({"local": {"sweeps_root": "~/hsm-sweeps"}})
        result = resolve_sweep_dir(cfg, "sweep_tilde", project_dir=tmp_path)
        # The result must live under the fake HOME, not under literal "~".
        assert str(fake_home) in str(result)
        assert "~" not in str(result)

    def test_replaces_stale_symlink(self, tmp_path):
        # If a symlink already exists at the discovery path (pointing to an
        # old target), resolve_sweep_dir replaces it with one pointing at
        # the new target.
        old_target = tmp_path / "old-target"
        new_root = tmp_path / "new-root"
        old_target.mkdir()
        new_root.mkdir()
        link_parent = tmp_path / "sweeps" / "outputs"
        link_parent.mkdir(parents=True)
        link = link_parent / "sweep_id"
        link.symlink_to(old_target, target_is_directory=True)

        cfg = HSMConfig({"local": {"sweeps_root": str(new_root)}})
        result = resolve_sweep_dir(cfg, "sweep_id", project_dir=tmp_path)
        assert link.is_symlink()
        assert link.resolve() == result
        assert (new_root / "sweep_id").resolve() == result

    def test_refuses_to_clobber_real_directory(self, tmp_path, caplog):
        # If a *real* (non-symlink) directory already lives at the discovery
        # path, resolve_sweep_dir warns instead of overwriting it. Data at
        # the target still gets created — discovery is just broken.
        big_disk = tmp_path / "big-disk"
        big_disk.mkdir()
        link_parent = tmp_path / "sweeps" / "outputs"
        link_parent.mkdir(parents=True)
        squatter = link_parent / "sweep_id"
        squatter.mkdir()  # real dir, not a symlink

        cfg = HSMConfig({"local": {"sweeps_root": str(big_disk)}})
        with caplog.at_level("WARNING"):
            result = resolve_sweep_dir(cfg, "sweep_id", project_dir=tmp_path)
        assert result == (big_disk / "sweep_id").resolve()
        assert result.is_dir()
        assert squatter.is_dir() and not squatter.is_symlink()
        assert any("non-symlink" in r.message for r in caplog.records)

    def test_raises_when_sweeps_root_missing(self, tmp_path):
        # Hard-error guard: shared .hsm/config.yaml that references a path
        # that exists on machine A but not on machine B should fail loudly
        # rather than silently mkdir-ing a probably-wrong directory.
        missing = tmp_path / "not-mounted"  # not created
        cfg = HSMConfig({"local": {"sweeps_root": str(missing)}})
        with pytest.raises(FileNotFoundError, match="does not.*exist"):
            resolve_sweep_dir(cfg, "sweep_x", project_dir=tmp_path)

    def test_raises_when_expanded_envvar_path_missing(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HSM_TEST_ROOT", str(tmp_path / "still-not-there"))
        cfg = HSMConfig({"local": {"sweeps_root": "$HSM_TEST_ROOT/sweeps"}})
        with pytest.raises(FileNotFoundError):
            resolve_sweep_dir(cfg, "sweep_x", project_dir=tmp_path)


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


# ----------------------------------------------------------- HSMConfig.load merge


class TestLoadMachineProjectMerge:
    """Cover the per-machine + per-project merge in :meth:`HSMConfig.load`.

    All tests pass ``machine_config_path`` explicitly so they never touch
    the real ``~/.hsm/config.yaml``.
    """

    def _write_yaml(self, path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    def test_neither_file_returns_none(self, tmp_path):
        project_path = tmp_path / "project" / ".hsm" / "config.yaml"  # missing
        machine_path = tmp_path / "home" / ".hsm" / "config.yaml"     # missing
        result = HSMConfig.load(
            config_path=project_path,
            machine_config_path=machine_path,
        )
        assert result is None

    def test_only_machine_file(self, tmp_path):
        machine_path = tmp_path / "home" / ".hsm" / "config.yaml"
        self._write_yaml(machine_path, "local:\n  sweeps_root: /big-disk/sweeps\n")
        result = HSMConfig.load(
            config_path=tmp_path / "missing.yaml",
            machine_config_path=machine_path,
        )
        assert result is not None
        assert result.get_local_sweeps_root() == "/big-disk/sweeps"

    def test_only_project_file(self, tmp_path):
        project_path = tmp_path / "project" / ".hsm" / "config.yaml"
        self._write_yaml(
            project_path,
            "project:\n  name: foo\nlocal:\n  gpus: 1\n",
        )
        result = HSMConfig.load(
            config_path=project_path,
            machine_config_path=tmp_path / "no-machine.yaml",
        )
        assert result is not None
        assert result.get_project_root() is None  # only name set, not root
        spec = result.get_local_spec()
        assert spec is not None
        assert spec.gpus == 1

    def test_local_block_deep_merged(self, tmp_path):
        # Machine sets sweeps_root + visible_gpus; project sets gpus.
        # All three should appear in the merged result.
        machine_path = tmp_path / "home" / ".hsm" / "config.yaml"
        project_path = tmp_path / "project" / ".hsm" / "config.yaml"
        self._write_yaml(
            machine_path,
            "local:\n  sweeps_root: /big-disk/sweeps\n  visible_gpus: [1, 2, 3]\n",
        )
        self._write_yaml(project_path, "local:\n  gpus: 1\n")
        result = HSMConfig.load(
            config_path=project_path,
            machine_config_path=machine_path,
        )
        assert result is not None
        assert result.get_local_sweeps_root() == "/big-disk/sweeps"
        assert result.get_local_visible_gpus() == [1, 2, 3]
        spec = result.get_local_spec()
        assert spec is not None
        assert spec.gpus == 1

    def test_project_wins_on_local_field_collision(self, tmp_path):
        machine_path = tmp_path / "home" / ".hsm" / "config.yaml"
        project_path = tmp_path / "project" / ".hsm" / "config.yaml"
        self._write_yaml(
            machine_path,
            "local:\n  visible_gpus: [1, 2, 3]\n  sweeps_root: /big-disk/m\n",
        )
        self._write_yaml(
            project_path,
            "local:\n  visible_gpus: [0]\n",
        )
        result = HSMConfig.load(
            config_path=project_path,
            machine_config_path=machine_path,
        )
        assert result is not None
        # Project overrides the field it sets.
        assert result.get_local_visible_gpus() == [0]
        # But machine's untouched fields flow through.
        assert result.get_local_sweeps_root() == "/big-disk/m"

    def test_non_local_keys_in_machine_dropped_with_warning(self, tmp_path, caplog):
        machine_path = tmp_path / "home" / ".hsm" / "config.yaml"
        self._write_yaml(
            machine_path,
            "local:\n  sweeps_root: /m\n"
            "slurm:\n  qos: bogus-from-machine\n"
            "distributed:\n  enabled: true\n",
        )
        with caplog.at_level("WARNING"):
            result = HSMConfig.load(
                config_path=tmp_path / "no-project.yaml",
                machine_config_path=machine_path,
            )
        assert result is not None
        # local: survives.
        assert result.get_local_sweeps_root() == "/m"
        # slurm:/distributed: from machine are dropped.
        assert result.get_slurm_spec() is None
        assert "distributed" not in result.config_data
        assert any("not honored" in r.message for r in caplog.records)

    def test_project_distributed_block_unaffected_by_machine_keys(self, tmp_path):
        # Even if the machine file (illegally) sets `distributed:`, the
        # project's `distributed:` block survives intact.
        machine_path = tmp_path / "home" / ".hsm" / "config.yaml"
        project_path = tmp_path / "project" / ".hsm" / "config.yaml"
        self._write_yaml(
            machine_path,
            "distributed:\n  enabled: true\n  remotes: {ghost: {}}\n",
        )
        self._write_yaml(
            project_path,
            "distributed:\n  enabled: true\n  remotes:\n    real:\n      backend: ssh\n",
        )
        result = HSMConfig.load(
            config_path=project_path,
            machine_config_path=machine_path,
        )
        assert result is not None
        remotes = result.config_data["distributed"]["remotes"]
        assert "real" in remotes
        assert "ghost" not in remotes

    def test_empty_machine_file_no_crash(self, tmp_path):
        # Comment-only YAML loads as None; merge should treat it as {}.
        machine_path = tmp_path / "home" / ".hsm" / "config.yaml"
        self._write_yaml(machine_path, "# just a comment\n")
        project_path = tmp_path / "project" / ".hsm" / "config.yaml"
        self._write_yaml(project_path, "local:\n  gpus: 1\n")
        result = HSMConfig.load(
            config_path=project_path,
            machine_config_path=machine_path,
        )
        assert result is not None
        spec = result.get_local_spec()
        assert spec is not None and spec.gpus == 1


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
