"""Known config keys (R4) and path checks before a run (R6)."""

from __future__ import annotations

import pytest

from hpc_sweep_manager.core.common.config import HSMConfig, config_warnings

SPEC = {"partition": "standard", "account": "a", "qos": "medium", "cpus_per_task": 4}

# Every block, every kind of key: none of it may warn.
FULL = {
    "project": {"name": "p", "root": "/p"},
    "paths": {"conda_env": "e", "train_script": "t.py", "config_dir": "c", "output_dir": "o"},
    "wandb": {"project": "p", "entity": ""},
    "metadata": {"created_at": "2026", "created_by": "HSM", "version": "1.0.0", "free": 1},
    "local": {"gpus": 1, "visible_gpus": [1, 2], "sweeps_root": "/s", "walltime": "1:00:00"},
    "slurm": {**SPEC, "gpu_type": "H100", "qos_whitelist": ["normal"], "max_array_size": 9},
    "distributed": {
        "enabled": False,
        "local_max_jobs": 1,
        "remote_root": "~/r",
        "remotes": {
            "uzh": {
                "backend": "slurm",
                "host": "uzh",
                "workdir": "/scratch",
                "archive_dir": "/shares",
                "archive_on": "completed",
                "max_parallel_jobs": 350,
                "speed_factors": {"a100": 1.0},
                "resumable": {"chunk_walltime": "23:00:00"},
                "spec": {**SPEC, "pre_script": ["x"], "array_throttle": 50, "gpus": 1},
            },
            "athena": {"backend": "ssh", "gpus": "cpu", "python_path": "/py", "ssh_port": 22},
        },
    },
}

# The shape of Comp-PVR's .hsm/config.yaml (2026-10-07), comments dropped.
COMP_PVR = {
    "distributed": {
        "enabled": False,
        "remotes": {
            "uzh": {
                "enabled": True,
                "backend": "slurm",
                "host": "uzh",
                "workdir": "/scratch/gbena/hsm-runs",
                "archive_dir": "/shares/payvand.ini.uzh/hsm-archive",
                "archive_on": "completed",
                "max_parallel_jobs": 350,
                "spec": {
                    "partition": "standard",
                    "extra_directives": {"--exclude": "u24-a0-[1-2]"},
                    "account": "payvand.ini.uzh",
                    "qos": "medium",
                    "cpus_per_task": 4,
                    "mem": "8G",
                    "walltime": "47:30:00",
                    "pre_script": ["export OMP_NUM_THREADS=1"],
                },
            },
            "athena": {
                "enabled": True,
                "backend": "ssh",
                "host": "athena",
                "max_parallel_jobs": 8,
                "python_path": "/home/gbena/miniforge3/envs/cpvr/bin/python",
                "spec": {"cpus_per_task": 4, "walltime": "96:00:00", "pre_script": ["x"]},
            },
        },
        "strategy": "round_robin",
        "sync_method": "rsync",
    },
    "local": {"gpus": 1, "sweeps_root": "/mnt/8TB_HDD/gbena/hsm-sweeps"},
    "metadata": {"created_at": "2026-06-03", "created_by": "HSM", "version": "1.0.0"},
    "paths": {"conda_env": "cpvr", "config_dir": "c", "output_dir": "outputs", "train_script": "t"},
    "project": {"name": "Comp-PVR", "root": "/r"},
    "wandb": {"entity": "", "project": "Comp-PVR"},
}

SWEEP = {
    "defaults": ["override hydra/launcher: basic"],
    "sweep": {"grid": {"lr": [1]}, "paired": [], "cost_param": "lr", "resumable": {}},
    "metadata": {"anything": 1},
    "script": "train.py",
    "resumable": {"enabled": True, "chunk_walltime": "23:00:00", "max_chunks": 3},
}


class TestConfigWarnings:
    def test_valid_configs_and_sweeps_are_silent(self):
        assert config_warnings(FULL, SWEEP) == []
        # Comp-PVR's only unknown keys are the old dispatcher's, which change nothing since X3.
        assert config_warnings(COMP_PVR) == [
            f"HSM config: `distributed.{k}` is not a known key, so it is ignored"
            for k in ("strategy", "sync_method")
        ]
        assert config_warnings(None, {"grid": {"lr": [1]}, "cost_param": "lr"}) == []  # flat

    @pytest.mark.parametrize(
        ("config", "sweep", "hint"),
        [
            ({}, {"sweep": {"gird": {}}}, "`sweep.gird` is not a known key"),
            ({}, {"sweep": {"gird": {}}}, "did you mean `sweep.grid`?"),
            ({}, {"gird": {}}, "did you mean `grid`?"),  # a flat sweep file
            ({}, {"sweep": {}, "grid": {}}, "did you mean `sweep.grid`?"),  # beside `sweep:`
            ({}, {"sweep": {"script": "t.py"}}, "did you mean `script`?"),  # one level up
            ({}, {"resumable": {"max_chunk": 3}}, "did you mean `resumable.max_chunks`?"),
            ({"remotes": {}}, None, "did you mean `distributed.remotes`?"),
            ({"train_script": "t"}, None, "did you mean `paths.train_script`?"),
            ({"local": {"gpu_type": "H100"}}, None, "did you mean `slurm.gpu_type`?"),
            ({"paths": {"train_scirpt": "t"}}, None, "did you mean `paths.train_script`?"),
            ({"distributed": {"local_max_job": 1}}, None, "`distributed.local_max_jobs`?"),
        ],
    )
    def test_unknown_key_with_suggestion(self, config, sweep, hint):
        (warning,) = config_warnings(config, sweep)
        assert hint in warning

    def test_remote_level_spec_key_points_into_spec(self):
        remote = {"backend": "slurm", "pre_script": ["x"], "walltime": "1:00:00", "gpus": [1]}
        warnings = config_warnings({"distributed": {"remotes": {"uzh": remote}}})
        p = "distributed.remotes.uzh"
        assert warnings == [
            f"HSM config: `{p}.pre_script` is not a known key, so it is ignored; "
            f"did you mean `{p}.spec.pre_script`?",
            f"HSM config: `{p}.walltime` is not a known key, so it is ignored; "
            f"did you mean `{p}.spec.walltime`?",
        ]  # `gpus` is known at both levels (the allowlist, the per-task count)

    def test_spec_level_remote_key_points_up(self):
        remote = {"spec": {"speed_factors": {"a100": 1.0}}, "resumable": {"chunk_walltim": "1"}}
        warnings = config_warnings({"distributed": {"remotes": {"uzh": remote}}})
        assert "did you mean `distributed.remotes.uzh.speed_factors`?" in warnings[0]
        assert "did you mean `distributed.remotes.uzh.resumable.chunk_walltime`?" in warnings[1]

    def test_unknown_key_without_a_near_one(self):
        (warning,) = config_warnings({"hpc": {}})
        assert warning == "HSM config: `hpc` is not a known key, so it is ignored"

    def test_hydra_overrides_names_are_checked(self):
        assert config_warnings({"hydra_overrides": ["output.dir", "hydra.run.dir"]}) == []
        (warning,) = config_warnings({"hydra_overrides": ["output.dir", "wandb_group"]})
        assert "`hydra_overrides: wandb_group` is not a known key" in warning
        assert "did you mean `hydra_overrides: wandb.group`?" in warning

    @pytest.mark.parametrize(
        ("value", "keys"),
        [
            (None, None),  # unset: every source appends all of HSM's overrides
            ([], ()),
            (["hydra.run.dir", "output.dir"], ("hydra.run.dir", "output.dir")),
            (["wandb_group", "output.dir", {"x": 1}], ("output.dir",)),  # unknowns dropped
            ("output.dir", ("output.dir",)),
        ],
    )
    def test_hydra_overrides_getter(self, value, keys):
        cfg = {} if value is None else {"hydra_overrides": value}
        assert HSMConfig(cfg).get_hydra_overrides() == keys

    def test_non_mapping_blocks_do_not_crash(self):
        bad = {"local": "x", "distributed": {"remotes": ["uzh"]}, "hydra_overrides": 5}
        assert config_warnings(bad, {"sweep": None}) == [
            "HSM config: `hydra_overrides: 5` is not a known key, so it is ignored"
        ]


class TestCheckPaths:
    def test_existing_paths_pass(self, tmp_path):
        (tmp_path / "train.py").write_text("")
        cfg = {"project": {"root": str(tmp_path)}, "paths": {"train_script": "train.py"}}
        assert HSMConfig(cfg).check_paths() == []  # relative to the project root

    def test_home_in_a_path_is_expanded(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        (tmp_path / "proj").mkdir()
        (tmp_path / "proj" / "train.py").write_text("")
        cfg = {"project": {"root": "~/proj"}, "paths": {"train_script": "$HOME/proj/train.py"}}
        assert HSMConfig(cfg).check_paths() == []

    def test_stale_root_and_script_are_named_with_their_keys(self, tmp_path):
        old = tmp_path / "moved"
        cfg = {"project": {"root": str(old)}, "paths": {"train_script": str(old / "t.py")}}
        assert HSMConfig(cfg).check_paths() == [
            f"`project.root` = '{old}' does not exist on this machine",
            f"`paths.train_script` = '{old / 't.py'}' does not exist on this machine",
        ]

    def test_the_sweep_script_wins_over_train_script(self, tmp_path):
        (tmp_path / "sweep_train.py").write_text("")
        cfg = HSMConfig({"project": {"root": str(tmp_path)}, "paths": {"train_script": "gone.py"}})
        assert cfg.check_paths("sweep_train.py") == []
        assert cfg.check_paths("gone_too.py") == [
            "the sweep file's `script` = 'gone_too.py' does not exist on this machine"
        ]
