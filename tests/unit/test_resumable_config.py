"""Unit tests for ResumableConfig + the precedence resolver (issue #12)."""

from __future__ import annotations

from hpc_sweep_manager.core.common.resumable import (
    ResumableConfig,
    ResumableContext,
    is_hms,
    resolve_resumable_config,
)


class TestFromDict:
    def test_defaults(self):
        c = ResumableConfig.from_dict(None)
        assert c.enabled is False
        assert c.chunk_walltime is None
        assert c.signal_grace == 120
        assert c.resume_arg is None  # env-only default (generality guardrail)
        assert c.done_sentinel == ".hsm_done"
        assert c.checkpoint_subdir == "resume"
        assert c.max_chunks == 10
        assert c.max_consecutive_failures == 2

    def test_unknown_key_dropped(self):
        c = ResumableConfig.from_dict({"enabled": True, "bogus": 7, "chunk_walltime": "23:00:00"})
        assert c.enabled is True
        assert not hasattr(c, "bogus")

    def test_int_coercion_from_yaml_strings(self):
        c = ResumableConfig.from_dict({"signal_grace": "90", "max_chunks": "4"})
        assert c.signal_grace == 90
        assert c.max_chunks == 4

    def test_resume_arg_defaults_to_env_only(self):
        # The default is env-only (None): HSM_RESUME_FROM is always exported, so
        # no project-specific hydra key leaks into a general project's command.
        assert ResumableConfig.from_dict({}).resume_arg is None
        # A hydra project opts in explicitly.
        assert (
            ResumableConfig.from_dict({"resume_arg": "training.resume_from"}).resume_arg
            == "training.resume_from"
        )


class TestValidate:
    def test_ok(self):
        assert ResumableConfig.from_dict(
            {"enabled": True, "chunk_walltime": "23:00:00"}
        ).validate() == []

    def test_enabled_without_walltime(self):
        errs = ResumableConfig(enabled=True).validate()
        assert any("chunk_walltime is unset" in e for e in errs)

    def test_mm_ss_rejected(self):
        errs = ResumableConfig(enabled=True, chunk_walltime="23:00").validate()
        assert any("HH:MM:SS" in e for e in errs)

    def test_signal_grace_ge_walltime_rejected(self):
        errs = ResumableConfig(
            enabled=True, chunk_walltime="00:01:00", signal_grace=120
        ).validate()
        assert any("smaller than" in e for e in errs)

    def test_negative_signal_grace(self):
        errs = ResumableConfig(signal_grace=-1).validate()
        assert any("signal_grace must be >= 0" in e for e in errs)

    def test_bad_caps(self):
        assert any("max_chunks" in e for e in ResumableConfig(max_chunks=0).validate())
        assert any(
            "max_consecutive_failures" in e
            for e in ResumableConfig(max_consecutive_failures=0).validate()
        )


class TestIsHms:
    def test_three_parts(self):
        assert is_hms("23:00:00")
        assert is_hms("01:02:03")

    def test_two_parts_rejected(self):
        assert not is_hms("23:00")

    def test_garbage_rejected(self):
        assert not is_hms("later")
        assert not is_hms("")


class TestResolver:
    def test_precedence_remote_sweep_cli(self):
        # remote sets the cluster cap; sweep sets the workload guards; CLI
        # overrides the cap.
        c = resolve_resumable_config(
            sweep_block={"enabled": True, "max_chunks": 5, "resume_arg": "tr.rf"},
            remote_block={"chunk_walltime": "23:00:00", "signal_grace": 90},
            cli_enabled=None,
            cli_chunk_walltime="20:00:00",
        )
        assert c.enabled is True
        assert c.max_chunks == 5
        assert c.resume_arg == "tr.rf"
        assert c.signal_grace == 90  # from remote
        assert c.chunk_walltime == "20:00:00"  # CLI beats remote

    def test_cli_flag_flips_enabled(self):
        c = resolve_resumable_config(
            sweep_block={"enabled": False, "chunk_walltime": "23:00:00"},
            remote_block=None,
            cli_enabled=True,
            cli_chunk_walltime=None,
        )
        assert c.enabled is True

    def test_remote_block_workload_keys_ignored(self):
        # A per-remote block may only carry cluster-bound knobs; max_chunks
        # there is dropped (workload guard belongs in the sweep YAML).
        c = resolve_resumable_config(
            sweep_block={"enabled": True, "chunk_walltime": "23:00:00"},
            remote_block={"max_chunks": 99, "checkpoint_subdir": "ckpt"},
            cli_enabled=None,
            cli_chunk_walltime=None,
        )
        assert c.max_chunks == 10  # default, NOT the remote's 99
        assert c.checkpoint_subdir == "ckpt"  # cluster-bound knob honored


class TestContext:
    def test_resume_from_present_only_after_chunk_zero(self):
        cfg = ResumableConfig(enabled=True, chunk_walltime="23:00:00")
        assert ResumableContext(chunk_index=0, config=cfg).resume_from_present is False
        assert ResumableContext(chunk_index=1, config=cfg).resume_from_present is True

    def test_manifest_round_trip(self):
        cfg = ResumableConfig(enabled=True, chunk_walltime="23:00:00", max_chunks=4)
        assert ResumableConfig.from_manifest(cfg.to_manifest()) == cfg
