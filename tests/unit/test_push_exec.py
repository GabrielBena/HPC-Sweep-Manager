"""Unit tests for the pure push-execution helpers."""

from __future__ import annotations

import subprocess

import pytest

from hpc_sweep_manager.core.remote.push_exec import (
    DEFAULT_RSYNC_EXCLUDES,
    RSYNC_SSH,
    build_rsync_pull_cmd,
    build_rsync_push_cmd,
    normalize_gpu_allowlist,
    own_snapshot,
    partition_gpu_slots,
    pin_code_refs,
    resolve_run_prefix,
    snapshot_prepare_cmd,
    warn_cpu_fallback,
)


class TestNormalizeGpuAllowlist:
    def test_none_uses_all_detected(self):
        assert normalize_gpu_allowlist(None, [0, 1, 2, 3]) == [0, 1, 2, 3]

    def test_none_skips_busy_gpus(self):
        assert normalize_gpu_allowlist(None, [0, 1, 2, 3], busy=[0, 2]) == [1, 3]

    def test_an_explicit_allowlist_keeps_busy_gpus(self):
        assert normalize_gpu_allowlist("all", [0, 1, 2, 3], busy=[0, 2]) == [0, 1, 2, 3]
        assert normalize_gpu_allowlist([0, 1], [0, 1, 2, 3], busy=[0, 2]) == [0, 1]
        assert normalize_gpu_allowlist(1, [0, 1, 2, 3], busy=[0]) == [0]

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

    def test_request_exceeds_supply_falls_back_to_cpu_with_no_gpu_visible(self):
        assert partition_gpu_slots([0], 2, cpu_slots=3) == [[], [], []]

    def test_no_gpus_uses_cpu_slots_with_no_gpu_visible(self):
        assert partition_gpu_slots([], 1, cpu_slots=2) == [[], []]
        assert partition_gpu_slots([], 0, cpu_slots=2) == [[], []]  # --gpus cpu

    def test_gpus_per_job_zero_leaves_the_environment_alone(self):
        assert partition_gpu_slots([0, 1], 0, cpu_slots=2) == [None, None]

    def test_cpu_slots_floor_of_one(self):
        assert partition_gpu_slots([], 1, cpu_slots=0) == [[]]

    def test_a_gpu_job_left_on_cpu_warns(self, caplog):
        with caplog.at_level("WARNING"):
            warn_cpu_fallback("box", [None], 0, [0, 1])  # asked for none: silent
            warn_cpu_fallback("box", [[0]], 1, [0, 1])  # on a GPU: silent
            assert not caplog.records
            warn_cpu_fallback("box", [[], []], 2, [3])
        [r] = caplog.records
        assert r.message.startswith("box: 2 GPU(s) per task, but 1 allowed GPU(s) [3]")


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

    def test_ssh_transport_never_prompts_or_hangs(self):
        # A password prompt or a dead link must fail the rsync, not hang the launcher.
        for cmd in (
            build_rsync_push_cmd("/l", "h", "/r", []),
            build_rsync_pull_cmd("h", "/r", "/l", ["resume/"]),
        ):
            assert cmd[cmd.index("-e") + 1] == RSYNC_SSH
        for opt in ("BatchMode=yes", "ConnectTimeout=30", "ServerAliveInterval=30"):
            assert f"-o {opt}" in RSYNC_SSH

    def test_agentless_skips_the_ssh_agent(self):
        for build in (
            lambda **kw: build_rsync_push_cmd("/l", "h", "/r", [], **kw),
            lambda **kw: build_rsync_pull_cmd("h", "/r", "/l", **kw),
        ):
            assert "IdentityAgent" not in " ".join(build())
            ssh = build(agentless=True)
            assert ssh[ssh.index("-e") + 1] == RSYNC_SSH + " -o IdentityAgent=none"


class TestDefaultExcludes:
    """The push payload should skip common ML artifact dirs/files (#5)."""

    def test_includes_ml_artifact_patterns(self):
        for pat in ("*.pth", "/checkpoints", "/multirun", ".hydra"):
            assert pat in DEFAULT_RSYNC_EXCLUDES, pat
        # Pre-existing patterns still present.
        for pat in (".git", "/wandb", "*.ckpt", "*.pt", "sweeps/outputs"):
            assert pat in DEFAULT_RSYNC_EXCLUDES, pat

    def test_does_not_exclude_input_ambiguous_patterns(self):
        # Bare `outputs` (could be a source dir) and `*.pkl` (often INPUT data)
        # are NOT default-excluded — opt in per-remote if they're artifacts.
        assert "outputs" not in DEFAULT_RSYNC_EXCLUDES
        assert "*.pkl" not in DEFAULT_RSYNC_EXCLUDES

    def test_output_dir_names_are_anchored(self):
        # B2 (field report 2026-06-03): an UNANCHORED dir name also matches a
        # Hydra config GROUP of the same name (`configs/wandb/`) and silently
        # strips it from the push — with no per-remote un-exclude mechanism.
        for bare in ("wandb", "checkpoints", "multirun"):
            assert bare not in DEFAULT_RSYNC_EXCLUDES, bare
            assert f"/{bare}" in DEFAULT_RSYNC_EXCLUDES, bare


class TestExcludeRsyncSemantics:
    """Pin the actual rsync filter behavior of the default excludes — tuple
    membership alone doesn't prove `configs/wandb/` survives a push."""

    @pytest.fixture()
    def pushed(self, tmp_path):
        import shutil
        import subprocess

        if shutil.which("rsync") is None:
            pytest.skip("rsync not available")

        src = tmp_path / "src"
        dst = tmp_path / "dst"
        dst.mkdir()
        for rel in (
            "wandb/run-1/log.txt",  # root output dir → dropped
            "checkpoints/model.bin",  # root output dir → dropped
            "multirun/2026/cfg.yaml",  # root output dir → dropped
            "configs/wandb/default.yaml",  # Hydra config GROUP → must ship
            "configs/checkpoints/opt.yaml",  # Hydra config GROUP → must ship
            "configs/multirun/sweep.yaml",  # Hydra config GROUP → must ship
            "sub/wandb/nested.txt",  # nested junk → ships (accepted cost)
            "scripts/train.py",
        ):
            p = src / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("x")

        # Same command construction as the real push; local→local instead of
        # over SSH (strip the `host:` prefix off the destination).
        cmd = build_rsync_push_cmd(str(src), "HOST", str(dst), DEFAULT_RSYNC_EXCLUDES)
        cmd[-1] = cmd[-1].removeprefix("HOST:")
        subprocess.run(cmd, check=True, capture_output=True)
        return dst

    def test_config_groups_survive_the_push(self, pushed):
        assert (pushed / "configs/wandb/default.yaml").is_file()
        assert (pushed / "configs/checkpoints/opt.yaml").is_file()
        assert (pushed / "configs/multirun/sweep.yaml").is_file()

    def test_root_output_dirs_are_dropped(self, pushed):
        assert not (pushed / "wandb").exists()
        assert not (pushed / "checkpoints").exists()
        assert not (pushed / "multirun").exists()

    def test_nested_same_name_dirs_now_ship(self, pushed):
        # Documented trade-off of anchoring: nested junk re-pushes (weight
        # globs still strip the heavy files). Add an unanchored per-remote
        # exclude if a project needs the old behavior.
        assert (pushed / "sub/wandb/nested.txt").is_file()


class TestCodeSnapshots:
    """Per-sweep code snapshots (tracker S4): queued tasks never run a later push's code."""

    def test_the_push_hard_links_against_the_previous_snapshot(self):
        cmd = build_rsync_push_cmd("/p", "h", "/r/p/snapshots/s2", [], link_dest="/r/p/snapshots/s")
        assert "--link-dest=/r/p/snapshots/s" in cmd
        assert cmd[-1] == "h:/r/p/snapshots/s2/"

    def test_prepare_links_against_the_newest_code_and_never_touches_the_legacy_dir(self, tmp_path):
        root = tmp_path / "proj"
        (root / "code").mkdir(parents=True)
        (root / "code" / "legacy.py").write_text("x")

        def prepare(sweep_id):
            cmd = snapshot_prepare_cmd(str(root), sweep_id, [f"{root}/sweeps/{sweep_id}/tasks"])
            return subprocess.run(["bash", "-c", cmd], capture_output=True, text=True).stdout

        assert prepare("s1").strip() == f"{root}/code/"  # the first snapshot's link base
        assert prepare("s2").strip() == f"{root}/snapshots/s1/"
        assert (root / "snapshots" / "s2").is_dir() and (root / "sweeps" / "s2" / "tasks").is_dir()
        assert (root / "code" / "legacy.py").read_text() == "x"  # left alone

    def test_pre_script_references_to_the_old_dir_are_pinned_to_the_snapshot(self, caplog):
        lines = (
            "export PYTHONPATH=/scratch/u/runs/proj/code:$PYTHONPATH",  # uzh's form
            'export PYTHONPATH="$HOME/.hsm/runs/proj/code/sub"',  # athena's form, nested
            "export DATA=/scratch/u/proj/codes:/data/other/code",  # neither is the old dir
            "export BIND=/data,/scratch/u/runs/proj/code; cd /x/proj/code&&ls",  # , ; && end paths
        )
        assert pin_code_refs(lines, "proj") == (
            "export PYTHONPATH=$HSM_CODE_DIR:$PYTHONPATH",
            'export PYTHONPATH="$HSM_CODE_DIR/sub"',
            "export DATA=/scratch/u/proj/codes:/data/other/code",
            "export BIND=/data,$HSM_CODE_DIR; cd $HSM_CODE_DIR&&ls",
        )
        assert "$HSM_CODE_DIR" in caplog.text
        assert "can't rewrite" not in caplog.text

    def test_command_substitutions_are_pinned_and_assembled_paths_left_alone(self, caplog):
        assert pin_code_refs(["export P=$(realpath /r/proj/code)"], "proj") == (
            "export P=$(realpath $HSM_CODE_DIR)",
        )
        # A path assembled from a variable is invisible to HSM: left as written.
        assert pin_code_refs(['R=/r/proj; export P="$R/code"'], "proj") == (
            'R=/r/proj; export P="$R/code"',
        )

    def test_a_cleanup_may_only_delete_the_sweep_s_own_snapshot(self):
        assert own_snapshot("/r/p/snapshots/sw1", "sw1") == "/r/p/snapshots/sw1"
        assert own_snapshot("/r/p/code", "sw1") is None  # an old manifest's shared dir
        assert own_snapshot("/r/p/snapshots/sw2", "sw1") is None
        assert own_snapshot(None, "sw1") is None
