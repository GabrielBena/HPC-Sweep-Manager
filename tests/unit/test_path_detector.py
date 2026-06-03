"""Unit tests for PathDetector training-script detection.

Focus: the multi-candidate API behind the P0 fix (FEEDBACK.md) — detection must
surface every plausible entrypoint (so `hsm setup init` can warn) while still
skipping vendored deps and real virtualenvs, and NOT silently hiding a script
that happens to live in a legitimately-named dir such as ``env/``.
"""

from __future__ import annotations

from pathlib import Path

from hpc_sweep_manager.core.common.path_detector import PathDetector


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x")


class TestTrainScriptCandidates:
    def test_field_report_scenario_surfaces_both(self, tmp_path):
        # The bug that motivated P0: train.py + train_2d.py side by side.
        _touch(tmp_path / "scripts" / "train.py")
        _touch(tmp_path / "scripts" / "train_2d.py")
        det = PathDetector(tmp_path)
        rels = [str(p.relative_to(tmp_path)) for p in det.detect_train_script_candidates()]
        assert "scripts/train.py" in rels and "scripts/train_2d.py" in rels
        # The canonical name is the auto-pick (first), matching detect_train_script.
        assert rels[0] == "scripts/train.py"
        assert det.detect_train_script() == tmp_path / "scripts" / "train.py"

    def test_single_candidate_is_unambiguous(self, tmp_path):
        _touch(tmp_path / "train.py")
        det = PathDetector(tmp_path)
        assert det.detect_train_script_candidates() == [tmp_path / "train.py"]

    def test_none_when_no_script(self, tmp_path):
        det = PathDetector(tmp_path)
        assert det.detect_train_script_candidates() == []
        assert det.detect_train_script() is None

    def test_source_dir_named_env_is_not_skipped(self, tmp_path):
        # Regression guard: env/ is a common RL/gym source dir, not a virtualenv.
        _touch(tmp_path / "env" / "train.py")
        det = PathDetector(tmp_path)
        assert det.detect_train_script() == tmp_path / "env" / "train.py"

    def test_real_virtualenv_named_env_is_skipped(self, tmp_path):
        # A genuine virtualenv (pyvenv.cfg present) must be excluded even though
        # it is named env/ — so we don't surface its vendored scripts.
        _touch(tmp_path / "env" / "pyvenv.cfg")
        _touch(tmp_path / "env" / "bin" / "trainer.py")
        _touch(tmp_path / "scripts" / "train.py")
        det = PathDetector(tmp_path)
        rels = [str(p.relative_to(tmp_path)) for p in det.detect_train_script_candidates()]
        assert rels == ["scripts/train.py"]

    def test_virtualenv_via_bin_activate_is_skipped(self, tmp_path):
        _touch(tmp_path / "myenv" / "bin" / "activate")
        _touch(tmp_path / "myenv" / "bin" / "run.py")
        _touch(tmp_path / "main.py")
        det = PathDetector(tmp_path)
        rels = [str(p.relative_to(tmp_path)) for p in det.detect_train_script_candidates()]
        assert rels == ["main.py"]

    def test_vendored_and_hidden_dirs_skipped(self, tmp_path):
        _touch(tmp_path / "lib" / "site-packages" / "pkg" / "main.py")
        _touch(tmp_path / "node_modules" / "thing" / "run.py")
        _touch(tmp_path / ".cache" / "train.py")
        _touch(tmp_path / "train.py")
        det = PathDetector(tmp_path)
        rels = [str(p.relative_to(tmp_path)) for p in det.detect_train_script_candidates()]
        assert rels == ["train.py"]

    def test_candidates_are_deduped(self, tmp_path):
        # scripts/train.py matches both the priority list and the *train*.py glob.
        _touch(tmp_path / "scripts" / "train.py")
        det = PathDetector(tmp_path)
        cands = det.detect_train_script_candidates()
        assert len(cands) == 1

    def test_get_project_info_exposes_candidates(self, tmp_path):
        _touch(tmp_path / "scripts" / "train.py")
        _touch(tmp_path / "scripts" / "train_2d.py")
        info = PathDetector(tmp_path).get_project_info()
        assert "train_script_candidates" in info
        assert len(info["train_script_candidates"]) == 2


class TestDetectHpcSystem:
    """Scheduler detection order: Slurm > SGE (qstat+qsub) > PBS (qstat) >
    unknown. qstat is ambiguous so it must not short-circuit ahead of Slurm/SGE,
    and a box with no scheduler must report 'unknown', not 'pbs'."""

    @staticmethod
    def _which(present):
        present = set(present)
        return lambda cmd: ("/usr/bin/" + cmd) if cmd in present else None

    def test_slurm_wins_even_with_qstat_present(self, monkeypatch):
        monkeypatch.setattr("shutil.which", self._which({"sbatch", "sinfo", "qstat"}))
        assert PathDetector().detect_hpc_system() == "slurm"

    def test_sbatch_only_is_slurm(self, monkeypatch):
        monkeypatch.setattr("shutil.which", self._which({"sbatch"}))
        assert PathDetector().detect_hpc_system() == "slurm"

    def test_sge_needs_qstat_and_qsub(self, monkeypatch):
        monkeypatch.setattr("shutil.which", self._which({"qstat", "qsub"}))
        assert PathDetector().detect_hpc_system() == "sge"

    def test_pbs_is_qstat_only(self, monkeypatch):
        monkeypatch.setattr("shutil.which", self._which({"qstat"}))
        assert PathDetector().detect_hpc_system() == "pbs"

    def test_no_scheduler_is_unknown_not_pbs(self, monkeypatch):
        # Field-report scenario: a box that only drives Slurm over SSH has no
        # local scheduler — must NOT mis-report PBS.
        monkeypatch.setattr("shutil.which", self._which(set()))
        assert PathDetector().detect_hpc_system() == "unknown"
