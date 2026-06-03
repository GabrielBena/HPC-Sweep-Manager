"""Unit tests for build_compute_source's remote backend dispatch + submission.

Focus: array-over-SSH wiring (#3). The SSH-Slurm source already implements
array submission; these assert the orchestrator returns the chosen submission
style for backend=slurm and falls back to individual (with a warning) for
backend=ssh.
"""

from __future__ import annotations

from hpc_sweep_manager.core.common.sweep_orchestrator import build_compute_source


class FakeConfig:
    def __init__(self, data):
        self.config_data = data

    def get_conda_env(self):
        return None


def _cfg(backend):
    return FakeConfig(
        {
            "distributed": {
                "remotes": {
                    "r": {
                        "host": "r",
                        "backend": backend,
                        "spec": {"walltime": "01:00:00"},
                    }
                }
            }
        }
    )


def _build(backend, remote_submission, tmp_path):
    return build_compute_source(
        mode="remote",
        python_path="python",
        script_path="train.py",
        project_dir=str(tmp_path),
        hsm_config=_cfg(backend),
        remote_alias="r",
        remote_submission=remote_submission,
    )


class TestRemoteSubmission:
    def test_slurm_array_returns_array(self, tmp_path):
        _src, mode, sub = _build("slurm", "array", tmp_path)
        assert mode == "remote"
        assert sub == "array"

    def test_slurm_default_is_individual(self, tmp_path):
        _src, _mode, sub = _build("slurm", None, tmp_path)
        assert sub == "individual"

    def test_slurm_explicit_individual(self, tmp_path):
        _src, _mode, sub = _build("slurm", "individual", tmp_path)
        assert sub == "individual"

    def test_ssh_array_falls_back_to_individual(self, tmp_path, caplog):
        # Bash SSH can't do array submission → warn + individual.
        import logging

        with caplog.at_level(logging.WARNING):
            _src, _mode, sub = _build("ssh", "array", tmp_path)
        assert sub == "individual"
        assert any("array" in r.message.lower() for r in caplog.records)
