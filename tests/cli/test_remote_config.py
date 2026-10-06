"""CLI tests for `hsm remote add` / `hsm remote remove` (C3).

They edit the PROJECT config file only: never the machine-merged view, never
replacing an existing entry wholesale, and never rewriting (so stripping) a
file with comments.
"""

from __future__ import annotations

import pytest
import yaml
from click.testing import CliRunner

from hpc_sweep_manager.cli import remote as remote_cli


@pytest.fixture
def project(tmp_path, monkeypatch):
    """A temp project as cwd, plus a machine config that must never leak into it."""
    machine = tmp_path / "home" / ".hsm" / "config.yaml"
    machine.parent.mkdir(parents=True)
    machine.write_text(yaml.safe_dump({"local": {"sweeps_root": "/mnt/big", "visible_gpus": [2]}}))
    monkeypatch.setattr("hpc_sweep_manager.core.common.config.MACHINE_CONFIG_PATH", machine)
    proj = tmp_path / "proj"
    (proj / ".hsm").mkdir(parents=True)
    monkeypatch.chdir(proj)
    return proj / ".hsm" / "config.yaml"


def _invoke(*args):
    return CliRunner().invoke(remote_cli.remote, list(args))


UZH = {"backend": "slurm", "workdir": "/scratch/u/hsm-runs", "spec": {"account": "lab"}}


class TestAdd:
    def test_machine_config_never_leaks_into_project_file(self, project):
        project.write_text(yaml.safe_dump({"paths": {"conda_env": "env"}}))
        result = _invoke("add", "box", "box.lab")
        assert result.exit_code == 0, result.output
        data = yaml.safe_load(project.read_text())
        assert "local" not in data
        assert data["paths"] == {"conda_env": "env"}
        assert data["distributed"]["remotes"]["box"] == {"host": "box.lab"}

    def test_existing_entry_fields_survive(self, project):
        project.write_text(yaml.safe_dump({"distributed": {"remotes": {"uzh": dict(UZH)}}}))
        result = _invoke("add", "uzh", "--max-jobs", "4", "--disabled")
        assert result.exit_code == 0, result.output
        entry = yaml.safe_load(project.read_text())["distributed"]["remotes"]["uzh"]
        assert entry == {**UZH, "max_parallel_jobs": 4, "enabled": False}

    def test_bootstraps_missing_project_file(self, project):
        result = _invoke("add", "box")
        assert result.exit_code == 0, result.output
        data = yaml.safe_load(project.read_text())
        assert data["distributed"]["remotes"]["box"] == {}
        assert "local" not in data

    def test_commented_file_left_byte_identical_and_snippet_printed(self, project):
        text = "# my notes\ndistributed:\n  remotes:\n    uzh:\n      backend: slurm  # cluster\n"
        project.write_text(text)
        result = _invoke("add", "uzh", "uzh.host")
        assert result.exit_code != 0
        assert project.read_text() == text
        snippet = result.output[: result.output.index("Error")]
        entry = yaml.safe_load(snippet)["distributed"]["remotes"]["uzh"]
        assert entry == {"backend": "slurm", "host": "uzh.host"}
        assert "has comments" in result.output


class TestRemove:
    def test_removes_only_that_entry_from_project_file(self, project):
        remotes = {"uzh": dict(UZH), "box": {"host": "box"}}
        project.write_text(yaml.safe_dump({"distributed": {"remotes": remotes}}))
        result = _invoke("remove", "box", "--yes")
        assert result.exit_code == 0, result.output
        data = yaml.safe_load(project.read_text())
        assert data["distributed"]["remotes"] == {"uzh": UZH}
        assert "local" not in data

    def test_commented_file_left_byte_identical(self, project):
        text = "distributed:\n  remotes:\n    box:\n      host: box  # lab box\n"
        project.write_text(text)
        result = _invoke("remove", "box", "--yes")
        assert result.exit_code != 0
        assert project.read_text() == text
        assert "box:" in result.output and "Delete this entry" in result.output
