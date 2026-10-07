"""Every task template, rendered and run under bash with a stub trainer (chore-5): the
contract a consumer sees is the trainer's argv and cwd, and the task dir's files."""

from __future__ import annotations

import pytest
import yaml
from fakes import run_template

TEMPLATES = [
    "local_compute_source.sh.j2",
    "ssh_compute_source.sh.j2",
    "slurm_single.sh.j2",
    "slurm_array.sh.j2",
]
# A list (its repr has a space), null, and a string with both quotes, a `;` and a trailing
# backslash: each once broke a template's COMMAND string, or would have (R13).
PARAMS = {"lr": 0.01, "layers": [64, 64], "seed": None, "note": 'it\'s; "fine" \\'}


@pytest.mark.parametrize("name", TEMPLATES)
def test_a_task_runs_with_its_params_and_overrides(tmp_path, name):
    proc, seen = run_template(name, tmp_path, PARAMS)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    task = tmp_path / "tasks" / "task_1"
    assert seen == {
        "argv": ["lr=0.01", "layers=[64, 64]", "seed=null", f"note={PARAMS['note']}"]
        + ["wandb.group=g", f"output.dir={task}", f"hydra.run.dir={task}/.hydra_run"],
        "cwd": str(tmp_path / "proj"),
    }
    assert "Status: SUCCESS" in (task / "task_info.txt").read_text()
    assert yaml.safe_load((task / "params.yaml").read_text()) == PARAMS


@pytest.mark.parametrize("name", TEMPLATES)
def test_a_failing_task_says_so_and_keeps_its_exit_code(tmp_path, name):
    proc, seen = run_template(name, tmp_path, PARAMS, rc=3)
    assert seen is not None and proc.returncode == 3, proc.stdout + proc.stderr
    info = (tmp_path / "tasks" / "task_1" / "task_info.txt").read_text()
    assert "Status: FAILED" in info and "Exit Code: 3" in info


def test_the_ssh_wrapper_leaves_its_exit_code_for_the_poll(tmp_path):
    for rc in (0, 3):
        run_template("ssh_compute_source.sh.j2", tmp_path, PARAMS, rc=rc)
        assert (tmp_path / "tasks" / "task_1" / ".hsm_rc").read_text() == f"{rc}\n"
