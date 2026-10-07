"""Task dir names (R5, FR#18): one reader for the three schemes the sources write.

``task_7`` (array), ``task_007`` (local, ssh), ``<sweep_id>_task_007`` (individual Slurm jobs).
The names stay as they are (consumers' scripts read them); the analyzer reads all three.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from hpc_sweep_manager.core.common.sweep_analysis import SweepCompletionAnalyzer
from hpc_sweep_manager.core.common.utils import task_index

SWEEP = "sweep_20261006_120000"


@pytest.mark.parametrize(
    "name, index",
    [
        ("task_7", 7),
        ("task_007", 7),
        (f"{SWEEP}_task_007", 7),
        ("my_task_3_sweep_task_012", 12),  # the last task_<N> is the task's, not the prefix's
        ("task_1234", 1234),
    ],
)
def test_task_index_reads_every_scheme(name, index):
    assert task_index(name) == index


@pytest.mark.parametrize(
    "name", ["task_info.txt", ".rsync-partial", "task_", "task_7.slurm", "xtask_7", "logs", ""]
)
def test_task_index_is_none_for_anything_else(name):
    assert task_index(name) is None


def _sweep(root: Path, tasks: dict[str, str], n: int) -> Path:
    """A sweep dir with an ``lr`` grid of ``n`` values and one task_info.txt per given task dir."""
    (root / "sweep_config.yaml").write_text(f"sweep:\n  grid:\n    lr: {list(range(n))}\n")
    for name, status in tasks.items():
        (root / "tasks" / name).mkdir(parents=True)
        (root / "tasks" / name / "task_info.txt").write_text(f"Status: {status}\n")
    return root


def test_analyzer_reads_an_individual_mode_sweep(tmp_path):
    # Individual Slurm jobs name their dirs after the job; they all read as missing before.
    _sweep(
        tmp_path,
        {
            f"{SWEEP}_task_001": "SUCCESS",
            f"{SWEEP}_task_002": "FAILED",
            f"{SWEEP}_task_004": "RUNNING",
        },
        n=4,
    )
    (tmp_path / "tasks" / ".rsync-partial").mkdir()
    a = SweepCompletionAnalyzer(tmp_path).analyze_from_task_directories()
    assert a["completed_tasks"] == [f"{SWEEP}_task_001"]
    assert a["failed_tasks"] == [f"{SWEEP}_task_002"]
    assert a["missing_task_numbers"] == [3]
    assert a["missing_combinations"] == [{"lr": 2}]
    assert a["failed_combinations"] == [{"lr": 1}]
    assert sorted(a["task_statuses"]) == [f"{SWEEP}_task_00{i}" for i in (1, 2, 4)]


def test_analyzer_maps_a_mixed_source_mapping(tmp_path):
    # A distributed sweep: a local child's task_001 beside a Slurm child's <id>_task_002.
    _sweep(tmp_path, {"task_001": "COMPLETED", f"{SWEEP}_task_002": "SUCCESS"}, n=3)
    (tmp_path / "source_mapping.yaml").write_text(
        "task_assignments:\n"
        "  task_001: {status: COMPLETED}\n"
        f"  {SWEEP}_task_002: {{status: COMPLETED}}\n"
    )
    a = SweepCompletionAnalyzer(tmp_path).analyze_completion_status(verify_running=False)
    assert a["total_completed"] == 2
    assert a["missing_combinations"] == [{"lr": 2}]
