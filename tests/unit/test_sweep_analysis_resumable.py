"""The `.hsm_done` sentinel override in SweepCompletionAnalyzer (issue #12).

A resumable chunk that times out exits non-zero and writes
``Status: CHUNK_INCOMPLETE``/``FAILED`` — but the task may simply need another
chunk. The sentinel is the authoritative done-signal; status must reflect that.
"""

from __future__ import annotations

from pathlib import Path

from hpc_sweep_manager.core.common.sweep_analysis import SweepCompletionAnalyzer


def _mk(tmp_path: Path, name: str, *, sentinel=False, status=None) -> Path:
    d = tmp_path / "tasks" / name
    d.mkdir(parents=True)
    if sentinel:
        (d / ".hsm_done").write_text("done\n")
    if status:
        (d / "task_info.txt").write_text(f"Global Task Index: 1\nStatus: {status}\n")
    return d


class TestSentinelOverride:
    def test_sentinel_overrides_stale_failed(self, tmp_path):
        _mk(tmp_path, "task_1", sentinel=True, status="FAILED")
        a = SweepCompletionAnalyzer(tmp_path)
        assert a._get_actual_task_status("task_1", verify_running=False) == "COMPLETED"

    def test_sentinel_overrides_chunk_incomplete(self, tmp_path):
        _mk(tmp_path, "task_2", sentinel=True, status="CHUNK_INCOMPLETE")
        a = SweepCompletionAnalyzer(tmp_path)
        assert a._get_actual_task_status("task_2", verify_running=False) == "COMPLETED"

    def test_chunk_incomplete_without_sentinel_is_not_completed(self, tmp_path):
        _mk(tmp_path, "task_3", status="CHUNK_INCOMPLETE")
        a = SweepCompletionAnalyzer(tmp_path)
        assert a._get_actual_task_status("task_3", verify_running=False) != "COMPLETED"

    def test_real_failure_without_sentinel_stays_failed(self, tmp_path):
        _mk(tmp_path, "task_4", status="FAILED")
        a = SweepCompletionAnalyzer(tmp_path)
        assert a._get_actual_task_status("task_4", verify_running=False) == "FAILED"
