"""tasks_state.json: how Slurm ended each task, recorded at collect (tracker S8).

A walltime kill leaves no ``Status:`` line in task_info.txt, so the analyzer read the task as
RUNNING forever and the CLI's failing-task list skipped it; TIMEOUT, OUT_OF_MEMORY, NODE_FAIL and
PREEMPTED all read as FAILED. One sacct at collect now records state, exit code, node and elapsed
per task; a failed or absent sacct writes nothing.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from rich.console import Console

from hpc_sweep_manager.cli.sweep import _report_slurm_ends
from hpc_sweep_manager.core.common.compute_source import JobInfo
from hpc_sweep_manager.core.common.resource_spec import ResourceSpec
from hpc_sweep_manager.core.common.sweep_analysis import SweepCompletionAnalyzer
from hpc_sweep_manager.core.hpc.slurm_base import SlurmBase
from hpc_sweep_manager.core.hpc.slurm_compute_source import SlurmComputeSource

# The mixed rows of one 4-task array: a success, a walltime kill, an OOM kill, a 12 s failure.
ROWS = (
    "{j}_1|COMPLETED|0:0|u24-gpu-01|3600\n"
    "{j}_2|TIMEOUT|0:15|u24-gpu-03|86400\n"
    "{j}_3|OUT_OF_MEMORY|0:125|u24-gpu-02|5000\n"
    "{j}_4|FAILED|1:0|u24-cpu-07|12\n"
)


def _state(state, code, node, secs):
    return {"state": state, "exit_code": code, "node": node, "elapsed_s": secs}


EXPECTED = {
    "task_1": _state("COMPLETED", "0:0", "u24-gpu-01", 3600),
    "task_2": _state("TIMEOUT", "0:15", "u24-gpu-03", 86400),
    "task_3": _state("OUT_OF_MEMORY", "0:125", "u24-gpu-02", 5000),
    "task_4": _state("FAILED", "1:0", "u24-cpu-07", 12),
}


async def _native_array(tmp_path, n=4, spec=None) -> tuple[SlurmComputeSource, Path, list[str]]:
    src = SlurmComputeSource(
        name="fake", script_path="t.py", project_dir=str(tmp_path), default_spec=spec
    )
    sweep_dir = tmp_path / "sw"
    assert await src.setup(sweep_dir, "sw")
    ids = await src.submit_batch([{"lr": i} for i in range(n)], "sw", mode="array")
    return src, sweep_dir, ids


@pytest.mark.asyncio
class TestRecordNative:
    """Through the fake-sacct PATH stub: the real subprocess transport."""

    async def test_collect_writes_one_entry_per_task(self, tmp_path, fake_slurm):
        src, sweep_dir, (job,) = await _native_array(tmp_path)
        (fake_slurm.state_dir / "sacct_rows").write_text(
            ROWS.format(j=job) + "999_1|FAILED|1:0|elsewhere|1\n"  # another job's row
        )
        assert await src.collect_results() is True
        assert json.loads((sweep_dir / "tasks_state.json").read_text()) == EXPECTED

    async def test_gpu_type_sub_arrays_map_rows_to_their_global_task_dirs(
        self, tmp_path, fake_slurm
    ):
        spec = ResourceSpec(gpus=1, gpu_type=("A100", "H200"))
        src, sweep_dir, ids = await _native_array(tmp_path, spec=spec)
        assert len(ids) == 2
        rows, want = "", {}
        for job, token in zip(ids, ("A100", "H200"), strict=True):
            entries = json.loads((sweep_dir / f"parameter_combinations_{token}.json").read_text())
            for e in entries:
                rows += f"{job}_{e['index']}|TIMEOUT|0:15|n-{token}|99\n"
                want[f"task_{e['global_index']}"] = _state("TIMEOUT", "0:15", f"n-{token}", 99)
        (fake_slurm.state_dir / "sacct_rows").write_text(rows)
        await src.collect_results()
        assert json.loads((sweep_dir / "tasks_state.json").read_text()) == want
        assert sorted(want) == [f"task_{i}" for i in range(1, 5)]

    async def test_sacct_absent_writes_nothing(self, tmp_path, fake_slurm, monkeypatch):
        src, sweep_dir, (job,) = await _native_array(tmp_path)
        (fake_slurm.state_dir / "sacct_rows").write_text(ROWS.format(j=job))
        (fake_slurm.bin_dir / "sacct").unlink()
        monkeypatch.setenv("PATH", str(fake_slurm.bin_dir))  # no sacct anywhere
        assert await src.collect_results() is True
        assert not (sweep_dir / "tasks_state.json").exists()

    async def test_sacct_failing_writes_nothing(self, tmp_path, fake_slurm, monkeypatch):
        src, sweep_dir, (job,) = await _native_array(tmp_path)
        (fake_slurm.state_dir / "sacct_rows").write_text(ROWS.format(j=job))
        monkeypatch.setenv("HSM_FAKE_SACCT_RC", "1")
        assert await src.collect_results() is True
        assert not (sweep_dir / "tasks_state.json").exists()


class Scripted(SlurmBase):
    """A SlurmBase whose ``_sh`` answers sacct with one scripted reply."""

    def __init__(self, sweep_dir, reply, jobs):
        super().__init__("fake", "slurm", 10_000)
        self.sweep_dir, self.reply, self.argv = sweep_dir, reply, None
        self.completed_jobs.update(jobs)

    async def _sh(self, argv):
        self.argv = argv
        return self.reply

    async def setup(self, sweep_dir, sweep_id):
        return True

    async def submit_job(self, *a, **k):
        raise NotImplementedError

    async def cancel_job(self, job_id):
        return True

    async def collect_results(self, job_ids=None, *, defer_cleanup=False):
        return True

    async def health_check(self):
        return {}

    async def cleanup(self):
        pass


def _array(job, indices):
    params = {"_array_size": len(indices), "_global_indices": indices}
    return job, JobInfo(job, job, params, "fake")


@pytest.mark.asyncio
class TestRecordMapping:
    async def test_one_sacct_for_all_jobs_and_every_mapping(self, tmp_path):
        jobs = dict(
            [
                _array("10", [1, 3]),
                _array("20", [2, 4]),
                ("30", JobInfo("30", "sw_task_005", {"lr": 1}, "fake", task_dir="/x/sw_task_005")),
            ]
        )
        out = (
            "10_1|TIMEOUT|0:15|a|9\n10_2|COMPLETED|0:0|a|9\n20_1|PREEMPTED|0:0|b|9\n"
            "20_2|CANCELLED by 1|0:0|b|9\n30|NODE_FAIL|0:0|c|9\n"
            "10_[3-4]|CANCELLED by 1|0:0|None assigned|0\n20_3|RUNNING|0:0|b|9\n"
        )
        src = Scripted(tmp_path, (0, out, ""), jobs)
        await src.record_task_states()
        assert src.argv[:3] == ["sacct", "-j", "10,20,30"]
        got = json.loads((tmp_path / "tasks_state.json").read_text())
        assert {t: s["state"] for t, s in got.items()} == {
            "task_1": "TIMEOUT",
            "task_3": "COMPLETED",
            "task_2": "PREEMPTED",
            "task_4": "CANCELLED",
            "sw_task_005": "NODE_FAIL",
        }

    async def test_a_reattached_array_maps_1_to_n_only_when_alone(self, tmp_path):
        one = {"7": JobInfo("7", "7", {}, "fake")}  # what adopt() builds: no params
        await Scripted(tmp_path, (0, "7_3|TIMEOUT|0:15|n|9\n", ""), one).record_task_states()
        assert list(json.loads((tmp_path / "tasks_state.json").read_text())) == ["task_3"]
        two = {j: JobInfo(j, j, {}, "fake") for j in ("7", "8")}
        other = tmp_path / "two"
        other.mkdir()
        await Scripted(other, (0, "7_1|TIMEOUT|0:15|n|9\n", ""), two).record_task_states()
        assert not (other / "tasks_state.json").exists()  # which sub-array? never a guess

    async def test_two_children_of_a_distributed_sweep_merge_into_one_file(self, tmp_path):
        a = Scripted(tmp_path, (0, "10_1|TIMEOUT|0:15|a|9\n", ""), dict([_array("10", [1])]))
        b = Scripted(tmp_path, (0, "20_1|COMPLETED|0:0|b|9\n", ""), dict([_array("20", [2])]))
        await a.record_task_states()
        await b.record_task_states()
        got = json.loads((tmp_path / "tasks_state.json").read_text())
        assert {t: s["state"] for t, s in got.items()} == {"task_1": "TIMEOUT", "task_2": "COMPLETED"}

    @pytest.mark.parametrize(
        "reply",
        [
            (1, "", "slurmdbd down"),
            (1, "10_1|TIMEOUT|0:15|a|9\n", "error mid-listing"),  # rows from a failed call
            (255, "10_1|TIMEOUT|0:15|a|9\n", "no exit status"),  # the link died as it ran
            (0, "", ""),  # accounting has no rows
        ],
    )
    async def test_no_answer_writes_nothing(self, tmp_path, reply):
        await Scripted(tmp_path, reply, dict([_array("10", [1, 2])])).record_task_states()
        assert not (tmp_path / "tasks_state.json").exists()


# ---------------------------------------------------------------------- the analyzer


def _sweep(tmp_path: Path, states: dict | None = None) -> Path:
    """Four tasks: 1 succeeded, 2 was walltime-killed (no Status: line), 3 hit OOM (the wrapper
    saw the kill: Status: FAILED), 4 died before it wrote task_info.txt."""
    (tmp_path / "sweep_config.yaml").write_text("sweep:\n  grid:\n    lr: [1, 2, 3, 4]\n")
    info = {1: "Status: SUCCESS\n", 2: "Started: now\n", 3: "Status: FAILED\n", 4: None}
    for i, text in info.items():
        d = tmp_path / "tasks" / f"task_{i}"
        d.mkdir(parents=True)
        if text is not None:
            (d / "task_info.txt").write_text(f"Global Task Index: {i}\n{text}")
    if states is not None:
        (tmp_path / "tasks_state.json").write_text(json.dumps(states))
    return tmp_path


class TestAnalyzer:
    def test_a_walltime_kill_reads_timeout_not_running(self, tmp_path):
        a = SweepCompletionAnalyzer(_sweep(tmp_path, EXPECTED))
        assert a._get_actual_task_status("task_2", verify_running=False) == "TIMEOUT"
        r = a.analyze_from_task_directories()
        assert r["failed_tasks"] == ["task_2", "task_3", "task_4"]
        assert r["running_tasks"] == []
        assert r["task_statuses"]["task_2"]["status"] == "TIMEOUT"
        assert r["task_statuses"]["task_3"]["status"] == "FAILED"  # the Status: line stands

    def test_the_mapping_path_counts_slurm_failures_as_failed(self, tmp_path):
        sweep = _sweep(tmp_path, EXPECTED)
        SweepCompletionAnalyzer(sweep).analyze_from_task_directories()  # writes the mapping
        r = SweepCompletionAnalyzer(sweep).analyze_completion_status()
        assert "source_mapping_exists" in r  # the mapping path, not the scan
        assert sorted(r["failed_tasks"]) == ["task_2", "task_3", "task_4"]

    def test_without_the_file_nothing_changes(self, tmp_path):
        a = SweepCompletionAnalyzer(_sweep(tmp_path))
        assert a._get_actual_task_status("task_2", verify_running=False) is None
        r = a.analyze_from_task_directories()
        assert r["running_tasks"] == ["task_2", "task_4"]
        assert r["failed_tasks"] == ["task_3"]

    def test_the_done_sentinel_stays_authoritative(self, tmp_path):
        sweep = _sweep(tmp_path, EXPECTED)
        (sweep / "tasks" / "task_2" / ".hsm_done").write_text("done\n")
        a = SweepCompletionAnalyzer(sweep)
        assert a._get_actual_task_status("task_2", verify_running=False) == "COMPLETED"
        assert "task_2" in a.analyze_from_task_directories()["completed_tasks"]


# ---------------------------------------------------------------------- the CLI summary


class TestSummary:
    def test_timeout_and_oom_apart_and_a_quick_failure_flagged(self, tmp_path):
        (tmp_path / "tasks_state.json").write_text(json.dumps(EXPECTED))
        console = Console(record=True, width=300)
        assert _report_slurm_ends(tmp_path, console) == ["task_2", "task_3", "task_4"]
        text = console.export_text()
        assert "Slurm ended: 1 TIMEOUT, 1 OUT_OF_MEMORY, 1 FAILED" in text
        assert "Infra suspect (failed in < 60 s): task_4" in text
        assert "--exclude=u24-cpu-07" in text

    def test_a_cancel_is_not_infra_suspect(self, tmp_path):
        states = {"task_1": _state("CANCELLED", "0:0", "None assigned", 0)}
        (tmp_path / "tasks_state.json").write_text(json.dumps(states))
        console = Console(record=True, width=300)
        assert _report_slurm_ends(tmp_path, console) == ["task_1"]
        assert "Infra suspect" not in console.export_text()

    def test_no_file_prints_nothing(self, tmp_path):
        console = Console(record=True)
        assert _report_slurm_ends(tmp_path, console) == []
        assert console.export_text() == ""
