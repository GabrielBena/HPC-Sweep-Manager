"""Unit tests for the ComputeSource ABC's added default implementations.

Covers ``submit_batch`` and ``wait_for_all``. Per-backend impls have their
own test files.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from hpc_sweep_manager.core.common.compute_source import (
    ComputeSource,
    JobInfo,
    TERMINAL_STATES,
)
from hpc_sweep_manager.core.common.resource_spec import ResourceSpec


class StubComputeSource(ComputeSource):
    """Minimal in-memory ComputeSource for exercising the ABC defaults."""

    def __init__(self, name: str = "stub", max_parallel_jobs: int = 4):
        super().__init__(name, "stub", max_parallel_jobs)
        self._next_id = 0
        # Map job_id -> list of statuses to yield on successive get_job_status calls.
        # If the list runs out we keep returning the last value.
        self.status_script: Dict[str, List[str]] = {}
        self.specs_received: List[Optional[ResourceSpec]] = []

    async def setup(self, sweep_dir: Path, sweep_id: str) -> bool:  # pragma: no cover - trivial
        return True

    async def submit_job(
        self,
        params: Dict[str, Any],
        job_name: str,
        sweep_id: str,
        wandb_group: Optional[str] = None,
        spec: Optional[ResourceSpec] = None,
    ) -> str:
        self._next_id += 1
        job_id = f"job_{self._next_id}"
        self.specs_received.append(spec)
        self.active_jobs[job_id] = JobInfo(
            job_id=job_id,
            job_name=job_name,
            params=params,
            source_name=self.name,
            status="RUNNING",
            submit_time=datetime.now(),
            start_time=datetime.now(),
        )
        return job_id

    async def get_job_status(self, job_id: str) -> str:
        script = self.status_script.get(job_id) or ["COMPLETED"]
        status = script.pop(0) if len(script) > 1 else script[0]
        if job_id in self.active_jobs:
            self.update_job_status(job_id, status)
        return status

    async def cancel_job(self, job_id: str) -> bool:  # pragma: no cover - trivial
        if job_id in self.active_jobs:
            self.update_job_status(job_id, "CANCELLED")
            return True
        return False

    async def collect_results(self, job_ids=None) -> bool:  # pragma: no cover - trivial
        return True

    async def health_check(self) -> Dict[str, Any]:  # pragma: no cover - trivial
        return {"status": "healthy"}

    async def cleanup(self) -> None:  # pragma: no cover - trivial
        return None


@pytest.mark.asyncio
class TestSubmitBatchDefault:
    async def test_individual_mode_calls_submit_job_in_order(self):
        src = StubComputeSource()
        params_list = [{"lr": 0.001}, {"lr": 0.01}, {"lr": 0.1}]
        ids = await src.submit_batch(params_list, sweep_id="sweep_42", mode="individual")
        assert ids == ["job_1", "job_2", "job_3"]
        assert [info.job_name for info in src.active_jobs.values()] == [
            "sweep_42_task_001",
            "sweep_42_task_002",
            "sweep_42_task_003",
        ]

    async def test_individual_mode_uses_prefix(self):
        src = StubComputeSource()
        await src.submit_batch(
            [{"x": 1}], sweep_id="sweep_xyz", mode="individual", job_name_prefix="myrun"
        )
        assert next(iter(src.active_jobs.values())).job_name == "myrun_task_001"

    async def test_individual_mode_propagates_spec(self):
        src = StubComputeSource()
        spec = ResourceSpec(walltime="04:00:00", cpus_per_task=4)
        await src.submit_batch([{"x": 1}, {"x": 2}], sweep_id="s", mode="individual", spec=spec)
        assert src.specs_received == [spec, spec]

    async def test_array_mode_default_raises(self):
        src = StubComputeSource()
        with pytest.raises(NotImplementedError, match="does not support array submission"):
            await src.submit_batch([{"x": 1}], sweep_id="s", mode="array")

    async def test_unknown_mode_raises_value_error(self):
        src = StubComputeSource()
        with pytest.raises(ValueError, match="Unknown submission mode"):
            await src.submit_batch([{"x": 1}], sweep_id="s", mode="weird")  # type: ignore[arg-type]


@pytest.mark.asyncio
class TestWaitForAllDefault:
    async def test_returns_immediately_when_no_active_jobs(self):
        src = StubComputeSource()
        result = await src.wait_for_all(poll_interval=0.01)
        assert result == {}

    async def test_polls_until_all_terminal(self):
        src = StubComputeSource()
        await src.submit_batch([{"x": 1}, {"x": 2}], sweep_id="s")
        # Both jobs script RUNNING then COMPLETED on next poll
        for jid in src.active_jobs:
            src.status_script[jid] = ["RUNNING", "COMPLETED"]
        final = await src.wait_for_all(poll_interval=0.001)
        assert set(final.keys()) == {"job_1", "job_2"}
        assert all(s == "COMPLETED" for s in final.values())
        assert src.active_jobs == {}

    async def test_progress_callback_invoked(self):
        src = StubComputeSource()
        await src.submit_batch([{"x": 1}, {"x": 2}, {"x": 3}], sweep_id="s")
        for jid in src.active_jobs:
            src.status_script[jid] = ["COMPLETED"]
        observed: list[tuple[int, int]] = []
        await src.wait_for_all(
            poll_interval=0.001,
            on_progress=lambda completed, total: observed.append((completed, total)),
        )
        assert observed, "callback should fire at least once"
        # Final callback should report all done.
        assert observed[-1] == (3, 3)

    async def test_mixed_terminal_states(self):
        src = StubComputeSource()
        ids = await src.submit_batch([{"x": 1}, {"x": 2}, {"x": 3}], sweep_id="s")
        src.status_script[ids[0]] = ["COMPLETED"]
        src.status_script[ids[1]] = ["FAILED"]
        src.status_script[ids[2]] = ["CANCELLED"]
        final = await src.wait_for_all(poll_interval=0.001)
        assert final[ids[0]] == "COMPLETED"
        assert final[ids[1]] == "FAILED"
        assert final[ids[2]] == "CANCELLED"


def test_terminal_states_constant():
    assert "COMPLETED" in TERMINAL_STATES
    assert "FAILED" in TERMINAL_STATES
    assert "CANCELLED" in TERMINAL_STATES
    assert "RUNNING" not in TERMINAL_STATES
    assert "PENDING" not in TERMINAL_STATES
