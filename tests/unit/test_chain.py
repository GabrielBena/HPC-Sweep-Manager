"""Unit tests for the pure chain state machine (issue #12).

``decide_next`` is the heart that keeps HSM ignorant of checkpoint formats:
it consumes only ``done_count`` / ``progressed`` and emits DONE / ADVANCE /
FAILED. The check order is load-bearing (DONE beats a no-progress FAILED), so
these tests pin it verbatim.
"""

from __future__ import annotations

from hpc_sweep_manager.core.common.chain import (
    ChainConfig,
    ChainDecision,
    ChainState,
    ChunkOutcome,
    decide_next,
)

CFG = ChainConfig(max_chunks=10, max_consecutive_failures=2)


def _outcome(chunk_index, done, n, progressed, states=("COMPLETED",)):
    return ChunkOutcome(
        chunk_index=chunk_index,
        done_count=done,
        num_tasks=n,
        progressed=progressed,
        terminal_states=states,
    )


class TestDone:
    def test_all_done_wins_even_when_not_progressed(self):
        # A chunk can finish the last task AND report a "failed" Slurm state;
        # the sentinel is the truth.
        step = decide_next(_outcome(0, 3, 3, progressed=False), ChainState(), CFG)
        assert step.decision is ChainDecision.DONE
        assert step.next_state.done is True
        assert step.next_state.failed is False

    def test_zero_task_sweep_is_vacuously_done(self):
        step = decide_next(_outcome(0, 0, 0, progressed=False), ChainState(), CFG)
        assert step.decision is ChainDecision.DONE


class TestAdvance:
    def test_progress_advances_and_resets_counter(self):
        state = ChainState(chunk_index=0, consecutive_no_progress=1)
        step = decide_next(_outcome(0, 1, 3, progressed=True), state, CFG)
        assert step.decision is ChainDecision.ADVANCE
        assert step.next_state.chunk_index == 1
        assert step.next_state.consecutive_no_progress == 0

    def test_single_no_progress_below_cap_still_advances(self):
        # cap=2, so the FIRST no-progress chunk advances (counter=1).
        step = decide_next(_outcome(0, 0, 3, progressed=False), ChainState(), CFG)
        assert step.decision is ChainDecision.ADVANCE
        assert step.next_state.consecutive_no_progress == 1


class TestFailed:
    def test_no_progress_reaches_cap_fails(self):
        state = ChainState(chunk_index=1, consecutive_no_progress=1)
        step = decide_next(_outcome(1, 0, 3, progressed=False), state, CFG)
        assert step.decision is ChainDecision.FAILED
        assert step.next_state.failed is True
        assert "no progress" in step.reason

    def test_max_consecutive_failures_one_fails_immediately(self):
        cfg = ChainConfig(max_chunks=10, max_consecutive_failures=1)
        step = decide_next(_outcome(0, 0, 3, progressed=False), ChainState(), cfg)
        assert step.decision is ChainDecision.FAILED

    def test_out_of_budget_fails_with_helpful_reason(self):
        # Progressing every chunk but the last allowed chunk is still not done.
        cfg = ChainConfig(max_chunks=3, max_consecutive_failures=5)
        state = ChainState(chunk_index=2, consecutive_no_progress=0)
        step = decide_next(_outcome(2, 2, 3, progressed=True), state, cfg)
        assert step.decision is ChainDecision.FAILED
        assert "max_chunks=3" in step.reason
        assert "chunk_walltime" in step.reason

    def test_max_chunks_one_single_shot_not_done_fails(self):
        cfg = ChainConfig(max_chunks=1, max_consecutive_failures=5)
        step = decide_next(_outcome(0, 1, 3, progressed=True), ChainState(), cfg)
        assert step.decision is ChainDecision.FAILED

    def test_done_beats_out_of_budget_on_final_chunk(self):
        cfg = ChainConfig(max_chunks=1, max_consecutive_failures=5)
        step = decide_next(_outcome(0, 3, 3, progressed=True), ChainState(), cfg)
        assert step.decision is ChainDecision.DONE


class TestStateRoundTrip:
    def test_chain_state_dict_round_trip(self):
        s = ChainState(chunk_index=2, consecutive_no_progress=1, done=False, failed=False)
        assert ChainState.from_dict(s.to_dict()) == s

    def test_from_dict_tolerates_missing_keys(self):
        assert ChainState.from_dict(None) == ChainState()
        assert ChainState.from_dict({"chunk_index": 5}).chunk_index == 5
