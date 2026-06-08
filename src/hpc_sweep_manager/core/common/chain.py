"""Pure chain state machine for resumable chained runs (issue #12).

A *chain* completes a job that exceeds the walltime cap as a sequence of
checkpoint-chained *chunks*: chunk ``k+1`` resumes chunk ``k`` from a
checkpoint the user's script wrote. This module is the heart that keeps HSM a
GENERAL orchestrator — it consumes only what HSM can observe WITHOUT knowing
anything about checkpoint formats, experiment trackers, or "epochs":

* how many tasks wrote their ``.hsm_done`` sentinel (``done_count``),
* whether the chunk made *any* progress (a sentinel appeared OR a checkpoint
  file's mtime advanced — the I/O layer computes this bool by stat-ing a path
  HSM itself provided),
* the chunk's terminal Slurm states (only for human-readable messaging).

Done-detection is the explicit sentinel, NEVER the exit code: a timed-out
chunk exits non-zero yet is the *normal* mid-budget outcome, so exit code is
ambiguous (see issue #12). The machine resumes on ANY terminal state but caps
two ways so a deterministically-crashing job can't resubmit forever:
``max_chunks`` (chain length) and ``max_consecutive_failures`` (no-progress
strikes).

Pure and no-I/O by design — mirrors the ``gpu_planner`` / ``slurm_protocol``
idiom so the transition logic is unit-testable without a cluster, and the
three dataclasses round-trip straight into ``.hsm_manifest.json`` so a
re-invoked ``hsm sweep advance`` can reconstruct the chain.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from enum import Enum
from typing import Any, Dict


class ChainDecision(Enum):
    """What to do after a chunk reaches a terminal Slurm state."""

    DONE = "done"  # every task wrote its .hsm_done sentinel
    ADVANCE = "advance"  # submit chunk k+1
    FAILED = "failed"  # ran out of chunks, or deterministic no-progress


@dataclass(frozen=True)
class ChunkOutcome:
    """What HSM observed about one finished chunk — the ONLY inputs the state
    machine consumes. No checkpoint format, no epochs, no exit codes."""

    chunk_index: int  # 0-based index of the chunk that just finished
    done_count: int  # tasks with a .hsm_done sentinel (0..num_tasks)
    num_tasks: int
    progressed: bool  # done_count rose OR a checkpoint mtime advanced this chunk
    terminal_states: tuple[str, ...] = ()  # chunk's job terminal states (messaging only)


@dataclass(frozen=True)
class ChainConfig:
    """The two runaway guards (subset of ResumableConfig the machine needs)."""

    max_chunks: int = 10
    max_consecutive_failures: int = 2


@dataclass(frozen=True)
class ChainState:
    """Carried across chunks; serialized into the manifest so a re-invoked
    ``hsm sweep advance`` can reconstruct the chain mid-flight."""

    chunk_index: int = 0
    consecutive_no_progress: int = 0
    done: bool = False
    failed: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any] | None) -> "ChainState":
        data = data or {}
        return cls(
            chunk_index=int(data.get("chunk_index", 0)),
            consecutive_no_progress=int(data.get("consecutive_no_progress", 0)),
            done=bool(data.get("done", False)),
            failed=bool(data.get("failed", False)),
        )


@dataclass(frozen=True)
class ChainStep:
    """Result of one transition: what to do, the next state, and why."""

    decision: ChainDecision
    next_state: ChainState
    reason: str


def decide_next(
    outcome: ChunkOutcome,
    state: ChainState,
    config: ChainConfig,
) -> ChainStep:
    """Pure transition. Check order is load-bearing:

    1. DONE wins unconditionally when every task signalled ``.hsm_done``
       (a chunk can both finish the last task AND be a "failed" Slurm state —
       the sentinel is the truth).
    2. else recompute ``consecutive_no_progress`` (reset to 0 on progress,
       else +1).
    3. FAILED if ``consecutive_no_progress >= max_consecutive_failures`` — a
       deterministic crash (e.g. a bad config) that never writes a sentinel
       or advances a checkpoint.
    4. FAILED if this was the last allowed chunk and we're still not done
       (out of budget — raise ``chunk_walltime`` or ``max_chunks``).
    5. else ADVANCE: submit chunk k+1.
    """
    n = outcome.num_tasks

    # 1. DONE — vacuously true for a degenerate zero-task sweep.
    if outcome.done_count >= n:
        return ChainStep(
            decision=ChainDecision.DONE,
            next_state=replace(state, done=True, failed=False),
            reason=(
                f"all {n} task(s) signalled {'.hsm_done' if n else 'done'} "
                f"after {outcome.chunk_index + 1} chunk(s)"
            ),
        )

    # 2. progress accounting
    new_no_progress = 0 if outcome.progressed else state.consecutive_no_progress + 1

    # 3. deterministic no-progress crash
    if new_no_progress >= config.max_consecutive_failures:
        return ChainStep(
            decision=ChainDecision.FAILED,
            next_state=replace(
                state, consecutive_no_progress=new_no_progress, failed=True
            ),
            reason=(
                f"{new_no_progress} consecutive chunk(s) made no progress "
                f"(no new .hsm_done sentinel and no checkpoint written) — "
                f"likely a deterministic crash; "
                f"only {outcome.done_count}/{n} task(s) done. Inspect the chunk "
                f"logs (terminal states: {', '.join(outcome.terminal_states) or 'n/a'})."
            ),
        )

    # 4. ran out of budget
    if outcome.chunk_index + 1 >= config.max_chunks:
        return ChainStep(
            decision=ChainDecision.FAILED,
            next_state=replace(
                state, consecutive_no_progress=new_no_progress, failed=True
            ),
            reason=(
                f"reached max_chunks={config.max_chunks} with "
                f"{outcome.done_count}/{n} task(s) done — raise chunk_walltime "
                f"or max_chunks to give the run more budget"
            ),
        )

    # 5. advance
    return ChainStep(
        decision=ChainDecision.ADVANCE,
        next_state=replace(
            state,
            chunk_index=outcome.chunk_index + 1,
            consecutive_no_progress=new_no_progress,
        ),
        reason=(
            f"chunk {outcome.chunk_index + 1} done, {outcome.done_count}/{n} "
            f"task(s) complete — submitting chunk {outcome.chunk_index + 2}"
        ),
    )
