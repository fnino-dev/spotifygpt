"""Pure novelty orchestration helper for behavior sequencing."""

from __future__ import annotations

from dataclasses import dataclass

from spotifygpt.novelty_budget import NoveltyBudget, compute_novelty_budget
from spotifygpt.novelty_sequencer import apply_novelty_budget
from spotifygpt.session_state import SessionState


@dataclass(frozen=True)
class OrchestrationResult:
    """Computed novelty budget and sequenced candidates."""

    budget: NoveltyBudget
    sequenced: list[str]


def orchestrate_candidates(
    *,
    time_block: str,
    session_state: SessionState | str,
    candidates: list[str],
    anchors: set[str],
) -> OrchestrationResult:
    """Compute budget and apply deterministic sequencing in one pure step."""

    budget = compute_novelty_budget(time_block, session_state)
    sequenced = apply_novelty_budget(candidates, anchors, budget)
    return OrchestrationResult(budget=budget, sequenced=sequenced)
