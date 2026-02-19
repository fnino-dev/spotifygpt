"""Deterministic novelty budget controller."""

from __future__ import annotations

from dataclasses import dataclass

from spotifygpt.diurnal import AFTERNOON, LATE_NIGHT, MORNING, NIGHT
from spotifygpt.session_state import SessionState


@dataclass(frozen=True)
class NoveltyBudget:
    """Output budget for exploration vs. anchors in recommendation sequencing."""

    exploration: float
    anchor_ratio: float
    anchor_every_n: int | None = None


def compute_novelty_budget(time_block: str, session_state: SessionState | str) -> NoveltyBudget:
    """Compute deterministic novelty budget from time block and session state."""
    state = SessionState(session_state)

    exploration = {
        SessionState.CALIENTE: 0.6,
        SessionState.NEUTRO: 0.4,
        SessionState.FRAGIL: 0.2,
        SessionState.CRITICO: 0.1,
    }[state]

    if time_block in (MORNING, AFTERNOON):
        exploration += 0.05
    elif time_block in (NIGHT, LATE_NIGHT):
        exploration -= 0.05

    exploration = round(min(0.8, max(0.0, exploration)), 2)
    anchor_every_n = {
        SessionState.FRAGIL: 2,
        SessionState.CRITICO: 1,
    }.get(state)

    return NoveltyBudget(
        exploration=exploration,
        anchor_ratio=round(1 - exploration, 2),
        anchor_every_n=anchor_every_n,
    )
