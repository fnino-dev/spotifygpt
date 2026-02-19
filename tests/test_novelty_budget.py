from __future__ import annotations

import pytest

from spotifygpt.diurnal import AFTERNOON, EVENING, LATE_NIGHT, MORNING, NIGHT
from spotifygpt.novelty_budget import NoveltyBudget, compute_novelty_budget
from spotifygpt.session_state import SessionState


@pytest.mark.parametrize(
    ("state", "expected_exploration", "expected_anchor_every_n"),
    [
        (SessionState.CALIENTE, 0.6, None),
        (SessionState.NEUTRO, 0.4, None),
        (SessionState.FRAGIL, 0.2, 2),
        (SessionState.CRITICO, 0.1, 1),
    ],
)
def test_base_rules_for_each_state(
    state: SessionState,
    expected_exploration: float,
    expected_anchor_every_n: int | None,
) -> None:
    budget = compute_novelty_budget(EVENING, state)

    assert isinstance(budget, NoveltyBudget)
    assert budget.exploration == expected_exploration
    assert budget.anchor_ratio == 1 - expected_exploration
    assert budget.anchor_every_n == expected_anchor_every_n


def test_diurnal_boost_for_morning_and_afternoon() -> None:
    morning = compute_novelty_budget(MORNING, SessionState.NEUTRO)
    afternoon = compute_novelty_budget(AFTERNOON, SessionState.NEUTRO)

    assert morning.exploration == 0.45
    assert afternoon.exploration == 0.45


def test_diurnal_reduction_for_night_and_late_night() -> None:
    night = compute_novelty_budget(NIGHT, SessionState.NEUTRO)
    late_night = compute_novelty_budget(LATE_NIGHT, SessionState.NEUTRO)

    assert night.exploration == 0.35
    assert late_night.exploration == 0.35


def test_unknown_time_block_has_no_diurnal_adjustment() -> None:
    budget = compute_novelty_budget("SUNRISE", SessionState.NEUTRO)
    assert budget.exploration == 0.4


def test_clamping_is_applied() -> None:
    critico_late_night = compute_novelty_budget(LATE_NIGHT, SessionState.CRITICO)
    assert critico_late_night.exploration == 0.05



def test_accepts_str_session_state() -> None:
    budget = compute_novelty_budget(EVENING, "FRAGIL")
    assert budget.anchor_every_n == 2


def test_unknown_session_state_raises() -> None:
    with pytest.raises(ValueError):
        compute_novelty_budget(EVENING, "UNKNOWN")
