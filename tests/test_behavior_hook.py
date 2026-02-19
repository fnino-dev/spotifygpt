from __future__ import annotations

from spotifygpt.behavior_hook import ANCHOR_BOOST, apply_session_interventions
from spotifygpt.session_state import SessionInterventions


def test_neutral_multiplier_applied() -> None:
    base_scores = {"a": 1.0, "b": 0.5}
    interventions = SessionInterventions(
        exploration_multiplier=0.8,
        should_inject_anchor=False,
        should_suggest_reset=False,
    )

    result = apply_session_interventions(base_scores=base_scores, interventions=interventions)

    assert result.scores == {"a": 0.8, "b": 0.4}
    assert result.needs_anchor is False
    assert result.should_suggest_reset is False


def test_caliente_multiplier_one_keeps_scores() -> None:
    base_scores = {"a": 1.0, "b": 0.5}
    interventions = SessionInterventions(
        exploration_multiplier=1.0,
        should_inject_anchor=False,
        should_suggest_reset=False,
    )

    result = apply_session_interventions(base_scores=base_scores, interventions=interventions)

    assert result.scores == base_scores


def test_fragil_anchor_boost_applies_to_present_ids_only() -> None:
    base_scores = {"a": 0.5, "b": 0.4, "c": 0.3}
    interventions = SessionInterventions(
        exploration_multiplier=0.4,
        should_inject_anchor=True,
        should_suggest_reset=False,
    )

    result = apply_session_interventions(
        base_scores=base_scores,
        interventions=interventions,
        anchor_track_ids=["a", "missing", "a"],
    )

    assert result.scores == {
        "a": (0.5 * 0.4) + ANCHOR_BOOST,
        "b": 0.4 * 0.4,
        "c": 0.3 * 0.4,
    }
    assert result.needs_anchor is False
    assert result.should_suggest_reset is False


def test_anchor_requested_without_ids_returns_needs_anchor() -> None:
    base_scores = {"a": 1.0}
    interventions = SessionInterventions(
        exploration_multiplier=0.2,
        should_inject_anchor=True,
        should_suggest_reset=True,
    )

    result = apply_session_interventions(
        base_scores=base_scores,
        interventions=interventions,
        anchor_track_ids=None,
    )

    assert result.scores == {"a": 0.2}
    assert result.needs_anchor is True
    assert result.should_suggest_reset is True


def test_copy_safe_inputs_unchanged() -> None:
    base_scores = {"a": 1.0, "b": 2.0}
    anchor_track_ids = ["a"]
    interventions = SessionInterventions(
        exploration_multiplier=0.4,
        should_inject_anchor=True,
        should_suggest_reset=False,
    )

    result = apply_session_interventions(
        base_scores=base_scores,
        interventions=interventions,
        anchor_track_ids=anchor_track_ids,
    )

    assert base_scores == {"a": 1.0, "b": 2.0}
    assert anchor_track_ids == ["a"]
    assert result.scores is not base_scores
