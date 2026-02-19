from __future__ import annotations

from spotifygpt.behavior_orchestrator import OrchestrationResult, orchestrate_candidates
from spotifygpt.diurnal import EVENING
from spotifygpt.novelty_budget import compute_novelty_budget
from spotifygpt.novelty_sequencer import apply_novelty_budget
from spotifygpt.session_state import SessionState


def test_determinism_same_inputs_same_outputs() -> None:
    kwargs = {
        "time_block": EVENING,
        "session_state": SessionState.FRAGIL,
        "candidates": ["n1", "n2", "a1", "n3", "a2"],
        "anchors": {"a1", "a2"},
    }

    first = orchestrate_candidates(**kwargs)
    second = orchestrate_candidates(**kwargs)

    assert isinstance(first, OrchestrationResult)
    assert first == second


def test_budget_matches_compute_novelty_budget() -> None:
    result = orchestrate_candidates(
        time_block=EVENING,
        session_state=SessionState.NEUTRO,
        candidates=["n1", "a1", "n2"],
        anchors={"a1"},
    )

    expected_budget = compute_novelty_budget(EVENING, SessionState.NEUTRO)
    assert result.budget == expected_budget


def test_sequenced_matches_apply_novelty_budget() -> None:
    candidates = ["n1", "n2", "a1", "n3", "a2", "n4"]
    anchors = {"a1", "a2"}
    budget = compute_novelty_budget(EVENING, SessionState.FRAGIL)

    result = orchestrate_candidates(
        time_block=EVENING,
        session_state=SessionState.FRAGIL,
        candidates=candidates,
        anchors=anchors,
    )

    assert result.sequenced == apply_novelty_budget(candidates, anchors, budget)


def test_accepts_session_state_as_enum_and_str() -> None:
    enum_result = orchestrate_candidates(
        time_block=EVENING,
        session_state=SessionState.CRITICO,
        candidates=["n1", "a1"],
        anchors={"a1"},
    )
    str_result = orchestrate_candidates(
        time_block=EVENING,
        session_state="CRITICO",
        candidates=["n1", "a1"],
        anchors={"a1"},
    )

    assert enum_result == str_result


def test_edge_case_empty_candidates() -> None:
    result = orchestrate_candidates(
        time_block=EVENING,
        session_state=SessionState.FRAGIL,
        candidates=[],
        anchors={"a1"},
    )

    assert result.sequenced == []


def test_edge_case_empty_anchors() -> None:
    candidates = ["n1", "n2", "n3"]

    result = orchestrate_candidates(
        time_block=EVENING,
        session_state=SessionState.FRAGIL,
        candidates=candidates,
        anchors=set(),
    )

    assert result.sequenced == candidates
