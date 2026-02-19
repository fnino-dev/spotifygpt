from __future__ import annotations

from spotifygpt.novelty_budget import NoveltyBudget
from spotifygpt.novelty_sequencer import apply_novelty_budget


def test_anchor_enforcement_when_anchor_every_n_is_set() -> None:
    candidates = ["n1", "n2", "a1", "n3", "n4", "a2", "n5"]
    anchors = {"a1", "a2"}
    budget = NoveltyBudget(exploration=0.6, anchor_ratio=0.4, anchor_every_n=3)

    sequenced = apply_novelty_budget(candidates, anchors, budget)

    assert sequenced == ["n1", "n2", "a1", "n3", "n4", "a2", "n5"]
    for start in range(0, len(sequenced), 3):
        chunk = sequenced[start : start + 3]
        if len(chunk) == 3:
            assert any(track in anchors for track in chunk)


def test_no_duplicates_in_output() -> None:
    candidates = ["a1", "a1", "n1", "n1", "a1", "n2"]
    anchors = {"a1"}
    budget = NoveltyBudget(exploration=0.5, anchor_ratio=0.5, anchor_every_n=2)

    sequenced = apply_novelty_budget(candidates, anchors, budget)

    assert sequenced == ["a1", "n1", "n2"]
    assert len(sequenced) == len(set(sequenced))


def test_deterministic_ordering() -> None:
    candidates = ["n1", "a1", "n2", "a2", "n3", "n4"]
    anchors = {"a1", "a2"}
    budget = NoveltyBudget(exploration=0.5, anchor_ratio=0.5, anchor_every_n=None)

    first = apply_novelty_budget(candidates, anchors, budget)
    second = apply_novelty_budget(candidates, anchors, budget)

    assert first == second


def test_graceful_fallback_with_empty_anchors() -> None:
    candidates = ["n1", "n2", "n3"]
    budget = NoveltyBudget(exploration=0.3, anchor_ratio=0.7, anchor_every_n=1)

    sequenced = apply_novelty_budget(candidates, set(), budget)

    assert sequenced == candidates


def test_empty_candidates_returns_empty_list() -> None:
    budget = NoveltyBudget(exploration=0.5, anchor_ratio=0.5, anchor_every_n=2)
    assert apply_novelty_budget([], {"a1"}, budget) == []
