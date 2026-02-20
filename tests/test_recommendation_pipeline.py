from spotifygpt.context_engine import RecommendationPlan
from spotifygpt.recommendation_pipeline import build_recommendation


def _plan(
    *,
    energy=(0.4, 0.8),
    tempo=(100, 140),
    mult=1.0,
    ratio=0.25,
    every_n=None,
    strategy="balanced",
):
    return RecommendationPlan(
        target_energy_range=energy,
        target_tempo_range=tempo,
        exploration_multiplier=mult,
        anchor_ratio=ratio,
        anchor_every_n=every_n,
        sequencing_strategy=strategy,
        explanation={},
    )


def test_empty_candidates_returns_empty():
    assert build_recommendation([], _plan()) == []


def test_filters_on_inclusive_boundaries():
    candidates = [
        {"id": "low", "energy": 0.4, "tempo": 100},
        {"id": "high", "energy": 0.8, "tempo": 140},
        {"id": "out_energy", "energy": 0.81, "tempo": 120},
        {"id": "out_tempo", "energy": 0.6, "tempo": 99},
    ]

    result = build_recommendation(candidates, _plan(mult=1.0, ratio=0.0))

    assert [track["id"] for track in result] == ["low", "high"]


def test_exploration_multiplier_limits_non_anchor_tracks():
    candidates = [
        {"id": "a", "energy": 0.5, "tempo": 110},
        {"id": "b", "energy": 0.55, "tempo": 112},
        {"id": "c", "energy": 0.6, "tempo": 115},
        {"id": "d", "energy": 0.7, "tempo": 130},
    ]

    result = build_recommendation(candidates, _plan(mult=0.5, ratio=0.0))

    assert len(result) == 2


def test_anchor_every_n_inserts_anchors_deterministically():
    candidates = [
        {"id": "e1", "energy": 0.5, "tempo": 110},
        {"id": "e2", "energy": 0.52, "tempo": 112},
        {"id": "e3", "energy": 0.54, "tempo": 114},
        {"id": "anchor1", "energy": 0.56, "tempo": 116, "is_anchor": True},
        {"id": "anchor2", "energy": 0.58, "tempo": 118, "is_anchor": True},
    ]

    result = build_recommendation(candidates, _plan(mult=1.0, every_n=2, strategy="balanced"))

    assert [track["id"] for track in result] == ["e1", "e2", "anchor1", "e3"]


def test_anchor_ratio_used_when_every_n_not_set():
    candidates = [
        {"id": "e1", "energy": 0.5, "tempo": 110},
        {"id": "e2", "energy": 0.52, "tempo": 112},
        {"id": "e3", "energy": 0.54, "tempo": 114},
        {"id": "e4", "energy": 0.56, "tempo": 116},
        {"id": "anchor1", "energy": 0.58, "tempo": 118, "is_anchor": True},
        {"id": "anchor2", "energy": 0.6, "tempo": 120, "is_anchor": True},
    ]

    result = build_recommendation(candidates, _plan(mult=1.0, ratio=0.5, every_n=None))

    ids = [track["id"] for track in result]
    assert ids.count("anchor1") + ids.count("anchor2") == 2
    assert len(result) == 6


def test_no_matches_returns_empty():
    candidates = [
        {"id": "x", "energy": 0.1, "tempo": 80},
        {"id": "y", "energy": 0.95, "tempo": 180},
    ]

    assert build_recommendation(candidates, _plan()) == []


def test_no_anchors_still_returns_exploration_tracks():
    candidates = [
        {"id": "e1", "energy": 0.5, "tempo": 111},
        {"id": "e2", "energy": 0.55, "tempo": 113},
    ]

    result = build_recommendation(candidates, _plan(mult=1.0, ratio=0.7, every_n=None))

    assert [track["id"] for track in result] == ["e1", "e2"]
