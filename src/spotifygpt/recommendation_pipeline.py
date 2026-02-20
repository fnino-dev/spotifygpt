"""Recommendation pipeline v1.

Builds a deterministic ordered recommendation list from candidates and a plan.
"""

from __future__ import annotations

from spotifygpt.context_engine import RecommendationPlan


def _within_range(value: float | int, value_range: tuple[float | int, float | int]) -> bool:
    return value_range[0] <= value <= value_range[1]


def _target_distance(track: dict, plan: RecommendationPlan) -> tuple[float, float, str]:
    energy_center = (plan.target_energy_range[0] + plan.target_energy_range[1]) / 2
    tempo_center = (plan.target_tempo_range[0] + plan.target_tempo_range[1]) / 2
    energy = float(track.get("energy", 0.0))
    tempo = float(track.get("tempo", 0.0))
    return (abs(energy - energy_center), abs(tempo - tempo_center), str(track.get("id", "")))


def _select_exploration_tracks(candidates: list[dict], plan: RecommendationPlan) -> list[dict]:
    if not candidates:
        return []
    keep = int(round(len(candidates) * plan.exploration_multiplier))
    keep = max(0, min(len(candidates), keep))
    return candidates[:keep]


def _order_tracks(tracks: list[dict], plan: RecommendationPlan) -> list[dict]:
    if plan.sequencing_strategy == "flow_explore":
        return sorted(tracks, key=lambda t: (float(t.get("tempo", 0.0)), float(t.get("energy", 0.0)), str(t.get("id", ""))))
    if plan.sequencing_strategy in {"stabilize_with_anchors", "recovery_mode"}:
        return sorted(tracks, key=lambda t: (float(t.get("energy", 0.0)), float(t.get("tempo", 0.0)), str(t.get("id", ""))))
    if plan.sequencing_strategy == "balanced":
        return list(tracks)
    return sorted(tracks, key=lambda t: _target_distance(t, plan))


def _inject_by_every_n(exploration: list[dict], anchors: list[dict], every_n: int) -> list[dict]:
    if every_n <= 0:
        return list(exploration)
    output: list[dict] = []
    anchor_index = 0
    count_since_anchor = 0
    for track in exploration:
        output.append(track)
        count_since_anchor += 1
        if count_since_anchor >= every_n and anchor_index < len(anchors):
            output.append(anchors[anchor_index])
            anchor_index += 1
            count_since_anchor = 0
    if not exploration and anchors:
        output.append(anchors[0])
    return output


def _inject_by_ratio(exploration: list[dict], anchors: list[dict], anchor_ratio: float) -> list[dict]:
    if not exploration:
        return anchors[:1] if anchors else []
    target_anchor_count = int(round(len(exploration) * anchor_ratio))
    target_anchor_count = max(0, min(len(anchors), target_anchor_count))
    if target_anchor_count == 0:
        return list(exploration)

    output: list[dict] = []
    used_anchors = anchors[:target_anchor_count]
    interval = max(1, len(exploration) // target_anchor_count)
    anchor_index = 0
    for idx, track in enumerate(exploration, start=1):
        output.append(track)
        if idx % interval == 0 and anchor_index < target_anchor_count:
            output.append(used_anchors[anchor_index])
            anchor_index += 1
    while anchor_index < target_anchor_count:
        output.append(used_anchors[anchor_index])
        anchor_index += 1
    return output


def build_recommendation(candidates: list[dict], plan: RecommendationPlan) -> list[dict]:
    """Apply a recommendation plan to candidates and return an ordered track list."""

    filtered = [
        track
        for track in candidates
        if _within_range(float(track.get("energy", 0.0)), plan.target_energy_range)
        and _within_range(float(track.get("tempo", 0.0)), plan.target_tempo_range)
    ]

    anchors = _order_tracks([track for track in filtered if bool(track.get("is_anchor", False))], plan)
    exploration_pool = [track for track in filtered if not bool(track.get("is_anchor", False))]
    exploration = _order_tracks(_select_exploration_tracks(exploration_pool, plan), plan)

    if plan.anchor_every_n is not None:
        return _inject_by_every_n(exploration, anchors, plan.anchor_every_n)
    return _inject_by_ratio(exploration, anchors, plan.anchor_ratio)
