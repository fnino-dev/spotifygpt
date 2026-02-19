"""Deterministic novelty-aware sequencing helper."""

from __future__ import annotations

from spotifygpt.novelty_budget import NoveltyBudget


def apply_novelty_budget(
    candidates: list[str],
    anchors: set[str],
    budget: NoveltyBudget,
) -> list[str]:
    """Apply novelty budget rules to a ranked candidate list.

    Preserves ranked order whenever possible. If ``anchor_every_n`` is configured,
    anchors can be pulled forward to satisfy spacing when possible.
    """

    seen: set[str] = set()
    unique_candidates: list[str] = []
    for track_id in candidates:
        if track_id not in seen:
            seen.add(track_id)
            unique_candidates.append(track_id)

    if not unique_candidates:
        return []

    if not anchors or budget.anchor_every_n is None:
        return unique_candidates

    output: list[str] = []
    emitted: set[str] = set()
    since_last_anchor = 0
    anchor_every_n = max(1, budget.anchor_every_n)

    for index, track_id in enumerate(unique_candidates):
        if track_id in emitted:
            continue

        if track_id not in anchors and since_last_anchor >= anchor_every_n - 1:
            pulled_anchor = None
            for candidate in unique_candidates[index + 1 :]:
                if candidate in anchors and candidate not in emitted:
                    pulled_anchor = candidate
                    break

            if pulled_anchor is not None:
                output.append(pulled_anchor)
                emitted.add(pulled_anchor)
                since_last_anchor = 0

        output.append(track_id)
        emitted.add(track_id)
        if track_id in anchors:
            since_last_anchor = 0
        else:
            since_last_anchor += 1

    return output
