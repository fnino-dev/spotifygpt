"""Pure session intervention hook for ranking/sequencing consumers."""

from __future__ import annotations

from dataclasses import dataclass

from spotifygpt.session_state import SessionInterventions


ANCHOR_BOOST = 1.0


@dataclass(frozen=True)
class BehaviorHookResult:
    """Result of applying session interventions to candidate scores.

    Downstream ranking can consume ``scores`` as-is. ``needs_anchor`` indicates
    anchor injection was requested but no anchor IDs were supplied. ``should_suggest_reset``
    is passed through as a signal for UX/state handling.
    """

    scores: dict[str, float]
    needs_anchor: bool
    should_suggest_reset: bool


def apply_session_interventions(
    *,
    base_scores: dict[str, float],
    interventions: SessionInterventions,
    anchor_track_ids: list[str] | None = None,
) -> BehaviorHookResult:
    """Apply deterministic score adjustments from session interventions.

    The function is intentionally pure and copy-safe: it never mutates
    ``base_scores`` or ``anchor_track_ids``.
    """

    adjusted_scores = {
        track_id: score * interventions.exploration_multiplier
        for track_id, score in base_scores.items()
    }

    needs_anchor = interventions.should_inject_anchor and not anchor_track_ids
    if interventions.should_inject_anchor and anchor_track_ids:
        anchor_ids = set(anchor_track_ids)
        for track_id in anchor_ids:
            if track_id in adjusted_scores:
                adjusted_scores[track_id] += ANCHOR_BOOST

    return BehaviorHookResult(
        scores=adjusted_scores,
        needs_anchor=needs_anchor,
        should_suggest_reset=interventions.should_suggest_reset,
    )
