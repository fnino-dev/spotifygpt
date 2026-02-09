from __future__ import annotations

import pytest

from spotifygpt.daily_mode import build_radar_track, generate_daily_mode


def _build_tracks(kind: str, count: int) -> list:
    return [
        build_radar_track(
            track_name=f"{kind.title()} Track {index}",
            artist_name=f"{kind.title()} Artist {index}",
            duration_ms=240_000,
            kind=kind,
        )
        for index in range(count)
    ]


def test_daily_mode_duration_and_composition():
    anchors = _build_tracks("anchor", 12)
    transitions = _build_tracks("transition", 6)

    entries = generate_daily_mode(anchors + transitions, target_minutes=60)

    total_ms = sum(entry.track.duration_ms for entry in entries)
    anchor_ms = sum(
        entry.track.duration_ms for entry in entries if entry.track.kind == "anchor"
    )
    transition_ms = sum(
        entry.track.duration_ms for entry in entries if entry.track.kind == "transition"
    )

    assert 45 * 60_000 <= total_ms <= 90 * 60_000
    assert anchor_ms + transition_ms == total_ms
    anchor_ratio = anchor_ms / total_ms
    transition_ratio = transition_ms / total_ms
    assert anchor_ratio == pytest.approx(0.7, rel=0.2)
    assert transition_ratio == pytest.approx(0.3, rel=0.2)
