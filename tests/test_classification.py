from __future__ import annotations

from spotifygpt.classification import TrackMetrics, classify_tracks


def test_role_assignment() -> None:
    metrics = [
        TrackMetrics(track_key="high", play_count=100, total_ms=300000, average_ms=3000),
        TrackMetrics(track_key="mid", play_count=50, total_ms=150000, average_ms=3000),
        TrackMetrics(track_key="low", play_count=10, total_ms=30000, average_ms=3000),
    ]

    classifications = {item.track_key: item.role for item in classify_tracks(metrics)}

    assert classifications["high"] == "anchor"
    assert classifications["mid"] == "transition"
    assert classifications["low"] == "exploration"


def test_energy_bucket_assignment() -> None:
    metrics = [
        TrackMetrics(track_key="low", play_count=10, total_ms=100000, average_ms=1000),
        TrackMetrics(track_key="mid", play_count=10, total_ms=250000, average_ms=2500),
        TrackMetrics(track_key="high", play_count=10, total_ms=400000, average_ms=4000),
    ]

    classifications = {item.track_key: item.energy_bucket for item in classify_tracks(metrics)}

    assert classifications["low"] == "low"
    assert classifications["mid"] == "medium"
    assert classifications["high"] == "high"


def test_usage_classification() -> None:
    metrics = [
        TrackMetrics(track_key="peak", play_count=100, total_ms=400000, average_ms=4000),
        TrackMetrics(track_key="background", play_count=100, total_ms=100000, average_ms=1000),
        TrackMetrics(track_key="discharge", play_count=10, total_ms=100000, average_ms=1000),
        TrackMetrics(track_key="focus", play_count=40, total_ms=250000, average_ms=2500),
    ]

    classifications = {item.track_key: item.usage_type for item in classify_tracks(metrics)}

    assert classifications["peak"] == "peak"
    assert classifications["background"] == "background"
    assert classifications["discharge"] == "discharge"
    assert classifications["focus"] == "focus"
