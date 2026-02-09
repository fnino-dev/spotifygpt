from __future__ import annotations

from datetime import datetime, timedelta

from spotifygpt.importer import Stream, compute_track_key
from spotifygpt.weekly_radar import generate_weekly_radar


def _make_streams(track_count: int = 45) -> list[Stream]:
    streams: list[Stream] = []
    base_time = datetime(2024, 1, 1, 8, 0, 0)
    for index in range(track_count):
        track_name = f"Track {index:02d}"
        artist_name = f"Artist {index:02d}"
        play_count = track_count - index
        for play in range(play_count):
            end_time = (base_time + timedelta(minutes=index, seconds=play)).isoformat()
            streams.append(
                Stream(
                    track_name=track_name,
                    artist_name=artist_name,
                    end_time=end_time,
                    ms_played=200_000 + (index * 1000),
                    track_key=compute_track_key(track_name, artist_name),
                )
            )
    return streams


def test_weekly_radar_size_and_composition() -> None:
    result = generate_weekly_radar(_make_streams())
    size = len(result.tracks)
    assert 30 <= size <= 40

    counts = {"anchor": 0, "transition": 0, "exploration": 0}
    for track in result.tracks:
        counts[track.category] += 1

    assert 0.6 <= counts["anchor"] / size <= 0.7
    assert 0.2 <= counts["transition"] / size <= 0.3
    assert 0.08 <= counts["exploration"] / size <= 0.12


def test_weekly_radar_block_ordering() -> None:
    result = generate_weekly_radar(_make_streams())
    block_order = {"entry": 0, "core": 1, "exit": 2}
    ranks = [block_order[track.block] for track in result.tracks]
    assert ranks == sorted(ranks)
    assert ranks[0] == 0
    assert ranks[-1] == 2
