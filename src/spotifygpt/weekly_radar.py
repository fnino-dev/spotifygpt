"""Generate a weekly radar playlist from streaming history."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Iterable

from spotifygpt.importer import Stream


@dataclass(frozen=True)
class TrackStats:
    track_key: str
    track_name: str
    artist_name: str
    play_count: int
    total_ms: int


@dataclass(frozen=True)
class WeeklyRadarTrack:
    track_key: str
    track_name: str
    artist_name: str
    category: str
    block: str
    position: int


@dataclass(frozen=True)
class WeeklyRadarResult:
    tracks: list[WeeklyRadarTrack]


def _summarize_tracks(streams: Iterable[Stream]) -> list[TrackStats]:
    stats: dict[str, TrackStats] = {}
    for stream in streams:
        if stream.track_key not in stats:
            stats[stream.track_key] = TrackStats(
                track_key=stream.track_key,
                track_name=stream.track_name,
                artist_name=stream.artist_name,
                play_count=1,
                total_ms=stream.ms_played,
            )
        else:
            current = stats[stream.track_key]
            stats[stream.track_key] = TrackStats(
                track_key=current.track_key,
                track_name=current.track_name,
                artist_name=current.artist_name,
                play_count=current.play_count + 1,
                total_ms=current.total_ms + stream.ms_played,
            )
    return sorted(
        stats.values(),
        key=lambda item: (item.total_ms, item.play_count, item.track_name),
        reverse=True,
    )


def _clamp_size(size: int, total_tracks: int) -> int:
    bounded = min(max(size, 30), 40)
    return min(bounded, total_tracks)


def _composition_counts(size: int) -> tuple[int, int, int]:
    anchors = round(size * 0.65)
    exploration = max(3, round(size * 0.1)) if size >= 30 else max(1, round(size * 0.1))
    transitions = size - anchors - exploration

    if transitions < round(size * 0.2):
        deficit = round(size * 0.2) - transitions
        anchors = max(0, anchors - deficit)
        transitions = size - anchors - exploration
    if transitions > round(size * 0.3):
        surplus = transitions - round(size * 0.3)
        anchors = min(size - exploration, anchors + surplus)
        transitions = size - anchors - exploration

    return anchors, transitions, exploration


def _assign_categories(
    stats: list[TrackStats],
    anchors: int,
    transitions: int,
    exploration: int,
) -> tuple[list[TrackStats], list[TrackStats], list[TrackStats]]:
    ordered = stats[: anchors + transitions + exploration]
    anchors_list = ordered[:anchors]
    transitions_list = ordered[anchors : anchors + transitions]
    exploration_list = ordered[anchors + transitions :]
    return anchors_list, transitions_list, exploration_list


def _order_blocks(
    anchors: list[TrackStats],
    transitions: list[TrackStats],
    exploration: list[TrackStats],
) -> list[WeeklyRadarTrack]:
    ordered_stats = anchors + transitions + exploration
    size = len(ordered_stats)
    entry_size = max(6, round(size * 0.2)) if size >= 30 else max(2, round(size * 0.2))
    core_size = max(1, round(size * 0.6))
    entry_size = min(entry_size, size)
    core_size = min(core_size, size - entry_size)
    exit_size = size - entry_size - core_size

    blocks = [
        ("entry", ordered_stats[:entry_size]),
        ("core", ordered_stats[entry_size : entry_size + core_size]),
        ("exit", ordered_stats[entry_size + core_size :]),
    ]

    tracks: list[WeeklyRadarTrack] = []
    position = 1
    for block_name, block_tracks in blocks:
        for stat in block_tracks:
            if stat in anchors:
                category = "anchor"
            elif stat in transitions:
                category = "transition"
            else:
                category = "exploration"
            tracks.append(
                WeeklyRadarTrack(
                    track_key=stat.track_key,
                    track_name=stat.track_name,
                    artist_name=stat.artist_name,
                    category=category,
                    block=block_name,
                    position=position,
                )
            )
            position += 1
    return tracks


def generate_weekly_radar(
    streams: Iterable[Stream],
    size: int = 36,
) -> WeeklyRadarResult:
    stats = _summarize_tracks(streams)
    if not stats:
        return WeeklyRadarResult(tracks=[])
    final_size = _clamp_size(size, len(stats))
    anchors_count, transitions_count, exploration_count = _composition_counts(final_size)
    anchors, transitions, exploration = _assign_categories(
        stats,
        anchors_count,
        transitions_count,
        exploration_count,
    )
    tracks = _order_blocks(anchors, transitions, exploration)
    return WeeklyRadarResult(tracks=tracks)


def store_weekly_radar_json(result: WeeklyRadarResult, path: Path | str) -> None:
    target = Path(path)
    payload = {
        "tracks": [
            {
                "track_key": track.track_key,
                "track_name": track.track_name,
                "artist_name": track.artist_name,
                "category": track.category,
                "block": track.block,
                "position": track.position,
            }
            for track in result.tracks
        ]
    }
    target.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def init_weekly_radar_db(connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS weekly_radar (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            track_key TEXT NOT NULL,
            track_name TEXT NOT NULL,
            artist_name TEXT NOT NULL,
            category TEXT NOT NULL,
            block TEXT NOT NULL,
            position INTEGER NOT NULL
        )
        """
    )
    connection.commit()


def store_weekly_radar(connection, result: WeeklyRadarResult) -> int:
    rows = [
        (
            track.track_key,
            track.track_name,
            track.artist_name,
            track.category,
            track.block,
            track.position,
        )
        for track in result.tracks
    ]
    if not rows:
        return 0
    connection.executemany(
        """
        INSERT INTO weekly_radar (
            track_key,
            track_name,
            artist_name,
            category,
            block,
            position
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    connection.commit()
    return len(rows)
