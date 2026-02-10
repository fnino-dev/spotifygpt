"""Generate daily listening modes from weekly radar tracks."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Iterable

from spotifygpt.importer import compute_track_key


@dataclass(frozen=True)
class RadarTrack:
    track_name: str
    artist_name: str
    duration_ms: int
    kind: str
    track_key: str


@dataclass(frozen=True)
class ModeEntry:
    track: RadarTrack
    position: int


def build_radar_track(
    track_name: str,
    artist_name: str,
    duration_ms: int,
    kind: str,
) -> RadarTrack:
    if kind not in {"anchor", "transition"}:
        raise ValueError("kind must be 'anchor' or 'transition'")
    return RadarTrack(
        track_name=track_name,
        artist_name=artist_name,
        duration_ms=duration_ms,
        kind=kind,
        track_key=compute_track_key(track_name, artist_name),
    )


def _select_tracks(
    tracks: Iterable[RadarTrack],
    target_ms: int,
    max_ms: int,
    selected_keys: set[str],
    avoid_keys: set[str],
) -> list[RadarTrack]:
    selections: list[RadarTrack] = []
    total_ms = 0
    for track in tracks:
        if track.track_key in selected_keys:
            continue
        if track.track_key in avoid_keys:
            continue
        if total_ms >= target_ms:
            break
        if total_ms + track.duration_ms > max_ms:
            continue
        selections.append(track)
        selected_keys.add(track.track_key)
        total_ms += track.duration_ms
    return selections


def generate_daily_mode(
    weekly_radar: Iterable[RadarTrack],
    played_today_keys: Iterable[str] | None = None,
    target_minutes: int = 60,
    min_minutes: int = 45,
    max_minutes: int = 90,
) -> list[ModeEntry]:
    played_today = set(played_today_keys or [])
    anchor_tracks = [track for track in weekly_radar if track.kind == "anchor"]
    transition_tracks = [track for track in weekly_radar if track.kind == "transition"]

    target_ms = target_minutes * 60_000
    min_ms = min_minutes * 60_000
    max_ms = max_minutes * 60_000

    anchor_target = int(target_ms * 0.7)
    transition_target = target_ms - anchor_target

    selected_keys: set[str] = set()
    anchors = _select_tracks(anchor_tracks, anchor_target, max_ms, selected_keys, played_today)
    transitions = _select_tracks(
        transition_tracks,
        transition_target,
        max_ms,
        selected_keys,
        played_today,
    )

    selected = anchors + transitions
    total_ms = sum(track.duration_ms for track in selected)

    if total_ms < min_ms:
        remaining_tracks = [
            *[track for track in anchor_tracks if track.track_key not in selected_keys],
            *[track for track in transition_tracks if track.track_key not in selected_keys],
        ]
        for track in remaining_tracks:
            if total_ms >= min_ms:
                break
            if total_ms + track.duration_ms > max_ms:
                continue
            selected.append(track)
            selected_keys.add(track.track_key)
            total_ms += track.duration_ms

    if total_ms < min_ms:
        for track in anchor_tracks + transition_tracks:
            if total_ms >= min_ms:
                break
            if track.track_key in selected_keys:
                continue
            if total_ms + track.duration_ms > max_ms:
                continue
            selected.append(track)
            selected_keys.add(track.track_key)
            total_ms += track.duration_ms

    return [ModeEntry(track=track, position=index + 1) for index, track in enumerate(selected)]


def init_daily_mode_db(connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_mode (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mode_date TEXT NOT NULL,
            position INTEGER NOT NULL,
            track_name TEXT NOT NULL,
            artist_name TEXT NOT NULL,
            duration_ms INTEGER NOT NULL,
            track_key TEXT NOT NULL,
            kind TEXT NOT NULL
        )
        """
    )
    connection.commit()


def store_daily_mode(
    connection,
    mode_date: str,
    entries: Iterable[ModeEntry],
) -> int:
    rows = [
        (
            mode_date,
            entry.position,
            entry.track.track_name,
            entry.track.artist_name,
            entry.track.duration_ms,
            entry.track.track_key,
            entry.track.kind,
        )
        for entry in entries
    ]
    if not rows:
        return 0
    connection.executemany(
        """
        INSERT INTO daily_mode (
            mode_date,
            position,
            track_name,
            artist_name,
            duration_ms,
            track_key,
            kind
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    connection.commit()
    return len(rows)


def write_daily_mode_json(path: Path | str, mode_date: str, entries: Iterable[ModeEntry]) -> None:
    payload = {
        "date": mode_date,
        "entries": [
            {
                "position": entry.position,
                "track_name": entry.track.track_name,
                "artist_name": entry.track.artist_name,
                "duration_ms": entry.track.duration_ms,
                "track_key": entry.track.track_key,
                "kind": entry.track.kind,
            }
            for entry in entries
        ],
    }
    Path(path).write_text(json.dumps(payload, indent=2), encoding="utf-8")
