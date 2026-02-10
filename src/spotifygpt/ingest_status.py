"""Ingest status report helpers for SpotifyGPT V2 datasets."""

from __future__ import annotations

from dataclasses import dataclass
import sqlite3


@dataclass(frozen=True)
class IngestStatus:
    tracks: int | None
    audio_features: int | None
    listening_events: int | None
    listening_min_date: str | None
    listening_max_date: str | None
    saved_tracks: int | None
    playlists: int | None
    playlist_tracks: int | None
    latest_ingest_run_by_mode: dict[str, str]
    warnings: list[str]


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _count_rows(connection: sqlite3.Connection, table: str) -> int:
    return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def collect_ingest_status(connection: sqlite3.Connection) -> IngestStatus:
    warnings: list[str] = []

    tracks = audio_features = listening_events = saved_tracks = playlists = playlist_tracks = None
    listening_min_date = listening_max_date = None

    if _table_exists(connection, "tracks"):
        tracks = _count_rows(connection, "tracks")
    else:
        warnings.append("Missing table: tracks")

    if _table_exists(connection, "audio_features"):
        audio_features = _count_rows(connection, "audio_features")
    else:
        warnings.append("Missing table: audio_features")

    if _table_exists(connection, "listening_events"):
        listening_events = _count_rows(connection, "listening_events")
        listening_min_date, listening_max_date = connection.execute(
            "SELECT MIN(played_at), MAX(played_at) FROM listening_events"
        ).fetchone()
    else:
        warnings.append("Missing table: listening_events")

    if _table_exists(connection, "saved_tracks"):
        saved_tracks = _count_rows(connection, "saved_tracks")
    else:
        warnings.append("Missing table: saved_tracks")

    if _table_exists(connection, "playlists"):
        playlists = _count_rows(connection, "playlists")
    else:
        warnings.append("Missing table: playlists")

    if _table_exists(connection, "playlist_tracks"):
        playlist_tracks = _count_rows(connection, "playlist_tracks")
    else:
        warnings.append("Missing table: playlist_tracks")

    latest_ingest_run_by_mode: dict[str, str] = {}
    if _table_exists(connection, "ingest_runs"):
        rows = connection.execute(
            """
            SELECT mode, MAX(created_at) AS latest_created_at
            FROM ingest_runs
            GROUP BY mode
            ORDER BY mode
            """
        ).fetchall()
        latest_ingest_run_by_mode = {mode: created_at for mode, created_at in rows}
    else:
        warnings.append("Missing table: ingest_runs")

    return IngestStatus(
        tracks=tracks,
        audio_features=audio_features,
        listening_events=listening_events,
        listening_min_date=listening_min_date,
        listening_max_date=listening_max_date,
        saved_tracks=saved_tracks,
        playlists=playlists,
        playlist_tracks=playlist_tracks,
        latest_ingest_run_by_mode=latest_ingest_run_by_mode,
        warnings=warnings,
    )


def render_ingest_status(status: IngestStatus) -> str:
    def fmt_count(value: int | None) -> str:
        return str(value) if value is not None else "n/a"

    date_range = "n/a"
    if status.listening_min_date is not None and status.listening_max_date is not None:
        date_range = f"{status.listening_min_date} .. {status.listening_max_date}"

    lines = [
        "Ingest status",
        f"- tracks: {fmt_count(status.tracks)}",
        f"- audio_features: {fmt_count(status.audio_features)}",
        f"- listening_events: {fmt_count(status.listening_events)} (date range: {date_range})",
        f"- saved_tracks: {fmt_count(status.saved_tracks)}",
        f"- playlists: {fmt_count(status.playlists)}",
        f"- playlist_tracks: {fmt_count(status.playlist_tracks)}",
    ]

    if status.latest_ingest_run_by_mode:
        lines.append("- latest ingest_run per mode:")
        for mode, created_at in status.latest_ingest_run_by_mode.items():
            lines.append(f"  - {mode}: {created_at}")
    else:
        lines.append("- latest ingest_run per mode: n/a")

    if status.warnings:
        lines.append("Warnings:")
        for warning in status.warnings:
            lines.append(f"- {warning}")

    return "\n".join(lines)
