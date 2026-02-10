"""Manual import flow for liked songs and playlists exports."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import json
import sqlite3
from typing import Any

from spotifygpt.importer import compute_track_key


@dataclass(frozen=True)
class ManualTrack:
    track_name: str
    artist_name: str
    spotify_uri: str | None
    added_at: str | None


@dataclass(frozen=True)
class ManualPlaylist:
    name: str
    tracks: list[ManualTrack]


@dataclass(frozen=True)
class ManualImportPayload:
    liked_tracks: list[ManualTrack]
    playlists: list[ManualPlaylist]


@dataclass(frozen=True)
class ManualImportResult:
    tracks: int
    library: int
    playlists: int
    playlist_tracks: int


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _first_string(source: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = source.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _extract_track(entry: dict[str, Any]) -> ManualTrack | None:
    if "track" in entry and isinstance(entry["track"], dict):
        source = entry["track"]
        added_at = _first_string(entry, ("added_at", "addedAt"))
    else:
        source = entry
        added_at = _first_string(entry, ("added_at", "addedAt"))

    track_name = _first_string(source, ("name", "track_name", "trackName"))
    if track_name is None:
        return None

    artist_name = _first_string(source, ("artist_name", "artistName", "artist"))
    artists = source.get("artists")
    if artist_name is None and isinstance(artists, list) and artists:
        first_artist = artists[0]
        if isinstance(first_artist, dict):
            artist_name = _first_string(first_artist, ("name",))
        elif isinstance(first_artist, str):
            artist_name = first_artist.strip() or None
    if artist_name is None:
        return None

    spotify_uri = _first_string(source, ("spotify_uri", "spotifyUri", "uri"))
    return ManualTrack(
        track_name=track_name,
        artist_name=artist_name,
        spotify_uri=spotify_uri,
        added_at=added_at,
    )


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_manual_payload(liked_path: Path, playlists_path: Path) -> ManualImportPayload:
    liked_raw = _load_json(liked_path)
    playlists_raw = _load_json(playlists_path)

    liked_tracks: list[ManualTrack] = []
    if isinstance(liked_raw, list):
        for entry in liked_raw:
            if not isinstance(entry, dict):
                continue
            track = _extract_track(entry)
            if track is not None:
                liked_tracks.append(track)

    playlists: list[ManualPlaylist] = []
    if isinstance(playlists_raw, list):
        for playlist_entry in playlists_raw:
            if not isinstance(playlist_entry, dict):
                continue
            name = _first_string(playlist_entry, ("name", "playlist_name", "playlistName"))
            if name is None:
                continue
            tracks_raw = playlist_entry.get("tracks")
            if not isinstance(tracks_raw, list):
                continue
            tracks: list[ManualTrack] = []
            for track_entry in tracks_raw:
                if not isinstance(track_entry, dict):
                    continue
                track = _extract_track(track_entry)
                if track is not None:
                    tracks.append(track)
            playlists.append(ManualPlaylist(name=name, tracks=tracks))

    return ManualImportPayload(liked_tracks=liked_tracks, playlists=playlists)


def init_manual_import_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spotify_uri TEXT,
            track_name TEXT NOT NULL,
            artist_name TEXT NOT NULL,
            track_key TEXT NOT NULL UNIQUE
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS library (
            track_id INTEGER PRIMARY KEY,
            added_at TEXT NOT NULL,
            FOREIGN KEY (track_id) REFERENCES tracks(id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS playlists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS playlist_tracks (
            playlist_id INTEGER NOT NULL,
            track_id INTEGER NOT NULL,
            position INTEGER NOT NULL,
            added_at TEXT NOT NULL,
            PRIMARY KEY (playlist_id, track_id),
            FOREIGN KEY (playlist_id) REFERENCES playlists(id),
            FOREIGN KEY (track_id) REFERENCES tracks(id)
        )
        """
    )
    connection.commit()


def _upsert_track(connection: sqlite3.Connection, track: ManualTrack) -> int:
    track_key = compute_track_key(track.track_name, track.artist_name)
    connection.execute(
        """
        INSERT INTO tracks (spotify_uri, track_name, artist_name, track_key)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(track_key) DO UPDATE SET spotify_uri = COALESCE(excluded.spotify_uri, tracks.spotify_uri)
        """,
        (track.spotify_uri, track.track_name, track.artist_name, track_key),
    )
    row = connection.execute("SELECT id FROM tracks WHERE track_key = ?", (track_key,)).fetchone()
    return int(row[0])


def store_manual_payload(connection: sqlite3.Connection, payload: ManualImportPayload) -> ManualImportResult:
    track_ids: set[int] = set()
    library_rows = 0
    playlist_rows = 0
    playlist_track_rows = 0

    for track in payload.liked_tracks:
        track_id = _upsert_track(connection, track)
        track_ids.add(track_id)
        connection.execute(
            """
            INSERT OR REPLACE INTO library (track_id, added_at)
            VALUES (?, ?)
            """,
            (track_id, track.added_at or _now_iso()),
        )
        library_rows += 1

    for playlist in payload.playlists:
        connection.execute(
            """
            INSERT OR IGNORE INTO playlists (name)
            VALUES (?)
            """,
            (playlist.name,),
        )
        playlist_id = int(
            connection.execute("SELECT id FROM playlists WHERE name = ?", (playlist.name,)).fetchone()[0]
        )
        playlist_rows += 1

        for position, track in enumerate(playlist.tracks, start=1):
            track_id = _upsert_track(connection, track)
            track_ids.add(track_id)
            connection.execute(
                """
                INSERT OR REPLACE INTO playlist_tracks (playlist_id, track_id, position, added_at)
                VALUES (?, ?, ?, ?)
                """,
                (playlist_id, track_id, position, track.added_at or _now_iso()),
            )
            playlist_track_rows += 1

    connection.commit()
    return ManualImportResult(
        tracks=len(track_ids),
        library=library_rows,
        playlists=playlist_rows,
        playlist_tracks=playlist_track_rows,
    )
