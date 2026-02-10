"""Spotify Web API standard ingestion sync (v2)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import sqlite3
import time
from typing import Any
from urllib import error, parse, request


TOP_TIME_RANGES = ("short_term", "medium_term", "long_term")


@dataclass(frozen=True)
class SyncSummary:
    run_id: int
    saved_tracks: int
    playlist_tracks: int
    top_items: int
    recent_tracks: int


class SpotifyAPIClient:
    """Small Spotify API client with light retry/backoff for rate limits."""

    base_url = "https://api.spotify.com/v1"

    def __init__(self, token: str, timeout: float = 20.0, max_retries: int = 3) -> None:
        self._token = token
        self._timeout = timeout
        self._max_retries = max_retries

    def _request_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        query = f"?{parse.urlencode(params)}" if params else ""
        url = f"{self.base_url}{path}{query}"
        req = request.Request(
            url,
            headers={"Authorization": f"Bearer {self._token}"},
        )
        attempt = 0
        while True:
            attempt += 1
            try:
                with request.urlopen(req, timeout=self._timeout) as response:
                    payload = response.read().decode("utf-8")
                    return json.loads(payload)
            except error.HTTPError as exc:
                if exc.code == 429 and attempt <= self._max_retries:
                    retry_after = exc.headers.get("Retry-After", "1")
                    wait_seconds = max(1.0, float(retry_after))
                    time.sleep(wait_seconds)
                    continue
                raise RuntimeError(f"Spotify API request failed ({exc.code}) for {url}") from exc
            except error.URLError as exc:
                raise RuntimeError(f"Spotify API request failed for {url}: {exc}") from exc

    def _paginate(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        merged = dict(params or {})
        merged.setdefault("limit", 50)
        offset = 0
        items: list[dict[str, Any]] = []
        while True:
            merged["offset"] = offset
            response = self._request_json(path, merged)
            page_items = response.get("items", [])
            if not isinstance(page_items, list):
                raise RuntimeError(f"Unexpected response shape for {path}")
            items.extend(page_items)
            if len(page_items) < int(merged["limit"]):
                break
            offset += int(merged["limit"])
        return items

    def get_saved_tracks(self, since: str | None) -> list[dict[str, Any]]:
        return self._paginate("/me/tracks")

    def get_playlists(self) -> list[dict[str, Any]]:
        return self._paginate("/me/playlists")

    def get_playlist_tracks(self, playlist_id: str) -> list[dict[str, Any]]:
        return self._paginate(f"/playlists/{playlist_id}/tracks")

    def get_top_tracks(self, time_range: str) -> list[dict[str, Any]]:
        return self._paginate("/me/top/tracks", {"time_range": time_range})

    def get_top_artists(self, time_range: str) -> list[dict[str, Any]]:
        return self._paginate("/me/top/artists", {"time_range": time_range})

    def get_recently_played(self, since: str | None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": 50}
        if since:
            params["after"] = int(datetime.fromisoformat(since).timestamp() * 1000)
        response = self._request_json("/me/player/recently-played", params)
        items = response.get("items", [])
        if not isinstance(items, list):
            raise RuntimeError("Unexpected recently played response")
        return items


class SyncService:
    def __init__(self, client: SpotifyAPIClient) -> None:
        self._client = client

    def init_db(self, connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS ingest_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mode TEXT NOT NULL,
                since TEXT,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                status TEXT NOT NULL,
                error_message TEXT
            );

            CREATE TABLE IF NOT EXISTS saved_tracks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                track_id TEXT NOT NULL,
                added_at TEXT NOT NULL,
                name TEXT NOT NULL,
                artists TEXT NOT NULL,
                album TEXT NOT NULL,
                UNIQUE(track_id, added_at)
            );

            CREATE TABLE IF NOT EXISTS playlists (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                owner_id TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS playlist_tracks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                playlist_id TEXT NOT NULL,
                track_id TEXT NOT NULL,
                added_at TEXT,
                position INTEGER NOT NULL,
                track_name TEXT NOT NULL,
                artists TEXT NOT NULL,
                UNIQUE(playlist_id, track_id, position)
            );

            CREATE TABLE IF NOT EXISTS top_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id TEXT NOT NULL,
                item_type TEXT NOT NULL,
                time_range TEXT NOT NULL,
                rank INTEGER NOT NULL,
                name TEXT NOT NULL,
                run_id INTEGER NOT NULL,
                UNIQUE(item_id, item_type, time_range, run_id),
                FOREIGN KEY(run_id) REFERENCES ingest_runs(id)
            );

            CREATE TABLE IF NOT EXISTS recently_played (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                played_at TEXT NOT NULL,
                track_id TEXT NOT NULL,
                track_name TEXT NOT NULL,
                artists TEXT NOT NULL,
                UNIQUE(played_at, track_id)
            );
            """
        )
        ingest_run_columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info(ingest_runs)").fetchall()
        }
        required_columns = {
            "source": "TEXT NOT NULL DEFAULT 'spotify_api'",
            "since": "TEXT",
            "completed_at": "TEXT",
            "status": "TEXT NOT NULL DEFAULT 'RUNNING'",
            "error_message": "TEXT",
        }
        for column, column_type in required_columns.items():
            if column not in ingest_run_columns:
                connection.execute(f"ALTER TABLE ingest_runs ADD COLUMN {column} {column_type}")
        connection.commit()

    def run_standard_sync(self, connection: sqlite3.Connection, since: str | None) -> SyncSummary:
        now = datetime.now(tz=timezone.utc).isoformat()
        run_id = connection.execute(
            "INSERT INTO ingest_runs (mode, source, since, started_at, status) VALUES (?, ?, ?, ?, ?)",
            ("STANDARD", "spotify_api", since, now, "RUNNING"),
        ).lastrowid
        connection.commit()

        try:
            saved_count = self._ingest_saved_tracks(connection, since)
            playlist_count = self._ingest_playlists(connection)
            top_count = self._ingest_top_items(connection, run_id)
            recent_count = self._ingest_recently_played(connection, since)

            connection.execute(
                "UPDATE ingest_runs SET completed_at = ?, status = ? WHERE id = ?",
                (datetime.now(tz=timezone.utc).isoformat(), "SUCCESS", run_id),
            )
            connection.commit()
            return SyncSummary(
                run_id=run_id,
                saved_tracks=saved_count,
                playlist_tracks=playlist_count,
                top_items=top_count,
                recent_tracks=recent_count,
            )
        except Exception as exc:
            connection.execute(
                "UPDATE ingest_runs SET completed_at = ?, status = ?, error_message = ? WHERE id = ?",
                (datetime.now(tz=timezone.utc).isoformat(), "FAILED", str(exc), run_id),
            )
            connection.commit()
            raise

    def _ingest_saved_tracks(self, connection: sqlite3.Connection, since: str | None) -> int:
        items = self._client.get_saved_tracks(since)
        inserted = 0
        for item in items:
            added_at = item.get("added_at")
            track = item.get("track") or {}
            if not isinstance(added_at, str) or not isinstance(track, dict):
                continue
            if since and added_at < since:
                continue
            track_id = track.get("id")
            track_name = track.get("name")
            album = ((track.get("album") or {}).get("name")) if isinstance(track.get("album"), dict) else ""
            artists = ", ".join(
                artist.get("name", "")
                for artist in track.get("artists", [])
                if isinstance(artist, dict)
            )
            if not isinstance(track_id, str) or not isinstance(track_name, str):
                continue
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO saved_tracks (track_id, added_at, name, artists, album)
                VALUES (?, ?, ?, ?, ?)
                """,
                (track_id, added_at, track_name, artists, album or ""),
            )
            inserted += cursor.rowcount
        connection.commit()
        return inserted

    def _ingest_playlists(self, connection: sqlite3.Connection) -> int:
        playlists = self._client.get_playlists()
        inserted = 0
        for playlist in playlists:
            playlist_id = playlist.get("id")
            owner = playlist.get("owner") or {}
            owner_id = owner.get("id") if isinstance(owner, dict) else None
            if not isinstance(playlist_id, str) or not isinstance(owner_id, str):
                continue
            connection.execute(
                "INSERT OR REPLACE INTO playlists (id, name, owner_id) VALUES (?, ?, ?)",
                (playlist_id, str(playlist.get("name", "")), owner_id),
            )
            for idx, item in enumerate(self._client.get_playlist_tracks(playlist_id)):
                track = item.get("track") or {}
                if not isinstance(track, dict):
                    continue
                track_id = track.get("id")
                track_name = track.get("name")
                artists = ", ".join(
                    artist.get("name", "")
                    for artist in track.get("artists", [])
                    if isinstance(artist, dict)
                )
                if not isinstance(track_id, str) or not isinstance(track_name, str):
                    continue
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO playlist_tracks
                    (playlist_id, track_id, added_at, position, track_name, artists)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (playlist_id, track_id, item.get("added_at"), idx, track_name, artists),
                )
                inserted += cursor.rowcount
        connection.commit()
        return inserted

    def _ingest_top_items(self, connection: sqlite3.Connection, run_id: int) -> int:
        inserted = 0
        for time_range in TOP_TIME_RANGES:
            for item_type, fetch in (
                ("track", self._client.get_top_tracks),
                ("artist", self._client.get_top_artists),
            ):
                for rank, item in enumerate(fetch(time_range), start=1):
                    item_id = item.get("id")
                    name = item.get("name")
                    if not isinstance(item_id, str) or not isinstance(name, str):
                        continue
                    cursor = connection.execute(
                        """
                        INSERT OR IGNORE INTO top_items
                        (item_id, item_type, time_range, rank, name, run_id)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (item_id, item_type, time_range, rank, name, run_id),
                    )
                    inserted += cursor.rowcount
        connection.commit()
        return inserted

    def _ingest_recently_played(self, connection: sqlite3.Connection, since: str | None) -> int:
        inserted = 0
        for item in self._client.get_recently_played(since):
            played_at = item.get("played_at")
            track = item.get("track") or {}
            if not isinstance(played_at, str) or not isinstance(track, dict):
                continue
            if since and played_at < since:
                continue
            track_id = track.get("id")
            track_name = track.get("name")
            artists = ", ".join(
                artist.get("name", "")
                for artist in track.get("artists", [])
                if isinstance(artist, dict)
            )
            if not isinstance(track_id, str) or not isinstance(track_name, str):
                continue
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO recently_played (played_at, track_id, track_name, artists)
                VALUES (?, ?, ?, ?)
                """,
                (played_at, track_id, track_name, artists),
            )
            inserted += cursor.rowcount
        connection.commit()
        return inserted
