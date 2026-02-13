from __future__ import annotations

from pathlib import Path
import sqlite3

from spotifygpt.cli import main
from spotifygpt.sync_v2 import SpotifyAPIError, SyncService


class FakeSpotifyClient:
    def __init__(self) -> None:
        self.saved_tracks_calls: list[str | None] = []
        self.recent_calls: list[str | None] = []

    def get_saved_tracks(self, since: str | None):
        self.saved_tracks_calls.append(since)
        return [
            {
                "added_at": "2026-01-04T10:00:00Z",
                "track": {
                    "id": "track-1",
                    "name": "Alpha",
                    "artists": [{"name": "Artist A"}],
                    "album": {"name": "Album A"},
                },
            },
            {
                "added_at": "2025-12-01T10:00:00Z",
                "track": {
                    "id": "track-2",
                    "name": "Old",
                    "artists": [{"name": "Artist B"}],
                    "album": {"name": "Album B"},
                },
            },
        ]

    def get_playlists(self):
        return [{"id": "pl-1", "name": "Main", "owner": {"id": "owner-1"}}]

    def get_playlist_tracks(self, playlist_id: str):
        assert playlist_id == "pl-1"
        return [
            {
                "added_at": "2026-01-05T10:00:00Z",
                "track": {
                    "id": "track-1",
                    "name": "Alpha",
                    "artists": [{"name": "Artist A"}],
                },
            }
        ]

    def get_top_tracks(self, time_range: str):
        return [{"id": f"top-track-{time_range}", "name": f"Top Track {time_range}"}]

    def get_top_artists(self, time_range: str):
        return [{"id": f"top-artist-{time_range}", "name": f"Top Artist {time_range}"}]

    def get_recently_played(self, since: str | None):
        self.recent_calls.append(since)
        return [
            {
                "played_at": "2026-01-06T10:00:00Z",
                "track": {
                    "id": "track-1",
                    "name": "Alpha",
                    "artists": [{"name": "Artist A"}],
                },
            },
            {
                "played_at": "2025-11-05T10:00:00Z",
                "track": {
                    "id": "track-9",
                    "name": "Very Old",
                    "artists": [{"name": "Artist Z"}],
                },
            },
        ]


def test_standard_sync_end_to_end_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "sync.db"
    client = FakeSpotifyClient()
    service = SyncService(client)

    with sqlite3.connect(db_path) as connection:
        service.init_db(connection)

        first = service.run_standard_sync(connection, since="2026-01-01T00:00:00Z")
        second = service.run_standard_sync(connection, since="2026-01-01T00:00:00Z")

        assert first.saved_tracks == 1
        assert first.playlist_tracks == 1
        assert first.top_items == 6
        assert first.recent_tracks == 1

        assert second.saved_tracks == 0
        assert second.playlist_tracks == 0
        assert second.top_items == 6
        assert second.recent_tracks == 0

        saved_count = connection.execute("SELECT COUNT(*) FROM saved_tracks").fetchone()[0]
        playlist_count = connection.execute("SELECT COUNT(*) FROM playlist_tracks").fetchone()[0]
        top_count = connection.execute("SELECT COUNT(*) FROM top_items").fetchone()[0]
        recent_count = connection.execute("SELECT COUNT(*) FROM recently_played").fetchone()[0]
        run_count = connection.execute("SELECT COUNT(*) FROM ingest_runs").fetchone()[0]

    assert saved_count == 1
    assert playlist_count == 1
    assert top_count == 12
    assert recent_count == 1
    assert run_count == 2
    assert client.saved_tracks_calls == ["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]
    assert client.recent_calls == ["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]


def test_sync_cli_command(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "cli-sync.db"

    class PatchedService(SyncService):
        def __init__(self, _client):
            super().__init__(FakeSpotifyClient())

    monkeypatch.setattr("spotifygpt.cli.SyncService", PatchedService)
    monkeypatch.setattr("spotifygpt.cli.SpotifyAPIClient", lambda token: object())

    exit_code = main(
        [
            "sync",
            str(db_path),
            "--token",
            "dummy-token",
            "--since",
            "2026-01-01T00:00:00Z",
        ]
    )

    assert exit_code == 0

    with sqlite3.connect(db_path) as connection:
        run = connection.execute("SELECT mode, since, status FROM ingest_runs").fetchone()

    assert run == ("STANDARD", "2026-01-01T00:00:00Z", "SUCCESS")


def test_standard_sync_skips_playlist_tracks_forbidden(tmp_path: Path, caplog) -> None:
    class ForbiddenPlaylistClient(FakeSpotifyClient):
        def get_playlists(self):
            return [
                {"id": "pl-1", "name": "Forbidden", "owner": {"id": "owner-1"}},
                {"id": "pl-2", "name": "Allowed", "owner": {"id": "owner-1"}},
            ]

        def get_playlist_tracks(self, playlist_id: str):
            if playlist_id == "pl-1":
                raise SpotifyAPIError("Spotify API request failed (403)", status_code=403)
            return [
                {
                    "added_at": "2026-01-05T10:00:00Z",
                    "track": {
                        "id": "track-allowed",
                        "name": "Allowed Song",
                        "artists": [{"name": "Artist A"}],
                    },
                }
            ]

    db_path = tmp_path / "sync-forbidden.db"
    service = SyncService(ForbiddenPlaylistClient())

    with sqlite3.connect(db_path) as connection:
        service.init_db(connection)
        summary = service.run_standard_sync(connection, since="2026-01-01T00:00:00Z")

        playlist_tracks_count = connection.execute("SELECT COUNT(*) FROM playlist_tracks").fetchone()[0]
        playlist_count = connection.execute("SELECT COUNT(*) FROM playlists").fetchone()[0]
        run_status = connection.execute("SELECT status FROM ingest_runs ORDER BY id DESC").fetchone()[0]

    assert summary.playlist_tracks == 1
    assert playlist_tracks_count == 1
    assert playlist_count == 2
    assert run_status == "SUCCESS"
    assert "Skipping playlist due to 403 Forbidden: id=pl-1 name=Forbidden" in caplog.text


def test_sync_cli_skips_forbidden_playlist_and_completes(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "cli-sync-forbidden.db"

    class ForbiddenPlaylistClient(FakeSpotifyClient):
        def get_playlists(self):
            return [
                {"id": "pl-1", "name": "Forbidden", "owner": {"id": "owner-1"}},
                {"id": "pl-2", "name": "Allowed", "owner": {"id": "owner-1"}},
            ]

        def get_playlist_tracks(self, playlist_id: str):
            if playlist_id == "pl-1":
                raise SpotifyAPIError("Spotify API request failed (403)", status_code=403)
            return [
                {
                    "added_at": "2026-01-05T10:00:00Z",
                    "track": {
                        "id": "track-allowed",
                        "name": "Allowed Song",
                        "artists": [{"name": "Artist A"}],
                    },
                }
            ]

    class PatchedService(SyncService):
        def __init__(self, _client):
            super().__init__(ForbiddenPlaylistClient())

    monkeypatch.setattr("spotifygpt.cli.SyncService", PatchedService)
    monkeypatch.setattr("spotifygpt.cli.SpotifyAPIClient", lambda token: object())

    exit_code = main(
        [
            "sync",
            str(db_path),
            "--token",
            "dummy-token",
            "--since",
            "2026-01-01T00:00:00Z",
        ]
    )

    assert exit_code == 0

    with sqlite3.connect(db_path) as connection:
        run_status = connection.execute("SELECT status FROM ingest_runs").fetchone()[0]
        playlist_tracks_count = connection.execute("SELECT COUNT(*) FROM playlist_tracks").fetchone()[0]
        playlist_count = connection.execute("SELECT COUNT(*) FROM playlists").fetchone()[0]

    assert run_status == "SUCCESS"
    assert playlist_tracks_count == 1
    assert playlist_count == 2
