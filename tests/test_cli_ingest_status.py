from __future__ import annotations

from pathlib import Path
import sqlite3

from spotifygpt.cli import main


def test_cli_ingest_status(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "ingest.db"
    with sqlite3.connect(db_path) as connection:
        connection.executescript(
            """
            CREATE TABLE tracks (id INTEGER PRIMARY KEY);
            CREATE TABLE audio_features (id INTEGER PRIMARY KEY);
            CREATE TABLE listening_events (id INTEGER PRIMARY KEY, played_at TEXT NOT NULL);
            CREATE TABLE saved_tracks (id INTEGER PRIMARY KEY);
            CREATE TABLE playlists (id INTEGER PRIMARY KEY);
            CREATE TABLE playlist_tracks (id INTEGER PRIMARY KEY);
            CREATE TABLE ingest_runs (id INTEGER PRIMARY KEY, mode TEXT NOT NULL, created_at TEXT NOT NULL);

            INSERT INTO tracks DEFAULT VALUES;
            INSERT INTO audio_features DEFAULT VALUES;
            INSERT INTO listening_events (played_at) VALUES ('2026-02-01T00:00:00Z');
            INSERT INTO saved_tracks DEFAULT VALUES;
            INSERT INTO playlists DEFAULT VALUES;
            INSERT INTO playlist_tracks DEFAULT VALUES;
            INSERT INTO ingest_runs (mode, created_at) VALUES ('full', '2026-02-02T00:00:00Z');
            """
        )

    code = main(["ingest-status", str(db_path)])
    captured = capsys.readouterr()

    assert code == 0
    assert "tracks: 1" in captured.out
    assert "audio_features: 1" in captured.out
    assert "latest ingest_run per mode" in captured.out
