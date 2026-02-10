from __future__ import annotations

import sqlite3

from spotifygpt.ingest_status import collect_ingest_status, render_ingest_status


def test_collect_ingest_status_complete_schema() -> None:
    with sqlite3.connect(":memory:") as connection:
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
            INSERT INTO tracks DEFAULT VALUES;
            INSERT INTO audio_features DEFAULT VALUES;
            INSERT INTO listening_events (played_at) VALUES ('2026-01-01T00:00:00Z');
            INSERT INTO listening_events (played_at) VALUES ('2026-01-15T12:30:00Z');
            INSERT INTO saved_tracks DEFAULT VALUES;
            INSERT INTO playlists DEFAULT VALUES;
            INSERT INTO playlists DEFAULT VALUES;
            INSERT INTO playlist_tracks DEFAULT VALUES;
            INSERT INTO playlist_tracks DEFAULT VALUES;
            INSERT INTO playlist_tracks DEFAULT VALUES;
            INSERT INTO ingest_runs (mode, created_at) VALUES ('full', '2026-01-10T00:00:00Z');
            INSERT INTO ingest_runs (mode, created_at) VALUES ('full', '2026-01-20T00:00:00Z');
            INSERT INTO ingest_runs (mode, created_at) VALUES ('incremental', '2026-01-22T09:00:00Z');
            """
        )

        status = collect_ingest_status(connection)

    assert status.tracks == 2
    assert status.audio_features == 1
    assert status.listening_events == 2
    assert status.listening_min_date == "2026-01-01T00:00:00Z"
    assert status.listening_max_date == "2026-01-15T12:30:00Z"
    assert status.saved_tracks == 1
    assert status.playlists == 2
    assert status.playlist_tracks == 3
    assert status.latest_ingest_run_by_mode == {
        "full": "2026-01-20T00:00:00Z",
        "incremental": "2026-01-22T09:00:00Z",
    }
    assert status.warnings == []


def test_collect_ingest_status_with_missing_tables() -> None:
    with sqlite3.connect(":memory:") as connection:
        status = collect_ingest_status(connection)

    assert status.tracks is None
    assert "Missing table: tracks" in status.warnings
    assert "Missing table: ingest_runs" in status.warnings


def test_render_ingest_status_includes_warning_block() -> None:
    with sqlite3.connect(":memory:") as connection:
        status = collect_ingest_status(connection)

    rendered = render_ingest_status(status)

    assert "Ingest status" in rendered
    assert "- latest ingest_run per mode: n/a" in rendered
    assert "Warnings:" in rendered
