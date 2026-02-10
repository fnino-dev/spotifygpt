from __future__ import annotations

from pathlib import Path
import sqlite3

from spotifygpt.cli import main
from spotifygpt.manual_import import (
    init_manual_import_tables,
    load_manual_payload,
    store_manual_payload,
)


SAMPLE_DIR = Path(__file__).resolve().parents[1] / "data" / "sample"


def test_manual_payload_loads_sample_files() -> None:
    payload = load_manual_payload(
        SAMPLE_DIR / "liked.json",
        SAMPLE_DIR / "playlists.json",
    )

    assert len(payload.liked_tracks) == 2
    assert len(payload.playlists) == 1
    assert payload.playlists[0].name == "Morning Mix"
    assert len(payload.playlists[0].tracks) == 2


def test_manual_import_stores_all_tables(tmp_path: Path) -> None:
    payload = load_manual_payload(
        SAMPLE_DIR / "liked.json",
        SAMPLE_DIR / "playlists.json",
    )
    db_path = tmp_path / "manual.db"

    with sqlite3.connect(db_path) as connection:
        init_manual_import_tables(connection)
        result = store_manual_payload(connection, payload)

        tracks_count = connection.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
        library_count = connection.execute("SELECT COUNT(*) FROM library").fetchone()[0]
        playlists_count = connection.execute("SELECT COUNT(*) FROM playlists").fetchone()[0]
        playlist_tracks_count = connection.execute(
            "SELECT COUNT(*) FROM playlist_tracks"
        ).fetchone()[0]

    assert result.tracks == 3
    assert result.library == 2
    assert result.playlists == 1
    assert result.playlist_tracks == 2
    assert tracks_count == 3
    assert library_count == 2
    assert playlists_count == 1
    assert playlist_tracks_count == 2


def test_cli_import_manual(tmp_path: Path) -> None:
    db_path = tmp_path / "spotifygpt.db"

    exit_code = main(
        [
            "import-manual",
            "--liked",
            str(SAMPLE_DIR / "liked.json"),
            "--playlists",
            str(SAMPLE_DIR / "playlists.json"),
            "--db",
            str(db_path),
        ]
    )

    assert exit_code == 0

    with sqlite3.connect(db_path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }

    assert {"tracks", "library", "playlists", "playlist_tracks"}.issubset(tables)
