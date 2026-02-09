from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

import pytest

from spotifygpt.import_streaming_history import (
    Stream,
    compute_track_key,
    import_streaming_history,
    initialize_db,
    insert_streams,
    load_streams,
)


@pytest.fixture()
def sample_dir() -> Path:
    return Path("data/sample")


def test_loads_multiple_files(sample_dir: Path) -> None:
    streams = load_streams(sample_dir)
    assert len(streams) == 3
    assert all(isinstance(stream, Stream) for stream in streams)


def test_normalization_track_key(sample_dir: Path) -> None:
    streams = load_streams(sample_dir)
    stream = streams[0]
    expected = hashlib.sha256(
        f"{stream.track_name}|{stream.artist_name}".encode("utf-8")
    ).hexdigest()
    assert compute_track_key(stream.track_name, stream.artist_name) == expected
    assert stream.track_key == expected


def test_inserts_into_sqlite(tmp_path: Path, sample_dir: Path) -> None:
    db_path = tmp_path / "streams.db"
    streams = load_streams(sample_dir)
    with sqlite3.connect(db_path) as connection:
        initialize_db(connection)
        insert_streams(connection, streams)
        rows = connection.execute("SELECT COUNT(*) FROM streams").fetchone()

    assert rows is not None
    assert rows[0] == len(streams)


def test_import_end_to_end(tmp_path: Path, sample_dir: Path) -> None:
    db_path = tmp_path / "import.db"
    count = import_streaming_history(sample_dir, db_path)
    with sqlite3.connect(db_path) as connection:
        rows = connection.execute("SELECT COUNT(*) FROM streams").fetchone()

    assert rows is not None
    assert rows[0] == count


def test_empty_input_folder_raises(tmp_path: Path) -> None:
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    with pytest.raises(FileNotFoundError):
        load_streams(empty_dir)
