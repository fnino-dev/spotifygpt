from __future__ import annotations

from hashlib import sha256
from pathlib import Path
import sqlite3

import pytest

from spotifygpt.importer import (
    compute_track_key,
    load_streaming_history,
    store_streams,
    init_db,
)


SAMPLE_DIR = Path(__file__).resolve().parents[1] / "data" / "sample"


def test_loads_multiple_files():
    result = load_streaming_history(SAMPLE_DIR)

    assert len(result.files) == 2
    assert len(result.streams) == 3
    assert result.errors == []


def test_schema_normalization():
    result = load_streaming_history(SAMPLE_DIR)
    stream = result.streams[0]

    expected = sha256(f"{stream.track_name}|{stream.artist_name}".encode("utf-8")).hexdigest()
    assert stream.track_key == expected
    assert compute_track_key(stream.track_name, stream.artist_name) == expected


def test_sqlite_insertion(tmp_path: Path):
    result = load_streaming_history(SAMPLE_DIR)
    db_path = tmp_path / "streams.db"

    with sqlite3.connect(db_path) as connection:
        init_db(connection)
        inserted = store_streams(connection, result.streams)
        count = connection.execute("SELECT COUNT(*) FROM streams").fetchone()[0]

    assert inserted == 3
    assert count == 3


def test_empty_input_folder(tmp_path: Path):
    result = load_streaming_history(tmp_path)

    assert result.files == []
    assert result.streams == []
    assert result.errors == []


def test_malformed_json_is_reported(tmp_path: Path):
    bad_file = tmp_path / "StreamingHistory0.json"
    bad_file.write_text("not-json", encoding="utf-8")

    result = load_streaming_history(tmp_path)

    assert result.streams == []
    assert len(result.errors) == 1
    assert "Malformed JSON" in result.errors[0].message
