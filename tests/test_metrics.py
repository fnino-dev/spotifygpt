import sqlite3
import pytest
from spotifygpt.importer import Stream, init_db, store_streams
from spotifygpt.metrics import build_metrics


def _rows_by_key(connection, table, key_column):
    rows = connection.execute(f"SELECT * FROM {table}").fetchall()
    column_names = [
        col[1] for col in connection.execute(f"PRAGMA table_info({table})")
    ]
    keyed = {}
    for row in rows:
        data = dict(zip(column_names, row))
        keyed[data[key_column]] = data
    return keyed


def test_metrics_multiple_tracks():
    streams = [
        Stream(
            track_name="Track A",
            artist_name="Artist A",
            end_time="2023-01-01 10:00",
            ms_played=30000,
            track_key="a",
        ),
        Stream(
            track_name="Track A",
            artist_name="Artist A",
            end_time="2023-01-02 11:00",
            ms_played=70000,
            track_key="a",
        ),
        Stream(
            track_name="Track B",
            artist_name="Artist B",
            end_time="2023-01-02 12:00",
            ms_played=80000,
            track_key="b",
        ),
    ]

    with sqlite3.connect(":memory:") as connection:
        init_db(connection)
        store_streams(connection, streams)
        build_metrics(connection)

        track_rows = _rows_by_key(connection, "track_aggregates", "track_key")

    track_a = track_rows["a"]
    assert track_a["play_count"] == 2
    assert track_a["total_ms_played"] == 100000
    assert track_a["avg_ms_played"] == 50000
    assert track_a["plays_over_60s"] == 1
    assert track_a["persistence_proxy"] == 0.5
    assert track_a["repetition_score"] > 0

    track_b = track_rows["b"]
    assert track_b["play_count"] == 1
    assert track_b["plays_over_60s"] == 1
    assert track_b["persistence_proxy"] == 1.0


def test_persistence_proxy_correctness():
    streams = [
        Stream(
            track_name="Track A",
            artist_name="Artist A",
            end_time="2023-01-01 10:00",
            ms_played=70000,
            track_key="a",
        ),
        Stream(
            track_name="Track A",
            artist_name="Artist A",
            end_time="2023-01-02 11:00",
            ms_played=30000,
            track_key="a",
        ),
    ]

    with sqlite3.connect(":memory:") as connection:
        init_db(connection)
        store_streams(connection, streams)
        build_metrics(connection)
        persistence_proxy = connection.execute(
            "SELECT persistence_proxy FROM track_aggregates WHERE track_key = ?",
            ("a",),
        ).fetchone()[0]

    assert persistence_proxy == 0.5


def test_rotation_correctness():
    streams = [
        Stream(
            track_name="Track A",
            artist_name="Artist A",
            end_time="2023-01-01 10:00",
            ms_played=30000,
            track_key="a",
        ),
        Stream(
            track_name="Track A",
            artist_name="Artist A",
            end_time="2023-01-01 11:00",
            ms_played=30000,
            track_key="a",
        ),
        Stream(
            track_name="Track B",
            artist_name="Artist B",
            end_time="2023-01-01 12:00",
            ms_played=30000,
            track_key="b",
        ),
    ]

    with sqlite3.connect(":memory:") as connection:
        init_db(connection)
        store_streams(connection, streams)
        build_metrics(connection)
        rotation = connection.execute(
            "SELECT metric_value FROM global_metrics WHERE metric_name = 'rotation'"
        ).fetchone()[0]

    assert rotation == pytest.approx(2 / 3)


def test_empty_db_behavior():
    with sqlite3.connect(":memory:") as connection:
        init_db(connection)
        build_metrics(connection)
        total_plays = connection.execute(
            "SELECT metric_value FROM global_metrics WHERE metric_name = 'total_plays'"
        ).fetchone()[0]
        rotation = connection.execute(
            "SELECT metric_value FROM global_metrics WHERE metric_name = 'rotation'"
        ).fetchone()[0]
        track_count = connection.execute(
            "SELECT COUNT(*) FROM track_aggregates"
        ).fetchone()[0]

    assert total_plays == 0
    assert rotation == 0
    assert track_count == 0
