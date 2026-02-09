from __future__ import annotations

import sqlite3
from spotifygpt.importer import init_db
from spotifygpt.metrics import compute_and_store_metrics


def _insert_streams(connection, rows):
    connection.executemany(
        """
        INSERT INTO streams (track_name, artist_name, end_time, ms_played, track_key)
        VALUES (?, ?, ?, ?, ?)
        """,
        rows,
    )


def test_multiple_tracks_and_aggregations():
    with sqlite3.connect(":memory:") as connection:
        init_db(connection)
        _insert_streams(
            connection,
            [
                ("Track A", "Artist A", "2023-01-02 10:00", 120_000, "key-a"),
                ("Track A", "Artist A", "2023-01-02 11:00", 30_000, "key-a"),
                ("Track B", "Artist B", "2023-01-03 10:00", 90_000, "key-b"),
                ("Track B", "Artist B", "2023-01-03 10:30", 90_000, "key-b"),
            ],
        )
        compute_and_store_metrics(connection)

        track_rows = connection.execute(
            """
            SELECT track_key, play_count, total_ms_played, avg_ms_played
            FROM track_aggregates
            ORDER BY track_key
            """
        ).fetchall()

        assert track_rows == [
            ("key-a", 2, 150_000, 75_000.0),
            ("key-b", 2, 180_000, 90_000.0),
        ]

        hour_counts = dict(
            connection.execute(
                """
                SELECT bucket, play_count
                FROM temporal_distributions
                WHERE scope = 'global' AND bucket_type = 'hour'
                """
            ).fetchall()
        )
        assert hour_counts == {10: 3, 11: 1}


def test_persistence_proxy_calculation():
    with sqlite3.connect(":memory:") as connection:
        init_db(connection)
        _insert_streams(
            connection,
            [
                ("Track A", "Artist A", "2023-02-01 10:00", 120_000, "key-a"),
                ("Track A", "Artist A", "2023-02-02 10:00", 30_000, "key-a"),
            ],
        )
        compute_and_store_metrics(connection)

        persistence = connection.execute(
            """
            SELECT persistence_proxy
            FROM track_aggregates
            WHERE track_key = 'key-a'
            """
        ).fetchone()[0]

        assert persistence == 0.5


def test_rotation_metric():
    with sqlite3.connect(":memory:") as connection:
        init_db(connection)
        _insert_streams(
            connection,
            [
                ("Track A", "Artist A", "2023-03-01 10:00", 120_000, "key-a"),
                ("Track B", "Artist B", "2023-03-01 11:00", 90_000, "key-b"),
                ("Track A", "Artist A", "2023-03-02 12:00", 30_000, "key-a"),
                ("Track B", "Artist B", "2023-03-02 13:00", 90_000, "key-b"),
            ],
        )
        compute_and_store_metrics(connection)

        rotation = connection.execute(
            """
            SELECT metric_value
            FROM metrics_global
            WHERE metric_name = 'rotation'
            """
        ).fetchone()[0]

        assert rotation == 0.5


def test_empty_database_behavior():
    with sqlite3.connect(":memory:") as connection:
        init_db(connection)
        compute_and_store_metrics(connection)

        track_count = connection.execute(
            "SELECT COUNT(*) FROM track_aggregates"
        ).fetchone()[0]
        rotation = connection.execute(
            """
            SELECT metric_value
            FROM metrics_global
            WHERE metric_name = 'rotation'
            """
        ).fetchone()[0]

        assert track_count == 0
        assert rotation == 0.0
