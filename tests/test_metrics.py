import sqlite3

from spotifygpt.importer import Stream, compute_track_key, init_db, store_streams
from spotifygpt.metrics import compute_metrics


def _insert_streams(connection, rows):
    init_db(connection)
    store_streams(connection, rows)


def test_metrics_multiple_tracks():
    streams = [
        {
            "track_name": "Track A",
            "artist_name": "Artist 1",
            "end_time": "2023-01-01 08:00",
            "ms_played": 30000,
        },
        {
            "track_name": "Track A",
            "artist_name": "Artist 1",
            "end_time": "2023-01-02 09:00",
            "ms_played": 70000,
        },
        {
            "track_name": "Track B",
            "artist_name": "Artist 2",
            "end_time": "2023-01-03 10:00",
            "ms_played": 90000,
        },
    ]
    with sqlite3.connect(":memory:") as connection:
        _insert_streams(
            connection,
            [
                Stream(
                    track_name=row["track_name"],
                    artist_name=row["artist_name"],
                    end_time=row["end_time"],
                    ms_played=row["ms_played"],
                    track_key=compute_track_key(row["track_name"], row["artist_name"]),
                )
                for row in streams
            ],
        )
        compute_metrics(connection)

        track_rows = connection.execute(
            """
            SELECT track_name, play_count, total_ms_played, plays_over_60s, persistence_proxy
            FROM track_aggregates
            ORDER BY track_name
            """
        ).fetchall()
        assert track_rows == [
            ("Track A", 2, 100000, 1, 0.5),
            ("Track B", 1, 90000, 1, 1.0),
        ]

        global_row = connection.execute(
            """
            SELECT total_plays, unique_tracks, rotation
            FROM global_metrics
            """
        ).fetchone()
        assert global_row == (3, 2, 2 / 3)

        hour_counts = dict(
            connection.execute(
                """
                SELECT hour, play_count
                FROM temporal_hourly
                """
            ).fetchall()
        )
        assert hour_counts == {8: 1, 9: 1, 10: 1}

        weekday_counts = dict(
            connection.execute(
                """
                SELECT day_of_week, play_count
                FROM temporal_weekday
                """
            ).fetchall()
        )
        assert weekday_counts == {6: 1, 0: 1, 1: 1}


def test_repetition_score_and_rotation():
    streams = [
        ("Track A", "Artist 1", "2023-01-01 08:00", 30000),
        ("Track A", "Artist 1", "2023-01-02 09:00", 30000),
        ("Track A", "Artist 1", "2023-01-03 10:00", 30000),
    ]
    with sqlite3.connect(":memory:") as connection:
        _insert_streams(
            connection,
            [
                Stream(
                    track_name=track,
                    artist_name=artist,
                    end_time=end_time,
                    ms_played=ms_played,
                    track_key=compute_track_key(track, artist),
                )
                for track, artist, end_time, ms_played in streams
            ],
        )
        compute_metrics(connection)
        total_plays, unique_tracks, rotation, repetition_score = connection.execute(
            """
            SELECT total_plays, unique_tracks, rotation, repetition_score
            FROM global_metrics
            """
        ).fetchone()
        assert (total_plays, unique_tracks) == (3, 1)
        assert rotation == 1 / 3
        expected = (1 + 1 / 2 + 1 / 3) / 3
        assert repetition_score == expected


def test_empty_db_metrics():
    with sqlite3.connect(":memory:") as connection:
        init_db(connection)
        compute_metrics(connection)

        assert connection.execute(
            "SELECT COUNT(*) FROM track_aggregates"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT COUNT(*) FROM weekly_metrics"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT COUNT(*) FROM temporal_hourly"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT total_plays, rotation FROM global_metrics"
        ).fetchone() == (0, 0.0)
