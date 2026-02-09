"""Compute and persist metrics from normalized stream records."""

from __future__ import annotations

import sqlite3


def init_metrics_db(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS track_aggregates (
            track_key TEXT PRIMARY KEY,
            track_name TEXT NOT NULL,
            artist_name TEXT NOT NULL,
            play_count INTEGER NOT NULL,
            total_ms_played INTEGER NOT NULL,
            avg_ms_played REAL NOT NULL,
            plays_over_60s INTEGER NOT NULL,
            persistence_proxy REAL NOT NULL,
            repetition_score REAL NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS global_metrics (
            metric_name TEXT PRIMARY KEY,
            metric_value REAL NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS weekly_metrics (
            week_key TEXT NOT NULL,
            total_plays INTEGER NOT NULL,
            unique_tracks INTEGER NOT NULL,
            rotation REAL NOT NULL,
            PRIMARY KEY (week_key)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS temporal_hourly (
            hour INTEGER PRIMARY KEY,
            play_count INTEGER NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS temporal_weekday (
            day_of_week INTEGER PRIMARY KEY,
            play_count INTEGER NOT NULL
        )
        """
    )
    connection.commit()


def build_metrics(connection: sqlite3.Connection) -> None:
    """Rebuild all aggregate and metric tables.

    Repetition score formula: for each stream play, weight = 1 / (1 + days_ago),
    where days_ago is the difference between the most recent end_time and the
    play's end_time in days. The repetition score for a track is the sum of
    these weights across its plays.
    """

    init_metrics_db(connection)
    connection.execute("DELETE FROM track_aggregates")
    connection.execute("DELETE FROM global_metrics")
    connection.execute("DELETE FROM weekly_metrics")
    connection.execute("DELETE FROM temporal_hourly")
    connection.execute("DELETE FROM temporal_weekday")

    connection.execute(
        """
        WITH max_time AS (
            SELECT MAX(end_time) AS max_end_time FROM streams
        ),
        scored AS (
            SELECT
                streams.track_key,
                streams.track_name,
                streams.artist_name,
                streams.ms_played,
                streams.end_time,
                CASE
                    WHEN (SELECT max_end_time FROM max_time) IS NULL THEN 0.0
                    ELSE 1.0 / (
                        1.0
                        + (
                            julianday((SELECT max_end_time FROM max_time))
                            - julianday(streams.end_time)
                        )
                    )
                END AS repetition_weight
            FROM streams
        )
        INSERT INTO track_aggregates (
            track_key,
            track_name,
            artist_name,
            play_count,
            total_ms_played,
            avg_ms_played,
            plays_over_60s,
            persistence_proxy,
            repetition_score
        )
        SELECT
            track_key,
            track_name,
            artist_name,
            COUNT(*) AS play_count,
            SUM(ms_played) AS total_ms_played,
            AVG(ms_played) AS avg_ms_played,
            SUM(CASE WHEN ms_played >= 60000 THEN 1 ELSE 0 END) AS plays_over_60s,
            CASE
                WHEN COUNT(*) = 0 THEN 0.0
                ELSE CAST(
                    SUM(CASE WHEN ms_played >= 60000 THEN 1 ELSE 0 END) AS REAL
                ) / COUNT(*)
            END AS persistence_proxy,
            SUM(repetition_weight) AS repetition_score
        FROM scored
        GROUP BY track_key, track_name, artist_name
        """
    )

    total_plays = connection.execute("SELECT COUNT(*) FROM streams").fetchone()[0]
    unique_tracks = connection.execute(
        "SELECT COUNT(DISTINCT track_key) FROM streams"
    ).fetchone()[0]
    rotation = (unique_tracks / total_plays) if total_plays else 0.0

    connection.executemany(
        "INSERT INTO global_metrics (metric_name, metric_value) VALUES (?, ?)",
        [
            ("total_plays", float(total_plays)),
            ("unique_tracks", float(unique_tracks)),
            ("rotation", float(rotation)),
        ],
    )

    connection.execute(
        """
        INSERT INTO weekly_metrics (week_key, total_plays, unique_tracks, rotation)
        SELECT
            strftime('%Y-%W', end_time) AS week_key,
            COUNT(*) AS total_plays,
            COUNT(DISTINCT track_key) AS unique_tracks,
            CASE
                WHEN COUNT(*) = 0 THEN 0.0
                ELSE CAST(COUNT(DISTINCT track_key) AS REAL) / COUNT(*)
            END AS rotation
        FROM streams
        GROUP BY week_key
        """
    )

    connection.execute(
        """
        INSERT INTO temporal_hourly (hour, play_count)
        SELECT
            CAST(strftime('%H', end_time) AS INTEGER) AS hour,
            COUNT(*) AS play_count
        FROM streams
        GROUP BY hour
        """
    )
    connection.execute(
        """
        INSERT INTO temporal_weekday (day_of_week, play_count)
        SELECT
            CAST(strftime('%w', end_time) AS INTEGER) AS day_of_week,
            COUNT(*) AS play_count
        FROM streams
        GROUP BY day_of_week
        """
    )
    connection.commit()
