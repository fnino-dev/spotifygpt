"""Compute aggregation tables and behavioral metrics from stream data."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable


DATETIME_FORMAT = "%Y-%m-%d %H:%M"


@dataclass
class StreamRow:
    track_name: str
    artist_name: str
    end_time: datetime
    ms_played: int
    track_key: str


def _parse_datetime(value: str) -> datetime:
    return datetime.strptime(value, DATETIME_FORMAT)


def init_metrics_db(connection) -> None:
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
            persistence_proxy REAL NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS global_metrics (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            total_plays INTEGER NOT NULL,
            unique_tracks INTEGER NOT NULL,
            rotation REAL NOT NULL,
            repetition_score REAL NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS weekly_metrics (
            week_start TEXT PRIMARY KEY,
            total_plays INTEGER NOT NULL,
            unique_tracks INTEGER NOT NULL,
            rotation REAL NOT NULL,
            repetition_score REAL NOT NULL
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


def _load_streams(connection) -> list[StreamRow]:
    rows = connection.execute(
        """
        SELECT track_name, artist_name, end_time, ms_played, track_key
        FROM streams
        """
    ).fetchall()
    return [
        StreamRow(
            track_name=row[0],
            artist_name=row[1],
            end_time=_parse_datetime(row[2]),
            ms_played=row[3],
            track_key=row[4],
        )
        for row in rows
    ]


def _recency_weight(reference: datetime, play_time: datetime) -> float:
    days_since = max((reference.date() - play_time.date()).days, 0)
    return 1 / (1 + days_since)


def _compute_repetition_score(reference: datetime, plays: Iterable[datetime]) -> float:
    play_list = list(plays)
    if not play_list:
        return 0.0
    # Recency-weighted repetition score:
    # weight(play) = 1 / (1 + days_since_play), where days_since_play is measured
    # from the most recent play in the scope being evaluated.
    total_weight = sum(_recency_weight(reference, play) for play in play_list)
    return total_weight / len(play_list)


def compute_metrics(connection) -> None:
    init_metrics_db(connection)
    connection.execute("DELETE FROM track_aggregates")
    connection.execute("DELETE FROM global_metrics")
    connection.execute("DELETE FROM weekly_metrics")
    connection.execute("DELETE FROM temporal_hourly")
    connection.execute("DELETE FROM temporal_weekday")

    streams = _load_streams(connection)
    if not streams:
        connection.execute(
            """
            INSERT INTO global_metrics (id, total_plays, unique_tracks, rotation, repetition_score)
            VALUES (1, 0, 0, 0.0, 0.0)
            """
        )
        connection.commit()
        return

    track_stats: dict[str, dict[str, object]] = {}
    hourly_counts: dict[int, int] = defaultdict(int)
    weekday_counts: dict[int, int] = defaultdict(int)
    weekly_plays: dict[str, list[StreamRow]] = defaultdict(list)

    for stream in streams:
        stats = track_stats.setdefault(
            stream.track_key,
            {
                "track_name": stream.track_name,
                "artist_name": stream.artist_name,
                "play_count": 0,
                "total_ms_played": 0,
                "plays_over_60s": 0,
            },
        )
        stats["play_count"] = int(stats["play_count"]) + 1
        stats["total_ms_played"] = int(stats["total_ms_played"]) + stream.ms_played
        if stream.ms_played >= 60000:
            stats["plays_over_60s"] = int(stats["plays_over_60s"]) + 1

        hourly_counts[stream.end_time.hour] += 1
        weekday_counts[stream.end_time.weekday()] += 1

        week_start = (stream.end_time.date() - timedelta(days=stream.end_time.weekday()))
        weekly_plays[week_start.isoformat()].append(stream)

    track_rows = []
    for track_key, stats in track_stats.items():
        play_count = int(stats["play_count"])
        total_ms_played = int(stats["total_ms_played"])
        plays_over_60s = int(stats["plays_over_60s"])
        avg_ms_played = total_ms_played / play_count if play_count else 0.0
        persistence_proxy = plays_over_60s / play_count if play_count else 0.0
        track_rows.append(
            (
                track_key,
                stats["track_name"],
                stats["artist_name"],
                play_count,
                total_ms_played,
                avg_ms_played,
                plays_over_60s,
                persistence_proxy,
            )
        )

    connection.executemany(
        """
        INSERT INTO track_aggregates (
            track_key,
            track_name,
            artist_name,
            play_count,
            total_ms_played,
            avg_ms_played,
            plays_over_60s,
            persistence_proxy
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        track_rows,
    )

    most_recent = max(stream.end_time for stream in streams)
    repetition_score = _compute_repetition_score(
        most_recent, (stream.end_time for stream in streams)
    )
    total_plays = len(streams)
    unique_tracks = len(track_stats)
    rotation = unique_tracks / total_plays if total_plays else 0.0

    connection.execute(
        """
        INSERT INTO global_metrics (id, total_plays, unique_tracks, rotation, repetition_score)
        VALUES (1, ?, ?, ?, ?)
        """,
        (total_plays, unique_tracks, rotation, repetition_score),
    )

    weekly_rows = []
    for week_start, week_streams in weekly_plays.items():
        week_total = len(week_streams)
        week_unique = len({stream.track_key for stream in week_streams})
        week_rotation = week_unique / week_total if week_total else 0.0
        week_latest = max(stream.end_time for stream in week_streams)
        week_repetition = _compute_repetition_score(
            week_latest, (stream.end_time for stream in week_streams)
        )
        weekly_rows.append(
            (week_start, week_total, week_unique, week_rotation, week_repetition)
        )

    connection.executemany(
        """
        INSERT INTO weekly_metrics (
            week_start,
            total_plays,
            unique_tracks,
            rotation,
            repetition_score
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        weekly_rows,
    )

    connection.executemany(
        """
        INSERT INTO temporal_hourly (hour, play_count)
        VALUES (?, ?)
        """,
        sorted(hourly_counts.items()),
    )
    connection.executemany(
        """
        INSERT INTO temporal_weekday (day_of_week, play_count)
        VALUES (?, ?)
        """,
        sorted(weekday_counts.items()),
    )

    connection.commit()
