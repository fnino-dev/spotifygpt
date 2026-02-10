"""Compute and persist metrics derived from normalized stream records."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True)
class StreamRecord:
    track_key: str
    track_name: str
    artist_name: str
    end_time: datetime
    ms_played: int


def init_metrics_tables(connection) -> None:
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
        CREATE TABLE IF NOT EXISTS metrics_global (
            metric_name TEXT PRIMARY KEY,
            metric_value REAL NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS metrics_weekly (
            week_start TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            metric_value REAL NOT NULL,
            PRIMARY KEY (week_start, metric_name)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS temporal_distributions (
            scope TEXT NOT NULL,
            week_start TEXT,
            bucket_type TEXT NOT NULL,
            bucket INTEGER NOT NULL,
            play_count INTEGER NOT NULL,
            PRIMARY KEY (scope, week_start, bucket_type, bucket)
        )
        """
    )
    connection.commit()


def _parse_end_time(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return datetime.strptime(value, "%Y-%m-%d %H:%M")


def _week_start(date_value: datetime) -> str:
    monday = date_value.date() - timedelta(days=date_value.weekday())
    return monday.isoformat()


def _load_streams(connection) -> list[StreamRecord]:
    rows = connection.execute(
        """
        SELECT track_key, track_name, artist_name, end_time, ms_played
        FROM streams
        """
    ).fetchall()
    records: list[StreamRecord] = []
    for track_key, track_name, artist_name, end_time, ms_played in rows:
        records.append(
            StreamRecord(
                track_key=track_key,
                track_name=track_name,
                artist_name=artist_name,
                end_time=_parse_end_time(end_time),
                ms_played=ms_played,
            )
        )
    return records


def _recency_weighted_repetition_score(records: list[StreamRecord]) -> float:
    """
    Compute recency-weighted repetition score.

    Formula:
        weight = 1 / (1 + days_since_play)
        repetition_score = sum(weight for each play) / unique_tracks
    """

    if not records:
        return 0.0
    reference_time = max(record.end_time for record in records)
    unique_tracks = {record.track_key for record in records}
    if not unique_tracks:
        return 0.0
    weighted_total = 0.0
    for record in records:
        days_since = (reference_time - record.end_time).days
        weighted_total += 1 / (1 + days_since)
    return weighted_total / len(unique_tracks)


def _clear_existing_metrics(connection) -> None:
    for table in (
        "track_aggregates",
        "metrics_global",
        "metrics_weekly",
        "temporal_distributions",
    ):
        connection.execute(f"DELETE FROM {table}")
    connection.commit()


def compute_and_store_metrics(connection) -> None:
    init_metrics_tables(connection)
    records = _load_streams(connection)
    _clear_existing_metrics(connection)

    if not records:
        connection.executemany(
            """
            INSERT INTO metrics_global (metric_name, metric_value)
            VALUES (?, ?)
            """,
            [
                ("rotation", 0.0),
                ("repetition_score", 0.0),
            ],
        )
        connection.commit()
        return

    track_stats: dict[str, dict[str, float]] = {}
    for record in records:
        stats = track_stats.setdefault(
            record.track_key,
            {
                "track_name": record.track_name,
                "artist_name": record.artist_name,
                "play_count": 0,
                "total_ms_played": 0,
                "plays_over_60s": 0,
            },
        )
        stats["play_count"] += 1
        stats["total_ms_played"] += record.ms_played
        if record.ms_played >= 60_000:
            stats["plays_over_60s"] += 1

    track_rows = []
    for track_key, stats in track_stats.items():
        play_count = int(stats["play_count"])
        total_ms = int(stats["total_ms_played"])
        plays_over_60s = int(stats["plays_over_60s"])
        avg_ms = total_ms / play_count if play_count else 0.0
        persistence = plays_over_60s / play_count if play_count else 0.0
        track_rows.append(
            (
                track_key,
                stats["track_name"],
                stats["artist_name"],
                play_count,
                total_ms,
                avg_ms,
                plays_over_60s,
                persistence,
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

    total_plays = len(records)
    unique_tracks = len(track_stats)
    rotation = unique_tracks / total_plays if total_plays else 0.0
    repetition_score = _recency_weighted_repetition_score(records)

    connection.executemany(
        """
        INSERT INTO metrics_global (metric_name, metric_value)
        VALUES (?, ?)
        """,
        [
            ("rotation", rotation),
            ("repetition_score", repetition_score),
        ],
    )

    hour_counts: dict[int, int] = defaultdict(int)
    weekday_counts: dict[int, int] = defaultdict(int)
    for record in records:
        hour_counts[record.end_time.hour] += 1
        weekday_counts[record.end_time.weekday()] += 1

    temporal_rows = [
        ("global", None, "hour", hour, count)
        for hour, count in sorted(hour_counts.items())
    ] + [
        ("global", None, "day_of_week", day, count)
        for day, count in sorted(weekday_counts.items())
    ]

    weekly_groups: dict[str, list[StreamRecord]] = defaultdict(list)
    for record in records:
        weekly_groups[_week_start(record.end_time)].append(record)

    weekly_metric_rows = []
    weekly_temporal_rows = []
    for week_start, week_records in weekly_groups.items():
        week_total = len(week_records)
        week_unique = len({record.track_key for record in week_records})
        week_rotation = week_unique / week_total if week_total else 0.0
        week_repetition = _recency_weighted_repetition_score(week_records)
        weekly_metric_rows.extend(
            [
                (week_start, "rotation", week_rotation),
                (week_start, "repetition_score", week_repetition),
            ]
        )

        week_hour_counts: dict[int, int] = defaultdict(int)
        week_day_counts: dict[int, int] = defaultdict(int)
        for record in week_records:
            week_hour_counts[record.end_time.hour] += 1
            week_day_counts[record.end_time.weekday()] += 1
        weekly_temporal_rows.extend(
            (
                "weekly",
                week_start,
                "hour",
                hour,
                count,
            )
            for hour, count in sorted(week_hour_counts.items())
        )
        weekly_temporal_rows.extend(
            (
                "weekly",
                week_start,
                "day_of_week",
                day,
                count,
            )
            for day, count in sorted(week_day_counts.items())
        )

    if temporal_rows or weekly_temporal_rows:
        connection.executemany(
            """
            INSERT INTO temporal_distributions (
                scope,
                week_start,
                bucket_type,
                bucket,
                play_count
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            temporal_rows + weekly_temporal_rows,
        )

    if weekly_metric_rows:
        connection.executemany(
            """
            INSERT INTO metrics_weekly (week_start, metric_name, metric_value)
            VALUES (?, ?, ?)
            """,
            weekly_metric_rows,
        )

    connection.commit()
