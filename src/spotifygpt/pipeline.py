"""Pipeline steps for SpotifyGPT."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import sqlite3
from typing import Iterable


@dataclass(frozen=True)
class StreamRecord:
    track_name: str
    artist_name: str
    track_key: str
    end_time: datetime
    ms_played: int


@dataclass(frozen=True)
class Metric:
    name: str
    value: str


@dataclass(frozen=True)
class Classification:
    track_key: str
    track_name: str
    artist_name: str
    total_ms: int
    category: str


@dataclass(frozen=True)
class WeeklyRadarEntry:
    week_start: str
    track_key: str
    track_name: str
    artist_name: str
    total_ms: int
    rank: int


@dataclass(frozen=True)
class DailyModeEntry:
    date: str
    total_ms: int
    stream_count: int


@dataclass(frozen=True)
class Alert:
    created_at: str
    level: str
    message: str


def _parse_end_time(value: str) -> datetime:
    return datetime.fromisoformat(value)


def fetch_streams(connection: sqlite3.Connection) -> list[StreamRecord]:
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT track_name, artist_name, track_key, end_time, ms_played
        FROM streams
        """
    ).fetchall()
    streams: list[StreamRecord] = []
    for row in rows:
        streams.append(
            StreamRecord(
                track_name=row["track_name"],
                artist_name=row["artist_name"],
                track_key=row["track_key"],
                end_time=_parse_end_time(row["end_time"]),
                ms_played=int(row["ms_played"]),
            )
        )
    return streams


def init_pipeline_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS metrics (
            name TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS classifications (
            track_key TEXT PRIMARY KEY,
            track_name TEXT NOT NULL,
            artist_name TEXT NOT NULL,
            total_ms INTEGER NOT NULL,
            category TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS weekly_radar (
            week_start TEXT NOT NULL,
            track_key TEXT NOT NULL,
            track_name TEXT NOT NULL,
            artist_name TEXT NOT NULL,
            total_ms INTEGER NOT NULL,
            rank INTEGER NOT NULL,
            PRIMARY KEY (week_start, track_key)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_mode (
            date TEXT PRIMARY KEY,
            total_ms INTEGER NOT NULL,
            stream_count INTEGER NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            level TEXT NOT NULL,
            message TEXT NOT NULL
        )
        """
    )
    connection.commit()


def compute_metrics(connection: sqlite3.Connection) -> list[Metric]:
    connection.row_factory = sqlite3.Row
    row = connection.execute(
        """
        SELECT
            COUNT(*) AS total_streams,
            COALESCE(SUM(ms_played), 0) AS total_ms,
            COUNT(DISTINCT track_key) AS unique_tracks,
            COUNT(DISTINCT artist_name) AS unique_artists
        FROM streams
        """
    ).fetchone()
    metrics = [
        Metric("total_streams", str(row["total_streams"])),
        Metric("total_ms", str(row["total_ms"])),
        Metric("unique_tracks", str(row["unique_tracks"])),
        Metric("unique_artists", str(row["unique_artists"])),
    ]
    connection.executemany(
        """
        INSERT OR REPLACE INTO metrics (name, value)
        VALUES (?, ?)
        """,
        [(metric.name, metric.value) for metric in metrics],
    )
    connection.commit()
    return metrics


def classify_tracks(
    connection: sqlite3.Connection, threshold_ms: int = 200_000
) -> list[Classification]:
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT track_key, track_name, artist_name, SUM(ms_played) AS total_ms
        FROM streams
        GROUP BY track_key, track_name, artist_name
        """
    ).fetchall()
    classifications: list[Classification] = []
    for row in rows:
        total_ms = int(row["total_ms"])
        category = "heavy_rotation" if total_ms >= threshold_ms else "casual"
        classifications.append(
            Classification(
                track_key=row["track_key"],
                track_name=row["track_name"],
                artist_name=row["artist_name"],
                total_ms=total_ms,
                category=category,
            )
        )
    connection.executemany(
        """
        INSERT OR REPLACE INTO classifications
            (track_key, track_name, artist_name, total_ms, category)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (
                entry.track_key,
                entry.track_name,
                entry.artist_name,
                entry.total_ms,
                entry.category,
            )
            for entry in classifications
        ],
    )
    connection.commit()
    return classifications


def _week_start(value: datetime) -> datetime:
    return value - timedelta(days=value.weekday())


def build_weekly_radar(
    connection: sqlite3.Connection, top_n: int = 5
) -> list[WeeklyRadarEntry]:
    streams = fetch_streams(connection)
    totals: dict[tuple[datetime, str], tuple[StreamRecord, int]] = {}
    for stream in streams:
        week = _week_start(stream.end_time).date()
        key = (week, stream.track_key)
        if key not in totals:
            totals[key] = (stream, 0)
        totals[key] = (totals[key][0], totals[key][1] + stream.ms_played)

    if not totals:
        return []

    weeks = {week for (week, _track) in totals.keys()}
    latest_week = max(weeks)
    entries: list[WeeklyRadarEntry] = []
    for (week, _track), (stream, total_ms) in totals.items():
        if week != latest_week:
            continue
        entries.append(
            WeeklyRadarEntry(
                week_start=week.isoformat(),
                track_key=stream.track_key,
                track_name=stream.track_name,
                artist_name=stream.artist_name,
                total_ms=total_ms,
                rank=0,
            )
        )

    entries.sort(key=lambda entry: entry.total_ms, reverse=True)
    ranked: list[WeeklyRadarEntry] = []
    for idx, entry in enumerate(entries[:top_n], start=1):
        ranked.append(
            WeeklyRadarEntry(
                week_start=entry.week_start,
                track_key=entry.track_key,
                track_name=entry.track_name,
                artist_name=entry.artist_name,
                total_ms=entry.total_ms,
                rank=idx,
            )
        )

    connection.execute("DELETE FROM weekly_radar WHERE week_start = ?", (latest_week.isoformat(),))
    connection.executemany(
        """
        INSERT INTO weekly_radar
            (week_start, track_key, track_name, artist_name, total_ms, rank)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                entry.week_start,
                entry.track_key,
                entry.track_name,
                entry.artist_name,
                entry.total_ms,
                entry.rank,
            )
            for entry in ranked
        ],
    )
    connection.commit()
    return ranked


def build_daily_mode(connection: sqlite3.Connection) -> DailyModeEntry | None:
    streams = fetch_streams(connection)
    totals: dict[str, list[int]] = {}
    for stream in streams:
        day = stream.end_time.date().isoformat()
        if day not in totals:
            totals[day] = [0, 0]
        totals[day][0] += stream.ms_played
        totals[day][1] += 1

    if not totals:
        return None

    latest_day = max(totals.keys())
    total_ms, stream_count = totals[latest_day]
    entry = DailyModeEntry(date=latest_day, total_ms=total_ms, stream_count=stream_count)
    connection.execute(
        """
        INSERT OR REPLACE INTO daily_mode (date, total_ms, stream_count)
        VALUES (?, ?, ?)
        """,
        (entry.date, entry.total_ms, entry.stream_count),
    )
    connection.commit()
    return entry


def generate_alerts(connection: sqlite3.Connection) -> list[Alert]:
    connection.row_factory = sqlite3.Row
    row = connection.execute(
        """
        SELECT
            SUM(ms_played) AS total_ms,
            MAX(ms_played) AS max_ms
        FROM streams
        """
    ).fetchone()
    total_ms = int(row["total_ms"] or 0)
    if total_ms == 0:
        return []

    top_row = connection.execute(
        """
        SELECT track_name, artist_name, SUM(ms_played) AS total_ms
        FROM streams
        GROUP BY track_key, track_name, artist_name
        ORDER BY total_ms DESC
        LIMIT 1
        """
    ).fetchone()
    if top_row is None:
        return []

    top_ms = int(top_row["total_ms"])
    share = top_ms / total_ms
    alerts: list[Alert] = []
    if share >= 0.5:
        created_at = datetime.utcnow().isoformat(timespec="seconds")
        message = (
            "Dominant track detected: "
            f"{top_row['track_name']} by {top_row['artist_name']} "
            f"accounted for {share:.0%} of playtime."
        )
        alerts.append(Alert(created_at=created_at, level="info", message=message))

    connection.executemany(
        """
        INSERT INTO alerts (created_at, level, message)
        VALUES (?, ?, ?)
        """,
        [(alert.created_at, alert.level, alert.message) for alert in alerts],
    )
    connection.commit()
    return alerts


def count_streams(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT COUNT(*) FROM streams").fetchone()
    return int(row[0]) if row else 0


def ensure_non_empty_streams(connection: sqlite3.Connection) -> bool:
    return count_streams(connection) > 0
