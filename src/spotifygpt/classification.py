"""Classify tracks based on relative listening metrics."""

from __future__ import annotations

from dataclasses import dataclass
import sqlite3
from typing import Iterable


@dataclass(frozen=True)
class TrackMetrics:
    track_key: str
    play_count: int
    total_ms: int
    average_ms: float


@dataclass(frozen=True)
class TrackClassification:
    track_key: str
    role: str
    energy_bucket: str
    usage_type: str


def compute_track_metrics(connection: sqlite3.Connection) -> list[TrackMetrics]:
    rows = connection.execute(
        """
        SELECT track_key,
               COUNT(*) AS play_count,
               SUM(ms_played) AS total_ms,
               AVG(ms_played) AS average_ms
        FROM streams
        GROUP BY track_key
        """
    ).fetchall()
    return [
        TrackMetrics(
            track_key=row[0],
            play_count=row[1],
            total_ms=row[2],
            average_ms=float(row[3]),
        )
        for row in rows
    ]


def init_classification_table(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS track_classifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            track_key TEXT NOT NULL,
            role TEXT NOT NULL,
            energy_bucket TEXT NOT NULL,
            usage_type TEXT NOT NULL
        )
        """
    )
    connection.commit()


def store_track_classifications(
    connection: sqlite3.Connection,
    classifications: Iterable[TrackClassification],
) -> int:
    rows = [
        (classification.track_key, classification.role, classification.energy_bucket, classification.usage_type)
        for classification in classifications
    ]
    if not rows:
        return 0
    connection.executemany(
        """
        INSERT INTO track_classifications (track_key, role, energy_bucket, usage_type)
        VALUES (?, ?, ?, ?)
        """,
        rows,
    )
    connection.commit()
    return len(rows)


def classify_tracks(metrics: Iterable[TrackMetrics]) -> list[TrackClassification]:
    metrics_list = list(metrics)
    if not metrics_list:
        return []

    play_counts = [metric.play_count for metric in metrics_list]
    totals = [metric.total_ms for metric in metrics_list]
    averages = [metric.average_ms for metric in metrics_list]

    def normalize(values: list[float], value: float) -> float:
        min_value = min(values)
        max_value = max(values)
        if max_value == min_value:
            return 0.5
        return (value - min_value) / (max_value - min_value)

    classifications: list[TrackClassification] = []
    for metric in metrics_list:
        play_norm = normalize(play_counts, metric.play_count)
        total_norm = normalize(totals, metric.total_ms)
        average_norm = normalize(averages, metric.average_ms)

        if play_norm >= 0.67 and total_norm >= 0.67:
            role = "anchor"
        elif play_norm <= 0.33 and total_norm <= 0.33:
            role = "exploration"
        else:
            role = "transition"

        if average_norm < 0.34:
            energy_bucket = "low"
        elif average_norm < 0.67:
            energy_bucket = "medium"
        else:
            energy_bucket = "high"

        if play_norm >= 0.67 and average_norm >= 0.67:
            usage_type = "peak"
        elif play_norm >= 0.67 and average_norm < 0.34:
            usage_type = "background"
        elif play_norm < 0.34 and average_norm < 0.34:
            usage_type = "discharge"
        else:
            usage_type = "focus"

        classifications.append(
            TrackClassification(
                track_key=metric.track_key,
                role=role,
                energy_bucket=energy_bucket,
                usage_type=usage_type,
            )
        )

    return classifications


def classify_tracks_in_db(connection: sqlite3.Connection) -> list[TrackClassification]:
    metrics = compute_track_metrics(connection)
    classifications = classify_tracks(metrics)
    init_classification_table(connection)
    store_track_classifications(connection, classifications)
    return classifications
