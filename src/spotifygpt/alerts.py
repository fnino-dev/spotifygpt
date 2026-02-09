"""Alert detection for Spotify listening patterns."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
from math import log2
from typing import Iterable, Protocol


class StreamLike(Protocol):
    track_key: str
    end_time: str


@dataclass(frozen=True)
class Alert:
    alert_type: str
    detected_at: str
    evidence: dict[str, object]

    def serialize_evidence(self) -> str:
        return json.dumps(self.evidence, ensure_ascii=False, sort_keys=True)


DATE_FORMATS = ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S")
DERIVA_TOP_N = 5
DERIVA_JACCARD_THRESHOLD = 0.3
DERIVA_MIN_PLAYS = 20
BLOCKED_MIN_PLAYS = 20
BLOCKED_TOP_SHARE = 0.4
BLOCKED_UNIQUE_SHARE = 0.2
CAOS_MIN_PLAYS = 20
CAOS_ENTROPY_THRESHOLD = 0.8


def detect_alerts(streams: Iterable[StreamLike]) -> list[Alert]:
    streams_by_week = _group_by_week(streams)
    alerts: list[Alert] = []

    alerts.extend(_detect_deriva(streams_by_week))
    alerts.extend(_detect_bloqueo(streams_by_week))
    alerts.extend(_detect_caos(streams_by_week))

    return alerts


def _group_by_week(streams: Iterable[StreamLike]) -> dict[datetime, list[StreamLike]]:
    grouped: dict[datetime, list[StreamLike]] = defaultdict(list)
    for stream in streams:
        parsed = _parse_end_time(stream.end_time)
        if parsed is None:
            continue
        week_start = parsed.date() - timedelta(days=parsed.weekday())
        grouped[datetime.combine(week_start, datetime.min.time())].append(stream)
    return grouped


def _parse_end_time(value: str) -> datetime | None:
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _detect_deriva(streams_by_week: dict[datetime, list[StreamLike]]) -> list[Alert]:
    alerts: list[Alert] = []
    weeks = sorted(streams_by_week.keys())
    weekly_top_sets: dict[datetime, set[str]] = {}
    weekly_counts: dict[datetime, int] = {}

    for week in weeks:
        counts = Counter(stream.track_key for stream in streams_by_week[week])
        weekly_counts[week] = sum(counts.values())
        weekly_top_sets[week] = {
            track_key for track_key, _ in counts.most_common(DERIVA_TOP_N)
        }

    for previous_week, current_week in zip(weeks, weeks[1:]):
        if (
            weekly_counts.get(previous_week, 0) < DERIVA_MIN_PLAYS
            or weekly_counts.get(current_week, 0) < DERIVA_MIN_PLAYS
        ):
            continue
        previous_set = weekly_top_sets.get(previous_week, set())
        current_set = weekly_top_sets.get(current_week, set())
        if not previous_set or not current_set:
            continue
        intersection = previous_set & current_set
        union = previous_set | current_set
        jaccard = len(intersection) / len(union)
        if jaccard < DERIVA_JACCARD_THRESHOLD:
            alerts.append(
                Alert(
                    alert_type="deriva",
                    detected_at=current_week.date().isoformat(),
                    evidence={
                        "previous_week_start": previous_week.date().isoformat(),
                        "current_week_start": current_week.date().isoformat(),
                        "jaccard_similarity": round(jaccard, 3),
                        "previous_top_tracks": sorted(previous_set),
                        "current_top_tracks": sorted(current_set),
                    },
                )
            )
    return alerts


def _detect_bloqueo(streams_by_week: dict[datetime, list[StreamLike]]) -> list[Alert]:
    alerts: list[Alert] = []
    for week_start, week_streams in streams_by_week.items():
        counts = Counter(stream.track_key for stream in week_streams)
        total = sum(counts.values())
        if total < BLOCKED_MIN_PLAYS or not counts:
            continue
        top_track, top_count = counts.most_common(1)[0]
        unique_share = len(counts) / total
        top_share = top_count / total
        if top_share >= BLOCKED_TOP_SHARE and unique_share <= BLOCKED_UNIQUE_SHARE:
            alerts.append(
                Alert(
                    alert_type="bloqueo",
                    detected_at=week_start.date().isoformat(),
                    evidence={
                        "week_start": week_start.date().isoformat(),
                        "total_plays": total,
                        "unique_tracks": len(counts),
                        "top_track_key": top_track,
                        "top_track_share": round(top_share, 3),
                        "unique_track_share": round(unique_share, 3),
                    },
                )
            )
    return alerts


def _detect_caos(streams_by_week: dict[datetime, list[StreamLike]]) -> list[Alert]:
    alerts: list[Alert] = []
    for week_start, week_streams in streams_by_week.items():
        hours: list[int] = []
        for stream in week_streams:
            parsed = _parse_end_time(stream.end_time)
            if parsed is None:
                continue
            hours.append(parsed.hour)
        if len(hours) < CAOS_MIN_PLAYS:
            continue
        counts = Counter(hours)
        total = sum(counts.values())
        entropy = _entropy(counts.values(), total)
        normalized_entropy = entropy / log2(24)
        if normalized_entropy >= CAOS_ENTROPY_THRESHOLD:
            alerts.append(
                Alert(
                    alert_type="caos",
                    detected_at=week_start.date().isoformat(),
                    evidence={
                        "week_start": week_start.date().isoformat(),
                        "total_plays": total,
                        "entropy": round(entropy, 3),
                        "normalized_entropy": round(normalized_entropy, 3),
                    },
                )
            )
    return alerts


def _entropy(values: Iterable[int], total: int) -> float:
    entropy = 0.0
    for count in values:
        if count == 0:
            continue
        probability = count / total
        entropy -= probability * log2(probability)
    return entropy
