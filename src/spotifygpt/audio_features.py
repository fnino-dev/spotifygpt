"""Audio feature backfill workflow with persistent caching and rate limiting."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import sqlite3
import time
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class AudioFeatures:
    track_key: str
    danceability: float
    energy: float
    valence: float
    tempo: float
    fetched_at: str


@dataclass(frozen=True)
class BackfillCandidate:
    track_key: str
    track_name: str
    artist_name: str


@dataclass(frozen=True)
class BackfillResult:
    scanned: int
    inserted: int
    cache_hits: int
    api_calls: int


class AudioFeatureProvider(Protocol):
    def fetch(self, candidate: BackfillCandidate) -> AudioFeatures | None:
        """Fetch audio features for a candidate track."""


class HttpAudioFeatureProvider:
    """HTTP provider for fetching audio features from a configurable endpoint."""

    def __init__(self, endpoint: str, auth_token: str | None = None):
        self.endpoint = endpoint
        self.auth_token = auth_token

    def fetch(self, candidate: BackfillCandidate) -> AudioFeatures | None:
        params = urlencode(
            {
                "track_name": candidate.track_name,
                "artist_name": candidate.artist_name,
                "track_key": candidate.track_key,
            }
        )
        request = Request(f"{self.endpoint}?{params}")
        if self.auth_token:
            request.add_header("Authorization", f"Bearer {self.auth_token}")

        try:
            with urlopen(request, timeout=20) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, OSError):
            return None

        required = ("danceability", "energy", "valence", "tempo")
        if any(key not in payload for key in required):
            return None

        try:
            danceability = float(payload["danceability"])
            energy = float(payload["energy"])
            valence = float(payload["valence"])
            tempo = float(payload["tempo"])
        except (TypeError, ValueError):
            return None

        return AudioFeatures(
            track_key=candidate.track_key,
            danceability=danceability,
            energy=energy,
            valence=valence,
            tempo=tempo,
            fetched_at=datetime.utcnow().isoformat(timespec="seconds"),
        )


class RateLimitedCachedProvider:
    """Wraps a provider with in-process rate limits and sqlite payload cache."""

    def __init__(
        self,
        connection: sqlite3.Connection,
        provider: AudioFeatureProvider,
        requests_per_second: float,
    ):
        self.connection = connection
        self.provider = provider
        self.min_interval = 0.0 if requests_per_second <= 0 else 1.0 / requests_per_second
        self.last_call_at = 0.0
        self.api_calls = 0
        self.cache_hits = 0

    def fetch(self, candidate: BackfillCandidate) -> AudioFeatures | None:
        cached = self.connection.execute(
            "SELECT payload FROM audio_feature_cache WHERE track_key = ?",
            (candidate.track_key,),
        ).fetchone()
        if cached:
            try:
                payload = json.loads(cached[0])
                self.cache_hits += 1
                return AudioFeatures(
                    track_key=candidate.track_key,
                    danceability=float(payload["danceability"]),
                    energy=float(payload["energy"]),
                    valence=float(payload["valence"]),
                    tempo=float(payload["tempo"]),
                    fetched_at=str(payload["fetched_at"]),
                )
            except (KeyError, ValueError, TypeError, json.JSONDecodeError):
                pass

        now = time.monotonic()
        wait_seconds = self.min_interval - (now - self.last_call_at)
        if wait_seconds > 0:
            time.sleep(wait_seconds)

        self.api_calls += 1
        feature = self.provider.fetch(candidate)
        self.last_call_at = time.monotonic()

        if feature is None:
            return None
        self.connection.execute(
            """
            INSERT OR REPLACE INTO audio_feature_cache (track_key, payload, fetched_at)
            VALUES (?, ?, ?)
            """,
            (
                feature.track_key,
                json.dumps(
                    {
                        "danceability": feature.danceability,
                        "energy": feature.energy,
                        "valence": feature.valence,
                        "tempo": feature.tempo,
                        "fetched_at": feature.fetched_at,
                    }
                ),
                feature.fetched_at,
            ),
        )
        return feature


def init_audio_feature_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS audio_features (
            track_key TEXT PRIMARY KEY,
            danceability REAL NOT NULL,
            energy REAL NOT NULL,
            valence REAL NOT NULL,
            tempo REAL NOT NULL,
            fetched_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS audio_feature_cache (
            track_key TEXT PRIMARY KEY,
            payload TEXT NOT NULL,
            fetched_at TEXT NOT NULL
        )
        """
    )
    connection.commit()


def _build_missing_query(since: str | None) -> tuple[str, tuple[object, ...]]:
    if since is None:
        return (
            """
            SELECT DISTINCT s.track_key, s.track_name, s.artist_name
            FROM streams s
            LEFT JOIN audio_features af ON af.track_key = s.track_key
            WHERE af.track_key IS NULL
            ORDER BY s.track_key
            """,
            (),
        )

    return (
        """
        SELECT DISTINCT s.track_key, s.track_name, s.artist_name
        FROM streams s
        LEFT JOIN audio_features af ON af.track_key = s.track_key
        WHERE af.track_key IS NULL AND s.end_time >= ?
        ORDER BY s.track_key
        """,
        (since,),
    )


def backfill_audio_features(
    connection: sqlite3.Connection,
    provider: AudioFeatureProvider,
    limit: int | None = None,
    since: str | None = None,
    requests_per_second: float = 5.0,
) -> BackfillResult:
    init_audio_feature_tables(connection)

    query, params = _build_missing_query(since)
    if limit is not None:
        query = f"{query}\nLIMIT ?"
        params = (*params, limit)

    rows = connection.execute(query, params).fetchall()
    candidates = [
        BackfillCandidate(track_key=row[0], track_name=row[1], artist_name=row[2])
        for row in rows
    ]

    wrapper = RateLimitedCachedProvider(
        connection=connection,
        provider=provider,
        requests_per_second=requests_per_second,
    )

    inserted = 0
    for candidate in candidates:
        feature = wrapper.fetch(candidate)
        if feature is None:
            continue
        connection.execute(
            """
            INSERT OR REPLACE INTO audio_features
                (track_key, danceability, energy, valence, tempo, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                feature.track_key,
                feature.danceability,
                feature.energy,
                feature.valence,
                feature.tempo,
                feature.fetched_at,
            ),
        )
        inserted += 1

    connection.commit()

    return BackfillResult(
        scanned=len(candidates),
        inserted=inserted,
        cache_hits=wrapper.cache_hits,
        api_calls=wrapper.api_calls,
    )
