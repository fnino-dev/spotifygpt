"""Audio feature backfill workflow with persistent caching and rate limiting."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import sqlite3
import time
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from spotifygpt.manual_import import init_manual_import_tables


@dataclass(frozen=True)
class AudioFeatures:
    track_key: str
    danceability: float
    energy: float
    valence: float
    tempo: float
    fetched_at: str
    loudness: float = 0.0
    acousticness: float = 0.0
    instrumentalness: float = 0.0
    speechiness: float = 0.0


@dataclass(frozen=True)
class BackfillCandidate:
    track_key: str
    track_name: str
    artist_name: str
    spotify_id: str | None = None


@dataclass(frozen=True)
class BackfillResult:
    scanned: int
    inserted: int
    cache_hits: int
    api_calls: int


class AudioFeatureProvider(Protocol):
    def fetch(self, candidate: BackfillCandidate) -> AudioFeatures | None:
        """Fetch audio features for a candidate track."""


class BatchAudioFeatureProvider(Protocol):
    def fetch_many(self, candidates: list[BackfillCandidate]) -> dict[str, AudioFeatures]:
        """Fetch audio features for multiple candidates keyed by track_key."""


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
            loudness = float(payload.get("loudness", 0.0))
            acousticness = float(payload.get("acousticness", 0.0))
            instrumentalness = float(payload.get("instrumentalness", 0.0))
            speechiness = float(payload.get("speechiness", 0.0))
        except (TypeError, ValueError):
            return None

        return AudioFeatures(
            track_key=candidate.track_key,
            danceability=danceability,
            energy=energy,
            valence=valence,
            tempo=tempo,
            loudness=loudness,
            acousticness=acousticness,
            instrumentalness=instrumentalness,
            speechiness=speechiness,
            fetched_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        )


class SpotifyWebApiAudioFeatureProvider:
    """Direct Spotify Web API provider for batched /audio-features requests."""

    endpoint = "https://api.spotify.com/v1/audio-features"

    def __init__(self, auth_token: str):
        self.auth_token = auth_token

    def _build_payload_features(
        self, candidate: BackfillCandidate, payload: dict[str, object]
    ) -> AudioFeatures | None:
        required = ("danceability", "energy", "valence", "tempo")
        if any(key not in payload for key in required):
            return None
        try:
            return AudioFeatures(
                track_key=candidate.track_key,
                danceability=float(payload["danceability"]),
                energy=float(payload["energy"]),
                valence=float(payload["valence"]),
                tempo=float(payload["tempo"]),
                loudness=float(payload.get("loudness", 0.0)),
                acousticness=float(payload.get("acousticness", 0.0)),
                instrumentalness=float(payload.get("instrumentalness", 0.0)),
                speechiness=float(payload.get("speechiness", 0.0)),
                fetched_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            )
        except (TypeError, ValueError):
            return None

    def fetch_many(self, candidates: list[BackfillCandidate]) -> dict[str, AudioFeatures]:
        id_to_candidate: dict[str, BackfillCandidate] = {
            c.spotify_id: c for c in candidates if c.spotify_id
        }
        if not id_to_candidate:
            return {}

        request = Request(
            f"{self.endpoint}?{urlencode({'ids': ','.join(id_to_candidate.keys())})}"
        )
        request.add_header("Authorization", f"Bearer {self.auth_token}")
        try:
            with urlopen(request, timeout=20) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, OSError):
            return {}

        features_payload = payload.get("audio_features")
        if not isinstance(features_payload, list):
            return {}

        features: dict[str, AudioFeatures] = {}
        for row in features_payload:
            if row is None or not isinstance(row, dict):
                continue
            spotify_id = row.get("id")
            if not isinstance(spotify_id, str):
                continue
            candidate = id_to_candidate.get(spotify_id)
            if candidate is None:
                continue
            feature = self._build_payload_features(candidate, row)
            if feature is not None:
                features[candidate.track_key] = feature
        return features

    def fetch(self, candidate: BackfillCandidate) -> AudioFeatures | None:
        return self.fetch_many([candidate]).get(candidate.track_key)


def _spotify_id_from_uri(raw_uri: str | None) -> str | None:
    if raw_uri is None:
        return None
    uri = raw_uri.strip()
    if not uri:
        return None
    if uri.startswith("spotify:track:"):
        value = uri.split(":")[-1]
        return value or None
    return uri


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
                    loudness=float(payload.get("loudness", 0.0)),
                    acousticness=float(payload.get("acousticness", 0.0)),
                    instrumentalness=float(payload.get("instrumentalness", 0.0)),
                    speechiness=float(payload.get("speechiness", 0.0)),
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
                        "loudness": feature.loudness,
                        "acousticness": feature.acousticness,
                        "instrumentalness": feature.instrumentalness,
                        "speechiness": feature.speechiness,
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
            loudness REAL,
            acousticness REAL,
            instrumentalness REAL,
            speechiness REAL,
            fetched_at TEXT NOT NULL
        )
        """
    )
    existing_columns = {
        row[1] for row in connection.execute("PRAGMA table_info(audio_features)").fetchall()
    }
    for column in ("loudness", "acousticness", "instrumentalness", "speechiness"):
        if column not in existing_columns:
            connection.execute(f"ALTER TABLE audio_features ADD COLUMN {column} REAL")
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
    stream_filter = ""
    params: tuple[object, ...] = ()
    if since is not None:
        stream_filter = "WHERE s.end_time >= ?"
        params = (since,)

    return (
        f"""
        WITH source_candidates AS (
            SELECT DISTINCT t.track_key, t.track_name, t.artist_name, t.spotify_uri, 1 AS priority
            FROM playlist_tracks pt
            JOIN tracks t ON t.id = pt.track_id

            UNION ALL

            SELECT DISTINCT t.track_key, t.track_name, t.artist_name, t.spotify_uri, 2 AS priority
            FROM library l
            JOIN tracks t ON t.id = l.track_id

            UNION ALL

            SELECT DISTINCT s.track_key, s.track_name, s.artist_name, NULL AS spotify_uri, 3 AS priority
            FROM streams s
            {stream_filter}
        ),
        deduped_candidates AS (
            SELECT
                track_key,
                MIN(priority) AS priority,
                MIN(track_name) AS track_name,
                MIN(artist_name) AS artist_name,
                MIN(spotify_uri) AS spotify_uri
            FROM source_candidates
            GROUP BY track_key
        )
        SELECT dc.track_key, dc.track_name, dc.artist_name, dc.spotify_uri
        FROM deduped_candidates dc
        LEFT JOIN audio_features af ON af.track_key = dc.track_key
        WHERE af.track_key IS NULL
        ORDER BY dc.priority, dc.track_key
        """,
        params,
    )


def _insert_audio_feature(connection: sqlite3.Connection, feature: AudioFeatures) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO audio_features
            (
                track_key,
                danceability,
                energy,
                valence,
                tempo,
                loudness,
                acousticness,
                instrumentalness,
                speechiness,
                fetched_at
            )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            feature.track_key,
            feature.danceability,
            feature.energy,
            feature.valence,
            feature.tempo,
            feature.loudness,
            feature.acousticness,
            feature.instrumentalness,
            feature.speechiness,
            feature.fetched_at,
        ),
    )


def backfill_audio_features(
    connection: sqlite3.Connection,
    provider: AudioFeatureProvider,
    limit: int | None = None,
    since: str | None = None,
    requests_per_second: float = 5.0,
) -> BackfillResult:
    init_audio_feature_tables(connection)
    init_manual_import_tables(connection)

    query, params = _build_missing_query(since)
    if limit is not None:
        query = f"{query}\nLIMIT ?"
        params = (*params, limit)

    rows = connection.execute(query, params).fetchall()
    candidates = [
        BackfillCandidate(
            track_key=row[0],
            track_name=row[1],
            artist_name=row[2],
            spotify_id=_spotify_id_from_uri(row[3]),
        )
        for row in rows
    ]

    if isinstance(provider, SpotifyWebApiAudioFeatureProvider):
        min_interval = 0.0 if requests_per_second <= 0 else 1.0 / requests_per_second
        api_calls = 0
        cache_hits = 0
        inserted = 0
        pending: list[BackfillCandidate] = []
        last_call_at = 0.0

        for candidate in candidates:
            cached = connection.execute(
                "SELECT payload FROM audio_feature_cache WHERE track_key = ?",
                (candidate.track_key,),
            ).fetchone()
            if cached:
                try:
                    payload = json.loads(cached[0])
                    feature = AudioFeatures(
                        track_key=candidate.track_key,
                        danceability=float(payload["danceability"]),
                        energy=float(payload["energy"]),
                        valence=float(payload["valence"]),
                        tempo=float(payload["tempo"]),
                        loudness=float(payload.get("loudness", 0.0)),
                        acousticness=float(payload.get("acousticness", 0.0)),
                        instrumentalness=float(payload.get("instrumentalness", 0.0)),
                        speechiness=float(payload.get("speechiness", 0.0)),
                        fetched_at=str(payload["fetched_at"]),
                    )
                    _insert_audio_feature(connection, feature)
                    cache_hits += 1
                    inserted += 1
                    continue
                except (KeyError, ValueError, TypeError, json.JSONDecodeError):
                    pass
            pending.append(candidate)

        for index in range(0, len(pending), 100):
            batch = pending[index : index + 100]
            now = time.monotonic()
            wait_seconds = min_interval - (now - last_call_at)
            if wait_seconds > 0:
                time.sleep(wait_seconds)

            features = provider.fetch_many(batch)
            api_calls += 1
            last_call_at = time.monotonic()

            for feature in features.values():
                connection.execute(
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
                                "loudness": feature.loudness,
                                "acousticness": feature.acousticness,
                                "instrumentalness": feature.instrumentalness,
                                "speechiness": feature.speechiness,
                                "fetched_at": feature.fetched_at,
                            }
                        ),
                        feature.fetched_at,
                    ),
                )
                _insert_audio_feature(connection, feature)
                inserted += 1

        connection.commit()
        return BackfillResult(
            scanned=len(candidates),
            inserted=inserted,
            cache_hits=cache_hits,
            api_calls=api_calls,
        )

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
        _insert_audio_feature(connection, feature)
        inserted += 1

    connection.commit()

    return BackfillResult(
        scanned=len(candidates),
        inserted=inserted,
        cache_hits=wrapper.cache_hits,
        api_calls=wrapper.api_calls,
    )
