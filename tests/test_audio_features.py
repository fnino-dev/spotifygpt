from __future__ import annotations

import sqlite3

from spotifygpt.audio_features import (
    AudioFeatures,
    BackfillCandidate,
    backfill_audio_features,
    init_audio_feature_tables,
)
from spotifygpt.importer import init_db


class FakeProvider:
    def __init__(self) -> None:
        self.calls = 0

    def fetch(self, candidate: BackfillCandidate) -> AudioFeatures | None:
        self.calls += 1
        return AudioFeatures(
            track_key=candidate.track_key,
            danceability=0.5,
            energy=0.7,
            valence=0.3,
            tempo=120.0,
            fetched_at="2026-01-01T00:00:00",
        )


def _seed_streams(connection: sqlite3.Connection) -> None:
    rows = [
        ("Track A", "Artist A", "2026-01-01T10:00:00", 1000, "key-a"),
        ("Track B", "Artist B", "2026-01-03T10:00:00", 1000, "key-b"),
        ("Track C", "Artist C", "2026-01-05T10:00:00", 1000, "key-c"),
    ]
    connection.executemany(
        """
        INSERT INTO streams (track_name, artist_name, end_time, ms_played, track_key)
        VALUES (?, ?, ?, ?, ?)
        """,
        rows,
    )
    connection.commit()


def test_backfill_audio_features_inserts_missing_tracks() -> None:
    connection = sqlite3.connect(":memory:")
    init_db(connection)
    init_audio_feature_tables(connection)
    _seed_streams(connection)

    provider = FakeProvider()
    result = backfill_audio_features(connection, provider=provider)

    assert result.scanned == 3
    assert result.inserted == 3
    assert result.cache_hits == 0
    assert result.api_calls == 3
    assert provider.calls == 3

    stored = connection.execute("SELECT COUNT(*) FROM audio_features").fetchone()[0]
    assert stored == 3


def test_backfill_audio_features_uses_cache_on_second_run() -> None:
    connection = sqlite3.connect(":memory:")
    init_db(connection)
    init_audio_feature_tables(connection)
    _seed_streams(connection)

    provider = FakeProvider()
    first = backfill_audio_features(connection, provider=provider, limit=1)
    connection.execute("DELETE FROM audio_features WHERE track_key = ?", ("key-a",))
    connection.commit()
    second = backfill_audio_features(connection, provider=provider, limit=1)

    assert first.scanned == 1
    assert first.inserted == 1
    assert second.scanned == 1
    assert second.inserted == 1
    assert second.cache_hits == 1
    assert second.api_calls == 0
    assert provider.calls == 1


def test_backfill_audio_features_since_filter() -> None:
    connection = sqlite3.connect(":memory:")
    init_db(connection)
    init_audio_feature_tables(connection)
    _seed_streams(connection)

    provider = FakeProvider()
    result = backfill_audio_features(
        connection,
        provider=provider,
        since="2026-01-04T00:00:00",
    )

    assert result.scanned == 1
    keys = {
        row[0]
        for row in connection.execute("SELECT track_key FROM audio_features").fetchall()
    }
    assert keys == {"key-c"}
