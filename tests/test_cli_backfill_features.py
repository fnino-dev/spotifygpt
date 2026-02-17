from __future__ import annotations

from pathlib import Path
import json
import sqlite3

from spotifygpt.audio_features import AudioFeatures, BackfillCandidate
from spotifygpt.cli import main


SAMPLE_DIR = Path(__file__).resolve().parents[1] / "data" / "sample"


class FakeProvider:
    def fetch(self, candidate: BackfillCandidate) -> AudioFeatures | None:
        return AudioFeatures(
            track_key=candidate.track_key,
            danceability=0.6,
            energy=0.4,
            valence=0.2,
            tempo=100.0,
            fetched_at="2026-01-01T00:00:00",
        )


def test_cli_backfill_features(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "streams.db"

    assert main(["import", str(SAMPLE_DIR), str(db_path)]) == 0
    monkeypatch.setattr("spotifygpt.cli._build_audio_feature_provider", lambda _args: FakeProvider())

    assert main(["backfill-features", str(db_path), "--limit", "2"]) == 0

    with sqlite3.connect(db_path) as connection:
        count = connection.execute("SELECT COUNT(*) FROM audio_features").fetchone()[0]

    assert count == 2


def test_cli_backfill_invalid_since(tmp_path: Path) -> None:
    db_path = tmp_path / "streams.db"

    assert main(["import", str(SAMPLE_DIR), str(db_path)]) == 0
    assert main(["backfill-features", str(db_path), "--since", "not-a-date"]) == 1


def test_cli_backfill_features_without_streams_uses_manual_import(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "manual.db"
    liked_path = tmp_path / "liked.json"
    playlists_path = tmp_path / "playlists.json"

    liked_path.write_text(
        json.dumps(
            [
                {
                    "track": {
                        "name": "Manual Track",
                        "artists": [{"name": "Manual Artist"}],
                    }
                }
            ]
        ),
        encoding="utf-8",
    )
    playlists_path.write_text("[]", encoding="utf-8")

    assert (
        main(
            [
                "import-manual",
                "--liked",
                str(liked_path),
                "--playlists",
                str(playlists_path),
                "--db",
                str(db_path),
            ]
        )
        == 0
    )
    monkeypatch.setattr("spotifygpt.cli._build_audio_feature_provider", lambda _args: FakeProvider())

    assert main(["backfill-features", str(db_path), "--limit", "5"]) == 0

    with sqlite3.connect(db_path) as connection:
        count = connection.execute("SELECT COUNT(*) FROM audio_features").fetchone()[0]

    assert count == 1


class _FakeSpotifyResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_cli_backfill_defaults_to_spotify_provider(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "manual.db"
    liked_path = tmp_path / "liked.json"
    playlists_path = tmp_path / "playlists.json"

    liked_path.write_text(
        json.dumps(
            [
                {
                    "track": {
                        "name": "Manual Track",
                        "artists": [{"name": "Manual Artist"}],
                        "spotify_uri": "spotify:track:track-1",
                    }
                }
            ]
        ),
        encoding="utf-8",
    )
    playlists_path.write_text("[]", encoding="utf-8")

    assert main(["import-manual", "--liked", str(liked_path), "--playlists", str(playlists_path), "--db", str(db_path)]) == 0

    def fake_urlopen(request, timeout=20):
        assert request.get_header("Authorization") == "Bearer secret"
        return _FakeSpotifyResponse(
            {
                "audio_features": [
                    {
                        "id": "track-1",
                        "danceability": 0.4,
                        "energy": 0.3,
                        "valence": 0.2,
                        "tempo": 90.0,
                    }
                ]
            }
        )

    monkeypatch.setenv("SPOTIFYGPT_AUDIO_FEATURES_TOKEN", "secret")
    monkeypatch.setattr("spotifygpt.audio_features.urlopen", fake_urlopen)

    assert main(["backfill-features", str(db_path), "--limit", "1"]) == 0


def test_cli_backfill_missing_provider_config(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "streams.db"
    assert main(["import", str(SAMPLE_DIR), str(db_path)]) == 0

    assert main(["backfill-features", str(db_path)]) == 1
    captured = capsys.readouterr()
    assert "Provide --auth-token" in captured.err
