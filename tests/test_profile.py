from __future__ import annotations

from pathlib import Path
import json
import sqlite3

from spotifygpt.audio_features import init_audio_feature_tables
from spotifygpt.cli import main
from spotifygpt.importer import init_db
from spotifygpt.manual_import import init_manual_import_tables
from spotifygpt.profile import FEATURES, generate_profile, render_profile_report


def _seed_profile_data(connection: sqlite3.Connection) -> None:
    init_db(connection)
    init_manual_import_tables(connection)
    init_audio_feature_tables(connection)

    tracks = [
        (1, "uri:1", "Track 1", "Artist", "k1"),
        (2, "uri:2", "Track 2", "Artist", "k2"),
        (3, "uri:3", "Track 3", "Artist", "k3"),
        (4, "uri:4", "Track 4", "Artist", "k4"),
    ]
    connection.executemany(
        "INSERT INTO tracks (id, spotify_uri, track_name, artist_name, track_key) VALUES (?, ?, ?, ?, ?)",
        tracks,
    )
    connection.executemany(
        "INSERT INTO library (track_id, added_at) VALUES (?, ?)",
        [
            (1, "2026-01-01T00:00:00+00:00"),
            (2, "2026-01-02T00:00:00+00:00"),
            (3, "2026-01-03T00:00:00+00:00"),
            (4, "2026-01-04T00:00:00+00:00"),
        ],
    )
    connection.executemany(
        "INSERT INTO playlists (id, name) VALUES (?, ?)",
        [
            (10, "FreshkitØ"),
            (11, "Suave_Suave_"),
            (12, "my_top_tracks_playlist"),
            (13, "radar_de_novedades"),
        ],
    )
    connection.executemany(
        "INSERT INTO playlist_tracks (playlist_id, track_id, position, added_at) VALUES (?, ?, ?, ?)",
        [
            (10, 1, 1, "2026-01-03T00:00:00+00:00"),
            (10, 2, 2, "2026-01-03T00:00:00+00:00"),
            (11, 3, 1, "2026-01-03T00:00:00+00:00"),
            (11, 4, 2, "2026-01-03T00:00:00+00:00"),
            (12, 1, 1, "2026-01-03T00:00:00+00:00"),
            (13, 4, 1, "2026-01-05T00:00:00+00:00"),
        ],
    )

    features = [
        ("k1", 0.1, 0.2, 0.3, 100.0, -5.0, 0.8, 0.0, 0.05, "2026-01-10T00:00:00+00:00"),
        ("k2", 0.3, 0.4, 0.5, 110.0, -6.0, 0.6, 0.1, 0.04, "2026-01-10T00:00:00+00:00"),
        ("k3", 0.7, 0.8, 0.2, 130.0, -4.0, 0.2, 0.5, 0.06, "2026-01-10T00:00:00+00:00"),
        ("k4", 0.9, 0.6, 0.1, 140.0, -3.0, 0.1, 0.2, 0.07, "2026-01-10T00:00:00+00:00"),
    ]
    connection.executemany(
        """
        INSERT INTO audio_features (
            track_key, danceability, energy, valence, tempo,
            loudness, acousticness, instrumentalness, speechiness, fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        features,
    )
    connection.commit()


def test_generate_profile_stats_and_comparisons() -> None:
    connection = sqlite3.connect(":memory:")
    _seed_profile_data(connection)

    profile = generate_profile(
        connection,
        mode_selectors=["10", "suave_suave_"],
        mode_labels={"10": "Activation"},
    )

    assert profile["version"] == "musical_dna_v1"
    assert profile["app_version"]
    assert profile["generated_at"] == "2026-01-10T00:00:00+00:00"
    assert profile["inputs"]["liked_songs"] == 4
    assert set(profile["global_profile"]["feature_stats"].keys()) == set(FEATURES)

    energy_stats = profile["global_profile"]["feature_stats"]["energy"]
    assert energy_stats["mean"] == 0.5
    assert energy_stats["p50"] == 0.5

    labels = [item["label"] for item in profile["mode_profiles"]]
    assert "Activation" in labels
    assert "Regulation" in labels

    comparisons = profile["comparisons"]
    assert comparisons
    comp = comparisons[0]
    assert "cosine" in comp
    assert "euclidean_z" in comp
    assert comp["top_differences"]
    abs_diffs = [abs(item["delta_mean"]) for item in comp["top_differences"]]
    assert abs_diffs == sorted(abs_diffs, reverse=True)


def test_cli_profile_writes_json_and_schema_keys(tmp_path: Path) -> None:
    db_path = tmp_path / "profile.db"
    out_path = tmp_path / "musical_dna_v1.json"
    with sqlite3.connect(db_path) as connection:
        _seed_profile_data(connection)

    mapping_path = tmp_path / "labels.json"
    mapping_path.write_text(json.dumps({"Suave_Suave_": "RegulationX"}), encoding="utf-8")

    code = main(
        [
            "profile",
            str(db_path),
            "--output",
            str(out_path),
            "--mode-playlist",
            "10",
            "--mode-playlist",
            "Suave_Suave_",
            "--mode-label",
            "FreshkitØ=ActivationX",
            "--mode-labels-file",
            str(mapping_path),
        ]
    )
    assert code == 0

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert set(payload.keys()) == {
        "version",
        "app_version",
        "generated_at",
        "inputs",
        "global_profile",
        "mode_profiles",
        "comparisons",
    }
    assert payload["version"] == "musical_dna_v1"
    assert payload["mode_profiles"][0]["label"] in {"ActivationX", "RegulationX"}


def test_render_profile_report_contains_required_sections() -> None:
    connection = sqlite3.connect(":memory:")
    _seed_profile_data(connection)

    profile = generate_profile(
        connection,
        mode_selectors=["10", "suave_suave_"],
        mode_labels={"10": "Activation", "suave_suave_": "Regulation"},
    )
    rendered = render_profile_report(profile)

    assert "## Metadata" in rendered
    assert "## Global summary" in rendered
    assert "## Mode summaries" in rendered
    assert "## Comparisons" in rendered
    assert "cosine=" in rendered
    assert "euclidean_z=" in rendered
    assert "## Top differences" in rendered
    assert "delta_mean=" in rendered
    assert "## Actionable" in rendered
    assert "Activation vs Regulation interpretation" in rendered
    assert "Tempo + energy transition hint" in rendered


def test_cli_profile_report_writes_deterministic_markdown(tmp_path: Path) -> None:
    db_path = tmp_path / "profile.db"
    out_path = tmp_path / "musical_dna_v1_report.md"
    with sqlite3.connect(db_path) as connection:
        _seed_profile_data(connection)

    code = main(
        [
            "profile-report",
            str(db_path),
            "--output",
            str(out_path),
            "--mode-playlist",
            "10",
            "--mode-playlist",
            "Suave_Suave_",
            "--mode-label",
            "FreshkitØ=Activation",
            "--mode-label",
            "Suave_Suave_=Regulation",
        ]
    )
    assert code == 0

    content = out_path.read_text(encoding="utf-8")
    assert content.startswith("# Musical DNA v1 Report\n")
    assert "generated_at: `2026-01-10T00:00:00+00:00`" in content
    assert "## Global summary" in content
    assert "## Mode summaries" in content
