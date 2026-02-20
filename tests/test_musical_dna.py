from __future__ import annotations

import math

from spotifygpt.musical_dna import compute_musical_dna


def test_compute_musical_dna_with_deterministic_fixture() -> None:
    tracks = [
        {
            "danceability": 0.2,
            "energy": 0.3,
            "tempo": 88.0,
            "valence": 0.4,
            "acousticness": 0.9,
            "instrumentalness": 0.1,
            "liveness": 0.2,
            "speechiness": 0.05,
        },
        {
            "danceability": 0.8,
            "energy": 0.7,
            "tempo": 125.0,
            "valence": 0.6,
            "acousticness": 0.1,
            "instrumentalness": 0.2,
            "liveness": 0.4,
            "speechiness": 0.07,
        },
    ]

    profile = compute_musical_dna(tracks).to_dict()

    assert profile["track_count"] == 2
    assert profile["feature_summary"]["energy"]["mean"] == 0.5
    assert profile["feature_summary"]["tempo"]["p50"] == 106.5
    assert profile["tempo_bands"][0]["band"] == "<90"
    assert profile["tempo_bands"][0]["count"] == 1
    assert profile["tempo_bands"][2]["count"] == 1
    assert profile["energy_dance_matrix"]["low"]["low"] == 1
    assert profile["energy_dance_matrix"]["high"]["high"] == 1
    assert 0.0 <= profile["taste_axes"]["chill_to_hype"] <= 1.0


def test_compute_musical_dna_handles_empty_and_invalid_values() -> None:
    profile = compute_musical_dna(
        [
            {},
            {
                "energy": float("nan"),
                "danceability": -5,
                "tempo": -20,
                "valence": 9,
                "acousticness": None,
                "instrumentalness": 5,
                "speechiness": "bad",
            },
            {
                "energy": 0.5,
                "danceability": 0.5,
                "tempo": 100.0,
                "valence": 0.5,
                "acousticness": 0.5,
                "instrumentalness": 0.5,
                "liveness": 0.5,
                "speechiness": 0.5,
            },
        ]
    ).to_dict()

    assert profile["track_count"] == 3
    assert profile["feature_summary"]["tempo"]["min"] == 0.0
    assert profile["feature_summary"]["valence"]["max"] == 1.0
    assert profile["feature_summary"]["danceability"]["p50"] == 0.25
    assert math.isclose(profile["feature_summary"]["energy"]["mean"], 0.5)


def test_compute_musical_dna_single_track_std_is_zero() -> None:
    profile = compute_musical_dna([{"energy": 0.4, "danceability": 0.3, "tempo": 120.0}]).to_dict()
    assert profile["feature_summary"]["tempo"]["std"] == 0.0
    assert profile["feature_summary"]["tempo"]["p10"] == 120.0
    assert profile["feature_summary"]["tempo"]["p90"] == 120.0
