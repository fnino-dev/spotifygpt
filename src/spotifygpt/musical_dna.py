"""Deterministic Musical DNA aggregation from local track feature payloads."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import math
from pathlib import Path
from typing import Any, Mapping

FEATURE_KEYS: tuple[str, ...] = (
    "danceability",
    "energy",
    "tempo",
    "valence",
    "acousticness",
    "instrumentalness",
    "liveness",
    "speechiness",
)


@dataclass(frozen=True)
class FeatureSummary:
    """Summary statistics for a single numeric audio feature."""

    count: int
    mean: float
    std: float
    min: float
    max: float
    p10: float
    p50: float
    p90: float


@dataclass(frozen=True)
class MusicalDNA:
    """JSON-serializable output for the v1 musical DNA profile."""

    feature_summary: dict[str, FeatureSummary]
    tempo_bands: list[dict[str, float | int | str]]
    energy_dance_matrix: dict[str, dict[str, int]]
    taste_axes: dict[str, float]
    track_count: int

    def to_dict(self) -> dict[str, Any]:
        """Return a deterministic dict payload suitable for JSON serialization."""

        payload = asdict(self)
        return payload


# Fixed ordering for deterministic JSON payloads.
_TEMPO_BANDS: tuple[tuple[str, float, float], ...] = (
    ("<90", float("-inf"), 90.0),
    ("90-110", 90.0, 110.0),
    ("110-130", 110.0, 130.0),
    ("130-150", 130.0, 150.0),
    ("150-170", 150.0, 170.0),
    (">=170", 170.0, float("inf")),
)


def _is_valid_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(float(value))


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(ordered[lower])
    weight = position - lower
    return float(ordered[lower] * (1 - weight) + ordered[upper] * weight)


def _population_std(values: list[float], mean_value: float) -> float:
    if not values:
        return 0.0
    variance = sum((item - mean_value) ** 2 for item in values) / len(values)
    return float(math.sqrt(variance))


def _normalize_feature_value(feature: str, value: float) -> float:
    if feature == "tempo":
        return max(0.0, value)
    return _clamp(value, 0.0, 1.0)


def _summarize_feature(values: list[float]) -> FeatureSummary:
    if not values:
        return FeatureSummary(count=0, mean=0.0, std=0.0, min=0.0, max=0.0, p10=0.0, p50=0.0, p90=0.0)
    mean_value = float(sum(values) / len(values))
    return FeatureSummary(
        count=len(values),
        mean=mean_value,
        std=_population_std(values, mean_value),
        min=float(min(values)),
        max=float(max(values)),
        p10=_quantile(values, 0.10),
        p50=_quantile(values, 0.50),
        p90=_quantile(values, 0.90),
    )


def _tempo_histogram(tempo_values: list[float]) -> list[dict[str, float | int | str]]:
    total = len(tempo_values)
    output: list[dict[str, float | int | str]] = []
    for label, low, high in _TEMPO_BANDS:
        if math.isinf(low):
            count = sum(1 for value in tempo_values if value < high)
        elif math.isinf(high):
            count = sum(1 for value in tempo_values if value >= low)
        else:
            count = sum(1 for value in tempo_values if low <= value < high)
        proportion = (count / total) if total else 0.0
        output.append({"band": label, "count": count, "proportion": proportion})
    return output


def _bucket_3(value: float) -> str:
    if value < 0.33:
        return "low"
    if value < 0.66:
        return "med"
    return "high"


def _energy_dance_matrix(energies: list[float], danceabilities: list[float]) -> dict[str, dict[str, int]]:
    matrix = {
        "low": {"low": 0, "med": 0, "high": 0},
        "med": {"low": 0, "med": 0, "high": 0},
        "high": {"low": 0, "med": 0, "high": 0},
    }
    for energy, danceability in zip(energies, danceabilities):
        matrix[_bucket_3(energy)][_bucket_3(danceability)] += 1
    return matrix


def _taste_axes(feature_summary: dict[str, FeatureSummary]) -> dict[str, float]:
    energy = feature_summary["energy"].mean
    tempo = feature_summary["tempo"].mean
    valence = feature_summary["valence"].mean
    acousticness = feature_summary["acousticness"].mean
    instrumentalness = feature_summary["instrumentalness"].mean
    speechiness = feature_summary["speechiness"].mean

    tempo_norm = _clamp((tempo - 60.0) / 140.0, 0.0, 1.0)
    chill_to_hype = (energy + tempo_norm) / 2.0
    dark_to_happy = valence
    organic_to_synthetic = 1.0 - ((acousticness + instrumentalness) / 2.0)
    denom = speechiness + instrumentalness
    vocal_to_instrumental = speechiness / denom if denom > 0 else 0.5

    return {
        "chill_to_hype": _clamp(chill_to_hype, 0.0, 1.0),
        "dark_to_happy": _clamp(dark_to_happy, 0.0, 1.0),
        "organic_to_synthetic": _clamp(organic_to_synthetic, 0.0, 1.0),
        "vocal_to_instrumental": _clamp(vocal_to_instrumental, 0.0, 1.0),
    }


def compute_musical_dna(tracks: list[Mapping[str, Any]]) -> MusicalDNA:
    """Compute the v1 musical DNA profile from track payloads.

    Invalid values (None/NaN/non-numeric) are ignored per feature.
    Normalized features are clamped to [0, 1]. Tempo is clamped to >=0.
    """

    values_by_feature: dict[str, list[float]] = {feature: [] for feature in FEATURE_KEYS}
    paired_energy_dance: list[tuple[float, float]] = []

    for track in tracks:
        for feature in FEATURE_KEYS:
            raw_value = track.get(feature)
            if not _is_valid_number(raw_value):
                continue
            values_by_feature[feature].append(_normalize_feature_value(feature, float(raw_value)))

        energy_raw = track.get("energy")
        dance_raw = track.get("danceability")
        if _is_valid_number(energy_raw) and _is_valid_number(dance_raw):
            paired_energy_dance.append(
                (
                    _normalize_feature_value("energy", float(energy_raw)),
                    _normalize_feature_value("danceability", float(dance_raw)),
                )
            )

    summary = {feature: _summarize_feature(values_by_feature[feature]) for feature in FEATURE_KEYS}
    tempos = values_by_feature["tempo"]
    energies = [pair[0] for pair in paired_energy_dance]
    dances = [pair[1] for pair in paired_energy_dance]

    return MusicalDNA(
        feature_summary=summary,
        tempo_bands=_tempo_histogram(tempos),
        energy_dance_matrix=_energy_dance_matrix(energies, dances),
        taste_axes=_taste_axes(summary),
        track_count=len(tracks),
    )


def load_tracks_from_json(path: Path) -> list[dict[str, Any]]:
    """Load tracks from JSON or NDJSON local artifact."""

    text = path.read_text(encoding="utf-8")
    stripped = text.strip()
    if not stripped:
        return []

    if path.suffix.lower() == ".ndjson":
        return [json.loads(line) for line in stripped.splitlines() if line.strip()]

    payload = json.loads(stripped)
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict) and isinstance(payload.get("tracks"), list):
        return [item for item in payload["tracks"] if isinstance(item, dict)]
    raise ValueError("Unsupported JSON shape. Expected a list of tracks or {\"tracks\": [...]}.")


def write_musical_dna(profile: MusicalDNA, output_path: Path) -> None:
    """Write full JSON output to disk."""

    output_path.write_text(json.dumps(profile.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
