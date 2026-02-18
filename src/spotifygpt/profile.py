"""Deterministic musical DNA profile generation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import sqlite3
from typing import Any

from spotifygpt import __version__

PROFILE_VERSION = "musical_dna_v1"
DEFAULT_OUTPUT_PATH = Path("musical_dna_v1.json")
DEFAULT_REPORT_OUTPUT_PATH = Path("musical_dna_v1_report.md")
FEATURES = (
    "energy",
    "valence",
    "danceability",
    "tempo",
    "loudness",
    "acousticness",
    "instrumentalness",
    "speechiness",
)
DEFAULT_MODE_LABELS = {
    "FreshkitØ": "Activation",
    "Suave_Suave_": "Regulation",
}


@dataclass(frozen=True)
class PlaylistRef:
    playlist_id: int
    name: str
    selector: str


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


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def _std(values: list[float], mean_value: float) -> float:
    if not values:
        return 0.0
    variance = sum((value - mean_value) ** 2 for value in values) / len(values)
    return float(math.sqrt(variance))


def _feature_stats(rows: list[dict[str, float]]) -> dict[str, dict[str, float]]:
    stats: dict[str, dict[str, float]] = {}
    for feature in FEATURES:
        values = [row[feature] for row in rows if feature in row]
        stats[feature] = {
            "mean": _mean(values),
            "p25": _quantile(values, 0.25),
            "p50": _quantile(values, 0.5),
            "p75": _quantile(values, 0.75),
        }
    return stats


def _load_feature_rows(connection: sqlite3.Connection, where_clause: str, params: tuple[Any, ...]) -> list[dict[str, float]]:
    fields = ", ".join(f"af.{feature}" for feature in FEATURES)
    query = f"""
        SELECT {fields}
        FROM ({where_clause}) dataset
        JOIN tracks t ON t.id = dataset.track_id
        JOIN audio_features af ON af.track_key = t.track_key
        ORDER BY t.track_key
    """
    rows = connection.execute(query, params).fetchall()
    payload: list[dict[str, float]] = []
    for row in rows:
        entry: dict[str, float] = {}
        for idx, feature in enumerate(FEATURES):
            value = row[idx]
            if value is None:
                entry[feature] = 0.0
            else:
                entry[feature] = float(value)
        payload.append(entry)
    return payload


def _resolve_playlist(connection: sqlite3.Connection, selector: str) -> PlaylistRef | None:
    by_id = connection.execute(
        "SELECT id, name FROM playlists WHERE CAST(id AS TEXT) = ?",
        (selector,),
    ).fetchone()
    if by_id is not None:
        return PlaylistRef(playlist_id=int(by_id[0]), name=str(by_id[1]), selector=selector)

    by_name = connection.execute(
        "SELECT id, name FROM playlists WHERE LOWER(name) = LOWER(?) ORDER BY id LIMIT 1",
        (selector,),
    ).fetchone()
    if by_name is None:
        return None
    return PlaylistRef(playlist_id=int(by_name[0]), name=str(by_name[1]), selector=selector)


def _deterministic_generated_at(connection: sqlite3.Connection) -> str:
    candidates = [
        "SELECT MAX(added_at) FROM library",
        "SELECT MAX(added_at) FROM playlist_tracks",
        "SELECT MAX(fetched_at) FROM audio_features",
    ]
    best: str | None = None
    for query in candidates:
        value = connection.execute(query).fetchone()[0]
        if isinstance(value, str) and value and (best is None or value > best):
            best = value
    if best is not None:
        return best
    return datetime(1970, 1, 1, tzinfo=timezone.utc).isoformat()


def _profile_from_rows(rows: list[dict[str, float]], label: str, source: str, external_signal: bool = False) -> dict[str, Any]:
    return {
        "label": label,
        "source": source,
        "external_signal": external_signal,
        "track_count": len(rows),
        "feature_stats": _feature_stats(rows),
    }


def _global_z_params(global_rows: list[dict[str, float]]) -> dict[str, tuple[float, float]]:
    params: dict[str, tuple[float, float]] = {}
    for feature in FEATURES:
        values = [row[feature] for row in global_rows]
        mean_value = _mean(values)
        std_value = _std(values, mean_value)
        params[feature] = (mean_value, std_value)
    return params


def _cosine_distance(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0 or nb == 0:
        return 0.0
    return float(1.0 - (dot / (na * nb)))


def _euclidean_distance(a: list[float], b: list[float]) -> float:
    return float(math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b))))


def _comparison(a: dict[str, Any], b: dict[str, Any], z_params: dict[str, tuple[float, float]]) -> dict[str, Any]:
    a_means = [a["feature_stats"][feature]["mean"] for feature in FEATURES]
    b_means = [b["feature_stats"][feature]["mean"] for feature in FEATURES]
    a_z: list[float] = []
    b_z: list[float] = []
    deltas: list[dict[str, float]] = []
    for idx, feature in enumerate(FEATURES):
        mean_value, std_value = z_params[feature]
        if std_value == 0:
            a_val = 0.0
            b_val = 0.0
        else:
            a_val = (a_means[idx] - mean_value) / std_value
            b_val = (b_means[idx] - mean_value) / std_value
        a_z.append(a_val)
        b_z.append(b_val)
        delta = a_means[idx] - b_means[idx]
        deltas.append({"feature": feature, "delta_mean": float(delta), "abs_delta_mean": abs(float(delta))})

    deltas.sort(key=lambda item: item["abs_delta_mean"], reverse=True)
    top_diffs = [{"feature": item["feature"], "delta_mean": item["delta_mean"]} for item in deltas]

    return {
        "left": a["label"],
        "right": b["label"],
        "cosine": _cosine_distance(a_means, b_means),
        "euclidean_z": _euclidean_distance(a_z, b_z),
        "top_differences": top_diffs,
    }


def _pairs(profiles: list[dict[str, Any]]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    out: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for i in range(len(profiles)):
        for j in range(i + 1, len(profiles)):
            out.append((profiles[i], profiles[j]))
    return out


def _load_mode_labels(mode_labels: dict[str, str] | None, mode_labels_file: Path | None) -> dict[str, str]:
    labels = dict(DEFAULT_MODE_LABELS)
    if mode_labels_file is not None:
        raw = json.loads(mode_labels_file.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            for key, value in raw.items():
                if isinstance(key, str) and isinstance(value, str):
                    labels[key] = value
    if mode_labels:
        labels.update(mode_labels)
    return labels


def generate_profile(
    connection: sqlite3.Connection,
    mode_selectors: list[str],
    mode_labels: dict[str, str] | None = None,
    mode_labels_file: Path | None = None,
    include_top_tracks_playlist: str | None = "my_top_tracks_playlist",
    include_radar_playlist: str | None = "radar_de_novedades",
    generated_at: str | None = None,
) -> dict[str, Any]:
    labels = _load_mode_labels(mode_labels, mode_labels_file)

    global_rows = _load_feature_rows(
        connection,
        "SELECT track_id FROM library ORDER BY track_id",
        (),
    )

    mode_profiles: list[dict[str, Any]] = []
    seen_ids: set[int] = set()

    all_selectors = list(mode_selectors)
    if include_top_tracks_playlist:
        all_selectors.append(include_top_tracks_playlist)
    if include_radar_playlist:
        all_selectors.append(include_radar_playlist)

    for selector in all_selectors:
        playlist = _resolve_playlist(connection, selector)
        if playlist is None or playlist.playlist_id in seen_ids:
            continue
        seen_ids.add(playlist.playlist_id)
        rows = _load_feature_rows(
            connection,
            "SELECT track_id FROM playlist_tracks WHERE playlist_id = ? ORDER BY position, track_id",
            (playlist.playlist_id,),
        )
        label = labels.get(selector) or labels.get(playlist.name) or playlist.name
        source = "playlist"
        external_signal = False
        if playlist.name.lower() == "radar_de_novedades" or selector.lower() == "radar_de_novedades":
            source = "radar_de_novedades"
            external_signal = True
        elif playlist.name.lower() == "my_top_tracks_playlist" or selector.lower() == "my_top_tracks_playlist":
            source = "my_top_tracks_playlist"

        mode_profiles.append(_profile_from_rows(rows, label=label, source=source, external_signal=external_signal))

    mode_profiles.sort(key=lambda item: item["label"].lower())
    z_params = _global_z_params(global_rows)
    comparisons = [_comparison(left, right, z_params) for left, right in _pairs(mode_profiles)]

    inputs = {
        "liked_songs": connection.execute("SELECT COUNT(*) FROM library").fetchone()[0],
        "mode_playlists_requested": len(mode_selectors),
        "mode_playlists_resolved": len(mode_profiles),
        "my_top_tracks_playlist": 1 if _resolve_playlist(connection, include_top_tracks_playlist or "") else 0,
        "radar_de_novedades": 1 if _resolve_playlist(connection, include_radar_playlist or "") else 0,
    }

    return {
        "version": PROFILE_VERSION,
        "app_version": __version__,
        "generated_at": generated_at or _deterministic_generated_at(connection),
        "inputs": inputs,
        "global_profile": _profile_from_rows(global_rows, label="global", source="liked_songs"),
        "mode_profiles": mode_profiles,
        "comparisons": comparisons,
    }


def write_profile(profile: dict[str, Any], output_path: Path = DEFAULT_OUTPUT_PATH) -> None:
    output_path.write_text(json.dumps(profile, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _find_activation_regulation_comparison(profile: dict[str, Any]) -> dict[str, Any] | None:
    comparisons = profile.get("comparisons", [])
    for comparison in comparisons:
        left = str(comparison.get("left", "")).lower()
        right = str(comparison.get("right", "")).lower()
        if "activation" in left and "regulation" in right:
            return comparison
        if "activation" in right and "regulation" in left:
            return comparison
    return comparisons[0] if comparisons else None


def _format_stat_row(label: str, stats: dict[str, dict[str, float]]) -> str:
    values = [
        label,
        *(
            f"{stats[feature]['mean']:.4f} / {stats[feature]['p25']:.4f} / "
            f"{stats[feature]['p50']:.4f} / {stats[feature]['p75']:.4f}"
            for feature in FEATURES
        ),
    ]
    return "| " + " | ".join(values) + " |"


def render_profile_report(profile: dict[str, Any]) -> str:
    lines: list[str] = []
    inputs = profile["inputs"]
    global_stats = profile["global_profile"]["feature_stats"]

    lines.extend(
        [
            "# Musical DNA v1 Report",
            "",
            "## Metadata",
            f"- version: `{profile['version']}`",
            f"- app_version: `{profile['app_version']}`",
            f"- generated_at: `{profile['generated_at']}`",
            f"- inputs: liked_songs={inputs['liked_songs']}, mode_playlists_requested={inputs['mode_playlists_requested']}, mode_playlists_resolved={inputs['mode_playlists_resolved']}, my_top_tracks_playlist={inputs['my_top_tracks_playlist']}, radar_de_novedades={inputs['radar_de_novedades']}",
            "",
            "## Global summary",
        ]
    )

    headers = ["scope", *FEATURES]
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    lines.append(_format_stat_row("global", global_stats))
    lines.append("")

    lines.append("## Mode summaries")
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for mode_profile in profile["mode_profiles"]:
        lines.append(_format_stat_row(mode_profile["label"], mode_profile["feature_stats"]))
    lines.append("")

    lines.append("## Comparisons")
    if not profile["comparisons"]:
        lines.append("- No mode comparisons available.")
    else:
        for comparison in profile["comparisons"]:
            lines.append(
                f"- {comparison['left']} vs {comparison['right']}: cosine={comparison['cosine']:.6f}, euclidean_z={comparison['euclidean_z']:.6f}"
            )
    lines.append("")

    lines.append("## Top differences")
    if not profile["comparisons"]:
        lines.append("- No differences available.")
    else:
        for comparison in profile["comparisons"]:
            lines.append(f"### {comparison['left']} vs {comparison['right']}")
            for delta in comparison["top_differences"]:
                lines.append(f"- {delta['feature']}: delta_mean={delta['delta_mean']:.6f}")
    lines.append("")

    lines.append("## Actionable")
    comparison = _find_activation_regulation_comparison(profile)
    if comparison is None:
        lines.append("- Activation vs Regulation interpretation unavailable (need at least two mode profiles).")
    else:
        delta_map = {item["feature"]: item["delta_mean"] for item in comparison["top_differences"]}
        energy_delta = delta_map.get("energy", 0.0)
        tempo_delta = delta_map.get("tempo", 0.0)
        energy_direction = "higher" if energy_delta >= 0 else "lower"
        tempo_direction = "faster" if tempo_delta >= 0 else "slower"
        lines.append(
            "- Activation vs Regulation interpretation: "
            f"`{comparison['left']}` is {energy_direction} energy ({energy_delta:+.4f}) and {tempo_direction} tempo ({tempo_delta:+.4f}) versus `{comparison['right']}`."
        )
        lines.append(
            "- Tempo + energy transition hint: "
            "start in the lower-energy/lower-tempo mode for focus, then ramp toward the higher-energy/higher-tempo mode for activation blocks."
        )

    return "\n".join(lines) + "\n"


def write_profile_report(profile: dict[str, Any], output_path: Path = DEFAULT_REPORT_OUTPUT_PATH) -> None:
    output_path.write_text(render_profile_report(profile), encoding="utf-8")
