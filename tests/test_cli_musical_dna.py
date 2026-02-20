from __future__ import annotations

import json
from pathlib import Path

from spotifygpt.cli import main


def test_cli_musical_dna_json_and_out(tmp_path: Path, capsys) -> None:
    in_path = tmp_path / "tracks.json"
    out_path = tmp_path / "dna.json"
    in_path.write_text(
        json.dumps(
            [
                {"energy": 0.2, "danceability": 0.1, "tempo": 85.0, "valence": 0.4, "acousticness": 0.9, "instrumentalness": 0.1, "liveness": 0.2, "speechiness": 0.05},
                {"energy": 0.8, "danceability": 0.9, "tempo": 145.0, "valence": 0.6, "acousticness": 0.2, "instrumentalness": 0.2, "liveness": 0.3, "speechiness": 0.06},
            ]
        ),
        encoding="utf-8",
    )

    code = main(["musical-dna", str(in_path), "--out", str(out_path)])
    stdout = capsys.readouterr().out

    assert code == 0
    assert "Musical DNA summary:" in stdout
    assert out_path.exists()
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["track_count"] == 2
    assert payload["feature_summary"]["tempo"]["p50"] == 115.0


def test_cli_musical_dna_ndjson(tmp_path: Path) -> None:
    in_path = tmp_path / "tracks.ndjson"
    in_path.write_text(
        "\n".join(
            [
                json.dumps({"energy": 0.5, "danceability": 0.5, "tempo": 100.0}),
                json.dumps({"energy": 0.6, "danceability": 0.6, "tempo": 120.0}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    code = main(["musical-dna", str(in_path)])
    assert code == 0


def test_cli_musical_dna_invalid_shape(tmp_path: Path, capsys) -> None:
    in_path = tmp_path / "bad.json"
    in_path.write_text(json.dumps({"bad": 1}), encoding="utf-8")

    code = main(["musical-dna", str(in_path)])
    stderr = capsys.readouterr().err

    assert code == 1
    assert "Failed to load tracks" in stderr
