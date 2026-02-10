from __future__ import annotations

from pathlib import Path
import sqlite3

from spotifygpt.cli import main


SAMPLE_DIR = Path(__file__).resolve().parents[1] / "data" / "sample"


def test_end_to_end_cli_pipeline(tmp_path: Path) -> None:
    db_path = tmp_path / "streams.db"

    assert main(["import", str(SAMPLE_DIR), str(db_path)]) == 0
    assert main(["metrics", str(db_path)]) == 0
    assert main(["classify", str(db_path)]) == 0
    assert main(["weekly-radar", str(db_path)]) == 0
    assert main(["daily-mode", str(db_path)]) == 0
    assert main(["alerts", str(db_path)]) == 0

    with sqlite3.connect(db_path) as connection:
        metrics_count = connection.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
        classifications_count = connection.execute(
            "SELECT COUNT(*) FROM classifications"
        ).fetchone()[0]
        weekly_count = connection.execute(
            "SELECT COUNT(*) FROM weekly_radar"
        ).fetchone()[0]
        daily_count = connection.execute("SELECT COUNT(*) FROM daily_mode").fetchone()[0]
        alert_count = connection.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]

    assert metrics_count == 4
    assert classifications_count == 3
    assert weekly_count == 3
    assert daily_count == 1
    assert alert_count == 0
