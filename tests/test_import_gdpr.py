from __future__ import annotations

from pathlib import Path
import sqlite3
import zipfile

from spotifygpt.cli import main
from spotifygpt.importer import import_gdpr, init_db


SAMPLE_GDPR_DIR = Path(__file__).resolve().parents[1] / "data" / "sample_gdpr"


def test_import_gdpr_from_directory(tmp_path: Path) -> None:
    db_path = tmp_path / "gdpr.db"

    with sqlite3.connect(db_path) as connection:
        init_db(connection)
        result = import_gdpr(connection, SAMPLE_GDPR_DIR)
        events = connection.execute("SELECT COUNT(*) FROM listening_events").fetchone()[0]
        mode = connection.execute("SELECT mode FROM ingest_runs WHERE id = ?", (result.run_id,)).fetchone()[0]

    assert len(result.files) == 1
    assert result.rows_seen == 4
    assert result.rows_inserted == 2
    assert events == 2
    assert mode == "DEEP"


def test_import_gdpr_from_zip(tmp_path: Path) -> None:
    zip_path = tmp_path / "gdpr.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.write(SAMPLE_GDPR_DIR / "endsong_0.json", arcname="Spotify/endsong_0.json")

    db_path = tmp_path / "zip.db"
    with sqlite3.connect(db_path) as connection:
        init_db(connection)
        result = import_gdpr(connection, zip_path)

    assert len(result.files) == 1
    assert result.rows_inserted == 2


def test_cli_import_gdpr(tmp_path: Path) -> None:
    db_path = tmp_path / "cli.db"

    code = main(["import-gdpr", str(SAMPLE_GDPR_DIR), str(db_path)])

    with sqlite3.connect(db_path) as connection:
        count = connection.execute("SELECT COUNT(*) FROM listening_events").fetchone()[0]

    assert code == 0
    assert count == 2
