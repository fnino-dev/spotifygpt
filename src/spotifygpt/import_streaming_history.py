"""Import Spotify StreamingHistory JSON files into SQLite."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from spotifygpt.alerts import Alert, detect_alerts

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class Stream:
    track_key: str
    track_name: str
    artist_name: str
    end_time: str
    ms_played: int


def compute_track_key(track_name: str, artist_name: str) -> str:
    """Return a stable hash of the track and artist names."""

    payload = f"{track_name}|{artist_name}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _normalize_row(row: dict) -> Stream | None:
    required_fields = ("trackName", "artistName", "endTime", "msPlayed")
    missing = [field for field in required_fields if field not in row]
    if missing:
        LOGGER.warning("Skipping row missing fields %s", missing)
        return None

    track_name = row["trackName"]
    artist_name = row["artistName"]
    end_time = row["endTime"]
    ms_played = row["msPlayed"]
    track_key = compute_track_key(track_name, artist_name)

    return Stream(
        track_key=track_key,
        track_name=track_name,
        artist_name=artist_name,
        end_time=end_time,
        ms_played=ms_played,
    )


def _load_json_file(path: Path) -> list[Stream]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        LOGGER.error("Skipping malformed JSON file %s: %s", path, exc)
        return []

    if not isinstance(payload, list):
        LOGGER.error("Skipping JSON file %s because it is not a list", path)
        return []

    streams: list[Stream] = []
    for row in payload:
        if not isinstance(row, dict):
            LOGGER.warning("Skipping non-dict row in %s", path)
            continue
        stream = _normalize_row(row)
        if stream is not None:
            streams.append(stream)
    return streams


def load_streams(input_dir: Path) -> list[Stream]:
    files = sorted(input_dir.glob("StreamingHistory*.json"))
    if not files:
        raise FileNotFoundError(f"No StreamingHistory JSON files found in {input_dir}")

    streams: list[Stream] = []
    for path in files:
        streams.extend(_load_json_file(path))
    return streams


def initialize_db(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS streams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            track_key TEXT NOT NULL,
            track_name TEXT NOT NULL,
            artist_name TEXT NOT NULL,
            end_time TEXT NOT NULL,
            ms_played INTEGER NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_type TEXT NOT NULL,
            detected_at TEXT NOT NULL,
            evidence JSON NOT NULL
        )
        """
    )
    connection.commit()


def insert_streams(connection: sqlite3.Connection, streams: Iterable[Stream]) -> None:
    connection.executemany(
        """
        INSERT INTO streams (track_key, track_name, artist_name, end_time, ms_played)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (
                stream.track_key,
                stream.track_name,
                stream.artist_name,
                stream.end_time,
                stream.ms_played,
            )
            for stream in streams
        ],
    )
    connection.commit()


def insert_alerts(connection: sqlite3.Connection, alerts: Iterable[Alert]) -> int:
    rows = [(alert.alert_type, alert.detected_at, alert.serialize_evidence()) for alert in alerts]
    if not rows:
        return 0
    connection.executemany(
        """
        INSERT INTO alerts (alert_type, detected_at, evidence)
        VALUES (?, ?, ?)
        """,
        rows,
    )
    connection.commit()
    return len(rows)


def import_streaming_history(input_dir: Path, db_path: Path) -> int:
    streams = load_streams(input_dir)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as connection:
        initialize_db(connection)
        insert_streams(connection, streams)
        alerts = detect_alerts(streams)
        insert_alerts(connection, alerts)
    return len(streams)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Import Spotify StreamingHistory JSON files into SQLite."
    )
    parser.add_argument("input_dir", type=Path, help="Folder with StreamingHistory files")
    parser.add_argument("db_path", type=Path, help="SQLite database path")
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = build_parser()
    args = parser.parse_args()
    count = import_streaming_history(args.input_dir, args.db_path)
    LOGGER.info("Imported %s stream records", count)


if __name__ == "__main__":
    main()
