"""Import streaming history data into normalized records."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
from datetime import datetime, timezone
from pathlib import Path
import zipfile
from typing import Iterable

from spotifygpt.alerts import Alert


@dataclass(frozen=True)
class Stream:
    track_name: str
    artist_name: str
    end_time: str
    ms_played: int
    track_key: str


@dataclass(frozen=True)
class ImportError:
    file: Path
    message: str
    record_index: int | None = None


@dataclass(frozen=True)
class ImportResult:
    streams: list[Stream]
    errors: list[ImportError]
    files: list[Path]


@dataclass(frozen=True)
class ListeningEvent:
    event_ts: str
    track_name: str
    artist_name: str
    ms_played: int
    track_key: str
    dedup_key: str


@dataclass(frozen=True)
class DeepImportResult:
    files: list[str]
    rows_seen: int
    rows_inserted: int
    run_id: int


def compute_track_key(track_name: str, artist_name: str) -> str:
    raw_key = f"{track_name}|{artist_name}".encode("utf-8")
    return sha256(raw_key).hexdigest()


def _parse_entry(entry: dict[str, object], file: Path, index: int) -> Stream | None:
    missing_fields = [
        field
        for field in ("trackName", "artistName", "endTime", "msPlayed")
        if field not in entry
    ]
    if missing_fields:
        return None

    track_name = entry["trackName"]
    artist_name = entry["artistName"]
    end_time = entry["endTime"]
    ms_played = entry["msPlayed"]

    if not isinstance(track_name, str) or not isinstance(artist_name, str):
        return None
    if not isinstance(end_time, str) or not isinstance(ms_played, int):
        return None

    return Stream(
        track_name=track_name,
        artist_name=artist_name,
        end_time=end_time,
        ms_played=ms_played,
        track_key=compute_track_key(track_name, artist_name),
    )


def discover_streaming_history_files(folder: Path) -> list[Path]:
    return sorted(folder.glob("StreamingHistory*.json"))


def load_streaming_history(folder: Path | str) -> ImportResult:
    folder_path = Path(folder)
    files = discover_streaming_history_files(folder_path)
    streams: list[Stream] = []
    errors: list[ImportError] = []

    for file in files:
        try:
            content = file.read_text(encoding="utf-8")
            data = json.loads(content)
        except json.JSONDecodeError as exc:
            errors.append(ImportError(file=file, message=f"Malformed JSON: {exc}"))
            continue
        except OSError as exc:
            errors.append(ImportError(file=file, message=f"Read error: {exc}"))
            continue

        if not isinstance(data, list):
            errors.append(ImportError(file=file, message="Expected JSON array"))
            continue

        for index, entry in enumerate(data):
            if not isinstance(entry, dict):
                errors.append(
                    ImportError(
                        file=file,
                        message="Expected JSON object",
                        record_index=index,
                    )
                )
                continue
            stream = _parse_entry(entry, file, index)
            if stream is None:
                errors.append(
                    ImportError(
                        file=file,
                        message="Missing or invalid fields",
                        record_index=index,
                    )
                )
                continue
            streams.append(stream)

    return ImportResult(streams=streams, errors=errors, files=files)


def init_db(connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS streams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            track_name TEXT NOT NULL,
            artist_name TEXT NOT NULL,
            end_time TEXT NOT NULL,
            ms_played INTEGER NOT NULL,
            track_key TEXT NOT NULL
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
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS listening_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_ts TEXT NOT NULL,
            track_name TEXT NOT NULL,
            artist_name TEXT NOT NULL,
            ms_played INTEGER NOT NULL,
            track_key TEXT NOT NULL,
            dedup_key TEXT NOT NULL UNIQUE
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mode TEXT NOT NULL,
            source TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            files_count INTEGER NOT NULL DEFAULT 0,
            rows_seen INTEGER NOT NULL DEFAULT 0,
            rows_inserted INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    connection.commit()


def _compute_dedup_key(row: dict[str, object]) -> str:
    payload = "|".join(
        [
            str(row.get("ts", "")),
            str(row.get("master_metadata_track_name", "")),
            str(row.get("master_metadata_album_artist_name", "")),
            str(row.get("ms_played", "")),
            str(row.get("spotify_track_uri", "")),
            str(row.get("platform", "")),
            str(row.get("reason_end", "")),
            str(row.get("conn_country", "")),
        ]
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def _parse_deep_row(row: dict[str, object]) -> ListeningEvent | None:
    ts = row.get("ts")
    track_name = row.get("master_metadata_track_name")
    artist_name = row.get("master_metadata_album_artist_name")
    ms_played = row.get("ms_played")

    if not isinstance(ts, str):
        return None
    if not isinstance(track_name, str) or not track_name.strip():
        return None
    if not isinstance(artist_name, str) or not artist_name.strip():
        return None
    if not isinstance(ms_played, int):
        return None

    return ListeningEvent(
        event_ts=ts,
        track_name=track_name,
        artist_name=artist_name,
        ms_played=ms_played,
        track_key=compute_track_key(track_name, artist_name),
        dedup_key=_compute_dedup_key(row),
    )


def _load_deep_json_bytes(payload: bytes) -> list[dict[str, object]]:
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return []
    if not isinstance(parsed, list):
        return []
    return [row for row in parsed if isinstance(row, dict)]


def _discover_deep_files_dir(input_path: Path) -> list[Path]:
    return sorted(input_path.rglob("endsong*.json"))


def _load_deep_rows(input_path: Path) -> tuple[list[str], list[ListeningEvent], int]:
    files: list[str] = []
    events: list[ListeningEvent] = []
    rows_seen = 0

    if input_path.is_file() and input_path.suffix.lower() == ".zip":
        with zipfile.ZipFile(input_path) as archive:
            names = sorted(
                [name for name in archive.namelist() if Path(name).name.startswith("endsong") and name.endswith(".json")]
            )
            for name in names:
                files.append(name)
                rows = _load_deep_json_bytes(archive.read(name))
                rows_seen += len(rows)
                for row in rows:
                    event = _parse_deep_row(row)
                    if event is not None:
                        events.append(event)
        return files, events, rows_seen

    for file in _discover_deep_files_dir(input_path):
        files.append(str(file))
        rows = _load_deep_json_bytes(file.read_bytes())
        rows_seen += len(rows)
        for row in rows:
            event = _parse_deep_row(row)
            if event is not None:
                events.append(event)

    return files, events, rows_seen


def import_gdpr(connection, input_path: Path | str) -> DeepImportResult:
    source = str(Path(input_path))
    started_at = datetime.now(timezone.utc).isoformat()
    cursor = connection.execute(
        """
        INSERT INTO ingest_runs (mode, source, started_at)
        VALUES (?, ?, ?)
        """,
        ("DEEP", source, started_at),
    )
    run_id = int(cursor.lastrowid)

    files, events, rows_seen = _load_deep_rows(Path(input_path))
    rows = [
        (e.event_ts, e.track_name, e.artist_name, e.ms_played, e.track_key, e.dedup_key)
        for e in events
    ]
    inserted = 0
    if rows:
        before = connection.total_changes
        connection.executemany(
            """
            INSERT OR IGNORE INTO listening_events (
                event_ts, track_name, artist_name, ms_played, track_key, dedup_key
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        inserted = connection.total_changes - before

    finished_at = datetime.now(timezone.utc).isoformat()
    connection.execute(
        """
        UPDATE ingest_runs
        SET finished_at = ?, files_count = ?, rows_seen = ?, rows_inserted = ?
        WHERE id = ?
        """,
        (finished_at, len(files), rows_seen, inserted, run_id),
    )
    connection.commit()
    return DeepImportResult(
        files=files,
        rows_seen=rows_seen,
        rows_inserted=inserted,
        run_id=run_id,
    )


def store_streams(connection, streams: Iterable[Stream]) -> int:
    rows = [
        (
            stream.track_name,
            stream.artist_name,
            stream.end_time,
            stream.ms_played,
            stream.track_key,
        )
        for stream in streams
    ]
    if not rows:
        return 0
    connection.executemany(
        """
        INSERT INTO streams (track_name, artist_name, end_time, ms_played, track_key)
        VALUES (?, ?, ?, ?, ?)
        """,
        rows,
    )
    connection.commit()
    return len(rows)


def store_alerts(connection, alerts: Iterable[Alert]) -> int:
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
