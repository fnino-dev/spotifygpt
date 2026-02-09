"""Import streaming history data into normalized records."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
from pathlib import Path
from typing import Iterable


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
    connection.commit()


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
