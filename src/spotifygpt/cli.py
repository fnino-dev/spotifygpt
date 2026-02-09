"""Command line interface for importing Spotify streaming history."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

from spotifygpt.alerts import detect_alerts
from spotifygpt.importer import init_db, load_streaming_history, store_alerts, store_streams


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import Spotify StreamingHistory JSON.")
    parser.add_argument("input", type=Path, help="Folder containing StreamingHistory*.json")
    parser.add_argument("db", type=Path, help="SQLite database path")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    result = load_streaming_history(args.input)
    if not result.files:
        print("No StreamingHistory files found.", file=sys.stderr)
        return 1

    with sqlite3.connect(args.db) as connection:
        init_db(connection)
        inserted = store_streams(connection, result.streams)
        alerts = detect_alerts(result.streams)
        alert_count = store_alerts(connection, alerts)

    print(f"Imported {inserted} streams from {len(result.files)} files.")
    if alert_count:
        print(f"Detected {alert_count} alerts.")

    if result.errors:
        print(f"Encountered {len(result.errors)} errors.", file=sys.stderr)
        for error in result.errors:
            location = (
                f"{error.file}:{error.record_index}"
                if error.record_index is not None
                else str(error.file)
            )
            print(f"- {location}: {error.message}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
