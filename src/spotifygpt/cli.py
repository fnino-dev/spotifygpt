"""Command line interface for SpotifyGPT pipelines."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

from spotifygpt.alerts import detect_alerts
from spotifygpt.importer import init_db, load_streaming_history, store_alerts, store_streams


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SpotifyGPT CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    import_parser = subparsers.add_parser(
        "import", help="Import StreamingHistory JSON files into SQLite."
    )
    import_parser.add_argument(
        "input", type=Path, help="Folder containing StreamingHistory*.json"
    )
    import_parser.add_argument("db", type=Path, help="SQLite database path")

    metrics_parser = subparsers.add_parser("metrics", help="Compute summary metrics.")
    metrics_parser.add_argument("db", type=Path, help="SQLite database path")

    classify_parser = subparsers.add_parser(
        "classify", help="Classify tracks by total listen time."
    )
    classify_parser.add_argument("db", type=Path, help="SQLite database path")
    classify_parser.add_argument(
        "--threshold-ms",
        type=int,
        default=200_000,
        help="Minimum milliseconds for heavy rotation classification.",
    )

    weekly_parser = subparsers.add_parser(
        "weekly-radar", help="Generate the weekly radar report."
    )
    weekly_parser.add_argument("db", type=Path, help="SQLite database path")
    weekly_parser.add_argument(
        "--top-n", type=int, default=5, help="Number of tracks to include."
    )

    daily_parser = subparsers.add_parser(
        "daily-mode", help="Generate the daily mode summary."
    )
    daily_parser.add_argument("db", type=Path, help="SQLite database path")

    alerts_parser = subparsers.add_parser(
        "alerts", help="Generate alerts from the latest data."
    )
    alerts_parser.add_argument("db", type=Path, help="SQLite database path")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "import":
        result = load_streaming_history(args.input)
        if not result.files:
            print("No StreamingHistory files found.", file=sys.stderr)
            return 1

        with sqlite3.connect(args.db) as connection:
            init_db(connection)
            # init_pipeline_tables(connection)  # TODO: create pipeline tables in later issues
            inserted = store_streams(connection, result.streams)

        print(f"Imported {inserted} streams from {len(result.files)} files.")

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

    with sqlite3.connect(args.db) as connection:
        init_db(connection)
        inserted = store_streams(connection, result.streams)
        alerts = detect_alerts(result.streams)
        alert_count = store_alerts(connection, alerts)

    print(f"Imported {inserted} streams from {len(result.files)} files.")
    if alert_count:
        print(f"Detected {alert_count} alerts.")

        if args.command == "classify":
            classifications = classify_tracks(connection, threshold_ms=args.threshold_ms)
            print(f"Classified {len(classifications)} tracks.")
            return 0

        if args.command == "weekly-radar":
            entries = build_weekly_radar(connection, top_n=args.top_n)
            print(f"Weekly radar entries: {len(entries)}")
            return 0

        if args.command == "daily-mode":
            entry = build_daily_mode(connection)
            if entry is None:
                print("No daily data available.", file=sys.stderr)
                return 1
            print(
                f"Daily mode for {entry.date}: {entry.stream_count} streams, {entry.total_ms} ms"
            )
            return 0

        if args.command == "alerts":
            alerts = generate_alerts(connection)
            print(f"Generated {len(alerts)} alerts.")
            return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
