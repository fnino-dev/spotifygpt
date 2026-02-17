"""Command line interface for SpotifyGPT pipelines."""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

from spotifygpt.auth import OAuthConfig, authenticate_browser_flow
from spotifygpt.audio_features import (
    HttpAudioFeatureProvider,
    SpotifyWebApiAudioFeatureProvider,
    backfill_audio_features,
    init_audio_feature_tables,
)
from spotifygpt.importer import import_gdpr, init_db, load_streaming_history, store_streams
from spotifygpt.ingest_status import collect_ingest_status, render_ingest_status
from spotifygpt.manual_import import (
    init_manual_import_tables,
    load_manual_payload,
    store_manual_payload,
)
from spotifygpt.pipeline import (
    build_daily_mode,
    build_weekly_radar,
    classify_tracks,
    compute_metrics,
    ensure_non_empty_streams,
    generate_alerts,
    init_pipeline_tables,
)
from spotifygpt.profile import DEFAULT_OUTPUT_PATH, generate_profile, write_profile
from spotifygpt.sync_v2 import SpotifyAPIClient, SyncService
from spotifygpt.token_store import TokenStore


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

    import_gdpr_parser = subparsers.add_parser(
        "import-gdpr",
        help="Import Spotify GDPR extended streaming history (folder or zip).",
    )
    import_gdpr_parser.add_argument(
        "input",
        type=Path,
        help="Path to GDPR export zip file or extracted folder",
    )
    import_gdpr_parser.add_argument("db", type=Path, help="SQLite database path")

    manual_parser = subparsers.add_parser(
        "import-manual", help="Import manually exported liked songs and playlists."
    )
    manual_parser.add_argument(
        "--liked", required=True, type=Path, help="Path to liked songs JSON export"
    )
    manual_parser.add_argument(
        "--playlists", required=True, type=Path, help="Path to playlists JSON export"
    )
    manual_parser.add_argument(
        "--db",
        default=Path("spotifygpt.db"),
        type=Path,
        help="SQLite database path (default: ./spotifygpt.db)",
    )

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

    sync_parser = subparsers.add_parser(
        "sync", help="Run Spotify API standard ingestion (v2)."
    )
    sync_parser.add_argument("db", type=Path, help="SQLite database path")
    sync_parser.add_argument(
        "--token",
        required=True,
        help="Spotify OAuth access token with user-library-read, playlist-read-private, user-top-read, user-read-recently-played.",
    )
    sync_parser.add_argument(
        "--since",
        help="ISO-8601 timestamp filter for incremental ingest.",
    )

    ingest_status_parser = subparsers.add_parser(
        "ingest-status", help="Print ingest V2 status and sanity checks."
    )
    ingest_status_parser.add_argument("db", type=Path, help="SQLite database path")

    auth_parser = subparsers.add_parser(
        "auth", help="Run Spotify OAuth and persist access/refresh tokens."
    )
    # IMPORTANT: default must be None so tests can monkeypatch env vars after import.
    auth_parser.add_argument(
        "--client-id",
        default=None,
        help="Spotify app client id. Defaults to SPOTIFY_CLIENT_ID env var.",
    )
    auth_parser.add_argument(
        "--redirect-uri",
        default="http://127.0.0.1:8888/callback",
        help="OAuth redirect URI configured in Spotify app settings.",
    )
    auth_parser.add_argument(
        "--scope",
        default="user-read-recently-played user-top-read",
        help="Space-separated Spotify OAuth scopes.",
    )
    auth_parser.add_argument(
        "--token-store",
        type=Path,
        default=Path.home() / ".spotifygpt" / "tokens.json",
        help="Path where tokens are stored with mode 600.",
    )

    backfill_parser = subparsers.add_parser(
        "backfill-features",
        help="Backfill missing audio features for tracks used in streams.",
    )
    backfill_parser.add_argument("db", type=Path, help="SQLite database path")
    backfill_parser.add_argument(
        "--limit", type=int, default=None, help="Maximum number of tracks to backfill."
    )
    backfill_parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Only consider streams at or after this ISO-8601 timestamp.",
    )
    backfill_parser.add_argument(
        "--requests-per-second",
        type=float,
        default=5.0,
        help="Maximum outbound audio-feature requests per second.",
    )
    backfill_parser.add_argument(
        "--endpoint",
        type=str,
        default=None,
        help="Audio-feature HTTP endpoint. Defaults to SPOTIFYGPT_AUDIO_FEATURES_ENDPOINT.",
    )
    backfill_parser.add_argument(
        "--auth-token",
        type=str,
        default=None,
        help="Optional bearer token for audio-feature endpoint.",
    )

    profile_parser = subparsers.add_parser(
        "profile", help="Generate deterministic musical_dna_v1 profile JSON."
    )
    profile_parser.add_argument(
        "db", type=Path, help="SQLite database path"
    )
    profile_parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Output JSON path (default: ./musical_dna_v1.json)",
    )
    profile_parser.add_argument(
        "--mode-playlist",
        action="append",
        default=[],
        help="Mode playlist selector (playlist id first, then name fallback). Repeatable.",
    )
    profile_parser.add_argument(
        "--mode-label",
        action="append",
        default=[],
        help="Mode label override in the form selector=Label. Repeatable.",
    )
    profile_parser.add_argument(
        "--mode-labels-file",
        type=Path,
        default=None,
        help="Optional JSON file mapping playlist selectors to labels.",
    )
    profile_parser.add_argument(
        "--my-top-tracks-playlist",
        default="my_top_tracks_playlist",
        help="Playlist selector for my_top_tracks signal.",
    )
    profile_parser.add_argument(
        "--radar-playlist",
        default="radar_de_novedades",
        help="Playlist selector for radar_de_novedades signal.",
    )

    return parser


def _ensure_pipeline_alerts_table(connection: sqlite3.Connection) -> None:
    columns = {row[1] for row in connection.execute("PRAGMA table_info(alerts)").fetchall()}
    expected = {"id", "created_at", "level", "message"}
    if columns and columns != expected:
        connection.execute("DROP TABLE IF EXISTS alerts")
        connection.commit()


def _is_valid_iso8601(value: str) -> bool:
    try:
        datetime.fromisoformat(value)
    except ValueError:
        return False
    return True


def _build_audio_feature_provider(
    args: argparse.Namespace,
) -> HttpAudioFeatureProvider | SpotifyWebApiAudioFeatureProvider | None:
    endpoint = args.endpoint or os.environ.get("SPOTIFYGPT_AUDIO_FEATURES_ENDPOINT")
    auth_token = args.auth_token or os.environ.get("SPOTIFYGPT_AUDIO_FEATURES_TOKEN")
    if endpoint is not None:
        return HttpAudioFeatureProvider(endpoint=endpoint, auth_token=auth_token)
    if auth_token is None:
        return None
    return SpotifyWebApiAudioFeatureProvider(auth_token=auth_token)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "auth":
        # Resolve env var at runtime (NOT at parser construction time).
        client_id = args.client_id or os.environ.get("SPOTIFY_CLIENT_ID")
        if not client_id:
            print(
                "Missing client id. Set --client-id or SPOTIFY_CLIENT_ID.",
                file=sys.stderr,
            )
            return 1

        config = OAuthConfig(
            client_id=client_id,
            redirect_uri=args.redirect_uri,
            scope=args.scope,
        )
        print("Starting OAuth browser flow...")
        token = authenticate_browser_flow(config)
        store = TokenStore(args.token_store)
        stored = store.store_from_oauth(token)
        print(f"Authenticated successfully. Token stored at {store.path}.")
        print(f"Access token expires at unix timestamp {stored.expires_at}.")
        return 0

    if args.command == "import":
        result = load_streaming_history(args.input)
        if not result.files:
            print("No StreamingHistory files found.", file=sys.stderr)
            return 1

        with sqlite3.connect(args.db) as connection:
            init_db(connection)
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

    if args.command == "import-gdpr":
        with sqlite3.connect(args.db) as connection:
            init_db(connection)
            result = import_gdpr(connection, args.input)

        if not result.files:
            print("No endsong JSON files found.", file=sys.stderr)
            return 1

        print(
            "Imported "
            f"{result.rows_inserted} listening events "
            f"from {len(result.files)} files (rows seen: {result.rows_seen}, run_id: {result.run_id})."
        )
        return 0

    if args.command == "import-manual":
        payload = load_manual_payload(args.liked, args.playlists)
        with sqlite3.connect(args.db) as connection:
            init_db(connection)
            init_manual_import_tables(connection)
            result = store_manual_payload(connection, payload)

        print(
            "Imported manual data: "
            f"{result.tracks} tracks, "
            f"{result.library} library rows, "
            f"{result.playlists} playlists, "
            f"{result.playlist_tracks} playlist tracks."
        )
        return 0

    if args.command == "profile":
        mode_labels: dict[str, str] = {}
        for raw in args.mode_label:
            key, sep, value = raw.partition("=")
            if not sep or not key.strip() or not value.strip():
                print("Invalid --mode-label value. Use selector=Label format.", file=sys.stderr)
                return 1
            mode_labels[key.strip()] = value.strip()

        with sqlite3.connect(args.db) as connection:
            init_db(connection)
            init_manual_import_tables(connection)
            init_audio_feature_tables(connection)
            profile = generate_profile(
                connection,
                mode_selectors=args.mode_playlist,
                mode_labels=mode_labels,
                mode_labels_file=args.mode_labels_file,
                include_top_tracks_playlist=args.my_top_tracks_playlist,
                include_radar_playlist=args.radar_playlist,
            )

        write_profile(profile, output_path=args.output)
        print(f"Wrote profile to {args.output}.")
        return 0

    with sqlite3.connect(args.db) as connection:
        init_db(connection)

        if args.command == "sync":
            service = SyncService(SpotifyAPIClient(token=args.token))
            service.init_db(connection)
            summary = service.run_standard_sync(connection, args.since)
            print(
                "Sync run "
                f"#{summary.run_id} complete: saved={summary.saved_tracks}, "
                f"playlist_tracks={summary.playlist_tracks}, top_items={summary.top_items}, "
                f"recent={summary.recent_tracks}"
            )
            return 0

        if args.command == "ingest-status":
            status = collect_ingest_status(connection)
            print(render_ingest_status(status))
            return 0

        _ensure_pipeline_alerts_table(connection)
        init_pipeline_tables(connection)
        init_audio_feature_tables(connection)

        if args.command == "backfill-features":
            init_manual_import_tables(connection)

            if args.since is not None and not _is_valid_iso8601(args.since):
                print("Invalid --since value. Use ISO-8601 format.", file=sys.stderr)
                return 1

            provider = _build_audio_feature_provider(args)
            if provider is None:
                print(
                    "Missing audio-features provider configuration. Provide --auth-token "
                    "for built-in Spotify Web API backfill or set --endpoint for a custom provider.",
                    file=sys.stderr,
                )
                return 1

            result = backfill_audio_features(
                connection,
                provider=provider,
                limit=args.limit,
                since=args.since,
                requests_per_second=args.requests_per_second,
            )
            print(
                "Backfilled audio features "
                f"(scanned={result.scanned}, inserted={result.inserted}, "
                f"cache_hits={result.cache_hits}, api_calls={result.api_calls})."
            )
            return 0

        if not ensure_non_empty_streams(connection):
            print("No streams available. Run the import command first.", file=sys.stderr)
            return 1

        if args.command == "metrics":
            metrics = compute_metrics(connection)
            print(f"Computed {len(metrics)} metrics.")
            return 0

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
            print(f"Daily mode for {entry.date}: {entry.stream_count} streams, {entry.total_ms} ms")
            return 0

        if args.command == "alerts":
            alerts = generate_alerts(connection)
            print(f"Generated {len(alerts)} alerts.")
            return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
