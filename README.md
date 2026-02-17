# SpotifyGPT

SpotifyGPT is a foundation repository for exploring deterministic, AI-assisted
workflows built on top of Spotify listening data.

The goal of this project is to design and validate a **behavior-driven music
intelligence system**, focused on signal extraction, repeatable patterns, and
decision-making — not recommendation engines or social discovery.

This repository contains the **core technical engine** of SpotifyGPT.

---

## Getting started

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
pytest
```

## CLI usage

```bash
# Authenticate against Spotify and persist tokens securely
python -m spotifygpt auth --client-id "$SPOTIFY_CLIENT_ID"

# Import streaming history into SQLite
python -m spotifygpt import data/sample ./spotifygpt.db

# Import GDPR extended streaming history (zip or folder)
python -m spotifygpt import-gdpr data/sample_gdpr ./spotifygpt.db

# Compute metrics
python -m spotifygpt metrics ./spotifygpt.db

# Classify tracks
python -m spotifygpt classify ./spotifygpt.db --threshold-ms 200000

# Generate weekly radar
python -m spotifygpt weekly-radar ./spotifygpt.db --top-n 5

# Generate daily mode summary
python -m spotifygpt daily-mode ./spotifygpt.db

# Generate alerts
python -m spotifygpt alerts ./spotifygpt.db

# Run Spotify API standard sync (Issue #23 scope)
python -m spotifygpt sync ./spotifygpt.db --token "$SPOTIFY_TOKEN" --since 2026-01-01T00:00:00Z

# Backfill missing audio features for used tracks
python -m spotifygpt backfill-features ./spotifygpt.db --auth-token "$SPOTIFY_TOKEN" --limit 100 --since 2026-01-01T00:00:00

# Manual-only workflow (no streams required yet):
# 1) import-manual
# 2) backfill-features
# 3) profile
python -m spotifygpt import-manual --liked liked.json --playlists playlists.json --db ./spotifygpt.db
python -m spotifygpt backfill-features ./spotifygpt.db --auth-token "$SPOTIFY_TOKEN" --limit 100
python -m spotifygpt profile ./spotifygpt.db --output musical_dna_v1.json

# Generate deterministic musical DNA profile JSON
python -m spotifygpt profile ./spotifygpt.db --output musical_dna_v1.json --mode-playlist FreshkitØ --mode-playlist Suave_Suave_

---

## Development

The auth command uses Spotify OAuth (authorization code + PKCE) and stores access/refresh
tokens at `~/.spotifygpt/tokens.json` with file mode `600`. When access tokens expire,
`TokenStore.get_access_token(...)` refreshes them automatically using the refresh token.

- Source code lives in `src/spotifygpt`
- Tests live in `tests/`
- The project is intentionally backend-only and CLI-driven
- No UI, API, or external services are included at this stage

The codebase prioritizes:
- deterministic behavior
- explicit rules over heuristics
- reproducibility and testability

---

## Project status

This repository is under **active development**.

The architecture and rules are evolving, and stability is **not yet guaranteed**.
The code is published to make the design and reasoning transparent, not to invite
external modification or collaboration.

---

## License & Usage

This repository is **source-available**, **not open source**.

The code is made publicly available for inspection and personal study only.
**No contributions are accepted at this time.**

You may **not**:
- use this code in commercial products or services
- redistribute this code, in whole or in part
- modify or create derivative works
- offer this software as a hosted service (SaaS)

All rights are reserved by the author.

For licensing inquiries or commercial use, contact the author directly.

---

## Trademark

"SpotifyGPT" is a project name and trademark of its author.

This repository and its license **do not grant any rights** to use the name
"SpotifyGPT" in connection with derivative works, products, services, or
commercial offerings, whether or not the underlying code is inspected or reused.

Any use of the name "SpotifyGPT" for commercial or promotional purposes requires
explicit written permission from the author.

---

## Attribution

Copyright (c) 2026 Fernando Niño
All rights reserved.
```
