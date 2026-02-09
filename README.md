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

---

## Development

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
