"""Persistent token storage with automatic refresh support."""

from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path

from spotifygpt.auth import OAuthConfig, OAuthTokenResponse, refresh_access_token, token_expiry_timestamp


@dataclass(frozen=True)
class StoredToken:
    access_token: str
    refresh_token: str
    token_type: str
    scope: str
    expires_at: int

    @property
    def is_expired(self) -> bool:
        from time import time

        return int(time()) >= self.expires_at


class TokenStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or (Path.home() / ".spotifygpt" / "tokens.json")

    def load(self) -> StoredToken | None:
        if not self.path.exists():
            return None
        data = json.loads(self.path.read_text(encoding="utf-8"))
        return StoredToken(
            access_token=str(data["access_token"]),
            refresh_token=str(data["refresh_token"]),
            token_type=str(data.get("token_type", "Bearer")),
            scope=str(data.get("scope", "")),
            expires_at=int(data["expires_at"]),
        )

    def save(self, token: StoredToken) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "access_token": token.access_token,
            "refresh_token": token.refresh_token,
            "token_type": token.token_type,
            "scope": token.scope,
            "expires_at": token.expires_at,
        }
        self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        if os.name == "posix":
            os.chmod(self.path, stat.S_IRUSR | stat.S_IWUSR)

    def store_from_oauth(self, token: OAuthTokenResponse, fallback_refresh_token: str | None = None) -> StoredToken:
        refresh_token = token.refresh_token or fallback_refresh_token
        if not refresh_token:
            raise ValueError("refresh_token required for persistence")
        stored = StoredToken(
            access_token=token.access_token,
            refresh_token=refresh_token,
            token_type=token.token_type,
            scope=token.scope,
            expires_at=token_expiry_timestamp(token.expires_in),
        )
        self.save(stored)
        return stored

    def get_access_token(self, config: OAuthConfig, refresh_margin_seconds: int = 60) -> str:
        token = self.load()
        if token is None:
            raise RuntimeError("No stored token found. Run auth first.")

        from time import time

        if int(time()) < (token.expires_at - refresh_margin_seconds):
            return token.access_token

        refreshed = refresh_access_token(config, token.refresh_token)
        updated = self.store_from_oauth(refreshed, fallback_refresh_token=token.refresh_token)
        return updated.access_token
