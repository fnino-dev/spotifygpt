from __future__ import annotations

import os
import stat
import time
from pathlib import Path

from spotifygpt.auth import OAuthConfig, OAuthTokenResponse
from spotifygpt.token_store import TokenStore


def test_store_and_load_token_with_secure_permissions(tmp_path: Path) -> None:
    path = tmp_path / "tokens.json"
    store = TokenStore(path)

    stored = store.store_from_oauth(
        OAuthTokenResponse(
            access_token="access-1",
            token_type="Bearer",
            expires_in=3600,
            refresh_token="refresh-1",
            scope="scope",
        )
    )
    loaded = store.load()

    assert loaded is not None
    assert loaded.access_token == stored.access_token
    assert loaded.refresh_token == stored.refresh_token

    mode = stat.S_IMODE(os.stat(path).st_mode)
    if os.name == "posix":
        assert mode == stat.S_IRUSR | stat.S_IWUSR
    else:
        # Windows does not reliably map ACLs to POSIX permission bits. Validate
        # portable security invariants without enforcing chmod(0o600): the file
        # must exist, be a regular file, and not be executable.
        assert path.exists()
        assert path.is_file()
        assert (mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)) == 0


def test_get_access_token_refreshes_when_expired(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "tokens.json"
    store = TokenStore(path)
    store.store_from_oauth(
        OAuthTokenResponse(
            access_token="expired",
            token_type="Bearer",
            expires_in=0,
            refresh_token="refresh-1",
            scope="scope",
        )
    )

    monkeypatch.setattr(time, "time", lambda: 9999999999)

    import spotifygpt.token_store as token_store_module

    def fake_refresh(_config: OAuthConfig, _refresh_token: str) -> OAuthTokenResponse:
        return OAuthTokenResponse(
            access_token="fresh-token",
            token_type="Bearer",
            expires_in=3600,
            refresh_token=None,
            scope="scope",
        )

    monkeypatch.setattr(token_store_module, "refresh_access_token", fake_refresh)

    token = store.get_access_token(
        OAuthConfig(client_id="abc", redirect_uri="http://127.0.0.1:8888/callback")
    )

    assert token == "fresh-token"
    assert store.load() is not None
    assert store.load().refresh_token == "refresh-1"
