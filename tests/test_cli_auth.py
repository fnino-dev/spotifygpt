from __future__ import annotations

from pathlib import Path

from spotifygpt import cli
from spotifygpt.auth import OAuthTokenResponse


def test_cli_auth_requires_client_id(capsys) -> None:
    rc = cli.main(["auth"])
    captured = capsys.readouterr()

    assert rc == 1
    assert "Missing client id" in captured.err


def test_cli_auth_stores_tokens(tmp_path: Path, monkeypatch) -> None:
    token_store = tmp_path / "tokens.json"

    def fake_authenticate(_config):
        return OAuthTokenResponse(
            access_token="access",
            token_type="Bearer",
            expires_in=3600,
            refresh_token="refresh",
            scope="scope",
        )

    monkeypatch.setattr(cli, "authenticate_browser_flow", fake_authenticate)

    rc = cli.main(
        [
            "auth",
            "--client-id",
            "abc123",
            "--redirect-uri",
            "http://127.0.0.1:8888/callback",
            "--token-store",
            str(token_store),
        ]
    )

    assert rc == 0
    assert token_store.exists()
