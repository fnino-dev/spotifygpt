from __future__ import annotations

from spotifygpt.auth import (
    OAuthConfig,
    authenticate_browser_flow,
    build_authorization_url,
    build_code_challenge,
    exchange_code_for_token,
    refresh_access_token,
)


def test_build_authorization_url_contains_required_values() -> None:
    config = OAuthConfig(
        client_id="abc123",
        redirect_uri="http://127.0.0.1:8888/callback",
        scope="user-read-email",
    )
    challenge = build_code_challenge("my-verifier")

    url = build_authorization_url(config, state="state-1", code_challenge=challenge)

    assert "response_type=code" in url
    assert "client_id=abc123" in url
    assert "state=state-1" in url
    assert "code_challenge_method=S256" in url


def test_exchange_and_refresh_token_from_mock_requester() -> None:
    config = OAuthConfig(
        client_id="abc123",
        redirect_uri="http://127.0.0.1:8888/callback",
    )

    def requester(payload: dict[str, str]) -> dict[str, object]:
        if payload["grant_type"] == "authorization_code":
            return {
                "access_token": "access-1",
                "refresh_token": "refresh-1",
                "token_type": "Bearer",
                "expires_in": 3600,
                "scope": "user-read-private",
            }
        return {
            "access_token": "access-2",
            "token_type": "Bearer",
            "expires_in": 3600,
            "scope": "user-read-private",
        }

    issued = exchange_code_for_token(config, "code", "verifier", token_requester=requester)
    refreshed = refresh_access_token(config, "refresh-1", token_requester=requester)

    assert issued.access_token == "access-1"
    assert issued.refresh_token == "refresh-1"
    assert refreshed.access_token == "access-2"


def test_authenticate_browser_flow_uses_callback_and_requester() -> None:
    config = OAuthConfig(
        client_id="abc123",
        redirect_uri="http://127.0.0.1:8888/callback",
    )
    opened: list[str] = []

    def callback_waiter(_redirect_uri: str, _state: str) -> str:
        return "auth-code"

    def browser_opener(url: str) -> bool:
        opened.append(url)
        return True

    def requester(payload: dict[str, str]) -> dict[str, object]:
        assert payload["grant_type"] == "authorization_code"
        return {
            "access_token": "access-1",
            "refresh_token": "refresh-1",
            "token_type": "Bearer",
            "expires_in": 3600,
            "scope": "user-read-private",
        }

    token = authenticate_browser_flow(
        config,
        callback_waiter=callback_waiter,
        browser_opener=browser_opener,
        token_requester=requester,
    )

    assert opened
    assert token.access_token == "access-1"
