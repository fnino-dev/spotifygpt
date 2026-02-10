"""Spotify OAuth helpers and browser-based auth flow."""

from __future__ import annotations

import base64
import hashlib
import json
import secrets
import threading
import time
import urllib.parse
import urllib.request
import webbrowser
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Callable

SPOTIFY_AUTHORIZE_URL = "https://accounts.spotify.com/authorize"
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"


@dataclass(frozen=True)
class OAuthConfig:
    client_id: str
    redirect_uri: str
    scope: str = "user-read-recently-played user-top-read"


@dataclass(frozen=True)
class OAuthTokenResponse:
    access_token: str
    token_type: str
    expires_in: int
    refresh_token: str | None
    scope: str


def generate_code_verifier() -> str:
    return secrets.token_urlsafe(64)


def _urlsafe_b64_without_padding(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def build_code_challenge(code_verifier: str) -> str:
    digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
    return _urlsafe_b64_without_padding(digest)


def build_authorization_url(config: OAuthConfig, state: str, code_challenge: str) -> str:
    query = urllib.parse.urlencode(
        {
            "response_type": "code",
            "client_id": config.client_id,
            "scope": config.scope,
            "redirect_uri": config.redirect_uri,
            "state": state,
            "code_challenge_method": "S256",
            "code_challenge": code_challenge,
        }
    )
    return f"{SPOTIFY_AUTHORIZE_URL}?{query}"


def _request_token(payload: dict[str, str]) -> dict[str, object]:
    body = urllib.parse.urlencode(payload).encode("utf-8")
    request = urllib.request.Request(
        SPOTIFY_TOKEN_URL,
        data=body,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.loads(response.read().decode("utf-8"))


def exchange_code_for_token(
    config: OAuthConfig,
    code: str,
    code_verifier: str,
    token_requester: Callable[[dict[str, str]], dict[str, object]] = _request_token,
) -> OAuthTokenResponse:
    data = token_requester(
        {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": config.redirect_uri,
            "client_id": config.client_id,
            "code_verifier": code_verifier,
        }
    )
    return OAuthTokenResponse(
        access_token=str(data["access_token"]),
        token_type=str(data.get("token_type", "Bearer")),
        expires_in=int(data.get("expires_in", 3600)),
        refresh_token=(None if "refresh_token" not in data else str(data["refresh_token"])),
        scope=str(data.get("scope", config.scope)),
    )


def refresh_access_token(
    config: OAuthConfig,
    refresh_token: str,
    token_requester: Callable[[dict[str, str]], dict[str, object]] = _request_token,
) -> OAuthTokenResponse:
    data = token_requester(
        {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": config.client_id,
        }
    )
    return OAuthTokenResponse(
        access_token=str(data["access_token"]),
        token_type=str(data.get("token_type", "Bearer")),
        expires_in=int(data.get("expires_in", 3600)),
        refresh_token=(None if "refresh_token" not in data else str(data["refresh_token"])),
        scope=str(data.get("scope", config.scope)),
    )


def wait_for_callback_code(redirect_uri: str, expected_state: str, timeout_seconds: int = 180) -> str:
    parsed = urllib.parse.urlparse(redirect_uri)
    if parsed.hostname is None or parsed.port is None or not parsed.path:
        raise ValueError("redirect_uri must include host, port, and path")

    result: dict[str, str] = {}
    signal = threading.Event()

    class CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            request_url = urllib.parse.urlparse(self.path)
            if request_url.path != parsed.path:
                self.send_response(404)
                self.end_headers()
                return

            params = urllib.parse.parse_qs(request_url.query)
            state = params.get("state", [""])[0]
            if state != expected_state:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Invalid state.")
                result["error"] = "Invalid OAuth state"
                signal.set()
                return

            code = params.get("code", [""])[0]
            if not code:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Missing code.")
                result["error"] = "Authorization code missing"
                signal.set()
                return

            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Authentication complete. You can close this tab.")
            result["code"] = code
            signal.set()

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = HTTPServer((parsed.hostname, parsed.port), CallbackHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        if not signal.wait(timeout=timeout_seconds):
            raise TimeoutError("Timed out waiting for OAuth callback")
        if "error" in result:
            raise RuntimeError(result["error"])
        return result["code"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)


def authenticate_browser_flow(
    config: OAuthConfig,
    callback_waiter: Callable[[str, str], str] = wait_for_callback_code,
    browser_opener: Callable[[str], bool] = webbrowser.open,
    token_requester: Callable[[dict[str, str]], dict[str, object]] = _request_token,
) -> OAuthTokenResponse:
    state = secrets.token_urlsafe(16)
    code_verifier = generate_code_verifier()
    code_challenge = build_code_challenge(code_verifier)
    url = build_authorization_url(config, state=state, code_challenge=code_challenge)

    browser_opener(url)
    code = callback_waiter(config.redirect_uri, state)

    return exchange_code_for_token(
        config,
        code=code,
        code_verifier=code_verifier,
        token_requester=token_requester,
    )


def token_expiry_timestamp(expires_in: int) -> int:
    return int(time.time()) + max(expires_in, 0)
