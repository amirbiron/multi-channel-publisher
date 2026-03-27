"""
google_auth.py — Google OAuth 2.0 token management for GBP.

Handles access-token acquisition and automatic refresh using a
long-lived refresh token (stored as env var).  The token is kept
in-memory and refreshed transparently when it expires.
"""

from __future__ import annotations

import logging
import threading
import time

import requests

logger = logging.getLogger(__name__)

# Google's OAuth 2.0 token endpoint
_TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token"

# Refresh the token 5 minutes before it actually expires
_EXPIRY_MARGIN_SECONDS = 300


class GoogleOAuthManager:
    """
    Manages a single OAuth 2.0 access token for the Google Business
    Profile API, refreshing it automatically when needed.

    Usage::

        mgr = GoogleOAuthManager(client_id, client_secret, refresh_token)
        headers = mgr.get_auth_headers()   # {"Authorization": "Bearer …"}
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: str,
    ) -> None:
        if not all([client_id, client_secret, refresh_token]):
            raise ValueError(
                "GBP OAuth credentials incomplete. "
                "Set GBP_OAUTH_CLIENT_ID, GBP_OAUTH_CLIENT_SECRET, and GBP_REFRESH_TOKEN."
            )

        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token

        self._access_token: str | None = None
        self._expires_at: float = 0.0  # epoch seconds
        self._lock = threading.Lock()

    # ── public API ───────────────────────────────────────────

    def get_access_token(self) -> str:
        """Return a valid access token, refreshing if necessary."""
        if self._is_token_valid():
            return self._access_token  # type: ignore[return-value]

        with self._lock:
            # Double-check after acquiring lock
            if self._is_token_valid():
                return self._access_token  # type: ignore[return-value]
            self._refresh()
            return self._access_token  # type: ignore[return-value]

    def get_auth_headers(self) -> dict[str, str]:
        """Return Authorization header dict ready for requests."""
        return {"Authorization": f"Bearer {self.get_access_token()}"}

    def force_refresh(self) -> str:
        """Force a token refresh regardless of expiry. Returns new token."""
        with self._lock:
            self._refresh()
            return self._access_token  # type: ignore[return-value]

    # ── internals ────────────────────────────────────────────

    def _is_token_valid(self) -> bool:
        return (
            self._access_token is not None
            and time.time() < self._expires_at - _EXPIRY_MARGIN_SECONDS
        )

    def _refresh(self) -> None:
        """Exchange the refresh token for a new access token."""
        logger.info("Refreshing GBP OAuth access token …")

        resp = requests.post(
            _TOKEN_ENDPOINT,
            data={
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "refresh_token": self._refresh_token,
                "grant_type": "refresh_token",
            },
            timeout=30,
        )

        if resp.status_code != 200:
            logger.error("OAuth token refresh failed: %s %s", resp.status_code, resp.text[:500])
            raise OAuthRefreshError(
                f"Token refresh failed ({resp.status_code}): {resp.text[:300]}"
            )

        data = resp.json()
        self._access_token = data["access_token"]
        expires_in = int(data.get("expires_in", 3600))
        self._expires_at = time.time() + expires_in

        logger.info("GBP OAuth token refreshed, expires in %ds", expires_in)


class OAuthRefreshError(Exception):
    """Raised when the OAuth token refresh request fails."""


# ── module-level singleton (lazy) ────────────────────────────

_manager: GoogleOAuthManager | None = None


def get_oauth_manager() -> GoogleOAuthManager:
    """
    Return the module-level GoogleOAuthManager singleton.

    Lazily created on first call so that missing env vars don't
    crash the process at import time (important for tests and
    for deployments that don't use GBP).
    """
    global _manager
    if _manager is None:
        from config import (
            GBP_OAUTH_CLIENT_ID,
            GBP_OAUTH_CLIENT_SECRET,
            GBP_REFRESH_TOKEN,
        )
        _manager = GoogleOAuthManager(
            client_id=GBP_OAUTH_CLIENT_ID,
            client_secret=GBP_OAUTH_CLIENT_SECRET,
            refresh_token=GBP_REFRESH_TOKEN,
        )
    return _manager


def reset_oauth_manager() -> None:
    """Reset the singleton (useful for tests)."""
    global _manager
    _manager = None
