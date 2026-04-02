"""
linkedin_auth.py — LinkedIn OAuth 2.0 token management.

Handles access-token acquisition and automatic refresh using a
long-lived refresh token (stored as env var).  The token is kept
in-memory and refreshed transparently when it expires.

LinkedIn three-legged OAuth 2.0 flow with permission: w_member_social.
"""

from __future__ import annotations

import logging
import threading
import time

import requests

logger = logging.getLogger(__name__)

# Refresh the token 5 minutes before it actually expires
_EXPIRY_MARGIN_SECONDS = 300


class LinkedInOAuthManager:
    """
    Manages LinkedIn OAuth 2.0 access tokens using a refresh token.

    LinkedIn's three-legged OAuth flow issues refresh tokens that can be
    exchanged for short-lived access tokens.

    Usage::

        mgr = LinkedInOAuthManager(client_id, client_secret, refresh_token)
        headers = mgr.get_auth_headers()   # {"Authorization": "Bearer ...", ...}
    """

    _TOKEN_ENDPOINT = "https://www.linkedin.com/oauth/v2/accessToken"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: str,
    ) -> None:
        if not all([client_id, client_secret, refresh_token]):
            raise ValueError(
                "LinkedIn OAuth credentials incomplete. "
                "Set LI_OAUTH_CLIENT_ID, LI_OAUTH_CLIENT_SECRET, and LI_REFRESH_TOKEN."
            )
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._access_token: str | None = None
        self._expires_at: float = 0.0  # epoch seconds
        self._lock = threading.Lock()

    # -- public API ---------------------------------------------------

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
        """Return headers required for LinkedIn REST API calls."""
        return {
            "Authorization": f"Bearer {self.get_access_token()}",
            "LinkedIn-Version": "202401",
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0",
        }

    def force_refresh(self) -> str:
        """Force a token refresh regardless of expiry. Returns new token."""
        with self._lock:
            self._refresh()
            return self._access_token  # type: ignore[return-value]

    # -- internals ----------------------------------------------------

    def _is_token_valid(self) -> bool:
        return (
            self._access_token is not None
            and time.time() < self._expires_at - _EXPIRY_MARGIN_SECONDS
        )

    def _refresh(self) -> None:
        """Exchange the refresh token for a new access token."""
        logger.info("Refreshing LinkedIn OAuth access token ...")

        resp = requests.post(
            self._TOKEN_ENDPOINT,
            data={
                "grant_type": "refresh_token",
                "refresh_token": self._refresh_token,
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
            timeout=30,
        )

        if resp.status_code != 200:
            logger.error(
                "LinkedIn OAuth token refresh failed: %s %s",
                resp.status_code,
                resp.text[:500],
            )
            raise LinkedInOAuthError(
                f"Token refresh failed ({resp.status_code}): {resp.text[:300]}"
            )

        data = resp.json()
        self._access_token = data["access_token"]
        expires_in = int(data.get("expires_in", 3600))
        self._expires_at = time.time() + expires_in
        logger.info("LinkedIn OAuth token refreshed, expires in %ds", expires_in)


class LinkedInOAuthError(Exception):
    """Raised when the LinkedIn OAuth token refresh request fails."""


# -- module-level singleton (lazy) ------------------------------------

_manager: LinkedInOAuthManager | None = None


def get_li_oauth_manager() -> LinkedInOAuthManager:
    """
    Return the module-level LinkedInOAuthManager singleton.

    Lazily created on first call so that missing env vars don't
    crash the process at import time (important for tests and
    for deployments that don't use LinkedIn).
    """
    global _manager
    if _manager is None:
        from config import (
            LI_OAUTH_CLIENT_ID,
            LI_OAUTH_CLIENT_SECRET,
            LI_REFRESH_TOKEN,
        )
        _manager = LinkedInOAuthManager(
            client_id=LI_OAUTH_CLIENT_ID,
            client_secret=LI_OAUTH_CLIENT_SECRET,
            refresh_token=LI_REFRESH_TOKEN,
        )
    return _manager


def reset_li_oauth_manager() -> None:
    """Reset the singleton (useful for tests)."""
    global _manager
    _manager = None
