"""
linkedin_auth.py — LinkedIn OAuth 2.0 token management.

Supports two modes:
1. **Access-token mode** (Share on LinkedIn): LI_REFRESH_TOKEN contains a
   long-lived access token (~60 days).  No refresh flow — the token is used
   directly until it expires.
2. **Refresh-token mode** (Community Management API): LI_REFRESH_TOKEN
   contains a real refresh token that is exchanged for short-lived access
   tokens automatically.

The mode is auto-detected: if a refresh attempt fails, the manager falls
back to using LI_REFRESH_TOKEN as a direct access token.
"""

from __future__ import annotations

import logging
import threading
import time

import requests

logger = logging.getLogger(__name__)

# LinkedIn API version header (required by Community Management API)
_LI_API_VERSION = "202401"

# Refresh the token 5 minutes before it actually expires
_EXPIRY_MARGIN_SECONDS = 300

# Default TTL for direct access tokens (60 days)
_DIRECT_TOKEN_TTL_SECONDS = 60 * 24 * 3600


class LinkedInOAuthManager:
    """
    Manages LinkedIn OAuth 2.0 access tokens.

    Supports both refresh-token flow (Community Management API) and
    direct access-token mode (Share on LinkedIn).

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
        self._direct_mode: bool | None = None  # auto-detected on first use

    # -- public API ---------------------------------------------------

    def get_access_token(self) -> str:
        """Return a valid access token, refreshing if necessary."""
        if self._is_token_valid():
            return self._access_token  # type: ignore[return-value]

        with self._lock:
            # Double-check after acquiring lock
            if self._is_token_valid():
                return self._access_token  # type: ignore[return-value]
            self._resolve_token()
            return self._access_token  # type: ignore[return-value]

    def get_auth_headers(self) -> dict[str, str]:
        """Return headers required for LinkedIn REST API calls."""
        return {
            "Authorization": f"Bearer {self.get_access_token()}",
            "LinkedIn-Version": _LI_API_VERSION,
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0",
        }

    def force_refresh(self) -> str:
        """Force a token refresh regardless of expiry. Returns new token."""
        with self._lock:
            self._resolve_token()
            return self._access_token  # type: ignore[return-value]

    # -- internals ----------------------------------------------------

    def _is_token_valid(self) -> bool:
        return (
            self._access_token is not None
            and time.time() < self._expires_at - _EXPIRY_MARGIN_SECONDS
        )

    def _resolve_token(self) -> None:
        """Get an access token via refresh flow or direct mode."""
        # If we already know we're in direct mode, skip the refresh attempt
        if self._direct_mode is True:
            self._use_direct_token()
            return

        # Try refresh flow first (Community Management API)
        try:
            self._refresh()
            self._direct_mode = False
        except LinkedInOAuthError as exc:
            # Only fall back to direct mode on auth errors (400/401).
            # Transient server errors (5xx) should propagate so the
            # caller can retry later without permanently locking into
            # the wrong mode.
            is_auth_error = exc.status_code in (400, 401)
            if self._direct_mode is None and is_auth_error:
                logger.info(
                    "LinkedIn refresh token flow failed (%s) — using "
                    "LI_REFRESH_TOKEN as direct access token "
                    "(Share on LinkedIn mode)",
                    exc.status_code,
                )
                self._direct_mode = True
                self._use_direct_token()
            else:
                raise

    def _use_direct_token(self) -> None:
        """Use LI_REFRESH_TOKEN directly as an access token."""
        self._access_token = self._refresh_token
        self._expires_at = time.time() + _DIRECT_TOKEN_TTL_SECONDS
        logger.info(
            "LinkedIn using direct access token (valid ~%d days)",
            _DIRECT_TOKEN_TTL_SECONDS // 86400,
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
                f"Token refresh failed ({resp.status_code}): {resp.text[:300]}",
                status_code=resp.status_code,
            )

        data = resp.json()
        self._access_token = data["access_token"]
        expires_in = int(data.get("expires_in", 3600))
        self._expires_at = time.time() + expires_in
        logger.info("LinkedIn OAuth token refreshed, expires in %ds", expires_in)


class LinkedInOAuthError(Exception):
    """Raised when the LinkedIn OAuth token refresh request fails."""

    def __init__(self, message: str, status_code: int = 0) -> None:
        super().__init__(message)
        self.status_code = status_code


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
