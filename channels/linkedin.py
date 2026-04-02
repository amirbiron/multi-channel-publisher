"""
linkedin.py — LinkedIn channel adapter.

Publishes posts via the LinkedIn Community Management API (api.linkedin.com/rest/posts).
Supports text-only, text+image, and text+video posts to personal profiles
and organization pages (determined by the author URN).

OAuth 2.0 three-legged flow with permission: w_member_social.
"""

from __future__ import annotations

import logging

import requests

from channels.base import BaseChannel, PublishResult
from config_constants import COL_CAPTION_LI, COL_LI_AUTHOR_URN

logger = logging.getLogger(__name__)

_LI_API_BASE = "https://api.linkedin.com/rest"

# LinkedIn API version header (required by Community Management API)
_LI_API_VERSION = "202401"


class LinkedInOAuthManager:
    """
    Manages LinkedIn OAuth 2.0 access tokens using a refresh token.

    LinkedIn's three-legged OAuth flow issues refresh tokens that can be
    exchanged for short-lived access tokens.
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

    def get_access_token(self) -> str:
        """Return a valid access token, refreshing if necessary."""
        if self._access_token is None:
            self._refresh()
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
        """Force a token refresh. Returns new token."""
        self._refresh()
        return self._access_token  # type: ignore[return-value]

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
        logger.info("LinkedIn OAuth token refreshed successfully")


class LinkedInOAuthError(Exception):
    """Raised when the LinkedIn OAuth token refresh request fails."""


# ── module-level singleton (lazy) ────────────────────────────

_manager: LinkedInOAuthManager | None = None


def get_li_oauth_manager() -> LinkedInOAuthManager:
    """
    Return the module-level LinkedInOAuthManager singleton.

    Lazily created on first call so that missing env vars don't
    crash the process at import time.
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


class LinkedInChannel(BaseChannel):
    CHANNEL_ID = "LI"
    CHANNEL_NAME = "LinkedIn"
    SUPPORTED_POST_TYPES = ("FEED",)
    SUPPORTED_MEDIA_TYPES = ("image", "video", "none")
    CAPTION_COLUMN = COL_CAPTION_LI

    def validate(self, post_data: dict) -> list[str]:
        errors = []

        # Author URN is required (personal profile or organization page)
        author_urn = post_data.get(COL_LI_AUTHOR_URN, "")
        if not author_urn:
            errors.append("Missing li_author_urn (required for LinkedIn)")

        # Caption is required
        caption = self.get_caption(post_data)
        if not caption:
            errors.append("Missing caption for LinkedIn")

        return errors

    def publish(self, post_data: dict) -> PublishResult:
        author_urn = post_data[COL_LI_AUTHOR_URN]
        caption = self.get_caption(post_data)
        cloud_urls: list[str] = post_data.get("cloud_urls", [])
        mime_types: list[str] = post_data.get("mime_types", [])

        # Build the post body per LinkedIn Community Management API
        body: dict = {
            "author": author_urn,
            "lifecycleState": "PUBLISHED",
            "visibility": "PUBLIC",
            "commentary": caption,
            "distribution": {
                "feedDistribution": "MAIN_FEED",
            },
        }

        # Add media if available
        if cloud_urls and mime_types:
            first_mime = mime_types[0]
            if first_mime.startswith("image/"):
                body["content"] = {
                    "media": {
                        "title": "",
                        "id": cloud_urls[0],
                    },
                }
            elif first_mime.startswith("video/"):
                body["content"] = {
                    "media": {
                        "title": "",
                        "id": cloud_urls[0],
                    },
                }

        url = f"{_LI_API_BASE}/posts"

        try:
            auth = get_li_oauth_manager()
            resp = requests.post(
                url,
                json=body,
                headers=auth.get_auth_headers(),
                timeout=30,
            )
            resp.raise_for_status()

            # LinkedIn returns the post ID in the x-restli-id header
            platform_post_id = resp.headers.get("x-restli-id", "")
            raw = None
            try:
                raw = resp.json()
            except Exception:
                raw = {"status": resp.status_code, "headers": dict(resp.headers)}

            return self._make_result(
                success=True,
                platform_post_id=platform_post_id,
                raw_response=raw,
            )

        except Exception as exc:
            raw = None
            if hasattr(exc, "response") and exc.response is not None:
                try:
                    raw = {"status": exc.response.status_code, "body": exc.response.text[:1000]}
                except Exception:
                    pass
            return self._make_result(
                success=False,
                error_code=self.classify_error(exc),
                error_message=str(exc)[:500],
                raw_response=raw,
            )
