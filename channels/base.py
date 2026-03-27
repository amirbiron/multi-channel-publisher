"""
base.py — BaseChannel interface + PublishResult dataclass.

Every publishing channel (IG, FB, GBP, …) subclasses BaseChannel
and implements validate() + publish().
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


@dataclass
class PublishResult:
    """Result of a single-channel publish attempt."""

    channel: str                            # "IG", "FB", "GBP"
    success: bool
    status: str                             # "POSTED" / "ERROR" / "SKIPPED"
    platform_post_id: str | None = None     # platform-side post ID
    error_code: str | None = None           # "timeout", "quota_exceeded", …
    error_message: str | None = None        # human-readable error
    raw_response: dict | None = None        # raw API response (for logs)
    published_at: str | None = None         # ISO-8601 timestamp


class BaseChannel:
    """
    Abstract base for a publishing channel.

    Subclasses MUST set the class-level attributes and implement
    validate() and publish().
    """

    CHANNEL_ID: str = ""                    # "IG", "FB", "GBP"
    CHANNEL_NAME: str = ""                  # "Instagram", "Facebook", …
    SUPPORTED_POST_TYPES: tuple[str, ...] = ()   # ("FEED", "REELS") / ("STANDARD",)
    SUPPORTED_MEDIA_TYPES: tuple[str, ...] = ()  # ("image", "video") / ("image", "none")
    CAPTION_COLUMN: str = ""                # "caption_ig", "caption_fb", …

    # ── interface ──────────────────────────────────────────────

    def validate(self, post_data: dict) -> list[str]:
        """
        Pre-publish validation.

        Returns a list of error strings (empty list = valid).
        post_data keys mirror Sheet column names.
        """
        raise NotImplementedError

    def publish(self, post_data: dict) -> PublishResult:
        """
        Publish to this channel.

        Must return a PublishResult — never raise on expected API errors.
        Unexpected/infrastructure errors may propagate.
        """
        raise NotImplementedError

    # ── helpers ────────────────────────────────────────────────

    def get_caption(self, post_data: dict) -> str:
        """Resolve caption: channel-specific → generic → empty."""
        return post_data.get(self.CAPTION_COLUMN, "") or post_data.get("caption", "")

    def _make_result(
        self,
        *,
        success: bool,
        platform_post_id: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
        raw_response: dict | None = None,
    ) -> PublishResult:
        """Convenience factory — fills channel + timestamp automatically."""
        return PublishResult(
            channel=self.CHANNEL_ID,
            success=success,
            status="POSTED" if success else "ERROR",
            platform_post_id=platform_post_id,
            error_code=error_code,
            error_message=error_message,
            raw_response=raw_response,
            published_at=datetime.now(timezone.utc).isoformat() if success else None,
        )

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self.CHANNEL_ID}>"
