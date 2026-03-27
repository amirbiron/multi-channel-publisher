"""
meta_facebook.py — Facebook Page channel adapter.

Wraps the existing meta_publish.py functions behind the BaseChannel interface.
"""

from __future__ import annotations

import logging

from channels.base import BaseChannel, PublishResult
from config_constants import COL_CAPTION_FB

logger = logging.getLogger(__name__)


class FacebookChannel(BaseChannel):
    CHANNEL_ID = "FB"
    CHANNEL_NAME = "Facebook"
    SUPPORTED_POST_TYPES = ("FEED", "REELS")
    SUPPORTED_MEDIA_TYPES = ("image", "video")
    CAPTION_COLUMN = COL_CAPTION_FB

    def validate(self, post_data: dict) -> list[str]:
        errors = []
        caption = self.get_caption(post_data)
        if not caption:
            errors.append("Missing caption for Facebook")
        cloud_urls = post_data.get("cloud_urls", [])
        if not cloud_urls:
            errors.append("No media URLs provided")
        return errors

    def publish(self, post_data: dict) -> PublishResult:
        from meta_publish import fb_publish_feed

        caption = self.get_caption(post_data)
        cloud_urls: list[str] = post_data["cloud_urls"]
        mime_types: list[str] = post_data["mime_types"]
        post_type: str = post_data.get("post_type", "FEED")
        is_carousel = len(cloud_urls) > 1

        try:
            if is_carousel:
                # FB carousel not fully supported — publish first item only
                logger.info("FB carousel not supported — publishing first item only")
                platform_id = fb_publish_feed(
                    cloud_urls[0], caption, mime_types[0], post_type,
                )
            else:
                platform_id = fb_publish_feed(
                    cloud_urls[0], caption, mime_types[0], post_type,
                )
            return self._make_result(success=True, platform_post_id=platform_id)
        except Exception as exc:
            return self._make_result(
                success=False,
                error_code=_classify_error(exc),
                error_message=str(exc)[:500],
            )


def _classify_error(exc: Exception) -> str:
    """Best-effort error classification for Meta API errors."""
    msg = str(exc).lower()
    if "timeout" in msg:
        return "timeout"
    if "rate" in msg or "limit" in msg:
        return "rate_limit"
    if hasattr(exc, "response") and exc.response is not None:
        return f"http_{exc.response.status_code}"
    return "api_error"
