"""
meta_instagram.py — Instagram channel adapter.

Wraps the existing meta_publish.py functions behind the BaseChannel interface.
"""

from __future__ import annotations

import logging

from channels.base import BaseChannel, PublishResult
from config_constants import COL_CAPTION_IG

logger = logging.getLogger(__name__)


class InstagramChannel(BaseChannel):
    CHANNEL_ID = "IG"
    CHANNEL_NAME = "Instagram"
    SUPPORTED_POST_TYPES = ("FEED", "REELS")
    SUPPORTED_MEDIA_TYPES = ("image", "video")
    CAPTION_COLUMN = COL_CAPTION_IG

    def validate(self, post_data: dict) -> list[str]:
        errors = []
        caption = self.get_caption(post_data)
        if not caption:
            errors.append("Missing caption for Instagram")
        cloud_urls = post_data.get("cloud_urls", [])
        if not cloud_urls:
            errors.append("No media URLs provided")
        return errors

    def publish(self, post_data: dict) -> PublishResult:
        from meta_publish import ig_publish_feed, ig_publish_carousel

        caption = self.get_caption(post_data)
        cloud_urls: list[str] = post_data["cloud_urls"]
        mime_types: list[str] = post_data["mime_types"]
        post_type: str = post_data.get("post_type", "FEED")
        is_carousel = len(cloud_urls) > 1

        try:
            if is_carousel:
                platform_id = ig_publish_carousel(cloud_urls, caption, mime_types)
            else:
                platform_id = ig_publish_feed(
                    cloud_urls[0], caption, mime_types[0], post_type,
                )
            return self._make_result(success=True, platform_post_id=platform_id)
        except Exception as exc:
            return self._make_result(
                success=False,
                error_code=self.classify_error(exc),
                error_message=str(exc)[:500],
            )
