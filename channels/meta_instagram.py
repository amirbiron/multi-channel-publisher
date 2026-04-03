"""
meta_instagram.py — Instagram channel adapter.

Wraps the existing meta_publish.py functions behind the BaseChannel interface.
"""

from __future__ import annotations

import logging

from channels.base import BaseChannel, PublishResult
from config_constants import COL_CAPTION_IG, COL_HASHTAGS, COL_FIRST_COMMENT

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
        from config import IG_USER_ID, IG_ACCESS_TOKEN

        if not IG_USER_ID or not IG_ACCESS_TOKEN:
            return self._make_result(
                success=False, error_code="missing_credentials",
                error_message="Instagram credentials not configured (IG_USER_ID, IG_ACCESS_TOKEN)",
            )

        from meta_publish import ig_publish_feed, ig_publish_carousel

        caption = self.get_caption(post_data)
        cloud_urls: list[str] = post_data["cloud_urls"]
        mime_types: list[str] = post_data["mime_types"]
        post_type: str = post_data.get("post_type", "FEED")
        is_carousel = len(cloud_urls) > 1

        # Build first comment text (hashtags go to first comment on IG)
        first_comment = self._build_first_comment(post_data)

        try:
            if is_carousel:
                platform_id = ig_publish_carousel(cloud_urls, caption, mime_types)
            else:
                platform_id = ig_publish_feed(
                    cloud_urls[0], caption, mime_types[0], post_type,
                )

            # Post first comment if provided
            if first_comment:
                self._post_first_comment(platform_id, first_comment)

            return self._make_result(success=True, platform_post_id=platform_id)
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

    @staticmethod
    def _build_first_comment(post_data: dict) -> str:
        """Combine first_comment and hashtags into a single comment text."""
        parts = []
        comment = (post_data.get(COL_FIRST_COMMENT) or "").strip()
        hashtags = (post_data.get(COL_HASHTAGS) or "").strip()
        if comment:
            parts.append(comment)
        if hashtags:
            parts.append(hashtags)
        return "\n\n".join(parts)

    @staticmethod
    def _post_first_comment(media_id: str, text: str) -> None:
        """Post first comment, logging errors without failing the publish."""
        try:
            from meta_publish import ig_post_comment
            ig_post_comment(media_id, text)
        except Exception:
            logger.warning("Failed to post IG first comment on %s", media_id, exc_info=True)
