"""
google_business.py — Google Business Profile channel adapter.

Publishes STANDARD localPosts via the GBP API.
Supports text-only and text+image posts, with optional CTA.
"""

from __future__ import annotations

import logging

import requests

from channels.base import BaseChannel, PublishResult
from config_constants import (
    COL_CAPTION_GBP,
    COL_GOOGLE_LOCATION_ID,
    COL_GBP_POST_TYPE,
    COL_CTA_TYPE,
    COL_CTA_URL,
    GBP_POST_TYPE_STANDARD,
)

logger = logging.getLogger(__name__)

# GBP v4 localPosts endpoint template
_GBP_API_BASE = "https://mybusiness.googleapis.com/v4"


class GoogleBusinessChannel(BaseChannel):
    CHANNEL_ID = "GBP"
    CHANNEL_NAME = "Google Business Profile"
    SUPPORTED_POST_TYPES = ("STANDARD",)
    SUPPORTED_MEDIA_TYPES = ("image", "none")
    CAPTION_COLUMN = COL_CAPTION_GBP

    def validate(self, post_data: dict) -> list[str]:
        errors = []

        # google_location_id is required
        location_id = post_data.get(COL_GOOGLE_LOCATION_ID, "")
        if not location_id:
            errors.append("Missing google_location_id (required for GBP)")

        # gbp_post_type must be STANDARD (MVP)
        gbp_post_type = post_data.get(COL_GBP_POST_TYPE, GBP_POST_TYPE_STANDARD)
        if gbp_post_type and gbp_post_type != GBP_POST_TYPE_STANDARD:
            errors.append(
                f"Unsupported gbp_post_type '{gbp_post_type}'. "
                f"Only STANDARD is supported in this version."
            )

        # Caption is required
        caption = self.get_caption(post_data)
        if not caption:
            errors.append("Missing caption for GBP")

        # If media is provided, only images are allowed
        mime_types = post_data.get("mime_types", [])
        for mt in mime_types:
            if mt and not mt.startswith("image/"):
                errors.append(f"GBP does not support media type '{mt}'. Only images are allowed.")
                break

        return errors

    def publish(self, post_data: dict) -> PublishResult:
        from channels.google_auth import get_oauth_manager
        from config import GBP_ACCOUNT_ID

        location_id = post_data[COL_GOOGLE_LOCATION_ID]
        caption = self.get_caption(post_data)
        cloud_urls: list[str] = post_data.get("cloud_urls", [])
        mime_types: list[str] = post_data.get("mime_types", [])

        # Build localPost body
        body: dict = {
            "languageCode": "he",
            "summary": caption,
            "topicType": "STANDARD",
        }

        # Add image media if available
        if cloud_urls and mime_types:
            first_mime = mime_types[0]
            if first_mime.startswith("image/"):
                body["media"] = [
                    {
                        "mediaFormat": "PHOTO",
                        "sourceUrl": cloud_urls[0],
                    }
                ]

        # Add CTA if provided
        cta_type = post_data.get(COL_CTA_TYPE, "")
        cta_url = post_data.get(COL_CTA_URL, "")
        if cta_type and cta_url:
            body["callToAction"] = {
                "actionType": cta_type,
                "url": cta_url,
            }

        # Construct the API URL
        # location_id can be "locations/123" or just "123"
        loc = location_id if location_id.startswith("locations/") else f"locations/{location_id}"
        url = f"{_GBP_API_BASE}/{GBP_ACCOUNT_ID}/{loc}/localPosts"

        try:
            auth = get_oauth_manager()
            resp = requests.post(
                url,
                json=body,
                headers=auth.get_auth_headers(),
                timeout=30,
            )
            resp.raise_for_status()

            data = resp.json()
            # The API returns the localPost resource with a "name" field
            # e.g. "accounts/123/locations/456/localPosts/789"
            platform_post_id = data.get("name", "")

            return self._make_result(
                success=True,
                platform_post_id=platform_post_id,
                raw_response=data,
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
