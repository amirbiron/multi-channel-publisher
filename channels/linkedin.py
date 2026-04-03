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
from channels.linkedin_auth import get_li_oauth_manager
from config_constants import (
    COL_CAPTION_LI, COL_FIRST_COMMENT, COL_HASHTAGS,
    COL_LI_AUTHOR_URN, LI_CAPTION_MAX_LENGTH, LI_URN_PATTERN,
)

logger = logging.getLogger(__name__)

_LI_API_BASE = "https://api.linkedin.com/rest"

class LinkedInChannel(BaseChannel):
    CHANNEL_ID = "LI"
    CHANNEL_NAME = "LinkedIn"
    SUPPORTED_POST_TYPES = ("FEED",)
    SUPPORTED_MEDIA_TYPES = ("image", "video", "none")
    CAPTION_COLUMN = COL_CAPTION_LI

    # -- validation ---------------------------------------------------

    def validate(self, post_data: dict) -> list[str]:
        errors: list[str] = []

        # Author URN is required (personal profile or organization page)
        author_urn = post_data.get(COL_LI_AUTHOR_URN, "")
        if not author_urn:
            errors.append("Missing li_author_urn (required for LinkedIn)")
        elif not LI_URN_PATTERN.match(author_urn):
            errors.append(
                f"Invalid li_author_urn format: '{author_urn}'. "
                f"Expected urn:li:person:{{id}} or urn:li:organization:{{id}}"
            )

        # Must have caption or supported media (not an empty post)
        caption = self.get_caption(post_data)
        cloud_urls: list[str] = post_data.get("cloud_urls", [])
        mime_types: list[str] = post_data.get("mime_types", [])

        # Only image/* and video/* are supported by LinkedIn
        has_supported_media = bool(
            cloud_urls
            and mime_types
            and any(
                m.startswith("image/") or m.startswith("video/")
                for m in mime_types
            )
        )

        if not caption and not has_supported_media:
            errors.append("Missing caption and media for LinkedIn (post cannot be empty)")

        # Warn about unsupported MIME types
        if cloud_urls and mime_types:
            for mt in mime_types:
                if mt and not mt.startswith("image/") and not mt.startswith("video/"):
                    errors.append(
                        f"Unsupported media type '{mt}' for LinkedIn. "
                        f"Only image/* and video/* are supported."
                    )

        # Caption length check
        if caption and len(caption) > LI_CAPTION_MAX_LENGTH:
            errors.append(
                f"LinkedIn caption too long ({len(caption)} chars). "
                f"Maximum is {LI_CAPTION_MAX_LENGTH} characters."
            )

        return errors

    # -- publishing ---------------------------------------------------

    def publish(self, post_data: dict) -> PublishResult:
        author_urn = post_data[COL_LI_AUTHOR_URN]
        caption = self.get_caption(post_data)
        cloud_urls: list[str] = post_data.get("cloud_urls", [])
        mime_types: list[str] = post_data.get("mime_types", [])

        try:
            auth = get_li_oauth_manager()
            headers = auth.get_auth_headers()

            # Determine media type and upload if needed
            media_urn: str | None = None
            if cloud_urls and mime_types:
                first_mime = mime_types[0]
                first_url = cloud_urls[0]

                if first_mime.startswith("image/"):
                    media_urn = self._upload_image(
                        author_urn, first_url, headers
                    )
                elif first_mime.startswith("video/"):
                    media_urn = self._upload_video(
                        author_urn, first_url, headers
                    )

            # Build the post body per LinkedIn Community Management API
            body: dict = {
                "author": author_urn,
                "lifecycleState": "PUBLISHED",
                "visibility": "PUBLIC",
                "distribution": {
                    "feedDistribution": "MAIN_FEED",
                },
            }

            # Only include commentary when non-empty (LinkedIn API treats
            # empty string differently from omitting the field)
            if caption:
                body["commentary"] = caption

            # Attach media if uploaded
            if media_urn:
                body["content"] = {
                    "media": {
                        "id": media_urn,
                    },
                }

            url = f"{_LI_API_BASE}/posts"
            resp = requests.post(
                url,
                json=body,
                headers=headers,
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

            # Post first comment (hashtags go to first comment on LinkedIn)
            first_comment = self._build_first_comment(post_data)
            if first_comment and platform_post_id:
                self._post_first_comment(
                    platform_post_id, author_urn, first_comment, headers,
                )

            return self._make_result(
                success=True,
                platform_post_id=platform_post_id,
                raw_response=raw,
            )

        except Exception as exc:
            error_code = self._classify_linkedin_error(exc)
            raw = None
            if hasattr(exc, "response") and exc.response is not None:
                try:
                    raw = {"status": exc.response.status_code, "body": exc.response.text[:1000]}
                except Exception:
                    pass
            return self._make_result(
                success=False,
                error_code=error_code,
                error_message=str(exc)[:500],
                raw_response=raw,
            )

    # -- image upload -------------------------------------------------

    def _upload_image(
        self, author_urn: str, image_url: str, headers: dict[str, str]
    ) -> str:
        """
        Upload an image to LinkedIn via the Images API.

        1. Initialize upload via POST /rest/images?action=initializeUpload
        2. Upload binary to the provided uploadUrl
        3. Return the image URN for use in the post
        """
        # Step 1: Initialize upload
        init_resp = requests.post(
            f"{_LI_API_BASE}/images?action=initializeUpload",
            json={
                "initializeUploadRequest": {
                    "owner": author_urn,
                }
            },
            headers=headers,
            timeout=30,
        )
        init_resp.raise_for_status()
        init_data = init_resp.json()

        upload_url = init_data["value"]["uploadUrl"]
        image_urn = init_data["value"]["image"]

        # Step 2: Download the image from cloud storage and upload to LinkedIn
        img_data = requests.get(image_url, timeout=60)
        img_data.raise_for_status()

        upload_headers = {
            "Authorization": headers["Authorization"],
        }
        upload_resp = requests.put(
            upload_url,
            data=img_data.content,
            headers=upload_headers,
            timeout=120,
        )
        upload_resp.raise_for_status()

        logger.info("LinkedIn image uploaded: %s", image_urn)
        return image_urn

    # -- video upload -------------------------------------------------

    def _upload_video(
        self, author_urn: str, video_url: str, headers: dict[str, str]
    ) -> str:
        """
        Upload a video to LinkedIn via the Videos API.

        1. Initialize upload via POST /rest/videos?action=initializeUpload
        2. Upload binary to the provided uploadUrl (single chunk for small files)
        3. Return the video URN for use in the post
        """
        # Step 1: Get file size via HEAD request (avoids loading into memory)
        head_resp = requests.head(video_url, timeout=30, allow_redirects=True)
        head_resp.raise_for_status()
        file_size = int(head_resp.headers.get("Content-Length", 0))

        if not file_size:
            # Fallback: download to get size if HEAD didn't return Content-Length
            vid_data = requests.get(video_url, timeout=120)
            vid_data.raise_for_status()
            file_size = len(vid_data.content)
        else:
            vid_data = None  # will download later per chunk

        # Step 2: Initialize upload
        init_resp = requests.post(
            f"{_LI_API_BASE}/videos?action=initializeUpload",
            json={
                "initializeUploadRequest": {
                    "owner": author_urn,
                    "fileSizeBytes": file_size,
                }
            },
            headers=headers,
            timeout=30,
        )
        init_resp.raise_for_status()
        init_data = init_resp.json()

        video_urn = init_data["value"]["video"]
        upload_instructions = init_data["value"]["uploadInstructions"]

        # Step 3: Download video content if not already fetched
        if vid_data is None:
            vid_data = requests.get(video_url, timeout=120)
            vid_data.raise_for_status()

        # Step 4: Upload chunks (usually single chunk for small files)
        upload_headers = {
            "Authorization": headers["Authorization"],
        }
        for instruction in upload_instructions:
            upload_url = instruction["uploadUrl"]
            first_byte = instruction.get("firstByte", 0)
            last_byte = instruction.get("lastByte", file_size - 1)

            chunk = vid_data.content[first_byte : last_byte + 1]
            upload_resp = requests.put(
                upload_url,
                data=chunk,
                headers=upload_headers,
                timeout=300,
            )
            upload_resp.raise_for_status()

        logger.info("LinkedIn video uploaded: %s (%d bytes)", video_urn, file_size)
        return video_urn

    # -- first comment ------------------------------------------------

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

    def _post_first_comment(
        self,
        post_id: str,
        author_urn: str,
        text: str,
        headers: dict[str, str],
    ) -> None:
        """Post a comment on a LinkedIn post. Logs errors without failing."""
        try:
            body = {
                "actor": author_urn,
                "object": f"urn:li:activity:{post_id}",
                "message": {"text": text},
            }
            resp = requests.post(
                f"{_LI_API_BASE}/socialActions/{post_id}/comments",
                json=body,
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            logger.info("LinkedIn first comment posted on %s", post_id)
        except Exception:
            logger.warning(
                "Failed to post LinkedIn first comment on %s",
                post_id, exc_info=True,
            )

    # -- error classification -----------------------------------------

    @staticmethod
    def _classify_linkedin_error(exc: Exception) -> str:
        """Classify LinkedIn API errors into specific error codes."""
        # Check HTTP status code first (before string matching, since
        # e.g. a 504 message contains "timeout" but should map to http_504)
        if hasattr(exc, "response") and exc.response is not None:
            status = exc.response.status_code
            if status == 401:
                return "auth_failure"
            if status == 422:
                return "validation_error"
            if status == 429:
                return "rate_limit"
            return f"http_{status}"

        if "timeout" in str(exc).lower():
            return "timeout"

        return "api_error"
