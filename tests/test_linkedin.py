"""
test_linkedin.py — tests for LinkedInChannel.

Covers:
- Validation: missing author URN, missing caption
- Publish: text-only, text+image, text+video, API errors
- Result includes platform_post_id from x-restli-id header
"""

from unittest.mock import patch, MagicMock

import pytest

import requests

from channels.linkedin import LinkedInChannel, LinkedInOAuthManager, LinkedInOAuthError


@pytest.fixture
def channel():
    return LinkedInChannel()


# ═══════════════════════════════════════════════════════════════
#  Class attributes
# ═══════════════════════════════════════════════════════════════

class TestAttributes:
    def test_channel_id(self, channel):
        assert channel.CHANNEL_ID == "LI"

    def test_channel_name(self, channel):
        assert channel.CHANNEL_NAME == "LinkedIn"

    def test_supported_post_types(self, channel):
        assert channel.SUPPORTED_POST_TYPES == ("FEED",)

    def test_supported_media_types(self, channel):
        assert channel.SUPPORTED_MEDIA_TYPES == ("image", "video", "none")

    def test_caption_column(self, channel):
        assert channel.CAPTION_COLUMN == "caption_li"


# ═══════════════════════════════════════════════════════════════
#  Validation
# ═══════════════════════════════════════════════════════════════

class TestValidation:
    def test_valid_text_only(self, channel):
        data = {
            "li_author_urn": "urn:li:person:abc123",
            "caption_li": "Hello LinkedIn",
        }
        assert channel.validate(data) == []

    def test_valid_with_generic_caption(self, channel):
        data = {
            "li_author_urn": "urn:li:person:abc123",
            "caption": "Generic caption",
        }
        assert channel.validate(data) == []

    def test_missing_author_urn(self, channel):
        data = {"caption_li": "Hello"}
        errors = channel.validate(data)
        assert any("li_author_urn" in e for e in errors)

    def test_empty_author_urn(self, channel):
        data = {"li_author_urn": "", "caption_li": "Hello"}
        errors = channel.validate(data)
        assert any("li_author_urn" in e for e in errors)

    def test_missing_caption(self, channel):
        data = {"li_author_urn": "urn:li:person:abc123"}
        errors = channel.validate(data)
        assert any("caption" in e.lower() for e in errors)

    def test_missing_both(self, channel):
        errors = channel.validate({})
        assert len(errors) == 2


# ═══════════════════════════════════════════════════════════════
#  Publish — text only
# ═══════════════════════════════════════════════════════════════

class TestPublishTextOnly:
    @patch("channels.linkedin.get_li_oauth_manager")
    @patch("channels.linkedin.requests.post")
    def test_text_only_success(self, mock_post, mock_auth, channel):
        mock_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer fake",
            "LinkedIn-Version": "202401",
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0",
        }
        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_resp.headers = {"x-restli-id": "urn:li:share:12345"}
        mock_resp.json.return_value = {"id": "urn:li:share:12345"}
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        data = {
            "li_author_urn": "urn:li:person:abc123",
            "caption_li": "Text only post",
        }

        result = channel.publish(data)

        assert result.success is True
        assert result.platform_post_id == "urn:li:share:12345"
        assert result.status == "POSTED"
        assert result.published_at is not None

        # Verify request body
        call_kwargs = mock_post.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert body["author"] == "urn:li:person:abc123"
        assert body["commentary"] == "Text only post"
        assert body["lifecycleState"] == "PUBLISHED"
        assert body["visibility"] == "PUBLIC"
        assert "content" not in body


# ═══════════════════════════════════════════════════════════════
#  Publish — text + image
# ═══════════════════════════════════════════════════════════════

class TestPublishWithImage:
    @patch("channels.linkedin.get_li_oauth_manager")
    @patch("channels.linkedin.requests.post")
    def test_text_image_success(self, mock_post, mock_auth, channel):
        mock_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer fake",
        }
        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_resp.headers = {"x-restli-id": "urn:li:share:99999"}
        mock_resp.json.return_value = {"id": "urn:li:share:99999"}
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        data = {
            "li_author_urn": "urn:li:organization:456",
            "caption_li": "Post with image",
            "cloud_urls": ["https://res.cloudinary.com/test/image.jpg"],
            "mime_types": ["image/jpeg"],
        }

        result = channel.publish(data)

        assert result.success is True

        call_kwargs = mock_post.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert "content" in body
        assert body["content"]["media"][0]["id"] == "https://res.cloudinary.com/test/image.jpg"


# ═══════════════════════════════════════════════════════════════
#  Publish — text + video
# ═══════════════════════════════════════════════════════════════

class TestPublishWithVideo:
    @patch("channels.linkedin.get_li_oauth_manager")
    @patch("channels.linkedin.requests.post")
    def test_text_video_success(self, mock_post, mock_auth, channel):
        mock_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer fake",
        }
        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_resp.headers = {"x-restli-id": "urn:li:share:77777"}
        mock_resp.json.return_value = {"id": "urn:li:share:77777"}
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        data = {
            "li_author_urn": "urn:li:person:abc",
            "caption_li": "Post with video",
            "cloud_urls": ["https://res.cloudinary.com/test/video.mp4"],
            "mime_types": ["video/mp4"],
        }

        result = channel.publish(data)

        assert result.success is True
        call_kwargs = mock_post.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert "content" in body


# ═══════════════════════════════════════════════════════════════
#  Publish — API error handling
# ═══════════════════════════════════════════════════════════════

class TestPublishErrors:
    @patch("channels.linkedin.get_li_oauth_manager")
    @patch("channels.linkedin.requests.post")
    def test_api_error(self, mock_post, mock_auth, channel):
        mock_auth.return_value.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.text = "Forbidden"
        mock_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_resp)
        mock_post.return_value = mock_resp

        data = {
            "li_author_urn": "urn:li:person:abc",
            "caption_li": "Will fail",
        }

        result = channel.publish(data)

        assert result.success is False
        assert result.status == "ERROR"
        assert result.error_code == "http_403"
        assert result.raw_response is not None

    @patch("channels.linkedin.get_li_oauth_manager")
    @patch("channels.linkedin.requests.post")
    def test_timeout_error(self, mock_post, mock_auth, channel):
        mock_auth.return_value.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
        mock_post.side_effect = requests.Timeout("Request timeout")

        data = {
            "li_author_urn": "urn:li:person:abc",
            "caption_li": "Will timeout",
        }

        result = channel.publish(data)

        assert result.success is False
        assert result.error_code == "timeout"


# ═══════════════════════════════════════════════════════════════
#  OAuth Manager
# ═══════════════════════════════════════════════════════════════

class TestLinkedInOAuthManager:
    def test_missing_credentials_raises(self):
        with pytest.raises(ValueError, match="LinkedIn OAuth credentials incomplete"):
            LinkedInOAuthManager("", "secret", "token")

    @patch("channels.linkedin.requests.post")
    def test_refresh_success(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"access_token": "new_token"}
        mock_post.return_value = mock_resp

        mgr = LinkedInOAuthManager("cid", "csecret", "rtoken")
        token = mgr.get_access_token()
        assert token == "new_token"

    @patch("channels.linkedin.requests.post")
    def test_refresh_failure_raises(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 400
        mock_resp.text = "invalid_grant"
        mock_post.return_value = mock_resp

        mgr = LinkedInOAuthManager("cid", "csecret", "rtoken")
        with pytest.raises(LinkedInOAuthError):
            mgr.get_access_token()

    @patch("channels.linkedin.requests.post")
    def test_get_auth_headers(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"access_token": "tok123"}
        mock_post.return_value = mock_resp

        mgr = LinkedInOAuthManager("cid", "csecret", "rtoken")
        headers = mgr.get_auth_headers()
        assert headers["Authorization"] == "Bearer tok123"
        assert "LinkedIn-Version" in headers


# ═══════════════════════════════════════════════════════════════
#  Registry integration
# ═══════════════════════════════════════════════════════════════

class TestRegistryIntegration:
    def test_li_in_default_registry(self):
        from channels import create_default_registry
        registry = create_default_registry()
        assert "LI" in registry.channel_ids
        ch = registry.get("LI")
        assert ch.CHANNEL_NAME == "LinkedIn"
