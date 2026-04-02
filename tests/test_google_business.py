"""
test_google_business.py — tests for GoogleBusinessChannel.

Covers:
- Validation: missing location, unsupported post type, missing caption, bad media
- Publish: text-only, text+image, CTA, API errors
- Result includes platform_post_id
"""

from unittest.mock import patch, MagicMock

import pytest

import requests

from channels.google_business import GoogleBusinessChannel


@pytest.fixture
def channel():
    return GoogleBusinessChannel()


# ═══════════════════════════════════════════════════════════════
#  Class attributes
# ═══════════════════════════════════════════════════════════════

class TestAttributes:
    def test_channel_id(self, channel):
        assert channel.CHANNEL_ID == "GBP"

    def test_supported_post_types(self, channel):
        assert channel.SUPPORTED_POST_TYPES == ("STANDARD",)

    def test_supported_media_types(self, channel):
        assert channel.SUPPORTED_MEDIA_TYPES == ("image", "none")


# ═══════════════════════════════════════════════════════════════
#  Validation
# ═══════════════════════════════════════════════════════════════

class TestValidation:
    def test_valid_text_only(self, channel):
        data = {
            "google_location_id": "locations/123",
            "caption_gbp": "Hello GBP",
        }
        assert channel.validate(data) == []

    def test_valid_text_with_image(self, channel):
        data = {
            "google_location_id": "locations/123",
            "caption_gbp": "Hello GBP",
            "cloud_urls": ["https://example.com/img.jpg"],
            "mime_types": ["image/jpeg"],
        }
        assert channel.validate(data) == []

    def test_missing_location_id(self, channel):
        data = {"caption_gbp": "Hello GBP"}
        errors = channel.validate(data)
        assert any("google_location_id" in e for e in errors)

    def test_empty_location_id(self, channel):
        data = {"google_location_id": "", "caption_gbp": "Hello"}
        errors = channel.validate(data)
        assert any("google_location_id" in e for e in errors)

    def test_unsupported_post_type(self, channel):
        data = {
            "google_location_id": "locations/123",
            "caption_gbp": "Hello",
            "gbp_post_type": "EVENT",
        }
        errors = channel.validate(data)
        assert any("STANDARD" in e for e in errors)

    def test_standard_post_type_ok(self, channel):
        data = {
            "google_location_id": "locations/123",
            "caption_gbp": "Hello",
            "gbp_post_type": "STANDARD",
        }
        assert channel.validate(data) == []

    def test_missing_caption(self, channel):
        data = {"google_location_id": "locations/123"}
        errors = channel.validate(data)
        assert any("caption" in e.lower() for e in errors)

    def test_fallback_to_generic_caption(self, channel):
        data = {
            "google_location_id": "locations/123",
            "caption": "Generic caption",
        }
        assert channel.validate(data) == []

    def test_video_media_rejected(self, channel):
        data = {
            "google_location_id": "locations/123",
            "caption_gbp": "Hello",
            "mime_types": ["video/mp4"],
        }
        errors = channel.validate(data)
        assert any("video/mp4" in e for e in errors)
        assert any("does not support video" in e.lower() for e in errors)

    def test_cta_type_without_url_ok_at_channel_level(self, channel):
        """Channel validate() does not check CTA — that's done by the validator."""
        data = {
            "google_location_id": "locations/123",
            "caption_gbp": "Hello",
            "cta_type": "LEARN_MORE",
            # no cta_url
        }
        # Channel-level validation doesn't enforce CTA consistency
        # (validator.py handles this at a higher level)
        errors = channel.validate(data)
        assert not any("cta" in e.lower() for e in errors)


# ═══════════════════════════════════════════════════════════════
#  Publish — text only
# ═══════════════════════════════════════════════════════════════

class TestPublishTextOnly:
    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.google_business.requests.post")
    def test_text_only_success(self, mock_post, mock_auth, channel):
        mock_auth.return_value.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "name": "accounts/123/locations/456/localPosts/789",
        }
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        data = {
            "google_location_id": "locations/456",
            "caption_gbp": "Text only post",
        }

        with patch("config.GBP_ACCOUNT_ID", "accounts/123"):
            result = channel.publish(data)

        assert result.success is True
        assert result.platform_post_id == "accounts/123/locations/456/localPosts/789"
        assert result.status == "POSTED"
        assert result.published_at is not None

        # Verify request body has no media
        call_kwargs = mock_post.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert "media" not in body
        assert body["topicType"] == "STANDARD"
        assert body["summary"] == "Text only post"


# ═══════════════════════════════════════════════════════════════
#  Publish — text + image
# ═══════════════════════════════════════════════════════════════

class TestPublishWithImage:
    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.google_business.requests.post")
    def test_text_image_success(self, mock_post, mock_auth, channel):
        mock_auth.return_value.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "name": "accounts/123/locations/456/localPosts/999",
        }
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        data = {
            "google_location_id": "locations/456",
            "caption_gbp": "Post with image",
            "cloud_urls": ["https://res.cloudinary.com/test/image.jpg"],
            "mime_types": ["image/jpeg"],
        }

        with patch("config.GBP_ACCOUNT_ID", "accounts/123"):
            result = channel.publish(data)

        assert result.success is True
        assert result.platform_post_id == "accounts/123/locations/456/localPosts/999"

        call_kwargs = mock_post.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert "media" in body
        assert body["media"][0]["mediaFormat"] == "PHOTO"
        assert body["media"][0]["sourceUrl"] == "https://res.cloudinary.com/test/image.jpg"


# ═══════════════════════════════════════════════════════════════
#  Publish — with CTA
# ═══════════════════════════════════════════════════════════════

class TestPublishWithCTA:
    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.google_business.requests.post")
    def test_cta_included(self, mock_post, mock_auth, channel):
        mock_auth.return_value.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"name": "accounts/1/locations/2/localPosts/3"}
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        data = {
            "google_location_id": "locations/2",
            "caption_gbp": "Check this out",
            "cta_type": "LEARN_MORE",
            "cta_url": "https://example.com",
        }

        with patch("config.GBP_ACCOUNT_ID", "accounts/1"):
            result = channel.publish(data)

        assert result.success is True

        call_kwargs = mock_post.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert body["callToAction"]["actionType"] == "LEARN_MORE"
        assert body["callToAction"]["url"] == "https://example.com"

    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.google_business.requests.post")
    def test_no_cta_when_empty(self, mock_post, mock_auth, channel):
        mock_auth.return_value.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"name": "accounts/1/locations/2/localPosts/3"}
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        data = {
            "google_location_id": "locations/2",
            "caption_gbp": "No CTA here",
        }

        with patch("config.GBP_ACCOUNT_ID", "accounts/1"):
            channel.publish(data)

        call_kwargs = mock_post.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert "callToAction" not in body


# ═══════════════════════════════════════════════════════════════
#  Publish — API error handling
# ═══════════════════════════════════════════════════════════════

class TestPublishErrors:
    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.google_business.requests.post")
    def test_api_error(self, mock_post, mock_auth, channel):
        mock_auth.return_value.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.text = "Quota exceeded"
        mock_resp.raise_for_status.side_effect = requests.HTTPError(
            response=mock_resp
        )
        mock_post.return_value = mock_resp

        data = {
            "google_location_id": "locations/456",
            "caption_gbp": "Will fail",
        }

        with patch("config.GBP_ACCOUNT_ID", "accounts/123"):
            result = channel.publish(data)

        assert result.success is False
        assert result.status == "ERROR"
        assert result.error_code == "http_403"
        assert result.raw_response is not None

    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.google_business.requests.post")
    def test_timeout_error(self, mock_post, mock_auth, channel):
        mock_auth.return_value.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
        mock_post.side_effect = requests.Timeout("Request timeout")

        data = {
            "google_location_id": "locations/456",
            "caption_gbp": "Will timeout",
        }

        with patch("config.GBP_ACCOUNT_ID", "accounts/123"):
            result = channel.publish(data)

        assert result.success is False
        assert result.error_code == "timeout"

    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.google_business.requests.post")
    def test_auth_error_401(self, mock_post, mock_auth, channel):
        mock_auth.return_value.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        mock_resp.text = "Unauthorized"
        mock_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_resp)
        mock_post.return_value = mock_resp

        data = {
            "google_location_id": "locations/456",
            "caption_gbp": "Will fail auth",
        }

        with patch("config.GBP_ACCOUNT_ID", "accounts/123"):
            result = channel.publish(data)

        assert result.success is False
        assert result.error_code == "http_401"

    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.google_business.requests.post")
    def test_connection_error(self, mock_post, mock_auth, channel):
        mock_auth.return_value.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
        mock_post.side_effect = requests.ConnectionError("Connection refused")

        data = {
            "google_location_id": "locations/456",
            "caption_gbp": "Will fail connect",
        }

        with patch("config.GBP_ACCOUNT_ID", "accounts/123"):
            result = channel.publish(data)

        assert result.success is False
        assert result.error_message is not None


# ═══════════════════════════════════════════════════════════════
#  Publish — location_id normalization
# ═══════════════════════════════════════════════════════════════

class TestLocationIdHandling:
    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.google_business.requests.post")
    def test_bare_location_id(self, mock_post, mock_auth, channel):
        """location_id without 'locations/' prefix should still work."""
        mock_auth.return_value.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"name": "accounts/1/locations/2/localPosts/3"}
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        data = {
            "google_location_id": "456",  # bare ID
            "caption_gbp": "Test",
        }

        with patch("config.GBP_ACCOUNT_ID", "accounts/123"):
            channel.publish(data)

        call_args = mock_post.call_args
        url = call_args[0][0] if call_args[0] else call_args.kwargs.get("url", "")
        assert "locations/456" in url

    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.google_business.requests.post")
    def test_prefixed_location_id(self, mock_post, mock_auth, channel):
        """location_id with 'locations/' prefix should not be doubled."""
        mock_auth.return_value.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"name": "accounts/1/locations/456/localPosts/3"}
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        data = {
            "google_location_id": "locations/456",
            "caption_gbp": "Test",
        }

        with patch("config.GBP_ACCOUNT_ID", "accounts/123"):
            channel.publish(data)

        call_args = mock_post.call_args
        url = call_args[0][0] if call_args[0] else call_args.kwargs.get("url", "")
        assert "locations/locations/" not in url
        assert "locations/456" in url

    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.google_business.requests.post")
    def test_full_resource_path_location_id(self, mock_post, mock_auth, channel):
        """Full resource path 'accounts/X/locations/Y' should not produce malformed URL."""
        mock_auth.return_value.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"name": "accounts/1/locations/456/localPosts/3"}
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        data = {
            "google_location_id": "accounts/999/locations/456",
            "caption_gbp": "Test",
        }

        with patch("config.GBP_ACCOUNT_ID", "accounts/123"):
            channel.publish(data)

        call_args = mock_post.call_args
        url = call_args[0][0] if call_args[0] else call_args.kwargs.get("url", "")
        assert "locations/accounts/" not in url
        assert "locations/456" in url


# ═══════════════════════════════════════════════════════════════
#  Registry integration
# ═══════════════════════════════════════════════════════════════

class TestRegistryIntegration:
    def test_gbp_in_default_registry(self):
        from channels import create_default_registry
        registry = create_default_registry()
        assert "GBP" in registry.channel_ids
        ch = registry.get("GBP")
        assert ch.CHANNEL_NAME == "Google Business Profile"
