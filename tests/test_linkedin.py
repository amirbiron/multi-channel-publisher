"""
test_linkedin.py — tests for LinkedInChannel + LinkedInOAuthManager.

Covers:
- Validation: missing author URN, invalid URN format, missing caption,
  caption too long, media-only posts
- Publish: text-only, text+image (with upload flow), text+video (with upload flow),
  API errors with LinkedIn-specific error classification
- OAuth: credentials validation, refresh, auth headers
- Registry integration
"""

from unittest.mock import patch, MagicMock, call

import pytest
import requests

from channels.linkedin import LinkedInChannel
from channels.linkedin_auth import LinkedInOAuthManager, LinkedInOAuthError


@pytest.fixture
def channel():
    return LinkedInChannel()


# ===================================================================
#  Class attributes
# ===================================================================

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


# ===================================================================
#  Validation
# ===================================================================

class TestValidation:
    def test_valid_text_only(self, channel):
        data = {
            "li_author_urn": "urn:li:person:abc123",
            "caption_li": "Hello LinkedIn",
        }
        assert channel.validate(data) == []

    def test_valid_organization_urn(self, channel):
        data = {
            "li_author_urn": "urn:li:organization:12345",
            "caption_li": "Org post",
        }
        assert channel.validate(data) == []

    def test_valid_with_generic_caption(self, channel):
        data = {
            "li_author_urn": "urn:li:person:abc123",
            "caption": "Generic caption",
        }
        assert channel.validate(data) == []

    def test_valid_media_only(self, channel):
        """A post with media but no caption should be valid."""
        data = {
            "li_author_urn": "urn:li:person:abc123",
            "cloud_urls": ["https://example.com/img.jpg"],
            "mime_types": ["image/jpeg"],
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

    def test_invalid_urn_format(self, channel):
        data = {"li_author_urn": "not-a-valid-urn", "caption_li": "Hello"}
        errors = channel.validate(data)
        assert any("Invalid li_author_urn format" in e for e in errors)

    def test_invalid_urn_wrong_entity(self, channel):
        data = {"li_author_urn": "urn:li:company:123", "caption_li": "Hello"}
        errors = channel.validate(data)
        assert any("Invalid li_author_urn format" in e for e in errors)

    def test_missing_caption_and_media(self, channel):
        data = {"li_author_urn": "urn:li:person:abc123"}
        errors = channel.validate(data)
        assert any("caption" in e.lower() and "media" in e.lower() for e in errors)

    def test_missing_both_urn_and_content(self, channel):
        errors = channel.validate({})
        assert len(errors) == 2

    def test_caption_too_long(self, channel):
        data = {
            "li_author_urn": "urn:li:person:abc123",
            "caption_li": "A" * 3001,
        }
        errors = channel.validate(data)
        assert any("too long" in e for e in errors)

    def test_caption_at_max_length(self, channel):
        data = {
            "li_author_urn": "urn:li:person:abc123",
            "caption_li": "A" * 3000,
        }
        assert channel.validate(data) == []

    def test_unsupported_mime_type(self, channel):
        """Unsupported MIME types (e.g. PDF) should be rejected."""
        data = {
            "li_author_urn": "urn:li:person:abc123",
            "cloud_urls": ["https://example.com/doc.pdf"],
            "mime_types": ["application/pdf"],
        }
        errors = channel.validate(data)
        assert any("Unsupported media type" in e for e in errors)
        # Also flagged as empty post since PDF isn't valid media
        assert any("caption" in e.lower() for e in errors)

    def test_cloud_urls_without_mime_types(self, channel):
        """cloud_urls with no mime_types should require caption."""
        data = {
            "li_author_urn": "urn:li:person:abc123",
            "cloud_urls": ["https://example.com/file"],
            "mime_types": [],
        }
        errors = channel.validate(data)
        assert any("caption" in e.lower() for e in errors)


# ===================================================================
#  Publish — text only
# ===================================================================

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


# ===================================================================
#  Publish — text + image (with upload flow)
# ===================================================================

class TestPublishWithImage:
    @patch("channels.linkedin.get_li_oauth_manager")
    @patch("channels.linkedin.requests.get")
    @patch("channels.linkedin.requests.put")
    @patch("channels.linkedin.requests.post")
    def test_text_image_upload_flow(self, mock_post, mock_put, mock_get, mock_auth, channel):
        mock_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer fake",
            "LinkedIn-Version": "202401",
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0",
        }

        # Mock initializeUpload response
        init_resp = MagicMock()
        init_resp.status_code = 200
        init_resp.json.return_value = {
            "value": {
                "uploadUrl": "https://www.linkedin.com/dms-uploads/image123",
                "image": "urn:li:image:abc123",
            }
        }
        init_resp.raise_for_status = MagicMock()

        # Mock create-post response
        post_resp = MagicMock()
        post_resp.status_code = 201
        post_resp.headers = {"x-restli-id": "urn:li:share:99999"}
        post_resp.json.return_value = {"id": "urn:li:share:99999"}
        post_resp.raise_for_status = MagicMock()

        # First post call = initializeUpload, second = create post
        mock_post.side_effect = [init_resp, post_resp]

        # Mock downloading the image from cloud storage
        img_resp = MagicMock()
        img_resp.content = b"fake-image-bytes"
        img_resp.raise_for_status = MagicMock()
        mock_get.return_value = img_resp

        # Mock the PUT upload
        upload_resp = MagicMock()
        upload_resp.raise_for_status = MagicMock()
        mock_put.return_value = upload_resp

        data = {
            "li_author_urn": "urn:li:organization:456",
            "caption_li": "Post with image",
            "cloud_urls": ["https://res.cloudinary.com/test/image.jpg"],
            "mime_types": ["image/jpeg"],
        }

        result = channel.publish(data)

        assert result.success is True
        assert result.platform_post_id == "urn:li:share:99999"

        # Verify initializeUpload was called
        init_call = mock_post.call_args_list[0]
        assert "images?action=initializeUpload" in init_call.args[0]

        # Verify image was downloaded
        mock_get.assert_called_once()

        # Verify image was uploaded via PUT
        mock_put.assert_called_once()

        # Verify final post body includes media URN
        create_call = mock_post.call_args_list[1]
        body = create_call.kwargs.get("json") or create_call[1].get("json")
        assert body["content"]["media"]["id"] == "urn:li:image:abc123"


# ===================================================================
#  Publish — text + video (with upload flow)
# ===================================================================

class TestPublishWithVideo:
    @patch("channels.linkedin.get_li_oauth_manager")
    @patch("channels.linkedin.requests.head")
    @patch("channels.linkedin.requests.get")
    @patch("channels.linkedin.requests.put")
    @patch("channels.linkedin.requests.post")
    def test_text_video_upload_flow(self, mock_post, mock_put, mock_get, mock_head, mock_auth, channel):
        mock_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer fake",
            "LinkedIn-Version": "202401",
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0",
        }

        video_content = b"fake-video-bytes-content"

        # Mock HEAD request for file size
        head_resp = MagicMock()
        head_resp.headers = {"Content-Length": str(len(video_content))}
        head_resp.raise_for_status = MagicMock()
        mock_head.return_value = head_resp

        # Mock downloading the video from cloud storage
        vid_resp = MagicMock()
        vid_resp.content = video_content
        vid_resp.raise_for_status = MagicMock()
        mock_get.return_value = vid_resp

        # Mock initializeUpload response
        init_resp = MagicMock()
        init_resp.status_code = 200
        init_resp.json.return_value = {
            "value": {
                "video": "urn:li:video:xyz789",
                "uploadInstructions": [
                    {
                        "uploadUrl": "https://www.linkedin.com/dms-uploads/video789",
                        "firstByte": 0,
                        "lastByte": len(video_content) - 1,
                    }
                ],
            }
        }
        init_resp.raise_for_status = MagicMock()

        # Mock create-post response
        post_resp = MagicMock()
        post_resp.status_code = 201
        post_resp.headers = {"x-restli-id": "urn:li:share:77777"}
        post_resp.json.return_value = {"id": "urn:li:share:77777"}
        post_resp.raise_for_status = MagicMock()

        mock_post.side_effect = [init_resp, post_resp]

        # Mock the PUT upload
        upload_resp = MagicMock()
        upload_resp.raise_for_status = MagicMock()
        mock_put.return_value = upload_resp

        data = {
            "li_author_urn": "urn:li:person:abc",
            "caption_li": "Post with video",
            "cloud_urls": ["https://res.cloudinary.com/test/video.mp4"],
            "mime_types": ["video/mp4"],
        }

        result = channel.publish(data)

        assert result.success is True
        assert result.platform_post_id == "urn:li:share:77777"

        # Verify HEAD request was made to get file size
        mock_head.assert_called_once()

        # Verify initializeUpload was called with file size
        init_call = mock_post.call_args_list[0]
        assert "videos?action=initializeUpload" in init_call.args[0]
        init_body = init_call.kwargs.get("json") or init_call[1].get("json")
        assert init_body["initializeUploadRequest"]["fileSizeBytes"] == len(video_content)

        # Verify video was uploaded via PUT
        mock_put.assert_called_once()

        # Verify final post body includes video URN
        create_call = mock_post.call_args_list[1]
        body = create_call.kwargs.get("json") or create_call[1].get("json")
        assert body["content"]["media"]["id"] == "urn:li:video:xyz789"


# ===================================================================
#  Publish — API error handling
# ===================================================================

class TestPublishErrors:
    @patch("channels.linkedin.get_li_oauth_manager")
    @patch("channels.linkedin.requests.post")
    def test_401_auth_failure(self, mock_post, mock_auth, channel):
        mock_auth.return_value.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        mock_resp.text = "Unauthorized"
        mock_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_resp)
        mock_post.return_value = mock_resp

        data = {"li_author_urn": "urn:li:person:abc", "caption_li": "Will fail"}
        result = channel.publish(data)

        assert result.success is False
        assert result.error_code == "auth_failure"

    @patch("channels.linkedin.get_li_oauth_manager")
    @patch("channels.linkedin.requests.post")
    def test_429_rate_limit(self, mock_post, mock_auth, channel):
        mock_auth.return_value.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
        mock_resp = MagicMock()
        mock_resp.status_code = 429
        mock_resp.text = "Too Many Requests"
        mock_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_resp)
        mock_post.return_value = mock_resp

        data = {"li_author_urn": "urn:li:person:abc", "caption_li": "Will fail"}
        result = channel.publish(data)

        assert result.success is False
        assert result.error_code == "rate_limit"

    @patch("channels.linkedin.get_li_oauth_manager")
    @patch("channels.linkedin.requests.post")
    def test_422_validation_error(self, mock_post, mock_auth, channel):
        mock_auth.return_value.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
        mock_resp = MagicMock()
        mock_resp.status_code = 422
        mock_resp.text = "Unprocessable Entity"
        mock_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_resp)
        mock_post.return_value = mock_resp

        data = {"li_author_urn": "urn:li:person:abc", "caption_li": "Will fail"}
        result = channel.publish(data)

        assert result.success is False
        assert result.error_code == "validation_error"

    @patch("channels.linkedin.get_li_oauth_manager")
    @patch("channels.linkedin.requests.post")
    def test_500_server_error(self, mock_post, mock_auth, channel):
        mock_auth.return_value.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.text = "Internal Server Error"
        mock_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_resp)
        mock_post.return_value = mock_resp

        data = {"li_author_urn": "urn:li:person:abc", "caption_li": "Will fail"}
        result = channel.publish(data)

        assert result.success is False
        assert result.error_code == "http_500"

    @patch("channels.linkedin.get_li_oauth_manager")
    @patch("channels.linkedin.requests.post")
    def test_timeout_error(self, mock_post, mock_auth, channel):
        mock_auth.return_value.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
        mock_post.side_effect = requests.Timeout("Request timeout")

        data = {"li_author_urn": "urn:li:person:abc", "caption_li": "Will timeout"}
        result = channel.publish(data)

        assert result.success is False
        assert result.error_code == "timeout"

    @patch("channels.linkedin.get_li_oauth_manager")
    @patch("channels.linkedin.requests.post")
    def test_generic_api_error(self, mock_post, mock_auth, channel):
        mock_auth.return_value.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
        mock_post.side_effect = ConnectionError("Connection refused")

        data = {"li_author_urn": "urn:li:person:abc", "caption_li": "Will fail"}
        result = channel.publish(data)

        assert result.success is False
        assert result.error_code == "api_error"


# ===================================================================
#  OAuth Manager
# ===================================================================

class TestLinkedInOAuthManager:
    def test_missing_credentials_raises(self):
        with pytest.raises(ValueError, match="LinkedIn OAuth credentials incomplete"):
            LinkedInOAuthManager("", "secret", "token")

    @patch("channels.linkedin_auth.requests.post")
    def test_refresh_success(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"access_token": "new_token"}
        mock_post.return_value = mock_resp

        mgr = LinkedInOAuthManager("cid", "csecret", "rtoken")
        token = mgr.get_access_token()
        assert token == "new_token"

    @patch("channels.linkedin_auth.requests.post")
    def test_refresh_failure_raises(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 400
        mock_resp.text = "invalid_grant"
        mock_post.return_value = mock_resp

        mgr = LinkedInOAuthManager("cid", "csecret", "rtoken")
        with pytest.raises(LinkedInOAuthError):
            mgr.get_access_token()

    @patch("channels.linkedin_auth.requests.post")
    def test_get_auth_headers(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"access_token": "tok123"}
        mock_post.return_value = mock_resp

        mgr = LinkedInOAuthManager("cid", "csecret", "rtoken")
        headers = mgr.get_auth_headers()
        assert headers["Authorization"] == "Bearer tok123"
        assert headers["LinkedIn-Version"] == "202401"
        assert headers["Content-Type"] == "application/json"
        assert headers["X-Restli-Protocol-Version"] == "2.0.0"

    @patch("channels.linkedin_auth.requests.post")
    def test_force_refresh(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"access_token": "forced_token"}
        mock_post.return_value = mock_resp

        mgr = LinkedInOAuthManager("cid", "csecret", "rtoken")
        token = mgr.force_refresh()
        assert token == "forced_token"

    @patch("channels.linkedin_auth.requests.post")
    def test_token_caching(self, mock_post):
        """Token should be cached and not refreshed on every call."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"access_token": "cached_token", "expires_in": 7200}
        mock_post.return_value = mock_resp

        mgr = LinkedInOAuthManager("cid", "csecret", "rtoken")
        token1 = mgr.get_access_token()
        token2 = mgr.get_access_token()
        assert token1 == token2 == "cached_token"
        # Should only have called refresh once
        assert mock_post.call_count == 1


# ===================================================================
#  Registry integration
# ===================================================================

class TestRegistryIntegration:
    def test_li_in_default_registry(self):
        from channels import create_default_registry
        registry = create_default_registry()
        assert "LI" in registry.channel_ids
        ch = registry.get("LI")
        assert ch.CHANNEL_NAME == "LinkedIn"
