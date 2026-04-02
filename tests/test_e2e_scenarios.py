"""
test_e2e_scenarios.py — End-to-end scenario tests for Multi-Channel Publisher.

Covers the full pipeline: validation → media processing → publishing → status update.
All external I/O (Google Sheets, Drive, Cloudinary, Meta API, GBP API) is mocked.

Scenarios:
1. IG+FB only (regression)
2. GBP text only
3. GBP text + image
4. IG+FB+GBP with GBP failure → PARTIAL
5. Retry GBP after PARTIAL
6. Location invalid → GBP blocked
7. Missing caption_gbp with fallback to generic caption
"""

from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import pytest

from config_constants import (
    STATUS_READY,
    STATUS_PROCESSING,
    STATUS_POSTED,
    STATUS_PARTIAL,
    STATUS_ERROR,
)
from main import (
    process_row,
    process_partial_row,
    _RUN_ID,
)

# ─── Header matching SHEET_COLUMNS ──────────────────────────────
HEADER = [
    "id", "status", "network", "post_type", "publish_at",
    "caption", "caption_ig", "caption_fb", "caption_gbp",
    "caption_li", "li_author_urn",
    "gbp_post_type", "cta_type", "cta_url", "google_location_id",
    "drive_file_id", "cloudinary_url", "source",
    "result", "error",
    "retry_count", "locked_at", "processing_by",
    "published_channels", "failed_channels",
]


def _build_row(
    row_id="1",
    status=STATUS_READY,
    network="IG+FB",
    post_type="FEED",
    publish_at="2026-03-22 10:00",
    caption="",
    caption_ig="",
    caption_fb="",
    caption_gbp="",
    caption_li="",
    li_author_urn="",
    gbp_post_type="",
    cta_type="",
    cta_url="",
    google_location_id="",
    drive_file_id="abc123",
    cloudinary_url="",
    source="",
    result="",
    error="",
    retry_count="",
    locked_at="",
    processing_by="",
    published_channels="",
    failed_channels="",
):
    """Build a row list matching HEADER order."""
    return [
        row_id, status, network, post_type, publish_at,
        caption, caption_ig, caption_fb, caption_gbp,
        caption_li, li_author_urn,
        gbp_post_type, cta_type, cta_url, google_location_id,
        drive_file_id, cloudinary_url, source,
        result, error,
        retry_count, locked_at, processing_by,
        published_channels, failed_channels,
    ]


def _locked_row(**kwargs):
    """Build a row with PROCESSING status and current run ID for lock verification."""
    kwargs.setdefault("status", STATUS_PROCESSING)
    kwargs.setdefault("processing_by", _RUN_ID)
    return _build_row(**kwargs)


# Standard mocks for Drive → normalize → Cloudinary pipeline
_DRIVE_RETURN = (b"fake-img", {"mimeType": "image/jpeg", "name": "pic.jpg"})
_CLOUD_URL = "https://res.cloudinary.com/x/image/upload/v1/social-publisher/abc.jpg"
_NORM_PASSTHROUGH = lambda b, m, n, p, *a: (b, m, n)  # noqa: E731


def _mock_gbp_success():
    """Create a mock GBP API response for successful publish."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "name": "accounts/123/locations/456/localPosts/789"
    }
    mock_resp.raise_for_status = MagicMock()
    return mock_resp


def _mock_gbp_failure(status_code=500, text="Internal Server Error"):
    """Create a mock GBP API response for failed publish."""
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.text = text
    mock_resp.raise_for_status.side_effect = Exception(
        f"{status_code} {text}"
    )
    return mock_resp


# ═══════════════════════════════════════════════════════════════
#  Scenario 1: IG+FB only (regression)
# ═══════════════════════════════════════════════════════════════

class TestE2E_Scenario1_IG_FB_Only:
    """Regression: publishing to IG+FB should work exactly as before GBP was added."""

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value=_CLOUD_URL)
    @patch("main.normalize_media", side_effect=_NORM_PASSTHROUGH)
    @patch("main.validate_media_pre_publish", return_value=None)
    @patch("main.drive_download_with_metadata", return_value=_DRIVE_RETURN)
    @patch("meta_publish.fb_publish_feed", return_value="fb_post_001")
    @patch("meta_publish.ig_publish_feed", return_value="ig_media_001")
    def test_ig_fb_both_succeed(self, mock_ig, mock_fb, mock_drive, _mock_vmp, mock_norm,
                                 mock_cloud, mock_sheets, mock_reread):
        row = _build_row(
            network="IG+FB", caption_ig="IG text", caption_fb="FB text",
        )
        mock_reread.return_value = _locked_row(
            network="IG+FB", caption_ig="IG text", caption_fb="FB text",
        )

        result = process_row(row, HEADER, 2)

        assert result is True
        mock_ig.assert_called_once()
        mock_fb.assert_called_once()

        # Verify status → POSTED
        final_update = mock_sheets.call_args_list[-1][0][1]
        assert final_update["status"] == STATUS_POSTED
        assert "IG:POSTED:ig_media_001" in final_update["result"]
        assert "FB:POSTED:fb_post_001" in final_update["result"]
        assert final_update["published_channels"] == "IG,FB"
        assert final_update["failed_channels"] == ""

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value=_CLOUD_URL)
    @patch("main.normalize_media", side_effect=_NORM_PASSTHROUGH)
    @patch("main.validate_media_pre_publish", return_value=None)
    @patch("main.drive_download_with_metadata", return_value=_DRIVE_RETURN)
    @patch("meta_publish.ig_publish_feed", return_value="ig_solo_001")
    def test_ig_only_regression(self, mock_ig, mock_drive, _mock_vmp, mock_norm,
                                 mock_cloud, mock_sheets, mock_reread):
        """Single IG post — legacy behavior should be unchanged."""
        row = _build_row(network="IG", caption_ig="IG only")
        mock_reread.return_value = _locked_row(network="IG", caption_ig="IG only")

        process_row(row, HEADER, 2)

        mock_ig.assert_called_once()
        final_update = mock_sheets.call_args_list[-1][0][1]
        assert final_update["status"] == STATUS_POSTED
        # Single channel → result is just the platform ID
        assert final_update["result"] == "ig_solo_001"


# ═══════════════════════════════════════════════════════════════
#  Scenario 2: GBP text only
# ═══════════════════════════════════════════════════════════════

class TestE2E_Scenario2_GBP_TextOnly:
    """GBP post with text only (no media) — GBP supports text-only posts."""

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.google_business.requests.post")
    def test_gbp_text_only_no_media(self, mock_gbp_post, mock_auth,
                                      mock_sheets, mock_reread):
        """GBP STANDARD post with caption only, no drive_file_id — true text-only."""
        mock_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer fake"
        }
        mock_gbp_post.return_value = _mock_gbp_success()

        row = _build_row(
            network="GBP",
            caption_gbp="Business update text",
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
            drive_file_id="",  # no media!
        )
        mock_reread.return_value = _locked_row(
            network="GBP",
            caption_gbp="Business update text",
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
            drive_file_id="",
        )

        process_row(row, HEADER, 2)

        # Verify GBP API was called with text-only body (no media)
        mock_gbp_post.assert_called_once()
        call_body = mock_gbp_post.call_args[1]["json"]
        assert call_body["summary"] == "Business update text"
        assert call_body["topicType"] == "STANDARD"
        assert "media" not in call_body

        # Verify status → POSTED
        final_update = mock_sheets.call_args_list[-1][0][1]
        assert final_update["status"] == STATUS_POSTED

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value=_CLOUD_URL)
    @patch("main.normalize_media", side_effect=_NORM_PASSTHROUGH)
    @patch("main.validate_media_pre_publish", return_value=None)
    @patch("main.drive_download_with_metadata", return_value=_DRIVE_RETURN)
    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.google_business.requests.post")
    def test_gbp_text_with_image_file(self, mock_gbp_post, mock_auth,
                                        mock_drive, _mock_vmp, mock_norm, mock_cloud,
                                        mock_sheets, mock_reread):
        """GBP STANDARD post with caption + image file."""
        mock_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer fake"
        }
        mock_gbp_post.return_value = _mock_gbp_success()

        row = _build_row(
            network="GBP",
            caption_gbp="Business update text",
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
        )
        mock_reread.return_value = _locked_row(
            network="GBP",
            caption_gbp="Business update text",
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
        )

        process_row(row, HEADER, 2)

        # Verify GBP API was called with media
        mock_gbp_post.assert_called_once()
        call_body = mock_gbp_post.call_args[1]["json"]
        assert call_body["summary"] == "Business update text"
        assert "media" in call_body

        # Verify status → POSTED
        final_update = mock_sheets.call_args_list[-1][0][1]
        assert final_update["status"] == STATUS_POSTED


# ═══════════════════════════════════════════════════════════════
#  Scenario 3: GBP text + image
# ═══════════════════════════════════════════════════════════════

class TestE2E_Scenario3_GBP_TextAndImage:
    """GBP post with text + image — should include media in API body."""

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value=_CLOUD_URL)
    @patch("main.normalize_media", side_effect=_NORM_PASSTHROUGH)
    @patch("main.validate_media_pre_publish", return_value=None)
    @patch("main.drive_download_with_metadata", return_value=_DRIVE_RETURN)
    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.google_business.requests.post")
    def test_gbp_text_and_image(self, mock_gbp_post, mock_auth,
                                  mock_drive, _mock_vmp, mock_norm, mock_cloud,
                                  mock_sheets, mock_reread):
        mock_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer fake"
        }
        mock_gbp_post.return_value = _mock_gbp_success()

        row = _build_row(
            network="GBP",
            caption_gbp="Post with image",
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
        )
        mock_reread.return_value = _locked_row(
            network="GBP",
            caption_gbp="Post with image",
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
        )

        process_row(row, HEADER, 2)

        mock_gbp_post.assert_called_once()
        call_body = mock_gbp_post.call_args[1]["json"]
        assert call_body["summary"] == "Post with image"
        assert "media" in call_body
        assert call_body["media"][0]["mediaFormat"] == "PHOTO"
        assert call_body["media"][0]["sourceUrl"] == _CLOUD_URL

        final_update = mock_sheets.call_args_list[-1][0][1]
        assert final_update["status"] == STATUS_POSTED

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value=_CLOUD_URL)
    @patch("main.normalize_media", side_effect=_NORM_PASSTHROUGH)
    @patch("main.validate_media_pre_publish", return_value=None)
    @patch("main.drive_download_with_metadata", return_value=_DRIVE_RETURN)
    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.google_business.requests.post")
    def test_gbp_with_cta(self, mock_gbp_post, mock_auth,
                            mock_drive, _mock_vmp, mock_norm, mock_cloud,
                            mock_sheets, mock_reread):
        """GBP post with CTA should include callToAction in API body."""
        mock_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer fake"
        }
        mock_gbp_post.return_value = _mock_gbp_success()

        row = _build_row(
            network="GBP",
            caption_gbp="CTA post",
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
            cta_type="LEARN_MORE",
            cta_url="https://example.com",
        )
        mock_reread.return_value = _locked_row(
            network="GBP",
            caption_gbp="CTA post",
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
            cta_type="LEARN_MORE",
            cta_url="https://example.com",
        )

        process_row(row, HEADER, 2)

        call_body = mock_gbp_post.call_args[1]["json"]
        assert call_body["callToAction"]["actionType"] == "LEARN_MORE"
        assert call_body["callToAction"]["url"] == "https://example.com"


# ═══════════════════════════════════════════════════════════════
#  Scenario 4: IG+FB+GBP with GBP failure → PARTIAL
# ═══════════════════════════════════════════════════════════════

class TestE2E_Scenario4_AllChannels_GBP_Fails:
    """IG+FB+GBP: IG and FB succeed, GBP fails → PARTIAL status."""

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value=_CLOUD_URL)
    @patch("main.normalize_media", side_effect=_NORM_PASSTHROUGH)
    @patch("main.validate_media_pre_publish", return_value=None)
    @patch("main.drive_download_with_metadata", return_value=_DRIVE_RETURN)
    @patch("meta_publish.fb_publish_feed", return_value="fb_post_004")
    @patch("meta_publish.ig_publish_feed", return_value="ig_media_004")
    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.google_business.requests.post")
    @patch("main.PUBLISH_MAX_RETRIES", 1)
    def test_partial_when_gbp_fails(self, mock_gbp_post, mock_auth,
                                      mock_ig, mock_fb,
                                      mock_drive, _mock_vmp, mock_norm, mock_cloud,
                                      mock_sheets, mock_reread):
        mock_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer fake"
        }
        mock_gbp_post.return_value = _mock_gbp_failure(500, "Internal Server Error")

        row = _build_row(
            network="IG+FB+GBP",
            caption_ig="IG text",
            caption_fb="FB text",
            caption_gbp="GBP text",
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
        )
        mock_reread.return_value = _locked_row(
            network="IG+FB+GBP",
            caption_ig="IG text",
            caption_fb="FB text",
            caption_gbp="GBP text",
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
        )

        process_row(row, HEADER, 2)

        # IG and FB published
        mock_ig.assert_called_once()
        mock_fb.assert_called_once()

        # Status → PARTIAL
        final_update = mock_sheets.call_args_list[-1][0][1]
        assert final_update["status"] == STATUS_PARTIAL
        assert "IG:POSTED:ig_media_004" in final_update["result"]
        assert "FB:POSTED:fb_post_004" in final_update["result"]
        assert "GBP:ERROR:" in final_update["result"]
        assert "IG" in final_update["published_channels"]
        assert "FB" in final_update["published_channels"]
        assert "GBP" in final_update["failed_channels"]
        assert "Partial success" in final_update["error"]


# ═══════════════════════════════════════════════════════════════
#  Scenario 5: Retry GBP after PARTIAL
# ═══════════════════════════════════════════════════════════════

class TestE2E_Scenario5_Retry_GBP_After_Partial:
    """After PARTIAL (IG+FB ok, GBP failed), retry should only publish to GBP."""

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.google_business.requests.post")
    @patch("main.PUBLISH_MAX_RETRIES", 1)
    def test_retry_gbp_succeeds(self, mock_gbp_post, mock_auth,
                                  mock_sheets, mock_reread):
        mock_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer fake"
        }
        mock_gbp_post.return_value = _mock_gbp_success()

        # Row is PARTIAL: IG+FB succeeded, GBP failed
        row = _build_row(
            network="IG+FB+GBP",
            status=STATUS_PARTIAL,
            caption_ig="IG text",
            caption_fb="FB text",
            caption_gbp="GBP text",
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
            published_channels="IG,FB",
            failed_channels="GBP",
            result="IG:POSTED:ig_004 | FB:POSTED:fb_004 | GBP:ERROR:api_error",
            cloudinary_url=_CLOUD_URL,
        )
        mock_reread.return_value = _locked_row(
            network="IG+FB+GBP",
            caption_ig="IG text",
            caption_fb="FB text",
            caption_gbp="GBP text",
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
            published_channels="IG,FB",
            failed_channels="GBP",
            result="IG:POSTED:ig_004 | FB:POSTED:fb_004 | GBP:ERROR:api_error",
            cloudinary_url=_CLOUD_URL,
        )

        process_partial_row(row, HEADER, 2)

        # GBP API called
        mock_gbp_post.assert_called_once()

        # Status → POSTED (all channels now succeeded)
        final_update = mock_sheets.call_args_list[-1][0][1]
        assert final_update["status"] == STATUS_POSTED
        assert "GBP:POSTED:" in final_update["result"]
        assert "GBP:ERROR" not in final_update["result"]
        # IG and FB results preserved
        assert "IG:POSTED:ig_004" in final_update["result"]
        assert "FB:POSTED:fb_004" in final_update["result"]
        assert "GBP" in final_update["published_channels"]
        assert final_update["failed_channels"] == ""

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.google_business.requests.post")
    @patch("main.PUBLISH_MAX_RETRIES", 1)
    def test_retry_gbp_still_fails(self, mock_gbp_post, mock_auth,
                                     mock_sheets, mock_reread):
        """Retry GBP but it still fails → stays PARTIAL."""
        mock_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer fake"
        }
        mock_gbp_post.return_value = _mock_gbp_failure(503, "Service Unavailable")

        row = _build_row(
            network="IG+FB+GBP",
            status=STATUS_PARTIAL,
            caption_ig="IG text",
            caption_fb="FB text",
            caption_gbp="GBP text",
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
            published_channels="IG,FB",
            failed_channels="GBP",
            result="IG:POSTED:ig_004 | FB:POSTED:fb_004 | GBP:ERROR:api_error",
            cloudinary_url=_CLOUD_URL,
        )
        mock_reread.return_value = _locked_row(
            network="IG+FB+GBP",
            caption_ig="IG text",
            caption_fb="FB text",
            caption_gbp="GBP text",
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
            published_channels="IG,FB",
            failed_channels="GBP",
            result="IG:POSTED:ig_004 | FB:POSTED:fb_004 | GBP:ERROR:api_error",
            cloudinary_url=_CLOUD_URL,
        )

        process_partial_row(row, HEADER, 2)

        final_update = mock_sheets.call_args_list[-1][0][1]
        assert final_update["status"] == STATUS_PARTIAL
        assert "GBP" in final_update["failed_channels"]

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.google_business.requests.post")
    @patch("main.PUBLISH_MAX_RETRIES", 1)
    def test_retry_does_not_republish_ig_fb(self, mock_gbp_post, mock_auth,
                                              mock_sheets, mock_reread):
        """Retry should NOT call IG or FB publish — only GBP."""
        mock_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer fake"
        }
        mock_gbp_post.return_value = _mock_gbp_success()

        row = _build_row(
            network="IG+FB+GBP",
            status=STATUS_PARTIAL,
            caption_ig="IG text",
            caption_fb="FB text",
            caption_gbp="GBP text",
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
            published_channels="IG,FB",
            failed_channels="GBP",
            result="IG:POSTED:ig_004 | FB:POSTED:fb_004 | GBP:ERROR:api_error",
            cloudinary_url=_CLOUD_URL,
        )
        mock_reread.return_value = _locked_row(
            network="IG+FB+GBP",
            caption_ig="IG text",
            caption_fb="FB text",
            caption_gbp="GBP text",
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
            published_channels="IG,FB",
            failed_channels="GBP",
            result="IG:POSTED:ig_004 | FB:POSTED:fb_004 | GBP:ERROR:api_error",
            cloudinary_url=_CLOUD_URL,
        )

        with patch("meta_publish.ig_publish_feed") as mock_ig, \
             patch("meta_publish.fb_publish_feed") as mock_fb:
            process_partial_row(row, HEADER, 2)

            # IG and FB should NOT be called
            mock_ig.assert_not_called()
            mock_fb.assert_not_called()


# ═══════════════════════════════════════════════════════════════
#  Scenario 6: Location invalid → GBP blocked
# ═══════════════════════════════════════════════════════════════

class TestE2E_Scenario6_Location_Invalid:
    """Missing or invalid google_location_id blocks GBP channel."""

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value=_CLOUD_URL)
    @patch("main.normalize_media", side_effect=_NORM_PASSTHROUGH)
    @patch("main.validate_media_pre_publish", return_value=None)
    @patch("main.drive_download_with_metadata", return_value=_DRIVE_RETURN)
    def test_gbp_only_missing_location_blocks_row(self, mock_drive, _mock_vmp, mock_norm,
                                                     mock_cloud, mock_sheets,
                                                     mock_reread):
        """GBP-only post without location_id → row ERROR (no channels can publish)."""
        row = _build_row(
            network="GBP",
            caption_gbp="GBP text",
            google_location_id="",  # missing!
        )
        mock_reread.return_value = _locked_row(
            network="GBP",
            caption_gbp="GBP text",
            google_location_id="",
        )

        process_row(row, HEADER, 2)

        final_update = mock_sheets.call_args_list[-1][0][1]
        assert final_update["status"] == STATUS_ERROR
        assert "google_location_id" in final_update["error"].lower() or \
               "GBP_LOCATION_MISSING" in final_update["error"]

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value=_CLOUD_URL)
    @patch("main.normalize_media", side_effect=_NORM_PASSTHROUGH)
    @patch("main.validate_media_pre_publish", return_value=None)
    @patch("main.drive_download_with_metadata", return_value=_DRIVE_RETURN)
    @patch("meta_publish.ig_publish_feed", return_value="ig_media_006")
    def test_ig_gbp_missing_location_partial(self, mock_ig, mock_drive, _mock_vmp, mock_norm,
                                               mock_cloud, mock_sheets, mock_reread):
        """IG+GBP with missing location → IG publishes, GBP blocked → PARTIAL."""
        row = _build_row(
            network="IG+GBP",
            caption_ig="IG text",
            caption_gbp="GBP text",
            google_location_id="",  # missing!
        )
        mock_reread.return_value = _locked_row(
            network="IG+GBP",
            caption_ig="IG text",
            caption_gbp="GBP text",
            google_location_id="",
        )

        process_row(row, HEADER, 2)

        mock_ig.assert_called_once()
        final_update = mock_sheets.call_args_list[-1][0][1]
        assert final_update["status"] == STATUS_PARTIAL
        assert "IG" in final_update["published_channels"]
        assert "GBP" in final_update["failed_channels"]
        assert "GBP:BLOCKED:validation" in final_update["result"]


# ═══════════════════════════════════════════════════════════════
#  Scenario 7: Missing caption_gbp with fallback to generic
# ═══════════════════════════════════════════════════════════════

class TestE2E_Scenario7_Caption_Fallback:
    """When caption_gbp is missing, GBP should use the generic caption field."""

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value=_CLOUD_URL)
    @patch("main.normalize_media", side_effect=_NORM_PASSTHROUGH)
    @patch("main.validate_media_pre_publish", return_value=None)
    @patch("main.drive_download_with_metadata", return_value=_DRIVE_RETURN)
    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.google_business.requests.post")
    def test_gbp_uses_generic_caption_when_specific_missing(
        self, mock_gbp_post, mock_auth,
        mock_drive, _mock_vmp, mock_norm, mock_cloud,
        mock_sheets, mock_reread,
    ):
        mock_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer fake"
        }
        mock_gbp_post.return_value = _mock_gbp_success()

        row = _build_row(
            network="GBP",
            caption="Generic caption for all channels",  # generic
            caption_gbp="",  # empty — should fallback
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
        )
        mock_reread.return_value = _locked_row(
            network="GBP",
            caption="Generic caption for all channels",
            caption_gbp="",
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
        )

        process_row(row, HEADER, 2)

        mock_gbp_post.assert_called_once()
        call_body = mock_gbp_post.call_args[1]["json"]
        assert call_body["summary"] == "Generic caption for all channels"

        final_update = mock_sheets.call_args_list[-1][0][1]
        assert final_update["status"] == STATUS_POSTED

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value=_CLOUD_URL)
    @patch("main.normalize_media", side_effect=_NORM_PASSTHROUGH)
    @patch("main.validate_media_pre_publish", return_value=None)
    @patch("main.drive_download_with_metadata", return_value=_DRIVE_RETURN)
    @patch("meta_publish.ig_publish_feed", return_value="ig_media_007")
    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.google_business.requests.post")
    def test_all_channels_use_generic_caption_fallback(
        self, mock_gbp_post, mock_auth, mock_ig,
        mock_drive, _mock_vmp, mock_norm, mock_cloud,
        mock_sheets, mock_reread,
    ):
        """IG+GBP: both use generic caption when channel-specific captions are empty."""
        mock_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer fake"
        }
        mock_gbp_post.return_value = _mock_gbp_success()

        row = _build_row(
            network="IG+GBP",
            caption="Shared caption",
            caption_ig="",
            caption_gbp="",
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
        )
        mock_reread.return_value = _locked_row(
            network="IG+GBP",
            caption="Shared caption",
            caption_ig="",
            caption_gbp="",
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
        )

        process_row(row, HEADER, 2)

        # IG should get the generic caption
        assert mock_ig.call_args[0][1] == "Shared caption"
        # GBP should get the generic caption
        call_body = mock_gbp_post.call_args[1]["json"]
        assert call_body["summary"] == "Shared caption"

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value=_CLOUD_URL)
    @patch("main.normalize_media", side_effect=_NORM_PASSTHROUGH)
    @patch("main.validate_media_pre_publish", return_value=None)
    @patch("main.drive_download_with_metadata", return_value=_DRIVE_RETURN)
    def test_no_caption_at_all_blocks_gbp(self, mock_drive, _mock_vmp, mock_norm,
                                            mock_cloud, mock_sheets, mock_reread):
        """GBP with neither caption_gbp nor generic caption → blocked."""
        row = _build_row(
            network="GBP",
            caption="",
            caption_gbp="",
            google_location_id="locations/456",
        )
        mock_reread.return_value = _locked_row(
            network="GBP",
            caption="",
            caption_gbp="",
            google_location_id="locations/456",
        )

        process_row(row, HEADER, 2)

        final_update = mock_sheets.call_args_list[-1][0][1]
        assert final_update["status"] == STATUS_ERROR
        assert "caption" in final_update["error"].lower() or \
               "GBP_CAPTION_MISSING" in final_update["error"]


# ═══════════════════════════════════════════════════════════════
#  LinkedIn helpers
# ═══════════════════════════════════════════════════════════════


def _mock_li_post_success(post_id="urn:li:share:12345"):
    """Create a mock LinkedIn create-post response (201)."""
    mock_resp = MagicMock()
    mock_resp.status_code = 201
    mock_resp.headers = {"x-restli-id": post_id}
    mock_resp.json.return_value = {"id": post_id}
    mock_resp.raise_for_status = MagicMock()
    return mock_resp


def _mock_li_post_failure(status_code=500, text="Internal Server Error"):
    """Create a mock LinkedIn API error response."""
    import requests as _req
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.text = text
    mock_resp.raise_for_status.side_effect = _req.HTTPError(response=mock_resp)
    return mock_resp


# ═══════════════════════════════════════════════════════════════
#  Scenario 8: LI text-only — success
# ═══════════════════════════════════════════════════════════════

class TestE2E_Scenario8_LI_TextOnly:
    """LinkedIn text-only post (no media) — should publish successfully."""

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("channels.linkedin.get_li_oauth_manager")
    @patch("channels.linkedin.requests.post")
    def test_li_text_only_success(self, mock_li_post, mock_auth,
                                    mock_sheets, mock_reread):
        mock_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer li_fake",
            "LinkedIn-Version": "202401",
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0",
        }
        mock_li_post.return_value = _mock_li_post_success("urn:li:share:88888")

        row = _build_row(
            network="LI",
            caption_li="LinkedIn text post",
            li_author_urn="urn:li:person:abc123",
            drive_file_id="",  # no media
        )
        mock_reread.return_value = _locked_row(
            network="LI",
            caption_li="LinkedIn text post",
            li_author_urn="urn:li:person:abc123",
            drive_file_id="",
        )

        process_row(row, HEADER, 2)

        # Verify LinkedIn API was called
        mock_li_post.assert_called_once()
        call_body = mock_li_post.call_args.kwargs.get("json") or mock_li_post.call_args[1].get("json")
        assert call_body["author"] == "urn:li:person:abc123"
        assert call_body["commentary"] == "LinkedIn text post"
        assert "content" not in call_body  # no media

        # Verify status → POSTED
        final_update = mock_sheets.call_args_list[-1][0][1]
        assert final_update["status"] == STATUS_POSTED


# ═══════════════════════════════════════════════════════════════
#  Scenario 9: IG+LI — both succeed
# ═══════════════════════════════════════════════════════════════

class TestE2E_Scenario9_IG_LI:
    """IG+LI: both channels succeed → POSTED."""

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value=_CLOUD_URL)
    @patch("main.normalize_media", side_effect=_NORM_PASSTHROUGH)
    @patch("main.validate_media_pre_publish", return_value=None)
    @patch("main.drive_download_with_metadata", return_value=_DRIVE_RETURN)
    @patch("meta_publish.ig_publish_feed", return_value="ig_media_009")
    @patch("channels.linkedin.get_li_oauth_manager")
    @patch("channels.linkedin.requests.get")
    @patch("channels.linkedin.requests.put")
    @patch("channels.linkedin.requests.post")
    def test_ig_li_both_succeed(self, mock_li_post, mock_li_put, mock_li_get,
                                  mock_li_auth, mock_ig,
                                  mock_drive, _mock_vmp, mock_norm, mock_cloud,
                                  mock_sheets, mock_reread):
        mock_li_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer li_fake",
            "LinkedIn-Version": "202401",
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0",
        }

        # LinkedIn with media: initializeUpload → create post
        init_resp = MagicMock()
        init_resp.status_code = 200
        init_resp.json.return_value = {
            "value": {
                "uploadUrl": "https://www.linkedin.com/dms-uploads/img",
                "image": "urn:li:image:e2e009",
            }
        }
        init_resp.raise_for_status = MagicMock()
        post_resp = _mock_li_post_success("urn:li:share:99009")
        mock_li_post.side_effect = [init_resp, post_resp]

        # Mock image download and upload
        img_resp = MagicMock()
        img_resp.content = b"fake-img-bytes"
        img_resp.raise_for_status = MagicMock()
        mock_li_get.return_value = img_resp
        upload_resp = MagicMock()
        upload_resp.raise_for_status = MagicMock()
        mock_li_put.return_value = upload_resp

        row = _build_row(
            network="IG+LI",
            caption_ig="IG text",
            caption_li="LI text",
            li_author_urn="urn:li:person:abc123",
        )
        mock_reread.return_value = _locked_row(
            network="IG+LI",
            caption_ig="IG text",
            caption_li="LI text",
            li_author_urn="urn:li:person:abc123",
        )

        process_row(row, HEADER, 2)

        mock_ig.assert_called_once()

        final_update = mock_sheets.call_args_list[-1][0][1]
        assert final_update["status"] == STATUS_POSTED
        assert "IG" in final_update["published_channels"]
        assert "LI" in final_update["published_channels"]


# ═══════════════════════════════════════════════════════════════
#  Scenario 10: IG+FB+LI — all succeed
# ═══════════════════════════════════════════════════════════════

class TestE2E_Scenario10_IG_FB_LI:
    """IG+FB+LI: all three channels succeed → POSTED."""

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value=_CLOUD_URL)
    @patch("main.normalize_media", side_effect=_NORM_PASSTHROUGH)
    @patch("main.validate_media_pre_publish", return_value=None)
    @patch("main.drive_download_with_metadata", return_value=_DRIVE_RETURN)
    @patch("meta_publish.fb_publish_feed", return_value="fb_post_010")
    @patch("meta_publish.ig_publish_feed", return_value="ig_media_010")
    @patch("channels.linkedin.get_li_oauth_manager")
    @patch("channels.linkedin.requests.get")
    @patch("channels.linkedin.requests.put")
    @patch("channels.linkedin.requests.post")
    def test_ig_fb_li_all_succeed(self, mock_li_post, mock_li_put, mock_li_get,
                                    mock_li_auth,
                                    mock_ig, mock_fb,
                                    mock_drive, _mock_vmp, mock_norm, mock_cloud,
                                    mock_sheets, mock_reread):
        mock_li_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer li_fake",
            "LinkedIn-Version": "202401",
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0",
        }

        # LinkedIn with media: initializeUpload → create post
        init_resp = MagicMock()
        init_resp.status_code = 200
        init_resp.json.return_value = {
            "value": {
                "uploadUrl": "https://www.linkedin.com/dms-uploads/img",
                "image": "urn:li:image:e2e010",
            }
        }
        init_resp.raise_for_status = MagicMock()
        post_resp = _mock_li_post_success("urn:li:share:10010")
        mock_li_post.side_effect = [init_resp, post_resp]

        img_resp = MagicMock()
        img_resp.content = b"fake-img-bytes"
        img_resp.raise_for_status = MagicMock()
        mock_li_get.return_value = img_resp
        upload_resp = MagicMock()
        upload_resp.raise_for_status = MagicMock()
        mock_li_put.return_value = upload_resp

        row = _build_row(
            network="IG+FB+LI",
            caption_ig="IG text",
            caption_fb="FB text",
            caption_li="LI text",
            li_author_urn="urn:li:person:abc123",
        )
        mock_reread.return_value = _locked_row(
            network="IG+FB+LI",
            caption_ig="IG text",
            caption_fb="FB text",
            caption_li="LI text",
            li_author_urn="urn:li:person:abc123",
        )

        process_row(row, HEADER, 2)

        mock_ig.assert_called_once()
        mock_fb.assert_called_once()

        final_update = mock_sheets.call_args_list[-1][0][1]
        assert final_update["status"] == STATUS_POSTED
        assert "IG" in final_update["published_channels"]
        assert "FB" in final_update["published_channels"]
        assert "LI" in final_update["published_channels"]


# ═══════════════════════════════════════════════════════════════
#  Scenario 11: IG+FB+GBP+LI — all four succeed (ALL)
# ═══════════════════════════════════════════════════════════════

class TestE2E_Scenario11_ALL_Four_Channels:
    """ALL (IG+FB+GBP+LI): all four channels succeed → POSTED."""

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value=_CLOUD_URL)
    @patch("main.normalize_media", side_effect=_NORM_PASSTHROUGH)
    @patch("main.validate_media_pre_publish", return_value=None)
    @patch("main.drive_download_with_metadata", return_value=_DRIVE_RETURN)
    @patch("meta_publish.fb_publish_feed", return_value="fb_post_011")
    @patch("meta_publish.ig_publish_feed", return_value="ig_media_011")
    @patch("channels.google_auth.get_oauth_manager")
    @patch("channels.linkedin.get_li_oauth_manager")
    @patch("channels.linkedin.requests.get")
    @patch("channels.linkedin.requests.put")
    @patch("requests.post")  # Shared: both GBP and LI use requests.post
    def test_all_four_succeed(self, mock_post, mock_li_put, mock_li_get,
                                mock_li_auth,
                                mock_gbp_auth,
                                mock_ig, mock_fb,
                                mock_drive, _mock_vmp, mock_norm, mock_cloud,
                                mock_sheets, mock_reread):
        mock_li_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer li_fake",
            "LinkedIn-Version": "202401",
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0",
        }
        mock_gbp_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer gbp_fake",
        }

        # URL-based routing for requests.post (shared by GBP and LI)
        gbp_resp = _mock_gbp_success()
        li_init_resp = MagicMock()
        li_init_resp.status_code = 200
        li_init_resp.json.return_value = {
            "value": {
                "uploadUrl": "https://www.linkedin.com/dms-uploads/img",
                "image": "urn:li:image:e2e011",
            }
        }
        li_init_resp.raise_for_status = MagicMock()
        li_post_resp = _mock_li_post_success("urn:li:share:11011")

        def route_post(url, **kwargs):
            if "mybusiness.googleapis.com" in url:
                return gbp_resp
            elif "images?action=initializeUpload" in url:
                return li_init_resp
            elif "api.linkedin.com/rest/posts" in url:
                return li_post_resp
            return MagicMock()

        mock_post.side_effect = route_post

        img_resp = MagicMock()
        img_resp.content = b"fake-img-bytes"
        img_resp.raise_for_status = MagicMock()
        mock_li_get.return_value = img_resp
        upload_resp = MagicMock()
        upload_resp.raise_for_status = MagicMock()
        mock_li_put.return_value = upload_resp

        row = _build_row(
            network="IG+FB+GBP+LI",
            caption_ig="IG text",
            caption_fb="FB text",
            caption_gbp="GBP text",
            caption_li="LI text",
            li_author_urn="urn:li:person:abc123",
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
        )
        mock_reread.return_value = _locked_row(
            network="IG+FB+GBP+LI",
            caption_ig="IG text",
            caption_fb="FB text",
            caption_gbp="GBP text",
            caption_li="LI text",
            li_author_urn="urn:li:person:abc123",
            google_location_id="locations/456",
            gbp_post_type="STANDARD",
        )

        process_row(row, HEADER, 2)

        mock_ig.assert_called_once()
        mock_fb.assert_called_once()

        final_update = mock_sheets.call_args_list[-1][0][1]
        assert final_update["status"] == STATUS_POSTED
        assert "IG" in final_update["published_channels"]
        assert "FB" in final_update["published_channels"]
        assert "GBP" in final_update["published_channels"]
        assert "LI" in final_update["published_channels"]
        assert final_update["failed_channels"] == ""


# ═══════════════════════════════════════════════════════════════
#  Scenario 12: LI fails → PARTIAL
# ═══════════════════════════════════════════════════════════════

class TestE2E_Scenario12_LI_Fails_Partial:
    """IG+LI: IG succeeds, LI fails → PARTIAL status."""

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value=_CLOUD_URL)
    @patch("main.normalize_media", side_effect=_NORM_PASSTHROUGH)
    @patch("main.validate_media_pre_publish", return_value=None)
    @patch("main.drive_download_with_metadata", return_value=_DRIVE_RETURN)
    @patch("meta_publish.ig_publish_feed", return_value="ig_media_012")
    @patch("channels.linkedin.get_li_oauth_manager")
    @patch("channels.linkedin.requests.post")
    @patch("main.PUBLISH_MAX_RETRIES", 1)
    def test_partial_when_li_fails(self, mock_li_post, mock_li_auth,
                                     mock_ig,
                                     mock_drive, _mock_vmp, mock_norm, mock_cloud,
                                     mock_sheets, mock_reread):
        mock_li_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer li_fake",
            "LinkedIn-Version": "202401",
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0",
        }
        # The initializeUpload call itself fails with 500
        mock_li_post.return_value = _mock_li_post_failure(500, "Internal Server Error")

        row = _build_row(
            network="IG+LI",
            caption_ig="IG text",
            caption_li="LI text",
            li_author_urn="urn:li:person:abc123",
        )
        mock_reread.return_value = _locked_row(
            network="IG+LI",
            caption_ig="IG text",
            caption_li="LI text",
            li_author_urn="urn:li:person:abc123",
        )

        process_row(row, HEADER, 2)

        mock_ig.assert_called_once()

        final_update = mock_sheets.call_args_list[-1][0][1]
        assert final_update["status"] == STATUS_PARTIAL
        assert "IG" in final_update["published_channels"]
        assert "LI" in final_update["failed_channels"]
        assert "IG:POSTED:ig_media_012" in final_update["result"]
        assert "LI:ERROR:" in final_update["result"]


# ═══════════════════════════════════════════════════════════════
#  Scenario 13: LI + FB fail → PARTIAL (only IG succeeded)
# ═══════════════════════════════════════════════════════════════

class TestE2E_Scenario13_LI_FB_Fail_Partial:
    """IG+FB+LI: LI and FB fail, only IG succeeds → PARTIAL."""

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value=_CLOUD_URL)
    @patch("main.normalize_media", side_effect=_NORM_PASSTHROUGH)
    @patch("main.validate_media_pre_publish", return_value=None)
    @patch("main.drive_download_with_metadata", return_value=_DRIVE_RETURN)
    @patch("meta_publish.fb_publish_feed", side_effect=Exception("FB API Error"))
    @patch("meta_publish.ig_publish_feed", return_value="ig_media_013")
    @patch("channels.linkedin.get_li_oauth_manager")
    @patch("channels.linkedin.requests.post")
    @patch("main.PUBLISH_MAX_RETRIES", 1)
    def test_partial_li_and_fb_fail(self, mock_li_post, mock_li_auth,
                                      mock_ig, mock_fb,
                                      mock_drive, _mock_vmp, mock_norm, mock_cloud,
                                      mock_sheets, mock_reread):
        mock_li_auth.return_value.get_auth_headers.return_value = {
            "Authorization": "Bearer li_fake",
            "LinkedIn-Version": "202401",
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0",
        }
        # initializeUpload fails with 429
        mock_li_post.return_value = _mock_li_post_failure(429, "Too Many Requests")

        row = _build_row(
            network="IG+FB+LI",
            caption_ig="IG text",
            caption_fb="FB text",
            caption_li="LI text",
            li_author_urn="urn:li:person:abc123",
        )
        mock_reread.return_value = _locked_row(
            network="IG+FB+LI",
            caption_ig="IG text",
            caption_fb="FB text",
            caption_li="LI text",
            li_author_urn="urn:li:person:abc123",
        )

        process_row(row, HEADER, 2)

        mock_ig.assert_called_once()

        final_update = mock_sheets.call_args_list[-1][0][1]
        assert final_update["status"] == STATUS_PARTIAL
        assert "IG" in final_update["published_channels"]
        assert "LI" in final_update["failed_channels"]
        assert "FB" in final_update["failed_channels"]


# ═══════════════════════════════════════════════════════════════
#  Scenario 14: LI disabled flag — LI not in network doesn't affect
# ═══════════════════════════════════════════════════════════════

class TestE2E_Scenario14_LI_Not_Targeted:
    """When LI is not in the network, it should not be invoked at all."""

    @patch("main.sheets_read_row")
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value=_CLOUD_URL)
    @patch("main.normalize_media", side_effect=_NORM_PASSTHROUGH)
    @patch("main.validate_media_pre_publish", return_value=None)
    @patch("main.drive_download_with_metadata", return_value=_DRIVE_RETURN)
    @patch("meta_publish.ig_publish_feed", return_value="ig_media_014")
    @patch("meta_publish.fb_publish_feed", return_value="fb_post_014")
    def test_ig_fb_without_li(self, mock_fb, mock_ig,
                                mock_drive, _mock_vmp, mock_norm, mock_cloud,
                                mock_sheets, mock_reread):
        """IG+FB row should not invoke LinkedIn channel at all."""
        row = _build_row(
            network="IG+FB",
            caption_ig="IG text",
            caption_fb="FB text",
        )
        mock_reread.return_value = _locked_row(
            network="IG+FB",
            caption_ig="IG text",
            caption_fb="FB text",
        )

        with patch("channels.linkedin.get_li_oauth_manager") as mock_li_auth, \
             patch("channels.linkedin.requests.post") as mock_li_post:
            process_row(row, HEADER, 2)

            # LinkedIn should NOT be called
            mock_li_post.assert_not_called()

        final_update = mock_sheets.call_args_list[-1][0][1]
        assert final_update["status"] == STATUS_POSTED
        assert "LI" not in final_update.get("published_channels", "")
        assert "LI" not in final_update.get("failed_channels", "")
