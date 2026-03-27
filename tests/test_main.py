"""
test_main.py — בדיקות יחידה ל-main.py

מכסה: is_due, get_cell, process_row (הצלחה + שגיאות),
       main loop (סינון שורות), cleanup_old_cloudinary_assets.
"""

from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock, call

import pytest

from config import (
    TZ_IL,
    STATUS_READY, STATUS_POSTED, STATUS_ERROR, STATUS_IN_PROGRESS,
    STATUS_DRAFT, STATUS_PARTIAL,
    NETWORK_GBP, VALID_NETWORKS,
    COL_CAPTION, COL_CAPTION_GBP, COL_GOOGLE_LOCATION_ID,
)
from main import (
    is_due,
    get_cell,
    process_row,
    main,
    cleanup_old_cloudinary_assets,
    _CLOUDINARY_URL_RE,
    _publish_with_retry,
)

# ─── Header fixture ──────────────────────────────────────────
# Mirrors SHEET_COLUMNS from config_constants.py.
# Old IG/FB-only rows still work — new columns default to "".
HEADER = [
    "id", "status", "network", "post_type", "publish_at",
    "caption", "caption_ig", "caption_fb", "caption_gbp",
    "gbp_post_type", "cta_type", "cta_url", "google_location_id",
    "drive_file_id", "cloudinary_url", "source",
    "result", "error",
    "retry_count", "locked_at", "processing_by",
    "published_channels", "failed_channels",
]

NOW_UTC = datetime(2026, 3, 22, 12, 0, 0, tzinfo=timezone.utc)


def _make_row(
    network="IG",
    post_type="FEED",
    drive_id="abc123",
    caption="",
    caption_ig="hello",
    caption_fb="",
    caption_gbp="",
    status=STATUS_READY,
    google_location_id="",
    source="",
):
    """Build a row matching HEADER order."""
    return [
        "1", status, network, post_type, "2026-03-22 10:00",
        caption, caption_ig, caption_fb, caption_gbp,
        "", "", "", google_location_id,
        drive_id, "", source,
        "", "",
        "", "", "",
        "", "",
    ]


# ═══════════════════════════════════════════════════════════════
#  is_due
# ═══════════════════════════════════════════════════════════════

class TestIsDue:
    def test_past_time_is_due(self):
        assert is_due("2026-03-22 10:00", NOW_UTC) is True

    def test_future_time_is_not_due(self):
        assert is_due("2026-03-23 20:00", NOW_UTC) is False

    def test_invalid_string_returns_false(self):
        assert is_due("not-a-date", NOW_UTC) is False

    def test_empty_string_returns_false(self):
        assert is_due("", NOW_UTC) is False

    def test_none_returns_false(self):
        assert is_due(None, NOW_UTC) is False


# ═══════════════════════════════════════════════════════════════
#  get_cell
# ═══════════════════════════════════════════════════════════════

class TestGetCell:
    def test_returns_value(self):
        row = _make_row()
        assert get_cell(row, HEADER, "network") == "IG"

    def test_missing_column_returns_default(self):
        row = ["1", "READY"]
        assert get_cell(row, HEADER, "nonexistent_col", "fallback") == "fallback"

    def test_short_row_returns_default(self):
        row = ["1", "READY"]
        assert get_cell(row, HEADER, "drive_file_id") == ""

    def test_empty_default(self):
        row = []
        assert get_cell(row, HEADER, "id") == ""


# ═══════════════════════════════════════════════════════════════
#  process_row — success paths
# ═══════════════════════════════════════════════════════════════

def _make_row_with_publish_at(publish_at, **kwargs):
    """Build a row with a custom publish_at timestamp."""
    row = _make_row(**kwargs)
    idx = HEADER.index("publish_at")
    row[idx] = publish_at
    return row


def _in_progress_row(**kwargs):
    """Build a row with IN_PROGRESS status for lock verification tests."""
    kwargs.setdefault("status", STATUS_IN_PROGRESS)
    return _make_row(**kwargs)


class TestProcessRowSuccess:
    @patch("main.sheets_read_row", return_value=_in_progress_row())
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value="https://res.cloudinary.com/x/image/upload/v1/social-publisher/abc.jpg")
    @patch("main.normalize_media", side_effect=lambda b, m, n, p: (b, m, n))
    @patch("main.drive_download_with_metadata", return_value=(b"fake-img", {"mimeType": "image/jpeg", "name": "pic.jpg"}))
    @patch("main.ig_publish_feed", return_value="media_111")
    def test_ig_image_feed(self, mock_ig, mock_drive, mock_norm, mock_cloud, mock_sheets, mock_reread):
        row = _make_row()
        process_row(row, HEADER, 2)

        mock_drive.assert_called_once_with("abc123")
        mock_cloud.assert_called_once_with(b"fake-img", "image/jpeg", "pic.jpg")
        mock_ig.assert_called_once_with(
            "https://res.cloudinary.com/x/image/upload/v1/social-publisher/abc.jpg",
            "hello",
            "image/jpeg",
            "FEED",
        )
        posted_call = mock_sheets.call_args_list[-1]
        assert posted_call[0][1]["status"] == STATUS_POSTED
        assert posted_call[0][1]["result"] == "media_111"

    @patch("main.sheets_read_row", return_value=_in_progress_row(network="FB", caption_ig="", caption_fb="fb caption"))
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value="https://example.com/vid.mp4")
    @patch("main.normalize_media", side_effect=lambda b, m, n, p: (b, m, n))
    @patch("main.drive_download_with_metadata", return_value=(b"fake-vid", {"mimeType": "video/mp4", "name": "vid.mp4"}))
    @patch("main.fb_publish_feed", return_value="post_222")
    def test_fb_video_feed(self, mock_fb, mock_drive, mock_norm, mock_cloud, mock_sheets, mock_reread):
        row = _make_row(network="FB", caption_ig="", caption_fb="fb caption")
        process_row(row, HEADER, 3)

        mock_fb.assert_called_once_with(
            "https://example.com/vid.mp4",
            "fb caption",
            "video/mp4",
            "FEED",
        )
        posted_call = mock_sheets.call_args_list[-1]
        assert posted_call[0][1]["status"] == STATUS_POSTED

    @patch("main.sheets_read_row", return_value=_in_progress_row(network="FB", post_type="REELS", caption_ig="", caption_fb="reel caption"))
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value="https://example.com/vid.mp4")
    @patch("main.normalize_media", side_effect=lambda b, m, n, p: (b, m, n))
    @patch("main.drive_download_with_metadata", return_value=(b"fake-vid", {"mimeType": "video/mp4", "name": "vid.mp4"}))
    @patch("main.fb_publish_feed", return_value="reel_333")
    def test_fb_video_reels(self, mock_fb, mock_drive, mock_norm, mock_cloud, mock_sheets, mock_reread):
        """post_type=REELS should be passed through to fb_publish_feed."""
        row = _make_row(network="FB", post_type="REELS", caption_ig="", caption_fb="reel caption")
        process_row(row, HEADER, 3)

        mock_fb.assert_called_once_with(
            "https://example.com/vid.mp4",
            "reel caption",
            "video/mp4",
            "REELS",
        )

    @patch("main.sheets_read_row", return_value=_in_progress_row(post_type="REELS"))
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value="https://example.com/vid.mp4")
    @patch("main.normalize_media", side_effect=lambda b, m, n, p: (b, m, n))
    @patch("main.drive_download_with_metadata", return_value=(b"fake-vid", {"mimeType": "video/mp4", "name": "vid.mp4"}))
    @patch("main.ig_publish_feed", return_value="media_444")
    def test_ig_video_reels(self, mock_ig, mock_drive, mock_norm, mock_cloud, mock_sheets, mock_reread):
        """post_type=REELS on IG should pass through."""
        row = _make_row(post_type="REELS")
        process_row(row, HEADER, 2)

        mock_ig.assert_called_once_with(
            "https://example.com/vid.mp4",
            "hello",
            "video/mp4",
            "REELS",
        )

    @patch("main.sheets_read_row", return_value=_in_progress_row(post_type=""))
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value="https://example.com/img.jpg")
    @patch("main.normalize_media", side_effect=lambda b, m, n, p: (b, m, n))
    @patch("main.drive_download_with_metadata", return_value=(b"fake-img", {"mimeType": "image/jpeg", "name": "img.jpg"}))
    @patch("main.ig_publish_feed", return_value="media_555")
    def test_empty_post_type_defaults_to_feed(self, mock_ig, mock_drive, mock_norm, mock_cloud, mock_sheets, mock_reread):
        """If post_type column is empty, should default to FEED."""
        row = _make_row(post_type="")
        process_row(row, HEADER, 2)

        mock_ig.assert_called_once()
        assert mock_ig.call_args[0][3] == "FEED"

    @patch("main.sheets_read_row", return_value=_in_progress_row(caption_ig="", caption_fb="fallback text"))
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value="https://example.com/img.jpg")
    @patch("main.normalize_media", side_effect=lambda b, m, n, p: (b, m, n))
    @patch("main.drive_download_with_metadata", return_value=(b"fake-img", {"mimeType": "image/jpeg", "name": "img.jpg"}))
    @patch("main.ig_publish_feed", return_value="media_333")
    def test_caption_fallback_ig_uses_fb_if_empty(self, mock_ig, mock_drive, mock_norm, mock_cloud, mock_sheets, mock_reread):
        """If caption_ig is empty, should fallback to caption_fb."""
        row = _make_row(caption_ig="", caption_fb="fallback text")
        process_row(row, HEADER, 2)

        mock_ig.assert_called_once()
        assert mock_ig.call_args[0][1] == "fallback text"


# ═══════════════════════════════════════════════════════════════
#  process_row — error handling
# ═══════════════════════════════════════════════════════════════

class TestProcessRowErrors:
    @patch("main.sheets_read_row", return_value=_in_progress_row(drive_id=""))
    @patch("main.sheets_update_cells")
    def test_missing_drive_file_id(self, mock_sheets, mock_reread):
        row = _make_row(drive_id="")
        process_row(row, HEADER, 2)

        assert mock_sheets.call_args[0][1]["status"] == STATUS_ERROR
        assert "Missing drive_file_id" in mock_sheets.call_args[0][1]["error"]

    @patch("main.sheets_read_row", return_value=_in_progress_row(network="TIKTOK"))
    @patch("main.sheets_update_cells")
    def test_unknown_network(self, mock_sheets, mock_reread):
        row = _make_row(network="TIKTOK")
        process_row(row, HEADER, 2)

        assert mock_sheets.call_args[0][1]["status"] == STATUS_ERROR
        assert "Unknown network" in mock_sheets.call_args[0][1]["error"]

    @patch("main.sheets_read_row", return_value=_in_progress_row())
    @patch("main.sheets_update_cells")
    @patch("main.drive_download_with_metadata", side_effect=Exception("Drive API error"))
    def test_drive_error_marks_error(self, mock_drive, mock_sheets, mock_reread):
        row = _make_row()
        process_row(row, HEADER, 2)

        last_call = mock_sheets.call_args_list[-1]
        assert last_call[0][1]["status"] == STATUS_ERROR
        assert "Drive API error" in last_call[0][1]["error"]

    @patch("main.sheets_read_row", return_value=_in_progress_row())
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value="https://example.com/img.jpg")
    @patch("main.normalize_media", side_effect=lambda b, m, n, p: (b, m, n))
    @patch("main.drive_download_with_metadata", return_value=(b"img", {"mimeType": "image/jpeg", "name": "x.jpg"}))
    @patch("main.ig_publish_feed", side_effect=Exception("API rate limit"))
    @patch("main.PUBLISH_MAX_RETRIES", 1)
    def test_publish_error_marks_error(self, mock_ig, mock_drive, mock_norm, mock_cloud, mock_sheets, mock_reread):
        row = _make_row()
        process_row(row, HEADER, 2)

        last_call = mock_sheets.call_args_list[-1]
        assert last_call[0][1]["status"] == STATUS_ERROR
        assert "rate limit" in last_call[0][1]["error"]

    @patch("main.sheets_read_row", return_value=_in_progress_row())
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value="https://example.com/img.jpg")
    @patch("main.normalize_media", side_effect=lambda b, m, n, p: (b, m, n))
    @patch("main.drive_download_with_metadata", return_value=(b"img", {"mimeType": "image/jpeg", "name": "x.jpg"}))
    @patch("main.ig_publish_feed", side_effect=Exception("x" * 600))
    @patch("main.PUBLISH_MAX_RETRIES", 1)
    def test_long_error_message_truncated(self, mock_ig, mock_drive, mock_norm, mock_cloud, mock_sheets, mock_reread):
        row = _make_row()
        process_row(row, HEADER, 2)

        last_call = mock_sheets.call_args_list[-1]
        error_msg = last_call[0][1]["error"]
        assert len(error_msg) <= 500


# ═══════════════════════════════════════════════════════════════
#  main() — row filtering
# ═══════════════════════════════════════════════════════════════

class TestMainLoop:
    @patch("main.cleanup_old_cloudinary_assets", return_value=0)
    @patch("main.sheets_update_cells")
    @patch("main.process_row")
    @patch("main.sheets_read_all_rows")
    def test_only_ready_rows_processed(self, mock_read, mock_process, mock_update, mock_cleanup):
        mock_read.return_value = (
            HEADER,
            [
                # row 2: READY + due → should process
                _make_row(),
                # row 3: POSTED → skip
                _make_row(status="POSTED"),
                # row 4: READY + future → skip
                _make_row_with_publish_at("2099-01-01 10:00"),
                # row 5: READY + due → should process
                _make_row(network="FB", caption_ig="", caption_fb="cap"),
            ],
        )

        main()

        assert mock_process.call_count == 2
        assert mock_process.call_args_list[0][0][2] == 2
        assert mock_process.call_args_list[1][0][2] == 5
        # Verify locking happened before process_row
        assert mock_update.call_count == 2

    @patch("main.cleanup_old_cloudinary_assets", return_value=0)
    @patch("main.process_row")
    @patch("main.sheets_read_all_rows")
    def test_empty_sheet(self, mock_read, mock_process, mock_cleanup):
        mock_read.return_value = ([], [])
        main()
        mock_process.assert_not_called()

    @patch("main.cleanup_old_cloudinary_assets", return_value=0)
    @patch("main.sheets_update_cells")
    @patch("main.process_row", side_effect=Exception("boom"))
    @patch("main.sheets_read_all_rows")
    def test_process_row_exception_propagates(self, mock_read, mock_process, mock_update, mock_cleanup):
        """main() does not catch exceptions from process_row — if one leaks
        past process_row's internal try/except, the run aborts."""
        mock_read.return_value = (
            HEADER,
            [_make_row()],
        )
        with pytest.raises(Exception, match="boom"):
            main()


# ═══════════════════════════════════════════════════════════════
#  Cloudinary URL regex
# ═══════════════════════════════════════════════════════════════

class TestCloudinaryUrlRegex:
    def test_image_url(self):
        url = "https://res.cloudinary.com/mycloud/image/upload/v123/social-publisher/abc.jpg"
        m = _CLOUDINARY_URL_RE.match(url)
        assert m
        assert m.group("rtype") == "image"
        assert m.group("pid") == "social-publisher/abc"

    def test_video_url(self):
        url = "https://res.cloudinary.com/mycloud/video/upload/v999/social-publisher/vid.mp4"
        m = _CLOUDINARY_URL_RE.match(url)
        assert m
        assert m.group("rtype") == "video"
        assert m.group("pid") == "social-publisher/vid"

    def test_invalid_url(self):
        assert _CLOUDINARY_URL_RE.match("https://example.com/foo.jpg") is None


# ═══════════════════════════════════════════════════════════════
#  cleanup_old_cloudinary_assets
# ═══════════════════════════════════════════════════════════════

def _make_cleanup_row(status, publish_at, cloud_url, drive_id="abc", result="r1"):
    """Build a row for cleanup tests with the correct HEADER layout."""
    row = _make_row(status=status, drive_id=drive_id)
    row[HEADER.index("publish_at")] = publish_at
    row[HEADER.index("cloudinary_url")] = cloud_url
    row[HEADER.index("result")] = result
    return row


class TestCleanup:
    @patch("main.sheets_update_cells")
    @patch("main.delete_from_cloudinary", return_value=True)
    def test_deletes_old_posted_assets(self, mock_delete, mock_sheets):
        rows = [
            _make_cleanup_row("POSTED", "2026-01-01 10:00",
                              "https://res.cloudinary.com/x/image/upload/v1/social-publisher/old.jpg"),
        ]
        deleted = cleanup_old_cloudinary_assets(HEADER, rows, NOW_UTC)
        assert deleted == 1
        mock_delete.assert_called_once_with("social-publisher/old", resource_type="image")

    @patch("main.delete_from_cloudinary")
    def test_skips_recent_posts(self, mock_delete):
        rows = [
            _make_cleanup_row("POSTED", "2026-03-22 10:00",
                              "https://res.cloudinary.com/x/image/upload/v1/social-publisher/new.jpg"),
        ]
        deleted = cleanup_old_cloudinary_assets(HEADER, rows, NOW_UTC)
        assert deleted == 0
        mock_delete.assert_not_called()

    @patch("main.delete_from_cloudinary")
    def test_skips_non_posted_rows(self, mock_delete):
        rows = [
            _make_cleanup_row("READY", "2026-01-01 10:00",
                              "https://res.cloudinary.com/x/image/upload/v1/social-publisher/x.jpg"),
        ]
        deleted = cleanup_old_cloudinary_assets(HEADER, rows, NOW_UTC)
        assert deleted == 0

    @patch("main.sheets_update_cells")
    @patch("main.delete_from_cloudinary", return_value=True)
    def test_deletes_carousel_multi_url_assets(self, mock_delete, mock_sheets):
        """Comma-separated Cloudinary URLs (carousel) should all be deleted."""
        urls = (
            "https://res.cloudinary.com/x/image/upload/v1/social-publisher/a.jpg,"
            "https://res.cloudinary.com/x/image/upload/v1/social-publisher/b.jpg,"
            "https://res.cloudinary.com/x/video/upload/v1/social-publisher/c.mp4"
        )
        rows = [
            _make_cleanup_row("POSTED", "2026-01-01 10:00", urls, drive_id="f1,f2,f3"),
        ]
        deleted = cleanup_old_cloudinary_assets(HEADER, rows, NOW_UTC)
        assert deleted == 3
        assert mock_delete.call_count == 3
        mock_sheets.assert_called_once()  # cleared URL field once


# ═══════════════════════════════════════════════════════════════
#  process_row — IG+FB (dual publish)
# ═══════════════════════════════════════════════════════════════

class TestProcessRowBothNetworks:
    @patch("main.sheets_read_row", return_value=_in_progress_row(network="IG+FB", caption_ig="ig cap", caption_fb="fb cap"))
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value="https://example.com/img.jpg")
    @patch("main.normalize_media", side_effect=lambda b, m, n, p: (b, m, n))
    @patch("main.drive_download_with_metadata", return_value=(b"fake-img", {"mimeType": "image/jpeg", "name": "pic.jpg"}))
    @patch("main.fb_publish_feed", return_value="fb_post_999")
    @patch("main.ig_publish_feed", return_value="ig_media_888")
    def test_both_networks_success(self, mock_ig, mock_fb, mock_drive, mock_norm, mock_cloud, mock_sheets, mock_reread):
        """IG+FB should publish to both and combine result IDs."""
        row = _make_row(network="IG+FB", caption_ig="ig cap", caption_fb="fb cap")
        process_row(row, HEADER, 2)

        mock_ig.assert_called_once_with(
            "https://example.com/img.jpg", "ig cap", "image/jpeg", "FEED",
        )
        mock_fb.assert_called_once_with(
            "https://example.com/img.jpg", "fb cap", "image/jpeg", "FEED",
        )
        posted_call = mock_sheets.call_args_list[-1]
        assert posted_call[0][1]["status"] == STATUS_POSTED
        assert "IG:ig_media_888" in posted_call[0][1]["result"]
        assert "FB:fb_post_999" in posted_call[0][1]["result"]

    @patch("main.sheets_read_row", return_value=_in_progress_row(network="IG+FB", caption_ig="ig cap", caption_fb="fb cap"))
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value="https://example.com/img.jpg")
    @patch("main.normalize_media", side_effect=lambda b, m, n, p: (b, m, n))
    @patch("main.drive_download_with_metadata", return_value=(b"fake-img", {"mimeType": "image/jpeg", "name": "pic.jpg"}))
    @patch("main.fb_publish_feed", side_effect=Exception("FB API error"))
    @patch("main.ig_publish_feed", return_value="ig_media_888")
    @patch("main.PUBLISH_MAX_RETRIES", 1)
    def test_both_networks_partial_failure(self, mock_ig, mock_fb, mock_drive, mock_norm, mock_cloud, mock_sheets, mock_reread):
        """If one network fails, should mark ERROR with partial success info."""
        row = _make_row(network="IG+FB", caption_ig="ig cap", caption_fb="fb cap")
        process_row(row, HEADER, 2)

        last_call = mock_sheets.call_args_list[-1]
        assert last_call[0][1]["status"] == STATUS_ERROR
        assert "ig_media_888" in last_call[0][1]["result"]
        assert "Partial success" in last_call[0][1]["error"]
        assert "FB" in last_call[0][1]["error"]

    @patch("main.sheets_read_row", return_value=_in_progress_row(network="IG+FB", caption_ig="cap", caption_fb="cap"))
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value="https://example.com/img.jpg")
    @patch("main.normalize_media", side_effect=lambda b, m, n, p: (b, m, n))
    @patch("main.drive_download_with_metadata", return_value=(b"fake-img", {"mimeType": "image/jpeg", "name": "pic.jpg"}))
    @patch("main.fb_publish_feed", side_effect=Exception("FB fail"))
    @patch("main.ig_publish_feed", side_effect=Exception("IG fail"))
    @patch("main.PUBLISH_MAX_RETRIES", 1)
    def test_both_networks_all_fail(self, mock_ig, mock_fb, mock_drive, mock_norm, mock_cloud, mock_sheets, mock_reread):
        """If all networks fail, should mark ERROR."""
        row = _make_row(network="IG+FB", caption_ig="cap", caption_fb="cap")
        process_row(row, HEADER, 2)

        last_call = mock_sheets.call_args_list[-1]
        assert last_call[0][1]["status"] == STATUS_ERROR

    @patch("main.sheets_read_row", return_value=_in_progress_row(network="IG+FB", caption_ig="", caption_fb="fb only"))
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value="https://example.com/img.jpg")
    @patch("main.normalize_media", side_effect=lambda b, m, n, p: (b, m, n))
    @patch("main.drive_download_with_metadata", return_value=(b"fake-img", {"mimeType": "image/jpeg", "name": "pic.jpg"}))
    @patch("main.fb_publish_feed", return_value="fb_222")
    @patch("main.ig_publish_feed", return_value="ig_111")
    def test_both_networks_caption_fallback(self, mock_ig, mock_fb, mock_drive, mock_norm, mock_cloud, mock_sheets, mock_reread):
        """IG should fallback to caption_fb, FB should fallback to caption_ig."""
        row = _make_row(network="IG+FB", caption_ig="", caption_fb="fb only")
        process_row(row, HEADER, 2)

        # IG: caption_ig is empty, should fallback to caption_fb
        assert mock_ig.call_args[0][1] == "fb only"
        # FB: caption_fb is "fb only"
        assert mock_fb.call_args[0][1] == "fb only"


# ═══════════════════════════════════════════════════════════════
#  Carousel (multiple drive_file_ids)
# ═══════════════════════════════════════════════════════════════

class TestProcessRowCarousel:
    """process_row with comma-separated drive_file_ids → carousel publishing."""

    @patch("main.notify_publish_error")
    @patch("main.sheets_update_cells")
    @patch("main.sheets_read_row")
    def test_carousel_reels_rejected(self, mock_read_row, mock_update, mock_notify):
        """REELS + multiple files → error."""
        mock_read_row.return_value = _make_row(status=STATUS_IN_PROGRESS)
        row = _make_row(network="IG", post_type="REELS", drive_id="a,b", status=STATUS_IN_PROGRESS)

        process_row(row, HEADER, 2)

        # Should mark error
        error_call = mock_update.call_args[0][1]
        assert error_call["status"] == STATUS_ERROR
        assert "Carousel not supported for REELS" in error_call["error"]

    @patch("main.notify_publish_error")
    @patch("main.ig_publish_carousel", return_value="ig_car_123")
    @patch("main.upload_to_cloudinary", side_effect=["https://cloud/1.jpg", "https://cloud/2.jpg"])
    @patch("main.normalize_media", side_effect=[
        (b"img1", "image/jpeg", "1.jpg"),
        (b"img2", "image/jpeg", "2.jpg"),
    ])
    @patch("main.drive_download_with_metadata", side_effect=[
        (b"raw1", {"mimeType": "image/jpeg", "name": "1.jpg"}),
        (b"raw2", {"mimeType": "image/jpeg", "name": "2.jpg"}),
    ])
    @patch("main.sheets_update_cells")
    @patch("main.sheets_read_row")
    def test_ig_carousel_success(self, mock_read_row, mock_update, mock_drive,
                                  mock_normalize, mock_cloud, mock_ig_car, mock_notify):
        mock_read_row.return_value = _make_row(status=STATUS_IN_PROGRESS)
        row = _make_row(network="IG", post_type="FEED", drive_id="fileA,fileB", status=STATUS_IN_PROGRESS)

        process_row(row, HEADER, 2)

        # ig_publish_carousel called with 2 URLs
        mock_ig_car.assert_called_once()
        call_urls = mock_ig_car.call_args[0][0]
        assert call_urls == ["https://cloud/1.jpg", "https://cloud/2.jpg"]

        # Sheet updated with POSTED and comma-separated cloudinary URLs
        update_call = mock_update.call_args_list[-1][0][1]
        assert update_call["status"] == STATUS_POSTED
        assert "https://cloud/1.jpg,https://cloud/2.jpg" == update_call["cloudinary_url"]


# ═══════════════════════════════════════════════════════════════
#  _publish_with_retry
# ═══════════════════════════════════════════════════════════════

class TestPublishWithRetry:
    @patch("main.time.sleep")
    def test_succeeds_first_try(self, mock_sleep):
        fn = MagicMock(return_value="result_1")
        result = _publish_with_retry(fn, "url", "cap", row_id="1", network_name="IG")
        assert result == "result_1"
        fn.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("main.PUBLISH_MAX_RETRIES", 3)
    @patch("main.PUBLISH_RETRY_DELAY", 2)
    @patch("main.time.sleep")
    def test_succeeds_after_retry_with_exponential_backoff(self, mock_sleep):
        fn = MagicMock(side_effect=[Exception("fail1"), Exception("fail2"), "result_ok"])
        result = _publish_with_retry(fn, "url", "cap", row_id="1", network_name="IG")
        assert result == "result_ok"
        assert fn.call_count == 3
        assert mock_sleep.call_count == 2
        # exponential backoff: delay * 2^(attempt-1)
        mock_sleep.assert_any_call(2)   # attempt 1: 2 * 2^0 = 2
        mock_sleep.assert_any_call(4)   # attempt 2: 2 * 2^1 = 4

    @patch("main.PUBLISH_MAX_RETRIES", 0)
    def test_raises_value_error_when_max_retries_zero(self):
        fn = MagicMock()
        with pytest.raises(ValueError, match="PUBLISH_MAX_RETRIES must be >= 1"):
            _publish_with_retry(fn, "url", "cap", row_id="1", network_name="IG")

    @patch("main.PUBLISH_MAX_RETRIES", 2)
    @patch("main.PUBLISH_RETRY_DELAY", 1)
    @patch("main.time.sleep")
    def test_raises_after_all_retries_exhausted(self, mock_sleep):
        fn = MagicMock(side_effect=Exception("persistent error"))
        with pytest.raises(Exception, match="persistent error"):
            _publish_with_retry(fn, "url", "cap", row_id="1", network_name="IG")
        assert fn.call_count == 2


# ═══════════════════════════════════════════════════════════════
#  Multi-Channel Schema — new column / value tests
# ═══════════════════════════════════════════════════════════════

class TestNewSchemaConstants:
    """Verify the new constants from Task 1 exist and are correct."""

    def test_new_status_values(self):
        assert STATUS_DRAFT == "DRAFT"
        assert STATUS_PARTIAL == "PARTIAL"

    def test_new_network_values(self):
        assert NETWORK_GBP == "GBP"
        assert "IG+GBP" in VALID_NETWORKS
        assert "FB+GBP" in VALID_NETWORKS
        assert "IG+FB+GBP" in VALID_NETWORKS
        assert "ALL" in VALID_NETWORKS

    def test_valid_networks_includes_legacy(self):
        assert "IG" in VALID_NETWORKS
        assert "FB" in VALID_NETWORKS
        assert "IG+FB" in VALID_NETWORKS

    def test_header_contains_new_columns(self):
        from config_constants import SHEET_COLUMNS
        assert "caption" in SHEET_COLUMNS
        assert "caption_gbp" in SHEET_COLUMNS
        assert "gbp_post_type" in SHEET_COLUMNS
        assert "cta_type" in SHEET_COLUMNS
        assert "cta_url" in SHEET_COLUMNS
        assert "google_location_id" in SHEET_COLUMNS
        assert "source" in SHEET_COLUMNS
        assert "retry_count" in SHEET_COLUMNS
        assert "locked_at" in SHEET_COLUMNS
        assert "processing_by" in SHEET_COLUMNS
        assert "published_channels" in SHEET_COLUMNS
        assert "failed_channels" in SHEET_COLUMNS


class TestCaptionFallbackToGeneric:
    """caption_{channel} → caption (generic) fallback."""

    @patch("main.sheets_read_row", return_value=_make_row(
        status=STATUS_IN_PROGRESS, caption="generic text", caption_ig="", caption_fb="",
    ))
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value="https://example.com/img.jpg")
    @patch("main.normalize_media", side_effect=lambda b, m, n, p: (b, m, n))
    @patch("main.drive_download_with_metadata", return_value=(b"img", {"mimeType": "image/jpeg", "name": "x.jpg"}))
    @patch("main.ig_publish_feed", return_value="media_999")
    def test_ig_falls_back_to_generic_caption(self, mock_ig, mock_drive, mock_norm, mock_cloud, mock_sheets, mock_reread):
        row = _make_row(caption="generic text", caption_ig="", caption_fb="")
        process_row(row, HEADER, 2)

        mock_ig.assert_called_once()
        assert mock_ig.call_args[0][1] == "generic text"

    @patch("main.sheets_read_row", return_value=_make_row(
        status=STATUS_IN_PROGRESS, network="FB",
        caption="generic fb", caption_ig="", caption_fb="",
    ))
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value="https://example.com/img.jpg")
    @patch("main.normalize_media", side_effect=lambda b, m, n, p: (b, m, n))
    @patch("main.drive_download_with_metadata", return_value=(b"img", {"mimeType": "image/jpeg", "name": "x.jpg"}))
    @patch("main.fb_publish_feed", return_value="fb_999")
    def test_fb_falls_back_to_generic_caption(self, mock_fb, mock_drive, mock_norm, mock_cloud, mock_sheets, mock_reread):
        row = _make_row(network="FB", caption="generic fb", caption_ig="", caption_fb="")
        process_row(row, HEADER, 2)

        mock_fb.assert_called_once()
        assert mock_fb.call_args[0][1] == "generic fb"

    @patch("main.sheets_read_row", return_value=_make_row(
        status=STATUS_IN_PROGRESS, caption="generic", caption_ig="specific",
    ))
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value="https://example.com/img.jpg")
    @patch("main.normalize_media", side_effect=lambda b, m, n, p: (b, m, n))
    @patch("main.drive_download_with_metadata", return_value=(b"img", {"mimeType": "image/jpeg", "name": "x.jpg"}))
    @patch("main.ig_publish_feed", return_value="media_777")
    def test_channel_caption_takes_precedence(self, mock_ig, mock_drive, mock_norm, mock_cloud, mock_sheets, mock_reread):
        """caption_ig should take precedence over generic caption."""
        row = _make_row(caption="generic", caption_ig="specific")
        process_row(row, HEADER, 2)

        mock_ig.assert_called_once()
        assert mock_ig.call_args[0][1] == "specific"


class TestGBPOnlyNetwork:
    """GBP-only rows should be handled gracefully until GBP channel is implemented."""

    @patch("main.sheets_read_row", return_value=_make_row(
        status=STATUS_IN_PROGRESS, network="GBP",
    ))
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary")
    @patch("main.drive_download_with_metadata")
    def test_gbp_only_marks_error_without_io(self, mock_drive, mock_cloud, mock_sheets, mock_reread):
        """GBP-only row should exit before any Drive/Cloudinary I/O."""
        row = _make_row(network="GBP")
        result = process_row(row, HEADER, 2)

        assert result is True
        last_call = mock_sheets.call_args_list[-1]
        assert last_call[0][1]["status"] == STATUS_ERROR
        assert "not yet implemented" in last_call[0][1]["error"]
        # No expensive I/O should have happened
        mock_drive.assert_not_called()
        mock_cloud.assert_not_called()

    @patch("main.sheets_read_row", return_value=_make_row(
        status=STATUS_IN_PROGRESS, network="IG+GBP",
    ))
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value="https://example.com/img.jpg")
    @patch("main.normalize_media", side_effect=lambda b, m, n, p: (b, m, n))
    @patch("main.drive_download_with_metadata", return_value=(b"img", {"mimeType": "image/jpeg", "name": "x.jpg"}))
    @patch("main.ig_publish_feed", return_value="ig_media_777")
    def test_mixed_gbp_marks_partial(self, mock_ig, mock_drive, mock_norm, mock_cloud, mock_sheets, mock_reread):
        """IG+GBP should publish IG and mark PARTIAL with GBP as failed_channels."""
        row = _make_row(network="IG+GBP")
        result = process_row(row, HEADER, 2)

        assert result is True
        mock_ig.assert_called_once()
        last_call = mock_sheets.call_args_list[-1]
        assert last_call[0][1]["status"] == STATUS_PARTIAL
        assert "GBP" in last_call[0][1]["error"]
        assert last_call[0][1]["failed_channels"] == "GBP"
        assert "IG" in last_call[0][1]["published_channels"]


class TestBackwardCompatibility:
    """Existing IG/FB rows with old data continue to work with the new schema."""

    @patch("main.sheets_read_row", return_value=_make_row(status=STATUS_IN_PROGRESS))
    @patch("main.sheets_update_cells")
    @patch("main.upload_to_cloudinary", return_value="https://example.com/img.jpg")
    @patch("main.normalize_media", side_effect=lambda b, m, n, p: (b, m, n))
    @patch("main.drive_download_with_metadata", return_value=(b"img", {"mimeType": "image/jpeg", "name": "x.jpg"}))
    @patch("main.ig_publish_feed", return_value="media_100")
    def test_legacy_ig_row_still_works(self, mock_ig, mock_drive, mock_norm, mock_cloud, mock_sheets, mock_reread):
        """A simple IG row with no new columns should still publish."""
        row = _make_row()
        process_row(row, HEADER, 2)

        mock_ig.assert_called_once()
        posted_call = mock_sheets.call_args_list[-1]
        assert posted_call[0][1]["status"] == STATUS_POSTED
