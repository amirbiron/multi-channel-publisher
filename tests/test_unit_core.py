"""
test_unit_core.py — Unit tests for core logic: network parsing, caption fallback,
status aggregation, result format, and lock handling.

These tests validate the building blocks used by the E2E scenarios,
testing functions in isolation without full pipeline mocking.
"""

from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import pytest

from config_constants import (
    COL_CAPTION, COL_CAPTION_FB, COL_CAPTION_GBP, COL_CAPTION_IG,
    COL_CTA_TYPE, COL_CTA_URL, COL_DRIVE_FILE_ID, COL_GBP_POST_TYPE,
    COL_GOOGLE_LOCATION_ID, COL_NETWORK, COL_POST_TYPE, COL_PUBLISH_AT,
    COL_STATUS, COL_PUBLISHED_CHANNELS, COL_FAILED_CHANNELS,
    STATUS_READY, STATUS_PROCESSING, STATUS_POSTED, STATUS_PARTIAL,
    STATUS_ERROR, LOCK_TIMEOUT_MINUTES,
    VALID_NETWORKS,
)
from validator import RowValidator, ErrorCode, format_validation_error
from main import (
    recover_stale_locks,
    get_cell,
    _RUN_ID,
)


# ─── Fixtures ────────────────────────────────────────────────

HEADER = [
    "id", "status", "network", "post_type", "publish_at",
    "caption", "caption_ig", "caption_fb", "caption_gbp",
    "gbp_post_type", "cta_type", "cta_url", "google_location_id",
    "drive_file_id", "cloudinary_url", "source",
    "result", "error",
    "retry_count", "locked_at", "processing_by",
    "published_channels", "failed_channels",
]


def _row_dict(**overrides) -> dict[str, str]:
    """Build a minimal valid row dict."""
    base = {
        "id": "test-1",
        COL_STATUS: STATUS_READY,
        COL_NETWORK: "IG+FB",
        COL_POST_TYPE: "FEED",
        COL_PUBLISH_AT: "2026-01-01 10:00",
        COL_CAPTION: "Hello world",
        COL_CAPTION_IG: "",
        COL_CAPTION_FB: "",
        COL_CAPTION_GBP: "",
        COL_GBP_POST_TYPE: "",
        COL_CTA_TYPE: "",
        COL_CTA_URL: "",
        COL_GOOGLE_LOCATION_ID: "",
        COL_DRIVE_FILE_ID: "file_abc",
        "cloudinary_url": "",
        "source": "",
        "result": "",
        "error": "",
        "retry_count": "",
        "locked_at": "",
        "processing_by": "",
        COL_PUBLISHED_CHANNELS: "",
        COL_FAILED_CHANNELS: "",
    }
    base.update(overrides)
    return base


def _build_sheet_row(
    status=STATUS_PROCESSING,
    locked_at="",
    processing_by="",
    retry_count="0",
    published_channels="",
    **extra,
):
    """Build a row list matching HEADER order for lock recovery tests."""
    vals = {
        "id": "1",
        "status": status,
        "network": "IG+FB",
        "post_type": "FEED",
        "publish_at": "2026-03-22 10:00",
        "caption": "text",
        "caption_ig": "ig",
        "caption_fb": "fb",
        "caption_gbp": "",
        "gbp_post_type": "",
        "cta_type": "",
        "cta_url": "",
        "google_location_id": "",
        "drive_file_id": "abc",
        "cloudinary_url": "",
        "source": "",
        "result": "",
        "error": "",
        "retry_count": retry_count,
        "locked_at": locked_at,
        "processing_by": processing_by,
        "published_channels": published_channels,
        "failed_channels": "",
    }
    vals.update(extra)
    return [vals[col] for col in HEADER]


# ═══════════════════════════════════════════════════════════════
#  Network Parsing
# ═══════════════════════════════════════════════════════════════

class TestNetworkParsing:
    """Test that the validator correctly parses network strings into channel lists."""

    def _validator(self):
        return RowValidator(registered_channel_ids=["IG", "FB", "GBP"])

    def test_single_ig(self):
        report = self._validator().validate(_row_dict(**{COL_NETWORK: "IG"}))
        assert report.approved_channels == ["IG"]

    def test_single_fb(self):
        report = self._validator().validate(_row_dict(**{COL_NETWORK: "FB"}))
        assert report.approved_channels == ["FB"]

    def test_single_gbp(self):
        report = self._validator().validate(_row_dict(**{
            COL_NETWORK: "GBP",
            COL_CAPTION_GBP: "text",
            COL_GOOGLE_LOCATION_ID: "locations/123",
        }))
        assert "GBP" in report.approved_channels

    def test_ig_fb(self):
        report = self._validator().validate(_row_dict(**{COL_NETWORK: "IG+FB"}))
        assert set(report.approved_channels) == {"IG", "FB"}

    def test_ig_fb_gbp(self):
        report = self._validator().validate(_row_dict(**{
            COL_NETWORK: "IG+FB+GBP",
            COL_CAPTION_GBP: "text",
            COL_GOOGLE_LOCATION_ID: "locations/123",
        }))
        assert set(report.approved_channels) == {"IG", "FB", "GBP"}

    def test_all_expands_to_three_channels(self):
        report = self._validator().validate(_row_dict(**{
            COL_NETWORK: "ALL",
            COL_CAPTION_GBP: "text",
            COL_GOOGLE_LOCATION_ID: "locations/123",
        }))
        assert set(report.approved_channels) == {"IG", "FB", "GBP"}

    def test_ig_gbp(self):
        report = self._validator().validate(_row_dict(**{
            COL_NETWORK: "IG+GBP",
            COL_CAPTION_GBP: "text",
            COL_GOOGLE_LOCATION_ID: "locations/123",
        }))
        assert set(report.approved_channels) == {"IG", "GBP"}

    def test_fb_gbp(self):
        report = self._validator().validate(_row_dict(**{
            COL_NETWORK: "FB+GBP",
            COL_CAPTION_GBP: "text",
            COL_GOOGLE_LOCATION_ID: "locations/123",
        }))
        assert set(report.approved_channels) == {"FB", "GBP"}

    def test_invalid_network_blocks_row(self):
        report = self._validator().validate(_row_dict(**{COL_NETWORK: "TIKTOK"}))
        assert report.row_blocked is True
        codes = [i.code for i in report.issues]
        assert ErrorCode.ROW_NETWORK_INVALID in codes

    def test_empty_network_blocks_row(self):
        report = self._validator().validate(_row_dict(**{COL_NETWORK: ""}))
        assert report.row_blocked is True
        codes = [i.code for i in report.issues]
        assert ErrorCode.ROW_NETWORK_MISSING in codes

    def test_case_insensitive_network(self):
        report = self._validator().validate(_row_dict(**{COL_NETWORK: "ig+fb"}))
        assert set(report.approved_channels) == {"IG", "FB"}

    def test_all_valid_networks_recognized(self):
        """Every value in VALID_NETWORKS should not produce ROW_NETWORK_INVALID."""
        for network in VALID_NETWORKS:
            report = self._validator().validate(_row_dict(**{
                COL_NETWORK: network,
                COL_CAPTION_GBP: "text",
                COL_GOOGLE_LOCATION_ID: "locations/123",
            }))
            invalid_codes = [
                i.code for i in report.issues
                if i.code == ErrorCode.ROW_NETWORK_INVALID
            ]
            assert not invalid_codes, f"Network {network!r} was rejected"

    def test_unregistered_channel_skipped(self):
        """When validator only knows IG+FB, GBP should be skipped (not blocked)."""
        validator = RowValidator(registered_channel_ids=["IG", "FB"])
        report = validator.validate(_row_dict(**{COL_NETWORK: "IG+FB+GBP"}))
        assert "GBP" in report.skipped_channels
        assert set(report.approved_channels) == {"IG", "FB"}


# ═══════════════════════════════════════════════════════════════
#  Caption Fallback Logic
# ═══════════════════════════════════════════════════════════════

class TestCaptionFallback:
    """Test the caption resolution: channel-specific → generic → error."""

    def _validator(self):
        return RowValidator(registered_channel_ids=["IG", "FB", "GBP"])

    def test_channel_specific_takes_precedence(self):
        report = self._validator().validate(_row_dict(**{
            COL_NETWORK: "IG",
            COL_CAPTION: "generic",
            COL_CAPTION_IG: "specific for IG",
        }))
        pd = report.normalized_post_data
        assert pd[COL_CAPTION_IG] == "specific for IG"

    def test_fallback_to_generic_when_channel_empty(self):
        report = self._validator().validate(_row_dict(**{
            COL_NETWORK: "IG",
            COL_CAPTION: "generic text",
            COL_CAPTION_IG: "",
        }))
        pd = report.normalized_post_data
        assert pd[COL_CAPTION_IG] == "generic text"

    def test_gbp_fallback_to_generic(self):
        report = self._validator().validate(_row_dict(**{
            COL_NETWORK: "GBP",
            COL_CAPTION: "generic",
            COL_CAPTION_GBP: "",
            COL_GOOGLE_LOCATION_ID: "locations/123",
        }))
        pd = report.normalized_post_data
        assert pd[COL_CAPTION_GBP] == "generic"
        assert "GBP" in report.approved_channels

    def test_no_caption_at_all_blocks_channel(self):
        report = self._validator().validate(_row_dict(**{
            COL_NETWORK: "GBP",
            COL_CAPTION: "",
            COL_CAPTION_GBP: "",
            COL_GOOGLE_LOCATION_ID: "locations/123",
        }))
        assert "GBP" not in report.approved_channels
        gbp_codes = [i.code for i in report.issues if i.channel == "GBP"]
        assert ErrorCode.GBP_CAPTION_MISSING in gbp_codes

    def test_fallback_emits_warning(self):
        report = self._validator().validate(_row_dict(**{
            COL_NETWORK: "IG",
            COL_CAPTION: "generic",
            COL_CAPTION_IG: "",
        }))
        warning_codes = [w.code for w in report.warnings]
        assert ErrorCode.COMMON_CAPTION_FALLBACK in warning_codes

    def test_fb_fallback_to_generic(self):
        report = self._validator().validate(_row_dict(**{
            COL_NETWORK: "FB",
            COL_CAPTION: "fb fallback",
            COL_CAPTION_FB: "",
        }))
        pd = report.normalized_post_data
        assert pd[COL_CAPTION_FB] == "fb fallback"

    def test_all_channels_use_generic_when_specific_empty(self):
        report = self._validator().validate(_row_dict(**{
            COL_NETWORK: "IG+FB+GBP",
            COL_CAPTION: "shared",
            COL_CAPTION_IG: "",
            COL_CAPTION_FB: "",
            COL_CAPTION_GBP: "",
            COL_GOOGLE_LOCATION_ID: "locations/123",
        }))
        pd = report.normalized_post_data
        assert pd[COL_CAPTION_IG] == "shared"
        assert pd[COL_CAPTION_FB] == "shared"
        assert pd[COL_CAPTION_GBP] == "shared"


# ═══════════════════════════════════════════════════════════════
#  Status Aggregation
# ═══════════════════════════════════════════════════════════════

class TestStatusAggregation:
    """Test how per-channel results map to row-level status (POSTED/PARTIAL/ERROR)."""

    def _validator(self):
        return RowValidator(registered_channel_ids=["IG", "FB", "GBP"])

    def test_all_approved_is_fully_approved(self):
        report = self._validator().validate(_row_dict(**{
            COL_NETWORK: "IG+FB",
            COL_CAPTION: "text",
        }))
        assert report.is_fully_approved is True
        assert report.row_blocked is False

    def test_partial_approval_when_gbp_blocked(self):
        """IG+FB+GBP where GBP is missing location → partially approved."""
        report = self._validator().validate(_row_dict(**{
            COL_NETWORK: "IG+FB+GBP",
            COL_CAPTION: "text",
            COL_GOOGLE_LOCATION_ID: "",
        }))
        assert report.is_partially_approved is True
        assert set(report.approved_channels) == {"IG", "FB"}
        assert "GBP" in report.blocked_channels

    def test_all_blocked_means_row_blocked(self):
        """GBP-only with missing location → all channels blocked → row blocked."""
        report = self._validator().validate(_row_dict(**{
            COL_NETWORK: "GBP",
            COL_CAPTION: "text",
            COL_GOOGLE_LOCATION_ID: "",
        }))
        assert report.row_blocked is True
        assert report.approved_channels == []

    def test_skipped_channels_not_fully_approved(self):
        """When channels are skipped (unregistered), is_fully_approved should be False."""
        validator = RowValidator(registered_channel_ids=["IG", "FB"])
        report = validator.validate(_row_dict(**{
            COL_NETWORK: "IG+FB+GBP",
            COL_CAPTION: "text",
        }))
        assert report.is_fully_approved is False
        assert "GBP" in report.skipped_channels


# ═══════════════════════════════════════════════════════════════
#  Result Format
# ═══════════════════════════════════════════════════════════════

class TestResultFormat:
    """Test the CHANNEL:STATUS:detail result string format."""

    def test_format_validation_error_row_block(self):
        """format_validation_error should include ROW_BLOCK issues."""
        report = RowValidator(["IG", "FB", "GBP"]).validate(
            _row_dict(**{COL_NETWORK: "INVALID"})
        )
        error_str = format_validation_error(report)
        assert "ROW_NETWORK_INVALID" in error_str

    def test_format_validation_error_channel_block(self):
        """format_validation_error should include CHANNEL_BLOCK issues."""
        report = RowValidator(["IG", "FB", "GBP"]).validate(
            _row_dict(**{
                COL_NETWORK: "GBP",
                COL_CAPTION: "text",
                COL_GOOGLE_LOCATION_ID: "",
            })
        )
        error_str = format_validation_error(report)
        assert "GBP_LOCATION_MISSING" in error_str

    def test_publish_result_format(self):
        """PublishResult should have the correct fields."""
        from channels.base import PublishResult
        r = PublishResult(
            channel="GBP",
            success=True,
            status="POSTED",
            platform_post_id="accounts/1/locations/2/localPosts/3",
        )
        assert r.channel == "GBP"
        assert r.status == "POSTED"
        assert "localPosts" in r.platform_post_id


# ═══════════════════════════════════════════════════════════════
#  Lock Handling (recover_stale_locks)
# ═══════════════════════════════════════════════════════════════

class TestLockHandling:
    """Test lock timeout recovery and stale lock detection."""

    NOW_UTC = datetime(2026, 3, 22, 12, 0, 0, tzinfo=timezone.utc)

    @patch("main.sheets_update_cells")
    def test_stale_lock_recovered_to_ready(self, mock_sheets):
        """PROCESSING row past timeout → reset to READY."""
        stale_time = (self.NOW_UTC - timedelta(minutes=LOCK_TIMEOUT_MINUTES + 5)).isoformat()
        rows = [_build_sheet_row(locked_at=stale_time)]

        recovered = recover_stale_locks(HEADER, rows, self.NOW_UTC)

        assert recovered == 1
        update = mock_sheets.call_args[0][1]
        assert update["status"] == STATUS_READY
        assert update["locked_at"] == ""
        assert update["processing_by"] == ""
        assert update["retry_count"] == "1"

    @patch("main.sheets_update_cells")
    def test_recent_lock_not_recovered(self, mock_sheets):
        """PROCESSING row within timeout → not touched."""
        recent_time = (self.NOW_UTC - timedelta(minutes=2)).isoformat()
        rows = [_build_sheet_row(locked_at=recent_time)]

        recovered = recover_stale_locks(HEADER, rows, self.NOW_UTC)

        assert recovered == 0
        mock_sheets.assert_not_called()

    @patch("main.sheets_update_cells")
    def test_non_processing_row_ignored(self, mock_sheets):
        """POSTED row should not be touched by lock recovery."""
        rows = [_build_sheet_row(status=STATUS_POSTED)]

        recovered = recover_stale_locks(HEADER, rows, self.NOW_UTC)

        assert recovered == 0
        mock_sheets.assert_not_called()

    @patch("main.sheets_update_cells")
    def test_stale_lock_with_published_channels_restores_to_partial(self, mock_sheets):
        """PROCESSING row with published channels → restore to PARTIAL, not READY."""
        stale_time = (self.NOW_UTC - timedelta(minutes=LOCK_TIMEOUT_MINUTES + 5)).isoformat()
        rows = [_build_sheet_row(
            locked_at=stale_time,
            published_channels="IG,FB",
        )]

        recovered = recover_stale_locks(HEADER, rows, self.NOW_UTC)

        assert recovered == 1
        update = mock_sheets.call_args[0][1]
        assert update["status"] == STATUS_PARTIAL

    @patch("main.sheets_update_cells")
    def test_missing_locked_at_treated_as_stale(self, mock_sheets):
        """PROCESSING row without locked_at → treated as stale (legacy)."""
        rows = [_build_sheet_row(locked_at="")]

        recovered = recover_stale_locks(HEADER, rows, self.NOW_UTC)

        assert recovered == 1
        update = mock_sheets.call_args[0][1]
        assert update["status"] == STATUS_READY

    @patch("main.sheets_update_cells")
    def test_retry_count_incremented(self, mock_sheets):
        """Recovery should increment retry_count."""
        stale_time = (self.NOW_UTC - timedelta(minutes=LOCK_TIMEOUT_MINUTES + 5)).isoformat()
        rows = [_build_sheet_row(locked_at=stale_time, retry_count="2")]

        recover_stale_locks(HEADER, rows, self.NOW_UTC)

        update = mock_sheets.call_args[0][1]
        assert update["retry_count"] == "3"

    @patch("main.sheets_update_cells")
    def test_multiple_stale_rows(self, mock_sheets):
        """Multiple stale rows should all be recovered."""
        stale_time = (self.NOW_UTC - timedelta(minutes=LOCK_TIMEOUT_MINUTES + 5)).isoformat()
        rows = [
            _build_sheet_row(locked_at=stale_time),
            _build_sheet_row(status=STATUS_POSTED),  # should be skipped
            _build_sheet_row(locked_at=stale_time),
        ]

        recovered = recover_stale_locks(HEADER, rows, self.NOW_UTC)

        assert recovered == 2
        assert mock_sheets.call_count == 2


# ═══════════════════════════════════════════════════════════════
#  GBP-specific Validation
# ═══════════════════════════════════════════════════════════════

class TestGBPValidation:
    """GBP channel-level validation rules."""

    def _validator(self):
        return RowValidator(registered_channel_ids=["IG", "FB", "GBP"])

    def test_gbp_unsupported_post_type_blocked(self):
        report = self._validator().validate(_row_dict(**{
            COL_NETWORK: "GBP",
            COL_CAPTION: "text",
            COL_GOOGLE_LOCATION_ID: "locations/123",
            COL_GBP_POST_TYPE: "EVENT",
        }))
        assert "GBP" in report.blocked_channels
        codes = [i.code for i in report.blocked_channels["GBP"]]
        assert ErrorCode.GBP_POST_TYPE_UNSUPPORTED in codes

    def test_gbp_update_maps_to_standard(self):
        """gbp_post_type 'UPDATE' should be normalized to 'STANDARD'."""
        report = self._validator().validate(_row_dict(**{
            COL_NETWORK: "GBP",
            COL_CAPTION: "text",
            COL_GOOGLE_LOCATION_ID: "locations/123",
            COL_GBP_POST_TYPE: "UPDATE",
        }))
        assert "GBP" in report.approved_channels
        warning_codes = [w.code for w in report.warnings]
        assert ErrorCode.GBP_POST_TYPE_MAPPED in warning_codes

    def test_gbp_cta_incomplete_blocked(self):
        """CTA with type but no URL should block GBP."""
        report = self._validator().validate(_row_dict(**{
            COL_NETWORK: "GBP",
            COL_CAPTION: "text",
            COL_GOOGLE_LOCATION_ID: "locations/123",
            COL_CTA_TYPE: "LEARN_MORE",
            COL_CTA_URL: "",
        }))
        assert "GBP" in report.blocked_channels
        codes = [i.code for i in report.blocked_channels["GBP"]]
        assert ErrorCode.GBP_CTA_INCOMPLETE in codes

    def test_gbp_valid_standard_approved(self):
        report = self._validator().validate(_row_dict(**{
            COL_NETWORK: "GBP",
            COL_CAPTION: "text",
            COL_GOOGLE_LOCATION_ID: "locations/123",
            COL_GBP_POST_TYPE: "STANDARD",
        }))
        assert "GBP" in report.approved_channels
        assert report.row_blocked is False


# ═══════════════════════════════════════════════════════════════
#  Error Classification
# ═══════════════════════════════════════════════════════════════

class TestErrorClassification:
    """Test BaseChannel.classify_error and is_retryable_error."""

    def test_timeout_is_retryable(self):
        from channels.base import BaseChannel
        assert BaseChannel.is_retryable_error("timeout") is True

    def test_rate_limit_is_retryable(self):
        from channels.base import BaseChannel
        assert BaseChannel.is_retryable_error("rate_limit") is True

    def test_http_500_is_retryable(self):
        from channels.base import BaseChannel
        assert BaseChannel.is_retryable_error("http_500") is True

    def test_http_400_is_not_retryable(self):
        from channels.base import BaseChannel
        assert BaseChannel.is_retryable_error("http_400") is False

    def test_http_403_is_not_retryable(self):
        from channels.base import BaseChannel
        assert BaseChannel.is_retryable_error("http_403") is False

    def test_classify_timeout_exception(self):
        from channels.base import BaseChannel
        exc = Exception("Connection timeout")
        assert BaseChannel.classify_error(exc) == "timeout"

    def test_classify_rate_limit_exception(self):
        from channels.base import BaseChannel
        exc = Exception("Rate limit exceeded")
        assert BaseChannel.classify_error(exc) == "rate_limit"
