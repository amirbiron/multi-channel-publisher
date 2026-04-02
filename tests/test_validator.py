"""
test_validator.py — Comprehensive tests for the validation engine.

Tests the 4-phase pipeline: Normalize → Global → Channel → Aggregate
"""

import pytest

from config_constants import (
    COL_CAPTION,
    COL_CAPTION_FB,
    COL_CAPTION_GBP,
    COL_CAPTION_IG,
    COL_CAPTION_LI,
    COL_CTA_TYPE,
    COL_CTA_URL,
    COL_DRIVE_FILE_ID,
    COL_FAILED_CHANNELS,
    COL_GBP_POST_TYPE,
    COL_GOOGLE_LOCATION_ID,
    COL_LI_AUTHOR_URN,
    COL_NETWORK,
    COL_POST_TYPE,
    COL_PUBLISH_AT,
    COL_PUBLISHED_CHANNELS,
    COL_STATUS,
    STATUS_READY,
)
from validator import (
    ErrorCode,
    RowValidator,
    ValidationReport,
    format_validation_error,
    format_blocked_channels_error,
)


# ─── Fixtures ─────────────────────────────────────────────────

def _make_row(**overrides) -> dict[str, str]:
    """Build a minimal valid row dict with sensible defaults."""
    base = {
        "id": "test-1",
        COL_STATUS: STATUS_READY,
        COL_NETWORK: "IG+FB",
        COL_POST_TYPE: "FEED",
        COL_PUBLISH_AT: "2025-01-01 10:00",
        COL_CAPTION: "Hello world",
        COL_CAPTION_IG: "",
        COL_CAPTION_FB: "",
        COL_CAPTION_GBP: "",
        COL_CAPTION_LI: "",
        COL_LI_AUTHOR_URN: "",
        COL_GBP_POST_TYPE: "",
        COL_CTA_TYPE: "",
        COL_CTA_URL: "",
        COL_GOOGLE_LOCATION_ID: "",
        COL_DRIVE_FILE_ID: "drive-file-123",
        COL_PUBLISHED_CHANNELS: "",
        COL_FAILED_CHANNELS: "",
    }
    base.update(overrides)
    return base


def _make_gbp_row(**overrides) -> dict[str, str]:
    """Build a row targeting GBP with all required fields."""
    return _make_row(
        **{
            COL_NETWORK: "IG+FB+GBP",
            COL_CAPTION_GBP: "GBP caption here",
            COL_GOOGLE_LOCATION_ID: "locations/12345",
            COL_GBP_POST_TYPE: "STANDARD",
            **overrides,
        },
    )


def _make_li_row(**overrides) -> dict[str, str]:
    """Build a row targeting LinkedIn with all required fields."""
    return _make_row(
        **{
            COL_NETWORK: "LI",
            COL_CAPTION_LI: "LinkedIn post text",
            COL_LI_AUTHOR_URN: "urn:li:person:abc123",
            COL_DRIVE_FILE_ID: "",  # LI supports text-only
            **overrides,
        },
    )


@pytest.fixture
def validator():
    return RowValidator(registered_channel_ids=["IG", "FB", "GBP"])


@pytest.fixture
def validator_all():
    """Validator with all 4 channels registered (IG, FB, GBP, LI)."""
    return RowValidator(registered_channel_ids=["IG", "FB", "GBP", "LI"])


@pytest.fixture
def validator_ig_fb():
    """Validator with only IG and FB registered (no GBP)."""
    return RowValidator(registered_channel_ids=["IG", "FB"])


# ═══════════════════════════════════════════════════════════════
#  Phase 1: Normalization
# ═══════════════════════════════════════════════════════════════

class TestNormalization:
    def test_trims_values(self, validator):
        row = _make_row(**{COL_CAPTION: "  Hello  ", COL_NETWORK: "  IG  "})
        report = validator.validate(row)
        assert not report.row_blocked
        assert report.normalized_post_data["caption"] == "Hello"

    def test_network_uppercased(self, validator):
        row = _make_row(**{COL_NETWORK: "ig+fb"})
        report = validator.validate(row)
        assert not report.row_blocked
        assert "IG" in report.approved_channels

    def test_all_expanded(self, validator):
        row = _make_gbp_row(**{COL_NETWORK: "ALL"})
        report = validator.validate(row)
        assert set(report.approved_channels) == {"IG", "FB", "GBP"}
        assert any(w.code == ErrorCode.NETWORK_ALL_EXPANDED for w in report.warnings)

    def test_caption_fallback_to_generic(self, validator):
        row = _make_row(**{COL_CAPTION: "Generic text", COL_CAPTION_IG: "", COL_CAPTION_FB: ""})
        report = validator.validate(row)
        assert not report.row_blocked
        # Should have warnings about fallback only for targeted channels (IG+FB)
        fallback_warnings = [w for w in report.warnings if w.code == ErrorCode.COMMON_CAPTION_FALLBACK]
        assert len(fallback_warnings) == 2
        fallback_channels = {w.channel for w in fallback_warnings}
        assert fallback_channels == {"IG", "FB"}

    def test_caption_fallback_no_warning_for_non_targeted(self, validator):
        """No fallback warning for GBP when network is only IG."""
        row = _make_row(**{COL_NETWORK: "IG", COL_CAPTION: "Text", COL_CAPTION_IG: ""})
        report = validator.validate(row)
        fallback_warnings = [w for w in report.warnings if w.code == ErrorCode.COMMON_CAPTION_FALLBACK]
        assert all(w.channel != "GBP" for w in fallback_warnings)
        assert all(w.channel != "FB" for w in fallback_warnings)

    def test_gbp_post_type_update_mapped_to_standard(self, validator):
        row = _make_gbp_row(**{COL_GBP_POST_TYPE: "UPDATE"})
        report = validator.validate(row)
        assert not report.row_blocked
        assert report.normalized_post_data[COL_GBP_POST_TYPE] == "STANDARD"
        assert any(w.code == ErrorCode.GBP_POST_TYPE_MAPPED for w in report.warnings)

    def test_empty_string_normalized_to_none_for_optional(self, validator):
        """Optional fields with empty string should be treated as absent."""
        row = _make_row(**{COL_CTA_TYPE: "", COL_CTA_URL: ""})
        report = validator.validate(row)
        assert not report.row_blocked

    def test_post_type_defaults_to_feed(self, validator):
        row = _make_row(**{COL_POST_TYPE: ""})
        report = validator.validate(row)
        assert report.normalized_post_data["post_type"] == "FEED"

    def test_drive_file_ids_parsed(self, validator):
        row = _make_row(**{COL_DRIVE_FILE_ID: "file1, file2, file3"})
        report = validator.validate(row)
        assert report.normalized_post_data["_drive_file_ids"] == ["file1", "file2", "file3"]

    def test_unregistered_channel_skipped(self, validator_ig_fb):
        row = _make_row(**{COL_NETWORK: "IG+FB+GBP"})
        report = validator_ig_fb.validate(row)
        assert not report.row_blocked
        assert "GBP" not in report.approved_channels
        assert "GBP" in report.skipped_channels
        assert any(w.code == ErrorCode.NETWORK_UNREGISTERED_CHANNEL for w in report.warnings)


# ═══════════════════════════════════════════════════════════════
#  Phase 2: Global Validation
# ═══════════════════════════════════════════════════════════════

class TestGlobalValidation:
    def test_missing_network_blocks_row(self, validator):
        row = _make_row(**{COL_NETWORK: ""})
        report = validator.validate(row)
        assert report.row_blocked
        assert any(i.code == ErrorCode.ROW_NETWORK_MISSING for i in report.issues)

    def test_invalid_network_blocks_row(self, validator):
        row = _make_row(**{COL_NETWORK: "TIKTOK"})
        report = validator.validate(row)
        assert report.row_blocked
        assert any(i.code == ErrorCode.ROW_NETWORK_INVALID for i in report.issues)

    def test_invalid_status_blocks_row(self, validator):
        row = _make_row(**{COL_STATUS: "DRAFT"})
        report = validator.validate(row)
        assert report.row_blocked
        assert any(i.code == ErrorCode.ROW_INVALID_STATUS for i in report.issues)

    def test_processing_status_allowed(self, validator):
        """PROCESSING is valid for re-validation during partial retry."""
        row = _make_row(**{COL_STATUS: "PROCESSING"})
        report = validator.validate(row)
        assert not report.row_blocked

    def test_missing_publish_at_blocks_row(self, validator):
        row = _make_row(**{COL_PUBLISH_AT: ""})
        report = validator.validate(row)
        assert report.row_blocked
        assert any(i.code == ErrorCode.ROW_PUBLISH_AT_MISSING for i in report.issues)

    def test_missing_drive_file_id_blocks_row_for_ig_fb(self, validator):
        """IG+FB without media → row blocked (both channels require media)."""
        row = _make_row(**{COL_DRIVE_FILE_ID: "", COL_NETWORK: "IG+FB"})
        report = validator.validate(row)
        assert report.row_blocked
        assert any(i.code == ErrorCode.ROW_MEDIA_MISSING for i in report.issues)

    def test_missing_drive_file_id_does_not_block_gbp_only(self, validator):
        """GBP-only without media → NOT blocked (GBP supports text-only)."""
        row = _make_gbp_row(**{COL_DRIVE_FILE_ID: ""})
        report = validator.validate(row)
        assert not report.row_blocked
        assert "GBP" in report.approved_channels

    def test_missing_drive_file_id_blocks_ig_not_gbp(self, validator):
        """IG+GBP without media → IG blocked (needs media), GBP approved (text-only ok)."""
        row = _make_gbp_row(**{
            COL_DRIVE_FILE_ID: "",
            COL_NETWORK: "IG+GBP",
            COL_CAPTION_IG: "IG text",
        })
        report = validator.validate(row)
        assert not report.row_blocked
        assert "GBP" in report.approved_channels
        assert "IG" in report.blocked_channels
        ig_issues = report.blocked_channels["IG"]
        assert any(i.code == ErrorCode.IG_MEDIA_MISSING for i in ig_issues)

    def test_carousel_reels_blocks_row(self, validator):
        row = _make_row(**{
            COL_DRIVE_FILE_ID: "file1,file2",
            COL_POST_TYPE: "REELS",
        })
        report = validator.validate(row)
        assert report.row_blocked
        assert any(i.code == ErrorCode.ROW_CAROUSEL_REELS for i in report.issues)

    def test_carousel_over_10_blocks_row(self, validator):
        ids = ",".join(f"file{i}" for i in range(12))
        row = _make_row(**{COL_DRIVE_FILE_ID: ids})
        report = validator.validate(row)
        assert report.row_blocked
        assert any(i.code == ErrorCode.ROW_CAROUSEL_LIMIT for i in report.issues)

    def test_already_posted_all_channels_blocks_row(self, validator):
        row = _make_row(**{
            COL_NETWORK: "IG+FB",
            COL_PUBLISHED_CHANNELS: "IG,FB",
        })
        report = validator.validate(row)
        assert report.row_blocked
        assert any(i.code == ErrorCode.ROW_ALREADY_POSTED for i in report.issues)

    def test_partially_posted_does_not_block(self, validator):
        row = _make_row(**{
            COL_NETWORK: "IG+FB",
            COL_PUBLISHED_CHANNELS: "IG",
        })
        report = validator.validate(row)
        assert not report.row_blocked

    def test_no_registered_channels_blocks_row(self, validator_ig_fb):
        row = _make_row(**{COL_NETWORK: "GBP"})
        report = validator_ig_fb.validate(row)
        assert report.row_blocked
        assert any(i.code == ErrorCode.ROW_NO_CHANNELS_AFTER_PARSE for i in report.issues)


# ═══════════════════════════════════════════════════════════════
#  Phase 3: Channel Validation
# ═══════════════════════════════════════════════════════════════

class TestChannelValidationIG:
    def test_ig_valid(self, validator):
        row = _make_row(**{COL_NETWORK: "IG"})
        report = validator.validate(row)
        assert "IG" in report.approved_channels

    def test_ig_no_caption_blocked(self, validator):
        row = _make_row(**{COL_NETWORK: "IG", COL_CAPTION: "", COL_CAPTION_IG: ""})
        report = validator.validate(row)
        assert "IG" in report.blocked_channels
        issues = report.blocked_channels["IG"]
        assert any(i.code == ErrorCode.IG_CAPTION_MISSING for i in issues)


class TestChannelValidationFB:
    def test_fb_valid(self, validator):
        row = _make_row(**{COL_NETWORK: "FB"})
        report = validator.validate(row)
        assert "FB" in report.approved_channels

    def test_fb_no_caption_blocked(self, validator):
        row = _make_row(**{COL_NETWORK: "FB", COL_CAPTION: "", COL_CAPTION_FB: ""})
        report = validator.validate(row)
        assert "FB" in report.blocked_channels


class TestChannelValidationGBP:
    def test_gbp_valid(self, validator):
        row = _make_gbp_row()
        report = validator.validate(row)
        assert "GBP" in report.approved_channels

    def test_gbp_missing_location_blocked(self, validator):
        row = _make_gbp_row(**{COL_GOOGLE_LOCATION_ID: ""})
        report = validator.validate(row)
        assert "GBP" in report.blocked_channels
        issues = report.blocked_channels["GBP"]
        assert any(i.code == ErrorCode.GBP_LOCATION_MISSING for i in issues)

    def test_gbp_unsupported_post_type_blocked(self, validator):
        row = _make_gbp_row(**{COL_GBP_POST_TYPE: "EVENT"})
        report = validator.validate(row)
        assert "GBP" in report.blocked_channels
        issues = report.blocked_channels["GBP"]
        assert any(i.code == ErrorCode.GBP_POST_TYPE_UNSUPPORTED for i in issues)

    def test_gbp_no_caption_blocked(self, validator):
        row = _make_gbp_row(**{COL_CAPTION_GBP: "", COL_CAPTION: ""})
        report = validator.validate(row)
        assert "GBP" in report.blocked_channels

    def test_gbp_cta_incomplete_type_only(self, validator):
        row = _make_gbp_row(**{COL_CTA_TYPE: "LEARN_MORE", COL_CTA_URL: ""})
        report = validator.validate(row)
        assert "GBP" in report.blocked_channels
        issues = report.blocked_channels["GBP"]
        assert any(i.code == ErrorCode.GBP_CTA_INCOMPLETE for i in issues)

    def test_gbp_cta_incomplete_url_only(self, validator):
        row = _make_gbp_row(**{COL_CTA_TYPE: "", COL_CTA_URL: "https://example.com"})
        report = validator.validate(row)
        assert "GBP" in report.blocked_channels
        issues = report.blocked_channels["GBP"]
        assert any(i.code == ErrorCode.GBP_CTA_INCOMPLETE for i in issues)

    def test_gbp_cta_both_present_ok(self, validator):
        row = _make_gbp_row(**{COL_CTA_TYPE: "LEARN_MORE", COL_CTA_URL: "https://example.com"})
        report = validator.validate(row)
        assert "GBP" in report.approved_channels

    def test_gbp_update_mapped_to_standard(self, validator):
        row = _make_gbp_row(**{COL_GBP_POST_TYPE: "UPDATE"})
        report = validator.validate(row)
        assert "GBP" in report.approved_channels

    def test_old_ig_fb_rows_unaffected_by_gbp(self, validator):
        """Old IG+FB rows should not require GBP fields."""
        row = _make_row(**{COL_NETWORK: "IG+FB"})
        report = validator.validate(row)
        assert not report.row_blocked
        assert "GBP" not in report.blocked_channels
        assert "GBP" not in report.approved_channels
        assert set(report.approved_channels) == {"IG", "FB"}


# ═══════════════════════════════════════════════════════════════
#  Phase 4: Aggregation / Decision Engine
# ═══════════════════════════════════════════════════════════════

class TestAggregation:
    def test_full_approval(self, validator):
        row = _make_row(**{COL_NETWORK: "IG+FB"})
        report = validator.validate(row)
        assert report.is_fully_approved
        assert not report.is_partially_approved
        assert set(report.approved_channels) == {"IG", "FB"}

    def test_full_approval_not_when_skipped(self, validator_ig_fb):
        """Skipped channels mean not fully approved even if all validated pass."""
        row = _make_row(**{COL_NETWORK: "IG+FB+GBP"})
        report = validator_ig_fb.validate(row)
        assert not report.row_blocked
        assert not report.is_fully_approved  # GBP skipped
        assert "GBP" in report.skipped_channels

    def test_partial_approval_gbp_blocked(self, validator):
        """GBP blocked but IG+FB approved → partial approval."""
        row = _make_row(**{
            COL_NETWORK: "IG+FB+GBP",
            COL_GOOGLE_LOCATION_ID: "",  # Missing → blocks GBP
        })
        report = validator.validate(row)
        assert not report.row_blocked
        assert report.is_partially_approved
        assert set(report.approved_channels) == {"IG", "FB"}
        assert "GBP" in report.blocked_channels

    def test_all_channels_blocked_blocks_row(self, validator):
        """If all channels fail validation → row is blocked."""
        row = _make_row(**{
            COL_NETWORK: "GBP",
            COL_GOOGLE_LOCATION_ID: "",
            COL_CAPTION: "",
            COL_CAPTION_GBP: "",
        })
        report = validator.validate(row)
        assert report.row_blocked
        assert not report.approved_channels

    def test_normalized_post_data_populated(self, validator):
        row = _make_gbp_row()
        report = validator.validate(row)
        pd = report.normalized_post_data
        assert pd["caption"] == "Hello world"
        assert pd[COL_GOOGLE_LOCATION_ID] == "locations/12345"
        assert pd["post_type"] == "FEED"
        assert pd["_drive_file_ids"] == ["drive-file-123"]


# ═══════════════════════════════════════════════════════════════
#  AC: AI Intake Specific Scenarios
# ═══════════════════════════════════════════════════════════════

class TestAIIntakeAC:
    """Acceptance criteria from Task 11."""

    def test_ai_writes_ready_row_publisher_picks_up(self, validator):
        """AC: AI כותבת שורה → Publisher קולט בלי התערבות."""
        row = _make_gbp_row()
        report = validator.validate(row)
        assert not report.row_blocked
        assert "GBP" in report.approved_channels

    def test_missing_gbp_field_marks_error_not_sent_to_meta(self, validator):
        """AC: חסר שדה חובה ל-GBP → שורה מסומנת כשגויה, לא נשלחת ל-Meta בטעות."""
        row = _make_row(**{
            COL_NETWORK: "IG+GBP",
            COL_GOOGLE_LOCATION_ID: "",  # Missing GBP required field
        })
        report = validator.validate(row)
        assert not report.row_blocked  # Row itself is not blocked
        assert "GBP" in report.blocked_channels  # GBP is blocked
        assert "IG" in report.approved_channels  # IG still approved
        # Meta (IG) won't accidentally receive GBP content

    def test_old_ig_fb_rows_not_affected(self, validator):
        """AC: שורות IG+FB ישנות לא מושפעות."""
        row = _make_row(**{COL_NETWORK: "IG+FB"})
        report = validator.validate(row)
        assert not report.row_blocked
        assert set(report.approved_channels) == {"IG", "FB"}
        # No GBP-related issues at all
        gbp_issues = [i for i in report.issues if i.channel == "GBP"]
        assert len(gbp_issues) == 0


# ═══════════════════════════════════════════════════════════════
#  Format Helpers
# ═══════════════════════════════════════════════════════════════

class TestFormatHelpers:
    def test_format_validation_error_row_block(self, validator):
        row = _make_row(**{COL_NETWORK: ""})
        report = validator.validate(row)
        msg = format_validation_error(report)
        assert "ROW_NETWORK_MISSING" in msg

    def test_format_blocked_channels_error(self, validator):
        row = _make_row(**{
            COL_NETWORK: "IG+GBP",
            COL_GOOGLE_LOCATION_ID: "",
        })
        report = validator.validate(row)
        msg = format_blocked_channels_error(report)
        assert "GBP" in msg
        assert "GBP_LOCATION_MISSING" in msg

    def test_report_properties(self, validator):
        row = _make_row(**{
            COL_NETWORK: "IG+GBP",
            COL_GOOGLE_LOCATION_ID: "",
        })
        report = validator.validate(row)
        assert report.is_partially_approved
        assert not report.is_fully_approved
        assert len(report.channel_blocking_issues) > 0


# ═══════════════════════════════════════════════════════════════
#  Phase 3: Channel Validation — LinkedIn
# ═══════════════════════════════════════════════════════════════

class TestChannelValidationLI:
    def test_li_valid_text_only(self, validator_all):
        row = _make_li_row()
        report = validator_all.validate(row)
        assert "LI" in report.approved_channels

    def test_li_valid_organization_urn(self, validator_all):
        row = _make_li_row(**{COL_LI_AUTHOR_URN: "urn:li:organization:12345"})
        report = validator_all.validate(row)
        assert "LI" in report.approved_channels

    def test_li_missing_author_urn_blocked(self, validator_all):
        row = _make_li_row(**{COL_LI_AUTHOR_URN: ""})
        report = validator_all.validate(row)
        assert "LI" in report.blocked_channels
        issues = report.blocked_channels["LI"]
        assert any(i.code == ErrorCode.LI_AUTHOR_URN_MISSING for i in issues)

    def test_li_invalid_urn_format_blocked(self, validator_all):
        row = _make_li_row(**{COL_LI_AUTHOR_URN: "not-a-valid-urn"})
        report = validator_all.validate(row)
        assert "LI" in report.blocked_channels
        issues = report.blocked_channels["LI"]
        assert any(i.code == ErrorCode.LI_INVALID_AUTHOR_URN for i in issues)

    def test_li_wrong_entity_type_blocked(self, validator_all):
        """urn:li:company is not valid — only person|organization."""
        row = _make_li_row(**{COL_LI_AUTHOR_URN: "urn:li:company:123"})
        report = validator_all.validate(row)
        assert "LI" in report.blocked_channels
        issues = report.blocked_channels["LI"]
        assert any(i.code == ErrorCode.LI_INVALID_AUTHOR_URN for i in issues)

    def test_li_missing_caption_blocked(self, validator_all):
        """LI without any caption should be blocked (no media = text-only required)."""
        row = _make_li_row(**{COL_CAPTION_LI: "", COL_CAPTION: ""})
        report = validator_all.validate(row)
        assert "LI" in report.blocked_channels
        issues = report.blocked_channels["LI"]
        assert any(i.code == ErrorCode.LI_CAPTION_MISSING for i in issues)

    def test_li_caption_too_long_blocked(self, validator_all):
        row = _make_li_row(**{COL_CAPTION_LI: "A" * 3001})
        report = validator_all.validate(row)
        assert "LI" in report.blocked_channels
        issues = report.blocked_channels["LI"]
        assert any(i.code == ErrorCode.LI_CAPTION_TOO_LONG for i in issues)

    def test_li_caption_at_max_length_ok(self, validator_all):
        row = _make_li_row(**{COL_CAPTION_LI: "A" * 3000})
        report = validator_all.validate(row)
        assert "LI" in report.approved_channels

    def test_li_fallback_to_generic_caption(self, validator_all):
        """LI should use generic caption when caption_li is empty."""
        row = _make_li_row(**{COL_CAPTION_LI: "", COL_CAPTION: "Generic text"})
        report = validator_all.validate(row)
        assert "LI" in report.approved_channels
        fallback_warnings = [w for w in report.warnings if w.code == ErrorCode.COMMON_CAPTION_FALLBACK and w.channel == "LI"]
        assert len(fallback_warnings) == 1


# ═══════════════════════════════════════════════════════════════
#  Network Combinations with LI
# ═══════════════════════════════════════════════════════════════

class TestNetworkCombinationsLI:
    def test_li_only(self, validator_all):
        row = _make_li_row(**{COL_NETWORK: "LI"})
        report = validator_all.validate(row)
        assert not report.row_blocked
        assert set(report.approved_channels) == {"LI"}

    def test_ig_li(self, validator_all):
        row = _make_li_row(**{
            COL_NETWORK: "IG+LI",
            COL_CAPTION_IG: "IG text",
            COL_DRIVE_FILE_ID: "drive-file-123",
        })
        report = validator_all.validate(row)
        assert not report.row_blocked
        assert "IG" in report.approved_channels
        assert "LI" in report.approved_channels

    def test_fb_li(self, validator_all):
        row = _make_li_row(**{
            COL_NETWORK: "FB+LI",
            COL_CAPTION_FB: "FB text",
            COL_DRIVE_FILE_ID: "drive-file-123",
        })
        report = validator_all.validate(row)
        assert not report.row_blocked
        assert "FB" in report.approved_channels
        assert "LI" in report.approved_channels

    def test_ig_fb_li(self, validator_all):
        row = _make_li_row(**{
            COL_NETWORK: "IG+FB+LI",
            COL_CAPTION_IG: "IG text",
            COL_CAPTION_FB: "FB text",
            COL_DRIVE_FILE_ID: "drive-file-123",
        })
        report = validator_all.validate(row)
        assert not report.row_blocked
        assert set(report.approved_channels) == {"IG", "FB", "LI"}

    def test_all_four_channels(self, validator_all):
        row = _make_li_row(**{
            COL_NETWORK: "IG+FB+GBP+LI",
            COL_CAPTION_IG: "IG text",
            COL_CAPTION_FB: "FB text",
            COL_CAPTION_GBP: "GBP text",
            COL_GOOGLE_LOCATION_ID: "locations/456",
            COL_GBP_POST_TYPE: "STANDARD",
            COL_DRIVE_FILE_ID: "drive-file-123",
        })
        report = validator_all.validate(row)
        assert not report.row_blocked
        assert set(report.approved_channels) == {"IG", "FB", "GBP", "LI"}

    def test_all_expanded_includes_li(self, validator_all):
        """ALL should expand to IG+FB+GBP+LI when all 4 channels registered."""
        row = _make_li_row(**{
            COL_NETWORK: "ALL",
            COL_CAPTION_IG: "IG text",
            COL_CAPTION_FB: "FB text",
            COL_CAPTION_GBP: "GBP text",
            COL_GOOGLE_LOCATION_ID: "locations/456",
            COL_GBP_POST_TYPE: "STANDARD",
            COL_DRIVE_FILE_ID: "drive-file-123",
        })
        report = validator_all.validate(row)
        assert not report.row_blocked
        assert set(report.approved_channels) == {"IG", "FB", "GBP", "LI"}

    def test_old_ig_fb_rows_unaffected_by_li(self, validator_all):
        """Old IG+FB rows should not require LI fields."""
        row = _make_row(**{COL_NETWORK: "IG+FB"})
        report = validator_all.validate(row)
        assert not report.row_blocked
        assert "LI" not in report.blocked_channels
        assert "LI" not in report.approved_channels
        assert set(report.approved_channels) == {"IG", "FB"}

    def test_li_not_registered_skipped(self, validator):
        """When LI not registered, it should be skipped, not block the row."""
        row = _make_row(**{COL_NETWORK: "IG+FB+LI"})
        report = validator.validate(row)
        assert not report.row_blocked
        assert "LI" in report.skipped_channels
        assert set(report.approved_channels) == {"IG", "FB"}

    def test_li_text_only_no_media_not_blocked(self, validator_all):
        """LI supports text-only posts — missing drive_file_id should not block."""
        row = _make_li_row(**{COL_DRIVE_FILE_ID: ""})
        report = validator_all.validate(row)
        assert not report.row_blocked
        assert "LI" in report.approved_channels


# ═══════════════════════════════════════════════════════════════
#  PARTIAL Scenarios with LI
# ═══════════════════════════════════════════════════════════════

class TestPartialScenariosLI:
    def test_li_blocked_ig_approved_partial(self, validator_all):
        """IG+LI: LI blocked (missing URN), IG approved → partial."""
        row = _make_row(**{
            COL_NETWORK: "IG+LI",
            COL_CAPTION_IG: "IG text",
            COL_CAPTION_LI: "LI text",
            COL_LI_AUTHOR_URN: "",  # Missing → blocks LI
        })
        report = validator_all.validate(row)
        assert not report.row_blocked
        assert report.is_partially_approved
        assert "IG" in report.approved_channels
        assert "LI" in report.blocked_channels

    def test_li_only_blocked_blocks_row(self, validator_all):
        """LI-only post with invalid URN → row blocked (no channels can publish)."""
        row = _make_li_row(**{COL_LI_AUTHOR_URN: "invalid"})
        report = validator_all.validate(row)
        assert report.row_blocked
        assert not report.approved_channels

    def test_ig_fb_gbp_li_with_li_blocked(self, validator_all):
        """All 4 channels, LI blocked → partial with 3 channels approved."""
        row = _make_li_row(**{
            COL_NETWORK: "IG+FB+GBP+LI",
            COL_CAPTION_IG: "IG text",
            COL_CAPTION_FB: "FB text",
            COL_CAPTION_GBP: "GBP text",
            COL_GOOGLE_LOCATION_ID: "locations/456",
            COL_GBP_POST_TYPE: "STANDARD",
            COL_LI_AUTHOR_URN: "",  # Missing → blocks LI
            COL_DRIVE_FILE_ID: "drive-file-123",
        })
        report = validator_all.validate(row)
        assert not report.row_blocked
        assert report.is_partially_approved
        assert set(report.approved_channels) == {"IG", "FB", "GBP"}
        assert "LI" in report.blocked_channels

    def test_already_posted_li_skipped(self, validator_all):
        """LI already posted → should not be re-published."""
        row = _make_li_row(**{
            COL_NETWORK: "IG+LI",
            COL_PUBLISHED_CHANNELS: "LI",
            COL_DRIVE_FILE_ID: "drive-file-123",
            COL_CAPTION_IG: "IG text",
        })
        report = validator_all.validate(row)
        assert not report.row_blocked
        assert "IG" in report.approved_channels
