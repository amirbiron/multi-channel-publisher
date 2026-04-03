"""
validator.py — Multi-layered validation engine for the Cron AI Intake flow.

Architecture:
    Normalize → Validate Global → Validate Per-Channel → Aggregate Decision

The validator returns a ValidationReport that tells the publisher:
- Whether the entire row is blocked
- Which channels are approved / blocked
- All issues (with severity and error codes)
- Normalized post data ready for publishing
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Literal, Optional

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
    GBP_POST_TYPE_STANDARD,
    LI_CAPTION_MAX_LENGTH,
    LI_URN_PATTERN,
    NETWORK_ALL,
    NETWORK_ALL_THREE,
    NETWORK_BOTH,
    NETWORK_FB,
    NETWORK_FB_GBP,
    NETWORK_FB_GBP_LI,
    NETWORK_FB_LI,
    NETWORK_GBP,
    NETWORK_GBP_LI,
    NETWORK_IG,
    NETWORK_IG_FB_GBP_LI,
    NETWORK_IG_FB_LI,
    NETWORK_IG_GBP,
    NETWORK_IG_GBP_LI,
    NETWORK_IG_LI,
    NETWORK_LI,
    POST_TYPE_FEED,
    POST_TYPE_REELS,
    STATUS_PROCESSING,
    STATUS_READY,
    VALID_NETWORKS,
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  Types & Data Models
# ═══════════════════════════════════════════════════════════════

Severity = Literal["ROW_BLOCK", "CHANNEL_BLOCK", "WARNING"]
ChannelId = Literal["IG", "FB", "GBP", "LI"]

# Map network value → list of channel IDs
_NETWORK_TO_CHANNELS: dict[str, list[str]] = {
    NETWORK_IG: [NETWORK_IG],
    NETWORK_FB: [NETWORK_FB],
    NETWORK_GBP: [NETWORK_GBP],
    NETWORK_LI: [NETWORK_LI],
    NETWORK_BOTH: [NETWORK_IG, NETWORK_FB],
    NETWORK_IG_GBP: [NETWORK_IG, NETWORK_GBP],
    NETWORK_FB_GBP: [NETWORK_FB, NETWORK_GBP],
    NETWORK_IG_LI: [NETWORK_IG, NETWORK_LI],
    NETWORK_FB_LI: [NETWORK_FB, NETWORK_LI],
    NETWORK_GBP_LI: [NETWORK_GBP, NETWORK_LI],
    NETWORK_ALL_THREE: [NETWORK_IG, NETWORK_FB, NETWORK_GBP],
    NETWORK_IG_FB_LI: [NETWORK_IG, NETWORK_FB, NETWORK_LI],
    NETWORK_IG_GBP_LI: [NETWORK_IG, NETWORK_GBP, NETWORK_LI],
    NETWORK_FB_GBP_LI: [NETWORK_FB, NETWORK_GBP, NETWORK_LI],
    NETWORK_IG_FB_GBP_LI: [NETWORK_IG, NETWORK_FB, NETWORK_GBP, NETWORK_LI],
    NETWORK_ALL: [NETWORK_IG, NETWORK_FB, NETWORK_GBP, NETWORK_LI],
}

# GBP post type aliases — normalized before validation
_GBP_POST_TYPE_ALIASES: dict[str, str] = {
    "UPDATE": GBP_POST_TYPE_STANDARD,
}


# ─── Error Codes ──────────────────────────────────────────────

class ErrorCode:
    # Row-level
    ROW_MISSING_ID = "ROW_MISSING_ID"
    ROW_INVALID_STATUS = "ROW_INVALID_STATUS"
    ROW_LOCKED = "ROW_LOCKED"
    ROW_NETWORK_MISSING = "ROW_NETWORK_MISSING"
    ROW_NETWORK_INVALID = "ROW_NETWORK_INVALID"
    ROW_NO_CHANNELS_AFTER_PARSE = "ROW_NO_CHANNELS_AFTER_PARSE"
    ROW_PUBLISH_AT_MISSING = "ROW_PUBLISH_AT_MISSING"
    ROW_PUBLISH_AT_INVALID = "ROW_PUBLISH_AT_INVALID"
    ROW_ALREADY_POSTED = "ROW_ALREADY_POSTED"
    ROW_MEDIA_MISSING = "ROW_MEDIA_MISSING"
    ROW_CAROUSEL_REELS = "ROW_CAROUSEL_REELS"
    ROW_CAROUSEL_LIMIT = "ROW_CAROUSEL_LIMIT"

    # Common content
    COMMON_NO_PUBLISHABLE_CONTENT = "COMMON_NO_PUBLISHABLE_CONTENT"
    COMMON_CAPTION_FALLBACK = "COMMON_CAPTION_FALLBACK"

    # Channel: GBP
    GBP_LOCATION_MISSING = "GBP_LOCATION_MISSING"
    GBP_POST_TYPE_UNSUPPORTED = "GBP_POST_TYPE_UNSUPPORTED"
    GBP_POST_TYPE_MAPPED = "GBP_POST_TYPE_MAPPED"
    GBP_CAPTION_MISSING = "GBP_CAPTION_MISSING"
    GBP_CTA_INCOMPLETE = "GBP_CTA_INCOMPLETE"
    GBP_MEDIA_NOT_IMAGE = "GBP_MEDIA_NOT_IMAGE"

    # Channel: IG
    IG_MEDIA_MISSING = "IG_MEDIA_MISSING"
    IG_CAPTION_MISSING = "IG_CAPTION_MISSING"

    # Channel: FB
    FB_MEDIA_MISSING = "FB_MEDIA_MISSING"
    FB_CAPTION_MISSING = "FB_CAPTION_MISSING"

    # Channel: LI
    LI_AUTHOR_URN_MISSING = "LI_AUTHOR_URN_MISSING"
    LI_INVALID_AUTHOR_URN = "LI_INVALID_AUTHOR_URN"
    LI_CAPTION_MISSING = "LI_CAPTION_MISSING"
    LI_CAPTION_TOO_LONG = "LI_CAPTION_TOO_LONG"

    # Network expansion
    NETWORK_ALL_EXPANDED = "NETWORK_ALL_EXPANDED"
    NETWORK_DUPLICATE_CHANNELS = "NETWORK_DUPLICATE_CHANNELS"
    NETWORK_UNREGISTERED_CHANNEL = "NETWORK_UNREGISTERED_CHANNEL"


# ─── Data Models ──────────────────────────────────────────────

@dataclass
class ValidationIssue:
    """A single validation issue found during processing."""
    code: str
    message: str
    severity: Severity
    field: Optional[str] = None
    channel: Optional[str] = None


@dataclass
class ChannelValidationResult:
    """Validation result for a single channel."""
    channel: str
    approved: bool
    issues: list[ValidationIssue] = field(default_factory=list)


@dataclass
class ValidationReport:
    """Complete validation decision report for one row."""
    row_blocked: bool
    approved_channels: list[str]
    blocked_channels: dict[str, list[ValidationIssue]]
    warnings: list[ValidationIssue]
    issues: list[ValidationIssue]
    normalized_post_data: dict
    skipped_channels: list[str] = field(default_factory=list)

    @property
    def is_partially_approved(self) -> bool:
        return bool(self.approved_channels) and bool(self.blocked_channels)

    @property
    def is_fully_approved(self) -> bool:
        return bool(self.approved_channels) and not self.blocked_channels and not self.skipped_channels

    @property
    def blocking_issues(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "ROW_BLOCK"]

    @property
    def channel_blocking_issues(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "CHANNEL_BLOCK"]


# ═══════════════════════════════════════════════════════════════
#  Row Validator
# ═══════════════════════════════════════════════════════════════

class RowValidator:
    """
    Validates and normalizes a row before publishing.

    Usage:
        validator = RowValidator(registered_channel_ids=["IG", "FB", "GBP"])
        report = validator.validate(row_data)

        if report.row_blocked:
            mark_error(report.blocking_issues)
        else:
            publish_to(report.approved_channels, report.normalized_post_data)
    """

    def __init__(self, registered_channel_ids: list[str]) -> None:
        self._registered = set(registered_channel_ids)

    def validate(self, row_data: dict[str, str]) -> ValidationReport:
        """
        Run the full validation pipeline on raw row data.

        row_data: dict mapping column names to string values (as read from sheet).
        Returns a ValidationReport with the decision.
        """
        issues: list[ValidationIssue] = []
        normalized = {}

        # Phase 1: Normalize
        norm_issues = self._normalize(row_data, normalized)
        issues.extend(norm_issues)

        # Check for row-blocking normalization issues (e.g. invalid network)
        if any(i.severity == "ROW_BLOCK" for i in norm_issues):
            return self._build_report(issues, normalized, row_blocked=True)

        # Phase 2: Global validation
        global_issues = self._validate_global(normalized)
        issues.extend(global_issues)

        if any(i.severity == "ROW_BLOCK" for i in global_issues):
            return self._build_report(issues, normalized, row_blocked=True)

        # Phase 3: Channel validation
        target_channels: list[str] = normalized.get("_target_channels", [])
        channel_results: dict[str, ChannelValidationResult] = {}

        for cid in target_channels:
            result = self._validate_channel(cid, normalized)
            channel_results[cid] = result
            issues.extend(result.issues)

        # Phase 4: Aggregate decision
        return self._aggregate(issues, normalized, channel_results)

    # ─── Phase 1: Normalization ───────────────────────────────

    def _normalize(
        self, row_data: dict[str, str], out: dict,
    ) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []

        # Trim all values
        for key, val in row_data.items():
            out[key] = val.strip() if isinstance(val, str) else val

        # Normalize empty strings to None for optional fields
        for key in (COL_CAPTION_GBP, COL_CAPTION_IG, COL_CAPTION_FB, COL_CAPTION_LI,
                     COL_CTA_TYPE, COL_CTA_URL, COL_GBP_POST_TYPE,
                     COL_GOOGLE_LOCATION_ID, COL_LI_AUTHOR_URN):
            if out.get(key) == "":
                out[key] = None

        # Network parsing
        raw_network = out.get(COL_NETWORK, "") or ""
        network = raw_network.upper().strip()
        out[COL_NETWORK] = network

        if not network:
            issues.append(ValidationIssue(
                code=ErrorCode.ROW_NETWORK_MISSING,
                message="Missing network field",
                severity="ROW_BLOCK",
                field=COL_NETWORK,
            ))
            out["_target_channels"] = []
            return issues

        if network not in VALID_NETWORKS:
            issues.append(ValidationIssue(
                code=ErrorCode.ROW_NETWORK_INVALID,
                message=f"Unknown network: {network!r}",
                severity="ROW_BLOCK",
                field=COL_NETWORK,
            ))
            out["_target_channels"] = []
            return issues

        # Expand network to channel list
        raw_channels = _NETWORK_TO_CHANNELS.get(network, [])

        if network == NETWORK_ALL:
            issues.append(ValidationIssue(
                code=ErrorCode.NETWORK_ALL_EXPANDED,
                message=f"ALL expanded to {raw_channels}",
                severity="WARNING",
                field=COL_NETWORK,
            ))

        # Deduplicate
        seen = set()
        channels = []
        for cid in raw_channels:
            if cid in seen:
                issues.append(ValidationIssue(
                    code=ErrorCode.NETWORK_DUPLICATE_CHANNELS,
                    message=f"Duplicate channel {cid!r} removed",
                    severity="WARNING",
                    field=COL_NETWORK,
                ))
                continue
            seen.add(cid)
            channels.append(cid)

        # Filter to registered channels, track skipped
        skipped = []
        registered_channels = []
        for cid in channels:
            if cid in self._registered:
                registered_channels.append(cid)
            else:
                skipped.append(cid)
                issues.append(ValidationIssue(
                    code=ErrorCode.NETWORK_UNREGISTERED_CHANNEL,
                    message=f"Channel {cid!r} not registered — skipping",
                    severity="WARNING",
                    channel=cid,
                ))

        out["_target_channels"] = registered_channels
        out["_skipped_channels"] = skipped

        if not registered_channels:
            issues.append(ValidationIssue(
                code=ErrorCode.ROW_NO_CHANNELS_AFTER_PARSE,
                message="No registered channels after parsing network",
                severity="ROW_BLOCK",
                field=COL_NETWORK,
            ))
            return issues

        # Normalize post_type
        post_type = (out.get(COL_POST_TYPE) or "").upper().strip()
        out[COL_POST_TYPE] = post_type if post_type in (POST_TYPE_FEED, POST_TYPE_REELS) else POST_TYPE_FEED

        # Normalize status
        status = (out.get(COL_STATUS) or "").upper().strip()
        out[COL_STATUS] = status

        # Caption fallback: channel-specific → generic (only for targeted channels)
        generic_caption = out.get(COL_CAPTION) or ""
        _caption_to_channel = {
            COL_CAPTION_IG: "IG",
            COL_CAPTION_FB: "FB",
            COL_CAPTION_GBP: "GBP",
            COL_CAPTION_LI: "LI",
        }
        for cap_col, ch_id in _caption_to_channel.items():
            if out.get(cap_col) is None:
                out[cap_col] = generic_caption or None
                if generic_caption and ch_id in registered_channels:
                    issues.append(ValidationIssue(
                        code=ErrorCode.COMMON_CAPTION_FALLBACK,
                        message=f"{cap_col} empty — falling back to generic caption",
                        severity="WARNING",
                        field=cap_col,
                        channel=ch_id,
                    ))

        # GBP post type normalization
        raw_gbp_type = out.get(COL_GBP_POST_TYPE)
        if raw_gbp_type:
            raw_gbp_upper = raw_gbp_type.upper().strip()
            if raw_gbp_upper in _GBP_POST_TYPE_ALIASES:
                mapped = _GBP_POST_TYPE_ALIASES[raw_gbp_upper]
                issues.append(ValidationIssue(
                    code=ErrorCode.GBP_POST_TYPE_MAPPED,
                    message=f"gbp_post_type {raw_gbp_upper!r} mapped to {mapped!r}",
                    severity="WARNING",
                    field=COL_GBP_POST_TYPE,
                ))
                out[COL_GBP_POST_TYPE] = mapped
            else:
                out[COL_GBP_POST_TYPE] = raw_gbp_upper

        # Drive file IDs parsing
        raw_drive = out.get(COL_DRIVE_FILE_ID) or ""
        drive_ids = [fid.strip() for fid in raw_drive.split(",") if fid.strip()]
        out["_drive_file_ids"] = drive_ids

        return issues

    # ─── Phase 2: Global Validation ──────────────────────────

    def _validate_global(
        self, normalized: dict,
    ) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []

        # Row ID
        row_id = normalized.get("id", "")
        if not row_id:
            issues.append(ValidationIssue(
                code=ErrorCode.ROW_MISSING_ID,
                message="Row has no id",
                severity="WARNING",
                field="id",
            ))

        # Status must be READY (or PROCESSING for re-validation in partial retry)
        status = normalized.get(COL_STATUS, "")
        if status not in (STATUS_READY, STATUS_PROCESSING):
            issues.append(ValidationIssue(
                code=ErrorCode.ROW_INVALID_STATUS,
                message=f"Status is {status!r}, expected READY",
                severity="ROW_BLOCK",
                field=COL_STATUS,
            ))

        # publish_at
        publish_at = normalized.get(COL_PUBLISH_AT, "")
        if not publish_at:
            issues.append(ValidationIssue(
                code=ErrorCode.ROW_PUBLISH_AT_MISSING,
                message="Missing publish_at",
                severity="ROW_BLOCK",
                field=COL_PUBLISH_AT,
            ))

        # Already fully posted check
        published_channels_str = normalized.get(COL_PUBLISHED_CHANNELS) or ""
        target_channels = normalized.get("_target_channels", [])
        if published_channels_str:
            already_published = {
                c.strip() for c in published_channels_str.split(",") if c.strip()
            }
            if all(cid in already_published for cid in target_channels):
                issues.append(ValidationIssue(
                    code=ErrorCode.ROW_ALREADY_POSTED,
                    message="All target channels already published",
                    severity="ROW_BLOCK",
                ))

        # Drive file IDs (media)
        # GBP supports text-only posts (no media required).
        # Only block the row if ALL target channels require media.
        _CHANNELS_SUPPORTING_TEXT_ONLY = {"GBP", "LI"}
        drive_ids: list[str] = normalized.get("_drive_file_ids", [])
        if not drive_ids:
            all_need_media = all(
                ch not in _CHANNELS_SUPPORTING_TEXT_ONLY
                for ch in target_channels
            )
            if all_need_media:
                issues.append(ValidationIssue(
                    code=ErrorCode.ROW_MEDIA_MISSING,
                    message="Missing drive_file_id",
                    severity="ROW_BLOCK",
                    field=COL_DRIVE_FILE_ID,
                ))
            else:
                # Some channels support text-only — media check deferred
                # to channel-level validation (IG/FB will be blocked there).
                pass

        # Carousel constraints
        is_carousel = len(drive_ids) > 1
        post_type = normalized.get(COL_POST_TYPE, POST_TYPE_FEED)

        if is_carousel and post_type == POST_TYPE_REELS:
            issues.append(ValidationIssue(
                code=ErrorCode.ROW_CAROUSEL_REELS,
                message="Carousel not supported for REELS — use FEED",
                severity="ROW_BLOCK",
                field=COL_POST_TYPE,
            ))

        if is_carousel and len(drive_ids) > 10:
            issues.append(ValidationIssue(
                code=ErrorCode.ROW_CAROUSEL_LIMIT,
                message=f"Carousel supports 2-10 items, got {len(drive_ids)}",
                severity="ROW_BLOCK",
                field=COL_DRIVE_FILE_ID,
            ))

        return issues

    # ─── Phase 3: Channel Validation ─────────────────────────

    def _validate_channel(
        self, channel_id: str, normalized: dict,
    ) -> ChannelValidationResult:
        dispatch = {
            "IG": self._validate_ig,
            "FB": self._validate_fb,
            "GBP": self._validate_gbp,
            "LI": self._validate_li,
        }
        validator_fn = dispatch.get(channel_id)
        if validator_fn is None:
            return ChannelValidationResult(channel=channel_id, approved=True)
        return validator_fn(normalized)

    def _validate_ig(self, n: dict) -> ChannelValidationResult:
        issues: list[ValidationIssue] = []

        # IG requires media
        drive_ids = n.get("_drive_file_ids", [])
        if not drive_ids:
            issues.append(ValidationIssue(
                code=ErrorCode.IG_MEDIA_MISSING,
                message="Instagram requires media",
                severity="CHANNEL_BLOCK",
                field=COL_DRIVE_FILE_ID,
                channel="IG",
            ))

        # IG requires caption
        caption = n.get(COL_CAPTION_IG) or n.get(COL_CAPTION) or ""
        if not caption:
            issues.append(ValidationIssue(
                code=ErrorCode.IG_CAPTION_MISSING,
                message="Missing caption for Instagram",
                severity="CHANNEL_BLOCK",
                field=COL_CAPTION_IG,
                channel="IG",
            ))

        blocked = any(i.severity == "CHANNEL_BLOCK" for i in issues)
        return ChannelValidationResult(channel="IG", approved=not blocked, issues=issues)

    def _validate_fb(self, n: dict) -> ChannelValidationResult:
        issues: list[ValidationIssue] = []

        # FB requires media
        drive_ids = n.get("_drive_file_ids", [])
        if not drive_ids:
            issues.append(ValidationIssue(
                code=ErrorCode.FB_MEDIA_MISSING,
                message="Facebook requires media",
                severity="CHANNEL_BLOCK",
                field=COL_DRIVE_FILE_ID,
                channel="FB",
            ))

        # FB requires caption
        caption = n.get(COL_CAPTION_FB) or n.get(COL_CAPTION) or ""
        if not caption:
            issues.append(ValidationIssue(
                code=ErrorCode.FB_CAPTION_MISSING,
                message="Missing caption for Facebook",
                severity="CHANNEL_BLOCK",
                field=COL_CAPTION_FB,
                channel="FB",
            ))

        blocked = any(i.severity == "CHANNEL_BLOCK" for i in issues)
        return ChannelValidationResult(channel="FB", approved=not blocked, issues=issues)

    def _validate_gbp(self, n: dict) -> ChannelValidationResult:
        issues: list[ValidationIssue] = []

        # google_location_id: from row or env var
        from config import GBP_DEFAULT_LOCATION_ID
        location_id = n.get(COL_GOOGLE_LOCATION_ID) or GBP_DEFAULT_LOCATION_ID
        if not location_id:
            issues.append(ValidationIssue(
                code=ErrorCode.GBP_LOCATION_MISSING,
                message="Missing google_location_id (set in row or GBP_DEFAULT_LOCATION_ID env var)",
                severity="CHANNEL_BLOCK",
                field=COL_GOOGLE_LOCATION_ID,
                channel="GBP",
            ))

        # gbp_post_type must be STANDARD (MVP)
        gbp_type = n.get(COL_GBP_POST_TYPE)
        if gbp_type and gbp_type != GBP_POST_TYPE_STANDARD:
            issues.append(ValidationIssue(
                code=ErrorCode.GBP_POST_TYPE_UNSUPPORTED,
                message=f"Unsupported gbp_post_type {gbp_type!r}. Only STANDARD supported.",
                severity="CHANNEL_BLOCK",
                field=COL_GBP_POST_TYPE,
                channel="GBP",
            ))

        # Caption: channel-specific → generic
        caption = n.get(COL_CAPTION_GBP) or n.get(COL_CAPTION) or ""
        if not caption:
            issues.append(ValidationIssue(
                code=ErrorCode.GBP_CAPTION_MISSING,
                message="Missing caption for GBP (no caption_gbp and no generic caption)",
                severity="CHANNEL_BLOCK",
                field=COL_CAPTION_GBP,
                channel="GBP",
            ))

        # CTA consistency
        cta_type = n.get(COL_CTA_TYPE)
        cta_url = n.get(COL_CTA_URL)
        if (cta_type and not cta_url) or (cta_url and not cta_type):
            issues.append(ValidationIssue(
                code=ErrorCode.GBP_CTA_INCOMPLETE,
                message="CTA requires both cta_type and cta_url",
                severity="CHANNEL_BLOCK",
                field=COL_CTA_TYPE if not cta_type else COL_CTA_URL,
                channel="GBP",
            ))

        blocked = any(i.severity == "CHANNEL_BLOCK" for i in issues)
        return ChannelValidationResult(channel="GBP", approved=not blocked, issues=issues)

    def _validate_li(self, n: dict) -> ChannelValidationResult:
        issues: list[ValidationIssue] = []

        # Author URN: from row, or fall back to env var
        from config import LI_AUTHOR_URN
        author_urn = n.get(COL_LI_AUTHOR_URN) or LI_AUTHOR_URN
        if not author_urn:
            issues.append(ValidationIssue(
                code=ErrorCode.LI_AUTHOR_URN_MISSING,
                message="Missing li_author_urn (set in row or LI_AUTHOR_URN env var)",
                severity="CHANNEL_BLOCK",
                field=COL_LI_AUTHOR_URN,
                channel="LI",
            ))
        elif not LI_URN_PATTERN.match(author_urn):
            issues.append(ValidationIssue(
                code=ErrorCode.LI_INVALID_AUTHOR_URN,
                message=(
                    f"Invalid author URN format: {author_urn!r}. "
                    f"Expected urn:li:person:{{id}} or urn:li:organization:{{id}}"
                ),
                severity="CHANNEL_BLOCK",
                field=COL_LI_AUTHOR_URN,
                channel="LI",
            ))

        # Caption: channel-specific → generic
        caption = n.get(COL_CAPTION_LI) or n.get(COL_CAPTION) or ""
        if not caption:
            issues.append(ValidationIssue(
                code=ErrorCode.LI_CAPTION_MISSING,
                message="Missing caption for LinkedIn (no caption_li and no generic caption)",
                severity="CHANNEL_BLOCK",
                field=COL_CAPTION_LI,
                channel="LI",
            ))
        elif len(caption) > LI_CAPTION_MAX_LENGTH:
            issues.append(ValidationIssue(
                code=ErrorCode.LI_CAPTION_TOO_LONG,
                message=(
                    f"LinkedIn caption too long — {len(caption)} characters "
                    f"(maximum {LI_CAPTION_MAX_LENGTH})"
                ),
                severity="CHANNEL_BLOCK",
                field=COL_CAPTION_LI,
                channel="LI",
            ))

        blocked = any(i.severity == "CHANNEL_BLOCK" for i in issues)
        return ChannelValidationResult(channel="LI", approved=not blocked, issues=issues)

    # ─── Phase 4: Aggregation ────────────────────────────────

    def _aggregate(
        self,
        issues: list[ValidationIssue],
        normalized: dict,
        channel_results: dict[str, ChannelValidationResult],
    ) -> ValidationReport:
        approved = [cid for cid, r in channel_results.items() if r.approved]
        blocked: dict[str, list[ValidationIssue]] = {
            cid: r.issues
            for cid, r in channel_results.items()
            if not r.approved
        }
        warnings = [i for i in issues if i.severity == "WARNING"]
        skipped = normalized.get("_skipped_channels", [])

        # If no channels approved → block the row
        row_blocked = len(approved) == 0

        if row_blocked and not any(i.severity == "ROW_BLOCK" for i in issues):
            issues.append(ValidationIssue(
                code=ErrorCode.COMMON_NO_PUBLISHABLE_CONTENT,
                message="No channels passed validation",
                severity="ROW_BLOCK",
            ))

        # Build normalized post_data for the publisher
        post_data = self._build_post_data(normalized)

        return ValidationReport(
            row_blocked=row_blocked,
            approved_channels=approved,
            blocked_channels=blocked,
            warnings=warnings,
            issues=issues,
            normalized_post_data=post_data,
            skipped_channels=skipped,
        )

    def _build_report(
        self,
        issues: list[ValidationIssue],
        normalized: dict,
        row_blocked: bool,
    ) -> ValidationReport:
        """Build a report for early-exit (row blocked before channel validation)."""
        warnings = [i for i in issues if i.severity == "WARNING"]
        skipped = normalized.get("_skipped_channels", [])
        return ValidationReport(
            row_blocked=row_blocked,
            approved_channels=[],
            blocked_channels={},
            warnings=warnings,
            issues=issues,
            normalized_post_data={},
            skipped_channels=skipped,
        )

    def _build_post_data(self, normalized: dict) -> dict:
        """Build the post_data dict that channels expect from normalized data."""
        return {
            "caption": normalized.get(COL_CAPTION) or "",
            COL_CAPTION_IG: normalized.get(COL_CAPTION_IG) or "",
            COL_CAPTION_FB: normalized.get(COL_CAPTION_FB) or "",
            COL_CAPTION_GBP: normalized.get(COL_CAPTION_GBP) or "",
            COL_CAPTION_LI: normalized.get(COL_CAPTION_LI) or "",
            COL_LI_AUTHOR_URN: normalized.get(COL_LI_AUTHOR_URN) or "",
            COL_GOOGLE_LOCATION_ID: normalized.get(COL_GOOGLE_LOCATION_ID) or "",
            COL_GBP_POST_TYPE: normalized.get(COL_GBP_POST_TYPE) or "",
            COL_CTA_TYPE: normalized.get(COL_CTA_TYPE) or "",
            COL_CTA_URL: normalized.get(COL_CTA_URL) or "",
            "post_type": normalized.get(COL_POST_TYPE) or POST_TYPE_FEED,
            "_drive_file_ids": normalized.get("_drive_file_ids", []),
        }


# ═══════════════════════════════════════════════════════════════
#  Convenience
# ═══════════════════════════════════════════════════════════════

def format_validation_error(report: ValidationReport) -> str:
    """Format a validation report into a human-readable error string for the sheet."""
    parts = []
    for issue in report.issues:
        if issue.severity == "ROW_BLOCK":
            parts.append(f"[{issue.code}] {issue.message}")
        elif issue.severity == "CHANNEL_BLOCK":
            parts.append(f"[{issue.code}] {issue.channel}: {issue.message}")
    return "; ".join(parts) if parts else "Unknown validation error"


def format_blocked_channels_error(report: ValidationReport) -> str:
    """Format blocked channel errors for the error column."""
    parts = []
    for cid, issues in report.blocked_channels.items():
        channel_errors = [f"[{i.code}] {i.message}" for i in issues if i.severity == "CHANNEL_BLOCK"]
        if channel_errors:
            parts.append(f"{cid}: {'; '.join(channel_errors)}")
    return " | ".join(parts)
