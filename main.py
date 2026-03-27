"""
main.py — סקריפט ראשי שרץ כ-Render Cron Job

זרימה:
1. קורא את Google Sheet → שורות READY שהגיע זמנן
2. לכל שורה: נועל → מוריד מ-Drive → מעלה ל-Cloudinary → מפרסם → מעדכן סטטוס
"""

import logging
import os
import re
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

from dateutil import parser as dtparser

from config import (
    TZ_IL,
    COL_STATUS,
    COL_NETWORK,
    COL_POST_TYPE,
    COL_PUBLISH_AT,
    COL_CAPTION,
    COL_CAPTION_IG,
    COL_CAPTION_FB,
    COL_CAPTION_GBP,
    COL_GBP_POST_TYPE,
    COL_CTA_TYPE,
    COL_CTA_URL,
    COL_GOOGLE_LOCATION_ID,
    COL_CLOUDINARY_URL,
    COL_RESULT,
    COL_ERROR,
    COL_PUBLISHED_CHANNELS,
    COL_FAILED_CHANNELS,
    STATUS_READY,
    STATUS_PROCESSING,
    STATUS_POSTED,
    STATUS_PARTIAL,
    STATUS_ERROR,
    POST_TYPE_FEED,
    PUBLISH_MAX_RETRIES,
    PUBLISH_RETRY_DELAY,
    COL_LOCKED_AT,
    COL_PROCESSING_BY,
    COL_RETRY_COUNT,
    LOCK_TIMEOUT_MINUTES,
)
from google_api import (
    sheets_read_all_rows,
    sheets_read_row,
    sheets_update_cells,
    drive_download_with_metadata,
)
from cloud_storage import upload_to_cloudinary, delete_from_cloudinary
from media_processor import normalize_media, MediaProcessingError
from channels import create_default_registry, PublishResult
from notifications import notify_publish_error, notify_partial_success
from validator import RowValidator, ValidationReport, format_validation_error, format_blocked_channels_error

# ─── Logging ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("social-publisher")

# ─── Channel Registry (singleton for the run) ────────────────
_registry = create_default_registry()

# ─── Validator (singleton for the run) ────────────────────────
_validator = RowValidator(registered_channel_ids=_registry.channel_ids)

# Unique run identifier — used for processing_by lock field
_RUN_ID = f"run_{uuid.uuid4().hex[:12]}"


# ═══════════════════════════════════════════════════════════════
#  Time Helpers
# ═══════════════════════════════════════════════════════════════

def is_due(publish_at_str: str, now_utc: datetime) -> bool:
    """
    בודק אם הגיע הזמן לפרסם.
    publish_at_str: תאריך+שעה בשעון ישראל (מהטבלה).
    now_utc: הזמן הנוכחי ב-UTC.
    """
    try:
        dt_il = dtparser.parse(publish_at_str)
    except (ValueError, TypeError):
        logger.warning(f"Invalid publish_at: {publish_at_str!r}")
        return False

    # אם אין timezone — מניחים ישראל
    if dt_il.tzinfo is None:
        dt_il = dt_il.replace(tzinfo=TZ_IL)

    dt_utc = dt_il.astimezone(timezone.utc)
    return dt_utc <= now_utc


# ═══════════════════════════════════════════════════════════════
#  Row Helpers
# ═══════════════════════════════════════════════════════════════

def get_cell(row: list[str], header: list[str], col_name: str, default: str = "") -> str:
    """שליפת ערך מהשורה לפי שם עמודה."""
    try:
        idx = header.index(col_name)
        return row[idx] if idx < len(row) else default
    except (ValueError, IndexError):
        return default


def _row_to_dict(row: list[str], header: list[str]) -> dict[str, str]:
    """Convert a sheet row + header into a dict for the validator."""
    return {col: get_cell(row, header, col) for col in header}


# ═══════════════════════════════════════════════════════════════
#  Process Single Row
# ═══════════════════════════════════════════════════════════════

def _publish_channel_with_retry(
    channel, post_data: dict, *, row_id: str,
) -> PublishResult:
    """
    Publish to a single channel with retry logic.
    Only retries transient (retryable) errors; non-retryable errors
    (validation, permissions, bad media) fail immediately.
    Returns a PublishResult (success or error).
    """
    from channels.base import BaseChannel

    if PUBLISH_MAX_RETRIES < 1:
        raise ValueError("PUBLISH_MAX_RETRIES must be >= 1")

    cid = channel.CHANNEL_ID
    last_result = None

    for attempt in range(1, PUBLISH_MAX_RETRIES + 1):
        result = channel.publish(post_data)
        if result.success:
            return result
        last_result = result

        # Non-retryable error → fail immediately, don't waste retries
        if not BaseChannel.is_retryable_error(result.error_code):
            logger.warning(
                f"Row {row_id}: {cid} failed with non-retryable error "
                f"'{result.error_code}': {result.error_message}"
            )
            return result

        if attempt < PUBLISH_MAX_RETRIES:
            delay = PUBLISH_RETRY_DELAY * (2 ** (attempt - 1))
            logger.warning(
                f"Row {row_id}: {cid} publish attempt {attempt}/{PUBLISH_MAX_RETRIES} "
                f"failed: {result.error_message} — retrying in {delay}s..."
            )
            time.sleep(delay)
        else:
            logger.error(
                f"Row {row_id}: {cid} publish failed after {PUBLISH_MAX_RETRIES} attempts"
            )

    return last_result


def process_row(
    row: list[str],
    header: list[str],
    sheet_row_number: int,
) -> bool:
    """
    מעבד שורה אחת: מאמת נעילה → validates → מוריד → מעלה → מפרסם → מעדכן.
    תומך ב-network מרובה לפרסום לכמה ערוצים מאותה שורה.
    מחזיר True אם השורה עובדה בפועל, False אם דולגה.
    """
    row_id = get_cell(row, header, "id", default=str(sheet_row_number))

    try:
        # ── שלב 0: אימות נעילה (re-read מהטבלה) ──
        fresh_row = sheets_read_row(sheet_row_number)
        fresh_status = get_cell(fresh_row, header, COL_STATUS).strip().upper()
        fresh_owner = get_cell(fresh_row, header, COL_PROCESSING_BY).strip()
        if fresh_status != STATUS_PROCESSING or fresh_owner != _RUN_ID:
            logger.warning(
                f"Row {row_id}: Lock lost (status={fresh_status!r}, "
                f"owner={fresh_owner!r}, expected={_RUN_ID!r}) — "
                f"another run may have claimed it. Skipping."
            )
            return False

        # ── שלב 1: Validation — before any I/O ──
        row_data = _row_to_dict(row, header)
        report = _validator.validate(row_data)

        # Log warnings
        for w in report.warnings:
            logger.info(f"Row {row_id}: [WARN] {w.code}: {w.message}")

        if report.row_blocked:
            error_msg = format_validation_error(report)
            logger.warning(f"Row {row_id}: Validation blocked — {error_msg}")
            _mark_error(header, sheet_row_number, error_msg)
            return True

        # Log channel-level blocks
        for cid, issues in report.blocked_channels.items():
            for issue in issues:
                logger.warning(f"Row {row_id}: {cid} blocked — [{issue.code}] {issue.message}")

        targets = report.approved_channels
        skipped_channels = report.skipped_channels
        validation_blocked_channels = list(report.blocked_channels.keys())
        post_data_norm = report.normalized_post_data

        for cid in skipped_channels:
            logger.info(f"Row {row_id}: {cid} channel not yet implemented — skipping")

        # ── שלב 2: הורדה מ-Drive + נרמול + העלאה לכל קובץ ──
        drive_file_ids = post_data_norm.get("_drive_file_ids", [])
        post_type = post_data_norm.get("post_type", POST_TYPE_FEED)
        is_carousel = len(drive_file_ids) > 1

        cloud_urls = []
        mime_types = []

        for idx, fid in enumerate(drive_file_ids):
            file_label = f"{idx+1}/{len(drive_file_ids)}" if is_carousel else ""
            logger.info(f"Row {row_id}: Downloading from Drive {file_label} ({fid})")
            file_bytes, metadata = drive_download_with_metadata(fid)
            mime_type = metadata.get("mimeType", "image/jpeg")
            file_name = metadata.get("name", "unknown")

            logger.info(
                f"Row {row_id}: File {file_label} '{file_name}' | MIME: {mime_type} | "
                f"Size: {len(file_bytes)} bytes"
            )

            # נרמול מדיה
            logger.info(f"Row {row_id}: Normalizing media {file_label}...")
            file_bytes, mime_type, file_name = normalize_media(
                file_bytes, mime_type, file_name, post_type
            )

            # העלאה ל-Cloudinary
            logger.info(f"Row {row_id}: Uploading to Cloudinary {file_label}...")
            cloud_url = upload_to_cloudinary(file_bytes, mime_type, file_name)
            cloud_urls.append(cloud_url)
            mime_types.append(mime_type)

        # שמירת כל ה-URLs לטבלה (מופרדים בפסיק)
        cloud_urls_str = ",".join(cloud_urls)

        # ── שלב 3: פרסום דרך ה-registry ──
        # Build post_data dict that channels understand (add cloud_urls from upload)
        post_data = {
            "caption": post_data_norm.get("caption", ""),
            COL_CAPTION_IG: post_data_norm.get(COL_CAPTION_IG, ""),
            COL_CAPTION_FB: post_data_norm.get(COL_CAPTION_FB, ""),
            COL_CAPTION_GBP: post_data_norm.get(COL_CAPTION_GBP, ""),
            COL_GOOGLE_LOCATION_ID: post_data_norm.get(COL_GOOGLE_LOCATION_ID, ""),
            COL_GBP_POST_TYPE: post_data_norm.get(COL_GBP_POST_TYPE, ""),
            COL_CTA_TYPE: post_data_norm.get(COL_CTA_TYPE, ""),
            COL_CTA_URL: post_data_norm.get(COL_CTA_URL, ""),
            "cloud_urls": cloud_urls,
            "mime_types": mime_types,
            "post_type": post_type,
        }

        publish_results: dict[str, PublishResult] = {}
        for cid in targets:
            channel = _registry.get(cid)
            logger.info(f"Row {row_id}: Publishing to {cid} ({channel.CHANNEL_NAME})...")
            try:
                publish_results[cid] = _publish_channel_with_retry(
                    channel, post_data, row_id=row_id,
                )
            except Exception as exc:
                logger.exception(f"Row {row_id}: Unexpected error publishing to {cid}")
                publish_results[cid] = PublishResult(
                    channel=cid,
                    success=False,
                    status="ERROR",
                    error_code="unexpected_error",
                    error_message=str(exc)[:500],
                )

        # ── שלב 4: סימון תוצאה ──
        succeeded = {cid: r for cid, r in publish_results.items() if r.success}
        failed = {cid: r for cid, r in publish_results.items() if not r.success}

        # Combine publish failures with validation-blocked channels
        all_failed_channels = list(failed.keys()) + validation_blocked_channels

        if failed and not succeeded:
            # כל הערוצים נכשלו — build detailed error and raise
            error_parts = []
            for cid, r in failed.items():
                detail = f"{cid}: {r.error_message}"
                if r.raw_response:
                    detail += f" | API response: {r.raw_response}"
                error_parts.append(detail)
            if validation_blocked_channels:
                blocked_err = format_blocked_channels_error(report)
                if blocked_err:
                    error_parts.append(f"Validation blocked: {blocked_err}")
            raise RuntimeError("; ".join(error_parts))

        # בניית מחרוזת תוצאה — פורמט: CHANNEL:STATUS:detail
        is_multi = len(targets) > 1 or validation_blocked_channels
        result_parts = []
        for cid, r in succeeded.items():
            result_parts.append(f"{cid}:POSTED:{r.platform_post_id}")
        for cid, r in failed.items():
            result_parts.append(f"{cid}:ERROR:{r.error_code}")
        for cid in validation_blocked_channels:
            result_parts.append(f"{cid}:BLOCKED:validation")
        result_str = (
            " | ".join(result_parts) if is_multi
            else str(list(succeeded.values())[0].platform_post_id)
        )

        if failed or validation_blocked_channels:
            # הצלחה חלקית — ערוצים שנכשלו בפרסום או נחסמו בולידציה
            error_parts = []
            for cid, r in failed.items():
                error_parts.append(f"{cid}: [{r.error_code}] {r.error_message}")
            for cid in validation_blocked_channels:
                ch_issues = report.blocked_channels.get(cid, [])
                for issue in ch_issues:
                    if issue.severity == "CHANNEL_BLOCK":
                        error_parts.append(f"{cid}: [{issue.code}] {issue.message}")
            error_detail = f"Partial success. Failures: {'; '.join(error_parts)}"
            logger.warning(f"Row {row_id}: PARTIAL — {error_detail}")
            notify_partial_success(row_id, result_str, "; ".join(error_parts))
            sheets_update_cells(
                sheet_row_number,
                {
                    COL_STATUS: STATUS_PARTIAL,
                    COL_CLOUDINARY_URL: cloud_urls_str,
                    COL_RESULT: result_str,
                    COL_ERROR: error_detail[:500],
                    COL_PUBLISHED_CHANNELS: ",".join(succeeded.keys()),
                    COL_FAILED_CHANNELS: ",".join(all_failed_channels),
                    COL_LOCKED_AT: "",
                    COL_PROCESSING_BY: "",
                },
                header,
            )
        elif skipped_channels:
            # All registered channels succeeded, but some were skipped
            skipped_note = f"{','.join(skipped_channels)}: skipped (not yet implemented)"
            logger.warning(f"Row {row_id}: PARTIAL — {result_str} | {skipped_note}")
            sheets_update_cells(
                sheet_row_number,
                {
                    COL_STATUS: STATUS_PARTIAL,
                    COL_CLOUDINARY_URL: cloud_urls_str,
                    COL_RESULT: result_str,
                    COL_ERROR: skipped_note,
                    COL_PUBLISHED_CHANNELS: ",".join(succeeded.keys()),
                    COL_FAILED_CHANNELS: ",".join(skipped_channels),
                    COL_LOCKED_AT: "",
                    COL_PROCESSING_BY: "",
                },
                header,
            )
        else:
            sheets_update_cells(
                sheet_row_number,
                {
                    COL_STATUS: STATUS_POSTED,
                    COL_CLOUDINARY_URL: cloud_urls_str,
                    COL_RESULT: result_str,
                    COL_ERROR: "",
                    COL_PUBLISHED_CHANNELS: ",".join(succeeded.keys()),
                    COL_FAILED_CHANNELS: "",
                    COL_LOCKED_AT: "",
                    COL_PROCESSING_BY: "",
                },
                header,
            )
            logger.info(f"Row {row_id}: POSTED successfully ({result_str})")

    except Exception as e:
        error_detail = (
            f"[{e.error_code}] {e}" if isinstance(e, MediaProcessingError)
            else str(e)
        )
        # Extract Meta API error details from response body
        if hasattr(e, "response") and e.response is not None:
            try:
                error_detail += f" | Meta response: {e.response.text}"
            except Exception:
                pass
        logger.error(f"Row {row_id}: ERROR — {error_detail}", exc_info=True)
        notify_publish_error(row_id, error_detail)
        try:
            _mark_error(header, sheet_row_number, error_detail)
        except Exception as mark_err:
            logger.error(f"Row {row_id}: Failed to mark error in sheet: {mark_err}")

    return True


def _mark_error(header: list[str], sheet_row_number: int, error_msg: str):
    """מסמן שורה כ-ERROR בטבלה."""
    if len(error_msg) > 500:
        error_msg = error_msg[:497] + "..."

    sheets_update_cells(
        sheet_row_number,
        {
            COL_STATUS: STATUS_ERROR,
            COL_ERROR: error_msg,
            COL_LOCKED_AT: "",
            COL_PROCESSING_BY: "",
        },
        header,
    )


# ═══════════════════════════════════════════════════════════════
#  Cloudinary Cleanup
# ═══════════════════════════════════════════════════════════════

CLOUDINARY_RETENTION_DAYS = int(os.environ.get("CLOUDINARY_RETENTION_DAYS", "10"))

# חילוץ public_id מ-URL של Cloudinary
# https://res.cloudinary.com/CLOUD/image/upload/v123/social-publisher/abc.jpg
#   → social-publisher/abc
_CLOUDINARY_URL_RE = re.compile(
    r"https?://res\.cloudinary\.com/[^/]+/(?P<rtype>image|video)/upload/(?:v\d+/)?(?P<pid>.+)\.\w+$"
)


def cleanup_old_cloudinary_assets(
    header: list[str],
    rows: list[list[str]],
    now_utc: datetime,
) -> int:
    """
    מוחק נכסים מ-Cloudinary עבור שורות POSTED או PARTIAL
    שפורסמו לפני יותר מ-CLOUDINARY_RETENTION_DAYS ימים.
    מחזיר מספר הנכסים שנמחקו.
    """
    cutoff = now_utc - timedelta(days=CLOUDINARY_RETENTION_DAYS)
    deleted = 0

    for i, row in enumerate(rows, start=2):
        status = get_cell(row, header, COL_STATUS).strip().upper()
        if status not in (STATUS_POSTED, STATUS_PARTIAL):
            continue

        cloud_url = get_cell(row, header, COL_CLOUDINARY_URL).strip()
        if not cloud_url:
            continue

        publish_at = get_cell(row, header, COL_PUBLISH_AT).strip()
        if not publish_at:
            continue

        # בדיקה אם עברו מספיק ימים
        try:
            dt_il = dtparser.parse(publish_at)
        except (ValueError, TypeError):
            continue

        if dt_il.tzinfo is None:
            dt_il = dt_il.replace(tzinfo=TZ_IL)

        if dt_il.astimezone(timezone.utc) > cutoff:
            continue

        # חילוץ public_id ו-resource_type מכל URL (תמיכה בקרוסלה עם URLs מופרדים בפסיק)
        urls = [u.strip() for u in cloud_url.split(",") if u.strip()]
        all_deleted = True

        for url in urls:
            match = _CLOUDINARY_URL_RE.match(url)
            if not match:
                logger.warning(f"Row {i}: Cannot parse Cloudinary URL: {url}")
                all_deleted = False
                continue

            public_id = match.group("pid")
            resource_type = match.group("rtype")

            logger.info(f"Row {i}: Deleting old asset {public_id} ({resource_type})")
            if delete_from_cloudinary(public_id, resource_type=resource_type):
                deleted += 1
            else:
                all_deleted = False

        if all_deleted:
            sheets_update_cells(i, {COL_CLOUDINARY_URL: ""}, header)

    return deleted


# ═══════════════════════════════════════════════════════════════
#  Lock Timeout Recovery
# ═══════════════════════════════════════════════════════════════

def recover_stale_locks(
    header: list[str],
    rows: list[list[str]],
    now_utc: datetime,
) -> int:
    """
    Reset rows stuck in PROCESSING beyond LOCK_TIMEOUT_MINUTES.
    Rows with published_channels are restored to PARTIAL (not READY)
    to avoid re-publishing already-succeeded channels.
    Increments retry_count for each recovered row.
    Returns the number of rows recovered.
    """
    cutoff = now_utc - timedelta(minutes=LOCK_TIMEOUT_MINUTES)
    recovered = 0

    for i, row in enumerate(rows, start=2):
        status = get_cell(row, header, COL_STATUS).strip().upper()
        if status != STATUS_PROCESSING:
            continue

        locked_at_str = get_cell(row, header, COL_LOCKED_AT).strip()
        if not locked_at_str:
            # Legacy row without locked_at — treat as stale
            pass
        else:
            try:
                locked_at = datetime.fromisoformat(locked_at_str)
                if locked_at.tzinfo is None:
                    locked_at = locked_at.replace(tzinfo=timezone.utc)
                if locked_at > cutoff:
                    continue  # still within timeout window
            except (ValueError, TypeError):
                pass  # unparseable — treat as stale

        row_id = get_cell(row, header, "id", default=str(i))
        retry_count_str = get_cell(row, header, COL_RETRY_COUNT).strip()
        retry_count = int(retry_count_str) if retry_count_str.isdigit() else 0

        # If some channels already published, restore to PARTIAL to avoid
        # re-publishing them via the READY loop.
        published = get_cell(row, header, COL_PUBLISHED_CHANNELS).strip()
        restore_status = STATUS_PARTIAL if published else STATUS_READY

        logger.warning(
            f"Row {row_id}: PROCESSING lock timed out — resetting to {restore_status} "
            f"(retry_count {retry_count} → {retry_count + 1})"
        )
        sheets_update_cells(
            i,
            {
                COL_STATUS: restore_status,
                COL_LOCKED_AT: "",
                COL_PROCESSING_BY: "",
                COL_RETRY_COUNT: str(retry_count + 1),
            },
            header,
        )
        recovered += 1

    return recovered


# ═══════════════════════════════════════════════════════════════
#  Retry PARTIAL — republish only failed channels
# ═══════════════════════════════════════════════════════════════

def process_partial_row(
    row: list[str],
    header: list[str],
    sheet_row_number: int,
) -> bool:
    """
    Retry publishing for a PARTIAL row — only republish channels listed
    in failed_channels, skip already-succeeded channels.
    Returns True if the row was processed.
    """
    row_id = get_cell(row, header, "id", default=str(sheet_row_number))
    failed_channels_str = get_cell(row, header, COL_FAILED_CHANNELS).strip()
    published_channels_str = get_cell(row, header, COL_PUBLISHED_CHANNELS).strip()

    if not failed_channels_str:
        logger.info(f"Row {row_id}: PARTIAL but no failed_channels recorded — skipping retry")
        return False

    retry_targets = [c.strip() for c in failed_channels_str.split(",") if c.strip()]
    already_published = set(
        c.strip() for c in published_channels_str.split(",") if c.strip()
    )

    # Filter to only registered channels
    registered_ids = set(_registry.channel_ids)
    retry_targets = [cid for cid in retry_targets if cid in registered_ids]

    if not retry_targets:
        logger.info(f"Row {row_id}: No retryable channels registered — skipping")
        return False

    # Lock the row
    logger.info(f"Row {row_id}: Retrying PARTIAL — channels: {retry_targets}")
    sheets_update_cells(
        sheet_row_number,
        {
            COL_STATUS: STATUS_PROCESSING,
            COL_LOCKED_AT: datetime.now(timezone.utc).isoformat(),
            COL_PROCESSING_BY: _RUN_ID,
        },
        header,
    )

    # Re-read to verify lock ownership
    fresh_row = sheets_read_row(sheet_row_number)
    fresh_status = get_cell(fresh_row, header, COL_STATUS).strip().upper()
    fresh_owner = get_cell(fresh_row, header, COL_PROCESSING_BY).strip()
    if fresh_status != STATUS_PROCESSING or fresh_owner != _RUN_ID:
        logger.warning(
            f"Row {row_id}: Lock lost (status={fresh_status!r}, "
            f"owner={fresh_owner!r}, expected={_RUN_ID!r}) — skipping"
        )
        return False

    try:
        # ── Validate retry targets using the validator ──
        # Use original row but override status to PROCESSING (we just locked it)
        # to avoid the validator blocking on PARTIAL status.
        row_data = _row_to_dict(row, header)
        row_data[COL_STATUS] = STATUS_PROCESSING
        report = _validator.validate(row_data)
        post_data_norm = report.normalized_post_data

        network = get_cell(row, header, COL_NETWORK).strip().upper()
        post_type = post_data_norm.get("post_type", POST_TYPE_FEED) if post_data_norm else (
            get_cell(row, header, COL_POST_TYPE).strip().upper() or POST_TYPE_FEED
        )
        caption_generic = post_data_norm.get("caption", "") if post_data_norm else get_cell(row, header, COL_CAPTION)
        cloud_urls_str = get_cell(row, header, COL_CLOUDINARY_URL).strip()
        cloud_urls = [u.strip() for u in cloud_urls_str.split(",") if u.strip()]

        # Determine mime types from URLs (best-effort)
        mime_types = []
        for url in cloud_urls:
            if any(url.lower().endswith(ext) for ext in (".mp4", ".mov", ".avi")):
                mime_types.append("video/mp4")
            else:
                mime_types.append("image/jpeg")

        post_data = {
            "caption": caption_generic,
            COL_CAPTION_IG: post_data_norm.get(COL_CAPTION_IG, "") if post_data_norm else (get_cell(row, header, COL_CAPTION_IG) or caption_generic),
            COL_CAPTION_FB: post_data_norm.get(COL_CAPTION_FB, "") if post_data_norm else (get_cell(row, header, COL_CAPTION_FB) or caption_generic),
            COL_CAPTION_GBP: post_data_norm.get(COL_CAPTION_GBP, "") if post_data_norm else get_cell(row, header, COL_CAPTION_GBP),
            COL_GOOGLE_LOCATION_ID: post_data_norm.get(COL_GOOGLE_LOCATION_ID, "") if post_data_norm else get_cell(row, header, COL_GOOGLE_LOCATION_ID).strip(),
            COL_GBP_POST_TYPE: post_data_norm.get(COL_GBP_POST_TYPE, "") if post_data_norm else get_cell(row, header, COL_GBP_POST_TYPE).strip(),
            COL_CTA_TYPE: post_data_norm.get(COL_CTA_TYPE, "") if post_data_norm else get_cell(row, header, COL_CTA_TYPE).strip(),
            COL_CTA_URL: post_data_norm.get(COL_CTA_URL, "") if post_data_norm else get_cell(row, header, COL_CTA_URL).strip(),
            "cloud_urls": cloud_urls,
            "mime_types": mime_types,
            "post_type": post_type,
        }

        # Filter retry targets: skip channels blocked by validation
        validation_blocked_retry = []
        if not report.row_blocked:
            for cid in list(retry_targets):
                if cid in report.blocked_channels:
                    validation_blocked_retry.append(cid)
                    retry_targets.remove(cid)
                    for issue in report.blocked_channels[cid]:
                        logger.warning(
                            f"Row {row_id}: {cid} still blocked on retry — "
                            f"[{issue.code}] {issue.message}"
                        )

        # Publish only to failed channels
        new_results: dict[str, PublishResult] = {}
        for cid in retry_targets:
            if cid in already_published:
                logger.info(f"Row {row_id}: {cid} already published — skipping")
                continue
            channel = _registry.get(cid)
            logger.info(f"Row {row_id}: Retrying {cid} ({channel.CHANNEL_NAME})...")
            try:
                new_results[cid] = _publish_channel_with_retry(
                    channel, post_data, row_id=row_id,
                )
            except Exception as exc:
                logger.exception(f"Row {row_id}: Unexpected error retrying {cid}")
                new_results[cid] = PublishResult(
                    channel=cid,
                    success=False,
                    status="ERROR",
                    error_code="unexpected_error",
                    error_message=str(exc)[:500],
                )

        # Merge results
        newly_succeeded = {cid for cid, r in new_results.items() if r.success}
        still_failed = {cid for cid, r in new_results.items() if not r.success}
        # Include validation-blocked channels as still-failed
        still_failed |= set(validation_blocked_retry)
        all_published = already_published | newly_succeeded

        # Build updated result string — same CHANNEL:STATUS:detail format as process_row
        # Remove stale entries for retried channels before appending new results
        existing_result = get_cell(row, header, COL_RESULT).strip()
        retried_cids = set(new_results.keys()) | set(validation_blocked_retry)
        if existing_result:
            kept_parts = [
                part.strip() for part in existing_result.split("|")
                if part.strip().split(":")[0] not in retried_cids
            ]
        else:
            kept_parts = []

        new_result_parts = []
        for cid, r in new_results.items():
            if r.success:
                new_result_parts.append(f"{cid}:POSTED:{r.platform_post_id}")
            else:
                new_result_parts.append(f"{cid}:ERROR:{r.error_code}")
        for cid in validation_blocked_retry:
            new_result_parts.append(f"{cid}:BLOCKED:validation")

        result_str = " | ".join(kept_parts + new_result_parts)

        if still_failed:
            error_parts = []
            for cid in still_failed:
                if cid in new_results:
                    r = new_results[cid]
                    error_parts.append(f"{cid}: [{r.error_code}] {r.error_message}")
                elif cid in validation_blocked_retry and not report.row_blocked:
                    ch_issues = report.blocked_channels.get(cid, [])
                    for issue in ch_issues:
                        if issue.severity == "CHANNEL_BLOCK":
                            error_parts.append(f"{cid}: [{issue.code}] {issue.message}")
                else:
                    error_parts.append(f"{cid}: validation blocked")
            sheets_update_cells(
                sheet_row_number,
                {
                    COL_STATUS: STATUS_PARTIAL,
                    COL_RESULT: result_str,
                    COL_ERROR: f"Retry partial. Still failed: {'; '.join(error_parts)}"[:500],
                    COL_PUBLISHED_CHANNELS: ",".join(sorted(all_published)),
                    COL_FAILED_CHANNELS: ",".join(sorted(still_failed)),
                    COL_LOCKED_AT: "",
                    COL_PROCESSING_BY: "",
                },
                header,
            )
            logger.warning(f"Row {row_id}: Still PARTIAL after retry — {still_failed}")
        else:
            sheets_update_cells(
                sheet_row_number,
                {
                    COL_STATUS: STATUS_POSTED,
                    COL_RESULT: result_str,
                    COL_ERROR: "",
                    COL_PUBLISHED_CHANNELS: ",".join(sorted(all_published)),
                    COL_FAILED_CHANNELS: "",
                    COL_LOCKED_AT: "",
                    COL_PROCESSING_BY: "",
                },
                header,
            )
            logger.info(f"Row {row_id}: Retry succeeded — now POSTED ({result_str})")

    except Exception as e:
        error_detail = str(e)
        logger.error(f"Row {row_id}: PARTIAL retry ERROR — {error_detail}", exc_info=True)
        try:
            _mark_error(header, sheet_row_number, f"Partial retry failed: {error_detail}")
        except Exception as mark_err:
            logger.error(f"Row {row_id}: Failed to mark error in sheet: {mark_err}")

    return True


# ═══════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════

def main():
    logger.info("═" * 50)
    logger.info("Multi-Channel Publisher — Run started")
    logger.info(f"Registered channels: {_registry.channel_ids}")
    logger.info("═" * 50)

    now_utc = datetime.now(timezone.utc)
    logger.info(f"Current UTC: {now_utc.isoformat()}")
    logger.info(
        f"Current Israel: "
        f"{now_utc.astimezone(TZ_IL).strftime('%Y-%m-%d %H:%M:%S')}"
    )

    # ── קריאת הטבלה ──
    header, rows = sheets_read_all_rows()

    if not header:
        logger.warning("Sheet is empty or header is missing.")
        return

    logger.info(f"Sheet has {len(rows)} data rows. Header: {header}")

    # ── שחרור נעילות תקועות ──
    recovered = recover_stale_locks(header, rows, now_utc)
    if recovered:
        logger.info(f"Lock recovery: reset {recovered} stale PROCESSING row(s) to READY")
        # Re-read after recovery so the loop sees updated statuses
        header, rows = sheets_read_all_rows()

    # ── retry שורות PARTIAL (רק ערוצים שנכשלו) ──
    partial_retried = 0
    for i, row in enumerate(rows, start=2):
        status = get_cell(row, header, COL_STATUS).strip().upper()
        if status != STATUS_PARTIAL:
            continue
        if process_partial_row(row, header, i):
            partial_retried += 1

    if partial_retried:
        logger.info(f"Partial retry: processed {partial_retried} PARTIAL row(s)")

    # ── סינון שורות שמוכנות לפרסום ──
    processed = 0
    skipped = 0

    for i, row in enumerate(rows, start=2):  # start=2 כי שורה 1 = header
        status = get_cell(row, header, COL_STATUS).strip().upper()

        if status != STATUS_READY:
            continue

        publish_at = get_cell(row, header, COL_PUBLISH_AT).strip()
        if not publish_at:
            logger.debug(f"Row {i}: No publish_at, skipping.")
            skipped += 1
            continue

        if not is_due(publish_at, now_utc):
            skipped += 1
            continue

        # ── נעילה מיידית לפני עיבוד — מצמצם race condition ──
        row_id = get_cell(row, header, "id", default=str(i))
        logger.info(f"Row {row_id}: Locking (PROCESSING) by {_RUN_ID}")
        sheets_update_cells(
            i,
            {
                COL_STATUS: STATUS_PROCESSING,
                COL_LOCKED_AT: datetime.now(timezone.utc).isoformat(),
                COL_PROCESSING_BY: _RUN_ID,
            },
            header,
        )

        # ── מעבד את השורה ──
        if process_row(row, header, i):
            processed += 1

    logger.info(f"Done. Processed: {processed}, Skipped (not due): {skipped}")

    # ── ניקוי נכסים ישנים מ-Cloudinary ──
    deleted = cleanup_old_cloudinary_assets(header, rows, now_utc)
    if deleted:
        logger.info(f"Cloudinary cleanup: deleted {deleted} old asset(s)")


if __name__ == "__main__":
    main()
