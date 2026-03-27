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
    COL_DRIVE_FILE_ID,
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
    NETWORK_IG,
    NETWORK_FB,
    NETWORK_GBP,
    NETWORK_BOTH,
    NETWORK_IG_GBP,
    NETWORK_FB_GBP,
    NETWORK_ALL_THREE,
    NETWORK_ALL,
    VALID_NETWORKS,
    POST_TYPE_FEED,
    POST_TYPE_REELS,
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


# Map network value → list of channel IDs it includes
_NETWORK_TO_CHANNELS = {
    NETWORK_IG: [NETWORK_IG],
    NETWORK_FB: [NETWORK_FB],
    NETWORK_GBP: [NETWORK_GBP],
    NETWORK_BOTH: [NETWORK_IG, NETWORK_FB],
    NETWORK_IG_GBP: [NETWORK_IG, NETWORK_GBP],
    NETWORK_FB_GBP: [NETWORK_FB, NETWORK_GBP],
    NETWORK_ALL_THREE: [NETWORK_IG, NETWORK_FB, NETWORK_GBP],
    NETWORK_ALL: [NETWORK_IG, NETWORK_FB, NETWORK_GBP],
}


def _resolve_targets(network: str) -> list[str]:
    """Resolve network to publishable channel IDs (only registered ones)."""
    requested = _NETWORK_TO_CHANNELS.get(network, [])
    registered_ids = set(_registry.channel_ids)
    return [cid for cid in requested if cid in registered_ids]


def _unregistered_channels(network: str) -> list[str]:
    """Return channel IDs requested by network but not yet registered."""
    requested = _NETWORK_TO_CHANNELS.get(network, [])
    registered_ids = set(_registry.channel_ids)
    return [cid for cid in requested if cid not in registered_ids]


# ═══════════════════════════════════════════════════════════════
#  Process Single Row
# ═══════════════════════════════════════════════════════════════

def _publish_channel_with_retry(
    channel, post_data: dict, *, row_id: str,
) -> PublishResult:
    """
    Publish to a single channel with retry logic.
    Returns a PublishResult (success or error).
    """
    if PUBLISH_MAX_RETRIES < 1:
        raise ValueError("PUBLISH_MAX_RETRIES must be >= 1")

    cid = channel.CHANNEL_ID
    last_result = None

    for attempt in range(1, PUBLISH_MAX_RETRIES + 1):
        result = channel.publish(post_data)
        if result.success:
            return result
        last_result = result
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
    מעבד שורה אחת: מאמת נעילה → מוריד → מעלה → מפרסם → מעדכן.
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

        network = get_cell(row, header, COL_NETWORK).strip().upper()
        post_type = get_cell(row, header, COL_POST_TYPE).strip().upper() or POST_TYPE_FEED
        drive_file_id = get_cell(row, header, COL_DRIVE_FILE_ID).strip()
        caption_generic = get_cell(row, header, COL_CAPTION)
        caption_ig = get_cell(row, header, COL_CAPTION_IG) or caption_generic
        caption_fb = get_cell(row, header, COL_CAPTION_FB) or caption_generic

        if not drive_file_id:
            _mark_error(header, sheet_row_number, "Missing drive_file_id")
            return True

        if network not in VALID_NETWORKS:
            _mark_error(header, sheet_row_number, f"Unknown network: {network}")
            return True

        # ── בדיקת ערוצי יעד לפני I/O — מונע העלאות מיותרות ──
        targets = _resolve_targets(network)
        skipped_channels = _unregistered_channels(network)
        for cid in skipped_channels:
            logger.info(f"Row {row_id}: {cid} channel not yet implemented — skipping")

        if not targets:
            # All requested channels are unregistered (e.g. GBP-only)
            _mark_error(
                header, sheet_row_number,
                f"{', '.join(skipped_channels)} channel not yet implemented",
            )
            return True

        # ── פירוק drive_file_id — תמיכה בקבצים מרובים (קרוסלה) ──
        drive_file_ids = [fid.strip() for fid in drive_file_id.split(",") if fid.strip()]
        if not drive_file_ids:
            _mark_error(header, sheet_row_number, "Missing drive_file_id")
            return True
        is_carousel = len(drive_file_ids) > 1

        if is_carousel and post_type == POST_TYPE_REELS:
            _mark_error(header, sheet_row_number, "Carousel not supported for REELS — use FEED")
            return True

        if is_carousel and len(drive_file_ids) > 10:
            _mark_error(header, sheet_row_number, f"Carousel supports 2-10 items, got {len(drive_file_ids)}")
            return True

        # ── שלב 2: הורדה מ-Drive + נרמול + העלאה לכל קובץ ──
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

        # ── שלב 4: פרסום דרך ה-registry ──
        # Build post_data dict that channels understand
        post_data = {
            "caption": caption_generic,
            COL_CAPTION_IG: caption_ig,
            COL_CAPTION_FB: caption_fb,
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

        # ── שלב 5: סימון תוצאה ──
        succeeded = {cid: r for cid, r in publish_results.items() if r.success}
        failed = {cid: r for cid, r in publish_results.items() if not r.success}

        if failed and not succeeded:
            # כל הערוצים נכשלו — build detailed error and raise
            error_parts = []
            for cid, r in failed.items():
                detail = f"{cid}: {r.error_message}"
                if r.raw_response:
                    detail += f" | API response: {r.raw_response}"
                error_parts.append(detail)
            raise RuntimeError("; ".join(error_parts))

        # בניית מחרוזת תוצאה
        is_multi = len(targets) > 1
        result_parts = [
            f"{cid}:{r.platform_post_id}" for cid, r in succeeded.items()
        ]
        result_str = (
            " | ".join(result_parts) if is_multi
            else str(list(succeeded.values())[0].platform_post_id)
        )

        if failed:
            # הצלחה חלקית בערוצים שפורסמו
            error_parts = [
                f"{cid}: {r.error_message}" for cid, r in failed.items()
            ]
            error_detail = f"Partial success ({result_str}). Failures: {'; '.join(error_parts)}"
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
                    COL_FAILED_CHANNELS: ",".join(failed.keys()),
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
    Reset rows stuck in PROCESSING beyond LOCK_TIMEOUT_MINUTES back to READY.
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

        logger.warning(
            f"Row {row_id}: PROCESSING lock timed out — resetting to READY "
            f"(retry_count {retry_count} → {retry_count + 1})"
        )
        sheets_update_cells(
            i,
            {
                COL_STATUS: STATUS_READY,
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
        network = get_cell(row, header, COL_NETWORK).strip().upper()
        post_type = get_cell(row, header, COL_POST_TYPE).strip().upper() or POST_TYPE_FEED
        caption_generic = get_cell(row, header, COL_CAPTION)
        caption_ig = get_cell(row, header, COL_CAPTION_IG) or caption_generic
        caption_fb = get_cell(row, header, COL_CAPTION_FB) or caption_generic
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
            COL_CAPTION_IG: caption_ig,
            COL_CAPTION_FB: caption_fb,
            "cloud_urls": cloud_urls,
            "mime_types": mime_types,
            "post_type": post_type,
        }

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
        all_published = already_published | newly_succeeded

        # Build updated result string
        existing_result = get_cell(row, header, COL_RESULT).strip()
        new_result_parts = [
            f"{cid}:{r.platform_post_id}" for cid, r in new_results.items() if r.success
        ]
        if new_result_parts:
            result_str = f"{existing_result} | {' | '.join(new_result_parts)}" if existing_result else " | ".join(new_result_parts)
        else:
            result_str = existing_result

        if still_failed:
            error_parts = [
                f"{cid}: {new_results[cid].error_message}" for cid in still_failed
            ]
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
