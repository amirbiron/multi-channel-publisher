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
    COL_DRIVE_FILE_ID,
    COL_CLOUDINARY_URL,
    COL_RESULT,
    COL_ERROR,
    COL_GOOGLE_LOCATION_ID,
    COL_LOCKED_AT,
    COL_PROCESSING_BY,
    COL_RETRY_COUNT,
    COL_PUBLISHED_CHANNELS,
    COL_FAILED_CHANNELS,
    STATUS_DRAFT,
    STATUS_READY,
    STATUS_IN_PROGRESS,
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
    CAPTION_COLUMNS_BY_CHANNEL,
    POST_TYPE_FEED,
    POST_TYPE_REELS,
    VIDEO_MIMES,
    PUBLISH_MAX_RETRIES,
    PUBLISH_RETRY_DELAY,
)
from google_api import (
    sheets_read_all_rows,
    sheets_read_row,
    sheets_update_cells,
    drive_download_with_metadata,
)
from cloud_storage import upload_to_cloudinary, delete_from_cloudinary
from media_processor import normalize_media, MediaProcessingError
from meta_publish import ig_publish_feed, fb_publish_feed, ig_publish_carousel
from notifications import notify_publish_error, notify_partial_success

# ─── Logging ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("social-publisher")


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


# ═══════════════════════════════════════════════════════════════
#  Process Single Row
# ═══════════════════════════════════════════════════════════════

def _publish_with_retry(publish_fn, *args, row_id: str, network_name: str) -> str:
    """
    מנסה לפרסם עם retry — עד PUBLISH_MAX_RETRIES ניסיונות.
    מחזיר את ה-result_id אם הצליח, אחרת מעלה את השגיאה האחרונה.
    """
    if PUBLISH_MAX_RETRIES < 1:
        raise ValueError("PUBLISH_MAX_RETRIES must be >= 1")

    last_error = None
    for attempt in range(1, PUBLISH_MAX_RETRIES + 1):
        try:
            return publish_fn(*args)
        except Exception as e:
            last_error = e
            if attempt < PUBLISH_MAX_RETRIES:
                delay = PUBLISH_RETRY_DELAY * (2 ** (attempt - 1))
                logger.warning(
                    f"Row {row_id}: {network_name} publish attempt {attempt}/{PUBLISH_MAX_RETRIES} "
                    f"failed: {e} — retrying in {delay}s..."
                )
                time.sleep(delay)
            else:
                logger.error(
                    f"Row {row_id}: {network_name} publish failed after {PUBLISH_MAX_RETRIES} attempts"
                )
    raise last_error


def process_row(
    row: list[str],
    header: list[str],
    sheet_row_number: int,
) -> bool:
    """
    מעבד שורה אחת: מאמת נעילה → מוריד → מעלה → מפרסם → מעדכן.
    תומך ב-network=IG+FB לפרסום לשתי הרשתות מאותה שורה.
    מחזיר True אם השורה עובדה בפועל, False אם דולגה.
    """
    row_id = get_cell(row, header, "id", default=str(sheet_row_number))

    try:
        # ── שלב 0: אימות נעילה (re-read מהטבלה) ──
        # בודקים שהסטטוס אכן IN_PROGRESS — אם ריצה מקבילה כבר תפסה את השורה, מדלגים
        fresh_row = sheets_read_row(sheet_row_number)
        fresh_status = get_cell(fresh_row, header, COL_STATUS).strip().upper()
        if fresh_status != STATUS_IN_PROGRESS:
            logger.warning(
                f"Row {row_id}: Status changed to {fresh_status!r} after locking — "
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
        targets = []
        if network in (NETWORK_IG, NETWORK_BOTH, NETWORK_IG_GBP, NETWORK_ALL_THREE, NETWORK_ALL):
            targets.append(NETWORK_IG)
        if network in (NETWORK_FB, NETWORK_BOTH, NETWORK_FB_GBP, NETWORK_ALL_THREE, NETWORK_ALL):
            targets.append(NETWORK_FB)
        # GBP publishing will be handled by the channel layer (Task 5+).
        has_gbp = network in (NETWORK_GBP, NETWORK_IG_GBP, NETWORK_FB_GBP, NETWORK_ALL_THREE, NETWORK_ALL)
        if has_gbp:
            logger.info(f"Row {row_id}: GBP channel not yet implemented — skipping GBP target")

        if not targets:
            # GBP-only row — nothing to publish yet; exit before expensive I/O
            _mark_error(header, sheet_row_number, "GBP channel not yet implemented")
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

        # ── שלב 4: פרסום ──
        results = {}
        errors = {}

        for target in targets:
            if target == NETWORK_IG:
                caption = caption_ig or caption_fb
                if is_carousel:
                    logger.info(f"Row {row_id}: Publishing carousel to Instagram ({len(cloud_urls)} items)...")
                    try:
                        results[NETWORK_IG] = _publish_with_retry(
                            ig_publish_carousel, cloud_urls, caption, mime_types,
                            row_id=row_id, network_name="IG",
                        )
                    except Exception as e:
                        errors[NETWORK_IG] = e
                else:
                    logger.info(f"Row {row_id}: Publishing to Instagram ({post_type})...")
                    try:
                        results[NETWORK_IG] = _publish_with_retry(
                            ig_publish_feed, cloud_urls[0], caption, mime_types[0], post_type,
                            row_id=row_id, network_name="IG",
                        )
                    except Exception as e:
                        errors[NETWORK_IG] = e
            else:
                caption = caption_fb or caption_ig
                if is_carousel:
                    # FB קרוסלה דורשת pages_manage_posts approved — fallback לתמונה ראשונה
                    logger.info(f"Row {row_id}: FB carousel not supported — publishing first item only...")
                    try:
                        results[NETWORK_FB] = _publish_with_retry(
                            fb_publish_feed, cloud_urls[0], caption, mime_types[0], post_type,
                            row_id=row_id, network_name="FB",
                        )
                    except Exception as e:
                        errors[NETWORK_FB] = e
                else:
                    logger.info(f"Row {row_id}: Publishing to Facebook ({post_type})...")
                    try:
                        results[NETWORK_FB] = _publish_with_retry(
                            fb_publish_feed, cloud_urls[0], caption, mime_types[0], post_type,
                            row_id=row_id, network_name="FB",
                        )
                    except Exception as e:
                        errors[NETWORK_FB] = e

        # ── שלב 5: סימון תוצאה ──
        if errors and not results:
            # כל הרשתות נכשלו
            raise list(errors.values())[0]

        # בניית מחרוזת תוצאה
        is_multi = len(targets) > 1
        result_parts = [f"{net}:{rid}" for net, rid in results.items()]
        result_str = " | ".join(result_parts) if is_multi else str(list(results.values())[0])

        if errors:
            # הצלחה חלקית — מסמנים ERROR עם פירוט מה הצליח ומה נכשל
            error_parts = []
            for net, err in errors.items():
                error_parts.append(f"{net}: {err}")
            error_detail = f"Partial success ({result_str}). Failures: {'; '.join(error_parts)}"
            logger.warning(f"Row {row_id}: PARTIAL — {error_detail}")
            notify_partial_success(row_id, result_str, "; ".join(error_parts))
            sheets_update_cells(
                sheet_row_number,
                {
                    COL_STATUS: STATUS_ERROR,
                    COL_CLOUDINARY_URL: cloud_urls_str,
                    COL_RESULT: result_str,
                    COL_ERROR: error_detail[:500],
                },
                header,
            )
        elif has_gbp:
            # IG/FB succeeded but GBP was skipped — mark PARTIAL so these
            # rows can be retried once GBP is implemented
            gbp_note = "GBP: skipped (not yet implemented)"
            logger.warning(f"Row {row_id}: PARTIAL — {result_str} | {gbp_note}")
            sheets_update_cells(
                sheet_row_number,
                {
                    COL_STATUS: STATUS_PARTIAL,
                    COL_CLOUDINARY_URL: cloud_urls_str,
                    COL_RESULT: result_str,
                    COL_ERROR: gbp_note,
                    COL_PUBLISHED_CHANNELS: ",".join(results.keys()),
                    COL_FAILED_CHANNELS: "GBP",
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
    # חותכים הודעות ארוכות מדי
    if len(error_msg) > 500:
        error_msg = error_msg[:497] + "..."

    sheets_update_cells(
        sheet_row_number,
        {COL_STATUS: STATUS_ERROR, COL_ERROR: error_msg},
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
    מוחק נכסים מ-Cloudinary עבור שורות POSTED
    שפורסמו לפני יותר מ-CLOUDINARY_RETENTION_DAYS ימים.
    מחזיר מספר הנכסים שנמחקו.
    """
    cutoff = now_utc - timedelta(days=CLOUDINARY_RETENTION_DAYS)
    deleted = 0

    for i, row in enumerate(rows, start=2):
        status = get_cell(row, header, COL_STATUS).strip().upper()
        if status != STATUS_POSTED:
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
#  Main
# ═══════════════════════════════════════════════════════════════

def main():
    logger.info("═" * 50)
    logger.info("Social Publisher — Run started")
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
        logger.info(f"Row {row_id}: Locking (IN_PROGRESS)")
        sheets_update_cells(i, {COL_STATUS: STATUS_IN_PROGRESS}, header)

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
