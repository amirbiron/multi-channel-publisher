"""
cloud_storage.py — העלאה ל-Cloudinary וקבלת URL ציבורי
"""

import logging
import tempfile

import cloudinary
import cloudinary.api
import cloudinary.uploader

import os

from config import (
    CLOUDINARY_CLOUD_NAME,
    CLOUDINARY_API_KEY,
    CLOUDINARY_API_SECRET,
    VIDEO_MIMES,
)

logger = logging.getLogger(__name__)

# ─── Init Cloudinary ─────────────────────────────────────────
# אם CLOUDINARY_URL מוגדר, ה-SDK קורא אותו אוטומטית.
# אחרת, מגדירים מהמשתנים הנפרדים.
if os.environ.get("CLOUDINARY_URL"):
    cloudinary.config(secure=True)
else:
    cloudinary.config(
        cloud_name=CLOUDINARY_CLOUD_NAME,
        api_key=CLOUDINARY_API_KEY,
        api_secret=CLOUDINARY_API_SECRET,
        secure=True,
    )


def upload_to_cloudinary(
    file_bytes: bytes,
    mime_type: str,
    file_name: str = "media",
) -> str:
    """
    מעלה קובץ (תמונה/וידאו) ל-Cloudinary.
    מחזיר secure_url (HTTPS).

    - לוידאו: resource_type="video"
    - לתמונה: resource_type="image"
    """
    is_video = mime_type in VIDEO_MIMES
    resource_type = "video" if is_video else "image"

    # סיומת לקובץ זמני (Cloudinary צריך לזהות פורמט)
    suffix = _get_suffix(mime_type)

    with tempfile.NamedTemporaryFile(suffix=suffix, delete=True) as tmp:
        tmp.write(file_bytes)
        tmp.flush()

        logger.info(
            f"Uploading to Cloudinary: {file_name} "
            f"({len(file_bytes)} bytes, {resource_type})"
        )

        result = cloudinary.uploader.upload(
            tmp.name,
            resource_type=resource_type,
            # folder אופציונלי — אפשר להוסיף לארגון
            folder="social-publisher",
            # public_id אופציונלי — Cloudinary ייצור אוטומטית
        )

    secure_url = result["secure_url"]
    logger.info(f"Cloudinary URL: {secure_url}")
    return secure_url


def delete_from_cloudinary(public_id: str, resource_type: str = "image") -> bool:
    """
    מוחק נכס מ-Cloudinary לפי public_id.
    מחזיר True אם הנכס נמחק בהצלחה.
    """
    logger.info(f"Deleting from Cloudinary: {public_id} ({resource_type})")
    result = cloudinary.uploader.destroy(public_id, resource_type=resource_type)
    ok = result.get("result") == "ok"
    if ok:
        logger.info(f"Deleted: {public_id}")
    else:
        logger.warning(f"Delete failed for {public_id}: {result}")
    return ok


def list_cloudinary_assets(
    resource_type: str = "image",
    prefix: str = "social-publisher",
    max_results: int = 50,
) -> list[dict]:
    """
    מחזיר רשימת נכסים מ-Cloudinary.
    ברירת מחדל: נכסים בתיקיית social-publisher.
    """
    result = cloudinary.api.resources(
        resource_type=resource_type,
        prefix=prefix,
        max_results=max_results,
    )
    return result.get("resources", [])


def _get_suffix(mime_type: str) -> str:
    """ממיר MIME type לסיומת קובץ."""
    mapping = {
        "video/mp4": ".mp4",
        "video/quicktime": ".mov",
        "video/x-msvideo": ".avi",
        "video/mpeg": ".mpeg",
        "video/webm": ".webm",
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/gif": ".gif",
        "image/webp": ".webp",
        "image/bmp": ".bmp",
    }
    return mapping.get(mime_type, ".bin")
