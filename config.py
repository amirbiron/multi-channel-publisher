"""
config.py — הגדרות סביבה וקבועים

Pure constants (column names, status values, etc.) live in config_constants.py
and are re-exported here so existing imports keep working.
"""

import os
import json

# Re-export all pure constants
from config_constants import *  # noqa: F401,F403

# ─── Google ──────────────────────────────────────────────────
GOOGLE_SA_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]  # כל ה-JSON כמחרוזת
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
SHEET_NAME = os.environ.get("SHEET_NAME", "Sheet1")

# ─── Meta / Facebook / Instagram ─────────────────────────────
META_API_VERSION = os.environ.get("META_API_VERSION", "v21.0")
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"

IG_USER_ID = os.environ.get("IG_USER_ID", "")
FB_PAGE_ID = os.environ.get("FB_PAGE_ID", "")

# טוקנים — אפשר להשתמש באותו טוקן אם יש לו הרשאות לשניהם
IG_ACCESS_TOKEN = os.environ.get("IG_ACCESS_TOKEN", "")
FB_PAGE_ACCESS_TOKEN = os.environ.get("FB_PAGE_ACCESS_TOKEN", "")

# ─── Cloudinary ──────────────────────────────────────────────
# אפשרות 1 (מועדפת): CLOUDINARY_URL=cloudinary://API_KEY:API_SECRET@CLOUD_NAME
#   ה-SDK קורא את CLOUDINARY_URL אוטומטית
# אפשרות 2: שלושה משתנים נפרדים
CLOUDINARY_CLOUD_NAME = os.environ.get("CLOUDINARY_CLOUD_NAME")
CLOUDINARY_API_KEY = os.environ.get("CLOUDINARY_API_KEY")
CLOUDINARY_API_SECRET = os.environ.get("CLOUDINARY_API_SECRET")

# וידוא שיש לפחות אחת מהאפשרויות
if not os.environ.get("CLOUDINARY_URL") and not all(
    [CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY, CLOUDINARY_API_SECRET]
):
    raise RuntimeError(
        "Cloudinary credentials missing. Set CLOUDINARY_URL or "
        "CLOUDINARY_CLOUD_NAME + CLOUDINARY_API_KEY + CLOUDINARY_API_SECRET"
    )

# ─── Google Scopes ───────────────────────────────────────────
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

# ─── Google Business Profile — OAuth 2.0 ─────────────────
GBP_ACCOUNT_ID = os.environ.get("GBP_ACCOUNT_ID", "")            # e.g. "accounts/123456789"
GBP_DEFAULT_LOCATION_ID = os.environ.get("GBP_DEFAULT_LOCATION_ID", "")  # e.g. "locations/123456789"
GBP_OAUTH_CLIENT_ID = os.environ.get("GBP_OAUTH_CLIENT_ID", "")
GBP_OAUTH_CLIENT_SECRET = os.environ.get("GBP_OAUTH_CLIENT_SECRET", "")
GBP_REFRESH_TOKEN = os.environ.get("GBP_REFRESH_TOKEN", "")

# ─── LinkedIn — OAuth 2.0 ────────────────────────────────
LI_OAUTH_CLIENT_ID = os.environ.get("LI_OAUTH_CLIENT_ID", "")
LI_OAUTH_CLIENT_SECRET = os.environ.get("LI_OAUTH_CLIENT_SECRET", "")
LI_REFRESH_TOKEN = os.environ.get("LI_REFRESH_TOKEN", "")
LI_AUTHOR_URN = os.environ.get("LI_AUTHOR_URN", "")  # e.g. "urn:li:person:ABC123"

# ─── Retry ────────────────────────────────────────────────
PUBLISH_MAX_RETRIES = int(os.environ.get("PUBLISH_MAX_RETRIES", "3"))
PUBLISH_RETRY_DELAY = int(os.environ.get("PUBLISH_RETRY_DELAY", "5"))  # seconds

# ─── Supported MIME types ────────────────────────────────────
VIDEO_MIMES = {
    "video/mp4",
    "video/quicktime",
    "video/x-msvideo",
    "video/mpeg",
    "video/webm",
}
IMAGE_MIMES = {
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/webp",
    "image/bmp",
}


def get_google_sa_info() -> dict:
    """פרסור ה-Service Account JSON מתוך env var."""
    return json.loads(GOOGLE_SA_JSON)
