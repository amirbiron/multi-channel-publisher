"""
config_constants.py — Pure constants that do NOT require any credentials.

Import from here when you only need column names, status values, etc.
The full config.py re-exports everything from this module, so existing
code that does `from config import COL_ID` continues to work.
"""

from zoneinfo import ZoneInfo

# ─── Timezone ────────────────────────────────────────────────
TZ_IL = ZoneInfo("Asia/Jerusalem")

# ─── Sheet Column Names ─────────────────────────────────────
COL_ID = "id"
COL_STATUS = "status"
COL_NETWORK = "network"
COL_POST_TYPE = "post_type"
COL_PUBLISH_AT = "publish_at"
COL_CAPTION_IG = "caption_ig"
COL_CAPTION_FB = "caption_fb"
COL_DRIVE_FILE_ID = "drive_file_id"
COL_CLOUDINARY_URL = "cloudinary_url"
COL_RESULT = "result"
COL_ERROR = "error"

# ─── Status Values ───────────────────────────────────────────
STATUS_READY = "READY"
STATUS_IN_PROGRESS = "IN_PROGRESS"
STATUS_POSTED = "POSTED"
STATUS_ERROR = "ERROR"

# ─── Network Values ─────────────────────────────────────────
NETWORK_IG = "IG"
NETWORK_FB = "FB"
NETWORK_BOTH = "IG+FB"

# ─── Post Type Values ──────────────────────────────────────
POST_TYPE_FEED = "FEED"
POST_TYPE_REELS = "REELS"
