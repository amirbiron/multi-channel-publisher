"""
config_constants.py — Pure constants that do NOT require any credentials.

Import from here when you only need column names, status values, etc.
The full config.py re-exports everything from this module, so existing
code that does `from config import COL_ID` continues to work.
"""

import re
from zoneinfo import ZoneInfo

# ─── Timezone ────────────────────────────────────────────────
TZ_IL = ZoneInfo("Asia/Jerusalem")

# ─── Sheet Column Names (existing) ──────────────────────────
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

# ─── Sheet Column Names (new — multi-channel) ───────────────
COL_CAPTION = "caption"
COL_CAPTION_GBP = "caption_gbp"
COL_CAPTION_LI = "caption_li"
COL_LI_AUTHOR_URN = "li_author_urn"
COL_GBP_POST_TYPE = "gbp_post_type"
COL_CTA_TYPE = "cta_type"
COL_CTA_URL = "cta_url"
COL_GOOGLE_LOCATION_ID = "google_location_id"
COL_HASHTAGS = "hashtags"
COL_FIRST_COMMENT = "first_comment"
COL_SOURCE = "source"
COL_RETRY_COUNT = "retry_count"
COL_LOCKED_AT = "locked_at"
COL_PROCESSING_BY = "processing_by"
COL_PUBLISHED_CHANNELS = "published_channels"
COL_FAILED_CHANNELS = "failed_channels"

# ─── Status Values ───────────────────────────────────────────
STATUS_DRAFT = "DRAFT"
STATUS_READY = "READY"
STATUS_PROCESSING = "PROCESSING"
STATUS_POSTED = "POSTED"
STATUS_PARTIAL = "PARTIAL"
STATUS_ERROR = "ERROR"

# ─── Network Values ─────────────────────────────────────────
NETWORK_IG = "IG"
NETWORK_FB = "FB"
NETWORK_GBP = "GBP"
NETWORK_BOTH = "IG+FB"
NETWORK_IG_GBP = "IG+GBP"
NETWORK_FB_GBP = "FB+GBP"
NETWORK_LI = "LI"
NETWORK_IG_LI = "IG+LI"
NETWORK_FB_LI = "FB+LI"
NETWORK_GBP_LI = "GBP+LI"
NETWORK_ALL_THREE = "IG+FB+GBP"
NETWORK_IG_FB_LI = "IG+FB+LI"
NETWORK_IG_GBP_LI = "IG+GBP+LI"
NETWORK_FB_GBP_LI = "FB+GBP+LI"
NETWORK_IG_FB_GBP_LI = "IG+FB+GBP+LI"
NETWORK_ALL = "ALL"

# Set of all valid network values for validation
VALID_NETWORKS = {
    NETWORK_IG, NETWORK_FB, NETWORK_GBP, NETWORK_LI,
    NETWORK_BOTH, NETWORK_IG_GBP, NETWORK_FB_GBP,
    NETWORK_IG_LI, NETWORK_FB_LI, NETWORK_GBP_LI,
    NETWORK_ALL_THREE, NETWORK_IG_FB_LI, NETWORK_IG_GBP_LI,
    NETWORK_FB_GBP_LI, NETWORK_IG_FB_GBP_LI,
    NETWORK_ALL,
}

# ─── Post Type Values ──────────────────────────────────────
POST_TYPE_FEED = "FEED"
POST_TYPE_REELS = "REELS"

# ─── GBP Post Type Values ──────────────────────────────────
GBP_POST_TYPE_STANDARD = "STANDARD"
GBP_POST_TYPE_EVENT = "EVENT"
GBP_POST_TYPE_OFFER = "OFFER"

# ─── LinkedIn Limits ───────────────────────────────────────
LI_CAPTION_MAX_LENGTH = 3000
LI_URN_PATTERN = re.compile(r"^urn:li:(person|organization):[A-Za-z0-9_-]+$")

# ─── Source Values ──────────────────────────────────────────
SOURCE_MANUAL = "manual"
SOURCE_AUTO = "auto"
SOURCE_AI_PANEL = "ai-panel"

# ─── CTA Types (Google Business Profile) ────────────────────
# ─── Lock Timeout ─────────────────────────────────────────────
LOCK_TIMEOUT_MINUTES = 10

CTA_LEARN_MORE = "LEARN_MORE"
CTA_CALL = "CALL"
CTA_BOOK = "BOOK"

# ─── Sheet Schema ───────────────────────────────────────────
# Canonical column order for the Google Sheet.
# Existing IG/FB rows continue to work — new columns are optional
# and default to empty string when absent.
SHEET_COLUMNS = [
    COL_ID,
    COL_STATUS,
    COL_NETWORK,
    COL_POST_TYPE,
    COL_PUBLISH_AT,
    COL_CAPTION,
    COL_CAPTION_IG,
    COL_CAPTION_FB,
    COL_CAPTION_GBP,
    COL_CAPTION_LI,
    COL_LI_AUTHOR_URN,
    COL_GBP_POST_TYPE,
    COL_CTA_TYPE,
    COL_CTA_URL,
    COL_GOOGLE_LOCATION_ID,
    COL_HASHTAGS,
    COL_FIRST_COMMENT,
    COL_DRIVE_FILE_ID,
    COL_CLOUDINARY_URL,
    COL_SOURCE,
    COL_RESULT,
    COL_ERROR,
    COL_RETRY_COUNT,
    COL_LOCKED_AT,
    COL_PROCESSING_BY,
    COL_PUBLISHED_CHANNELS,
    COL_FAILED_CHANNELS,
]
