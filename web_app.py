"""
web_app.py — פאנל ווב לניהול פוסטים ברשתות חברתיות

Flask app שמתחבר ל-Google Sheets ו-Google Drive,
ומספק ממשק פשוט ללקוחה לניהול הפוסטים.
"""

import hashlib
import hmac
import logging
import os
import re
import sys
import threading
import requests as http_requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from dateutil import parser as dtparser
from flask import Flask, Response, jsonify, render_template, request

from config_constants import (
    TZ_IL,
    COL_ID,
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
    STATUS_DRAFT,
    STATUS_READY,
    STATUS_POSTED,
    STATUS_PARTIAL,
    STATUS_ERROR,
    STATUS_PROCESSING,
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
    GBP_POST_TYPE_STANDARD,
    SHEET_COLUMNS,
)
from google_api import (
    sheets_read_all_rows,
    sheets_read_row,
    sheets_update_cells,
    sheets_append_row,
    sheets_delete_row,
    col_letter_from_header,
    drive_list_folder,
    get_drive_service,
)
from notifications import notify_health_issue, notify_meta_api_version_expiry, notify_meta_api_version_unknown

# ─── Logging ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("web-panel")

# ─── Flask App ───────────────────────────────────────────────
app = Flask(__name__)

# ─── Authentication ──────────────────────────────────────────
# Set WEB_PANEL_SECRET to require a bearer token / query param for all requests.
# Without it the panel is fully open — do NOT deploy without setting this.
WEB_PANEL_SECRET = os.environ.get("WEB_PANEL_SECRET", "")
WEB_PANEL_DEV_SECRET = os.environ.get("WEB_PANEL_DEV_SECRET", "")

if not WEB_PANEL_SECRET:
    logger.warning(
        "WEB_PANEL_SECRET is not set — the web panel has NO authentication! "
        "Set this env var before deploying to production."
    )

# Derive a cookie token via HMAC so the raw secret is never stored in the browser.
_COOKIE_TOKEN = (
    hmac.new(
        WEB_PANEL_SECRET.encode(), b"panel_cookie", hashlib.sha256
    ).hexdigest()
    if WEB_PANEL_SECRET
    else ""
)
_DEV_COOKIE_TOKEN = (
    hmac.new(
        WEB_PANEL_DEV_SECRET.encode(), b"panel_dev_cookie", hashlib.sha256
    ).hexdigest()
    if WEB_PANEL_DEV_SECRET
    else ""
)


@app.before_request
def _check_auth():
    """Verify every request carries a valid secret (header, query-param, or cookie)."""
    if not WEB_PANEL_SECRET:
        return  # auth disabled (dev mode)

    # Static assets and health check are public
    if request.path.startswith("/static/") or request.path == "/api/health":
        return

    # Accept: Authorization: Bearer <secret>
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer ") and hmac.compare_digest(
        auth_header[7:], WEB_PANEL_SECRET
    ):
        return

    # Accept: ?token=<secret>  (useful for browser bookmarks)
    token_param = request.args.get("token", "")
    if token_param and hmac.compare_digest(token_param, WEB_PANEL_SECRET):
        return

    # Accept: dev secret via ?token=<dev_secret>
    if WEB_PANEL_DEV_SECRET and token_param and hmac.compare_digest(token_param, WEB_PANEL_DEV_SECRET):
        return

    # Accept: HMAC cookie set by a previous token= visit
    if request.cookies.get("panel_token") and hmac.compare_digest(
        request.cookies["panel_token"], _COOKIE_TOKEN
    ):
        return

    # Accept: dev cookie
    if _DEV_COOKIE_TOKEN and request.cookies.get("panel_dev") and hmac.compare_digest(
        request.cookies["panel_dev"], _DEV_COOKIE_TOKEN
    ):
        return

    # Not authenticated — show login page for browser requests, JSON for API
    if request.path == "/" or not request.path.startswith("/api/"):
        return _login_page(), 401
    return jsonify({"error": "Unauthorized"}), 401


@app.after_request
def _set_auth_cookie(response):
    """When the user authenticates via ?token=, persist an HMAC-derived cookie."""
    token_param = request.args.get("token", "")
    if not token_param:
        return response

    is_https = request.is_secure or request.headers.get("X-Forwarded-Proto") == "https"
    cookie_opts = dict(httponly=True, secure=is_https, samesite="Lax", max_age=60 * 60 * 24 * 30)

    # Dev secret → set dev cookie
    if (
        WEB_PANEL_DEV_SECRET
        and hmac.compare_digest(token_param, WEB_PANEL_DEV_SECRET)
        and not request.cookies.get("panel_dev")
    ):
        response.set_cookie("panel_dev", _DEV_COOKIE_TOKEN, **cookie_opts)
        # Also set regular panel_token so dev user has full access
        if not request.cookies.get("panel_token"):
            response.set_cookie("panel_token", _COOKIE_TOKEN, **cookie_opts)
    # Regular secret → set panel cookie
    elif (
        WEB_PANEL_SECRET
        and hmac.compare_digest(token_param, WEB_PANEL_SECRET)
        and not request.cookies.get("panel_token")
    ):
        response.set_cookie("panel_token", _COOKIE_TOKEN, **cookie_opts)

    return response


def _login_page() -> str:
    """Simple Hebrew login page shown when no valid auth is present."""
    return """<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Social Publisher — כניסה</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: 'Segoe UI', Arial, sans-serif; background: #1a1a2e; color: #e0e0e0;
           display: flex; align-items: center; justify-content: center; min-height: 100vh; }
    .login-box { background: #252540; border: 1px solid #3a3a5c; border-radius: 12px;
                 padding: 40px; max-width: 380px; width: 100%; text-align: center; }
    .login-box h1 { font-size: 22px; margin-bottom: 8px; }
    .login-box p { font-size: 14px; color: #999; margin-bottom: 24px; }
    .login-box input { width: 100%; padding: 10px 14px; border-radius: 8px; border: 1px solid #3a3a5c;
                       background: #1a1a2e; color: #e0e0e0; font-size: 15px; margin-bottom: 16px;
                       direction: ltr; text-align: center; }
    .login-box input:focus { outline: none; border-color: #6c63ff; }
    .login-box button { width: 100%; padding: 10px; border-radius: 8px; border: none;
                        background: #6c63ff; color: white; font-size: 15px; cursor: pointer; }
    .login-box button:hover { background: #5a52d5; }
    .error { color: #ff6b6b; font-size: 13px; margin-bottom: 12px; display: none; }
  </style>
</head>
<body>
  <div class="login-box">
    <h1>Social Publisher</h1>
    <p>הזיני את הסיסמה כדי להיכנס לפאנל</p>
    <div class="error" id="err">סיסמה שגויה</div>
    <form onsubmit="go(event)">
      <input type="password" id="pw" placeholder="סיסמה" autofocus>
      <button type="submit">כניסה</button>
    </form>
  </div>
  <script>
    function go(e) {
      e.preventDefault();
      const pw = document.getElementById('pw').value;
      if (!pw) return;
      window.location.href = '/?token=' + encodeURIComponent(pw);
    }
    // If we arrived with a wrong token, show error
    if (location.search.includes('token=')) {
      document.getElementById('err').style.display = 'block';
    }
  </script>
</body>
</html>"""


# Drive folder ID (root folder for media files)
DRIVE_FOLDER_ID = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")


# ═══════════════════════════════════════════════════════════════
#  Pages
# ═══════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template("index.html")


# ═══════════════════════════════════════════════════════════════
#  API — Posts (Google Sheets)
# ═══════════════════════════════════════════════════════════════

@app.route("/api/posts", methods=["GET"])
def api_get_posts():
    """מחזיר את כל הפוסטים מהטבלה."""
    try:
        header, rows = sheets_read_all_rows()
        if not header:
            return jsonify({"posts": [], "header": []})

        posts = []
        for i, row in enumerate(rows, start=2):
            post = {"_row": i}
            for j, col_name in enumerate(header):
                post[col_name] = row[j] if j < len(row) else ""
            posts.append(post)

        return jsonify({"posts": posts, "header": header})

    except Exception as e:
        logger.error(f"Error fetching posts: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/posts/status", methods=["GET"])
def api_get_posts_status():
    """מחזיר רק ID וסטטוס לכל פוסט — אנדפוינט קל לפולינג."""
    try:
        header, rows = sheets_read_all_rows()
        if not header:
            return jsonify({"statuses": []})

        try:
            id_col = header.index(COL_ID)
        except ValueError:
            return jsonify({"statuses": []})

        try:
            status_col = header.index(COL_STATUS)
        except ValueError:
            return jsonify({"statuses": []})

        try:
            error_col = header.index(COL_ERROR)
        except ValueError:
            error_col = None

        statuses = []
        for row in rows:
            post_id = row[id_col] if id_col < len(row) else ""
            post_status = row[status_col] if status_col < len(row) else ""
            entry = {"id": post_id, "status": post_status}
            if error_col is not None:
                entry["error"] = row[error_col] if error_col < len(row) else ""
            statuses.append(entry)

        return jsonify({"statuses": statuses})

    except Exception as e:
        logger.error(f"Error fetching post statuses: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


def _normalize_publish_at(value: str) -> str:
    """
    Convert a publish_at value (potentially ISO 8601 with timezone) to
    Israel-local 'YYYY-MM-DD HH:MM' format for storage in the sheet.
    The cron publisher expects naive Israel-time strings.
    """
    if not value or not value.strip():
        return value
    try:
        dt = dtparser.parse(value)
        if dt.tzinfo is not None:
            dt = dt.astimezone(TZ_IL)
        else:
            # If no timezone, assume it's already Israel time
            dt = dt.replace(tzinfo=TZ_IL)
        return dt.strftime("%Y-%m-%d %H:%M")
    except (ValueError, TypeError):
        return value  # pass through unparseable values as-is


def _network_includes_gbp(network: str) -> bool:
    """Check whether a network string includes GBP (handles 'ALL' too)."""
    return network == NETWORK_ALL or NETWORK_GBP in network.split("+")


def _validate_gbp_fields(data: dict) -> str | None:
    """Return an error message if GBP fields are invalid, or None if OK."""
    network = data.get(COL_NETWORK, "")
    if _network_includes_gbp(network):
        location_id = data.get(COL_GOOGLE_LOCATION_ID, "").strip()
        if not location_id:
            return "google_location_id is required when GBP is selected"
    return None


@app.route("/api/posts", methods=["POST"])
def api_create_post():
    """יצירת פוסט חדש (שורה חדשה בטבלה)."""
    try:
        data = request.json

        # Validate: GBP requires google_location_id
        err = _validate_gbp_fields(data)
        if err:
            return jsonify({"error": err}), 400

        header, rows = sheets_read_all_rows()

        if not header:
            return jsonify({"error": "Sheet has no header"}), 400

        # Generate next ID
        max_id = 0
        for row in rows:
            try:
                idx = header.index(COL_ID)
                val = int(row[idx]) if idx < len(row) else 0
                max_id = max(max_id, val)
            except (ValueError, IndexError):
                pass
        next_id = str(max_id + 1)

        # Only allow user-editable fields — system fields are set by the server
        allowed_fields = {
            COL_NETWORK, COL_POST_TYPE, COL_PUBLISH_AT,
            COL_CAPTION, COL_CAPTION_IG, COL_CAPTION_FB,
            COL_CAPTION_GBP, COL_GBP_POST_TYPE,
            COL_GOOGLE_LOCATION_ID, COL_CTA_TYPE, COL_CTA_URL,
            COL_DRIVE_FILE_ID,
        }

        # Build row values in header order
        row_values = []
        for col_name in header:
            if col_name == COL_ID:
                row_values.append(next_id)
            elif col_name == COL_STATUS:
                row_values.append(STATUS_READY)
            elif col_name == COL_PUBLISH_AT:
                row_values.append(_normalize_publish_at(data.get(col_name, "")))
            elif col_name in allowed_fields:
                row_values.append(data.get(col_name, ""))
            else:
                row_values.append("")

        sheets_append_row(row_values)
        logger.info(f"Created post ID {next_id}")

        return jsonify({"success": True, "id": next_id})

    except Exception as e:
        logger.error(f"Error creating post: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


def _verify_row_id(row_number: int, expected_id: str, header: list, rows: list) -> str | None:
    """
    Verify the row still contains the expected post ID.
    Returns an error message if mismatched, or None if OK.
    """
    if not expected_id:
        return None  # client didn't send an ID — skip check (backward compat)
    try:
        id_col = header.index(COL_ID)
    except ValueError:
        return None  # no ID column — can't verify
    row_idx = row_number - 2  # rows are 0-indexed, row_number starts at 2
    if row_idx < 0 or row_idx >= len(rows):
        return "Row does not exist"
    row = rows[row_idx]
    actual_id = row[id_col] if id_col < len(row) else ""
    if str(actual_id) != str(expected_id):
        return f"Row {row_number} no longer contains post #{expected_id} (found #{actual_id}). Please refresh."
    return None


@app.route("/api/posts/<int:row_number>", methods=["PUT"])
def api_update_post(row_number):
    """עדכון פוסט קיים."""
    if row_number < 2:
        return jsonify({"error": "Invalid row number"}), 400

    try:
        data = request.json

        # Validate: GBP requires google_location_id
        err = _validate_gbp_fields(data)
        if err:
            return jsonify({"error": err}), 400

        header, rows = sheets_read_all_rows()

        if not header:
            return jsonify({"error": "Sheet has no header"}), 400

        # Verify the row still holds the expected post
        id_err = _verify_row_id(row_number, data.get("expected_id"), header, rows)
        if id_err:
            return jsonify({"error": id_err}), 409

        # Only allow updating content fields — status is managed by the publisher
        allowed_fields = {
            COL_NETWORK, COL_POST_TYPE, COL_PUBLISH_AT,
            COL_CAPTION, COL_CAPTION_IG, COL_CAPTION_FB,
            COL_CAPTION_GBP, COL_GBP_POST_TYPE,
            COL_GOOGLE_LOCATION_ID, COL_CTA_TYPE, COL_CTA_URL,
            COL_DRIVE_FILE_ID,
        }

        updates = {}
        for key, value in data.items():
            if key in allowed_fields:
                if key == COL_PUBLISH_AT:
                    updates[key] = _normalize_publish_at(value)
                else:
                    updates[key] = value

        if updates:
            sheets_update_cells(row_number, updates, header)
            logger.info(f"Updated row {row_number}: {list(updates.keys())}")

        return jsonify({"success": True})

    except Exception as e:
        logger.error(f"Error updating post: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/posts/<int:row_number>/retry", methods=["POST"])
def api_retry_post(row_number):
    """
    Retry failed channels for a PARTIAL or ERROR post.

    Accepts optional JSON body:
        {"channels": ["GBP"]}  — retry only specific channels
    If omitted, retries all channels listed in failed_channels.

    Sets the post to PARTIAL so the cron's process_partial_row picks it up.
    For ERROR posts where all channels failed — if cloudinary_url exists,
    uses PARTIAL retry; otherwise resets to READY for full re-processing.
    """
    if row_number < 2:
        return jsonify({"error": "Invalid row number"}), 400

    try:
        data = request.json or {}
        requested_channels = data.get("channels")  # optional list

        header, rows = sheets_read_all_rows()
        if not header:
            return jsonify({"error": "Sheet has no header"}), 400

        row_idx = row_number - 2
        if row_idx < 0 or row_idx >= len(rows):
            return jsonify({"error": "Row does not exist"}), 404

        row = rows[row_idx]

        def _cell(col):
            try:
                idx = header.index(col)
                return row[idx] if idx < len(row) else ""
            except ValueError:
                return ""

        status = _cell(COL_STATUS).strip().upper()
        if status not in (STATUS_PARTIAL, STATUS_ERROR):
            return jsonify({"error": f"רק פוסטים עם שגיאה או הצלחה חלקית ניתנים ל-retry (סטטוס נוכחי: {status})"}), 400

        failed_channels = _cell(COL_FAILED_CHANNELS).strip()
        published_channels = _cell(COL_PUBLISHED_CHANNELS).strip()
        cloudinary_url = _cell(COL_CLOUDINARY_URL).strip()

        if requested_channels:
            # Validate requested channels are actually failed
            failed_set = {c.strip() for c in failed_channels.split(",") if c.strip()}
            invalid = [c for c in requested_channels if c not in failed_set]
            if invalid:
                return jsonify({"error": f"הערוצים {', '.join(invalid)} לא נמצאים ברשימת הכשלונות"}), 400
            retry_channels = requested_channels
        else:
            if not failed_channels:
                return jsonify({"error": "אין ערוצים שנכשלו ל-retry"}), 400
            retry_channels = [c.strip() for c in failed_channels.split(",") if c.strip()]

        # Determine retry strategy
        if status == STATUS_ERROR and not cloudinary_url and not published_channels:
            # All channels failed and no media was uploaded — full re-process
            updates = {
                COL_STATUS: STATUS_READY,
                COL_ERROR: "",
                COL_FAILED_CHANNELS: "",
                COL_PUBLISHED_CHANNELS: "",
                COL_LOCKED_AT: "",
                COL_PROCESSING_BY: "",
                COL_RETRY_COUNT: "0",
            }
            logger.info(f"Retry row {row_number}: resetting to READY (no cloudinary_url)")
        else:
            # Has cloudinary URLs or partial success — set PARTIAL for channel-level retry
            updates = {
                COL_STATUS: STATUS_PARTIAL,
                COL_FAILED_CHANNELS: ",".join(retry_channels),
                COL_ERROR: "",
                COL_LOCKED_AT: "",
                COL_PROCESSING_BY: "",
            }
            logger.info(f"Retry row {row_number}: setting PARTIAL for channels {retry_channels}")

        sheets_update_cells(row_number, updates, header)
        return jsonify({"success": True, "retry_channels": retry_channels})

    except Exception as e:
        logger.error(f"Error retrying post row {row_number}: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/posts/<int:row_number>", methods=["DELETE"])
def api_delete_post(row_number):
    """מחיקת פוסט (שורה מהטבלה)."""
    if row_number < 2:
        return jsonify({"error": "Invalid row number"}), 400

    try:
        # Verify the row still holds the expected post
        expected_id = request.args.get("expected_id")
        if expected_id:
            header, rows = sheets_read_all_rows()
            if header:
                id_err = _verify_row_id(row_number, expected_id, header, rows)
                if id_err:
                    return jsonify({"error": id_err}), 409

        sheets_delete_row(row_number)
        logger.info(f"Deleted row {row_number}")
        return jsonify({"success": True})

    except Exception as e:
        logger.error(f"Error deleting post: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════
#  API — Google Business Profile Locations
# ═══════════════════════════════════════════════════════════════

@app.route("/api/gbp/locations", methods=["GET"])
def api_gbp_locations():
    """מחזיר רשימת מיקומי GBP זמינים."""
    try:
        from channels.google_locations import get_locations_service
        svc = get_locations_service()
        locations = svc.list_locations()
        return jsonify({
            "locations": [
                {
                    "name": loc.get("name", ""),
                    "title": loc.get("title", ""),
                    "id": loc.get("name", ""),
                }
                for loc in locations
            ]
        })
    except ValueError as e:
        # GBP_ACCOUNT_ID not configured — return empty list (not an error)
        logger.debug(f"GBP locations not available: {e}")
        return jsonify({"locations": []})
    except Exception as e:
        logger.error(f"Error fetching GBP locations: {e}", exc_info=True)
        return jsonify({"locations": [], "error": str(e)})


# ═══════════════════════════════════════════════════════════════
#  API — Google Drive
# ═══════════════════════════════════════════════════════════════

def _is_folder_within_root(folder_id: str, root_id: str, max_depth: int = 10) -> bool:
    """
    Verify folder_id is the root or a descendant of root_id
    by walking up the parent chain. Prevents folder traversal attacks.
    """
    if folder_id == root_id:
        return True

    svc = get_drive_service()
    current = folder_id
    for _ in range(max_depth):
        try:
            meta = svc.files().get(fileId=current, fields="parents", supportsAllDrives=True).execute()
            parents = meta.get("parents", [])
            if not parents:
                return False
            if root_id in parents:
                return True
            current = parents[0]
        except Exception:
            return False
    return False


def _is_known_drive_file(file_id: str) -> bool:
    """Check if file_id exists in the Google Sheet's drive_file_id column.
    Handles comma-separated IDs for carousel posts."""
    try:
        header, rows = sheets_read_all_rows()
        if not header:
            return False
        idx = header.index(COL_DRIVE_FILE_ID)
        return any(
            idx < len(row) and file_id in {fid.strip() for fid in row[idx].split(",")}
            for row in rows
        )
    except (ValueError, Exception):
        return False


@app.route("/api/drive/thumbnail/<file_id>", methods=["GET"])
def api_drive_thumbnail(file_id):
    """מחזיר תמונה ממוזערת של קובץ מ-Drive (proxy)."""
    # Debug mode: only available via FLASK_DEBUG env var (not exposed to regular users)
    debug = (
        request.args.get("debug") == "1"
        and os.environ.get("FLASK_DEBUG", "").lower() == "true"
    )
    try:
        if not file_id or len(file_id) > 120 or not re.fullmatch(r'[A-Za-z0-9_-]+', file_id):
            if debug:
                return jsonify({"step": "validate", "error": "Invalid file_id format"})
            return Response(status=400)

        if not DRIVE_FOLDER_ID:
            if debug:
                return jsonify({"step": "config", "error": "GOOGLE_DRIVE_FOLDER_ID not set"})
            return Response(status=404)

        # Verify the file belongs to the configured root folder tree
        svc = get_drive_service()
        meta = svc.files().get(fileId=file_id, fields="thumbnailLink,parents", supportsAllDrives=True).execute()

        parents = meta.get("parents", [])

        # Verify file belongs to the configured root folder tree.
        # Some Drive configs return empty parents — in that case, fall back
        # to checking if the file_id exists in our Google Sheet (since
        # file_id is a user-controlled URL parameter, not just Sheet data).
        if parents:
            if not any(_is_folder_within_root(p, DRIVE_FOLDER_ID) for p in parents):
                logger.warning(f"Thumbnail denied: file {file_id} not within root folder")
                if debug:
                    folder_check = {p: _is_folder_within_root(p, DRIVE_FOLDER_ID) for p in parents}
                    return jsonify({"step": "folder_check", "error": "File not within root folder",
                                    "parents": parents, "root": DRIVE_FOLDER_ID, "checks": folder_check})
                return Response(status=403)
        else:
            if not _is_known_drive_file(file_id):
                logger.warning(f"Thumbnail denied: file {file_id} has no parents and is not in Sheet")
                if debug:
                    return jsonify({"step": "folder_check", "error": "File not in Sheet and no parents to verify",
                                    "parents": [], "root": DRIVE_FOLDER_ID})
                return Response(status=403)

        thumb_url = meta.get("thumbnailLink")
        if not thumb_url:
            logger.debug(f"No thumbnailLink for file {file_id}")
            if debug:
                return jsonify({"step": "thumbnailLink", "error": "Google returned no thumbnailLink",
                                "meta_keys": list(meta.keys())})
            return Response(status=404)

        # Google's thumbnailLink ends with =s220 (default size).
        # Replace with a larger size when requested for the lightbox.
        size = request.args.get("size", "small")
        if size == "large":
            thumb_url = re.sub(r'=s\d+$', '=s1200', thumb_url)

        if debug:
            return jsonify({"step": "ready", "thumbnailLink": thumb_url[:80] + "...",
                            "will_fetch": True})

        # Fetch thumbnail with service-account auth via requests (thread-safe).
        # svc._http.credentials is auto-refreshed by prior API calls above.
        MAX_THUMB_BYTES = 5 * 1024 * 1024  # 5 MB safety cap
        creds = svc._http.credentials
        if not creds.token:
            import google.auth.transport.requests as gauth_transport
            creds.refresh(gauth_transport.Request())

        thumb_resp = http_requests.get(
            thumb_url,
            headers={"Authorization": f"Bearer {creds.token}"},
            timeout=10,
            stream=True,
        )

        try:
            if thumb_resp.status_code != 200:
                logger.warning(f"Thumbnail fetch failed for {file_id}: HTTP {thumb_resp.status_code}")
                return Response(status=502)

            data = thumb_resp.raw.read(MAX_THUMB_BYTES + 1, decode_content=True)

            if len(data) > MAX_THUMB_BYTES:
                return Response(status=413)

            content_type = thumb_resp.headers.get("Content-Type", "image/png")
        finally:
            thumb_resp.close()

        # Only proxy image MIME types to prevent serving active content (XSS)
        if not content_type.startswith("image/"):
            logger.warning(f"Thumbnail for {file_id} returned non-image type: {content_type}")
            return Response(status=502)

        return Response(
            data,
            mimetype=content_type,
            headers={"Cache-Control": "public, max-age=3600"},
        )

    except Exception as e:
        logger.error(f"Thumbnail error for {file_id}: {e}", exc_info=True)
        return Response(status=404)


@app.route("/api/drive/files", methods=["GET"])
def api_drive_files():
    """מחזיר קבצים מתיקיית Drive."""
    try:
        folder_id = request.args.get("folder_id", DRIVE_FOLDER_ID)
        if not folder_id:
            return jsonify({"error": "No folder ID configured"}), 400

        if not DRIVE_FOLDER_ID:
            return jsonify({"error": "No root folder configured"}), 400

        # Validate the folder is within the allowed root
        if not _is_folder_within_root(folder_id, DRIVE_FOLDER_ID):
            return jsonify({"error": "Access denied: folder outside allowed scope"}), 403

        page_token = request.args.get("page_token")
        result = drive_list_folder(folder_id, page_token)

        return jsonify(result)

    except Exception as e:
        logger.error(f"Error listing Drive files: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500



# ═══════════════════════════════════════════════════════════════
#  API — Health Check (public, no auth)
# ═══════════════════════════════════════════════════════════════

# Cooldown: שולחים התראת טלגרם על שירות שנפל לכל היותר פעם ב-30 דקות
HEALTH_NOTIFY_COOLDOWN_SECONDS = int(os.environ.get("HEALTH_NOTIFY_COOLDOWN_SECONDS", "1800"))
_health_notify_cooldown: dict[str, datetime] = {}  # {service_name: last_notified_utc}

# Cache: תוצאת health check נשמרת למשך 60 שניות למניעת ניצול API quotas
HEALTH_CACHE_TTL_SECONDS = int(os.environ.get("HEALTH_CACHE_TTL_SECONDS", "60"))
_health_cache: dict = {}  # {"result": ..., "status_code": ..., "timestamp": datetime}

def _check_google_sheets() -> dict:
    """בדיקת חיבור ל-Google Sheets — קורא רק את שורת ה-header."""
    try:
        header = sheets_read_row(1)
        if header:
            return {"status": "ok", "columns": len(header)}
        return {"status": "error", "error": "Sheet is empty or has no header"}
    except Exception as e:
        return {"status": "error", "error": str(e)[:200]}


def _check_google_drive() -> dict:
    """בדיקת חיבור ל-Google Drive."""
    try:
        if not DRIVE_FOLDER_ID:
            return {"status": "error", "error": "GOOGLE_DRIVE_FOLDER_ID not configured"}
        svc = get_drive_service()
        meta = svc.files().get(fileId=DRIVE_FOLDER_ID, fields="id,name", supportsAllDrives=True).execute()
        return {"status": "ok", "folder": meta.get("name", DRIVE_FOLDER_ID)}
    except Exception as e:
        return {"status": "error", "error": str(e)[:200]}


def _check_cloudinary() -> dict:
    """בדיקת חיבור ל-Cloudinary (ping via API)."""
    try:
        import cloudinary.api
        result = cloudinary.api.ping()
        if result.get("status") == "ok":
            return {"status": "ok"}
        return {"status": "error", "error": f"Unexpected response: {result}"}
    except Exception as e:
        return {"status": "error", "error": str(e)[:200]}


META_API_VERSION_WARN_DAYS = int(os.environ.get("META_API_VERSION_WARN_DAYS", "30"))

# תאריכי תפוגה ידועים של גרסאות Meta Graph API (fallback)
# עדכנו ידנית כשמטא מפרסמים תאריכים ב:
# https://developers.facebook.com/docs/graph-api/changelog/
_META_VERSION_EXPIRY = {
    # "v21.0": "2026-XX-XX",  # TODO: update when Meta publishes expiry
}


def _get_version_expiry(version: str) -> str | None:
    """מחזיר תאריך תפוגה ידוע לגרסה, או None."""
    return _META_VERSION_EXPIRY.get(version) or _META_VERSION_EXPIRY.get("v" + version.lstrip("v"))


def _check_meta_api_version() -> dict:
    """בדיקת תוקף גרסת Meta Graph API — מחזיר ימים עד תפוגה."""
    meta_api_version = os.environ.get("META_API_VERSION", "v21.0")

    expiry_str = None

    # ── ניסיון 1: API ──
    try:
        resp = http_requests.get(
            "https://graph.facebook.com/api_versioning",
            params={"access_token": os.environ.get("FB_PAGE_ACCESS_TOKEN", "")},
            timeout=10,
        )
        if resp.ok:
            versions = resp.json().get("data", [])
            for v in versions:
                if v.get("gl_api_version") in (meta_api_version, meta_api_version.lstrip("v")):
                    expiry_str = (v.get("gl_end_date") or v.get("end_date") or "")[:10] or None
                    break
    except Exception as e:
        logger.debug(f"Meta API versioning endpoint failed: {e}")

    # ── ניסיון 2: fallback לתאריכים ידועים ──
    if not expiry_str:
        expiry_str = _get_version_expiry(meta_api_version)

    # ── אם אין מידע בכלל ──
    if not expiry_str:
        return {
            "status": "unknown",
            "version": meta_api_version,
            "note": f"Could not determine expiry for {meta_api_version}",
        }

    # ── חישוב ימים ──
    try:
        expiry_date = datetime.strptime(expiry_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return {
            "status": "unknown",
            "version": meta_api_version,
            "note": f"Invalid expiry date format: {expiry_str}",
        }

    now = datetime.now(timezone.utc)
    days_left = (expiry_date - now).days

    result = {
        "version": meta_api_version,
        "expiry": expiry_str,
        "days_left": days_left,
    }

    if days_left < 0:
        result["status"] = "error"
        result["error"] = f"API version {meta_api_version} has expired!"
    elif days_left <= META_API_VERSION_WARN_DAYS:
        result["status"] = "warning"
    else:
        result["status"] = "ok"

    return result


def _check_meta_token(token_name: str, token: str) -> dict:
    """בדיקת תוקף טוקן Meta (debug_token או /me)."""
    if not token:
        return {"status": "error", "error": f"{token_name} not configured"}
    try:
        meta_api_version = os.environ.get("META_API_VERSION", "v21.0")
        url = f"https://graph.facebook.com/{meta_api_version}/me"
        resp = http_requests.get(url, params={"access_token": token}, timeout=10)
        if resp.ok:
            data = resp.json()
            return {"status": "ok", "name": data.get("name", "OK")}
        error = resp.json().get("error", {})
        return {"status": "error", "error": error.get("message", resp.text)[:200]}
    except Exception as e:
        return {"status": "error", "error": str(e)[:200]}


@app.route("/api/health", methods=["GET"])
def api_health():
    """
    בדיקת תקינות כל השירותים החיצוניים.
    מחזיר סטטוס לכל שירות + סטטוס כללי.
    לא דורש אימות — מיועד ל-uptime monitoring.
    תוצאות נשמרות ב-cache למשך 60 שניות למניעת ניצול API quotas.
    """
    # החזרת תוצאה מה-cache אם עדיין תקפה
    now = datetime.now(timezone.utc)
    if _health_cache:
        age = (now - _health_cache["timestamp"]).total_seconds()
        if age < HEALTH_CACHE_TTL_SECONDS:
            return jsonify(_health_cache["result"]), _health_cache["status_code"]

    ig_token = os.environ.get("IG_ACCESS_TOKEN", "")
    fb_token = os.environ.get("FB_PAGE_ACCESS_TOKEN", "")

    checks = {}

    # רץ במקביל לחיסכון בזמן
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {
            pool.submit(_check_google_sheets): "google_sheets",
            pool.submit(_check_google_drive): "google_drive",
            pool.submit(_check_cloudinary): "cloudinary",
            pool.submit(_check_meta_token, "IG_ACCESS_TOKEN", ig_token): "instagram",
            pool.submit(_check_meta_token, "FB_PAGE_ACCESS_TOKEN", fb_token): "facebook",
            pool.submit(_check_meta_api_version): "meta_api_version",
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                checks[name] = future.result()
            except Exception as e:
                checks[name] = {"status": "error", "error": str(e)[:200]}

    # meta_api_version warning/unknown לא נחשב כ-unhealthy
    all_ok = all(
        c["status"] in ("ok", "warning", "unknown") if name == "meta_api_version" else c["status"] == "ok"
        for name, c in checks.items()
    )
    status_code = 200 if all_ok else 503

    # שליחת התראות טלגרם על שירותים שנפלו (עם cooldown למניעת ספאם)
    now = datetime.now(timezone.utc)
    for name, check in checks.items():
        if name == "meta_api_version" and check["status"] in ("warning", "error"):
            last_sent = _health_notify_cooldown.get("meta_api_version")
            if last_sent is None or (now - last_sent).total_seconds() >= HEALTH_NOTIFY_COOLDOWN_SECONDS:
                notify_meta_api_version_expiry(
                    check.get("version", "?"), check.get("expiry", "?"), check.get("days_left", 0)
                )
                _health_notify_cooldown["meta_api_version"] = now
        elif name == "meta_api_version" and check["status"] == "unknown":
            last_sent = _health_notify_cooldown.get("meta_api_version")
            if last_sent is None or (now - last_sent).total_seconds() >= HEALTH_NOTIFY_COOLDOWN_SECONDS:
                notify_meta_api_version_unknown(check.get("version", "?"))
                _health_notify_cooldown["meta_api_version"] = now
        elif check["status"] == "error":
            last_sent = _health_notify_cooldown.get(name)
            if last_sent is None or (now - last_sent).total_seconds() >= HEALTH_NOTIFY_COOLDOWN_SECONDS:
                notify_health_issue(name, check.get("error", "Unknown"))
                _health_notify_cooldown[name] = now

    if all_ok:
        # ניקוי cooldown לשירותים שחזרו לפעול — למעט meta_api_version שמנוהל בנפרד
        for name in list(_health_notify_cooldown):
            if name != "meta_api_version":
                _health_notify_cooldown.pop(name)
        # ניקוי meta_api_version cooldown רק אם הגרסה תקינה
        meta_check = checks.get("meta_api_version", {})
        if meta_check.get("status") == "ok":
            _health_notify_cooldown.pop("meta_api_version", None)

    result = {
        "status": "healthy" if all_ok else "unhealthy",
        "services": checks,
        "timestamp": now.isoformat(),
    }

    # שמירה ב-cache
    _health_cache.update({"result": result, "status_code": status_code, "timestamp": now})

    return jsonify(result), status_code


# ═══════════════════════════════════════════════════════════════
#  API — Config (public, non-sensitive)
# ═══════════════════════════════════════════════════════════════

@app.route("/api/config", methods=["GET"])
def api_config():
    """מחזיר הגדרות ציבוריות לפרונטאנד."""
    is_dev = (
        _DEV_COOKIE_TOKEN
        and request.cookies.get("panel_dev")
        and hmac.compare_digest(request.cookies["panel_dev"], _DEV_COOKIE_TOKEN)
    )
    return jsonify({
        "driveFolderId": DRIVE_FOLDER_ID,
        "columns": SHEET_COLUMNS,
        "statuses": [STATUS_DRAFT, STATUS_READY, STATUS_PROCESSING, STATUS_POSTED, STATUS_PARTIAL, STATUS_ERROR],
        "networks": sorted(VALID_NETWORKS),
        "postTypes": [POST_TYPE_FEED, POST_TYPE_REELS],
        "isDev": bool(is_dev),
    })


# ═══════════════════════════════════════════════════════════════
#  Daily Meta API Version Check
# ═══════════════════════════════════════════════════════════════

_DAILY_CHECK_INTERVAL = 24 * 60 * 60  # 24 hours


_last_daily_version_check: datetime | None = None

@app.before_request
def _maybe_run_daily_version_check():
    """בדיקה יומית של גרסת Meta API — רצה פעם ב-24 שעות על בקשה ראשונה."""
    global _last_daily_version_check
    now = datetime.now(timezone.utc)
    if _last_daily_version_check and (now - _last_daily_version_check).total_seconds() < _DAILY_CHECK_INTERVAL:
        return
    _last_daily_version_check = now

    def _run():
        try:
            result = _check_meta_api_version()
            logger.info(f"Daily Meta API version check: {result}")
            status = result.get("status")
            now_inner = datetime.now(timezone.utc)
            last_sent = _health_notify_cooldown.get("meta_api_version")
            if last_sent and (now_inner - last_sent).total_seconds() < _DAILY_CHECK_INTERVAL:
                return
            if status in ("warning", "error"):
                notify_meta_api_version_expiry(
                    result.get("version", "?"),
                    result.get("expiry", "?"),
                    result.get("days_left", 0),
                )
                _health_notify_cooldown["meta_api_version"] = now_inner
            elif status == "unknown":
                notify_meta_api_version_unknown(result.get("version", "?"))
                _health_notify_cooldown["meta_api_version"] = now_inner
        except Exception as e:
            logger.warning(f"Daily Meta API version check failed: {e}")

    threading.Thread(target=_run, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    debug = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=port, debug=debug)
