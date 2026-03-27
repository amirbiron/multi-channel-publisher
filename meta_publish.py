"""
meta_publish.py — פרסום ל-Instagram ו-Facebook דרך Graph API

Instagram: 2 קריאות (create container → publish)
Facebook: תמונה = /photos, וידאו = /videos, ריל = /video_reels
קרוסלה: מספר תמונות/סרטונים בפוסט אחד (2-10 פריטים)
"""

import logging
import time

import requests

from config import (
    META_BASE_URL,
    META_API_VERSION,
    IG_USER_ID,
    IG_ACCESS_TOKEN,
    FB_PAGE_ID,
    FB_PAGE_ACCESS_TOKEN,
    VIDEO_MIMES,
    POST_TYPE_FEED,
    POST_TYPE_REELS,
)

logger = logging.getLogger(__name__)

# Timeout לקריאות API
TIMEOUT_SHORT = 60   # תמונות
TIMEOUT_LONG = 180   # וידאו (העלאה + עיבוד)


# ═══════════════════════════════════════════════════════════════
#  Instagram — Feed / Reels
# ═══════════════════════════════════════════════════════════════

def ig_publish_feed(
    cloud_url: str,
    caption: str,
    mime_type: str,
    post_type: str = POST_TYPE_FEED,
) -> str:
    """
    מפרסם פוסט באינסטגרם.
    וידאו תמיד נשלח כ-REELS (אילוץ API — אין דרך אחרת).
    תמונה תמיד נשלחת כ-Feed (IG Reels לא תומך בתמונות).
    מחזיר את ה-media ID של הפוסט שפורסם.
    """
    is_video = mime_type in VIDEO_MIMES

    # ── שלב 1: יצירת Container ──
    container_id = _ig_create_container(cloud_url, caption, is_video)

    # ── שלב 1.5: חכה לעיבוד (וידאו + תמונות) ──
    _ig_wait_for_container_ready(container_id, is_video=is_video)

    # ── שלב 2: פרסום ──
    result_id = _ig_publish_container(container_id)
    logger.info(f"Instagram published: {result_id} (post_type={post_type})")
    return result_id


def _ig_create_container(
    cloud_url: str, caption: str, is_video: bool
) -> str:
    """יצירת media container באינסטגרם."""
    url = f"{META_BASE_URL}/{IG_USER_ID}/media"
    data = {
        "caption": caption,
        "access_token": IG_ACCESS_TOKEN,
    }

    if is_video:
        data["video_url"] = cloud_url
        data["media_type"] = "REELS"
    else:
        data["image_url"] = cloud_url

    resp = requests.post(url, data=data, timeout=TIMEOUT_SHORT)
    if not resp.ok:
        logger.error(f"IG create container failed ({resp.status_code}): {resp.text}")
        resp.raise_for_status()

    container_id = resp.json()["id"]
    logger.info(f"IG container created: {container_id} (video={is_video})")
    return container_id


def _ig_wait_for_container_ready(
    container_id: str,
    is_video: bool = False,
    max_wait: int = 300,
    interval: int = 5,
) -> None:
    """
    ממתין עד שה-container מוכן לפרסום (סטטוס FINISHED).
    גם תמונות צריכות עיבוד — בד"כ 2-10 שניות, וידאו יותר.
    """
    url = f"{META_BASE_URL}/{container_id}"
    params = {
        "fields": "status_code",
        "access_token": IG_ACCESS_TOKEN,
    }

    elapsed = 0
    while elapsed < max_wait:
        resp = requests.get(url, params=params, timeout=TIMEOUT_SHORT)
        resp.raise_for_status()
        status = resp.json().get("status_code")

        logger.info(f"IG container {container_id}: status={status} ({elapsed}s)")

        if status == "FINISHED":
            return

        if status == "ERROR":
            error_msg = resp.json().get("status", "Unknown error")
            raise RuntimeError(
                f"Instagram container processing failed: {error_msg}"
            )

        time.sleep(interval)
        elapsed += interval

    raise TimeoutError(
        f"Instagram container processing timed out after {max_wait}s "
        f"for container {container_id}"
    )


def _ig_publish_container(container_id: str) -> str:
    """פרסום container שמוכן."""
    url = f"{META_BASE_URL}/{IG_USER_ID}/media_publish"
    data = {
        "creation_id": container_id,
        "access_token": IG_ACCESS_TOKEN,
    }

    resp = requests.post(url, data=data, timeout=TIMEOUT_SHORT)
    if not resp.ok:
        logger.error(f"IG publish container failed ({resp.status_code}): {resp.text}")
        resp.raise_for_status()
    return resp.json()["id"]


# ═══════════════════════════════════════════════════════════════
#  Instagram — Carousel (2-10 items)
# ═══════════════════════════════════════════════════════════════

def ig_publish_carousel(
    cloud_urls: list[str],
    caption: str,
    mime_types: list[str],
) -> str:
    """
    מפרסם קרוסלה באינסטגרם (2-10 תמונות/סרטונים).
    שלבים:
      1. יצירת container לכל פריט (ללא caption, עם is_carousel_item=true)
      2. המתנה שכל ה-containers מוכנים
      3. יצירת carousel container עם children + caption
      4. המתנה ופרסום
    מחזיר media ID.
    """
    if len(cloud_urls) < 2:
        raise ValueError("Carousel requires at least 2 items")
    if len(cloud_urls) > 10:
        raise ValueError("Carousel supports at most 10 items")

    # ── שלב 1: יצירת containers לפריטים ──
    child_ids = []
    for i, (url, mime) in enumerate(zip(cloud_urls, mime_types)):
        is_video = mime in VIDEO_MIMES
        container_id = _ig_create_carousel_item(url, is_video)
        logger.info(f"IG carousel item {i+1}/{len(cloud_urls)}: {container_id}")
        child_ids.append(container_id)

    # ── שלב 2: המתנה לעיבוד כל הפריטים ──
    for i, (cid, mime) in enumerate(zip(child_ids, mime_types)):
        is_video = mime in VIDEO_MIMES
        _ig_wait_for_container_ready(cid, is_video=is_video)
        logger.info(f"IG carousel item {i+1}/{len(child_ids)} ready")

    # ── שלב 3: יצירת carousel container ──
    carousel_id = _ig_create_carousel_container(child_ids, caption)

    # ── שלב 4: המתנה ופרסום ──
    _ig_wait_for_container_ready(carousel_id)
    result_id = _ig_publish_container(carousel_id)
    logger.info(f"Instagram carousel published: {result_id} ({len(cloud_urls)} items)")
    return result_id


def _ig_create_carousel_item(cloud_url: str, is_video: bool) -> str:
    """יצירת container לפריט בתוך קרוסלה (ללא caption)."""
    url = f"{META_BASE_URL}/{IG_USER_ID}/media"
    data = {
        "is_carousel_item": "true",
        "access_token": IG_ACCESS_TOKEN,
    }

    if is_video:
        data["video_url"] = cloud_url
        data["media_type"] = "VIDEO"
    else:
        data["image_url"] = cloud_url

    resp = requests.post(url, data=data, timeout=TIMEOUT_SHORT)
    if not resp.ok:
        logger.error(f"IG carousel item failed ({resp.status_code}): {resp.text}")
        resp.raise_for_status()

    return resp.json()["id"]


def _ig_create_carousel_container(child_ids: list[str], caption: str) -> str:
    """יצירת carousel container מ-children containers."""
    url = f"{META_BASE_URL}/{IG_USER_ID}/media"
    data = {
        "media_type": "CAROUSEL",
        "caption": caption,
        "children": ",".join(child_ids),
        "access_token": IG_ACCESS_TOKEN,
    }

    resp = requests.post(url, data=data, timeout=TIMEOUT_SHORT)
    if not resp.ok:
        logger.error(f"IG carousel container failed ({resp.status_code}): {resp.text}")
        resp.raise_for_status()

    carousel_id = resp.json()["id"]
    logger.info(f"IG carousel container created: {carousel_id} ({len(child_ids)} children)")
    return carousel_id


# ═══════════════════════════════════════════════════════════════
#  Facebook Page — Feed / Reels
# ═══════════════════════════════════════════════════════════════

def fb_publish_feed(
    cloud_url: str,
    caption: str,
    mime_type: str,
    post_type: str = POST_TYPE_FEED,
) -> str:
    """
    מפרסם פוסט בעמוד פייסבוק.
    post_type=REELS → מפרסם כ-Reel (וידאו בלבד).
    post_type=FEED  → תמונה רגילה או וידאו רגיל.
    מחזיר post_id / video_id.
    """
    is_video = mime_type in VIDEO_MIMES

    if post_type == POST_TYPE_REELS and is_video:
        return _fb_publish_reel(cloud_url, caption)
    elif is_video:
        return _fb_publish_video(cloud_url, caption)
    else:
        return _fb_publish_photo(cloud_url, caption)


def _fb_publish_photo(cloud_url: str, caption: str) -> str:
    """פרסום תמונה בעמוד פייסבוק."""
    url = f"{META_BASE_URL}/{FB_PAGE_ID}/photos"
    data = {
        "url": cloud_url,
        "caption": caption,
        "access_token": FB_PAGE_ACCESS_TOKEN,
    }

    resp = requests.post(url, data=data, timeout=TIMEOUT_SHORT)
    if not resp.ok:
        logger.error(f"FB publish photo failed ({resp.status_code}): {resp.text}")
        resp.raise_for_status()

    result = resp.json()
    result_id = result.get("post_id") or result.get("id")
    logger.info(f"FB photo published: {result_id}")
    return result_id


def _fb_publish_video(cloud_url: str, caption: str) -> str:
    """פרסום וידאו רגיל בעמוד פייסבוק."""
    url = f"{META_BASE_URL}/{FB_PAGE_ID}/videos"
    data = {
        "file_url": cloud_url,
        "description": caption,
        "access_token": FB_PAGE_ACCESS_TOKEN,
        "published": "true",
    }

    resp = requests.post(url, data=data, timeout=TIMEOUT_LONG)
    if not resp.ok:
        logger.error(f"FB publish video failed ({resp.status_code}): {resp.text}")
        resp.raise_for_status()

    result_id = resp.json().get("id")
    logger.info(f"FB video published: {result_id}")
    return result_id


def _fb_publish_reel(cloud_url: str, caption: str) -> str:
    """
    פרסום Reel בעמוד פייסבוק — 3 שלבים:
      1. start  → מקבלים video_id + upload_url
      2. transfer → שולחים את הוידאו (file_url header עבור CDN)
      3. finish  → מפרסמים עם description
    """
    base = f"{META_BASE_URL}/{FB_PAGE_ID}/video_reels"

    # ── שלב 1: start ──
    resp = requests.post(
        base,
        data={"upload_phase": "start", "access_token": FB_PAGE_ACCESS_TOKEN},
        timeout=TIMEOUT_SHORT,
    )
    if not resp.ok:
        logger.error(f"FB reel start failed ({resp.status_code}): {resp.text}")
        resp.raise_for_status()

    start_data = resp.json()
    video_id = start_data["video_id"]
    upload_url = start_data["upload_url"]
    logger.info(f"FB reel start: video_id={video_id}")

    # ── שלב 2: transfer (file_url header עבור CDN-hosted video) ──
    headers = {
        "Authorization": f"OAuth {FB_PAGE_ACCESS_TOKEN}",
        "file_url": cloud_url,
    }
    resp = requests.post(upload_url, headers=headers, timeout=TIMEOUT_LONG)
    if not resp.ok:
        logger.error(f"FB reel transfer failed ({resp.status_code}): {resp.text}")
        resp.raise_for_status()
    logger.info(f"FB reel transfer done for video_id={video_id}")

    # ── שלב 3: finish ──
    resp = requests.post(
        base,
        data={
            "upload_phase": "finish",
            "video_id": video_id,
            "video_state": "PUBLISHED",
            "description": caption,
            "access_token": FB_PAGE_ACCESS_TOKEN,
        },
        timeout=TIMEOUT_LONG,
    )
    if not resp.ok:
        logger.error(f"FB reel finish failed ({resp.status_code}): {resp.text}")
        resp.raise_for_status()

    logger.info(f"FB reel published: {video_id}")
    return video_id


# ═══════════════════════════════════════════════════════════════
#  Facebook Page — Multi-photo post
# ═══════════════════════════════════════════════════════════════

def fb_publish_carousel(
    cloud_urls: list[str],
    caption: str,
    mime_types: list[str],
) -> str:
    """
    מפרסם פוסט עם מספר תמונות בפייסבוק.
    שלבים:
      1. העלאת כל תמונה כ-unpublished photo
      2. יצירת פוסט feed עם attached_media
    סרטונים בתוך קרוסלה: מעלים כרגיל (FB תומך ב-mixed media).
    מחזיר post_id.
    """
    if len(cloud_urls) < 2:
        raise ValueError("Carousel requires at least 2 items")

    # ── שלב 1: העלאת כל פריט כ-unpublished ──
    media_ids = []
    for i, (url, mime) in enumerate(zip(cloud_urls, mime_types)):
        is_video = mime in VIDEO_MIMES
        if is_video:
            media_id = _fb_upload_unpublished_video(url)
        else:
            media_id = _fb_upload_unpublished_photo(url)
        logger.info(f"FB carousel item {i+1}/{len(cloud_urls)}: {media_id}")
        media_ids.append(media_id)

    # ── שלב 2: יצירת פוסט feed עם כל התמונות ──
    url = f"{META_BASE_URL}/{FB_PAGE_ID}/feed"
    data = {
        "message": caption,
        "access_token": FB_PAGE_ACCESS_TOKEN,
    }
    for i, mid in enumerate(media_ids):
        data[f"attached_media[{i}]"] = f'{{"media_fbid":"{mid}"}}'

    resp = requests.post(url, data=data, timeout=TIMEOUT_SHORT)
    if not resp.ok:
        logger.error(f"FB carousel post failed ({resp.status_code}): {resp.text}")
        resp.raise_for_status()

    post_id = resp.json().get("id")
    logger.info(f"FB carousel published: {post_id} ({len(media_ids)} items)")
    return post_id


def _fb_upload_unpublished_photo(cloud_url: str) -> str:
    """העלאת תמונה כ-unpublished (לשימוש ב-multi-photo post)."""
    url = f"{META_BASE_URL}/{FB_PAGE_ID}/photos"
    data = {
        "url": cloud_url,
        "published": "false",
        "access_token": FB_PAGE_ACCESS_TOKEN,
    }

    resp = requests.post(url, data=data, timeout=TIMEOUT_SHORT)
    if not resp.ok:
        logger.error(f"FB unpublished photo failed ({resp.status_code}): {resp.text}")
        resp.raise_for_status()

    return resp.json()["id"]


def _fb_upload_unpublished_video(cloud_url: str) -> str:
    """העלאת וידאו כ-unpublished (לשימוש ב-multi-media post)."""
    url = f"{META_BASE_URL}/{FB_PAGE_ID}/videos"
    data = {
        "file_url": cloud_url,
        "published": "false",
        "access_token": FB_PAGE_ACCESS_TOKEN,
    }

    resp = requests.post(url, data=data, timeout=TIMEOUT_LONG)
    if not resp.ok:
        logger.error(f"FB unpublished video failed ({resp.status_code}): {resp.text}")
        resp.raise_for_status()

    return resp.json()["id"]
