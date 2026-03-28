"""
media_processor.py — נרמול ותיקוף מדיה לפני העלאה ל-Meta API

תמונות: המרה ל-JPEG, שינוי גודל, דחיסה, בדיקת יחס
וידאו:  המרה ל-MP4 H.264+AAC עם faststart
"""

import io
import json
import logging
import os
import subprocess
import tempfile

from PIL import Image, ImageOps, UnidentifiedImageError

from config import (
    IMAGE_MIMES,
    NETWORK_FB,
    NETWORK_GBP,
    NETWORK_IG,
    NETWORK_ALL,
    POST_TYPE_REELS,
    VIDEO_MIMES,
)

logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────
MAX_IMAGE_SIZE = 8_388_608  # 8 MB
TARGET_WIDTH = 1080
MIN_WIDTH = 320
MIN_RATIO = 0.8   # 4:5  (feed)
MAX_RATIO = 1.91  # 1.91:1
REELS_MIN_RATIO = 0.5625  # 9:16
REELS_MAX_RATIO = 1.91    # 1.91:1
JPEG_QUALITY_STEPS = [85, 80, 75, 70, 68]
FFMPEG_TIMEOUT = int(os.environ.get("FFMPEG_TIMEOUT", "300"))  # seconds

# ─── Platform-specific limits ────────────────────────────────
IG_VIDEO_MAX_SIZE = 314_572_800     # 300 MB
FB_VIDEO_MAX_SIZE = 2_147_483_648   # 2 GB
GBP_IMAGE_MIN_SIZE = 10_240         # 10 KB
GBP_IMAGE_MAX_SIZE = 5_242_880      # 5 MB
GBP_IMAGE_MIN_DIM = 250             # 250x250 px minimum
GBP_VIDEO_MAX_SIZE = 78_643_200     # 75 MB
GBP_VIDEO_MAX_DURATION = 30         # seconds
GBP_VIDEO_MIN_HEIGHT = 720          # 720p minimum
IG_VIDEO_MIN_DURATION = 3           # seconds
IG_VIDEO_MAX_DURATION = 900         # 15 minutes
IG_REELS_MAX_DURATION = 900         # 15 minutes
IG_REELS_VIDEO_MIN_RATIO = 0.01    # per Meta API docs
IG_REELS_VIDEO_MAX_RATIO = 10.0    # per Meta API docs


# ─── Network helpers ─────────────────────────────────────────
def _targets_ig(network: str) -> bool:
    """Does this network value include Instagram?"""
    if not network:
        return True  # default: assume IG (strictest)
    return network == NETWORK_ALL or NETWORK_IG in network


def _targets_fb(network: str) -> bool:
    """Does this network value include Facebook?"""
    if not network:
        return True
    return network == NETWORK_ALL or NETWORK_FB in network


def _targets_gbp(network: str) -> bool:
    """Does this network value include Google Business Profile?"""
    if not network:
        return False  # GBP is opt-in, not assumed by default
    return network == NETWORK_ALL or NETWORK_GBP in network


# ─── Exception ────────────────────────────────────────────────
class MediaProcessingError(Exception):
    """שגיאה בעיבוד מדיה עם קוד שגיאה מובנה."""

    def __init__(self, message: str, error_code: str):
        super().__init__(message)
        self.error_code = error_code


# ─── Public API ───────────────────────────────────────────────
def normalize_media(
    file_bytes: bytes,
    mime_type: str,
    file_name: str,
    post_type: str,
    network: str = "",
) -> tuple[bytes, str, str]:
    """נקודת כניסה ראשית — מנרמל מדיה לפי דרישות Meta API.

    Returns:
        (processed_bytes, new_mime_type, new_file_name)
    """
    if not file_bytes:
        raise MediaProcessingError(
            "Empty file received", "UNSUPPORTED_MEDIA_TYPE"
        )

    if mime_type in IMAGE_MIMES:
        return _normalize_image(file_bytes, file_name, post_type, network)

    if mime_type in VIDEO_MIMES:
        return _normalize_video(file_bytes, mime_type, file_name)

    raise MediaProcessingError(
        f"Unsupported MIME type: {mime_type}", "UNSUPPORTED_MEDIA_TYPE"
    )


def validate_media_pre_publish(
    file_bytes: bytes,
    mime_type: str,
    post_type: str,
    network: str,
) -> str | None:
    """בדיקת מדיה לפני פרסום — מחזירה הודעת שגיאה בעברית או None אם תקין.

    בודקת גודל קובץ, יחס גובה-רוחב (תמונות), ומשך (וידאו)
    בהתאם לדרישות הפלטפורמה (Instagram / Facebook / Google Business Profile).
    """
    if not file_bytes:
        return None

    publishes_to_ig = _targets_ig(network)
    publishes_to_fb = _targets_fb(network)
    publishes_to_gbp = _targets_gbp(network)

    if mime_type in IMAGE_MIMES:
        return _validate_image_pre_publish(
            file_bytes, post_type, publishes_to_ig, publishes_to_fb, publishes_to_gbp,
        )

    if mime_type in VIDEO_MIMES:
        return _validate_video_pre_publish(
            file_bytes, post_type, publishes_to_ig, publishes_to_fb, publishes_to_gbp,
        )

    return None


def _validate_image_pre_publish(
    file_bytes: bytes,
    post_type: str,
    publishes_to_ig: bool,
    publishes_to_fb: bool,
    publishes_to_gbp: bool,
) -> str | None:
    """בדיקת תמונה — יחס גובה-רוחב (IG), גודל ורזולוציה (GBP).

    Note: IG/FB image file size is NOT checked here because images are
    compressed to JPEG by normalize_media().  GBP file size IS checked
    because Google enforces a strict 5MB limit on the uploaded file.
    """
    file_size = len(file_bytes)

    # GBP — בדיקת גודל קובץ (Google לא דוחסת אוטומטית)
    if publishes_to_gbp:
        if file_size < GBP_IMAGE_MIN_SIZE:
            size_kb = file_size / 1024
            return f"תמונה קטנה מדי ל-Google Business Profile — {size_kb:.1f}KB (מינימום 10KB)"
        if file_size > GBP_IMAGE_MAX_SIZE:
            size_mb = file_size / (1024 * 1024)
            return f"תמונה גדולה מדי ל-Google Business Profile — {size_mb:.1f}MB (מקסימום 5MB)"

    # פתיחת התמונה — נדרש לבדיקות IG ו-GBP
    needs_image_open = publishes_to_ig or publishes_to_gbp
    if needs_image_open:
        try:
            img = Image.open(io.BytesIO(file_bytes))
            img.load()
            img = ImageOps.exif_transpose(img)
        except Exception:
            return "לא ניתן לפתוח את התמונה — ייתכן שהקובץ פגום"

        width, height = img.size
        if height == 0:
            return "תמונה לא תקינה — גובה 0 פיקסלים"

        # GBP — בדיקת רזולוציה מינימלית
        if publishes_to_gbp:
            if width < GBP_IMAGE_MIN_DIM or height < GBP_IMAGE_MIN_DIM:
                return (
                    f"תמונה קטנה מדי ל-Google Business Profile — "
                    f"{width}x{height} (מינימום {GBP_IMAGE_MIN_DIM}x{GBP_IMAGE_MIN_DIM})"
                )

        # IG — בדיקת יחס גובה-רוחב
        if publishes_to_ig:
            ratio = width / height
            if post_type == POST_TYPE_REELS:
                if ratio < REELS_MIN_RATIO or ratio > REELS_MAX_RATIO:
                    return (
                        f"תמונה לא תקינה ל-Instagram Reels — "
                        f"יחס {ratio:.2f} (נדרש בין {REELS_MIN_RATIO} ל-{REELS_MAX_RATIO}). "
                        f"מומלץ 9:16. מידות: {width}x{height}"
                    )
            else:
                if ratio < MIN_RATIO or ratio > MAX_RATIO:
                    return (
                        f"תמונה לא תקינה ל-Instagram — "
                        f"יחס {ratio:.2f} (נדרש בין {MIN_RATIO} ל-{MAX_RATIO}). "
                        f"מומלץ 1:1 או 4:5. מידות: {width}x{height}"
                    )

    return None


def _validate_video_pre_publish(
    file_bytes: bytes,
    post_type: str,
    publishes_to_ig: bool,
    publishes_to_fb: bool,
    publishes_to_gbp: bool,
) -> str | None:
    """בדיקת וידאו — גודל קובץ, משך, יחס גובה-רוחב.

    Note: file size is checked on the raw (pre-transcode) bytes.
    Transcoding can change the size, but checking here avoids wasting
    minutes of CPU on a file that is obviously too large.  This is a
    best-effort early filter, not a strict post-transcode guard.
    """
    file_size = len(file_bytes)

    # בדיקת גודל קובץ (pre-transcode — best-effort early filter)
    if publishes_to_ig and file_size > IG_VIDEO_MAX_SIZE:
        size_mb = file_size / (1024 * 1024)
        return f"סרטון גדול מדי ל-Instagram — {size_mb:.0f}MB (מקסימום 300MB)"
    if publishes_to_fb and file_size > FB_VIDEO_MAX_SIZE:
        size_mb = file_size / (1024 * 1024)
        return f"סרטון גדול מדי ל-Facebook — {size_mb:.0f}MB (מקסימום 2GB)"
    if publishes_to_gbp and file_size > GBP_VIDEO_MAX_SIZE:
        size_mb = file_size / (1024 * 1024)
        return f"סרטון גדול מדי ל-Google Business Profile — {size_mb:.0f}MB (מקסימום 75MB)"

    # בדיקת משך ויחס — צריך ffprobe
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
            tmp.write(file_bytes)
            tmp_path = tmp.name

        probe = _probe_video(tmp_path)
    except MediaProcessingError:
        return "לא ניתן לקרוא את הסרטון — ייתכן שהקובץ פגום"
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    duration = _get_video_duration(probe)
    video_ratio = _get_video_aspect_ratio(probe)

    # בדיקות Instagram — משך ויחס
    if publishes_to_ig:
        if duration is not None:
            if duration < IG_VIDEO_MIN_DURATION:
                return f"סרטון קצר מדי ל-Instagram — {duration:.1f} שניות (מינימום {IG_VIDEO_MIN_DURATION} שניות)"
            max_dur = IG_REELS_MAX_DURATION if post_type == POST_TYPE_REELS else IG_VIDEO_MAX_DURATION
            if duration > max_dur:
                mins = duration / 60
                max_mins = max_dur / 60
                return f"סרטון ארוך מדי ל-Instagram — {mins:.1f} דקות (מקסימום {max_mins:.0f} דקות)"

        if video_ratio is not None:
            if post_type == POST_TYPE_REELS:
                if video_ratio < IG_REELS_VIDEO_MIN_RATIO or video_ratio > IG_REELS_VIDEO_MAX_RATIO:
                    return (
                        f"סרטון עם יחס לא תקין ל-Instagram Reels — "
                        f"יחס {video_ratio:.2f} (נדרש בין {IG_REELS_VIDEO_MIN_RATIO} ל-{IG_REELS_VIDEO_MAX_RATIO}). מומלץ 9:16"
                    )
            else:
                if video_ratio < MIN_RATIO or video_ratio > MAX_RATIO:
                    return (
                        f"סרטון עם יחס לא תקין ל-Instagram — "
                        f"יחס {video_ratio:.2f} (נדרש בין {MIN_RATIO} ל-{MAX_RATIO}). "
                        f"מומלץ 1:1 או 4:5"
                    )

    # בדיקות GBP — משך ורזולוציה
    if publishes_to_gbp:
        if duration is not None and duration > GBP_VIDEO_MAX_DURATION:
            return (
                f"סרטון ארוך מדי ל-Google Business Profile — "
                f"{duration:.0f} שניות (מקסימום {GBP_VIDEO_MAX_DURATION} שניות)"
            )
        video_height = _get_video_height(probe)
        if video_height is not None and video_height < GBP_VIDEO_MIN_HEIGHT:
            return (
                f"סרטון ברזולוציה נמוכה מדי ל-Google Business Profile — "
                f"{video_height}p (מינימום {GBP_VIDEO_MIN_HEIGHT}p)"
            )

    return None


def _get_video_duration(probe: dict) -> float | None:
    """חילוץ משך הסרטון מתוצאת ffprobe (בשניות)."""
    # Try format-level duration first
    fmt = probe.get("format", {})
    dur_str = fmt.get("duration")
    if dur_str:
        try:
            return float(dur_str)
        except (ValueError, TypeError):
            pass

    # Fallback to first video stream duration
    for stream in probe.get("streams", []):
        if stream.get("codec_type") == "video":
            dur_str = stream.get("duration")
            if dur_str:
                try:
                    return float(dur_str)
                except (ValueError, TypeError):
                    pass
    return None


def _get_video_aspect_ratio(probe: dict) -> float | None:
    """חילוץ יחס גובה-רוחב מסטרים הווידאו."""
    for stream in probe.get("streams", []):
        if stream.get("codec_type") == "video":
            w = stream.get("width")
            h = stream.get("height")
            if w and h and int(h) > 0:
                return int(w) / int(h)
    return None


def _get_video_height(probe: dict) -> int | None:
    """חילוץ גובה הווידאו בפיקסלים (לבדיקת רזולוציה מינימלית)."""
    for stream in probe.get("streams", []):
        if stream.get("codec_type") == "video":
            h = stream.get("height")
            if h is not None:
                return int(h)
    return None


# ─── Image Processing ────────────────────────────────────────
def _normalize_image(
    file_bytes: bytes, file_name: str, post_type: str = "", network: str = ""
) -> tuple[bytes, str, str]:
    """המרת תמונה ל-JPEG תקין לפי דרישות Instagram API."""
    # 1. Open & fix EXIF orientation
    try:
        img = Image.open(io.BytesIO(file_bytes))
        img.load()
        img = ImageOps.exif_transpose(img)
    except (UnidentifiedImageError, Exception) as exc:
        raise MediaProcessingError(
            f"Cannot open image: {exc}", "UNSUPPORTED_MEDIA_TYPE"
        ) from exc

    # 3. Flatten transparency
    if img.mode in ("RGBA", "LA") or (
        img.mode == "P" and "transparency" in img.info
    ):
        background = Image.new("RGB", img.size, (255, 255, 255))
        # Convert palette mode to RGBA first
        alpha_img = img.convert("RGBA")
        background.paste(alpha_img, mask=alpha_img.split()[3])
        img = background

    # 4. Ensure RGB
    if img.mode != "RGB":
        img = img.convert("RGB")

    # 5. Validate aspect ratio (Instagram only — Facebook has no strict ratio)
    width, height = img.size
    ratio = width / height
    publishes_to_ig = _targets_ig(network)
    if publishes_to_ig:
        if post_type == POST_TYPE_REELS:
            min_r, max_r = REELS_MIN_RATIO, REELS_MAX_RATIO
            error_code = "INVALID_REELS_RATIO"
        else:
            min_r, max_r = MIN_RATIO, MAX_RATIO
            error_code = "INVALID_FEED_RATIO"
        if ratio < min_r or ratio > max_r:
            raise MediaProcessingError(
                f"Invalid aspect ratio {ratio:.2f} "
                f"(must be between {min_r} and {max_r}). "
                f"Image dimensions: {width}x{height}",
                error_code,
            )

    # 6. Resize
    if width > TARGET_WIDTH:
        new_height = int(height * TARGET_WIDTH / width)
        img = img.resize((TARGET_WIDTH, new_height), Image.LANCZOS)
        logger.info(f"Resized image from {width}x{height} to {TARGET_WIDTH}x{new_height}")
    elif width < MIN_WIDTH:
        new_height = int(height * MIN_WIDTH / width)
        img = img.resize((MIN_WIDTH, new_height), Image.LANCZOS)
        logger.info(f"Upscaled image from {width}x{height} to {MIN_WIDTH}x{new_height}")

    # 7. Progressive JPEG compression
    jpeg_bytes = _compress_jpeg(img)

    # 8. Update filename
    new_name = _replace_extension(file_name, ".jpg")

    logger.info(
        f"Image normalized: {file_name} → {new_name} | "
        f"Size: {len(jpeg_bytes)} bytes | "
        f"Dimensions: {img.size[0]}x{img.size[1]}"
    )

    return jpeg_bytes, "image/jpeg", new_name


def _compress_jpeg(img: Image.Image) -> bytes:
    """דחיסת JPEG פרוגרסיבית — מנסה רמות איכות יורדות עד שמתחת ל-8MB."""
    for quality in JPEG_QUALITY_STEPS:
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=quality, optimize=True, progressive=True)
        data = buf.getvalue()
        if len(data) <= MAX_IMAGE_SIZE:
            if quality < JPEG_QUALITY_STEPS[0]:
                logger.info(f"Compressed JPEG to quality={quality}, size={len(data)} bytes")
            return data

    raise MediaProcessingError(
        f"Image exceeds {MAX_IMAGE_SIZE // (1024 * 1024)}MB even at quality "
        f"{JPEG_QUALITY_STEPS[-1]} ({len(data)} bytes)",
        "IMAGE_TOO_LARGE",
    )


# ─── Video Processing ────────────────────────────────────────
def _normalize_video(
    file_bytes: bytes, mime_type: str, file_name: str
) -> tuple[bytes, str, str]:
    """נרמול וידאו ל-MP4 H.264+AAC עם faststart."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Determine input suffix from MIME
        suffix_map = {
            "video/mp4": ".mp4",
            "video/quicktime": ".mov",
            "video/x-msvideo": ".avi",
            "video/mpeg": ".mpg",
            "video/webm": ".webm",
        }
        in_suffix = suffix_map.get(mime_type, ".mp4")
        in_path = os.path.join(tmpdir, f"input{in_suffix}")
        out_path = os.path.join(tmpdir, "output.mp4")

        with open(in_path, "wb") as f:
            f.write(file_bytes)

        # Probe video
        probe = _probe_video(in_path)
        compliant = _is_video_compliant(probe)

        # Build ffmpeg command
        if compliant:
            # Fast remux — no re-encoding
            cmd = [
                "ffmpeg", "-i", in_path,
                "-c", "copy",
                "-movflags", "+faststart",
                "-y", out_path,
            ]
            logger.info("Video already H.264+AAC — remuxing with faststart")
        else:
            # Full transcode
            has_audio = _has_audio_stream(probe)
            cmd = [
                "ffmpeg", "-i", in_path,
                "-c:v", "libx264",
                "-pix_fmt", "yuv420p",
            ]
            if has_audio:
                cmd += ["-c:a", "aac"]
            else:
                cmd += ["-an"]
            cmd += ["-movflags", "+faststart", "-y", out_path]
            logger.info("Transcoding video to H.264+AAC MP4")

        # Run ffmpeg
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                timeout=FFMPEG_TIMEOUT,
            )
        except subprocess.TimeoutExpired as exc:
            raise MediaProcessingError(
                f"Video transcode timed out after {FFMPEG_TIMEOUT}s",
                "VIDEO_TRANSCODE_FAILED",
            ) from exc

        if result.returncode != 0:
            stderr = result.stderr.decode("utf-8", errors="replace")[-500:]
            raise MediaProcessingError(
                f"ffmpeg failed (exit {result.returncode}): {stderr}",
                "VIDEO_TRANSCODE_FAILED",
            )

        with open(out_path, "rb") as f:
            mp4_bytes = f.read()

    new_name = _replace_extension(file_name, ".mp4")
    logger.info(
        f"Video normalized: {file_name} → {new_name} | "
        f"Size: {len(mp4_bytes)} bytes"
    )
    return mp4_bytes, "video/mp4", new_name


# ─── Video Helpers ────────────────────────────────────────────
def _probe_video(path: str) -> dict:
    """הרצת ffprobe וקבלת מידע על הסטרימים."""
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "quiet",
                "-print_format", "json",
                "-show_streams", "-show_format", path,
            ],
            capture_output=True,
            timeout=30,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
        raise MediaProcessingError(
            f"ffprobe failed: {exc}", "VIDEO_TRANSCODE_FAILED"
        ) from exc

    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace")[-300:]
        raise MediaProcessingError(
            f"ffprobe failed (exit {result.returncode}): {stderr}",
            "VIDEO_TRANSCODE_FAILED",
        )

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise MediaProcessingError(
            f"ffprobe returned invalid JSON: {exc}",
            "VIDEO_TRANSCODE_FAILED",
        ) from exc


def _is_video_compliant(probe: dict) -> bool:
    """בדיקה אם הוידאו כבר H.264 + AAC + MP4."""
    streams = probe.get("streams", [])

    video_ok = False
    audio_ok = True  # True by default (no audio = OK)

    for stream in streams:
        codec_type = stream.get("codec_type", "")
        codec_name = stream.get("codec_name", "")

        if codec_type == "video":
            video_ok = video_ok or codec_name == "h264"
        elif codec_type == "audio":
            if codec_name != "aac":
                audio_ok = False

    return video_ok and audio_ok


def _has_audio_stream(probe: dict) -> bool:
    """בדיקה אם יש סטרים אודיו בוידאו."""
    return any(
        s.get("codec_type") == "audio"
        for s in probe.get("streams", [])
    )


# ─── General Helpers ──────────────────────────────────────────
def _replace_extension(file_name: str, new_ext: str) -> str:
    """החלפת סיומת קובץ."""
    base, _ = os.path.splitext(file_name)
    return base + new_ext
