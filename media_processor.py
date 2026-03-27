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

from config import IMAGE_MIMES, POST_TYPE_REELS, VIDEO_MIMES

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
        return _normalize_image(file_bytes, file_name, post_type)

    if mime_type in VIDEO_MIMES:
        return _normalize_video(file_bytes, mime_type, file_name)

    raise MediaProcessingError(
        f"Unsupported MIME type: {mime_type}", "UNSUPPORTED_MEDIA_TYPE"
    )


# ─── Image Processing ────────────────────────────────────────
def _normalize_image(
    file_bytes: bytes, file_name: str, post_type: str = ""
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

    # 5. Validate aspect ratio
    width, height = img.size
    ratio = width / height
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
                "-show_streams", path,
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
