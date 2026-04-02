"""
test_media_processor.py — בדיקות יחידה ל-media_processor.py

מכסה: נרמול תמונות (המרה, שינוי גודל, דחיסה, יחס),
       נרמול וידאו (ffprobe/ffmpeg mocking), dispatch.
"""

import io
import json
import struct
from unittest.mock import patch, MagicMock

import pytest
from PIL import Image, ExifTags

from media_processor import (
    normalize_media,
    MediaProcessingError,
    _normalize_image,
    _normalize_video,
    _replace_extension,
    _is_video_compliant,
    _has_audio_stream,
    _targets_li,
    MAX_IMAGE_SIZE,
    MIN_RATIO,
    MAX_RATIO,
    MIN_WIDTH,
    TARGET_WIDTH,
    LI_VIDEO_MAX_SIZE,
    LI_VIDEO_MAX_DURATION,
)


# ─── Helpers ──────────────────────────────────────────────────
def _make_image(
    width=800,
    height=600,
    mode="RGB",
    fmt="PNG",
    color=(100, 150, 200),
) -> bytes:
    """יוצר תמונה בזיכרון ומחזיר bytes."""
    if mode == "RGBA":
        color = color + (128,)
    img = Image.new(mode, (width, height), color)
    buf = io.BytesIO()
    # BMP doesn't support RGBA
    if fmt == "BMP" and mode == "RGBA":
        img = img.convert("RGB")
    img.save(buf, format=fmt)
    return buf.getvalue()


def _make_jpeg_with_exif_orientation(width=800, height=600, orientation=6):
    """יוצר JPEG עם תג EXIF orientation."""
    img = Image.new("RGB", (width, height), (100, 150, 200))

    # Use piexif-free approach: save JPEG, then manually set orientation
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    # Re-open and use Pillow's exif support
    buf.seek(0)
    img2 = Image.open(buf)
    exif = img2.getexif()
    # Tag 274 = Orientation
    exif[274] = orientation
    buf2 = io.BytesIO()
    img.save(buf2, format="JPEG", exif=exif.tobytes())
    return buf2.getvalue()


def _make_ffprobe_output(
    video_codec="h264", audio_codec="aac", has_audio=True
) -> bytes:
    """יוצר פלט JSON של ffprobe."""
    streams = [
        {"codec_type": "video", "codec_name": video_codec},
    ]
    if has_audio:
        streams.append({"codec_type": "audio", "codec_name": audio_codec})
    return json.dumps({"streams": streams}).encode()


# ═══════════════════════════════════════════════════════════════
#  _replace_extension
# ═══════════════════════════════════════════════════════════════

class TestReplaceExtension:
    def test_png_to_jpg(self):
        assert _replace_extension("photo.png", ".jpg") == "photo.jpg"

    def test_multiple_dots(self):
        assert _replace_extension("my.photo.2024.png", ".jpg") == "my.photo.2024.jpg"

    def test_no_extension(self):
        assert _replace_extension("noext", ".jpg") == "noext.jpg"

    def test_mov_to_mp4(self):
        assert _replace_extension("clip.mov", ".mp4") == "clip.mp4"


# ═══════════════════════════════════════════════════════════════
#  _is_video_compliant / _has_audio_stream
# ═══════════════════════════════════════════════════════════════

class TestVideoHelpers:
    def test_compliant_h264_aac(self):
        probe = {"streams": [
            {"codec_type": "video", "codec_name": "h264"},
            {"codec_type": "audio", "codec_name": "aac"},
        ]}
        assert _is_video_compliant(probe) is True

    def test_non_compliant_vp9(self):
        probe = {"streams": [
            {"codec_type": "video", "codec_name": "vp9"},
            {"codec_type": "audio", "codec_name": "opus"},
        ]}
        assert _is_video_compliant(probe) is False

    def test_h264_no_audio_is_compliant(self):
        probe = {"streams": [
            {"codec_type": "video", "codec_name": "h264"},
        ]}
        assert _is_video_compliant(probe) is True

    def test_h264_wrong_audio_not_compliant(self):
        probe = {"streams": [
            {"codec_type": "video", "codec_name": "h264"},
            {"codec_type": "audio", "codec_name": "mp3"},
        ]}
        assert _is_video_compliant(probe) is False

    def test_multiple_audio_streams_non_compliant_if_any_not_aac(self):
        """mp3 track before aac should still fail compliance."""
        probe = {"streams": [
            {"codec_type": "video", "codec_name": "h264"},
            {"codec_type": "audio", "codec_name": "mp3"},
            {"codec_type": "audio", "codec_name": "aac"},
        ]}
        assert _is_video_compliant(probe) is False

    def test_has_audio_true(self):
        probe = {"streams": [
            {"codec_type": "video", "codec_name": "h264"},
            {"codec_type": "audio", "codec_name": "aac"},
        ]}
        assert _has_audio_stream(probe) is True

    def test_has_audio_false(self):
        probe = {"streams": [
            {"codec_type": "video", "codec_name": "h264"},
        ]}
        assert _has_audio_stream(probe) is False


# ═══════════════════════════════════════════════════════════════
#  _normalize_image
# ═══════════════════════════════════════════════════════════════

class TestNormalizeImage:
    def test_jpeg_passthrough_no_resize(self):
        """JPEG within valid size range passes through as JPEG."""
        data = _make_image(800, 600, fmt="JPEG")
        result, mime, name = _normalize_image(data, "photo.jpg")
        assert mime == "image/jpeg"
        assert name == "photo.jpg"
        # Verify it's valid JPEG
        img = Image.open(io.BytesIO(result))
        assert img.format == "JPEG"
        # Width unchanged (between 320 and 1080)
        assert img.size[0] == 800

    def test_png_converted_to_jpeg(self):
        data = _make_image(800, 600, fmt="PNG")
        result, mime, name = _normalize_image(data, "photo.png")
        assert mime == "image/jpeg"
        assert name == "photo.jpg"
        img = Image.open(io.BytesIO(result))
        assert img.format == "JPEG"

    def test_webp_converted_to_jpeg(self):
        data = _make_image(800, 600, fmt="WEBP")
        result, mime, name = _normalize_image(data, "photo.webp")
        assert mime == "image/jpeg"
        assert name == "photo.jpg"

    def test_bmp_converted_to_jpeg(self):
        data = _make_image(800, 600, fmt="BMP")
        result, mime, name = _normalize_image(data, "image.bmp")
        assert mime == "image/jpeg"
        assert name == "image.jpg"

    def test_rgba_transparency_flattened(self):
        """RGBA PNG should be flattened to RGB with white background."""
        data = _make_image(800, 600, mode="RGBA", fmt="PNG")
        result, mime, name = _normalize_image(data, "transparent.png")
        img = Image.open(io.BytesIO(result))
        assert img.mode == "RGB"
        assert mime == "image/jpeg"

    def test_large_image_resized_to_1080(self):
        data = _make_image(4000, 3000, fmt="JPEG")
        result, _, _ = _normalize_image(data, "big.jpg")
        img = Image.open(io.BytesIO(result))
        assert img.size[0] == TARGET_WIDTH
        assert img.size[1] == int(3000 * 1080 / 4000)

    def test_small_image_below_320_resized_up(self):
        data = _make_image(200, 150, fmt="PNG")
        result, _, _ = _normalize_image(data, "tiny.png")
        img = Image.open(io.BytesIO(result))
        assert img.size[0] == MIN_WIDTH

    def test_image_between_320_and_1080_unchanged_width(self):
        data = _make_image(800, 600, fmt="JPEG")
        result, _, _ = _normalize_image(data, "mid.jpg")
        img = Image.open(io.BytesIO(result))
        assert img.size[0] == 800

    def test_invalid_ratio_too_tall(self):
        """Ratio 0.5 (1000/2000) < 0.8 → INVALID_FEED_RATIO."""
        data = _make_image(500, 1000, fmt="JPEG")
        with pytest.raises(MediaProcessingError) as exc_info:
            _normalize_image(data, "tall.jpg")
        assert exc_info.value.error_code == "INVALID_FEED_RATIO"

    def test_invalid_ratio_too_wide(self):
        """Ratio 4.0 (2000/500) > 1.91 → INVALID_FEED_RATIO."""
        data = _make_image(2000, 500, fmt="JPEG")
        with pytest.raises(MediaProcessingError) as exc_info:
            _normalize_image(data, "wide.jpg")
        assert exc_info.value.error_code == "INVALID_FEED_RATIO"

    def test_valid_ratio_boundary_4_5(self):
        """Ratio exactly 0.8 (4:5) should pass."""
        data = _make_image(800, 1000, fmt="JPEG")
        result, mime, _ = _normalize_image(data, "ratio45.jpg")
        assert mime == "image/jpeg"

    def test_valid_ratio_boundary_191(self):
        """Ratio exactly 1.91 should pass."""
        data = _make_image(955, 500, fmt="JPEG")  # 955/500 = 1.91
        result, mime, _ = _normalize_image(data, "ratio191.jpg")
        assert mime == "image/jpeg"

    def test_grayscale_converted_to_rgb(self):
        data = _make_image(800, 600, mode="L", fmt="JPEG", color=128)
        result, mime, _ = _normalize_image(data, "gray.jpg")
        img = Image.open(io.BytesIO(result))
        assert img.mode == "RGB"

    def test_output_under_8mb(self):
        data = _make_image(1080, 1080, fmt="PNG")
        result, _, _ = _normalize_image(data, "normal.png")
        assert len(result) <= MAX_IMAGE_SIZE

    def test_file_extension_replaced(self):
        data = _make_image(800, 600, fmt="PNG")
        _, _, name = _normalize_image(data, "my.photo.png")
        assert name == "my.photo.jpg"

    def test_reels_post_type_allows_9_16_ratio(self):
        """9:16 ratio (0.5625) should pass for REELS but fail for FEED."""
        data = _make_image(900, 1600, fmt="JPEG")
        # Should fail for feed
        with pytest.raises(MediaProcessingError) as exc_info:
            _normalize_image(data, "tall.jpg")
        assert exc_info.value.error_code == "INVALID_FEED_RATIO"
        # Should pass for reels
        result, mime, _ = _normalize_image(data, "tall.jpg", post_type="REELS")
        assert mime == "image/jpeg"

    def test_reels_post_type_rejects_too_tall(self):
        """Ratio below 9:16 should fail even for REELS."""
        data = _make_image(400, 1000, fmt="JPEG")  # ratio 0.4
        with pytest.raises(MediaProcessingError) as exc_info:
            _normalize_image(data, "tall.jpg", post_type="REELS")
        assert exc_info.value.error_code == "INVALID_REELS_RATIO"

    def test_corrupted_file_raises(self):
        with pytest.raises(MediaProcessingError) as exc_info:
            _normalize_image(b"not an image at all", "bad.jpg")
        assert exc_info.value.error_code == "UNSUPPORTED_MEDIA_TYPE"

    def test_gif_first_frame_extracted(self):
        """GIF should be converted to JPEG (first frame)."""
        data = _make_image(800, 600, fmt="GIF", color=128)
        result, mime, name = _normalize_image(data, "anim.gif")
        assert mime == "image/jpeg"
        assert name == "anim.jpg"


# ═══════════════════════════════════════════════════════════════
#  _normalize_image — EXIF orientation
# ═══════════════════════════════════════════════════════════════

class TestImageExifOrientation:
    def test_exif_rotation_applied(self):
        """Image with EXIF orientation 6 (90 CW) should have dimensions swapped."""
        # Use 2000x1600 so after rotation (1600x2000) ratio is 0.8, within 0.8–1.91
        data = _make_jpeg_with_exif_orientation(2000, 1600, orientation=6)
        result, _, _ = _normalize_image(data, "rotated.jpg")
        img = Image.open(io.BytesIO(result))
        # After 90 CW rotation, 2000x1600 becomes 1600x2000
        # Then resized to TARGET_WIDTH=1080 → 1080x1350
        assert img.size[0] == 1080
        assert img.size[1] == 1350

    def test_malformed_exif_raises_media_error(self):
        """Malformed EXIF data should raise MediaProcessingError, not a raw exception."""
        # Create a JPEG with corrupted EXIF segment
        img = Image.new("RGB", (1080, 1080), (100, 150, 200))
        buf = io.BytesIO()
        img.save(buf, format="JPEG")
        raw = buf.getvalue()
        # Inject garbage EXIF marker (APP1 with invalid data)
        # JPEG starts with FF D8; insert a corrupt APP1 segment after it
        corrupt_exif = b"\xff\xe1\x00\x10Exif\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff"
        corrupted = raw[:2] + corrupt_exif + raw[2:]
        # Should either succeed (Pillow ignores bad EXIF) or raise MediaProcessingError
        try:
            result, mime, _ = _normalize_image(corrupted, "bad_exif.jpg")
            assert mime == "image/jpeg"
        except MediaProcessingError:
            pass  # This is the acceptable error path


# ═══════════════════════════════════════════════════════════════
#  _normalize_video (mocked subprocess)
# ═══════════════════════════════════════════════════════════════

class TestNormalizeVideo:
    @patch("media_processor.subprocess.run")
    def test_compliant_mp4_remuxed(self, mock_run):
        """H.264+AAC MP4 should be remuxed (copy), not transcoded."""
        probe_result = MagicMock(
            returncode=0,
            stdout=_make_ffprobe_output("h264", "aac"),
        )
        ffmpeg_result = MagicMock(returncode=0)
        mock_run.side_effect = [probe_result, ffmpeg_result]

        # We need a real temp file for output — mock the read
        with patch("builtins.open", create=True) as mock_open:
            mock_open.return_value.__enter__ = lambda s: s
            mock_open.return_value.__exit__ = MagicMock(return_value=False)
            mock_open.return_value.write = MagicMock()
            mock_open.return_value.read = MagicMock(return_value=b"mp4data")

            # Use tmpdir directly
            with patch("media_processor.tempfile.TemporaryDirectory") as mock_tmpdir:
                mock_tmpdir.return_value.__enter__ = MagicMock(return_value="/tmp/fakedir")
                mock_tmpdir.return_value.__exit__ = MagicMock(return_value=False)

                result, mime, name = _normalize_video(b"fakemp4", "video/mp4", "clip.mp4")

        assert mime == "video/mp4"
        assert name == "clip.mp4"
        # Verify ffmpeg used -c copy (remux)
        ffmpeg_call = mock_run.call_args_list[1]
        cmd = ffmpeg_call[0][0]
        assert "-c" in cmd and "copy" in cmd

    @patch("media_processor.subprocess.run")
    def test_non_h264_transcoded(self, mock_run):
        """VP9+Opus should trigger full transcode."""
        probe_result = MagicMock(
            returncode=0,
            stdout=_make_ffprobe_output("vp9", "opus"),
        )
        ffmpeg_result = MagicMock(returncode=0)
        mock_run.side_effect = [probe_result, ffmpeg_result]

        with patch("media_processor.tempfile.TemporaryDirectory") as mock_tmpdir:
            mock_tmpdir.return_value.__enter__ = MagicMock(return_value="/tmp/fakedir")
            mock_tmpdir.return_value.__exit__ = MagicMock(return_value=False)
            with patch("builtins.open", create=True) as mock_open:
                mock_open.return_value.__enter__ = lambda s: s
                mock_open.return_value.__exit__ = MagicMock(return_value=False)
                mock_open.return_value.write = MagicMock()
                mock_open.return_value.read = MagicMock(return_value=b"mp4data")

                result, mime, name = _normalize_video(b"fakewebm", "video/webm", "clip.webm")

        assert mime == "video/mp4"
        assert name == "clip.mp4"
        # Verify ffmpeg used libx264 (transcode)
        ffmpeg_call = mock_run.call_args_list[1]
        cmd = ffmpeg_call[0][0]
        assert "libx264" in cmd

    @patch("media_processor.subprocess.run")
    def test_ffprobe_failure_raises(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=1,
            stderr=b"error reading file",
        )
        with patch("media_processor.tempfile.TemporaryDirectory") as mock_tmpdir:
            mock_tmpdir.return_value.__enter__ = MagicMock(return_value="/tmp/fakedir")
            mock_tmpdir.return_value.__exit__ = MagicMock(return_value=False)
            with patch("builtins.open", create=True) as mock_open:
                mock_open.return_value.__enter__ = lambda s: s
                mock_open.return_value.__exit__ = MagicMock(return_value=False)
                mock_open.return_value.write = MagicMock()

                with pytest.raises(MediaProcessingError) as exc_info:
                    _normalize_video(b"bad", "video/mp4", "bad.mp4")
                assert exc_info.value.error_code == "VIDEO_TRANSCODE_FAILED"

    @patch("media_processor.subprocess.run")
    def test_ffmpeg_failure_raises(self, mock_run):
        probe_result = MagicMock(
            returncode=0,
            stdout=_make_ffprobe_output("h264", "aac"),
        )
        ffmpeg_result = MagicMock(
            returncode=1,
            stderr=b"encoding failed",
        )
        mock_run.side_effect = [probe_result, ffmpeg_result]

        with patch("media_processor.tempfile.TemporaryDirectory") as mock_tmpdir:
            mock_tmpdir.return_value.__enter__ = MagicMock(return_value="/tmp/fakedir")
            mock_tmpdir.return_value.__exit__ = MagicMock(return_value=False)
            with patch("builtins.open", create=True) as mock_open:
                mock_open.return_value.__enter__ = lambda s: s
                mock_open.return_value.__exit__ = MagicMock(return_value=False)
                mock_open.return_value.write = MagicMock()

                with pytest.raises(MediaProcessingError) as exc_info:
                    _normalize_video(b"fakemp4", "video/mp4", "clip.mp4")
                assert exc_info.value.error_code == "VIDEO_TRANSCODE_FAILED"

    @patch("media_processor.subprocess.run")
    def test_ffmpeg_timeout_raises(self, mock_run):
        import subprocess as sp
        probe_result = MagicMock(
            returncode=0,
            stdout=_make_ffprobe_output("vp9", "opus"),
        )
        mock_run.side_effect = [probe_result, sp.TimeoutExpired(cmd="ffmpeg", timeout=300)]

        with patch("media_processor.tempfile.TemporaryDirectory") as mock_tmpdir:
            mock_tmpdir.return_value.__enter__ = MagicMock(return_value="/tmp/fakedir")
            mock_tmpdir.return_value.__exit__ = MagicMock(return_value=False)
            with patch("builtins.open", create=True) as mock_open:
                mock_open.return_value.__enter__ = lambda s: s
                mock_open.return_value.__exit__ = MagicMock(return_value=False)
                mock_open.return_value.write = MagicMock()

                with pytest.raises(MediaProcessingError) as exc_info:
                    _normalize_video(b"fakewebm", "video/webm", "clip.webm")
                assert exc_info.value.error_code == "VIDEO_TRANSCODE_FAILED"

    @patch("media_processor.subprocess.run")
    def test_mov_extension_changed_to_mp4(self, mock_run):
        probe_result = MagicMock(
            returncode=0,
            stdout=_make_ffprobe_output("h264", "aac"),
        )
        ffmpeg_result = MagicMock(returncode=0)
        mock_run.side_effect = [probe_result, ffmpeg_result]

        with patch("media_processor.tempfile.TemporaryDirectory") as mock_tmpdir:
            mock_tmpdir.return_value.__enter__ = MagicMock(return_value="/tmp/fakedir")
            mock_tmpdir.return_value.__exit__ = MagicMock(return_value=False)
            with patch("builtins.open", create=True) as mock_open:
                mock_open.return_value.__enter__ = lambda s: s
                mock_open.return_value.__exit__ = MagicMock(return_value=False)
                mock_open.return_value.write = MagicMock()
                mock_open.return_value.read = MagicMock(return_value=b"mp4data")

                _, mime, name = _normalize_video(b"fakemov", "video/quicktime", "clip.mov")

        assert mime == "video/mp4"
        assert name == "clip.mp4"

    @patch("media_processor.subprocess.run")
    def test_no_audio_stream_uses_an_flag(self, mock_run):
        """Video without audio should use -an flag instead of -c:a aac."""
        probe_result = MagicMock(
            returncode=0,
            stdout=_make_ffprobe_output("vp9", has_audio=False),
        )
        ffmpeg_result = MagicMock(returncode=0)
        mock_run.side_effect = [probe_result, ffmpeg_result]

        with patch("media_processor.tempfile.TemporaryDirectory") as mock_tmpdir:
            mock_tmpdir.return_value.__enter__ = MagicMock(return_value="/tmp/fakedir")
            mock_tmpdir.return_value.__exit__ = MagicMock(return_value=False)
            with patch("builtins.open", create=True) as mock_open:
                mock_open.return_value.__enter__ = lambda s: s
                mock_open.return_value.__exit__ = MagicMock(return_value=False)
                mock_open.return_value.write = MagicMock()
                mock_open.return_value.read = MagicMock(return_value=b"mp4data")

                _normalize_video(b"fakevid", "video/webm", "silent.webm")

        ffmpeg_call = mock_run.call_args_list[1]
        cmd = ffmpeg_call[0][0]
        assert "-an" in cmd
        assert "-c:a" not in cmd


# ═══════════════════════════════════════════════════════════════
#  normalize_media — dispatch
# ═══════════════════════════════════════════════════════════════

class TestNormalizeMediaDispatch:
    def test_image_mime_dispatches_to_image(self):
        """image/png should route to image processing."""
        data = _make_image(800, 600, fmt="PNG")
        result, mime, name = normalize_media(data, "image/png", "pic.png", "FEED")
        assert mime == "image/jpeg"
        assert name == "pic.jpg"

    def test_jpeg_mime_dispatches_to_image(self):
        data = _make_image(800, 600, fmt="JPEG")
        result, mime, name = normalize_media(data, "image/jpeg", "pic.jpg", "FEED")
        assert mime == "image/jpeg"

    @patch("media_processor._normalize_video", return_value=(b"mp4", "video/mp4", "v.mp4"))
    def test_video_mime_dispatches_to_video(self, mock_vid):
        normalize_media(b"vid", "video/mp4", "v.mp4", "FEED")
        mock_vid.assert_called_once()

    def test_unsupported_mime_raises(self):
        with pytest.raises(MediaProcessingError) as exc_info:
            normalize_media(b"data", "application/pdf", "doc.pdf", "FEED")
        assert exc_info.value.error_code == "UNSUPPORTED_MEDIA_TYPE"

    def test_empty_bytes_raises(self):
        with pytest.raises(MediaProcessingError) as exc_info:
            normalize_media(b"", "image/jpeg", "empty.jpg", "FEED")
        assert exc_info.value.error_code == "UNSUPPORTED_MEDIA_TYPE"

    def test_gif_treated_as_image(self):
        """image/gif should go through image processing."""
        data = _make_image(800, 600, fmt="GIF", color=128)
        result, mime, name = normalize_media(data, "image/gif", "anim.gif", "FEED")
        assert mime == "image/jpeg"
        assert name == "anim.jpg"

    @patch("media_processor._normalize_video", return_value=(b"mp4", "video/mp4", "v.mp4"))
    def test_quicktime_dispatches_to_video(self, mock_vid):
        normalize_media(b"mov", "video/quicktime", "v.mov", "REELS")
        mock_vid.assert_called_once()


# ═══════════════════════════════════════════════════════════════
#  _targets_li — network detection
# ═══════════════════════════════════════════════════════════════

class TestTargetsLi:
    def test_li_only(self):
        assert _targets_li("LI") is True

    def test_ig_li(self):
        assert _targets_li("IG+LI") is True

    def test_fb_li(self):
        assert _targets_li("FB+LI") is True

    def test_gbp_li(self):
        assert _targets_li("GBP+LI") is True

    def test_ig_fb_li(self):
        assert _targets_li("IG+FB+LI") is True

    def test_ig_fb_gbp_li(self):
        assert _targets_li("IG+FB+GBP+LI") is True

    def test_all_includes_li(self):
        assert _targets_li("ALL") is True

    def test_ig_only_no_li(self):
        assert _targets_li("IG") is False

    def test_ig_fb_no_li(self):
        assert _targets_li("IG+FB") is False

    def test_ig_fb_gbp_no_li(self):
        assert _targets_li("IG+FB+GBP") is False

    def test_empty_string_no_li(self):
        assert _targets_li("") is False

    def test_gbp_only_no_li(self):
        assert _targets_li("GBP") is False


# ═══════════════════════════════════════════════════════════════
#  LinkedIn video size/duration limits
# ═══════════════════════════════════════════════════════════════

class TestLinkedInMediaLimits:
    def test_li_video_max_size_is_200mb(self):
        assert LI_VIDEO_MAX_SIZE == 209_715_200

    def test_li_video_max_duration_is_10min(self):
        assert LI_VIDEO_MAX_DURATION == 600
