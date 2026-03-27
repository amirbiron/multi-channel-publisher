"""
test_meta_publish.py — בדיקות יחידה ל-meta_publish.py

מכסה: IG container creation (image vs video), IG wait loop,
       FB photo/video/reel publishing, post_type routing, error handling.
"""

from unittest.mock import patch, MagicMock, call
import pytest

from meta_publish import (
    ig_publish_feed,
    fb_publish_feed,
    ig_publish_carousel,
    fb_publish_carousel,
    _ig_create_container,
    _ig_create_carousel_item,
    _ig_create_carousel_container,
    _ig_publish_container,
    _ig_wait_for_container_ready,
    _fb_publish_reel,
    _fb_upload_unpublished_photo,
)


def _mock_response(json_data, status_code=200, ok=True):
    resp = MagicMock()
    resp.ok = ok
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.text = str(json_data)
    return resp


# ═══════════════════════════════════════════════════════════════
#  _ig_create_container
# ═══════════════════════════════════════════════════════════════

class TestIgCreateContainer:
    @patch("meta_publish.requests.post")
    def test_image_sends_image_url(self, mock_post):
        mock_post.return_value = _mock_response({"id": "container_1"})

        result = _ig_create_container("https://example.com/img.jpg", "caption", is_video=False)

        assert result == "container_1"
        call_data = mock_post.call_args[1]["data"]
        assert call_data["image_url"] == "https://example.com/img.jpg"
        assert "video_url" not in call_data
        assert "media_type" not in call_data

    @patch("meta_publish.requests.post")
    def test_video_sends_video_url_and_reels(self, mock_post):
        mock_post.return_value = _mock_response({"id": "container_2"})

        result = _ig_create_container("https://example.com/vid.mp4", "caption", is_video=True)

        assert result == "container_2"
        call_data = mock_post.call_args[1]["data"]
        assert call_data["video_url"] == "https://example.com/vid.mp4"
        assert call_data["media_type"] == "REELS"
        assert "image_url" not in call_data

    @patch("meta_publish.requests.post")
    def test_api_error_raises(self, mock_post):
        resp = _mock_response({"error": {"message": "bad token"}}, status_code=400, ok=False)
        resp.raise_for_status.side_effect = Exception("400 Bad Request")
        mock_post.return_value = resp

        with pytest.raises(Exception, match="400"):
            _ig_create_container("https://example.com/img.jpg", "cap", is_video=False)

    @patch("meta_publish.requests.post")
    def test_caption_with_special_chars(self, mock_post):
        """Captions with emoji, newlines, Hebrew should pass through."""
        mock_post.return_value = _mock_response({"id": "container_3"})
        caption = "שלום עולם! 🎉\nLine 2 & <special>"

        _ig_create_container("https://example.com/img.jpg", caption, is_video=False)

        call_data = mock_post.call_args[1]["data"]
        assert call_data["caption"] == caption

    @patch("meta_publish.requests.post")
    def test_empty_caption(self, mock_post):
        mock_post.return_value = _mock_response({"id": "container_4"})

        _ig_create_container("https://example.com/img.jpg", "", is_video=False)

        call_data = mock_post.call_args[1]["data"]
        assert call_data["caption"] == ""


# ═══════════════════════════════════════════════════════════════
#  _ig_wait_for_container_ready
# ═══════════════════════════════════════════════════════════════

class TestIgWaitForContainer:
    @patch("meta_publish.time.sleep")
    @patch("meta_publish.requests.get")
    def test_finished_immediately(self, mock_get, mock_sleep):
        mock_get.return_value = _mock_response({"status_code": "FINISHED"})

        _ig_wait_for_container_ready("container_1")

        mock_sleep.assert_not_called()

    @patch("meta_publish.time.sleep")
    @patch("meta_publish.requests.get")
    def test_finished_after_retries(self, mock_get, mock_sleep):
        mock_get.side_effect = [
            _mock_response({"status_code": "IN_PROGRESS"}),
            _mock_response({"status_code": "IN_PROGRESS"}),
            _mock_response({"status_code": "FINISHED"}),
        ]

        _ig_wait_for_container_ready("container_1", interval=1)

        assert mock_sleep.call_count == 2

    @patch("meta_publish.time.sleep")
    @patch("meta_publish.requests.get")
    def test_error_status_raises(self, mock_get, mock_sleep):
        mock_get.return_value = _mock_response({
            "status_code": "ERROR",
            "status": "Media upload failed",
        })

        with pytest.raises(RuntimeError, match="Media upload failed"):
            _ig_wait_for_container_ready("container_1")

    @patch("meta_publish.time.sleep")
    @patch("meta_publish.requests.get")
    def test_timeout_raises(self, mock_get, mock_sleep):
        mock_get.return_value = _mock_response({"status_code": "IN_PROGRESS"})

        with pytest.raises(TimeoutError):
            _ig_wait_for_container_ready("container_1", max_wait=3, interval=1)


# ═══════════════════════════════════════════════════════════════
#  ig_publish_feed (full flow with post_type)
# ═══════════════════════════════════════════════════════════════

class TestIgPublishFeed:
    @patch("meta_publish._ig_publish_container", return_value="media_final")
    @patch("meta_publish._ig_wait_for_container_ready")
    @patch("meta_publish._ig_create_container", return_value="container_1")
    def test_image_feed(self, mock_create, mock_wait, mock_publish):
        result = ig_publish_feed("https://example.com/img.jpg", "cap", "image/jpeg", "FEED")

        assert result == "media_final"
        mock_create.assert_called_once_with("https://example.com/img.jpg", "cap", False)
        mock_wait.assert_called_once_with("container_1", is_video=False)

    @patch("meta_publish._ig_publish_container", return_value="media_final")
    @patch("meta_publish._ig_wait_for_container_ready")
    @patch("meta_publish._ig_create_container", return_value="container_2")
    def test_video_feed_uses_reels_anyway(self, mock_create, mock_wait, mock_publish):
        """Even with post_type=FEED, video on IG must use REELS (API limitation)."""
        result = ig_publish_feed("https://example.com/vid.mp4", "cap", "video/mp4", "FEED")

        assert result == "media_final"
        # is_video=True → use_reels=True regardless of post_type
        mock_create.assert_called_once_with("https://example.com/vid.mp4", "cap", True)
        mock_wait.assert_called_once_with("container_2", is_video=True)

    @patch("meta_publish._ig_publish_container", return_value="media_final")
    @patch("meta_publish._ig_wait_for_container_ready")
    @patch("meta_publish._ig_create_container", return_value="container_3")
    def test_video_reels(self, mock_create, mock_wait, mock_publish):
        result = ig_publish_feed("https://example.com/vid.mp4", "cap", "video/mp4", "REELS")

        assert result == "media_final"
        mock_create.assert_called_once_with("https://example.com/vid.mp4", "cap", True)

    @patch("meta_publish._ig_publish_container", return_value="media_final")
    @patch("meta_publish._ig_wait_for_container_ready")
    @patch("meta_publish._ig_create_container", return_value="container_4")
    def test_image_reels_still_sends_as_image(self, mock_create, mock_wait, mock_publish):
        """post_type=REELS with image → IG Reels don't support images, sends as regular image."""
        result = ig_publish_feed("https://example.com/img.jpg", "cap", "image/jpeg", "REELS")

        assert result == "media_final"
        # is_video=False because mime is image, regardless of post_type
        mock_create.assert_called_once_with("https://example.com/img.jpg", "cap", False)
        mock_wait.assert_called_once_with("container_4", is_video=False)

    @patch("meta_publish._ig_publish_container", return_value="media_final")
    @patch("meta_publish._ig_wait_for_container_ready")
    @patch("meta_publish._ig_create_container", return_value="container_5")
    def test_default_post_type_is_feed(self, mock_create, mock_wait, mock_publish):
        """If post_type is not provided, defaults to FEED."""
        ig_publish_feed("https://example.com/img.jpg", "cap", "image/jpeg")

        mock_create.assert_called_once_with("https://example.com/img.jpg", "cap", False)


# ═══════════════════════════════════════════════════════════════
#  Facebook — post_type routing
# ═══════════════════════════════════════════════════════════════

class TestFbPublishFeed:
    @patch("meta_publish.requests.post")
    def test_photo_feed(self, mock_post):
        mock_post.return_value = _mock_response({"post_id": "fb_post_1"})

        result = fb_publish_feed("https://example.com/img.jpg", "hello FB", "image/jpeg", "FEED")

        assert result == "fb_post_1"
        call_data = mock_post.call_args[1]["data"]
        assert call_data["url"] == "https://example.com/img.jpg"
        assert call_data["caption"] == "hello FB"

    @patch("meta_publish.requests.post")
    def test_video_feed(self, mock_post):
        mock_post.return_value = _mock_response({"id": "fb_vid_1"})

        result = fb_publish_feed("https://example.com/vid.mp4", "video desc", "video/mp4", "FEED")

        assert result == "fb_vid_1"
        call_data = mock_post.call_args[1]["data"]
        assert call_data["file_url"] == "https://example.com/vid.mp4"
        assert call_data["description"] == "video desc"
        assert call_data["published"] == "true"

    @patch("meta_publish.requests.post")
    def test_video_reels_uses_3_phase_upload(self, mock_post):
        """post_type=REELS + video should use 3-phase upload to /{page_id}/video_reels."""
        mock_post.side_effect = [
            # Phase 1: start
            _mock_response({"video_id": "vid_123", "upload_url": "https://rupload.facebook.com/video-upload/v21.0/vid_123"}),
            # Phase 2: transfer
            _mock_response({"success": True}),
            # Phase 3: finish
            _mock_response({"success": True}),
        ]

        result = fb_publish_feed("https://example.com/vid.mp4", "reel desc", "video/mp4", "REELS")

        assert result == "vid_123"
        assert mock_post.call_count == 3

        # Phase 1: start
        start_call = mock_post.call_args_list[0]
        assert "/video_reels" in start_call[0][0]
        assert start_call[1]["data"]["upload_phase"] == "start"

        # Phase 2: transfer via file_url header
        transfer_call = mock_post.call_args_list[1]
        assert "rupload.facebook.com" in transfer_call[0][0]
        assert transfer_call[1]["headers"]["file_url"] == "https://example.com/vid.mp4"

        # Phase 3: finish
        finish_call = mock_post.call_args_list[2]
        assert "/video_reels" in finish_call[0][0]
        assert finish_call[1]["data"]["upload_phase"] == "finish"
        assert finish_call[1]["data"]["video_id"] == "vid_123"
        assert finish_call[1]["data"]["description"] == "reel desc"
        assert finish_call[1]["data"]["video_state"] == "PUBLISHED"

    @patch("meta_publish.requests.post")
    def test_photo_reels_falls_back_to_photo(self, mock_post):
        """post_type=REELS + image → can't make a reel from image, falls back to photo."""
        mock_post.return_value = _mock_response({"post_id": "fb_post_2"})

        result = fb_publish_feed("https://example.com/img.jpg", "cap", "image/jpeg", "REELS")

        assert result == "fb_post_2"
        call_data = mock_post.call_args[1]["data"]
        # Should use photo endpoint (url key, not video_url)
        assert call_data["url"] == "https://example.com/img.jpg"

    @patch("meta_publish.requests.post")
    def test_fb_photo_api_error(self, mock_post):
        resp = _mock_response({}, status_code=403, ok=False)
        resp.raise_for_status.side_effect = Exception("403 Forbidden")
        mock_post.return_value = resp

        with pytest.raises(Exception, match="403"):
            fb_publish_feed("https://example.com/img.jpg", "cap", "image/jpeg")

    @patch("meta_publish.requests.post")
    def test_mov_detected_as_video(self, mock_post):
        """video/quicktime (MOV) should go through the video path."""
        mock_post.return_value = _mock_response({"id": "fb_vid_2"})

        fb_publish_feed("https://example.com/vid.mov", "cap", "video/quicktime")

        call_data = mock_post.call_args[1]["data"]
        assert "file_url" in call_data

    @patch("meta_publish.requests.post")
    def test_default_post_type_is_feed(self, mock_post):
        """If post_type not provided, defaults to FEED (regular video, not reel)."""
        mock_post.return_value = _mock_response({"id": "fb_vid_3"})

        fb_publish_feed("https://example.com/vid.mp4", "cap", "video/mp4")

        call_data = mock_post.call_args[1]["data"]
        # Should use regular video endpoint (file_url, not video_url)
        assert "file_url" in call_data


# ═══════════════════════════════════════════════════════════════
#  _fb_publish_reel (direct)
# ═══════════════════════════════════════════════════════════════

class TestFbPublishReel:
    @patch("meta_publish.requests.post")
    def test_reel_3_phase_workflow(self, mock_post):
        """Full 3-phase workflow: start → transfer → finish."""
        mock_post.side_effect = [
            _mock_response({"video_id": "vid_99", "upload_url": "https://rupload.facebook.com/video-upload/v21.0/vid_99"}),
            _mock_response({"success": True}),
            _mock_response({"success": True}),
        ]

        result = _fb_publish_reel("https://example.com/vid.mp4", "reel caption")

        assert result == "vid_99"
        assert mock_post.call_count == 3

        # Verify transfer uses rupload URL with file_url header
        transfer_call = mock_post.call_args_list[1]
        assert transfer_call[0][0] == "https://rupload.facebook.com/video-upload/v21.0/vid_99"
        assert transfer_call[1]["headers"]["file_url"] == "https://example.com/vid.mp4"

    @patch("meta_publish.requests.post")
    def test_reel_start_phase_error(self, mock_post):
        """Error in start phase should raise immediately."""
        resp = _mock_response({}, status_code=400, ok=False)
        resp.raise_for_status.side_effect = Exception("400 Bad Request")
        mock_post.return_value = resp

        with pytest.raises(Exception, match="400"):
            _fb_publish_reel("https://example.com/vid.mp4", "cap")

        # Only one call (start phase failed)
        assert mock_post.call_count == 1

    @patch("meta_publish.requests.post")
    def test_reel_transfer_phase_error(self, mock_post):
        """Error in transfer phase should raise after start succeeds."""
        transfer_err = _mock_response({}, status_code=500, ok=False)
        transfer_err.raise_for_status.side_effect = Exception("500 Server Error")
        mock_post.side_effect = [
            _mock_response({"video_id": "vid_100", "upload_url": "https://rupload.facebook.com/video-upload/v21.0/vid_100"}),
            transfer_err,
        ]

        with pytest.raises(Exception, match="500"):
            _fb_publish_reel("https://example.com/vid.mp4", "cap")

        assert mock_post.call_count == 2

    @patch("meta_publish.requests.post")
    def test_reel_finish_phase_error(self, mock_post):
        """Error in finish phase should raise after transfer succeeds."""
        finish_err = _mock_response({}, status_code=403, ok=False)
        finish_err.raise_for_status.side_effect = Exception("403 Forbidden")
        mock_post.side_effect = [
            _mock_response({"video_id": "vid_101", "upload_url": "https://rupload.facebook.com/video-upload/v21.0/vid_101"}),
            _mock_response({"success": True}),
            finish_err,
        ]

        with pytest.raises(Exception, match="403"):
            _fb_publish_reel("https://example.com/vid.mp4", "cap")

        assert mock_post.call_count == 3


# ═══════════════════════════════════════════════════════════════
#  Instagram Carousel
# ═══════════════════════════════════════════════════════════════

class TestIgCarousel:
    @patch("meta_publish._ig_publish_container", return_value="published_id")
    @patch("meta_publish._ig_wait_for_container_ready")
    @patch("meta_publish._ig_create_carousel_container", return_value="carousel_c")
    @patch("meta_publish._ig_create_carousel_item", side_effect=["item_1", "item_2", "item_3"])
    def test_carousel_full_flow(self, mock_item, mock_carousel, mock_wait, mock_publish):
        urls = ["https://a.com/1.jpg", "https://a.com/2.jpg", "https://a.com/3.jpg"]
        mimes = ["image/jpeg", "image/jpeg", "image/jpeg"]

        result = ig_publish_carousel(urls, "carousel caption", mimes)

        assert result == "published_id"
        assert mock_item.call_count == 3
        mock_carousel.assert_called_once_with(["item_1", "item_2", "item_3"], "carousel caption")
        # Wait called for 3 items + 1 carousel container = 4 times
        assert mock_wait.call_count == 4
        mock_publish.assert_called_once_with("carousel_c")

    def test_carousel_too_few_items(self):
        with pytest.raises(ValueError, match="at least 2"):
            ig_publish_carousel(["https://a.com/1.jpg"], "cap", ["image/jpeg"])

    def test_carousel_too_many_items(self):
        urls = [f"https://a.com/{i}.jpg" for i in range(11)]
        mimes = ["image/jpeg"] * 11
        with pytest.raises(ValueError, match="at most 10"):
            ig_publish_carousel(urls, "cap", mimes)

    @patch("meta_publish.requests.post")
    def test_create_carousel_item_image(self, mock_post):
        mock_post.return_value = _mock_response({"id": "item_x"})
        result = _ig_create_carousel_item("https://a.com/img.jpg", is_video=False)
        assert result == "item_x"
        data = mock_post.call_args[1]["data"]
        assert data["is_carousel_item"] == "true"
        assert data["image_url"] == "https://a.com/img.jpg"
        assert "video_url" not in data

    @patch("meta_publish.requests.post")
    def test_create_carousel_item_video(self, mock_post):
        mock_post.return_value = _mock_response({"id": "item_v"})
        result = _ig_create_carousel_item("https://a.com/vid.mp4", is_video=True)
        assert result == "item_v"
        data = mock_post.call_args[1]["data"]
        assert data["is_carousel_item"] == "true"
        assert data["video_url"] == "https://a.com/vid.mp4"
        assert data["media_type"] == "VIDEO"

    @patch("meta_publish.requests.post")
    def test_create_carousel_container(self, mock_post):
        mock_post.return_value = _mock_response({"id": "car_123"})
        result = _ig_create_carousel_container(["a", "b", "c"], "my caption")
        assert result == "car_123"
        data = mock_post.call_args[1]["data"]
        assert data["media_type"] == "CAROUSEL"
        assert data["children"] == "a,b,c"
        assert data["caption"] == "my caption"


# ═══════════════════════════════════════════════════════════════
#  Facebook Carousel (multi-photo)
# ═══════════════════════════════════════════════════════════════

class TestFbCarousel:
    @patch("meta_publish.requests.post")
    def test_carousel_full_flow(self, mock_post):
        """Upload 2 unpublished photos then create post with attached_media."""
        mock_post.side_effect = [
            _mock_response({"id": "photo_1"}),   # unpublished photo 1
            _mock_response({"id": "photo_2"}),   # unpublished photo 2
            _mock_response({"id": "post_999"}),  # feed post
        ]

        result = fb_publish_carousel(
            ["https://a.com/1.jpg", "https://a.com/2.jpg"],
            "multi caption",
            ["image/jpeg", "image/jpeg"],
        )

        assert result == "post_999"
        assert mock_post.call_count == 3

        # Verify unpublished photos
        first_call = mock_post.call_args_list[0][1]["data"]
        assert first_call["published"] == "false"

        # Verify feed post has attached_media
        feed_call = mock_post.call_args_list[2][1]["data"]
        assert feed_call["message"] == "multi caption"
        assert "attached_media[0]" in feed_call
        assert "attached_media[1]" in feed_call

    def test_carousel_too_few_items(self):
        with pytest.raises(ValueError, match="at least 2"):
            fb_publish_carousel(["https://a.com/1.jpg"], "cap", ["image/jpeg"])

    @patch("meta_publish.requests.post")
    def test_upload_unpublished_photo(self, mock_post):
        mock_post.return_value = _mock_response({"id": "up_photo"})
        result = _fb_upload_unpublished_photo("https://a.com/img.jpg")
        assert result == "up_photo"
        data = mock_post.call_args[1]["data"]
        assert data["published"] == "false"

    @patch("meta_publish.requests.post")
    def test_mixed_media_carousel(self, mock_post):
        """Carousel with images and video."""
        mock_post.side_effect = [
            _mock_response({"id": "photo_1"}),   # unpublished photo
            _mock_response({"id": "video_1"}),   # unpublished video
            _mock_response({"id": "post_mixed"}),  # feed post
        ]

        result = fb_publish_carousel(
            ["https://a.com/1.jpg", "https://a.com/2.mp4"],
            "mixed caption",
            ["image/jpeg", "video/mp4"],
        )

        assert result == "post_mixed"
        assert mock_post.call_count == 3
