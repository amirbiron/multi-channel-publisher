"""
test_notifications.py — בדיקות יחידה ל-notifications.py
"""

from unittest.mock import patch, MagicMock

from notifications import (
    send_telegram,
    notify_publish_error,
    notify_partial_success,
    notify_health_issue,
    is_telegram_configured,
    _truncate,
)


class TestIsTelegramConfigured:
    @patch("notifications.TELEGRAM_BOT_TOKEN", "tok")
    @patch("notifications.TELEGRAM_CHAT_IDS", ["123"])
    def test_configured(self):
        assert is_telegram_configured() is True

    @patch("notifications.TELEGRAM_BOT_TOKEN", "tok")
    @patch("notifications.TELEGRAM_CHAT_IDS", ["123", "456"])
    def test_configured_multiple(self):
        assert is_telegram_configured() is True

    @patch("notifications.TELEGRAM_BOT_TOKEN", "")
    @patch("notifications.TELEGRAM_CHAT_IDS", ["123"])
    def test_missing_token(self):
        assert is_telegram_configured() is False

    @patch("notifications.TELEGRAM_BOT_TOKEN", "tok")
    @patch("notifications.TELEGRAM_CHAT_IDS", [])
    def test_missing_chat_id(self):
        assert is_telegram_configured() is False


class TestSendTelegram:
    @patch("notifications.TELEGRAM_BOT_TOKEN", "")
    @patch("notifications.TELEGRAM_CHAT_IDS", [])
    def test_not_configured_returns_false(self):
        assert send_telegram("hello") is False

    @patch("notifications.TELEGRAM_BOT_TOKEN", "tok123")
    @patch("notifications.TELEGRAM_CHAT_IDS", ["999"])
    @patch("notifications.requests.post")
    def test_success(self, mock_post):
        mock_post.return_value = MagicMock(ok=True)
        assert send_telegram("test msg") is True
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        assert call_kwargs[1]["json"]["chat_id"] == "999"
        assert call_kwargs[1]["json"]["text"] == "test msg"

    @patch("notifications.TELEGRAM_BOT_TOKEN", "tok123")
    @patch("notifications.TELEGRAM_CHAT_IDS", ["111", "222"])
    @patch("notifications.requests.post")
    def test_success_multiple_ids(self, mock_post):
        mock_post.return_value = MagicMock(ok=True)
        assert send_telegram("test msg") is True
        assert mock_post.call_count == 2
        chat_ids = [c[1]["json"]["chat_id"] for c in mock_post.call_args_list]
        assert chat_ids == ["111", "222"]

    @patch("notifications.TELEGRAM_BOT_TOKEN", "tok123")
    @patch("notifications.TELEGRAM_CHAT_IDS", ["999"])
    @patch("notifications.requests.post")
    def test_api_failure_returns_false(self, mock_post):
        mock_post.return_value = MagicMock(ok=False, status_code=400, text="Bad Request")
        assert send_telegram("test") is False

    @patch("notifications.TELEGRAM_BOT_TOKEN", "tok123")
    @patch("notifications.TELEGRAM_CHAT_IDS", ["111", "222"])
    @patch("notifications.requests.post")
    def test_partial_failure_returns_true(self, mock_post):
        """אם ID אחד נכשל והשני מצליח — מחזיר True."""
        mock_post.side_effect = [
            MagicMock(ok=False, status_code=400, text="Bad"),
            MagicMock(ok=True),
        ]
        assert send_telegram("test") is True

    @patch("notifications.TELEGRAM_BOT_TOKEN", "tok123")
    @patch("notifications.TELEGRAM_CHAT_IDS", ["999"])
    @patch("notifications.requests.post", side_effect=Exception("network error"))
    def test_exception_returns_false(self, mock_post):
        assert send_telegram("test") is False


class TestNotifyFunctions:
    @patch("notifications.send_telegram")
    def test_notify_publish_error(self, mock_send):
        notify_publish_error("42", "Something broke")
        mock_send.assert_called_once()
        msg = mock_send.call_args[0][0]
        assert "#42" in msg
        assert "Something broke" in msg

    @patch("notifications.send_telegram")
    def test_notify_publish_error_escapes_html(self, mock_send):
        notify_publish_error("5", 'Error: <script>alert("xss")</script> & more')
        msg = mock_send.call_args[0][0]
        assert "<script>" not in msg
        assert "&lt;script&gt;" in msg
        assert "&amp; more" in msg

    @patch("notifications.send_telegram")
    def test_notify_partial_success(self, mock_send):
        notify_partial_success("7", "IG:123", "FB: timeout")
        mock_send.assert_called_once()
        msg = mock_send.call_args[0][0]
        assert "#7" in msg
        assert "IG:123" in msg

    @patch("notifications.send_telegram")
    def test_notify_partial_success_escapes_html(self, mock_send):
        notify_partial_success("3", "IG:<ok>", "FB: <error>")
        msg = mock_send.call_args[0][0]
        assert "&lt;ok&gt;" in msg
        assert "&lt;error&gt;" in msg

    @patch("notifications.send_telegram")
    def test_notify_health_issue(self, mock_send):
        notify_health_issue("Cloudinary", "connection refused")
        mock_send.assert_called_once()
        msg = mock_send.call_args[0][0]
        assert "Cloudinary" in msg

    @patch("notifications.send_telegram")
    def test_notify_health_issue_escapes_html(self, mock_send):
        notify_health_issue("Meta", "Token <expired> & invalid")
        msg = mock_send.call_args[0][0]
        assert "&lt;expired&gt;" in msg
        assert "&amp; invalid" in msg


class TestTruncate:
    def test_short_text(self):
        assert _truncate("hello", 100) == "hello"

    def test_long_text(self):
        result = _truncate("a" * 200, 50)
        assert len(result) == 50
        assert result.endswith("...")
