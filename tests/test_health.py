"""
test_health.py — בדיקות יחידה ל-health check endpoint
"""

import json
from unittest.mock import patch, MagicMock

import pytest


@pytest.fixture(autouse=True)
def _clear_health_state():
    """Clear health check cache and cooldown before each test."""
    import web_app
    web_app._health_cache.clear()
    web_app._health_notify_cooldown.clear()
    yield


@pytest.fixture
def client():
    """Flask test client."""
    from web_app import app
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


class TestHealthEndpoint:
    @patch("web_app._check_meta_token", return_value={"status": "ok", "name": "Page"})
    @patch("web_app._check_cloudinary", return_value={"status": "ok"})
    @patch("web_app._check_google_drive", return_value={"status": "ok", "folder": "Media"})
    @patch("web_app._check_google_sheets", return_value={"status": "ok", "columns": 11})
    def test_all_healthy(self, mock_sheets, mock_drive, mock_cloud, mock_meta, client):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["status"] == "healthy"
        assert all(s["status"] == "ok" for s in data["services"].values())

    @patch("web_app.notify_health_issue")
    @patch("web_app._check_meta_token", return_value={"status": "ok", "name": "Page"})
    @patch("web_app._check_cloudinary", return_value={"status": "error", "error": "timeout"})
    @patch("web_app._check_google_drive", return_value={"status": "ok", "folder": "Media"})
    @patch("web_app._check_google_sheets", return_value={"status": "ok", "columns": 11})
    def test_one_unhealthy_returns_503(self, mock_sheets, mock_drive, mock_cloud, mock_meta, mock_notify, client):
        resp = client.get("/api/health")
        assert resp.status_code == 503
        data = json.loads(resp.data)
        assert data["status"] == "unhealthy"
        assert data["services"]["cloudinary"]["status"] == "error"
        mock_notify.assert_called_once_with("cloudinary", "timeout")

    @patch("web_app.notify_health_issue")
    @patch("web_app._check_meta_token", return_value={"status": "ok", "name": "Page"})
    @patch("web_app._check_cloudinary", return_value={"status": "error", "error": "timeout"})
    @patch("web_app._check_google_drive", return_value={"status": "ok", "folder": "Media"})
    @patch("web_app._check_google_sheets", return_value={"status": "ok", "columns": 11})
    def test_cooldown_prevents_duplicate_notifications(self, mock_sheets, mock_drive, mock_cloud, mock_meta, mock_notify, client):
        """Repeated health checks should not spam Telegram (even if cache expires)."""
        import web_app
        # First call — should notify
        client.get("/api/health")
        assert mock_notify.call_count == 1
        # Second call with expired cache — cooldown prevents duplicate notification
        web_app._health_cache.clear()
        client.get("/api/health")
        assert mock_notify.call_count == 1

    @patch("web_app.notify_health_issue")
    @patch("web_app._check_meta_token", return_value={"status": "ok", "name": "Page"})
    @patch("web_app._check_cloudinary", return_value={"status": "error", "error": "timeout"})
    @patch("web_app._check_google_drive", return_value={"status": "ok", "folder": "Media"})
    @patch("web_app._check_google_sheets", return_value={"status": "ok", "columns": 11})
    def test_cooldown_resets_after_recovery(self, mock_sheets, mock_drive, mock_cloud, mock_meta, mock_notify, client):
        """After all services recover, cooldown resets so next failure notifies immediately."""
        import web_app
        # Unhealthy — notifies
        client.get("/api/health")
        assert mock_notify.call_count == 1
        # Simulate recovery (clear cache so new checks run)
        mock_cloud.return_value = {"status": "ok"}
        web_app._health_cache.clear()
        client.get("/api/health")
        assert web_app._health_notify_cooldown == {}
        # Break again — should notify immediately (cooldown was cleared)
        mock_cloud.return_value = {"status": "error", "error": "timeout"}
        web_app._health_cache.clear()
        client.get("/api/health")
        assert mock_notify.call_count == 2

    @patch("web_app._check_meta_token", return_value={"status": "ok", "name": "Page"})
    @patch("web_app._check_cloudinary", return_value={"status": "ok"})
    @patch("web_app._check_google_drive", return_value={"status": "ok", "folder": "Media"})
    @patch("web_app._check_google_sheets", return_value={"status": "ok", "columns": 11})
    def test_cache_prevents_repeated_api_calls(self, mock_sheets, mock_drive, mock_cloud, mock_meta, client):
        """Repeated requests within TTL should return cached result without new API calls."""
        client.get("/api/health")
        assert mock_sheets.call_count == 1
        # Second request — should be cached
        client.get("/api/health")
        assert mock_sheets.call_count == 1  # NOT called again

    def test_no_auth_required(self, client):
        """Health check should not require authentication."""
        with patch("web_app._check_google_sheets", return_value={"status": "ok", "columns": 11}), \
             patch("web_app._check_google_drive", return_value={"status": "ok", "folder": "X"}), \
             patch("web_app._check_cloudinary", return_value={"status": "ok"}), \
             patch("web_app._check_meta_token", return_value={"status": "ok", "name": "P"}), \
             patch("web_app.WEB_PANEL_SECRET", "supersecret"):
            resp = client.get("/api/health")
            assert resp.status_code == 200


class TestCheckGoogleSheets:
    @patch("web_app.sheets_read_row", return_value=["id", "status"])
    def test_ok(self, mock_read):
        from web_app import _check_google_sheets
        result = _check_google_sheets()
        assert result["status"] == "ok"
        assert result["columns"] == 2
        mock_read.assert_called_once_with(1)

    @patch("web_app.sheets_read_row", return_value=[])
    def test_empty_sheet(self, mock_read):
        from web_app import _check_google_sheets
        result = _check_google_sheets()
        assert result["status"] == "error"

    @patch("web_app.sheets_read_row", side_effect=Exception("API error"))
    def test_exception(self, mock_read):
        from web_app import _check_google_sheets
        result = _check_google_sheets()
        assert result["status"] == "error"
        assert "API error" in result["error"]


class TestCheckCloudinary:
    @patch("cloudinary.api.ping", return_value={"status": "ok"})
    def test_ok(self, mock_ping):
        from web_app import _check_cloudinary
        result = _check_cloudinary()
        assert result["status"] == "ok"

    @patch("cloudinary.api.ping", side_effect=Exception("auth failed"))
    def test_exception(self, mock_ping):
        from web_app import _check_cloudinary
        result = _check_cloudinary()
        assert result["status"] == "error"


class TestCheckMetaToken:
    @patch("web_app.http_requests.get")
    def test_valid_token(self, mock_get):
        mock_get.return_value = MagicMock(ok=True, json=lambda: {"name": "My Page"})
        from web_app import _check_meta_token
        result = _check_meta_token("IG_ACCESS_TOKEN", "valid-token")
        assert result["status"] == "ok"

    @patch("web_app.http_requests.get")
    def test_expired_token(self, mock_get):
        mock_get.return_value = MagicMock(
            ok=False,
            json=lambda: {"error": {"message": "Token expired"}},
        )
        from web_app import _check_meta_token
        result = _check_meta_token("IG_ACCESS_TOKEN", "expired-token")
        assert result["status"] == "error"
        assert "expired" in result["error"].lower()

    def test_missing_token(self):
        from web_app import _check_meta_token
        result = _check_meta_token("IG_ACCESS_TOKEN", "")
        assert result["status"] == "error"
        assert "not configured" in result["error"]
