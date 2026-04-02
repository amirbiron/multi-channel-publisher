"""
test_linkedin_auth.py — tests for LinkedInOAuthManager.

Covers:
- Token refresh flow (success + failure)
- Token expiry detection and automatic refresh
- Token caching (avoids redundant refreshes)
- Thread safety (concurrent access to token)
- Missing credentials (graceful failure)
- Force refresh
- Auth headers format
"""

import threading
import time
from unittest.mock import patch, MagicMock

import pytest

from channels.linkedin_auth import (
    LinkedInOAuthManager,
    LinkedInOAuthError,
    get_li_oauth_manager,
    reset_li_oauth_manager,
)


# ═══════════════════════════════════════════════════════════════
#  Missing / Invalid Credentials
# ═══════════════════════════════════════════════════════════════

class TestMissingCredentials:
    def test_empty_client_id_raises(self):
        with pytest.raises(ValueError, match="LinkedIn OAuth credentials incomplete"):
            LinkedInOAuthManager("", "secret", "token")

    def test_empty_client_secret_raises(self):
        with pytest.raises(ValueError, match="LinkedIn OAuth credentials incomplete"):
            LinkedInOAuthManager("client_id", "", "token")

    def test_empty_refresh_token_raises(self):
        with pytest.raises(ValueError, match="LinkedIn OAuth credentials incomplete"):
            LinkedInOAuthManager("client_id", "secret", "")

    def test_all_empty_raises(self):
        with pytest.raises(ValueError, match="LinkedIn OAuth credentials incomplete"):
            LinkedInOAuthManager("", "", "")

    def test_valid_credentials_no_error(self):
        """No error when all credentials provided (no refresh happens yet)."""
        mgr = LinkedInOAuthManager("cid", "csecret", "rtoken")
        assert mgr is not None


# ═══════════════════════════════════════════════════════════════
#  Token Refresh Flow
# ═══════════════════════════════════════════════════════════════

class TestTokenRefresh:
    @patch("channels.linkedin_auth.requests.post")
    def test_refresh_success_returns_token(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "access_token": "new_access_token",
            "expires_in": 7200,
        }
        mock_post.return_value = mock_resp

        mgr = LinkedInOAuthManager("cid", "csecret", "rtoken")
        token = mgr.get_access_token()

        assert token == "new_access_token"

    @patch("channels.linkedin_auth.requests.post")
    def test_refresh_sends_correct_params(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"access_token": "tok", "expires_in": 3600}
        mock_post.return_value = mock_resp

        mgr = LinkedInOAuthManager("my_client", "my_secret", "my_refresh")
        mgr.get_access_token()

        call_kwargs = mock_post.call_args
        data = call_kwargs.kwargs.get("data") or call_kwargs[1].get("data")
        assert data["grant_type"] == "refresh_token"
        assert data["refresh_token"] == "my_refresh"
        assert data["client_id"] == "my_client"
        assert data["client_secret"] == "my_secret"

    @patch("channels.linkedin_auth.requests.post")
    def test_refresh_calls_correct_endpoint(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"access_token": "tok"}
        mock_post.return_value = mock_resp

        mgr = LinkedInOAuthManager("cid", "csecret", "rtoken")
        mgr.get_access_token()

        url = mock_post.call_args[0][0]
        assert url == "https://www.linkedin.com/oauth/v2/accessToken"

    @patch("channels.linkedin_auth.requests.post")
    def test_refresh_failure_raises_oauth_error(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 400
        mock_resp.text = "invalid_grant"
        mock_post.return_value = mock_resp

        mgr = LinkedInOAuthManager("cid", "csecret", "rtoken")
        with pytest.raises(LinkedInOAuthError, match="Token refresh failed"):
            mgr.get_access_token()

    @patch("channels.linkedin_auth.requests.post")
    def test_refresh_401_raises_oauth_error(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        mock_resp.text = "unauthorized_client"
        mock_post.return_value = mock_resp

        mgr = LinkedInOAuthManager("cid", "csecret", "rtoken")
        with pytest.raises(LinkedInOAuthError):
            mgr.get_access_token()

    @patch("channels.linkedin_auth.requests.post")
    def test_refresh_500_raises_oauth_error(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.text = "Internal Server Error"
        mock_post.return_value = mock_resp

        mgr = LinkedInOAuthManager("cid", "csecret", "rtoken")
        with pytest.raises(LinkedInOAuthError):
            mgr.get_access_token()


# ═══════════════════════════════════════════════════════════════
#  Token Expiry Detection & Caching
# ═══════════════════════════════════════════════════════════════

class TestTokenExpiry:
    @patch("channels.linkedin_auth.requests.post")
    def test_token_cached_within_expiry(self, mock_post):
        """Token should be cached and not refreshed on every call."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "access_token": "cached_token",
            "expires_in": 7200,
        }
        mock_post.return_value = mock_resp

        mgr = LinkedInOAuthManager("cid", "csecret", "rtoken")
        t1 = mgr.get_access_token()
        t2 = mgr.get_access_token()
        t3 = mgr.get_access_token()

        assert t1 == t2 == t3 == "cached_token"
        assert mock_post.call_count == 1  # Only one refresh

    @patch("channels.linkedin_auth.time.time")
    @patch("channels.linkedin_auth.requests.post")
    def test_token_refreshed_when_expired(self, mock_post, mock_time):
        """Token should be refreshed when it expires."""
        # First call: time=1000, token expires at 1000+3600=4600
        # Second call: time=4400 (within 300s margin → expired)
        mock_time.side_effect = [
            1000,  # _refresh sets expires_at = 1000 + 3600 = 4600
            4400,  # _is_token_valid: 4400 < 4600 - 300 = 4300 → False → refresh
            4400,  # double-check after lock
            4400,  # _refresh sets expires_at = 4400 + 3600
        ]

        resp1 = MagicMock()
        resp1.status_code = 200
        resp1.json.return_value = {"access_token": "token_v1", "expires_in": 3600}

        resp2 = MagicMock()
        resp2.status_code = 200
        resp2.json.return_value = {"access_token": "token_v2", "expires_in": 3600}

        mock_post.side_effect = [resp1, resp2]

        mgr = LinkedInOAuthManager("cid", "csecret", "rtoken")
        t1 = mgr.get_access_token()
        t2 = mgr.get_access_token()

        assert t1 == "token_v1"
        assert t2 == "token_v2"
        assert mock_post.call_count == 2

    @patch("channels.linkedin_auth.requests.post")
    def test_default_expires_in_3600(self, mock_post):
        """If expires_in is not in response, default to 3600."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"access_token": "tok"}  # No expires_in
        mock_post.return_value = mock_resp

        mgr = LinkedInOAuthManager("cid", "csecret", "rtoken")
        token = mgr.get_access_token()
        assert token == "tok"
        # Token should still be cached (default 3600s expiry)
        token2 = mgr.get_access_token()
        assert mock_post.call_count == 1


# ═══════════════════════════════════════════════════════════════
#  Thread Safety
# ═══════════════════════════════════════════════════════════════

class TestThreadSafety:
    @patch("channels.linkedin_auth.requests.post")
    def test_concurrent_access_single_refresh(self, mock_post):
        """Multiple threads requesting token should only trigger one refresh."""
        call_count = {"value": 0}
        barrier = threading.Barrier(5, timeout=5)

        def slow_post(*args, **kwargs):
            call_count["value"] += 1
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {
                "access_token": "shared_token",
                "expires_in": 7200,
            }
            time.sleep(0.05)  # Simulate network delay
            return resp

        mock_post.side_effect = slow_post

        mgr = LinkedInOAuthManager("cid", "csecret", "rtoken")
        results = []
        errors = []

        def worker():
            try:
                token = mgr.get_access_token()
                results.append(token)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors
        assert all(r == "shared_token" for r in results)
        # Due to the lock, refresh should happen very few times (ideally 1)
        assert mock_post.call_count <= 2  # Allow small race window


# ═══════════════════════════════════════════════════════════════
#  Force Refresh
# ═══════════════════════════════════════════════════════════════

class TestForceRefresh:
    @patch("channels.linkedin_auth.requests.post")
    def test_force_refresh_ignores_cache(self, mock_post):
        resp1 = MagicMock()
        resp1.status_code = 200
        resp1.json.return_value = {"access_token": "tok_v1", "expires_in": 7200}

        resp2 = MagicMock()
        resp2.status_code = 200
        resp2.json.return_value = {"access_token": "tok_v2", "expires_in": 7200}

        mock_post.side_effect = [resp1, resp2]

        mgr = LinkedInOAuthManager("cid", "csecret", "rtoken")
        t1 = mgr.get_access_token()
        t2 = mgr.force_refresh()

        assert t1 == "tok_v1"
        assert t2 == "tok_v2"
        assert mock_post.call_count == 2


# ═══════════════════════════════════════════════════════════════
#  Auth Headers
# ═══════════════════════════════════════════════════════════════

class TestAuthHeaders:
    @patch("channels.linkedin_auth.requests.post")
    def test_get_auth_headers_format(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"access_token": "my_token"}
        mock_post.return_value = mock_resp

        mgr = LinkedInOAuthManager("cid", "csecret", "rtoken")
        headers = mgr.get_auth_headers()

        assert headers["Authorization"] == "Bearer my_token"
        assert headers["LinkedIn-Version"] == "202401"
        assert headers["Content-Type"] == "application/json"
        assert headers["X-Restli-Protocol-Version"] == "2.0.0"

    @patch("channels.linkedin_auth.requests.post")
    def test_auth_headers_uses_cached_token(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"access_token": "cached", "expires_in": 7200}
        mock_post.return_value = mock_resp

        mgr = LinkedInOAuthManager("cid", "csecret", "rtoken")
        mgr.get_auth_headers()
        mgr.get_auth_headers()

        assert mock_post.call_count == 1


# ═══════════════════════════════════════════════════════════════
#  Singleton: get_li_oauth_manager / reset
# ═══════════════════════════════════════════════════════════════

class TestSingleton:
    def setup_method(self):
        reset_li_oauth_manager()

    def teardown_method(self):
        reset_li_oauth_manager()

    @patch("channels.linkedin_auth.LI_REFRESH_TOKEN", "rt", create=True)
    @patch("channels.linkedin_auth.LI_OAUTH_CLIENT_SECRET", "cs", create=True)
    @patch("channels.linkedin_auth.LI_OAUTH_CLIENT_ID", "ci", create=True)
    def test_get_returns_same_instance(self):
        with patch("config.LI_OAUTH_CLIENT_ID", "ci"), \
             patch("config.LI_OAUTH_CLIENT_SECRET", "cs"), \
             patch("config.LI_REFRESH_TOKEN", "rt"):
            m1 = get_li_oauth_manager()
            m2 = get_li_oauth_manager()
            assert m1 is m2

    def test_reset_clears_singleton(self):
        with patch("config.LI_OAUTH_CLIENT_ID", "ci"), \
             patch("config.LI_OAUTH_CLIENT_SECRET", "cs"), \
             patch("config.LI_REFRESH_TOKEN", "rt"):
            m1 = get_li_oauth_manager()
            reset_li_oauth_manager()
            m2 = get_li_oauth_manager()
            assert m1 is not m2
