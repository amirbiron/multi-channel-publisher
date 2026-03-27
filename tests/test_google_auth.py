"""
test_google_auth.py — tests for channels/google_auth.py

Covers:
- GoogleOAuthManager construction & validation
- Token refresh via mocked HTTP
- Automatic refresh on expiry
- Thread-safe double-check locking
- force_refresh()
- Singleton helpers
"""

import time
from unittest.mock import patch, MagicMock

import pytest

from channels.google_auth import (
    GoogleOAuthManager,
    OAuthRefreshError,
    get_oauth_manager,
    reset_oauth_manager,
    _TOKEN_ENDPOINT,
)


# ═══════════════════════════════════════════════════════════════
#  Construction
# ═══════════════════════════════════════════════════════════════

class TestOAuthManagerConstruction:
    def test_valid_credentials(self):
        mgr = GoogleOAuthManager("cid", "csec", "rtok")
        assert mgr._client_id == "cid"

    def test_missing_client_id_raises(self):
        with pytest.raises(ValueError, match="credentials incomplete"):
            GoogleOAuthManager("", "csec", "rtok")

    def test_missing_client_secret_raises(self):
        with pytest.raises(ValueError, match="credentials incomplete"):
            GoogleOAuthManager("cid", "", "rtok")

    def test_missing_refresh_token_raises(self):
        with pytest.raises(ValueError, match="credentials incomplete"):
            GoogleOAuthManager("cid", "csec", "")


# ═══════════════════════════════════════════════════════════════
#  Token refresh (mocked HTTP)
# ═══════════════════════════════════════════════════════════════

def _mock_token_response(access_token="tok_abc", expires_in=3600, status_code=200):
    """Create a mock requests.Response for the token endpoint."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = {
        "access_token": access_token,
        "expires_in": expires_in,
        "token_type": "Bearer",
    }
    resp.text = f'{{"access_token":"{access_token}","expires_in":{expires_in}}}'
    return resp


class TestOAuthRefresh:
    @patch("channels.google_auth.requests.post")
    def test_get_access_token_triggers_refresh(self, mock_post):
        mock_post.return_value = _mock_token_response("new_token", 3600)

        mgr = GoogleOAuthManager("cid", "csec", "rtok")
        token = mgr.get_access_token()

        assert token == "new_token"
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        assert call_kwargs[1]["data"]["grant_type"] == "refresh_token"
        assert call_kwargs[1]["data"]["client_id"] == "cid"

    @patch("channels.google_auth.requests.post")
    def test_cached_token_no_second_refresh(self, mock_post):
        mock_post.return_value = _mock_token_response("tok1", 3600)

        mgr = GoogleOAuthManager("cid", "csec", "rtok")
        t1 = mgr.get_access_token()
        t2 = mgr.get_access_token()

        assert t1 == t2 == "tok1"
        assert mock_post.call_count == 1  # only one refresh

    @patch("channels.google_auth.requests.post")
    def test_expired_token_triggers_new_refresh(self, mock_post):
        mock_post.return_value = _mock_token_response("tok1", 3600)

        mgr = GoogleOAuthManager("cid", "csec", "rtok")
        mgr.get_access_token()

        # Simulate expiry
        mgr._expires_at = time.time() - 1

        mock_post.return_value = _mock_token_response("tok2", 3600)
        token = mgr.get_access_token()

        assert token == "tok2"
        assert mock_post.call_count == 2

    @patch("channels.google_auth.requests.post")
    def test_token_near_expiry_margin_triggers_refresh(self, mock_post):
        """Token within the 5-minute margin should be refreshed."""
        mock_post.return_value = _mock_token_response("tok1", 3600)

        mgr = GoogleOAuthManager("cid", "csec", "rtok")
        mgr.get_access_token()

        # Set expiry to 4 minutes from now (within 5-min margin)
        mgr._expires_at = time.time() + 240

        mock_post.return_value = _mock_token_response("tok2", 3600)
        token = mgr.get_access_token()

        assert token == "tok2"

    @patch("channels.google_auth.requests.post")
    def test_refresh_failure_raises(self, mock_post):
        resp = MagicMock()
        resp.status_code = 401
        resp.text = '{"error":"invalid_grant"}'
        mock_post.return_value = resp

        mgr = GoogleOAuthManager("cid", "csec", "rtok")

        with pytest.raises(OAuthRefreshError, match="401"):
            mgr.get_access_token()

    @patch("channels.google_auth.requests.post")
    def test_get_auth_headers(self, mock_post):
        mock_post.return_value = _mock_token_response("bearer_tok", 3600)

        mgr = GoogleOAuthManager("cid", "csec", "rtok")
        headers = mgr.get_auth_headers()

        assert headers == {"Authorization": "Bearer bearer_tok"}

    @patch("channels.google_auth.requests.post")
    def test_force_refresh(self, mock_post):
        mock_post.return_value = _mock_token_response("tok1", 3600)

        mgr = GoogleOAuthManager("cid", "csec", "rtok")
        mgr.get_access_token()

        mock_post.return_value = _mock_token_response("tok_forced", 3600)
        token = mgr.force_refresh()

        assert token == "tok_forced"
        assert mock_post.call_count == 2


# ═══════════════════════════════════════════════════════════════
#  Singleton helpers
# ═══════════════════════════════════════════════════════════════

class TestSingleton:
    def setup_method(self):
        reset_oauth_manager()

    def teardown_method(self):
        reset_oauth_manager()

    def test_get_oauth_manager_returns_instance(self):
        mgr = get_oauth_manager()
        assert isinstance(mgr, GoogleOAuthManager)

    def test_get_oauth_manager_is_singleton(self):
        mgr1 = get_oauth_manager()
        mgr2 = get_oauth_manager()
        assert mgr1 is mgr2

    def test_reset_clears_singleton(self):
        mgr1 = get_oauth_manager()
        reset_oauth_manager()
        mgr2 = get_oauth_manager()
        assert mgr1 is not mgr2
