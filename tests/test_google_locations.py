"""
test_google_locations.py — tests for channels/google_locations.py

Covers:
- LocationsCache TTL behaviour
- GoogleLocationsService: list, get, validate
- Pagination support
- Cache hit / miss / refresh
- Error handling for inaccessible locations
- Singleton helpers
"""

import time
from unittest.mock import patch, MagicMock, PropertyMock

import pytest

from channels.google_auth import GoogleOAuthManager
from channels.google_locations import (
    LocationsCache,
    GoogleLocationsService,
    LocationAccessError,
    LocationFetchError,
    get_locations_service,
    reset_locations_service,
)


# ═══════════════════════════════════════════════════════════════
#  Fixtures
# ═══════════════════════════════════════════════════════════════

FAKE_LOCATIONS = [
    {
        "name": "accounts/111/locations/AAA",
        "locationName": "Downtown Branch",
        "address": {"locality": "Tel Aviv"},
    },
    {
        "name": "accounts/111/locations/BBB",
        "locationName": "North Branch",
        "address": {"locality": "Haifa"},
    },
]


def _make_auth_mock() -> MagicMock:
    """Return a mock GoogleOAuthManager."""
    auth = MagicMock(spec=GoogleOAuthManager)
    auth.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
    return auth


def _mock_locations_response(locations=None, next_page_token=None, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    body = {"locations": locations or []}
    if next_page_token:
        body["nextPageToken"] = next_page_token
    resp.json.return_value = body
    resp.text = str(body)
    return resp


# ═══════════════════════════════════════════════════════════════
#  LocationsCache
# ═══════════════════════════════════════════════════════════════

class TestLocationsCache:
    def test_empty_cache_returns_none(self):
        cache = LocationsCache(ttl=60)
        assert cache.get() is None
        assert cache.is_valid is False

    def test_set_and_get(self):
        cache = LocationsCache(ttl=60)
        cache.set(FAKE_LOCATIONS)
        assert cache.get() == FAKE_LOCATIONS
        assert cache.is_valid is True

    def test_expired_cache_returns_none(self):
        cache = LocationsCache(ttl=1)
        cache.set(FAKE_LOCATIONS)
        # Simulate expiry
        cache._fetched_at = time.time() - 2
        assert cache.get() is None
        assert cache.is_valid is False

    def test_invalidate(self):
        cache = LocationsCache(ttl=60)
        cache.set(FAKE_LOCATIONS)
        cache.invalidate()
        assert cache.get() is None


# ═══════════════════════════════════════════════════════════════
#  GoogleLocationsService — construction
# ═══════════════════════════════════════════════════════════════

class TestLocationsServiceConstruction:
    def test_valid_construction(self):
        svc = GoogleLocationsService(
            account_id="accounts/111",
            auth=_make_auth_mock(),
        )
        assert svc._account_id == "accounts/111"

    def test_missing_account_id_raises(self):
        with pytest.raises(ValueError, match="GBP_ACCOUNT_ID"):
            GoogleLocationsService(account_id="", auth=_make_auth_mock())


# ═══════════════════════════════════════════════════════════════
#  GoogleLocationsService — list_locations
# ═══════════════════════════════════════════════════════════════

class TestListLocations:
    @patch("channels.google_locations.requests.get")
    def test_fetches_locations(self, mock_get):
        mock_get.return_value = _mock_locations_response(FAKE_LOCATIONS)

        svc = GoogleLocationsService("accounts/111", _make_auth_mock())
        result = svc.list_locations()

        assert len(result) == 2
        assert result[0]["name"] == "accounts/111/locations/AAA"
        mock_get.assert_called_once()

    @patch("channels.google_locations.requests.get")
    def test_caches_results(self, mock_get):
        mock_get.return_value = _mock_locations_response(FAKE_LOCATIONS)

        svc = GoogleLocationsService("accounts/111", _make_auth_mock())
        svc.list_locations()
        svc.list_locations()

        assert mock_get.call_count == 1  # second call uses cache

    @patch("channels.google_locations.requests.get")
    def test_force_refresh_bypasses_cache(self, mock_get):
        mock_get.return_value = _mock_locations_response(FAKE_LOCATIONS)

        svc = GoogleLocationsService("accounts/111", _make_auth_mock())
        svc.list_locations()
        svc.list_locations(force_refresh=True)

        assert mock_get.call_count == 2

    @patch("channels.google_locations.requests.get")
    def test_pagination(self, mock_get):
        """Handles paginated responses correctly."""
        page1 = _mock_locations_response(
            [FAKE_LOCATIONS[0]], next_page_token="page2"
        )
        page2 = _mock_locations_response([FAKE_LOCATIONS[1]])
        mock_get.side_effect = [page1, page2]

        svc = GoogleLocationsService("accounts/111", _make_auth_mock())
        result = svc.list_locations()

        assert len(result) == 2
        assert mock_get.call_count == 2

    @patch("channels.google_locations.requests.get")
    def test_api_error_raises(self, mock_get):
        mock_get.return_value = _mock_locations_response(status_code=403)

        svc = GoogleLocationsService("accounts/111", _make_auth_mock())

        with pytest.raises(LocationFetchError, match="403"):
            svc.list_locations()

    @patch("channels.google_locations.requests.get")
    def test_empty_locations(self, mock_get):
        mock_get.return_value = _mock_locations_response([])

        svc = GoogleLocationsService("accounts/111", _make_auth_mock())
        result = svc.list_locations()

        assert result == []


# ═══════════════════════════════════════════════════════════════
#  GoogleLocationsService — get_location
# ═══════════════════════════════════════════════════════════════

class TestGetLocation:
    @patch("channels.google_locations.requests.get")
    def test_get_by_full_name(self, mock_get):
        mock_get.return_value = _mock_locations_response(FAKE_LOCATIONS)

        svc = GoogleLocationsService("accounts/111", _make_auth_mock())
        loc = svc.get_location("accounts/111/locations/AAA")

        assert loc is not None
        assert loc["locationName"] == "Downtown Branch"

    @patch("channels.google_locations.requests.get")
    def test_get_by_short_name(self, mock_get):
        mock_get.return_value = _mock_locations_response(FAKE_LOCATIONS)

        svc = GoogleLocationsService("accounts/111", _make_auth_mock())
        loc = svc.get_location("locations/AAA")

        assert loc is not None
        assert loc["locationName"] == "Downtown Branch"

    @patch("channels.google_locations.requests.get")
    def test_get_nonexistent_returns_none(self, mock_get):
        mock_get.return_value = _mock_locations_response(FAKE_LOCATIONS)

        svc = GoogleLocationsService("accounts/111", _make_auth_mock())
        loc = svc.get_location("locations/NOPE")

        assert loc is None


# ═══════════════════════════════════════════════════════════════
#  GoogleLocationsService — validate_location_access
# ═══════════════════════════════════════════════════════════════

class TestValidateLocationAccess:
    @patch("channels.google_locations.requests.get")
    def test_valid_location(self, mock_get):
        mock_get.return_value = _mock_locations_response(FAKE_LOCATIONS)

        svc = GoogleLocationsService("accounts/111", _make_auth_mock())
        loc = svc.validate_location_access("locations/BBB")

        assert loc["locationName"] == "North Branch"

    @patch("channels.google_locations.requests.get")
    def test_invalid_location_raises(self, mock_get):
        mock_get.return_value = _mock_locations_response(FAKE_LOCATIONS)

        svc = GoogleLocationsService("accounts/111", _make_auth_mock())

        with pytest.raises(LocationAccessError, match="not accessible"):
            svc.validate_location_access("locations/INVALID")

    @patch("channels.google_locations.requests.get")
    def test_invalid_location_error_lists_available(self, mock_get):
        mock_get.return_value = _mock_locations_response(FAKE_LOCATIONS)

        svc = GoogleLocationsService("accounts/111", _make_auth_mock())

        with pytest.raises(LocationAccessError, match="locations/AAA"):
            svc.validate_location_access("locations/INVALID")

    def test_empty_location_id_raises(self):
        svc = GoogleLocationsService("accounts/111", _make_auth_mock())

        with pytest.raises(LocationAccessError, match="google_location_id is required"):
            svc.validate_location_access("")

    def test_none_location_id_raises(self):
        svc = GoogleLocationsService("accounts/111", _make_auth_mock())

        with pytest.raises(LocationAccessError, match="google_location_id is required"):
            svc.validate_location_access("")


# ═══════════════════════════════════════════════════════════════
#  GoogleLocationsService — _matches helper
# ═══════════════════════════════════════════════════════════════

class TestMatches:
    def test_exact_match(self):
        assert GoogleLocationsService._matches(
            "accounts/1/locations/2", "accounts/1/locations/2"
        ) is True

    def test_short_form_match(self):
        assert GoogleLocationsService._matches(
            "accounts/1/locations/2", "locations/2"
        ) is True

    def test_no_match(self):
        assert GoogleLocationsService._matches(
            "accounts/1/locations/2", "locations/999"
        ) is False

    def test_bare_id_no_match(self):
        """A bare number should not match (must have 'locations/' prefix)."""
        assert GoogleLocationsService._matches(
            "accounts/1/locations/2", "2"
        ) is False


# ═══════════════════════════════════════════════════════════════
#  GoogleLocationsService — refresh_cache
# ═══════════════════════════════════════════════════════════════

class TestRefreshCache:
    @patch("channels.google_locations.requests.get")
    def test_refresh_cache(self, mock_get):
        mock_get.return_value = _mock_locations_response(FAKE_LOCATIONS)

        svc = GoogleLocationsService("accounts/111", _make_auth_mock())
        result = svc.refresh_cache()

        assert len(result) == 2
        mock_get.assert_called_once()


# ═══════════════════════════════════════════════════════════════
#  Singleton helpers
# ═══════════════════════════════════════════════════════════════

class TestSingleton:
    def setup_method(self):
        reset_locations_service()

    def teardown_method(self):
        reset_locations_service()

    def test_get_locations_service_returns_instance(self):
        svc = get_locations_service()
        assert isinstance(svc, GoogleLocationsService)

    def test_get_locations_service_is_singleton(self):
        svc1 = get_locations_service()
        svc2 = get_locations_service()
        assert svc1 is svc2

    def test_reset_clears_singleton(self):
        svc1 = get_locations_service()
        reset_locations_service()
        svc2 = get_locations_service()
        assert svc1 is not svc2
