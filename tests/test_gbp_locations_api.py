"""
test_gbp_locations_api.py — tests for the /api/gbp/locations endpoint
and the _format_storefront_address helper.
"""

from unittest.mock import patch, MagicMock

import pytest

from web_app import app, _format_storefront_address


FAKE_LOCATIONS = [
    {
        "name": "locations/AAA",
        "title": "Downtown Branch",
        "storefrontAddress": {
            "addressLines": ["123 Main St"],
            "locality": "Tel Aviv",
        },
    },
    {
        "name": "locations/BBB",
        "title": "North Branch",
        "storefrontAddress": {"locality": "Haifa"},
    },
]


@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


# ═══════════════════════════════════════════════════════════════
#  _format_storefront_address
# ═══════════════════════════════════════════════════════════════

class TestFormatStorefrontAddress:
    def test_full_address(self):
        addr = {"addressLines": ["123 Main St"], "locality": "Tel Aviv"}
        assert _format_storefront_address(addr) == "123 Main St, Tel Aviv"

    def test_locality_only(self):
        addr = {"locality": "Haifa"}
        assert _format_storefront_address(addr) == "Haifa"

    def test_address_line_only(self):
        addr = {"addressLines": ["456 Oak Ave"]}
        assert _format_storefront_address(addr) == "456 Oak Ave"

    def test_empty_dict(self):
        assert _format_storefront_address({}) == ""

    def test_none(self):
        assert _format_storefront_address(None) == ""

    def test_empty_address_lines(self):
        addr = {"addressLines": [], "locality": "Jerusalem"}
        assert _format_storefront_address(addr) == "Jerusalem"


# ═══════════════════════════════════════════════════════════════
#  /api/gbp/locations endpoint
# ═══════════════════════════════════════════════════════════════

class TestGbpLocationsApi:
    @patch("channels.google_locations.get_locations_service")
    def test_returns_locations(self, mock_get_svc, client):
        svc = MagicMock()
        svc.list_locations.return_value = FAKE_LOCATIONS
        mock_get_svc.return_value = svc

        resp = client.get("/api/gbp/locations")
        data = resp.get_json()

        assert resp.status_code == 200
        assert len(data["locations"]) == 2
        assert data["locations"][0]["name"] == "locations/AAA"
        assert data["locations"][0]["title"] == "Downtown Branch"
        assert data["locations"][0]["address"] == "123 Main St, Tel Aviv"
        svc.list_locations.assert_called_once_with(force_refresh=False)

    @patch("channels.google_locations.get_locations_service")
    def test_refresh_param_forces_cache_bypass(self, mock_get_svc, client):
        svc = MagicMock()
        svc.list_locations.return_value = FAKE_LOCATIONS
        mock_get_svc.return_value = svc

        resp = client.get("/api/gbp/locations?refresh=1")
        data = resp.get_json()

        assert resp.status_code == 200
        svc.list_locations.assert_called_once_with(force_refresh=True)

    @patch("channels.google_locations.get_locations_service")
    def test_no_refresh_param_uses_cache(self, mock_get_svc, client):
        svc = MagicMock()
        svc.list_locations.return_value = FAKE_LOCATIONS
        mock_get_svc.return_value = svc

        resp = client.get("/api/gbp/locations")
        svc.list_locations.assert_called_once_with(force_refresh=False)

    @patch("channels.google_locations.get_locations_service")
    def test_address_formatting_locality_only(self, mock_get_svc, client):
        svc = MagicMock()
        svc.list_locations.return_value = FAKE_LOCATIONS
        mock_get_svc.return_value = svc

        resp = client.get("/api/gbp/locations")
        data = resp.get_json()

        # Second location has only locality
        assert data["locations"][1]["address"] == "Haifa"

    @patch("channels.google_locations.get_locations_service", side_effect=ValueError("GBP_ACCOUNT_ID not set"))
    def test_value_error_returns_empty(self, mock_get_svc, client):
        resp = client.get("/api/gbp/locations")
        data = resp.get_json()

        assert resp.status_code == 200
        assert data["locations"] == []
        assert "error" not in data

    @patch("channels.google_locations.get_locations_service")
    def test_generic_error_returns_error_message(self, mock_get_svc, client):
        svc = MagicMock()
        svc.list_locations.side_effect = RuntimeError("API down")
        mock_get_svc.return_value = svc

        resp = client.get("/api/gbp/locations")
        data = resp.get_json()

        assert resp.status_code == 200
        assert data["locations"] == []
        assert "API down" in data["error"]
