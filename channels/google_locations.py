"""
google_locations.py — Google Business Profile location service.

Provides helpers to list, fetch, and validate GBP locations that
belong to the configured account.  Results are cached with a short
TTL so repeated validation calls within the same publish cycle
don't hammer the API.
"""

from __future__ import annotations

import logging
import threading
import time

import requests

from channels.google_auth import GoogleOAuthManager, get_oauth_manager

logger = logging.getLogger(__name__)

# GBP (My Business) API base
_GBP_API_BASE = "https://mybusiness.googleapis.com/v4"

# Default cache TTL in seconds (5 minutes)
_CACHE_TTL_SECONDS = 300


class LocationsCache:
    """Thread-safe, TTL-based cache for a list of GBP locations."""

    def __init__(self, ttl: int = _CACHE_TTL_SECONDS) -> None:
        self._ttl = ttl
        self._data: list[dict] | None = None
        self._fetched_at: float = 0.0
        self._lock = threading.Lock()

    @property
    def is_valid(self) -> bool:
        return self._data is not None and (time.time() - self._fetched_at) < self._ttl

    def get(self) -> list[dict] | None:
        if self.is_valid:
            return self._data
        return None

    def set(self, data: list[dict]) -> None:
        with self._lock:
            self._data = data
            self._fetched_at = time.time()

    def invalidate(self) -> None:
        with self._lock:
            self._data = None
            self._fetched_at = 0.0


class GoogleLocationsService:
    """
    Fetches and validates GBP locations for a given account.

    Usage::

        svc = GoogleLocationsService(account_id="accounts/123", auth=mgr)
        locations = svc.list_locations()
        loc = svc.get_location("locations/456")
        svc.validate_location_access("locations/456")  # raises if invalid
    """

    def __init__(
        self,
        account_id: str,
        auth: GoogleOAuthManager,
        cache_ttl: int = _CACHE_TTL_SECONDS,
    ) -> None:
        if not account_id:
            raise ValueError(
                "GBP_ACCOUNT_ID is required. "
                "Set it as an environment variable (e.g. 'accounts/123456789')."
            )
        self._account_id = account_id
        self._auth = auth
        self._cache = LocationsCache(ttl=cache_ttl)

    # ── public API ───────────────────────────────────────────

    def list_locations(self, *, force_refresh: bool = False) -> list[dict]:
        """
        Return all locations accessible under the configured account.

        Results are cached for ``cache_ttl`` seconds.  Pass
        ``force_refresh=True`` to bypass the cache.

        Each location dict contains at least::

            {
                "name": "accounts/123/locations/456",
                "locationName": "My Business — Downtown",
                "address": { ... },
                ...
            }
        """
        if not force_refresh:
            cached = self._cache.get()
            if cached is not None:
                logger.debug("Returning %d cached locations", len(cached))
                return cached

        locations = self._fetch_all_locations()
        self._cache.set(locations)
        logger.info("Fetched %d GBP locations for %s", len(locations), self._account_id)
        return locations

    def get_location(self, location_id: str) -> dict | None:
        """
        Return a single location by its resource name (e.g. ``locations/456``),
        or ``None`` if not found among accessible locations.

        The *location_id* can be either:
        - a full name: ``accounts/123/locations/456``
        - a short name: ``locations/456``
        """
        locations = self.list_locations()
        for loc in locations:
            if self._matches(loc["name"], location_id):
                return loc
        return None

    def validate_location_access(self, location_id: str) -> dict:
        """
        Verify that *location_id* is among the accessible locations.

        Returns the location dict on success.
        Raises ``LocationAccessError`` with a clear message on failure.
        """
        if not location_id:
            raise LocationAccessError(
                "google_location_id is required for GBP publishing. "
                "Please set it in the post row."
            )

        loc = self.get_location(location_id)
        if loc is None:
            available = [l["name"] for l in self.list_locations()]
            raise LocationAccessError(
                f"Location '{location_id}' is not accessible under account "
                f"'{self._account_id}'. Available locations: {available}"
            )
        return loc

    def refresh_cache(self) -> list[dict]:
        """Force-refresh the locations cache and return fresh data."""
        return self.list_locations(force_refresh=True)

    # ── internals ────────────────────────────────────────────

    def _fetch_all_locations(self) -> list[dict]:
        """Paginate through the locations.list API."""
        url = f"{_GBP_API_BASE}/{self._account_id}/locations"
        all_locations: list[dict] = []
        page_token: str | None = None

        while True:
            params: dict[str, str] = {}
            if page_token:
                params["pageToken"] = page_token

            resp = requests.get(
                url,
                headers=self._auth.get_auth_headers(),
                params=params,
                timeout=30,
            )

            if resp.status_code != 200:
                logger.error(
                    "GBP locations.list failed: %s %s",
                    resp.status_code,
                    resp.text[:500],
                )
                raise LocationFetchError(
                    f"Failed to fetch locations ({resp.status_code}): {resp.text[:300]}"
                )

            data = resp.json()
            all_locations.extend(data.get("locations", []))

            page_token = data.get("nextPageToken")
            if not page_token:
                break

        return all_locations

    @staticmethod
    def _matches(full_name: str, location_id: str) -> bool:
        """
        Check whether *location_id* matches *full_name*.

        Supports both ``accounts/X/locations/Y`` and ``locations/Y``.
        """
        if full_name == location_id:
            return True
        # Allow only the "locations/Y" short form
        if location_id.startswith("locations/") and full_name.endswith("/" + location_id):
            return True
        return False


class LocationAccessError(Exception):
    """Raised when a location_id is missing or not accessible."""


class LocationFetchError(Exception):
    """Raised when the API call to list locations fails."""


# ── module-level singleton (lazy) ────────────────────────────

_service: GoogleLocationsService | None = None


def get_locations_service() -> GoogleLocationsService:
    """
    Return the module-level GoogleLocationsService singleton.

    Lazily created on first call.
    """
    global _service
    if _service is None:
        from config import GBP_ACCOUNT_ID
        _service = GoogleLocationsService(
            account_id=GBP_ACCOUNT_ID,
            auth=get_oauth_manager(),
        )
    return _service


def reset_locations_service() -> None:
    """Reset the singleton (useful for tests)."""
    global _service
    _service = None
