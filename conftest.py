"""
conftest.py — מגדיר env vars מזויפים לפני שכל מודול נטען,
כדי ש-config.py לא ייכשל על חוסר credentials.
"""

import os

# Must be set BEFORE importing config
os.environ.setdefault("GOOGLE_SERVICE_ACCOUNT_JSON", '{"type":"service_account"}')
os.environ.setdefault("SPREADSHEET_ID", "fake-spreadsheet-id")
os.environ.setdefault("IG_USER_ID", "123456")
os.environ.setdefault("IG_ACCESS_TOKEN", "fake-ig-token")
os.environ.setdefault("FB_PAGE_ID", "654321")
os.environ.setdefault("FB_PAGE_ACCESS_TOKEN", "fake-fb-token")
os.environ.setdefault("CLOUDINARY_URL", "cloudinary://key:secret@cloud")

# GBP feature flag — enable for tests
os.environ.setdefault("GBP_ENABLED", "true")

# GBP OAuth (optional — only needed when GBP channel is active)
os.environ.setdefault("GBP_ACCOUNT_ID", "accounts/fake-account-id")
os.environ.setdefault("GBP_OAUTH_CLIENT_ID", "fake-gbp-client-id")
os.environ.setdefault("GBP_OAUTH_CLIENT_SECRET", "fake-gbp-client-secret")
os.environ.setdefault("GBP_REFRESH_TOKEN", "fake-gbp-refresh-token")
