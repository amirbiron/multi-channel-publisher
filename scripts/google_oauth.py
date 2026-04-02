#!/usr/bin/env python3
"""
google_oauth.py — One-time OAuth flow to obtain a Google Refresh Token for GBP.

Usage:
    1. Set GBP_OAUTH_CLIENT_ID and GBP_OAUTH_CLIENT_SECRET in your .env
    2. Run: python scripts/google_oauth.py
    3. Open the printed URL in your browser and authorize
    4. Paste the redirect URL (or just the code) back here
    5. Copy the refresh token to your .env as GBP_REFRESH_TOKEN
"""

import os
import sys
import urllib.parse

try:
    import requests
except ImportError:
    print("Error: 'requests' package required. Install with: pip install requests")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

CLIENT_ID = os.environ.get("GBP_OAUTH_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("GBP_OAUTH_CLIENT_SECRET", "")
REDIRECT_URI = "http://localhost:8080/callback"
SCOPES = "https://www.googleapis.com/auth/business.manage"

AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
GBP_ACCOUNTS_URL = "https://mybusinessaccountmanagement.googleapis.com/v1/accounts"


def build_auth_url(client_id: str) -> str:
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPES,
        "access_type": "offline",
        "prompt": "consent",
    }
    return f"{AUTH_URL}?{urllib.parse.urlencode(params)}"


def exchange_code(code: str, client_id: str, client_secret: str) -> dict:
    resp = requests.post(TOKEN_URL, data={
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": client_id,
        "client_secret": client_secret,
    }, timeout=30)

    if resp.status_code != 200:
        print(f"Error: Token exchange failed ({resp.status_code})")
        print(resp.text)
        sys.exit(1)

    return resp.json()


def fetch_gbp_accounts(access_token: str) -> None:
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(GBP_ACCOUNTS_URL, headers=headers, timeout=15)

    if resp.status_code == 200:
        data = resp.json()
        accounts = data.get("accounts", [])
        if accounts:
            print("  GBP Accounts found:")
            for acc in accounts:
                name = acc.get("name", "")
                account_name = acc.get("accountName", "")
                print(f"    GBP_ACCOUNT_ID={name}")
                print(f"    Name: {account_name}")
        else:
            print("  No GBP accounts found.")
    else:
        print(f"  Could not fetch accounts ({resp.status_code}): {resp.text[:200]}")


def main():
    client_id = CLIENT_ID
    client_secret = CLIENT_SECRET

    if not client_id or not client_secret:
        print("Error: GBP_OAUTH_CLIENT_ID and GBP_OAUTH_CLIENT_SECRET must be set.")
        sys.exit(1)

    auth_url = build_auth_url(client_id)

    print()
    print("=" * 60)
    print("  Google Business Profile — Refresh Token Generator")
    print("=" * 60)
    print()
    print("Step 1: Open this URL in your browser:")
    print()
    print(f"  {auth_url}")
    print()
    print("Step 2: Authorize, then paste the full redirect URL here.")
    print("        (The page will show an error — that's OK!)")
    print("        Copy the URL from the browser address bar.")
    print()

    redirect_url = input("Paste redirect URL: ").strip()

    # Extract code from URL or raw code
    if redirect_url.startswith("http"):
        parsed = urllib.parse.urlparse(redirect_url)
        query = urllib.parse.parse_qs(parsed.query)
        if "code" not in query:
            print("Error: No 'code' parameter found in URL.")
            sys.exit(1)
        code = query["code"][0]
    else:
        code = redirect_url

    print()
    print("Exchanging authorization code for tokens...")

    data = exchange_code(code, client_id, client_secret)

    access_token = data.get("access_token", "")
    refresh_token = data.get("refresh_token", "")
    expires_in = data.get("expires_in", 0)

    print()
    print("=" * 60)
    print("  Success!")
    print("=" * 60)
    print()

    if refresh_token:
        print(f"  GBP_REFRESH_TOKEN={refresh_token}")
        print()
        print(f"  Access token expires in: {expires_in // 3600} hours")
        print("  Refresh token does NOT expire (auto-renews).")
    else:
        print("  Warning: No refresh token returned!")
        print("  Make sure 'access_type=offline' and 'prompt=consent' were used.")

    # Fetch GBP accounts
    print()
    print("-" * 60)
    print("  Fetching GBP accounts...")
    print("-" * 60)
    print()
    fetch_gbp_accounts(access_token)
    print()


if __name__ == "__main__":
    main()
