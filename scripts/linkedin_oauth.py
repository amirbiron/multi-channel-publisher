#!/usr/bin/env python3
"""
linkedin_oauth.py — One-time OAuth flow to obtain a LinkedIn Refresh Token.

Usage:
    1. Set LI_OAUTH_CLIENT_ID and LI_OAUTH_CLIENT_SECRET in your .env or environment
    2. Run: python scripts/linkedin_oauth.py
    3. Open the printed URL in your browser and authorize
    4. Paste the redirect URL back here
    5. Copy the refresh token to your .env as LI_REFRESH_TOKEN
"""

import os
import sys
import urllib.parse
import http.server
import threading

try:
    import requests
except ImportError:
    print("Error: 'requests' package required. Install with: pip install requests")
    sys.exit(1)

# Try loading .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

CLIENT_ID = os.environ.get("LI_OAUTH_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("LI_OAUTH_CLIENT_SECRET", "")
REDIRECT_URI = "http://localhost:8080/callback"
SCOPES = "w_member_social openid profile"

TOKEN_URL = "https://www.linkedin.com/oauth/v2/accessToken"
AUTH_URL = "https://www.linkedin.com/oauth/v2/authorization"


def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        print("Error: LI_OAUTH_CLIENT_ID and LI_OAUTH_CLIENT_SECRET must be set.")
        print("Set them in .env or as environment variables.")
        sys.exit(1)

    # Step 1: Build authorization URL
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPES,
    }
    auth_url = f"{AUTH_URL}?{urllib.parse.urlencode(params)}"

    print()
    print("=" * 60)
    print("  LinkedIn OAuth — Refresh Token Generator")
    print("=" * 60)
    print()
    print("Step 1: Open this URL in your browser:")
    print()
    print(f"  {auth_url}")
    print()
    print("Step 2: Authorize the app, then paste the full redirect URL here.")
    print("        (It will look like: http://localhost:8080/callback?code=AQ...)")
    print()

    # Try to start local server to catch the callback automatically
    auth_code = _try_auto_capture(auth_url)

    if not auth_code:
        redirect_url = input("Paste redirect URL: ").strip()
        parsed = urllib.parse.urlparse(redirect_url)
        query = urllib.parse.parse_qs(parsed.query)
        if "code" not in query:
            print("Error: No 'code' parameter found in URL.")
            sys.exit(1)
        auth_code = query["code"][0]

    # Step 2: Exchange code for tokens
    print()
    print("Exchanging authorization code for tokens...")

    resp = requests.post(TOKEN_URL, data={
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }, timeout=30)

    if resp.status_code != 200:
        print(f"Error: Token exchange failed ({resp.status_code})")
        print(resp.text)
        sys.exit(1)

    data = resp.json()
    access_token = data.get("access_token", "")
    refresh_token = data.get("refresh_token", "")
    expires_in = data.get("expires_in", 0)
    refresh_expires_in = data.get("refresh_token_expires_in", 0)

    print()
    print("=" * 60)
    print("  Success!")
    print("=" * 60)
    print()

    if refresh_token:
        print(f"  LI_REFRESH_TOKEN={refresh_token}")
        print()
        print(f"  Refresh token expires in: {refresh_expires_in // 86400} days")
        print(f"  Access token expires in:  {expires_in // 3600} hours")
        print()
        print("  Copy LI_REFRESH_TOKEN to your .env file or Render Dashboard.")
    else:
        print("  Warning: No refresh token returned!")
        print("  LinkedIn may not return refresh tokens for all app types.")
        print()
        print(f"  Access Token (short-lived): {access_token[:20]}...")
        print(f"  Expires in: {expires_in // 3600} hours")
        print()
        print("  You can use this access token directly as LI_REFRESH_TOKEN")
        print("  but it will expire and need manual renewal.")

    # Step 3: Fetch member URN
    print()
    print("-" * 60)
    print("  Fetching your LinkedIn member URN...")
    print("-" * 60)

    headers = {
        "Authorization": f"Bearer {access_token}",
        "LinkedIn-Version": "202401",
    }
    me_resp = requests.get(
        "https://api.linkedin.com/v2/userinfo",
        headers=headers,
        timeout=15,
    )
    if me_resp.status_code == 200:
        me_data = me_resp.json()
        sub = me_data.get("sub", "")
        name = me_data.get("name", "")
        if sub:
            print()
            print(f"  Name: {name}")
            print(f"  Author URN: urn:li:person:{sub}")
            print()
            print("  Use this URN in the li_author_urn column in your sheet.")
    else:
        print(f"  Could not fetch profile ({me_resp.status_code})")
        print("  You can find your URN manually in LinkedIn settings.")

    print()


def _try_auto_capture(auth_url: str) -> str | None:
    """Try to start a local server to auto-capture the OAuth callback."""
    captured_code = {"value": None}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urllib.parse.urlparse(self.path)
            query = urllib.parse.parse_qs(parsed.query)
            if "code" in query:
                captured_code["value"] = query["code"][0]
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(
                    b"<html><body><h2>Authorization successful!</h2>"
                    b"<p>You can close this tab and return to the terminal.</p>"
                    b"</body></html>"
                )
            else:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"No code parameter found")

        def log_message(self, format, *args):
            pass  # Suppress server logs

    try:
        server = http.server.HTTPServer(("localhost", 8080), Handler)
        thread = threading.Thread(target=server.handle_request, daemon=True)
        thread.start()
        print("  (Local server listening on localhost:8080 to auto-capture callback)")
        print()
        thread.join(timeout=120)
        server.server_close()
        return captured_code["value"]
    except OSError:
        # Port already in use — fall back to manual paste
        return None


if __name__ == "__main__":
    main()
