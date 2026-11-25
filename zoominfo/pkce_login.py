"""One-time OAuth 2.0 Authorization Code (PKCE) login helper for the ZoomInfo connector.

Run this script on your own machine, outside the connector, to sign in to ZoomInfo in a
browser and obtain the refresh token the connector needs (the `refresh_token` value in
`configuration.json`). It is not called during a sync and uses only the Python standard
library.

What it does:
1. Generates a PKCE code_verifier and its S256 code_challenge.
2. Opens the ZoomInfo authorize URL in your browser and waits for the redirect on a
   temporary local web server.
3. Exchanges the authorization code (plus the code_verifier) for tokens.
4. Prints the refresh token. The access token and id_token are never printed or stored.

Prerequisite: the redirect URI (default http://localhost:8765/callback) must be listed in
your ZoomInfo app's Sign-in Redirect URIs. It must be an http loopback address.

Example:
    python pkce_login.py --client-id <ID> --redirect-uri http://localhost:8765/callback
"""

# For parsing the command-line arguments.
import argparse

# For prompting for the client secret without echoing it.
import getpass

# For reading default credentials from environment variables.
import os

# For exiting with a clear error message.
import sys

# For Base64-encoding the PKCE challenge and the Basic auth header.
import base64

# For the SHA-256 hash behind the PKCE code challenge.
import hashlib

# For generating the random code verifier and state value.
import secrets

# For parsing the token response.
import json

# For the login timeout deadline.
import time

# For catching HTTP errors from the token request.
import urllib.error

# For building the authorize URL and parsing the redirect.
import urllib.parse

# For sending the token request.
import urllib.request

# For opening the authorize URL in the default browser.
import webbrowser

# For the temporary local web server that receives the redirect.
from http.server import BaseHTTPRequestHandler, HTTPServer

AUTHORIZE_URL = "https://api.zoominfo.com/gtm/oauth/v1/authorize"
TOKEN_URL = "https://api.zoominfo.com/gtm/oauth/v1/token"
DEFAULT_REDIRECT_URI = "http://localhost:8765/callback"
LOGIN_TIMEOUT_SECONDS = 300
LOOPBACK_HOSTS = ("localhost", "127.0.0.1")


def code_challenge(verifier: str) -> str:
    """
    Derive the S256 PKCE code_challenge: BASE64URL(SHA256(verifier)) without padding.
    Args:
        verifier: the high-entropy code_verifier string.
    Returns:
        The code_challenge string to send in the authorize request.
    """
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def generate_pkce_pair() -> tuple:
    """
    Generate a fresh PKCE code_verifier (86 URL-safe characters) and its code_challenge.
    Returns:
        A (code_verifier, code_challenge) tuple.
    """
    verifier = secrets.token_urlsafe(64)
    return verifier, code_challenge(verifier)


def build_authorize_url(client_id, redirect_uri, challenge, state, scopes=None) -> str:
    """
    Build the ZoomInfo authorization URL the user opens in a browser.
    Args:
        client_id: the app's client ID.
        redirect_uri: a Sign-in Redirect URI registered on the app.
        challenge: the S256 code_challenge.
        state: a random value checked again on the redirect.
        scopes: optional space-delimited scopes (a subset of the app's scopes).
    Returns:
        The full authorize URL.
    """
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "state": state,
    }
    if scopes:
        params["scope"] = scopes
    return f"{AUTHORIZE_URL}?{urllib.parse.urlencode(params)}"


def wait_for_redirect(redirect_uri: str, expected_state: str) -> str:
    """
    Listen on the loopback redirect URI until ZoomInfo redirects the browser back, then
    return the authorization code after checking the state value.
    Args:
        redirect_uri: the http loopback redirect URI to listen on.
        expected_state: the state value sent in the authorize request.
    Returns:
        The single-use authorization code.
    Raises:
        SystemExit: on timeout, an error redirect, or a state mismatch.
    """
    parsed = urllib.parse.urlparse(redirect_uri)
    result = {}

    class Handler(BaseHTTPRequestHandler):
        """Capture the OAuth redirect query string and show a short confirmation page."""

        def do_GET(self):
            """Handle the browser redirect from ZoomInfo."""
            request = urllib.parse.urlparse(self.path)
            if request.path != parsed.path:
                self.send_response(404)
                self.end_headers()
                return
            result.update(urllib.parse.parse_qs(request.query))
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"ZoomInfo login received. You can close this tab.")

        def log_message(self, *args):
            """Silence request logging so codes are never written to the terminal."""

    server = HTTPServer((parsed.hostname, parsed.port or 80), Handler)
    server.timeout = 1
    deadline = time.time() + LOGIN_TIMEOUT_SECONDS
    try:
        while not result and time.time() < deadline:
            server.handle_request()
    finally:
        server.server_close()

    if not result:
        sys.exit(f"Timed out after {LOGIN_TIMEOUT_SECONDS}s waiting for the browser redirect.")
    if "error" in result:
        description = result.get("error_description", [""])[0]
        sys.exit(f"Authorization failed: {result['error'][0]} {description}".strip())
    returned_state = result.get("state", [""])[0]
    if not secrets.compare_digest(returned_state, expected_state):
        sys.exit("State mismatch on redirect; aborting (possible forged callback).")
    code = result.get("code", [""])[0]
    if not code:
        sys.exit("The redirect did not include an authorization code.")
    return code


def exchange_code(client_id, client_secret, code, verifier, redirect_uri) -> dict:
    """
    Exchange the authorization code and PKCE code_verifier for tokens.
    Args:
        client_id: the app's client ID.
        client_secret: the app's client secret (sent as an HTTP Basic header).
        code: the authorization code from the redirect.
        verifier: the original PKCE code_verifier.
        redirect_uri: the same redirect URI used in the authorize request.
    Returns:
        The parsed token response.
    Raises:
        SystemExit: if ZoomInfo rejects the exchange.
    """
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    body = urllib.parse.urlencode(
        {
            "grant_type": "authorization_code",
            "code": code,
            "code_verifier": verifier,
            "redirect_uri": redirect_uri,
        }
    ).encode()
    request = urllib.request.Request(
        TOKEN_URL,
        data=body,
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as error:
        sys.exit(f"Token exchange failed (HTTP {error.code}): {error.read().decode()[:300]}")


def main():
    """Run the login: open the browser, wait for the redirect, print the refresh token."""
    parser = argparse.ArgumentParser(description="Get a ZoomInfo refresh token via PKCE.")
    parser.add_argument("--client-id", default=os.environ.get("ZOOMINFO_CLIENT_ID"))
    parser.add_argument("--client-secret", default=os.environ.get("ZOOMINFO_CLIENT_SECRET"))
    parser.add_argument("--redirect-uri", default=DEFAULT_REDIRECT_URI)
    parser.add_argument("--scopes", default=None, help="Space-delimited scopes (optional).")
    args = parser.parse_args()

    client_id = args.client_id or input("ZoomInfo client_id: ").strip()
    client_secret = args.client_secret or getpass.getpass("ZoomInfo client_secret: ").strip()

    parsed = urllib.parse.urlparse(args.redirect_uri)
    if parsed.scheme != "http" or parsed.hostname not in LOOPBACK_HOSTS:
        sys.exit("--redirect-uri must be an http://localhost or http://127.0.0.1 address.")

    verifier, challenge = generate_pkce_pair()
    state = secrets.token_urlsafe(24)
    url = build_authorize_url(client_id, args.redirect_uri, challenge, state, args.scopes)

    print("Opening your browser to sign in to ZoomInfo. If it does not open, visit:\n")
    print(url + "\n")
    webbrowser.open(url)

    code = wait_for_redirect(args.redirect_uri, state)
    tokens = exchange_code(client_id, client_secret, code, verifier, args.redirect_uri)
    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        sys.exit("ZoomInfo did not return a refresh_token. Check the app and requested scopes.")

    print("Success. Set this value as `refresh_token` in configuration.json:\n")
    print(refresh_token + "\n")
    print("Treat it like a password. It is single-use: the connector's first sync exchanges it")
    print("for a new one and keeps the new one in connector state, so do not use it elsewhere.")


if __name__ == "__main__":
    main()
