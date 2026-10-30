"""Integration tests — verify that requests physically flow through the proxy.

The unit tests in test_connector.py verify that session.proxies is configured
correctly. These tests go one level deeper: they start two real TCP servers in
background threads (a mock API server and a recording proxy), make a real HTTP
request through the connector, and assert the proxy actually received and
forwarded the call.

No extra pip dependencies — everything uses Python's standard library:
  http.server, socketserver, threading, urllib.request.

Architecture
------------
  [connector fetch_with_retry]
       |
       v  (session.proxies set to http://127.0.0.1:<proxy_port>)
  [RecordingProxy  :proxy_port]   <-- records every URL it sees
       |
       v  (forwards with urllib, proxy-env disabled to avoid loops)
  [MockAPIServer   :api_port]     <-- serves deterministic JSON

Run: python -m pytest tests/test_proxy_integration.py -v
"""

import json
import sys
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import connector as c  # noqa: E402

# --------------------------------------------------------------------- payloads
_MOCK_POSTS = [{"id": 1, "userId": 1, "title": "hello", "body": "world"}]
_MOCK_USERS = [
    {
        "id": 1,
        "name": "Alice",
        "username": "alice",
        "email": "alice@example.com",
        "phone": "555-0100",
        "website": "example.com",
        "address": {
            "street": "1 Main St",
            "suite": "",
            "city": "Metropolis",
            "zipcode": "10001",
            "geo": {"lat": "40.71", "lng": "-74.00"},
        },
        "company": {
            "name": "Acme Corp",
            "catchPhrase": "Quality products",
            "bs": "leverage synergies",
        },
    }
]


# --------------------------------------------------------------- mock API server
class _MockAPIHandler(BaseHTTPRequestHandler):
    """Serve deterministic JSON for /posts and /users, 404 for everything else."""

    def do_GET(self):
        if self.path == "/posts":
            body = json.dumps(_MOCK_POSTS).encode()
        elif self.path == "/users":
            body = json.dumps(_MOCK_USERS).encode()
        else:
            self.send_error(404, f"Unknown path: {self.path}")
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass  # suppress access log noise in test output


# ------------------------------------------------------------ recording proxy
class _RecordingProxyHandler(BaseHTTPRequestHandler):
    """HTTP forwarding proxy that records every full URL it proxied.

    When a requests.Session is configured with session.proxies, the library
    sends an absolute-URI GET to the proxy:

        GET http://127.0.0.1:<api_port>/posts HTTP/1.1

    self.path therefore contains the full URL, which we record and forward.
    urllib.request is used with ProxyHandler({}) to prevent the forward call
    from looping back through any ambient proxy environment variable.
    """

    # Shared across the class; reset per test by the fixture.
    proxied_urls: list = []

    def do_GET(self):
        self.__class__.proxied_urls.append(self.path)
        try:
            # Disable any ambient HTTP_PROXY env var so the forwarding request
            # goes directly to the origin, not back through this proxy.
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open(self.path, timeout=5) as upstream:
                body = upstream.read()
                content_type = upstream.headers.get("Content-Type", "application/json")
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except Exception as exc:
            self.send_error(502, f"Proxy forward failed: {exc}")

    def log_message(self, *args):
        pass


# ------------------------------------------------------------------- helpers
def _start(handler_class):
    """Start an HTTPServer on an OS-assigned free port; return it."""
    server = HTTPServer(("127.0.0.1", 0), handler_class)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


# ------------------------------------------------------------------- fixtures
@pytest.fixture(scope="module")
def mock_api_server():
    """Mock API server — started once for the whole module."""
    server = _start(_MockAPIHandler)
    yield server
    server.shutdown()


@pytest.fixture()
def recording_proxy(mock_api_server):
    """Recording proxy — fresh instance (and empty URL list) per test."""
    _RecordingProxyHandler.proxied_urls = []
    server = _start(_RecordingProxyHandler)
    yield server
    server.shutdown()


# --------------------------------------------------------- integration tests
class TestProxyIntegration:
    def test_request_flows_through_proxy_and_proxy_records_it(
        self, mock_api_server, recording_proxy
    ):
        """The proxy server receives the GET request; the origin is not contacted directly.

        This is the key integration proof: if build_session does NOT set
        session.proxies, the request skips the recording proxy entirely and the
        assertion on proxied_urls fails.
        """
        api_port = mock_api_server.server_address[1]
        proxy_port = recording_proxy.server_address[1]

        api_url = f"http://127.0.0.1:{api_port}/posts"
        proxy_url = f"http://127.0.0.1:{proxy_port}"

        session = c.build_session({"proxy_url": proxy_url})
        try:
            result = c.fetch_with_retry(session, api_url)
        finally:
            session.close()

        assert api_url in _RecordingProxyHandler.proxied_urls, (
            f"Proxy did not see the request. "
            f"Expected {api_url!r} in proxied_urls but got: "
            f"{_RecordingProxyHandler.proxied_urls}"
        )
        assert result == _MOCK_POSTS

    def test_without_proxy_url_request_bypasses_proxy(self, mock_api_server, recording_proxy):
        """No proxy_url → request goes directly to origin; proxy sees nothing.

        Counterpart to the test above: confirms the presence of the proxy in the
        first test was due to the session.proxies config, not an accident.
        """
        api_port = mock_api_server.server_address[1]
        api_url = f"http://127.0.0.1:{api_port}/posts"

        session = c.build_session({})  # no proxy_url
        try:
            result = c.fetch_with_retry(session, api_url)
        finally:
            session.close()

        assert _RecordingProxyHandler.proxied_urls == [], (
            "Expected the proxy to receive no requests when proxy_url is absent, "
            f"but it saw: {_RecordingProxyHandler.proxied_urls}"
        )
        assert result == _MOCK_POSTS

    def test_proxy_receives_both_posts_and_users_requests(self, mock_api_server, recording_proxy):
        """Every API call in a sync goes through the proxy, not just the first one."""
        api_port = mock_api_server.server_address[1]
        proxy_port = recording_proxy.server_address[1]

        posts_url = f"http://127.0.0.1:{api_port}/posts"
        users_url = f"http://127.0.0.1:{api_port}/users"

        session = c.build_session({"proxy_url": f"http://127.0.0.1:{proxy_port}"})
        try:
            c.fetch_with_retry(session, posts_url)
            c.fetch_with_retry(session, users_url)
        finally:
            session.close()

        assert posts_url in _RecordingProxyHandler.proxied_urls
        assert users_url in _RecordingProxyHandler.proxied_urls
        assert len(_RecordingProxyHandler.proxied_urls) == 2

    def test_authenticated_proxy_url_routes_through_proxy(self, mock_api_server, recording_proxy):
        """Credentials embedded in the proxy URL do not break routing.

        The recording proxy ignores auth headers (it doesn't validate them), so
        this test verifies that build_session constructs a valid authenticated
        proxy URL that the requests library accepts and uses.
        """
        api_port = mock_api_server.server_address[1]
        proxy_port = recording_proxy.server_address[1]

        api_url = f"http://127.0.0.1:{api_port}/posts"

        session = c.build_session(
            {
                "proxy_url": f"http://127.0.0.1:{proxy_port}",
                "proxy_username": "alice",
                "proxy_password": "s3cret",
            }
        )
        try:
            result = c.fetch_with_retry(session, api_url)
        finally:
            session.close()

        assert api_url in _RecordingProxyHandler.proxied_urls
        assert result == _MOCK_POSTS

    def test_proxy_returns_correct_json_payload(self, mock_api_server, recording_proxy):
        """The proxy forwards the response intact — no data corruption in transit."""
        api_port = mock_api_server.server_address[1]
        proxy_port = recording_proxy.server_address[1]

        session = c.build_session({"proxy_url": f"http://127.0.0.1:{proxy_port}"})
        try:
            posts = c.fetch_with_retry(session, f"http://127.0.0.1:{api_port}/posts")
            users = c.fetch_with_retry(session, f"http://127.0.0.1:{api_port}/users")
        finally:
            session.close()

        assert posts == _MOCK_POSTS
        assert users[0]["name"] == "Alice"
        assert users[0]["address"]["city"] == "Metropolis"
        assert users[0]["company"]["name"] == "Acme Corp"
