"""Tests for the http_proxy connector example.

These tests verify the proxy-specific behaviour that cannot be inferred from reading
the connector in isolation:

  - build_session wires session.proxies for http and https when proxy_url is set.
  - build_session leaves session.proxies empty when proxy_url is absent or a placeholder.
  - Credentials are embedded in the proxy URL and never appear in the plain proxy_url.
  - validate_configuration rejects malformed proxy URLs and incomplete auth pairs.
  - fetch_with_retry retries ProxyError and ConnectionError with exponential backoff.
  - fetch_with_retry surfaces a 4xx non-retryable error immediately without sleeping.
  - flatten_user hoists nested address/geo/company dicts to scalar columns.
  - update skips posts already delivered (post_id <= last_post_id) and advances state.
  - update skips records without an id field instead of raising.
  - update re-checkpoints the existing cursor on an empty result set.

Run: python -m pytest tests/ -v
"""

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import connector as c  # noqa: E402


# ----------------------------------------------------------------- helpers
def _post(post_id=1, user_id=1, title="t", body="b"):
    return {"id": post_id, "userId": user_id, "title": title, "body": body}


def _user(user_id=1, **overrides):
    base = {
        "id": user_id,
        "name": "Leanne Graham",
        "username": "Bret",
        "email": "Sincere@april.biz",
        "phone": "1-770-736-8031",
        "website": "hildegard.org",
        "address": {
            "street": "Kulas Light",
            "suite": "Apt. 556",
            "city": "Gwenborough",
            "zipcode": "92998-3874",
            "geo": {"lat": "-37.3159", "lng": "81.1496"},
        },
        "company": {
            "name": "Romaguera-Crona",
            "catchPhrase": "Multi-layered client-server neural-net",
            "bs": "harness real-time e-markets",
        },
    }
    base.update(overrides)
    return base


def _ok_response(json_body):
    """Return a mock requests.Response with status 200 and a JSON body."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = 200
    resp.json.return_value = json_body
    resp.raise_for_status.return_value = None
    return resp


def _error_response(status_code):
    """Return a mock requests.Response that raises HTTPError on raise_for_status.

    session.get returns this object; raise_for_status() then raises HTTPError with
    the response attached so fetch_with_retry can read e.response.status_code.
    """
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    # Attach the response to the HTTPError so the connector can inspect the status code.
    err = requests.exceptions.HTTPError(response=resp)
    err.response = resp
    resp.raise_for_status.side_effect = err
    return resp


# ======================================================== proxy session setup
class TestBuildSession:
    def test_proxy_url_wires_both_http_and_https(self):
        session = c.build_session({"proxy_url": "http://proxy.example.com:8080"})
        assert session.proxies.get("http") == "http://proxy.example.com:8080"
        assert session.proxies.get("https") == "http://proxy.example.com:8080"

    def test_no_proxy_url_leaves_proxies_empty(self):
        session = c.build_session({})
        assert not session.proxies

    def test_placeholder_proxy_url_is_ignored(self):
        session = c.build_session({"proxy_url": "<YOUR_PROXY_URL>"})
        assert not session.proxies

    def test_credentials_embedded_in_proxy_url(self):
        session = c.build_session(
            {
                "proxy_url": "http://proxy.example.com:8080",
                "proxy_username": "alice",
                "proxy_password": "s3cret",
            }
        )
        assert "alice:s3cret@" in session.proxies["http"]
        assert "alice:s3cret@" in session.proxies["https"]

    def test_credentials_not_present_when_placeholders(self):
        session = c.build_session(
            {
                "proxy_url": "http://proxy.example.com:8080",
                "proxy_username": "<OPTIONAL_PROXY_USERNAME>",
                "proxy_password": "<OPTIONAL_PROXY_PASSWORD>",
            }
        )
        assert "proxy.example.com" in session.proxies["http"]
        assert "@" not in session.proxies["http"]

    def test_https_proxy_url_accepted(self):
        session = c.build_session({"proxy_url": "https://secure-proxy.corp.com:3128"})
        assert session.proxies.get("https") == "https://secure-proxy.corp.com:3128"

    def test_proxy_label_redacts_credentials(self):
        label = c._proxy_label("http://user:pass@proxy.corp.com:8080")
        assert "user" not in label
        assert "pass" not in label
        assert "proxy.corp.com:8080" in label

    def test_proxy_label_empty_username_does_not_expose_password(self):
        """Empty username (':secret@host') must not cause the raw URL to be returned."""
        label = c._proxy_label("http://:secret@proxy.corp.com:8080")
        assert "secret" not in label

    def test_proxy_label_malformed_url_returns_placeholder(self):
        """A URL missing the scheme colon ('http//...') must not expose credentials."""
        label = c._proxy_label("http//alice:secret@proxy.corp.com:8080")
        assert "secret" not in label
        assert "alice" not in label


# ======================================================== configuration validation
class TestValidateConfiguration:
    @pytest.mark.parametrize(
        "config",
        [
            {"proxy_url": "ftp://proxy.example.com:21"},
            {"proxy_url": "http://proxy.example.com"},
            {"proxy_url": "not-a-url"},
            {"proxy_url": "http://proxy.example.com:8080", "proxy_username": "alice"},
            {"proxy_url": "http://proxy.example.com:8080", "proxy_password": "s3cr"},
        ],
    )
    def test_invalid_config_raises_value_error(self, config):
        with pytest.raises(ValueError):
            c.validate_configuration(config)

    @pytest.mark.parametrize(
        "config",
        [
            {},
            {"proxy_url": "<YOUR_PROXY_URL>"},
            {"proxy_url": "http://proxy.example.com:8080"},
            {
                "proxy_url": "http://proxy.example.com:8080",
                "proxy_username": "alice",
                "proxy_password": "s3cret",
            },
        ],
    )
    def test_valid_config_does_not_raise(self, config):
        c.validate_configuration(config)

    def test_missing_proxy_password_fails_fast(self):
        with pytest.raises(ValueError, match="proxy_password"):
            c.validate_configuration(
                {
                    "proxy_url": "http://proxy.example.com:8080",
                    "proxy_username": "alice",
                }
            )

    def test_missing_proxy_username_fails_fast(self):
        with pytest.raises(ValueError, match="proxy_username"):
            c.validate_configuration(
                {
                    "proxy_url": "http://proxy.example.com:8080",
                    "proxy_password": "s3cret",
                }
            )


# ======================================================== fetch_with_retry
class TestFetchWithRetry:
    def _session(self):
        session = MagicMock(spec=requests.Session)
        session.proxies = {}  # fetch_with_retry passes session.proxies explicitly
        return session

    def test_successful_fetch_returns_json(self):
        session = self._session()
        session.get.return_value = _ok_response([_post()])
        result = c.fetch_with_retry(session, "https://example.invalid/posts")
        assert result == [_post()]
        assert session.get.call_count == 1

    def test_proxy_error_retries_then_raises(self):
        session = self._session()
        session.get.side_effect = requests.exceptions.ProxyError("proxy down")
        max_retries = getattr(c, "__MAX_RETRIES")
        with patch.object(c.time, "sleep") as sleep:
            with pytest.raises(RuntimeError, match="Proxy connection failed"):
                c.fetch_with_retry(session, "https://example.invalid/posts")
        assert session.get.call_count == max_retries
        assert sleep.call_count == max_retries - 1

    def test_proxy_error_retries_then_succeeds(self):
        session = self._session()
        session.get.side_effect = [
            requests.exceptions.ProxyError("proxy down"),
            _ok_response([_post()]),
        ]
        with patch.object(c.time, "sleep"):
            result = c.fetch_with_retry(session, "https://example.invalid/posts")
        assert result == [_post()]
        assert session.get.call_count == 2

    def test_connection_error_retries_with_backoff(self):
        session = self._session()
        session.get.side_effect = requests.exceptions.ConnectionError("reset")
        with patch.object(c.time, "sleep") as sleep:
            with pytest.raises(RuntimeError, match="Connection failed"):
                c.fetch_with_retry(session, "https://example.invalid/posts")
        delays = [call_args[0][0] for call_args in sleep.call_args_list]
        assert delays == sorted(delays), "retry delays should increase (exponential backoff)"

    def test_timeout_retries_and_raises(self):
        session = self._session()
        session.get.side_effect = requests.exceptions.Timeout("timed out")
        max_retries = getattr(c, "__MAX_RETRIES")
        with patch.object(c.time, "sleep"):
            with pytest.raises(RuntimeError, match="timed out"):
                c.fetch_with_retry(session, "https://example.invalid/posts")
        assert session.get.call_count == max_retries

    def test_retryable_http_500_retries(self):
        session = self._session()
        session.get.side_effect = [
            _error_response(500),
            _ok_response({"ok": True}),
        ]
        with patch.object(c.time, "sleep"):
            result = c.fetch_with_retry(session, "https://example.invalid/posts")
        assert result == {"ok": True}

    def test_non_retryable_http_404_raises_immediately(self):
        session = self._session()
        session.get.return_value = _error_response(404)
        with patch.object(c.time, "sleep") as sleep:
            with pytest.raises(RuntimeError, match="404"):
                c.fetch_with_retry(session, "https://example.invalid/posts")
        assert sleep.call_count == 0, "4xx errors must not be retried"
        assert session.get.call_count == 1

    def test_fetch_calls_session_get_not_requests_get(self):
        """fetch_with_retry calls session.get, not the module-level requests.get.

        If the implementation called requests.get directly it would bypass the
        proxy configuration stored on the session and connect straight to the
        origin. This test gives a real requests.Session whose get() is patched so
        only that specific session object can satisfy the call.
        """
        session = requests.Session()
        with patch.object(
            session, "get", return_value=_ok_response([_post()])
        ) as mock_get, patch.object(c.requests, "get") as module_get:
            c.fetch_with_retry(session, "https://example.invalid/posts")
        mock_get.assert_called_once()
        module_get.assert_not_called()

    def test_proxy_session_proxies_are_present_during_get(self):
        """The session.get call happens while session.proxies is already set.

        This is the closest unit-level proof that requests will use the proxy:
        we configure a real Session with proxies, patch its get() to record the
        session state at call time, and assert the proxies dict was intact.
        """
        session = requests.Session()
        session.proxies = {
            "http": "http://proxy.corp.com:8080",
            "https": "http://proxy.corp.com:8080",
        }
        proxies_at_call_time = {}

        def capture_and_respond(*args, **kwargs):
            proxies_at_call_time.update(session.proxies)
            return _ok_response([_post()])

        with patch.object(session, "get", side_effect=capture_and_respond):
            c.fetch_with_retry(session, "https://example.invalid/posts")

        assert proxies_at_call_time.get("http") == "http://proxy.corp.com:8080"
        assert proxies_at_call_time.get("https") == "http://proxy.corp.com:8080"


# ======================================================== flatten_user
class TestFlattenUser:
    def test_all_nested_fields_hoisted(self):
        row = c.flatten_user(_user())
        assert row["city"] == "Gwenborough"
        assert row["geo_lat"] == "-37.3159"
        assert row["geo_lng"] == "81.1496"
        assert row["company_name"] == "Romaguera-Crona"
        assert row["company_catch_phrase"] == "Multi-layered client-server neural-net"

    def test_missing_address_yields_null_columns(self):
        row = c.flatten_user({"id": 2, "name": "Bob"})
        assert row["city"] is None
        assert row["geo_lat"] is None
        assert row["company_name"] is None

    def test_missing_geo_yields_null_lat_lng(self):
        user = _user()
        user["address"].pop("geo")
        row = c.flatten_user(user)
        assert row["geo_lat"] is None
        assert row["geo_lng"] is None

    def test_null_address_field_handled_gracefully(self):
        row = c.flatten_user({"id": 3, "address": None})
        assert row["city"] is None

    def test_top_level_fields_preserved(self):
        row = c.flatten_user(_user(user_id=7))
        assert row["id"] == 7
        assert row["username"] == "Bret"
        assert row["email"] == "Sincere@april.biz"


# ======================================================== update behaviour
class TestUpdate:
    def _api_responses(self, posts, users):
        """Return a side_effect list for two fetch_with_retry calls."""
        return [posts, users]

    def test_new_posts_are_upserted(self):
        posts = [_post(1), _post(2)]
        users = [_user(1)]
        with patch.object(
            c, "fetch_with_retry", side_effect=self._api_responses(posts, users)
        ), patch.object(c.op, "upsert") as upsert, patch.object(c.op, "checkpoint"), patch.object(
            c, "build_session", return_value=MagicMock()
        ):
            c.update({}, {})
        post_upserts = [x for x in upsert.call_args_list if x.kwargs["table"] == "posts"]
        assert len(post_upserts) == 2

    def test_already_synced_posts_are_skipped(self):
        posts = [_post(1), _post(2), _post(3)]
        users = [_user(1)]
        with patch.object(
            c, "fetch_with_retry", side_effect=self._api_responses(posts, users)
        ), patch.object(c.op, "upsert") as upsert, patch.object(c.op, "checkpoint"), patch.object(
            c, "build_session", return_value=MagicMock()
        ):
            c.update({}, {"last_post_id": 2})
        post_upserts = [x for x in upsert.call_args_list if x.kwargs["table"] == "posts"]
        ids = [x.kwargs["data"]["id"] for x in post_upserts]
        assert ids == [3], f"only post 3 should be new, got: {ids}"

    def test_state_advances_to_highest_post_id(self):
        posts = [_post(5), _post(3), _post(7)]
        users = [_user(1)]
        with patch.object(
            c, "fetch_with_retry", side_effect=self._api_responses(posts, users)
        ), patch.object(c.op, "upsert"), patch.object(
            c.op, "checkpoint"
        ) as checkpoint, patch.object(
            c, "build_session", return_value=MagicMock()
        ):
            c.update({}, {})
        final_state = checkpoint.call_args.kwargs["state"]
        assert final_state == {"last_post_id": 7}

    def test_post_without_id_is_skipped(self):
        posts = [{"userId": 1, "title": "no id", "body": "b"}, _post(1)]
        users = [_user(1)]
        with patch.object(
            c, "fetch_with_retry", side_effect=self._api_responses(posts, users)
        ), patch.object(c.op, "upsert") as upsert, patch.object(c.op, "checkpoint"), patch.object(
            c, "build_session", return_value=MagicMock()
        ):
            c.update({}, {})
        post_upserts = [x for x in upsert.call_args_list if x.kwargs["table"] == "posts"]
        assert len(post_upserts) == 1

    def test_user_without_id_is_skipped(self):
        posts = [_post(1)]
        users = [{"name": "No ID User"}, _user(1)]
        with patch.object(
            c, "fetch_with_retry", side_effect=self._api_responses(posts, users)
        ), patch.object(c.op, "upsert") as upsert, patch.object(c.op, "checkpoint"), patch.object(
            c, "build_session", return_value=MagicMock()
        ):
            c.update({}, {})
        user_upserts = [x for x in upsert.call_args_list if x.kwargs["table"] == "users"]
        assert len(user_upserts) == 1

    def test_empty_posts_does_not_regress_state(self):
        users = [_user(1)]
        with patch.object(c, "fetch_with_retry", side_effect=[[], users]), patch.object(
            c.op, "upsert"
        ), patch.object(c.op, "checkpoint") as checkpoint, patch.object(
            c, "build_session", return_value=MagicMock()
        ):
            c.update({}, {"last_post_id": 42})
        final_state = checkpoint.call_args.kwargs["state"]
        assert final_state == {"last_post_id": 42}, "state must not regress on empty response"

    def test_proxy_session_is_passed_to_every_fetch_call(self):
        """update passes the same session object (from build_session) to every
        fetch_with_retry call, not a fresh session that would bypass the proxy.

        We inject a real Session with proxies set and capture the session argument
        on each fetch_with_retry invocation, then assert it is always the same
        proxy-configured object.
        """
        posts = [_post(1)]
        users = [_user(1)]

        proxy_session = requests.Session()
        proxy_session.proxies = {
            "http": "http://proxy.example.com:8080",
            "https": "http://proxy.example.com:8080",
        }
        sessions_seen = []

        def tracking_fetch(session, url, params=None):
            sessions_seen.append(session)
            return posts if "posts" in url else users

        with patch.object(c, "build_session", return_value=proxy_session), patch.object(
            c, "fetch_with_retry", side_effect=tracking_fetch
        ), patch.object(c.op, "upsert"), patch.object(c.op, "checkpoint"):
            c.update({"proxy_url": "http://proxy.example.com:8080"}, {})

        assert len(sessions_seen) == 2, "expected one fetch for posts and one for users"
        for s in sessions_seen:
            assert s is proxy_session, "update must use the session returned by build_session"
            assert s.proxies.get("http") == "http://proxy.example.com:8080"

    def test_session_is_closed_after_update(self):
        posts = [_post(1)]
        users = [_user(1)]
        mock_session = MagicMock()
        with patch.object(c, "build_session", return_value=mock_session), patch.object(
            c, "fetch_with_retry", side_effect=self._api_responses(posts, users)
        ), patch.object(c.op, "upsert"), patch.object(c.op, "checkpoint"):
            c.update({}, {})
        mock_session.close.assert_called_once()

    def test_session_closed_even_when_api_raises(self):
        mock_session = MagicMock()
        with patch.object(c, "build_session", return_value=mock_session), patch.object(
            c, "fetch_with_retry", side_effect=RuntimeError("API down")
        ), patch.object(c.op, "upsert"), patch.object(c.op, "checkpoint"):
            with pytest.raises(RuntimeError):
                c.update({}, {})
        mock_session.close.assert_called_once()


# ======================================================== build_authenticated_proxy_url
class TestBuildAuthenticatedProxyUrl:
    def test_credentials_embedded_correctly(self):
        url = c._build_authenticated_proxy_url("http://proxy.example.com:8080", "alice", "s3cr3t")
        assert url == "http://alice:s3cr3t@proxy.example.com:8080"

    def test_https_scheme_preserved(self):
        url = c._build_authenticated_proxy_url("https://secure-proxy.corp.com:3128", "bob", "pass")
        assert url.startswith("https://")
        assert "bob:pass@" in url

    def test_reserved_characters_in_credentials_are_percent_encoded(self):
        """Credentials containing /, @, #, or : are percent-encoded so the URL stays valid.

        urlparse does not decode percent-encoded components, so assert on the raw URL
        and verify the encoded forms are present rather than the raw special characters.
        """
        url = c._build_authenticated_proxy_url(
            "http://proxy.example.com:8080", "alice/corp", "p@ss#w:rd"
        )
        # The literal / @ # : characters must be encoded so they do not break URL structure.
        assert (
            "/" not in url.split("@")[0].split("//", 1)[1]
        ), "unencoded '/' in username would corrupt the URL path"
        # Confirm the encoded forms are present
        assert "%2F" in url  # '/' → %2F
        assert "%40" in url  # '@' → %40
        assert "%23" in url  # '#' → %23

    def test_ipv6_host_brackets_are_preserved(self):
        """IPv6 proxy addresses retain their brackets in the constructed URL."""
        url = c._build_authenticated_proxy_url("http://[::1]:8080", "alice", "pass")
        assert "[::1]" in url
        assert url == "http://alice:pass@[::1]:8080"

    def test_at_sign_in_password_does_not_corrupt_host(self):
        """A password containing '@' must be encoded; splitting at the first '@' would
        truncate the password and leave a stray '@' in the host authority."""
        url = c._build_authenticated_proxy_url("http://proxy.example.com:8080", "alice", "p@ss")
        # The host must be intact and the '@' in the password must be encoded.
        assert url.endswith("@proxy.example.com:8080")
        assert "%40" in url  # '@' → %40


# ======================================================== validation errors do not expose credentials
class TestValidationErrorsRedactCredentials:
    def test_scheme_error_does_not_expose_password(self):
        """ValueError for bad scheme must not include embedded credentials."""
        with pytest.raises(ValueError) as exc_info:
            c.validate_configuration({"proxy_url": "ftp://user:s3cr3t@proxy.example.com:21"})
        assert "s3cr3t" not in str(exc_info.value)

    def test_missing_port_error_does_not_expose_password(self):
        """ValueError for missing port must not include embedded credentials."""
        with pytest.raises(ValueError) as exc_info:
            c.validate_configuration({"proxy_url": "http://user:s3cr3t@proxy.example.com"})
        assert "s3cr3t" not in str(exc_info.value)


# ======================================================== env proxy override
class TestEnvProxyOverride:
    def test_fetch_passes_session_proxies_explicitly_to_session_get(self):
        """fetch_with_retry passes proxies=session.proxies to session.get so the configured
        proxy takes precedence over HTTP_PROXY / HTTPS_PROXY environment variables."""
        session = MagicMock(spec=requests.Session)
        session.proxies = {
            "http": "http://proxy.example.com:8080",
            "https": "http://proxy.example.com:8080",
        }
        session.get.return_value = _ok_response([_post()])

        with patch.dict(os.environ, {"HTTP_PROXY": "http://evil-proxy.corp:9999"}):
            c.fetch_with_retry(session, "https://example.invalid/posts")

        _, call_kwargs = session.get.call_args
        assert call_kwargs.get("proxies") == session.proxies, (
            "fetch_with_retry must forward session.proxies explicitly so env HTTP_PROXY "
            "cannot override the configured proxy"
        )
