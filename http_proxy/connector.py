"""Demonstrates routing API requests through an HTTP or HTTPS proxy using requests.Session.

This connector syncs posts and users from JSONPlaceholder (https://jsonplaceholder.typicode.com)
while routing outbound traffic through a configurable HTTP or HTTPS proxy server.
See the Technical Reference documentation (https://fivetran.com/docs/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connector-sdk#connectordevelopmentbestpractices) for details.
"""

import json
import time
from urllib.parse import urlparse, urlunparse

import requests

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

__BASE_URL = "https://jsonplaceholder.typicode.com"
__MAX_RETRIES = 3
__API_TIMEOUT_SECONDS = 30
__RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

_VALID_PROXY_SCHEMES = {"http", "https"}
_PLACEHOLDER_PREFIX = "<"


def _is_placeholder(value: str) -> bool:
    return not value or value.startswith(_PLACEHOLDER_PREFIX)


def validate_configuration(configuration: dict):
    """Validate proxy settings; raise ValueError on misconfiguration.

    Called at the start of update to fail fast on bad proxy URLs or incomplete
    credential pairs before any network request is attempted.
    """
    proxy_url = configuration.get("proxy_url", "")
    if not proxy_url or _is_placeholder(proxy_url):
        return

    try:
        parsed = urlparse(proxy_url)
    except Exception as exc:
        raise ValueError(f"Invalid proxy_url {proxy_url!r}: {exc}") from exc

    if parsed.scheme not in _VALID_PROXY_SCHEMES:
        raise ValueError(
            f"proxy_url must use http:// or https://, got scheme {parsed.scheme!r}: {proxy_url!r}"
        )
    if not parsed.hostname:
        raise ValueError(f"proxy_url must include a hostname: {proxy_url!r}")
    if not parsed.port:
        raise ValueError(
            f"proxy_url must include an explicit port number: {proxy_url!r}"
        )

    proxy_username = configuration.get("proxy_username", "")
    proxy_password = configuration.get("proxy_password", "")
    has_username = bool(proxy_username) and not _is_placeholder(proxy_username)
    has_password = bool(proxy_password) and not _is_placeholder(proxy_password)

    if has_username and not has_password:
        raise ValueError("proxy_password is required when proxy_username is set")
    if has_password and not has_username:
        raise ValueError("proxy_username is required when proxy_password is set")


def _build_authenticated_proxy_url(proxy_url: str, username: str, password: str) -> str:
    """Embed credentials into the proxy URL."""
    parsed = urlparse(proxy_url)
    return urlunparse(
        parsed._replace(netloc=f"{username}:{password}@{parsed.hostname}:{parsed.port}")
    )


def _proxy_label(proxy_url: str) -> str:
    """Return a proxy URL string safe for logging (credentials redacted)."""
    parsed = urlparse(proxy_url)
    if parsed.username:
        return urlunparse(
            parsed._replace(netloc=f"***:***@{parsed.hostname}:{parsed.port}")
        )
    return proxy_url


def build_session(configuration: dict) -> requests.Session:
    """Create a requests.Session with proxy configured from configuration.

    Setting session.proxies once applies the proxy to every request made through
    the session without requiring per-call configuration.
    """
    session = requests.Session()

    proxy_url = configuration.get("proxy_url", "")
    if not proxy_url or _is_placeholder(proxy_url):
        return session

    proxy_username = configuration.get("proxy_username", "")
    proxy_password = configuration.get("proxy_password", "")
    has_username = bool(proxy_username) and not _is_placeholder(proxy_username)
    has_password = bool(proxy_password) and not _is_placeholder(proxy_password)

    effective_proxy_url = proxy_url
    if has_username and has_password:
        effective_proxy_url = _build_authenticated_proxy_url(
            proxy_url, proxy_username, proxy_password
        )

    session.proxies = {
        "http": effective_proxy_url,
        "https": effective_proxy_url,
    }
    log.info(f"Proxy configured: {_proxy_label(effective_proxy_url)}")
    return session


def fetch_with_retry(session: requests.Session, url: str, params: dict = None):
    """GET url via session with exponential backoff retry on transient errors.

    ProxyError, ConnectionError, Timeout, and retryable HTTP status codes
    (408, 429, 5xx) are retried up to __MAX_RETRIES times. Non-retryable
    HTTP errors (e.g. 400, 404) raise immediately without sleeping.
    """
    for attempt in range(__MAX_RETRIES):
        try:
            resp = session.get(url, params=params, timeout=__API_TIMEOUT_SECONDS)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.ProxyError as exc:
            log.warning(f"Proxy error on attempt {attempt + 1}/{__MAX_RETRIES}: {exc}")
            if attempt < __MAX_RETRIES - 1:
                time.sleep(2**attempt)
            else:
                raise RuntimeError(
                    f"Proxy connection failed after {__MAX_RETRIES} attempts: {exc}"
                ) from exc
        except requests.exceptions.ConnectionError as exc:
            log.warning(
                f"Connection error on attempt {attempt + 1}/{__MAX_RETRIES}: {exc}"
            )
            if attempt < __MAX_RETRIES - 1:
                time.sleep(2**attempt)
            else:
                raise RuntimeError(
                    f"Connection failed after {__MAX_RETRIES} attempts: {exc}"
                ) from exc
        except requests.exceptions.Timeout as exc:
            log.warning(f"Request timed out on attempt {attempt + 1}/{__MAX_RETRIES}")
            if attempt < __MAX_RETRIES - 1:
                time.sleep(2**attempt)
            else:
                raise RuntimeError(
                    f"Request timed out after {__MAX_RETRIES} attempts: {exc}"
                ) from exc
        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code in __RETRYABLE_STATUS_CODES:
                log.warning(
                    f"HTTP {status_code} on attempt {attempt + 1}/{__MAX_RETRIES}, retrying"
                )
                if attempt < __MAX_RETRIES - 1:
                    time.sleep(2**attempt)
                    continue
                raise RuntimeError(
                    f"HTTP {status_code} after {__MAX_RETRIES} attempts: {exc}"
                ) from exc
            raise RuntimeError(f"HTTP {status_code} error from {url}: {exc}") from exc


def flatten_user(user: dict) -> dict:
    """Flatten nested address, geo, and company dicts to scalar columns."""
    address = user.get("address") or {}
    geo = address.get("geo") or {}
    company = user.get("company") or {}

    row = {k: v for k, v in user.items() if k not in {"address", "company"}}
    row.update(
        {
            "street": address.get("street"),
            "suite": address.get("suite"),
            "city": address.get("city"),
            "zipcode": address.get("zipcode"),
            "geo_lat": geo.get("lat"),
            "geo_lng": geo.get("lng"),
            "company_name": company.get("name"),
            "company_catch_phrase": company.get("catchPhrase"),
            "company_bs": company.get("bs"),
        }
    )
    return row


def schema(configuration: dict):
    """Define the schema for posts (incremental) and users (full refresh) tables.

    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-methods#schema
    """
    return [
        {
            "table": "posts",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "user_id": "INT",
                "title": "STRING",
                "body": "STRING",
            },
        },
        {
            "table": "users",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "name": "STRING",
                "username": "STRING",
                "email": "STRING",
                "phone": "STRING",
                "website": "STRING",
                "street": "STRING",
                "suite": "STRING",
                "city": "STRING",
                "zipcode": "STRING",
                "geo_lat": "STRING",
                "geo_lng": "STRING",
                "company_name": "STRING",
                "company_catch_phrase": "STRING",
                "company_bs": "STRING",
            },
        },
    ]


def update(configuration: dict, state: dict):
    """Sync posts (incremental by ID) and users (full refresh) from JSONPlaceholder.

    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-methods#update
    """
    validate_configuration(configuration=configuration)

    last_post_id = state.get("last_post_id")
    new_last_post_id = last_post_id

    session = build_session(configuration)
    try:
        posts = fetch_with_retry(session, f"{__BASE_URL}/posts")
        users = fetch_with_retry(session, f"{__BASE_URL}/users")

        for post in posts:
            post_id = post.get("id")
            if post_id is None:
                continue
            if last_post_id is not None and post_id <= last_post_id:
                continue
            op.upsert(
                table="posts",
                data={
                    "id": post_id,
                    "user_id": post.get("userId"),
                    "title": post.get("title"),
                    "body": post.get("body"),
                },
            )
            if new_last_post_id is None or post_id > new_last_post_id:
                new_last_post_id = post_id

        for user in users:
            if user.get("id") is None:
                continue
            op.upsert(table="users", data=flatten_user(user))

        new_state = {"last_post_id": new_last_post_id}
        op.checkpoint(state=new_state)
        log.info(f"Sync complete. last_post_id={new_last_post_id}")
    finally:
        session.close()


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
