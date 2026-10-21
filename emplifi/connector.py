"""This connector demonstrates syncing social profile, metrics, posts, and aggregated
post-metrics data across multiple networks from the Emplifi social media management API.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For encoding the Basic-auth token:secret credential pair
import base64

# For serializing nested post fields and parsing Emplifi error payloads
import json

# For sleeping between retry attempts
import time

# For computing the incremental sync window and chunking it into date ranges
from datetime import date, datetime, timedelta

# For issuing HTTP requests to the Emplifi API
import requests

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

__BASE_URL = "https://api.emplifi.io"
__RETRY_STATUS_CODES = {500, 502, 503, 504}
__MAX_ATTEMPTS = 4

# Networks whose profiles/metrics/posts this connector syncs.
NETWORKS = [
    "facebook",
    "instagram",
    "twitter",
    "youtube",
    "linkedin",
    "pinterest",
    "tiktok",
    "snapchat",
    "whatsapp",
]

# 1-year backfill on first run; override via lookback_days in configuration.json.
DEFAULT_LOOKBACK_DAYS = 365

# Metrics calls are limited to a 12-month date range per the Emplifi API; chunk every sync
# window into 30-day slices so a large backfill never exceeds that limit in one call.
METRICS_CHUNK_DAYS = 30

# Emplifi's posts endpoints page at up to 100 posts per request.
POSTS_PAGE_SIZE = 100

# Per-network post endpoint paths.
POSTS_ENDPOINT = {
    "facebook": "/3/facebook/page/posts",
    "instagram": "/3/instagram/profile/posts",
    "twitter": "/3/twitter/profile/tweets",
    "youtube": "/3/youtube/profile/videos",
    "linkedin": "/3/linkedin/profile/posts",
    "pinterest": "/3/pinterest/profile/posts",
    "tiktok": "/3/tiktok/profile/posts",
    "snapchat": "/3/snapchat/profile/posts",
}

# Per-network valid profile metrics, from the Emplifi API docs metric table. A network not
# listed here (e.g. snapchat, whatsapp) has no supported profile metrics and is skipped in
# step 2.
PROFILE_METRICS = {
    "facebook": [
        "fans_lifetime",
        "fans_change",
        "insights_fan_adds",
        "insights_fan_removes",
        "insights_impressions",
        "insights_reach",
        "insights_reactions",
        "insights_follows",
    ],
    "instagram": [
        "followers_lifetime",
        "followers_change",
        "following_lifetime",
        "following_change",
        "insights_followers",
        "insights_impressions",
        "insights_media_views",
        "insights_profile_views",
    ],
    "twitter": [
        "followers_lifetime",
        "followers_change",
        "following_lifetime",
    ],
    "youtube": [
        "subscribers_lifetime",
        "subscribers_change",
        "views_change",
        "viewed_time_change",
        "video_lifetime",
    ],
    "linkedin": [
        "followers_lifetime",
        "followers_change",
        "insights_impressions",
    ],
    "pinterest": [
        "followers_lifetime",
    ],
    "tiktok": [
        "insights_fans_lifetime",
        "insights_fans_change",
        "insights_video_views",
        "insights_profile_views",
    ],
}

# Per-network valid post fields, sourced from the Emplifi API docs per-network posts section.
# Only confirmed-valid fields are included per network to avoid Code-3 validation errors.
POST_FIELDS_BY_NETWORK = {
    # /3/facebook/page/posts - reactions, comments, shares, interactions, post_attribution
    # are all documented as valid.
    "facebook": [
        "id",
        "created_time",
        "content",
        "url",
        "media_type",
        "interactions",
        "reactions",
        "comments",
        "shares",
        "post_attribution",
        "profileId",
    ],
    # /3/instagram/profile/posts - comments (not number_of_comments), likes, and
    # interactions are all documented as valid.
    "instagram": [
        "id",
        "created_time",
        "content",
        "url",
        "media_type",
        "interactions",
        "likes",
        "comments",
        "profileId",
    ],
    # /3/twitter/profile/tweets - per Twitter's Developer Policy, Emplifi no longer provides
    # individual tweet details. Only these fields are allowed: no metrics (no created_time,
    # interactions, likes, etc.).
    "twitter": [
        "id",
        "profileId",
        "origin",
    ],
    # /3/youtube/profile/videos - documented fields: comments, created_time, id,
    # interactions, likes, url, video_views, video_view_time, duration, profileId.
    # title is invalid (use description); number_of_comments and insights_video_views
    # are also invalid here.
    "youtube": [
        "id",
        "created_time",
        "url",
        "description",
        "interactions",
        "likes",
        "comments",
        "video_views",
        "video_view_time",
        "duration",
        "profileId",
    ],
    # /3/linkedin/profile/posts - documented fields: comments, reactions, interactions,
    # content, created_time, id, url. likes is invalid (LinkedIn uses reactions).
    "linkedin": [
        "id",
        "created_time",
        "content",
        "url",
        "media_type",
        "interactions",
        "reactions",
        "comments",
        "profileId",
    ],
    # /3/pinterest/profile/posts - kept to a minimal safe set; few profiles use this network.
    "pinterest": [
        "id",
        "created_time",
        "url",
        "interactions",
        "comments",
    ],
    # /3/tiktok/profile/posts - basic fields per the docs: id, created_time, link, media,
    # message, duration, content_type (no profileId, no interactions, no likes in basic
    # fields). insights_ fields require insights_enabled=true on the profile; they fail
    # gracefully (Code-7) per-profile when not enabled.
    "tiktok": [
        "id",
        "created_time",
        "link",
        "media",
        "message",
        "duration",
        "insights_video_views",
        "insights_engagements",
        "insights_likes",
        "insights_comments",
        "insights_shares",
    ],
}

# Cross-platform aggregated metrics for /3/aggregated-metrics, with names taken directly
# from the Emplifi API docs metric table. Invalid names that cause Code-3 errors: reactions,
# comments, video_views, reach, shares - the correct equivalents are number_of_comments,
# page_shares, insights_video_views, etc.
AGGREGATED_METRICS = [
    "interactions",
    "likes",
    "number_of_comments",
    "page_posts",
    "insights_engagements",
    "insights_video_views",
    "insights_impressions",
    "page_shares",
    "engagement_rate",
]

# Emplifi allows up to 100 profiles per /3/aggregated-metrics request.
AGGREGATED_METRICS_BATCH_SIZE = 100


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    for key in ("token", "secret"):
        if not str(configuration.get(key, "")).strip():
            raise ValueError(f"Missing required configuration value: '{key}'")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": "profiles", "primary_key": ["id", "network"]},
        {
            "table": "profile_metrics",
            "primary_key": ["profile_id", "network", "date", "metric_name"],
        },
        {"table": "posts", "primary_key": ["id", "network"]},
        {
            "table": "aggregated_post_metrics",
            "primary_key": ["profile_id", "network", "date", "metric_name"],
        },
    ]


def _auth_header(configuration: dict) -> dict:
    """Build the Basic-auth and JSON headers used on every Emplifi API request."""
    token = configuration["token"]
    secret = configuration["secret"]
    encoded = base64.b64encode(f"{token}:{secret}".encode()).decode()
    return {
        "Authorization": f"Basic {encoded}",
        "Content-Type": "application/json; charset=utf-8",
    }


def _request_with_backoff(method, url, configuration, **kwargs):
    """Issue an HTTP request, retrying transient failures with exponential backoff.

    Retries (up to __MAX_ATTEMPTS): connection errors, timeouts, HTTP 429 (honoring
    Retry-After when present), and HTTP 5xx. Any other response, including a permanent
    client error, is returned as-is for the caller to inspect or raise on.

    Args:
        method: the HTTP method to use.
        url: the absolute URL to request.
        configuration: the validated connector configuration, used to build auth headers.
        **kwargs: additional keyword arguments forwarded to requests.request().

    Returns:
        The requests.Response object for the final attempt.

    Raises:
        RuntimeError: if every attempt fails with a connection error or timeout.
    """
    last_exception = None
    for attempt in range(1, __MAX_ATTEMPTS + 1):
        try:
            response = requests.request(
                method,
                url,
                headers=_auth_header(configuration),
                timeout=60,
                **kwargs,
            )
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            last_exception = exc
            if attempt >= __MAX_ATTEMPTS:
                raise RuntimeError(
                    f"{method} {url} failed after {__MAX_ATTEMPTS} attempts: {exc}"
                ) from exc
            log.warning(
                f"{method} {url} attempt {attempt}/{__MAX_ATTEMPTS} failed ({exc}), retrying"
            )
            time.sleep(min(60, 2**attempt))
            continue

        retryable = response.status_code == 429 or response.status_code in __RETRY_STATUS_CODES
        if retryable and attempt < __MAX_ATTEMPTS:
            wait_seconds = min(60, 2**attempt)
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.strip().isdigit():
                    wait_seconds = min(60, int(retry_after))
            log.warning(
                f"{method} {url} got HTTP {response.status_code} on attempt "
                f"{attempt}/{__MAX_ATTEMPTS}; retrying in {wait_seconds:.0f}s"
            )
            time.sleep(wait_seconds)
            continue

        return response

    raise RuntimeError(f"{method} {url} failed after {__MAX_ATTEMPTS} attempts: {last_exception}")


def _parse_body(body: dict, url: str, payload: dict = None) -> dict:
    """Check an Emplifi response body for application-level error codes.

    These are Emplifi-specific semantics returned with a 200 status, layered on top of the
    transport-level retry handled by _request_with_backoff: a successful HTTP response can
    still carry a body-level error that calls for different handling per code.

    Args:
        body: the decoded JSON response body.
        url: the request URL, used for logging context.
        payload: the request payload, used for logging context on a Code-7 error.

    Returns:
        The original body if successful, or an empty header/data body for a recoverable
        application-level error (Code-3, 6, or 7).

    Raises:
        RuntimeError: on a rate-limit error code (10/11) or any other unrecognized error.
    """
    if body.get("success", True):
        return body

    errors = body.get("errors", [])
    err_codes = [e.get("code") for e in errors if isinstance(e, dict)]

    if 10 in err_codes or 11 in err_codes:
        code = 10 if 10 in err_codes else 11
        raise RuntimeError(f"Rate limit (code {code}): {json.dumps(errors)}")

    if 6 in err_codes:
        log.warning(f"Code-6 (before history limit): {url}")
        return {"success": True, "header": [], "data": []}

    if 3 in err_codes:
        log.warning(f"Code-3 validation error at POST {url}: {json.dumps(errors)}")
        return {"success": True, "header": [], "data": []}

    if 7 in err_codes:
        log.warning(
            f"Code-7 (insights not enabled): {payload.get('profiles') if payload else 'n/a'}"
        )
        return {"success": True, "header": [], "data": []}

    raise RuntimeError(f"Emplifi API error at {url}: {json.dumps(errors)}")


def _get(url: str, configuration: dict) -> dict:
    """Issue a GET request against the Emplifi API."""
    response = _request_with_backoff("GET", url, configuration)
    response.raise_for_status()
    return response.json()


def _post(url: str, payload: dict, configuration: dict) -> dict:
    """Issue a POST request against the Emplifi API."""
    response = _request_with_backoff("POST", url, configuration, json=payload)
    response.raise_for_status()
    return _parse_body(response.json(), url, payload)


def _date_chunks(date_start: str, date_end: str, chunk_days: int = METRICS_CHUNK_DAYS):
    """Split a date range into chunks of at most chunk_days days, inclusive of both ends."""
    start = datetime.strptime(date_start, "%Y-%m-%d").date()
    end = datetime.strptime(date_end, "%Y-%m-%d").date()
    while start <= end:
        end_chunk = min(start + timedelta(days=chunk_days - 1), end)
        yield start.strftime("%Y-%m-%d"), end_chunk.strftime("%Y-%m-%d")
        start = end_chunk + timedelta(days=1)


def _parse_metrics_response(body: dict, profile_id: str) -> list:
    """Parse an Emplifi header+data matrix response into narrow per-metric rows.

    Response shape:
      header: [{type: "date.day", rows: ["2024-01-01", ...]},
               {type: "metric",   rows: ["fans_lifetime", ...]}]
      data:   [[val0, val1, ...], ...]  (one inner list per date)

    Args:
        body: the decoded response body.
        profile_id: the profile ID these metrics belong to.

    Returns:
        A list of {profile_id, date, metric_name, value} dicts.
    """
    header = body.get("header", [])
    data = body.get("data", [])
    if not header or not data:
        return []

    dates, metrics = [], []
    for h in header:
        if h.get("type") == "date.day":
            dates = h["rows"]
        elif h.get("type") == "metric":
            metrics = h["rows"]

    if not dates or not metrics:
        return []

    rows_out = []
    for di, d in enumerate(dates):
        if di >= len(data):
            continue
        row_vals = data[di]
        for mi, mname in enumerate(metrics):
            if mi >= len(row_vals):
                continue
            raw = row_vals[mi]
            if isinstance(raw, list):
                log.warning(f"Nested list in response for {mname} on {d} - skipping")
                continue
            rows_out.append(
                {
                    "profile_id": profile_id,
                    "date": d,
                    "metric_name": mname,
                    "value": float(raw) if raw is not None else None,
                }
            )
    return rows_out


def _fetch_all_profiles(configuration: dict) -> dict:
    """Fetch connected profiles for every supported network.

    Args:
        configuration: the validated connector configuration.

    Returns:
        A dict mapping network name to its list of profile dicts.
    """
    base_url = configuration.get("base_url", __BASE_URL)
    result = {}
    for network in NETWORKS:
        url = f"{base_url}/3/{network}/profiles"
        try:
            body = _get(url, configuration)
            profiles = body.get("profiles", [])
            result[network] = profiles
            log.info(f"  {network}: {len(profiles)} profile(s)")
        except requests.HTTPError as e:
            log.warning(f"  {network}: skipped ({e})")
            result[network] = []
    return result


def _sync_profiles(profiles_by_network: dict):
    """Upsert every fetched profile into the profiles table."""
    for network, profiles in profiles_by_network.items():
        for p in profiles:
            op.upsert(
                "profiles",
                {
                    "id": str(p.get("id", "")),
                    "network": network,
                    "name": p.get("name"),
                    "profile_name": p.get("profile_name"),
                    "picture": p.get("picture"),
                    "timezone": p.get("timezone"),
                    "insights_enabled": p.get("insights_enabled", False),
                    "community_enabled": p.get("community_enabled", False),
                },
            )


def _sync_profile_metrics(configuration, profiles_by_network, date_start, date_end):
    """Fetch and upsert daily profile metrics for every profile, one profile per call."""
    base_url = configuration.get("base_url", __BASE_URL)

    for network, profiles in profiles_by_network.items():
        if not profiles:
            continue
        metrics = PROFILE_METRICS.get(network, [])
        if not metrics:
            continue

        url = f"{base_url}/3/{network}/metrics"
        log.info(f"  profile_metrics [{network}] - {len(profiles)} profiles")

        for profile in profiles:
            pid = str(profile["id"])
            for d_start, d_end in _date_chunks(date_start, date_end):
                payload = {
                    "date_start": d_start,
                    "date_end": d_end,
                    "profiles": [pid],
                    # Emplifi caps metrics requests at 25 per call.
                    "metrics": metrics[:25],
                    "dimensions": [{"type": "date.day"}],
                }
                try:
                    body = _post(url, payload, configuration)
                except (RuntimeError, requests.HTTPError) as e:
                    log.warning(f"profile_metrics [{network}] pid={pid}: {e}")
                    continue

                for row in _parse_metrics_response(body, pid):
                    op.upsert(
                        "profile_metrics",
                        {
                            "profile_id": row["profile_id"],
                            "network": network,
                            "date": row["date"],
                            "metric_name": row["metric_name"],
                            "value": row["value"],
                        },
                    )


def _sync_posts(configuration, profiles_by_network, date_start, date_end):
    """Fetch and upsert posts for every profile, paginating with Emplifi's cursor.

    Key requirements from the Emplifi API:
      - "fields" must be present (per-network valid fields only, from POST_FIELDS_BY_NETWORK).
      - "after" must NOT be present on the first page request (omit it entirely).
      - Subsequent pages send ONLY {"after": "<cursor>"}, no other fields.

    Twitter note: due to Twitter's Developer Policy, Emplifi only returns id/profileId/origin.
    TikTok note: insights_ fields only populate for profiles with insights_enabled=true.
    """
    base_url = configuration.get("base_url", __BASE_URL)

    for network, profiles in profiles_by_network.items():
        if not profiles:
            continue
        endpoint = POSTS_ENDPOINT.get(network)
        if not endpoint:
            continue

        fields = POST_FIELDS_BY_NETWORK.get(network, ["id", "created_time"])
        url = f"{base_url}{endpoint}"
        log.info(f"  posts [{network}] - {len(profiles)} profiles")

        for profile in profiles:
            pid = str(profile["id"])

            # First-page payload has no "after" key at all. Emplifi uses a oneOf schema:
            # branch A (first page) requires profiles + date_start + date_end + fields, no
            # after; branch B (pagination) requires ONLY the after cursor from the previous
            # response. Sending after:"" or after:null breaks branch A validation with a
            # 400 Bad Request.
            payload = {
                "profiles": [pid],
                "date_start": date_start,
                "date_end": date_end,
                "fields": fields,
                "limit": POSTS_PAGE_SIZE,
            }

            while True:
                try:
                    body = _post(url, payload, configuration)
                except (RuntimeError, requests.HTTPError) as e:
                    log.warning(f"posts [{network}] pid={pid}: {e}")
                    break

                data_obj = body.get("data", {})
                if isinstance(data_obj, dict):
                    posts = data_obj.get("posts", [])
                    next_cursor = data_obj.get("next")
                elif isinstance(data_obj, list):
                    posts = data_obj
                    next_cursor = None
                else:
                    break

                for post in posts:
                    post_id = str(post.get("id", ""))
                    if not post_id:
                        continue
                    row = {"network": network, "profile_id": pid}
                    for k, v in post.items():
                        if isinstance(v, (str, int, float, bool)) or v is None:
                            row[k] = v
                        elif isinstance(v, dict):
                            row[k] = json.dumps(v)
                        else:
                            row[k] = str(v)
                    op.upsert("posts", row)

                if not next_cursor:
                    break

                # Branch B: subsequent pages send only the cursor string.
                payload = {"after": next_cursor}


def _sync_aggregated_post_metrics(configuration, profiles_by_network, date_start, date_end):
    """Fetch and upsert cross-platform aggregated post metrics, batched across profiles.

    POST /3/aggregated-metrics requires:
      - "metric" (singular string), one metric per request.
      - profiles items are {"id": pid, "platform": network} objects.
      - up to AGGREGATED_METRICS_BATCH_SIZE profiles per request, to save rate-limit quota.

    The response is a header+data matrix; a profile dimension is added when more than one
    profile is requested. This batches all profiles across all networks into groups of
    AGGREGATED_METRICS_BATCH_SIZE, issuing one call per metric per batch per date chunk.
    """
    base_url = configuration.get("base_url", __BASE_URL)
    url = f"{base_url}/3/aggregated-metrics"

    all_profile_objs = [
        {"id": str(p["id"]), "platform": network}
        for network, profiles in profiles_by_network.items()
        for p in profiles
    ]
    if not all_profile_objs:
        return

    batches = [
        all_profile_objs[i : i + AGGREGATED_METRICS_BATCH_SIZE]
        for i in range(0, len(all_profile_objs), AGGREGATED_METRICS_BATCH_SIZE)
    ]

    log.info(
        f"  aggregated_post_metrics - {len(all_profile_objs)} profiles, {len(batches)} "
        f"batch(es), {len(AGGREGATED_METRICS)} metrics"
    )

    endpoint_confirmed_missing = False
    for metric in AGGREGATED_METRICS:
        if endpoint_confirmed_missing:
            break

        for batch in batches:
            if endpoint_confirmed_missing:
                break

            for d_start, d_end in _date_chunks(date_start, date_end):
                payload = {
                    "date_start": d_start,
                    "date_end": d_end,
                    "profiles": batch,
                    "metric": metric,
                    "dimensions": [{"type": "date.day"}, {"type": "profile"}],
                }
                try:
                    body = _post(url, payload, configuration)
                except requests.HTTPError as e:
                    status = e.response.status_code if e.response is not None else None
                    if status in (404, 405):
                        log.warning(f"Aggregated endpoint returned {status} - skipping")
                        endpoint_confirmed_missing = True
                        return
                    log.warning(f"aggregated metric={metric} batch[0]={batch[0]}: {e}")
                    continue
                except RuntimeError as e:
                    log.warning(f"aggregated metric={metric}: {e}")
                    continue

                _upsert_aggregated_metrics_response(body, metric, batch)


def _upsert_aggregated_metrics_response(body, metric, batch):
    """Parse one aggregated-metrics response and upsert its rows.

    header/data matrix format:
      header[0]: type=date.day, rows=['2026-03-03', ...]
      header[1]: type=profile,  rows=[{id, platform}, ...] (present only for batch size > 1)
      data shape: data[date_idx] is a list with one 1-element [value] list per profile,
      or data[date_idx] is a single 1-element [value] list when there's no profile dimension.
    """
    header = body.get("header", [])
    data = body.get("data", [])
    if not header or not data:
        return

    dates, profiles_resp = [], []
    for h in header:
        htype = h.get("type", "")
        if htype == "date.day":
            dates = h.get("rows", [])
        elif htype == "profile":
            profiles_resp = h.get("rows", [])

    if not dates:
        log.warning(f"  [AGG] metric={metric}: no date.day dimension in header, skipping")
        return

    for di, d in enumerate(dates):
        if di >= len(data):
            continue
        row_data = data[di]

        if profiles_resp:
            if not isinstance(row_data, list):
                row_data = [row_data]
            for pi, pid_obj in enumerate(profiles_resp):
                if pi >= len(row_data):
                    continue
                raw = row_data[pi]
                if isinstance(raw, list):
                    raw = raw[0] if raw else None
                pid_str = str(pid_obj["id"]) if isinstance(pid_obj, dict) else str(pid_obj)
                nw = next(
                    (obj["platform"] for obj in batch if obj["id"] == pid_str),
                    "unknown",
                )
                op.upsert(
                    "aggregated_post_metrics",
                    {
                        "profile_id": pid_str,
                        "network": nw,
                        "date": d,
                        "metric_name": metric,
                        "value": float(raw) if raw is not None else None,
                    },
                )
        else:
            raw = row_data[0] if isinstance(row_data, list) else row_data
            if isinstance(raw, list):
                raw = raw[0] if raw else None
            op.upsert(
                "aggregated_post_metrics",
                {
                    "profile_id": batch[0]["id"],
                    "network": batch[0]["platform"],
                    "date": d,
                    "metric_name": metric,
                    "value": float(raw) if raw is not None else None,
                },
            )


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    validate_configuration(configuration=configuration)

    today = date.today()
    yesterday = today - timedelta(days=1)

    last_synced = state.get("last_synced_date")
    if last_synced:
        # Re-sync from 2 days before the last checkpoint to absorb any late-arriving data.
        sync_start = (
            datetime.strptime(last_synced, "%Y-%m-%d").date() - timedelta(days=2)
        ).strftime("%Y-%m-%d")
        log.info(f"Incremental from {sync_start} (last: {last_synced})")
    else:
        lookback = int(configuration.get("lookback_days", DEFAULT_LOOKBACK_DAYS))
        sync_start = (today - timedelta(days=lookback)).strftime("%Y-%m-%d")
        log.info(f"Initial backfill - {lookback} days from {sync_start}")

    sync_end = yesterday.strftime("%Y-%m-%d")
    log.info(f"Window: {sync_start} -> {sync_end}")

    log.info("STEP 1 - Fetching all connected profiles ...")
    profiles_by_network = _fetch_all_profiles(configuration)
    total = sum(len(v) for v in profiles_by_network.values())
    log.info(f"  Total: {total} profiles")

    if total == 0:
        log.warning(
            "Zero profiles returned - likely rate-limited. State NOT advanced. " "Wait and re-run."
        )
        return

    _sync_profiles(profiles_by_network)
    # Checkpoint the same (unfinalized) state after each step so upserts made so far are
    # durably committed sooner, bounding how much work is replayed on an interruption.
    # last_synced_date itself is only finalized once the full window has been synced below.
    op.checkpoint(state)

    log.info("STEP 2 - Profile metrics ...")
    _sync_profile_metrics(configuration, profiles_by_network, sync_start, sync_end)
    op.checkpoint(state)

    log.info("STEP 3 - Posts ...")
    _sync_posts(configuration, profiles_by_network, sync_start, sync_end)
    op.checkpoint(state)

    log.info("STEP 4 - Aggregated post metrics ...")
    _sync_aggregated_post_metrics(configuration, profiles_by_network, sync_start, sync_end)

    state["last_synced_date"] = sync_end
    op.checkpoint(state)
    log.info(f"Done. Checkpointed last_synced_date={sync_end}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
#
# IMPORTANT: The recommended way to test your connector is using the Fivetran debug command:
#   fivetran debug
#
# This local testing block is provided as a convenience for quick debugging during development,
# such as using IDE debug tools (breakpoints, step-through debugging, etc.).
# Note: This method is not called by Fivetran when executing your connector in production.
# Always test using 'fivetran debug' prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)