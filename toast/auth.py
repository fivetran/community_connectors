import requests as rq
import datetime
import time
import copy
import os

from fivetran_connector_sdk import Logging as log

# Matches Toast's own token lifetime, and the 23h59m cadence this connector already
# used historically for the same reason: refresh just before expiry rather than on it.
TOKEN_REFRESH_INTERVAL_SECONDS = 86340
FIVETRAN_CONNECTIONS_BASE_URL = "https://api.fivetran.com/v1/connections"

def get_session_token(base_url, configuration, state):
    """
    Returns a valid Toast bearer token. Reuses configuration["token"] directly when it
    was refreshed within TOKEN_REFRESH_INTERVAL_SECONDS; otherwise gets a fresh one from
    Toast. Does NOT push the fresh token to this connection's own Fivetran configuration
    itself - that's deferred to push_token_to_fivetran(), called once at the end of a
    sync (see connector.py's update()), so a broken fivetran_system_key never blocks this
    sync's actual Toast data pull.

    Deliberately does not cache/encrypt a token in sync state the way this connector
    used to (Fernet + state["encrypted_token"]) - Fivetran's configuration store is
    already encrypted at rest, so storing the current token there instead avoids
    reinventing secret storage in the connector's own code. See
    https://github.com/fivetran/connector_sdk/tree/main/examples/common_patterns/update_configuration_during_sync
    for the canonical pattern this follows.
    :param base_url: Toast API URL.
    :param configuration: connector configuration - holds the current token, if any.
    :param state: connector state - holds last_token_refresh_time.
    :return: a valid Toast bearer token (not the full headers dict).
    """
    last_refresh = state.get("last_token_refresh_time")
    has_token = bool(configuration.get("token"))

    if last_refresh and not has_token:
        # Expected in two cases: (1) the first sync after migrating off the old Fernet/
        # state-cached-token approach, since there's nothing to carry over from state
        # into configuration; (2) a `fivetran deploy --configuration <file>` ran after a
        # token was already pushed - that PATCHes the connection's whole config from the
        # local file, which never has "token" (it's only ever added at runtime), wiping
        # it out even though state's last_token_refresh_time is untouched by that deploy.
        # Any other time, the token went missing from Fivetran's stored config some other
        # way (cleared manually, a prior push_token_to_fivetran() PATCH that didn't actually
        # persist, etc.). Not fatal in any case - this still self-heals by refreshing
        # below - but it means the "store the token in configuration instead of state"
        # approach isn't holding, so flag it loudly rather than silently refreshing.
        log.warning("state has a prior last_token_refresh_time but configuration['token'] "
                   "is missing - refreshing again. Expected once during migration off the "
                   "old state-cached-token approach, or after a `fivetran deploy "
                   "--configuration <file>` reset the config post-migration; unexpected "
                   "otherwise")

    if last_refresh and has_token:
        seconds_since_refresh = (datetime.datetime.now(datetime.timezone.utc)
                                 - datetime.datetime.fromisoformat(last_refresh.replace("Z", "+00:00"))).total_seconds()
        if seconds_since_refresh < TOKEN_REFRESH_INTERVAL_SECONDS:
            log.info("token still fresh, reusing configuration['token']")
            return configuration["token"]

    return refresh_session_token(base_url, configuration, state)

def refresh_session_token(base_url, configuration, state):
    """
    Always gets a brand-new token from Toast and stores it in configuration["token"] for
    immediate use this sync. Deliberately does NOT push it to this connection's own
    Fivetran configuration here - that's deferred to push_token_to_fivetran(), attempted
    once at the end of a sync, so a failure to push (e.g. a broken fivetran_system_key)
    never blocks this sync's actual Toast data pull. Marks state["_needs_token_push"] so
    update() knows a push is owed once this run's syncing is done.
    :param base_url: Toast API URL.
    :param configuration: connector configuration - configuration["token"] is set here.
    :param state: connector state - state["_needs_token_push"] is set here.
    :return: a fresh Toast bearer token.
    """
    payload = {
        "clientId": configuration.get("clientId"),
        "clientSecret": configuration.get("clientSecret"),
        "userAccessType": configuration.get("userAccessType")
    }

    try:
        auth_response = rq.post(f"{base_url}/authentication/v1/authentication/login",
                                json=payload, timeout=10)
        auth_response.raise_for_status()
        auth_token = auth_response.json().get("token", {}).get("accessToken")

        if not auth_token:
            raise ValueError("Authentication failed: accessToken missing in response")

    except rq.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to authenticate: {e}")

    configuration["token"] = auth_token
    state["_needs_token_push"] = True

    return auth_token

def push_token_to_fivetran(configuration, state):
    """
    Best-effort push of configuration["token"] into this connection's own configuration
    via the Fivetran REST API, per
    https://fivetran.com/docs/rest-api/api-reference/connections/modify-connection .
    Fivetran's API overwrites every config key on each PATCH, so the whole configuration
    dict (not just the token) is sent as the secrets_list payload.

    Called once at the end of a sync (see connector.py's update()), after all of this
    sync's Toast data has already been upserted/checkpointed - deliberately not called
    inline at token-refresh time, so a broken fivetran_system_key (missing
    CONNECTOR: MANAGE, wrong/deleted key, etc.) never costs a sync its Toast data. On
    failure, logs loudly and returns False rather than raising: state["last_token_refresh_time"]
    is only set here, on success, so every sync will just keep re-authenticating against
    Toast fresh until the key is fixed - data keeps flowing in the meantime, just with an
    extra Toast login call each time.
    :param configuration: this connector's configuration dict - holds the new token to push.
    :param state: connector state - state["last_token_refresh_time"] is set here on success.
    :return: True if the push succeeded (or was skipped in local_debug), False otherwise.
    """
    connection_id = os.environ.get("FIVETRAN_CONNECTION_ID")
    update_url = f"{FIVETRAN_CONNECTIONS_BASE_URL}/{connection_id}"

    payload = {
        "config": {
            "secrets_list": [
                {"key": str(key), "value": str(value)} for key, value in configuration.items()
            ]
        }
    }

    # fivetran debug sets FIVETRAN_DEPLOYMENT_MODEL="local_debug" - there's no real
    # connection to update there, so skip the live call. Log only which KEYS would have
    # been sent, never the values - configuration holds secrets (clientSecret,
    # fivetran_system_key, token) that must never be written to logs.
    if os.environ.get("FIVETRAN_DEPLOYMENT_MODEL") == "local_debug":
        updated_keys = [entry["key"] for entry in payload["config"]["secrets_list"]]
        log.info(f"[local_debug] Skipping Fivetran REST API call. Would have PATCHed {update_url} with keys: {updated_keys}")
        state["last_token_refresh_time"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        return True

    headers = {
        "Accept": "application/json;version=2",
        "Authorization": f"Basic {configuration.get('fivetran_system_key')}",
        "content-type": "application/json",
    }
    try:
        response = rq.patch(update_url, json=payload, headers=headers)
        response.raise_for_status()
    except rq.exceptions.RequestException as e:
        log.error(f"Failed to push refreshed Toast token to this connection's Fivetran "
                  f"configuration (PATCH {update_url}) - fivetran_system_key may be missing, "
                  f"invalid, or lacking CONNECTOR: MANAGE for this connection. Toast data "
                  f"synced fine this run, but every sync will keep re-authenticating against "
                  f"Toast from scratch until this is fixed: {e}")
        return False

    refreshed_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    log.info(f"Configuration updated successfully with token last refreshed at {refreshed_at}")
    state["last_token_refresh_time"] = refreshed_at
    return True

def get_api_response(endpoint_path, headers, configuration=None, base_url=None, state=None, **kwargs):
    """
    Sends an HTTP GET request to the provided URL with specified parameters.

    - Retries if a 401 Unauthorized response occurs (up to a limit), forcing a real
      token refresh first if configuration/base_url are provided.
    - Skips the endpoint if a 403 Forbidden response is received.
    - Handles rate-limiting (429) and retries accordingly.
    - Retries transient server errors (5xx) with exponential backoff (10s, 20s, 40s, 80s).
    - Logs and returns API responses.

    :param endpoint_path: API URL
    :param headers: Request headers - mutated in place on a token refresh so every
        other caller holding a reference to this same dict also picks up the new token
    :param configuration: connector configuration, needed to re-authenticate on a 401.
        If omitted, a 401 just retries with a delay instead of refreshing the token.
    :param base_url: Toast API base URL, needed to re-authenticate on a 401.
    :param state: connector state - if provided, last_token_refresh_time is updated on
        a 401 refresh too, so the next sync doesn't needlessly re-refresh a still-fresh
        token. Optional: a 401 refresh is correct either way since it always forces a
        real Toast call rather than trusting the cadence check.
    :param kwargs: Additional request parameters
    :return: Tuple (response JSON, next_page_token) or (None, None) if failed
    """
    timerange_data = kwargs.get("data", {})
    params = copy.deepcopy(kwargs.get("params", {}))

    max_retries_401 = 3  # Limit retries for 401 errors
    retry_count_401 = 0

    max_retries_5xx = 4  # Limit retries for transient server errors
    retry_count_5xx = 0

    while True:
        response = rq.get(endpoint_path, headers=headers, data=timerange_data, params=params)

        # Handle 401 Unauthorized (retry up to max retries)
        if response.status_code == 401:
            if retry_count_401 >= max_retries_401:  # Fail after max retries
                log.error(f"401 Unauthorized - Max retries reached for {endpoint_path}")
                return None, None

            retry_count_401 += 1

            if configuration is not None and base_url is not None:
                log.warning(f"401 Unauthorized - refreshing token and retrying {retry_count_401}/{max_retries_401}")
                fresh_token = refresh_session_token(base_url, configuration, state if state is not None else {})
                headers["Authorization"] = f"Bearer {fresh_token}"
            else:
                log.warning(f"401 Unauthorized - Retrying {retry_count_401}/{max_retries_401}")
                time.sleep(2)
            continue

        # Handle 403 Forbidden (Skip the endpoint)
        if response.status_code == 403:
            log.info(f"403 Forbidden - Skipping {endpoint_path}")
            raise PermissionError(f"403 Forbidden: Access denied to {endpoint_path}")

        # Handle 429 Too Many Requests
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            rate_limit_reset = response.headers.get("X-Toast-RateLimit-Reset")

            wait_time = None
            if retry_after:
                wait_time = int(retry_after)
            elif rate_limit_reset:
                try:
                    reset_time = int(rate_limit_reset)
                    wait_time = max(0, reset_time - int(time.time()))
                except ValueError:
                    log.warning(f"Invalid X-Toast-RateLimit-Reset value: {rate_limit_reset}")
                    wait_time = 5  # Fallback wait time

            if wait_time:
                log.info(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue  # Retry request

        # Handle 409 Conflict: Retry without pageToken
        if response.status_code == 409:
            params.pop("pageToken", None)
            log.info(f"Received 409 error, retrying {endpoint_path} without pageToken")
            continue  # Retry without pageToken

        # Handle 400 Bad Request
        if response.status_code == 400:
            log.info(f"Bad request: {response.json().get('message')}")
            return None, None

        # Handle 204 No Content - e.g. itemFulfillments for a restaurant without an
        # active Toast RMS Pro subscription. Has no body, so response.json() below
        # would raise; treat it the same as an empty page.
        if response.status_code == 204:
            return [], None

        # Handle 5xx Server Errors: these are usually transient on Toast's side, so
        # retry with exponential backoff (10s, 20s, 40s, 80s) before giving up and
        # raising - previously any 500 crashed the whole sync immediately.
        if response.status_code >= 500:
            if retry_count_5xx >= max_retries_5xx:
                log.error(f"{response.status_code} Server Error - Max retries reached for {endpoint_path}")
                response.raise_for_status()
            wait_time = 10 * (2 ** retry_count_5xx)
            retry_count_5xx += 1
            log.warning(f"{response.status_code} Server Error, retrying {retry_count_5xx}/{max_retries_5xx} in {wait_time}s: {endpoint_path}")
            time.sleep(wait_time)
            continue

        response.raise_for_status()  # Raise error for unexpected HTTP issues

        response_page = response.json()
        response_headers = response.headers
        next_page_token = response_headers.get("Toast-Next-Page-Token")

        return response_page, next_page_token  # Return successful response
