"""Optional Snowflake Cortex enrichment for the Semantic Scholar connector.

This module holds everything specific to Cortex: settings validation, the dedicated
HTTP session, the inference call, response parsing and validation, and the
per-paper enrichment orchestration. It never reads the connector configuration:
the connector resolves every setting and default and passes plain values in, so
the two modules cannot disagree about what "default" means. When enable_cortex is
"false" nothing here runs, no Snowflake credential is read, and the cortex_*
columns are null.

A transient failure (connection error, timeout, HTTP 429 or 5xx) is retried with
exponential backoff and then degrades that one paper's enrichment to null. Any
other 4xx response means the account, token, or model is wrong, so it fails the
sync immediately rather than nulling every enrichment while the sync reports
success.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For parsing the JSON payload returned inside the streamed inference response
import json

# For the backoff delay between retries and the pacing delay between calls
import time

# For issuing the inference request and classifying transport failures
import requests

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

__INFERENCE_ENDPOINT = "/api/v2/cortex/inference:complete"

# A Snowflake account hostname is a label under this suffix. The leading dot
# matters: without it a lookalike such as attacker-snowflakecomputing.com would
# pass and receive the token.
__SNOWFLAKE_HOST_SUFFIX = ".snowflakecomputing.com"

__ALLOWED_MODELS = (
    "claude-sonnet-5",
    "claude-sonnet-4-6",
    "mistral-large2",
    "llama3.1-70b",
    "llama3.1-8b",
)

# The three assessments the model is asked for, and the only values accepted back.
# The prompt is built from these same tuples, so the request and the validation
# cannot drift apart.
__ALLOWED_RESEARCH_IMPACT = ("high", "medium", "low")
__ALLOWED_TECHNICAL_DOMAIN = (
    "NLP",
    "CV",
    "ML",
    "Systems",
    "Theory",
    "Biology",
    "Chemistry",
    "Physics",
    "Medicine",
    "Social",
    "Other",
)
__ALLOWED_ACCESSIBILITY_LEVEL = ("beginner", "intermediate", "advanced")

# Only the start of a long abstract is sent; it is enough for a classification
# and keeps the per-call token cost flat.
__ABSTRACT_MAX_CHARS = 500
__MAX_OUTPUT_TOKENS = 150

# Paced so a long enrichment run does not hammer the inference endpoint.
__RATE_LIMIT_DELAY_SECONDS = 0.2

__MAX_RETRIES = 3
__BASE_DELAY_SECONDS = 2
__RETRYABLE_STATUS_CODES = [429, 500, 502, 503, 504]


def validate_settings(account: str, pat_token: str, model: str) -> None:
    """
    Validate the resolved Cortex settings.

    Only called when enable_cortex is "true" -- a data-only sync must not require
    a Snowflake account at all.

    Args:
        account: Snowflake account hostname, for example <locator>.snowflakecomputing.com
        pat_token: Snowflake programmatic access token
        model: Cortex model name

    Raises:
        ValueError: if any setting is missing or invalid.
    """
    if not account:
        raise ValueError("snowflake_account is required when enable_cortex is true")

    # A scheme prefix here would produce "https://https://..." when the URL is built.
    if account.startswith(("http://", "https://")):
        raise ValueError(
            f"snowflake_account must be a hostname (no scheme prefix), got: {account!r}"
        )
    if any(character in account for character in "/?#@ \t"):
        raise ValueError(f"snowflake_account must be a bare hostname, got: {account!r}")

    # Require a real label before the suffix so neither the bare suffix nor a
    # lookalike domain is accepted as the place to send the token.
    has_suffix = account.endswith(__SNOWFLAKE_HOST_SUFFIX)
    has_label = len(account) > len(__SNOWFLAKE_HOST_SUFFIX)
    if not has_suffix or not has_label:
        raise ValueError(
            f"snowflake_account must be a hostname under 'snowflakecomputing.com', "
            f"for example '<locator>.snowflakecomputing.com', got: {account!r}"
        )

    if not pat_token:
        raise ValueError("snowflake_pat_token is required when enable_cortex is true")

    if model not in __ALLOWED_MODELS:
        raise ValueError(f"cortex_model must be one of {list(__ALLOWED_MODELS)}, got: {model!r}")


def create_session(pat_token: str) -> requests.Session:
    """
    Create a requests session used ONLY for Cortex inference.

    Deliberately not shared with the data-source session. Connection pooling is
    per-host and Cortex is a different host from the Semantic Scholar API, and a
    Snowflake bearer token has no business living on a session that talks to a
    third-party API.

    Args:
        pat_token: Snowflake programmatic access token

    Returns:
        requests.Session carrying the Snowflake bearer token
    """
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {pat_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    )
    return session


def parse_streaming_response(response: requests.Response) -> str:
    """
    Parse the server-sent-events streaming response from the Cortex inference API.

    Args:
        response: requests.Response with SSE content

    Returns:
        Concatenated content string from all SSE data events
    """
    content = ""
    for line in response.text.split("\n"):
        if line.startswith("data: "):
            try:
                data = json.loads(line[6:])
                if data.get("choices"):
                    # Use `or {}` rather than `.get(k, {})`: dict.get's default applies
                    # only when the key is absent. A key present with value None returns
                    # None and the chained .get() raises AttributeError.
                    content += (data["choices"][0].get("delta") or {}).get("content", "")
            except (json.JSONDecodeError, KeyError, IndexError):
                continue
    return content


def extract_json_from_content(content: str) -> dict | None:
    """
    Extract a JSON object from a string that may contain surrounding text.

    The model is asked for bare JSON but is not guaranteed to comply, so the
    outermost brace pair is located rather than assuming the whole string parses.

    Args:
        content: string potentially containing a JSON object

    Returns:
        parsed dictionary if JSON found, or None
    """
    if "{" in content and "}" in content:
        start = content.find("{")
        end = content.rfind("}") + 1
        try:
            return json.loads(content[start:end])
        except json.JSONDecodeError:
            return None
    return None


def is_permanent_client_error(status: int | None) -> bool:
    """
    Report whether an HTTP status means the request can never succeed as sent.

    429 is excluded: it is rate limiting, which clears on its own and is retried.

    Args:
        status: HTTP status code, or None when no response was received

    Returns:
        True for any 4xx other than 429
    """
    if status is None:
        return False
    return 400 <= status < 500 and status != 429


def build_prompt(title: str, abstract_text: str | None) -> str:
    """
    Build the single-call assessment prompt for one paper.

    Args:
        title: paper title
        abstract_text: paper abstract or None

    Returns:
        prompt string asking for a bare JSON object with the three assessments
    """
    context = f"Title: {title}"
    if abstract_text:
        context += f"\nAbstract: {abstract_text[:__ABSTRACT_MAX_CHARS]}"
    return (
        "Analyze this academic paper and respond ONLY with a JSON object in this exact format:\n"
        f'{{"research_impact": "{"|".join(__ALLOWED_RESEARCH_IMPACT)}", '
        f'"technical_domain": "{"|".join(__ALLOWED_TECHNICAL_DOMAIN)}", '
        f'"accessibility_level": "{"|".join(__ALLOWED_ACCESSIBILITY_LEVEL)}"}}\n\n'
        f"{context}\n\nJSON:"
    )


def call_enrich(
    cortex_session: requests.Session,
    account: str,
    title: str,
    abstract_text: str | None,
    model: str,
    timeout: int,
) -> dict | None:
    """
    Call the Cortex inference API to assess a single paper.

    Asks for research impact, technical domain, and accessibility level in one
    call rather than three, because per-paper call count is the cost driver.

    Transient transport failures and retryable status codes are retried with
    exponential backoff; once the budget is exhausted the paper still syncs with
    null cortex_* columns. A permanent client error (any 4xx other than 429) is
    raised immediately, because it will fail identically for every paper.

    Args:
        cortex_session: session created by create_session()
        account: Snowflake account hostname
        title: paper title
        abstract_text: paper abstract or None
        model: Cortex LLM model name
        timeout: API request timeout in seconds

    Returns:
        dict of assessments, or None if every attempt failed transiently

    Raises:
        RuntimeError: on a permanent client error from the inference API
    """
    url = f"https://{account}{__INFERENCE_ENDPOINT}"
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": build_prompt(title, abstract_text)}],
        "temperature": 0.0,
        "max_tokens": __MAX_OUTPUT_TOKENS,
    }

    for attempt in range(__MAX_RETRIES):
        try:
            # Headers live on the dedicated session, not on each call.
            response = cortex_session.post(url, json=payload, timeout=timeout)
            response.raise_for_status()
            return extract_json_from_content(parse_streaming_response(response))

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            error_type = (
                "Timeout" if isinstance(e, requests.exceptions.Timeout) else "Connection error"
            )
            if attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(f"Cortex {error_type}, retrying in {delay}s: {e}")
                time.sleep(delay)
            else:
                log.warning(
                    f"Cortex {error_type} after {__MAX_RETRIES} attempts, "
                    f"skipping enrichment for: {title[:60]}"
                )
                return None

        except requests.exceptions.RequestException as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if is_permanent_client_error(status):
                raise RuntimeError(
                    f"Cortex inference request rejected (HTTP {status}); check "
                    "snowflake_account, snowflake_pat_token and cortex_model"
                ) from e
            if status in __RETRYABLE_STATUS_CODES and attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(f"Cortex HTTP {status}, retrying in {delay}s (attempt {attempt + 1})")
                time.sleep(delay)
            else:
                log.warning(
                    f"Cortex request failed ({type(e).__name__}, HTTP {status}) after "
                    f"{attempt + 1} attempt(s), skipping enrichment for: {title[:60]}"
                )
                return None

    return None


def enrich_paper(
    cortex_session: requests.Session, account: str, model: str, timeout: int, record: dict
) -> dict:
    """
    Produce the cortex_* column values for a single paper record.

    Always returns the full set of enrichment keys so the destination column set
    is identical whether or not the assessment succeeded. Each returned value is
    checked against the documented set for its column; anything else is stored
    as null, and cortex_model_used is set only when at least one value was
    accepted, so a null-enriched row never looks enriched.

    Args:
        cortex_session: session created by create_session()
        account: Snowflake account hostname
        model: Cortex LLM model name
        timeout: API request timeout in seconds
        record: raw paper object from the API

    Returns:
        dict with cortex_* fields for the papers table
    """
    enrichment = {
        "cortex_research_impact": None,
        "cortex_technical_domain": None,
        "cortex_accessibility_level": None,
        "cortex_model_used": None,
    }

    title = (record.get("title") or "").strip()
    if not title:
        return enrichment

    result = call_enrich(cortex_session, account, title, record.get("abstract"), model, timeout)
    time.sleep(__RATE_LIMIT_DELAY_SECONDS)
    if not isinstance(result, dict):
        return enrichment

    accepted = {}
    rejected = []
    for column, key, allowed in (
        ("cortex_research_impact", "research_impact", __ALLOWED_RESEARCH_IMPACT),
        ("cortex_technical_domain", "technical_domain", __ALLOWED_TECHNICAL_DOMAIN),
        ("cortex_accessibility_level", "accessibility_level", __ALLOWED_ACCESSIBILITY_LEVEL),
    ):
        value = str(result.get(key) or "").strip()
        if value in allowed:
            accepted[column] = value
        elif value:
            rejected.append(f"{key}={value!r}")

    if rejected:
        log.warning(
            f"Cortex returned values outside the documented sets for '{title[:60]}', "
            f"stored as null: {', '.join(rejected)}"
        )
    if accepted:
        enrichment.update(accepted)
        enrichment["cortex_model_used"] = model
    return enrichment
