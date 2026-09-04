"""Optional Snowflake Cortex enrichment for the Semantic Scholar connector.

This module holds everything specific to Cortex: configuration validation, the
dedicated HTTP session, the inference call, response parsing, and the per-paper
enrichment orchestration. The connector works fully without it -- when
enable_cortex is "false" nothing here is imported at runtime beyond the module
itself, no Snowflake credential is read, and the cortex_* columns are null.

Keeping it separate means the data path and the inference path can be read, and
broken, independently.
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

# Public defaults. The connector reads these so the two modules cannot disagree
# about what "default" means.
DEFAULT_MODEL = "claude-sonnet-5"
DEFAULT_TIMEOUT = 30

# Enrichment is billed inference, so it is bounded twice. A per-sync cap alone is
# not a spend control: it multiplies by sync frequency, and a connector on a
# 15-minute schedule runs 96 times a day. The daily cap is the ceiling that
# actually holds, because it is carried in connector state across syncs.
DEFAULT_MAX_ENRICHMENTS = 3
DEFAULT_MAX_ENRICHMENTS_PER_DAY = 15

ALLOWED_MODELS = frozenset(
    {
        "claude-sonnet-5",
        "claude-sonnet-4-6",
        "mistral-large2",
        "llama3.1-70b",
        "llama3.1-8b",
    }
)

__INFERENCE_ENDPOINT = "/api/v2/cortex/inference:complete"

# Paced so a long enrichment run does not hammer the inference endpoint.
__RATE_LIMIT_DELAY = 0.2

__MAX_RETRIES = 3
__BASE_DELAY_SECONDS = 2
__RETRYABLE_STATUS_CODES = [429, 500, 502, 503, 504]


def validate_configuration(configuration: dict) -> None:
    """
    Validate the Cortex-specific configuration parameters.

    Only called when enable_cortex is "true" -- a data-only sync must not require
    a Snowflake account at all.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: if any required Cortex configuration parameter is missing or invalid.
    """
    account = configuration.get("snowflake_account", "")
    if not account:
        raise ValueError("snowflake_account is required when enable_cortex is true")

    # A scheme prefix here would produce "https://https://..." when the URL is built.
    if account.startswith(("http://", "https://")):
        raise ValueError(
            f"snowflake_account must be a hostname (no scheme prefix), got: {account!r}"
        )
    if not account.endswith("snowflakecomputing.com"):
        raise ValueError(
            f"snowflake_account must end with 'snowflakecomputing.com', got: {account!r}"
        )

    if not configuration.get("snowflake_pat_token", ""):
        raise ValueError("snowflake_pat_token is required when enable_cortex is true")

    model = configuration.get("cortex_model", DEFAULT_MODEL)
    if model not in ALLOWED_MODELS:
        raise ValueError(f"cortex_model must be one of {sorted(ALLOWED_MODELS)}, got: {model!r}")


def create_session(configuration: dict) -> requests.Session:
    """
    Create a requests session used ONLY for Cortex inference.

    Deliberately not shared with the data-source session. Connection pooling is
    per-host and Cortex is a different host from the Semantic Scholar API, and a
    Snowflake bearer token has no business living on a session that talks to a
    third-party API.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Returns:
        requests.Session carrying the Snowflake bearer token
    """
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {configuration['snowflake_pat_token']}",
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
    exponential backoff. A failed enrichment returns None and is not fatal: the
    paper still syncs with null cortex_* columns.

    Args:
        cortex_session: session created by create_session()
        account: Snowflake account hostname
        title: paper title
        abstract_text: paper abstract or None
        model: Cortex LLM model name
        timeout: API request timeout in seconds

    Returns:
        dict of assessments, or None if every attempt failed
    """
    url = f"https://{account}{__INFERENCE_ENDPOINT}"

    context = f"Title: {title}"
    if abstract_text:
        context += f"\nAbstract: {abstract_text[:500]}"

    prompt = (
        "Analyze this academic paper and respond ONLY with a JSON object in this exact format:\n"
        '{"research_impact": "high|medium|low", '
        '"technical_domain": '
        '"NLP|CV|ML|Systems|Theory|Biology|Chemistry|Physics|Medicine|Social|Other", '
        '"accessibility_level": "beginner|intermediate|advanced"}\n\n'
        f"{context}\n\nJSON:"
    )

    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0,
        "max_tokens": 150,
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
            retryable = status in __RETRYABLE_STATUS_CODES
            if retryable and attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(f"Cortex HTTP {status}, retrying in {delay}s (attempt {attempt + 1})")
                time.sleep(delay)
            else:
                log.warning(f"Cortex enrichment API error, skipping paper: {e}")
                return None

    return None


def enrich_paper(cortex_session: requests.Session, configuration: dict, record: dict) -> dict:
    """
    Produce the cortex_* column values for a single paper record.

    Always returns the full set of enrichment keys so the destination column set
    is identical whether or not the assessment succeeded.

    Args:
        cortex_session: session created by create_session()
        configuration: a dictionary that holds the configuration settings for the connector.
        record: raw paper object from the API

    Returns:
        dict with cortex_* fields for the papers table
    """
    account = configuration.get("snowflake_account")
    model = configuration.get("cortex_model", DEFAULT_MODEL)
    timeout = int(configuration.get("cortex_timeout", str(DEFAULT_TIMEOUT)))

    enrichment = {
        "cortex_research_impact": None,
        "cortex_technical_domain": None,
        "cortex_accessibility_level": None,
        "cortex_model_used": model,
    }

    title = record.get("title") or ""
    if not title:
        return enrichment

    result = call_enrich(cortex_session, account, title, record.get("abstract"), model, timeout)

    if result:
        enrichment["cortex_research_impact"] = result.get("research_impact")
        enrichment["cortex_technical_domain"] = result.get("technical_domain")
        enrichment["cortex_accessibility_level"] = result.get("accessibility_level")

    time.sleep(__RATE_LIMIT_DELAY)
    return enrichment
