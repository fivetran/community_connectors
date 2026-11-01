"""Optional Snowflake Cortex vision enrichment for the Met Museum connector.
Sends an object's small image and its catalog text to the Cortex Chat Completions REST API, validates the
JSON it returns against fixed sets of allowed values, and compares the depicted elements with the curator tags.
Nothing in this module runs unless enable_cortex is true.
"""

# For encoding images as base64 data URLs
import base64

# For hashing the enrichment inputs
import hashlib

# For building the request body, parsing the model's JSON answer and hashing the inputs
import json

# For validating the model name and stripping code fences from the model's answer
import re

# For waiting between retries
import time

# For timestamping each enrichment
from datetime import datetime

# For making HTTP requests to the Cortex REST API
import requests

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For writing the agreement score to a DOUBLE column
from transform import as_float

# The allowed values for each categorical field. The prompt lists them and the validator accepts only them,
# so a value outside the set is stored as null instead of reaching the destination.
SUBJECT_CATEGORIES = (
    "landscape",
    "portrait",
    "religious",
    "mythological",
    "history",
    "genre_scene",
    "still_life",
    "animal",
    "architecture",
    "marine",
    "abstract",
    "other",
)
SCENE_TYPES = ("outdoor", "indoor", "mixed", "none")
STYLE_PERIODS = (
    "medieval",
    "renaissance",
    "baroque",
    "rococo",
    "neoclassical",
    "romantic",
    "realist",
    "impressionist",
    "post_impressionist",
    "symbolist",
    "modern",
    "other",
)
COLORS = (
    "red",
    "orange",
    "yellow",
    "green",
    "blue",
    "purple",
    "pink",
    "brown",
    "black",
    "white",
    "gray",
    "gold",
    "silver",
)
MATERIALS = (
    "oil_paint",
    "canvas",
    "wood_panel",
    "paper",
    "ink",
    "watercolor",
    "tempera",
    "fresco",
    "stone",
    "marble",
    "bronze",
    "metal",
    "gold_leaf",
    "textile",
    "ceramic",
    "glass",
    "ivory",
)

# The Cortex Chat Completions endpoint, relative to the account host.
__CHAT_COMPLETIONS_PATH = "/api/v2/cortex/v1/chat/completions"

# The account host must end with this suffix, with a label before it.
__SNOWFLAKE_HOST_SUFFIX = ".snowflakecomputing.com"

# Every label before the suffix is letters, digits, hyphens or underscores, so no character such as
# ?, #, @ or : can move the request to another host while the value still ends with the suffix.
__SNOWFLAKE_HOST_PATTERN = re.compile(
    r"\A(?:[a-z0-9](?:[a-z0-9_\-]{0,61}[a-z0-9])?\.)+snowflakecomputing\.com\Z"
)

# Model names are letters, digits, dots and hyphens, so a value can never change the request's meaning.
__MODEL_NAME_PATTERN = re.compile(r"\A[a-z0-9][a-z0-9.\-]{0,63}\Z")

# Vision calls take longer than text calls; a minute and a half covers a slow cross-region call.
__REQUEST_TIMEOUT_SECONDS = 90
__MAX_RETRIES = 4
__BASE_DELAY_SECONDS = 2
__MAX_DELAY_SECONDS = 60
__RETRYABLE_STATUS_CODES = (429, 500, 502, 503, 504)
__ERROR_BODY_PREVIEW_CHARS = 300

# The answer is a short JSON object; this bounds the cost of each call.
__MAX_COMPLETION_TOKENS = 800

# Bounds on the free-text fields, so one verbose answer cannot produce an oversized row.
__MAX_DEPICTED_ELEMENTS = 20
__MAX_ELEMENT_CHARS = 60
__MAX_DESCRIPTION_CHARS = 1000

# Bumped whenever the prompt or the validator changes, so every object is enriched again.
__PROMPT_VERSION = "1"

# Words this short are never treated as plurals, so gas and bus stay as they are.
__MIN_PLURAL_LENGTH = 3

# The first 16 hex characters of a SHA-256 identify an enrichment's inputs.
__HASH_CHARS = 16

# A model can wrap its JSON in a Markdown code fence.
__CODE_FENCE = re.compile(r"\A```(?:json)?\s*(.*?)\s*```\Z", re.S)


class CortexEnrichmentError(RuntimeError):
    """A permanent Cortex failure, such as bad credentials or an unknown model, that stops the sync."""


def validate_settings(snowflake_account, snowflake_pat_token, cortex_model):
    """
    Validate the Cortex settings, which are required only when enable_cortex is true.
    Args:
        snowflake_account: the account host, for example abc12345.us-east-1.snowflakecomputing.com.
        snowflake_pat_token: a Snowflake programmatic access token.
        cortex_model: the Cortex model name.
    Raises:
        ValueError: if a value is missing, the host carries a scheme or a path or is not a Snowflake
            host, or the model name has characters outside letters, digits, dots and hyphens.
    """
    if not snowflake_account:
        raise ValueError("snowflake_account is required when enable_cortex is true.")
    if not snowflake_pat_token:
        raise ValueError("snowflake_pat_token is required when enable_cortex is true.")
    host = snowflake_account.lower()
    if "/" in host:
        raise ValueError(
            "snowflake_account must be a hostname such as abc12345.us-east-1.snowflakecomputing.com, "
            "without a scheme or a path."
        )
    if not (host.endswith(__SNOWFLAKE_HOST_SUFFIX) and len(host) > len(__SNOWFLAKE_HOST_SUFFIX)):
        raise ValueError(f"snowflake_account must end with {__SNOWFLAKE_HOST_SUFFIX}.")
    if not __SNOWFLAKE_HOST_PATTERN.match(host):
        raise ValueError(
            "snowflake_account must be a hostname of letters, digits, hyphens and underscores, "
            f"with dots between labels, ending with {__SNOWFLAKE_HOST_SUFFIX}."
        )
    if not __MODEL_NAME_PATTERN.match(cortex_model):
        raise ValueError(
            "cortex_model must be a Cortex model name such as claude-sonnet-5 "
            "(lowercase letters, digits, dots and hyphens)."
        )


def create_session(snowflake_pat_token: str) -> requests.Session:
    """
    Create the session for every Cortex call in a sync, so calls reuse pooled connections.
    It is separate from the Met session: different host, different credentials.
    Args:
        snowflake_pat_token: a Snowflake programmatic access token.
    Returns:
        The configured session. The caller closes it.
    """
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {snowflake_pat_token}",
            "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    )
    return session


def enrichment_inputs(obj: dict, terms: list, model: str) -> dict:
    """
    Collect everything an enrichment depends on: the image URL, the catalog text, the curator tags,
    the model and the prompt version. The hash covers the URL, not the image bytes, so a file the
    Met replaces at the same URL is not enriched again.
    Args:
        obj: the object detail response.
        terms: the object's curator tag terms.
        model: the Cortex model name.
    Returns:
        A dictionary of the inputs.
    """
    return {
        "image": obj.get("primaryImageSmall"),
        "title": obj.get("title"),
        "artist": obj.get("artistDisplayName"),
        "object_date": obj.get("objectDate"),
        "medium": obj.get("medium"),
        "classification": obj.get("classification"),
        "tags": terms,
        "model": model,
        "prompt_version": __PROMPT_VERSION,
    }


def enrichment_hash(inputs: dict) -> str:
    """
    Hash the enrichment inputs. An object whose hash is unchanged since its last enrichment is not
    sent to Cortex again.
    Args:
        inputs: the dictionary from enrichment_inputs.
    Returns:
        The first 16 hex characters of the SHA-256 of the inputs.
    """
    canonical = json.dumps(inputs, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:__HASH_CHARS]


def enrichment_stamp(input_hash: str, enriched_at: datetime) -> str:
    """
    Stamp one enrichment's rows. The input hash alone is not unique: inputs that change and change
    back give a hash that depicted-element rows from the earlier enrichment still carry, and those
    rows would look current. The time of the enrichment makes the stamp unique.
    Args:
        input_hash: the enrichment hash of the inputs.
        enriched_at: the time of this enrichment.
    Returns:
        The first 16 hex characters of the SHA-256 of both.
    """
    canonical = f"{input_hash}|{enriched_at.isoformat()}"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:__HASH_CHARS]


def build_prompt(inputs: dict) -> str:
    """
    Build the instruction text sent with the image. The allowed values come from the same tuples
    the validator uses.
    Args:
        inputs: the dictionary from enrichment_inputs.
    Returns:
        The prompt.
    """
    return (
        "You are cataloguing a public-domain museum object from its photograph and catalog record.\n"
        f"Title: {inputs['title'] or 'unknown'}\n"
        f"Artist: {inputs['artist'] or 'unknown'}\n"
        f"Date: {inputs['object_date'] or 'unknown'}\n"
        f"Medium: {inputs['medium'] or 'unknown'}\n"
        f"Classification: {inputs['classification'] or 'unknown'}\n\n"
        "Answer with one JSON object and nothing else, with these keys:\n"
        f"- subject_category: one of {', '.join(SUBJECT_CATEGORIES)}\n"
        f"- scene_type: one of {', '.join(SCENE_TYPES)}\n"
        f"- style_period: one of {', '.join(STYLE_PERIODS)}\n"
        f"- dominant_colors: a list of up to five of {', '.join(COLORS)}\n"
        f"- materials_visible: a list drawn from {', '.join(MATERIALS)}\n"
        "- depicted_elements: a list of up to 20 short lowercase nouns for things visible in the image, "
        "such as cypress, wheat field, sky, woman, horse\n"
        "- visual_description: two or three plain sentences describing what is visible, written as "
        "alt text for someone who cannot see the image\n"
        "Describe only what the image shows. Do not guess at facts the image does not show."
    )


def build_request(model: str, prompt: str, image_bytes: bytes, content_type: str) -> dict:
    """
    Build the Chat Completions request body: the prompt and the image as a base64 data URL.
    Args:
        model: the Cortex model name.
        prompt: the instruction text.
        image_bytes: the image file.
        content_type: the image's media type, for example image/jpeg.
    Returns:
        The request body.
    """
    encoded = base64.b64encode(image_bytes).decode("ascii")
    return {
        "model": model,
        "max_completion_tokens": __MAX_COMPLETION_TOKENS,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{content_type};base64,{encoded}"},
                    },
                ],
            }
        ],
    }


def retry_delay(attempt: int) -> float:
    """
    Compute the exponential backoff before the next Cortex attempt.
    Args:
        attempt: the attempt that just failed, starting at 1.
    Returns:
        The number of seconds to wait, capped at __MAX_DELAY_SECONDS.
    """
    return min(__BASE_DELAY_SECONDS * 2 ** (attempt - 1), __MAX_DELAY_SECONDS)


def answer_text(payload):
    """
    Return the text of the first choice of a Chat Completions response, or None when the response
    does not have that shape.
    Args:
        payload: the decoded response body.
    Returns:
        The text, or None.
    """
    choices = payload.get("choices") if isinstance(payload, dict) else None
    first = choices[0] if isinstance(choices, list) and choices else None
    message = first.get("message") if isinstance(first, dict) else None
    content = message.get("content") if isinstance(message, dict) else None
    return content if isinstance(content, str) else None


def call_cortex(session: requests.Session, snowflake_account: str, body: dict):
    """
    Send one Chat Completions request. HTTP 429 and 5xx, connection errors, timeouts and invalid
    JSON are retried; any other 4xx stops the sync, because it means the settings are wrong.
    Args:
        session: the Cortex session.
        snowflake_account: the account host.
        body: the request body.
    Returns:
        The text of the first choice, or None when the response had no text or every attempt failed
        transiently.
    Raises:
        CortexEnrichmentError: on HTTP 400, 401, 403, 404 or another non-retryable status.
    """
    url = f"https://{snowflake_account}{__CHAT_COMPLETIONS_PATH}"
    failure = None
    for attempt in range(1, __MAX_RETRIES + 1):
        try:
            response = session.post(url, json=body, timeout=__REQUEST_TIMEOUT_SECONDS)
            if response.status_code == 200:
                return answer_text(response.json())
            if response.status_code not in __RETRYABLE_STATUS_CODES:
                raise CortexEnrichmentError(
                    f"Cortex returned HTTP {response.status_code}: "
                    f"{response.text[:__ERROR_BODY_PREVIEW_CHARS]}. Check snowflake_account, "
                    "snowflake_pat_token, and that cortex_model is available to the token's role "
                    "and supports images."
                )
            failure = f"HTTP {response.status_code}"
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as error:
            failure = f"{type(error).__name__}: {error}"
        except requests.exceptions.JSONDecodeError as error:
            failure = f"invalid JSON: {error}"
        except requests.exceptions.RequestException as error:
            raise CortexEnrichmentError(f"Cortex request failed: {error}") from error
        if attempt < __MAX_RETRIES:
            delay = retry_delay(attempt)
            log.warning(f"Cortex request failed ({failure}); retrying in {delay:.0f}s")
            time.sleep(delay)
    log.warning(f"Cortex request failed after {__MAX_RETRIES} attempts: {failure}")
    return None


def parse_answer(content):
    """
    Parse the model's answer as a JSON object, removing a Markdown code fence if there is one.
    Args:
        content: the text of the first choice, or None.
    Returns:
        The parsed dictionary, or None when the answer is not a JSON object.
    """
    if not isinstance(content, str):
        return None
    stripped = content.strip()
    fenced = __CODE_FENCE.match(stripped)
    if fenced:
        stripped = fenced.group(1)
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def normalize_token(value):
    """
    Lowercase a categorical answer and turn spaces and hyphens into underscores, so Post-Impressionist
    matches post_impressionist.
    Args:
        value: the raw value.
    Returns:
        The normalized string, or None when the value is not a string.
    """
    if not isinstance(value, str):
        return None
    return re.sub(r"[\s\-]+", "_", value.strip().lower())


def pick_one(value, allowed: tuple):
    """
    Accept a single categorical value only when it is in the allowed set.
    Args:
        value: the raw value.
        allowed: the allowed values.
    Returns:
        The value, or None when it is outside the set.
    """
    token = normalize_token(value)
    return token if token in allowed else None


def pick_many(value, allowed: tuple):
    """
    Keep the values of a list that are in the allowed set, in order and without duplicates.
    Args:
        value: the raw list.
        allowed: the allowed values.
    Returns:
        A list, or None when no value was accepted.
    """
    if not isinstance(value, list):
        return None
    tokens = (normalize_token(item) for item in value)
    kept = list(dict.fromkeys(token for token in tokens if token in allowed))
    return kept or None


def clean_elements(value) -> list:
    """
    Normalize the depicted elements: lowercase, trimmed, bounded in length and count, no duplicates.
    Args:
        value: the raw list.
    Returns:
        A list of strings, empty when the value is not a list.
    """
    if not isinstance(value, list):
        return []
    elements = []
    for item in value:
        if not isinstance(item, str):
            continue
        element = " ".join(item.lower().split())[:__MAX_ELEMENT_CHARS]
        if element and element not in elements:
            elements.append(element)
        if len(elements) == __MAX_DEPICTED_ELEMENTS:
            break
    return elements


def validate_answer(answer: dict) -> dict:
    """
    Keep only the accepted values of a parsed answer.
    Args:
        answer: the parsed JSON object.
    Returns:
        A dictionary with every field; a field that failed validation is None, and
        depicted_elements is a possibly empty list.
    """
    description = answer.get("visual_description")
    description = " ".join(description.split()) if isinstance(description, str) else ""
    return {
        "subject_category": pick_one(answer.get("subject_category"), SUBJECT_CATEGORIES),
        "scene_type": pick_one(answer.get("scene_type"), SCENE_TYPES),
        "style_period": pick_one(answer.get("style_period"), STYLE_PERIODS),
        "dominant_colors": pick_many(answer.get("dominant_colors"), COLORS),
        "materials_visible": pick_many(answer.get("materials_visible"), MATERIALS),
        "depicted_elements": clean_elements(answer.get("depicted_elements")),
        "visual_description": description[:__MAX_DESCRIPTION_CHARS] or None,
    }


def singular(word: str) -> str:
    """
    Reduce a regular English plural to its singular, so the curator tag Cypresses matches the
    element cypress. Irregular plurals such as women are left as they are.
    Args:
        word: a lowercase word.
    Returns:
        The singular form.
    """
    if len(word) <= __MIN_PLURAL_LENGTH:
        return word
    if word.endswith("ies"):
        return word[:-3] + "y"
    if word.endswith(("sses", "shes", "ches", "xes", "zes")):
        return word[:-2]
    if word.endswith(("ss", "us", "is")):
        return word
    return word[:-1] if word.endswith("s") else word


def phrase_words(phrase: str) -> set:
    """
    Split a tag or an element into its singular, case-folded words.
    Args:
        phrase: a curator tag or a depicted element.
    Returns:
        A set of words.
    """
    return {singular(word) for word in phrase.casefold().split()}


def curator_agreement(elements: list, terms: list):
    """
    Compare the model's depicted elements with the curator's subject tags, ignoring case and regular
    plural endings. A tag is matched when the words of the tag and of an element are the same, or
    when either one contains all the words of the other (the tag Trees matches the element cypress
    trees, and the tag Cypresses matches cypress).
    Args:
        elements: the validated depicted elements.
        terms: the curator tag terms.
    Returns:
        A tuple (agreement, matched, missed): the share of tags matched, or None when the object
        has no tags, and the counts of matched and unmatched tags.
    """
    if not terms:
        return None, 0, 0
    element_words = [words for words in (phrase_words(element) for element in elements) if words]
    matched = 0
    for term in terms:
        words = phrase_words(term)
        if words and any(words <= element or element <= words for element in element_words):
            matched += 1
    return matched / len(terms), matched, len(terms) - matched


def has_accepted_value(validated: dict) -> bool:
    """
    Report whether at least one field of a validated answer was accepted. The model marker is
    written only then, so a row never claims an enrichment that produced nothing.
    Args:
        validated: the dictionary from validate_answer.
    Returns:
        True when any field holds a value.
    """
    return any(validated[key] for key in validated)


def enrich(
    session: requests.Session,
    snowflake_account: str,
    inputs: dict,
    image_bytes: bytes,
    content_type: str,
):
    """
    Run one vision enrichment and validate the answer.
    Args:
        session: the Cortex session.
        snowflake_account: the account host.
        inputs: the dictionary from enrichment_inputs.
        image_bytes: the image file.
        content_type: the image's media type.
    Returns:
        The validated answer, or None when the call failed transiently or no field was accepted.
    Raises:
        CortexEnrichmentError: on a permanent Cortex failure.
    """
    body = build_request(inputs["model"], build_prompt(inputs), image_bytes, content_type)
    answer = parse_answer(call_cortex(session, snowflake_account, body))
    if answer is None:
        return None
    validated = validate_answer(answer)
    return validated if has_accepted_value(validated) else None


def build_visual_attribute_row(
    object_id: int, validated: dict, terms: list, model: str, stamp: str, enriched_at: datetime
) -> dict:
    """
    Build the object_visual_attribute row for one enriched object.
    Args:
        object_id: the object id.
        validated: the dictionary from validate_answer, with at least one accepted value.
        terms: the object's curator tag terms.
        model: the Cortex model that produced the answer.
        stamp: the enrichment stamp, which the object's depicted-element rows also carry.
        enriched_at: the time of this enrichment.
    Returns:
        A dictionary whose keys match the object_visual_attribute table columns.
    """
    agreement, matched, missed = curator_agreement(validated["depicted_elements"], terms)
    return {
        "object_id": object_id,
        "subject_category": validated["subject_category"],
        "scene_type": validated["scene_type"],
        "style_period": validated["style_period"],
        "dominant_colors": validated["dominant_colors"],
        "materials_visible": validated["materials_visible"],
        "visual_description": validated["visual_description"],
        "depicted_element_count": len(validated["depicted_elements"]),
        "curator_tag_agreement": as_float(agreement),
        "curator_tags_matched": matched,
        "curator_tags_missed": missed,
        "enrichment_model": model,
        "enrichment_hash": stamp,
        "enriched_at": enriched_at,
    }


def build_depicted_element_rows(object_id: int, validated: dict, stamp: str) -> list:
    """
    Build the object_depicted_element rows for one enriched object.
    Args:
        object_id: the object id.
        validated: the dictionary from validate_answer.
        stamp: the enrichment stamp of the object_visual_attribute row.
    Returns:
        A list of dictionaries whose keys match the object_depicted_element table columns.
    """
    return [
        {"object_id": object_id, "element": element, "enrichment_hash": stamp}
        for element in validated["depicted_elements"]
    ]
