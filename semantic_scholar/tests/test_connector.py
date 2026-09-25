"""Tests for the Semantic Scholar connector.

The tests target the properties of this source that a generic connector would get
wrong, and the behaviours the connector promises in its README:

  - 'year' and 'abstract' are renamed to publication_year and paper_abstract
  - externalIds (nested object) is flattened to scalar columns
  - openAccessPdf (nullable object) is flattened to two scalar columns
  - authors (array of objects) are extracted to a separate paper_authors table,
    and an author with a null authorId is skipped rather than written
  - sparse or null fields produce None rather than KeyError
  - HTTP 429 is retried with backoff; HTTP 400 fails immediately
  - the bulk endpoint ignores 'limit' and returns full pages, so a per-sync cap
    stops mid-page and the next sync resumes inside the same page without
    skipping or duplicating a record, even when the page shifted underneath it
  - pagination state is tied to the search query that produced it
  - only search_query is required; a placeholder in any optional key is unset
  - Cortex enrichment is off by default, bounded per sync and per day, retried on
    transient failures, fails the sync on a permanent 4xx, and stores only the
    documented values

Run from the repository root:  python -m pytest semantic_scholar/tests -q
"""

# For building fake API responses and reading the shipped configuration.json
import json

# For locating connector.py and configuration.json relative to this file
import sys
from pathlib import Path

# For the UTC-day key the daily enrichment ceiling is tracked under
from datetime import datetime, timezone

# For stubbing HTTP sessions and the SDK operations without any network
from unittest.mock import MagicMock, patch

# For fixtures and parametrised cases
import pytest

# For constructing the exception types the connector classifies
import requests

# For the SDK logger, which needs a level set before update() can log
from fivetran_connector_sdk import Logging

# The connector modules live one directory up from this file
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import connector as c  # noqa: E402
import cortex  # noqa: E402
import transform  # noqa: E402

CONNECTOR_DIR = Path(__file__).resolve().parent.parent


@pytest.fixture(autouse=True)
def sdk_log_level():
    """Give the SDK logger a level for every test; it is None until a real sync starts."""
    previous = Logging.LOG_LEVEL
    Logging.LOG_LEVEL = Logging.Level.WARNING
    yield
    Logging.LOG_LEVEL = previous


# ----------------------------------------------------------------- fixtures
def _paper(paper_id="649def34f8be52c8b66281af98ae884c09aef38d", **overrides):
    """Build a minimal but realistic Semantic Scholar paper record."""
    base = {
        "paperId": paper_id,
        "title": "Attention Is All You Need",
        "year": 2017,
        "abstract": "The dominant sequence transduction models are based on complex recurrent...",
        "authors": [
            {"authorId": "1234", "name": "Ashish Vaswani"},
            {"authorId": "5678", "name": "Noam Shazeer"},
        ],
        "externalIds": {
            "DOI": "10.48550/arXiv.1706.03762",
            "ArXiv": "1706.03762",
            "MAG": "2963403868",
            "PubMed": None,
            "DBLP": "journals/corr/VaswaniSPUJGKP17",
            "ACL": None,
            "CorpusId": 13756489,
        },
        "openAccessPdf": {
            "url": "https://arxiv.org/pdf/1706.03762",
            "status": "GREEN",
        },
        "referenceCount": 38,
        "citationCount": 95000,
        "publicationDate": "2017-06-12",
        "publicationTypes": ["JournalArticle"],
    }
    base.update(overrides)
    return base


def _sparse_paper():
    """A record with only the primary key; every optional field absent or null."""
    return {"paperId": "abc123"}


def _enrichment():
    """A fully populated enrichment dict as enrich_paper() would return it."""
    return {
        "cortex_research_impact": "high",
        "cortex_technical_domain": "NLP",
        "cortex_accessibility_level": "advanced",
        "cortex_model_used": "claude-sonnet-5",
    }


def _data_only_config(**overrides):
    """A configuration that exercises the data path with Cortex off."""
    config = {
        "search_query": "test",
        "enable_cortex": "false",
        "max_records_per_sync": "10",
        "batch_size": "10",
    }
    config.update(overrides)
    return config


def _cortex_config(**overrides):
    """A configuration with Cortex on and fake, clearly non-real credentials."""
    config = _data_only_config(
        enable_cortex="true",
        snowflake_account="testaccount.snowflakecomputing.com",
        snowflake_pat_token="FAKE-PAT-TOKEN-FOR-TESTS-ONLY-NOT-A-REAL-CREDENTIAL",
        cortex_model="claude-sonnet-5",
    )
    config.update(overrides)
    return config


def _run_update(config, state, fetch, cortex_session=None, enrich=None):
    """Run update() with the network and the SDK operations stubbed out.

    Returns the final checkpointed state and the list of (table, row) upserts.
    """
    upserts = []

    def record_upsert(table, data):
        upserts.append((table, data))

    patches = [
        patch.object(c, "fetch_bulk_page", side_effect=fetch),
        patch.object(c.op, "upsert", side_effect=record_upsert),
        patch.object(c.op, "checkpoint"),
    ]
    if cortex_session is not None:
        patches.append(patch.object(cortex, "create_session", return_value=cortex_session))
    if enrich is not None:
        patches.append(patch.object(cortex, "enrich_paper", side_effect=enrich))
    active = [p.start() for p in patches]
    try:
        c.update(config, state)
        checkpoint = active[2]
        final_state = checkpoint.call_args_list[-1].kwargs["state"] if checkpoint.called else state
    finally:
        for p in patches:
            p.stop()
    return final_state, upserts


def _paper_ids(upserts):
    """The paper_id of every papers-table upsert, in order."""
    return [data["paper_id"] for table, data in upserts if table == "papers"]


# --------------------------------------------------- reserved-word renames
def test_year_is_renamed_to_publication_year():
    """API field 'year' is renamed; it is a temporal keyword in several SQL engines."""
    row = c.flatten_paper(_paper())
    assert "publication_year" in row
    assert row["publication_year"] == 2017
    assert "year" not in row


def test_abstract_is_renamed_to_paper_abstract():
    """API field 'abstract' is renamed to avoid tool and engine collisions."""
    row = c.flatten_paper(_paper())
    assert "paper_abstract" in row
    assert "transduction" in (row["paper_abstract"] or "").lower()
    assert "abstract" not in row


# ------------------------------------------------- nested flatten: externalIds
def test_external_ids_are_flattened_to_scalar_columns():
    """Each externalIds key lands in its own external_id_* column."""
    row = c.flatten_paper(_paper())
    assert row["external_id_doi"] == "10.48550/arXiv.1706.03762"
    assert row["external_id_arxiv"] == "1706.03762"
    assert row["external_id_dblp"] == "journals/corr/VaswaniSPUJGKP17"
    assert row["external_id_pubmed"] is None
    assert row["external_id_acl"] is None
    assert row["external_id_corpus_id"] == "13756489"


def test_missing_external_ids_object_becomes_null_columns():
    """An absent externalIds field yields None in every external_id_* column."""
    row = c.flatten_paper({"paperId": "x", "title": "T"})
    assert row["external_id_doi"] is None
    assert row["external_id_arxiv"] is None
    assert row["external_id_corpus_id"] is None


def test_null_external_ids_object_becomes_null_columns():
    """An explicit null externalIds must not raise on the chained lookup."""
    row = c.flatten_paper({"paperId": "x", "externalIds": None})
    assert row["external_id_doi"] is None


# ----------------------------------------------- nested flatten: openAccessPdf
def test_open_access_pdf_flattened_to_two_columns():
    """openAccessPdf.url and .status become two scalar columns."""
    row = c.flatten_paper(_paper())
    assert row["open_access_pdf_url"] == "https://arxiv.org/pdf/1706.03762"
    assert row["open_access_pdf_status"] == "GREEN"


def test_null_open_access_pdf_yields_null_columns():
    """A null openAccessPdf yields None in both columns."""
    row = c.flatten_paper(_paper(openAccessPdf=None))
    assert row["open_access_pdf_url"] is None
    assert row["open_access_pdf_status"] is None


# ------------------------------------------------ authors to paper_authors
def test_authors_extracted_to_paper_authors_table():
    """Each author becomes one paper_authors row keyed on (paper_id, author_id)."""
    rows = c.flatten_authors(_paper())
    assert len(rows) == 2
    assert rows[0] == {
        "paper_id": _paper()["paperId"],
        "author_id": "1234",
        "author_name": "Ashish Vaswani",
    }
    assert rows[1]["author_id"] == "5678"


def test_empty_authors_returns_empty_list():
    """An empty authors array yields no rows."""
    assert c.flatten_authors(_paper(authors=[])) == []


def test_absent_authors_returns_empty_list():
    """A record without an authors field yields no rows."""
    assert c.flatten_authors({"paperId": "x"}) == []


def test_null_authors_returns_empty_list():
    """A null authors field yields no rows."""
    assert c.flatten_authors({"paperId": "x", "authors": None}) == []


def test_author_with_null_author_id_is_skipped():
    """The API returns authorId=null for authors without a profile; a null primary key
    column fails at the destination, so those authors are skipped."""
    paper = _paper(
        authors=[
            {"authorId": "1234", "name": "Known Author"},
            {"authorId": None, "name": "Unknown Author"},
            {"authorId": "5678", "name": "Another Known"},
        ]
    )
    rows = c.flatten_authors(paper)
    assert len(rows) == 2
    assert all(r["author_id"] is not None for r in rows)
    assert rows[0]["author_id"] == "1234"
    assert rows[1]["author_id"] == "5678"


def test_skipped_author_is_logged_at_debug_without_the_name():
    """A skipped author is a debug-level diagnostic that does not log personal data."""
    paper = _paper(authors=[{"authorId": None, "name": "Private Person"}])
    with patch.object(transform, "log") as log:
        c.flatten_authors(paper)
    assert log.info.call_count == 0
    assert log.debug.call_count == 1
    assert "Private Person" not in log.debug.call_args.args[0]


# ------------------------------------------- publication_types serialisation
def test_publication_types_serialised_as_json_string():
    """The publicationTypes array is stored as a JSON string."""
    assert c.flatten_paper(_paper())["publication_types"] == '["JournalArticle"]'


def test_null_publication_types_becomes_null():
    """A null publicationTypes is stored as None."""
    assert c.flatten_paper(_paper(publicationTypes=None))["publication_types"] is None


def test_empty_publication_types_becomes_null():
    """An empty publicationTypes array is stored as None."""
    assert c.flatten_paper({"paperId": "x", "publicationTypes": []})["publication_types"] is None


# ------------------------------------------------- enrichment merge
def test_enrichment_fields_populated_when_provided():
    """Enrichment values are merged into the papers row."""
    row = c.flatten_paper(_paper(), enrichment=_enrichment())
    assert row["cortex_research_impact"] == "high"
    assert row["cortex_technical_domain"] == "NLP"
    assert row["cortex_accessibility_level"] == "advanced"
    assert row["cortex_model_used"] == "claude-sonnet-5"


def test_enrichment_fields_null_when_not_provided():
    """Without enrichment every cortex_* column is None."""
    row = c.flatten_paper(_paper())
    assert row["cortex_research_impact"] is None
    assert row["cortex_model_used"] is None


def test_llm_output_cannot_overwrite_the_primary_key():
    """Model output is free-form JSON, so a hallucinated paper_id key must not
    clobber the primary key; the identifier is stamped after the merge."""
    real_id = "649def34f8be52c8b66281af98ae884c09aef38d"
    hostile = {"paper_id": "HALLUCINATED-BY-THE-MODEL", "cortex_research_impact": "high"}
    row = c.flatten_paper(_paper(paper_id=real_id), enrichment=hostile)
    assert row["paper_id"] == real_id
    assert row["cortex_research_impact"] == "high"


# -------------------------------------------------------------- sparse records
def test_flatten_sparse_record_produces_all_columns_as_none():
    """A record with only paperId must not raise and must fill every column."""
    row = c.flatten_paper(_sparse_paper())
    declared = {col for t in c.schema({}) for col in t["columns"] if t["table"] == "papers"}
    assert set(row) == declared


def test_primary_key_populated_for_sample_record():
    """The primary key is taken from paperId."""
    row = c.flatten_paper(_paper())
    assert row["paper_id"] == _paper()["paperId"]


# ------------------------------------------------------------ configuration
@pytest.mark.parametrize(
    "config,match",
    [
        ({"search_query": ""}, "search_query"),
        ({"search_query": "  "}, "search_query"),
        ({"search_query": "<YOUR_SEARCH_QUERY>"}, "placeholder"),
        ({"search_query": "x", "enable_cortex": "yes"}, "enable_cortex"),
        ({"search_query": "x", "enable_cortex": "TRUE"}, "enable_cortex"),
        ({"search_query": "x", "max_records_per_sync": "0"}, "positive integer"),
        ({"search_query": "x", "max_records_per_sync": "-1"}, "positive integer"),
        ({"search_query": "x", "batch_size": "0"}, "positive integer"),
        ({"search_query": "x", "cortex_timeout": "abc"}, "cortex_timeout"),
        ({"search_query": "x", "enable_cortex": "true"}, "snowflake_account"),
        (
            {"search_query": "x", "enable_cortex": "true", "snowflake_account": ""},
            "snowflake_account",
        ),
        (
            {
                "search_query": "x",
                "enable_cortex": "true",
                "snowflake_account": "https://myaccount.snowflakecomputing.com",
                "snowflake_pat_token": "tok",
            },
            "scheme",
        ),
        (
            {
                "search_query": "x",
                "enable_cortex": "true",
                "snowflake_account": "myaccount.example.com",
                "snowflake_pat_token": "tok",
            },
            "snowflakecomputing",
        ),
        (
            {
                "search_query": "x",
                "enable_cortex": "true",
                "snowflake_account": "myaccount.snowflakecomputing.com",
                "snowflake_pat_token": "",
                "cortex_model": "claude-sonnet-5",
            },
            "snowflake_pat_token",
        ),
        (
            {
                "search_query": "x",
                "enable_cortex": "true",
                "snowflake_account": "myaccount.snowflakecomputing.com",
                "snowflake_pat_token": "tok",
                "cortex_model": "gpt-4",
            },
            "cortex_model",
        ),
    ],
)
def test_invalid_configuration_fails_fast(config, match):
    """Every invalid configuration is rejected with a message naming the field."""
    with pytest.raises(ValueError, match=match):
        c.validate_configuration(config)


def test_valid_cortex_config_passes():
    """A complete Cortex configuration validates."""
    c.validate_configuration(_cortex_config(cortex_timeout="30", max_enrichments="50"))


def test_cortex_disabled_skips_snowflake_credential_check():
    """When enable_cortex is false, Snowflake credentials are not required."""
    c.validate_configuration({"search_query": "nlp research", "enable_cortex": "false"})


def test_placeholder_detection_does_not_fire_on_real_values():
    """The placeholder guard must not reject legitimate configuration."""
    assert not c.is_placeholder("machine learning")
    assert not c.is_placeholder("")
    assert not c.is_placeholder("a<b")
    assert c.is_placeholder("<YOUR_SEARCH_QUERY>")
    assert c.is_placeholder("  <MAX_RECORDS_PER_SYNC>  ")


def test_placeholder_optional_fields_are_ignored_when_cortex_off():
    """The shipped configuration.json runs data-only once search_query is edited;
    every other placeholder is treated as unset and falls back to its default."""
    with open(CONNECTOR_DIR / "configuration.json") as f:
        config = json.load(f)
    config["search_query"] = "data engineering pipelines"

    c.validate_configuration(config)
    settings = c.resolve_settings(config)
    assert settings["enable_cortex"] is False
    assert settings["api_key"] == ""
    assert settings["max_records_per_sync"] == 200
    assert settings["batch_size"] == 50
    assert settings["max_enrichments"] == 3
    assert settings["max_enrichments_per_day"] == 15
    assert settings["cortex_model"] == "claude-sonnet-5"
    assert settings["cortex_timeout"] == 30

    def fetch(session, query, token):
        return {"data": [_paper()], "token": None}

    session = MagicMock()
    session.headers = {}
    with patch.object(c, "create_session", return_value=session):
        state, upserts = _run_update(config, {}, fetch)
    assert len(_paper_ids(upserts)) == 1
    assert "x-api-key" not in session.headers


@pytest.mark.parametrize(
    "config", [{"search_query": "x"}, {"search_query": "x", "enable_cortex": "<TRUE_OR_FALSE>"}]
)
def test_enable_cortex_defaults_to_false_when_omitted_or_placeholder(config):
    """An omitted or placeholder enable_cortex means off: no credentials are required,
    no Cortex session is built, and no enrichment is attempted."""
    c.validate_configuration(config)
    assert c.resolve_settings(config)["enable_cortex"] is False

    def fetch(session, query, token):
        return {"data": [_paper()], "token": None}

    with patch.object(cortex, "create_session", side_effect=AssertionError("built a session")):
        state, upserts = _run_update(config, {}, fetch)
    assert _paper_ids(upserts) == [_paper()["paperId"]]


@pytest.mark.parametrize(
    "account",
    [
        "attacker-snowflakecomputing.com",
        "snowflakecomputing.com",
        ".snowflakecomputing.com",
        "myaccount.snowflakecomputing.com.evil.example",
        "myaccount.snowflakecomputing.com/path",
        "user@myaccount.snowflakecomputing.com",
    ],
)
def test_lookalike_snowflake_host_is_rejected(account):
    """Only a label directly under snowflakecomputing.com may receive the token."""
    with pytest.raises(ValueError, match="snowflake_account"):
        cortex.validate_settings(account, "tok", "claude-sonnet-5")


def test_real_snowflake_host_is_accepted():
    """A hostname of the documented form validates."""
    cortex.validate_settings("abc12345-xy67890.snowflakecomputing.com", "tok", "claude-sonnet-5")


# ------------------------------------------------------- HTTP retry behaviour
def test_429_retries_with_backoff():
    """The unauthenticated pool returns 429 readily; it is retried with backoff."""
    resp_429 = requests.Response()
    resp_429.status_code = 429
    exc = requests.exceptions.RequestException()
    exc.response = resp_429

    resp_ok = requests.Response()
    resp_ok.status_code = 200
    resp_ok._content = json.dumps({"data": [], "token": None}).encode()

    session = MagicMock()
    session.get.side_effect = [exc, resp_ok]
    session.headers = {}

    with patch.object(c.time, "sleep"):
        result = c.fetch_bulk_page(session, "test", None)

    assert result == {"data": [], "token": None}


def test_400_fails_immediately_without_retry():
    """HTTP 400 means the request is structurally wrong; retrying would waste attempts."""
    resp_400 = requests.Response()
    resp_400.status_code = 400
    resp_400._content = b'{"message":"bad request"}'

    session = MagicMock()
    session.get.return_value = resp_400
    session.headers = {}

    with pytest.raises(RuntimeError, match="rejected the request"):
        c.fetch_bulk_page(session, "test", None)
    assert session.get.call_count == 1


# -------------------------------------------------- cursor and resume behaviour
def test_second_sync_resumes_from_stored_token_not_from_scratch():
    """Run 2 must fetch with the token saved by run 1, not restart from page one."""
    call_count = {"n": 0}

    def fake_fetch(session, query, token):
        call_count["n"] += 1
        return {"data": [_paper()], "token": f"token_call_{call_count['n']}"}

    config = _data_only_config(max_records_per_sync="1", batch_size="1")
    state, _ = _run_update(config, {}, fake_fetch)
    saved_token = state["bulk_token"]
    assert saved_token is not None

    tokens_seen = []

    def tracking_fetch(session, query, token):
        tokens_seen.append(token)
        return fake_fetch(session, query, token)

    _run_update(config, state, tracking_fetch)
    assert tokens_seen[0] == saved_token


def test_drain_covers_every_page_record_across_multiple_capped_runs():
    """The bulk endpoint ignores 'limit' and returns full pages. A per-sync cap that
    stops mid-page must not advance the token, or the rest of the page is lost. This
    drains one 100-record page across five capped 20-record runs and asserts every
    record is delivered exactly once, in order."""
    page_papers = [_paper(paper_id=f"p{i}", authors=[]) for i in range(100)]
    fetch_calls = {"n": 0}

    def fake_fetch(session, query, token):
        fetch_calls["n"] += 1
        return {"data": page_papers, "token": None}

    config = _data_only_config(max_records_per_sync="20", batch_size="20")
    state = {}
    seen = []
    for _ in range(5):
        state, upserts = _run_update(config, state, fake_fetch)
        seen.extend(_paper_ids(upserts))

    assert seen == [f"p{i}" for i in range(100)]
    assert fetch_calls["n"] == 5
    assert state["bulk_token"] is None
    assert state["page_offset"] == 0
    assert state["last_paper_id"] is None


def test_partial_page_consumption_does_not_advance_the_token():
    """Stopping mid-page leaves bulk_token unchanged and records the offset and anchor."""

    def fake_fetch(session, query, token):
        return {"data": [_paper(paper_id=f"x{i}") for i in range(30)], "token": "NEXT_PAGE_TOKEN"}

    state, _ = _run_update(_data_only_config(), {}, fake_fetch)
    assert state["bulk_token"] is None
    assert state["page_offset"] == 10
    assert state["last_paper_id"] == "x9"


def test_exhausted_token_does_not_loop():
    """When the API returns no token, the connector stops and does not re-fetch."""
    call_count = {"n": 0}

    def counting_fetch(session, query, token):
        call_count["n"] += 1
        return {"data": [_paper()], "token": None}

    _run_update(
        _data_only_config(max_records_per_sync="1000", batch_size="50"), {}, counting_fetch
    )
    assert call_count["n"] == 1


def test_resume_relocates_anchor_when_page_shifts():
    """Pages are sorted by publication date, so papers indexed between syncs shift the
    page. The resume point follows the last consumed paperId, not the bare offset."""
    page = [_paper(paper_id=f"p{i}", authors=[]) for i in range(30)]

    def first_fetch(session, query, token):
        return {"data": page, "token": None}

    state, upserts = _run_update(_data_only_config(), {}, first_fetch)
    assert _paper_ids(upserts) == [f"p{i}" for i in range(10)]
    assert state["last_paper_id"] == "p9"

    shifted = [_paper(paper_id=f"new{i}", authors=[]) for i in range(3)] + page

    def shifted_fetch(session, query, token):
        return {"data": shifted, "token": None}

    state, upserts = _run_update(_data_only_config(), state, shifted_fetch)
    assert _paper_ids(upserts) == [f"p{i}" for i in range(10, 20)]
    assert state["page_offset"] == 23
    assert state["last_paper_id"] == "p19"


def test_resume_restarts_page_when_anchor_is_missing():
    """If the last consumed record is no longer on the page, the page is reprocessed
    from its first record rather than trusting a stale offset that could skip records."""
    page = [_paper(paper_id=f"p{i}", authors=[]) for i in range(30)]

    def first_fetch(session, query, token):
        return {"data": page, "token": None}

    state, _ = _run_update(_data_only_config(), {}, first_fetch)
    assert state["last_paper_id"] == "p9"

    without_anchor = [p for p in page if p["paperId"] != "p9"]

    def changed_fetch(session, query, token):
        return {"data": without_anchor, "token": None}

    with patch.object(c, "log") as log:
        state, upserts = _run_update(_data_only_config(), state, changed_fetch)
    assert _paper_ids(upserts)[0] == "p0"
    assert len(_paper_ids(upserts)) == 10
    warnings = [call.args[0] for call in log.warning.call_args_list]
    assert any("reprocessing" in message for message in warnings)


def test_search_query_change_resets_pagination_state():
    """State belongs to the query that produced it; a new query restarts from page one."""

    def fetch(session, query, token):
        return {"data": [_paper(paper_id=f"{query}-{i}") for i in range(5)], "token": "T1"}

    state, _ = _run_update(
        _data_only_config(search_query="alpha", max_records_per_sync="5"), {}, fetch
    )
    assert state["bulk_token"] == "T1"
    assert state["search_query"] == "alpha"

    tokens_seen = []

    def tracking_fetch(session, query, token):
        tokens_seen.append(token)
        return fetch(session, query, token)

    state, upserts = _run_update(
        _data_only_config(search_query="beta", max_records_per_sync="5"), state, tracking_fetch
    )
    assert tokens_seen == [None]
    assert state["search_query"] == "beta"
    assert state["total_synced"] == 5
    assert _paper_ids(upserts) == [f"beta-{i}" for i in range(5)]


# ------------------------------------------------------- Cortex enrichment
def test_data_only_mode_never_reads_a_snowflake_credential():
    """enable_cortex=false must sync with no Snowflake credential present at all."""
    config = {"search_query": "test", "api_key": "", "enable_cortex": "false"}
    c.validate_configuration(config)

    def fetch(session, query, token):
        return {"data": [_paper()], "token": None}

    with patch.object(cortex, "create_session", side_effect=AssertionError("built a session")):
        _run_update(config, {}, fetch)


def test_enrichment_defaults_are_small():
    """The documented defaults demonstrate the feature rather than enrich a corpus."""
    settings = c.resolve_settings({"search_query": "x"})
    assert settings["max_enrichments"] == 3
    assert settings["max_enrichments_per_day"] == 15


def test_enrichment_is_bounded_per_day_through_state():
    """The daily ceiling lives in state, so a run that starts at the ceiling builds no
    Cortex session and still syncs its records."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    state = {"enrichment_day": today, "enriched_today": 1}

    def fetch(session, query, token):
        return {"data": [_paper()], "token": None}

    with patch.object(cortex, "create_session", side_effect=AssertionError("built a session")):
        state, upserts = _run_update(_cortex_config(max_enrichments_per_day="1"), state, fetch)
    assert len(_paper_ids(upserts)) == 1
    assert state["enriched_today"] == 1


def test_enrichment_is_bounded_per_sync():
    """max_enrichments caps the calls in one sync; the remaining papers still sync."""
    papers = [_paper(paper_id=f"p{i}") for i in range(5)]

    def fetch(session, query, token):
        return {"data": papers, "token": None}

    calls = []

    def enrich(session, account, model, timeout, record):
        calls.append(record["paperId"])
        return _enrichment()

    state, upserts = _run_update(
        _cortex_config(max_enrichments="2"), {}, fetch, cortex_session=MagicMock(), enrich=enrich
    )
    assert calls == ["p0", "p1"]
    assert len(_paper_ids(upserts)) == 5
    assert state["enriched_today"] == 2


def test_titleless_paper_does_not_consume_enrichment_quota():
    """A paper without a title is not sent for inference and does not count against
    either cap, so the quota is spent on papers that can actually be assessed."""
    papers = [_paper(paper_id="blank", title=None), _paper(paper_id="titled")]

    def fetch(session, query, token):
        return {"data": papers, "token": None}

    calls = []

    def enrich(session, account, model, timeout, record):
        calls.append(record["paperId"])
        return _enrichment()

    state, upserts = _run_update(
        _cortex_config(max_enrichments="1"), {}, fetch, cortex_session=MagicMock(), enrich=enrich
    )
    assert calls == ["titled"]
    assert state["enriched_today"] == 1
    assert len(_paper_ids(upserts)) == 2


def test_cortex_call_retries_a_connection_error_then_succeeds():
    """A connection error is retried and the successful response is parsed."""
    ok = MagicMock()
    ok.raise_for_status = MagicMock()
    ok.text = 'data: {"choices":[{"delta":{"content":"{\\"research_impact\\":\\"high\\"}"}}]}'

    session = MagicMock()
    session.post.side_effect = [requests.exceptions.ConnectionError("boom"), ok]

    with patch.object(cortex.time, "sleep"):
        result = cortex.call_enrich(
            session, "acct.snowflakecomputing.com", "A paper", None, "claude-sonnet-5", 30
        )
    assert session.post.call_count == 2
    assert result == {"research_impact": "high"}


def test_cortex_call_gives_up_after_max_retries_without_raising():
    """A transient failure that exhausts the budget returns None; the paper still lands."""
    session = MagicMock()
    session.post.side_effect = requests.exceptions.Timeout("slow")

    with patch.object(cortex.time, "sleep"):
        result = cortex.call_enrich(
            session, "acct.snowflakecomputing.com", "A paper", None, "claude-sonnet-5", 30
        )
    assert result is None
    assert session.post.call_count == 3


def _http_error(status):
    """Build the HTTPError raise_for_status() produces for a given status code."""
    response = requests.Response()
    response.status_code = status
    error = requests.exceptions.HTTPError(response=response)
    error.response = response
    return error


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_cortex_permanent_4xx_fails_the_sync(status):
    """A permanent client error means the account, token, or model is wrong; it fails
    immediately with a message naming the settings, instead of nulling every paper."""
    session = MagicMock()
    session.post.side_effect = _http_error(status)

    with patch.object(cortex.time, "sleep"):
        with pytest.raises(RuntimeError, match="snowflake_pat_token"):
            cortex.call_enrich(
                session, "acct.snowflakecomputing.com", "A paper", None, "claude-sonnet-5", 30
            )
    assert session.post.call_count == 1


def test_cortex_429_is_still_retried():
    """Rate limiting is transient and is retried rather than treated as permanent."""
    ok = MagicMock()
    ok.raise_for_status = MagicMock()
    ok.text = 'data: {"choices":[{"delta":{"content":"{\\"research_impact\\":\\"low\\"}"}}]}'

    session = MagicMock()
    session.post.side_effect = [_http_error(429), ok]

    with patch.object(cortex.time, "sleep"):
        result = cortex.call_enrich(
            session, "acct.snowflakecomputing.com", "A paper", None, "claude-sonnet-5", 30
        )
    assert result == {"research_impact": "low"}
    assert session.post.call_count == 2


def test_permanent_cortex_error_propagates_out_of_update():
    """update() re-raises the permanent error so the sync fails rather than succeeds."""

    def fetch(session, query, token):
        return {"data": [_paper()], "token": None}

    def enrich(session, account, model, timeout, record):
        raise RuntimeError("Cortex inference request rejected (HTTP 401)")

    with pytest.raises(RuntimeError, match="HTTP 401"):
        _run_update(_cortex_config(), {}, fetch, cortex_session=MagicMock(), enrich=enrich)


def test_invalid_enum_values_are_dropped_and_model_marker_only_on_valid_result():
    """Only the documented values are stored; anything else is null, and the model
    marker is set only when at least one value was accepted."""
    args = (MagicMock(), "acct.snowflakecomputing.com", "claude-sonnet-5", 30, _paper())

    partial = {"research_impact": "HIGH", "technical_domain": "NLP", "accessibility_level": "x"}
    with patch.object(cortex, "call_enrich", return_value=partial), patch.object(
        cortex.time, "sleep"
    ):
        row = cortex.enrich_paper(*args)
    assert row["cortex_research_impact"] is None
    assert row["cortex_technical_domain"] == "NLP"
    assert row["cortex_accessibility_level"] is None
    assert row["cortex_model_used"] == "claude-sonnet-5"

    garbage = {"research_impact": "unbelievable", "technical_domain": ["NLP"]}
    with patch.object(cortex, "call_enrich", return_value=garbage), patch.object(
        cortex.time, "sleep"
    ):
        row = cortex.enrich_paper(*args)
    assert row == {
        "cortex_research_impact": None,
        "cortex_technical_domain": None,
        "cortex_accessibility_level": None,
        "cortex_model_used": None,
    }

    with patch.object(cortex, "call_enrich", return_value=None), patch.object(
        cortex.time, "sleep"
    ):
        row = cortex.enrich_paper(*args)
    assert row["cortex_model_used"] is None


def test_titleless_paper_returns_null_enrichment_without_a_call():
    """enrich_paper() itself never calls the API for a paper with no title."""
    with patch.object(cortex, "call_enrich") as call:
        row = cortex.enrich_paper(
            MagicMock(), "acct.snowflakecomputing.com", "claude-sonnet-5", 30, {"paperId": "x"}
        )
    assert call.call_count == 0
    assert row["cortex_model_used"] is None


def test_prompt_lists_exactly_the_accepted_values():
    """The prompt and the validator are built from the same value sets."""
    prompt = cortex.build_prompt("T", "A" * 2000)
    assert '"research_impact": "high|medium|low"' in prompt
    assert '"accessibility_level": "beginner|intermediate|advanced"' in prompt
    assert "NLP|CV|ML|Systems|Theory|Biology|Chemistry|Physics|Medicine|Social|Other" in prompt
    assert "A" * 501 not in prompt
