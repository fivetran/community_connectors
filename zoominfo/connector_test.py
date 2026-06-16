"""
Unit tests for the ZoomInfo connector.

Covers the pure helper functions (config parsers, enrichment-eligibility
predicates, JSON:API body builder, cursor/date coercion) plus the HTTP-layer
retry logic exercised against a mocked transport. No real ZoomInfo credentials
or network access are required — every external call is mocked with `responses`.

Run from this connector's directory:

    pip install pytest responses
    pytest connector_test.py
"""

import sys
from pathlib import Path

import pytest
import responses

# Make connector.py importable when running pytest from any directory.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fivetran_connector_sdk import Logging  # noqa: E402

# The Fivetran SDK's Logging class only initializes its log level when the
# harness runs; outside the harness, log.warning() / log.info() raise a
# TypeError comparing None to Level. Initialize it once for all tests.
Logging.LOG_LEVEL = Logging.Level.INFO

from connector import (  # noqa: E402
    _bool_config,
    _list_config,
    _fields_config,
    _max_cursor,
    _parse_iso_for_compare,
    _safe_int,
    _safe_float,
    _safe_utc_datetime,
    _iso_to_yyyymmdd,
    build_body,
    build_search_filter,
    apply_incremental_filter,
    should_enrich,
    matches_mgmt_level,
    validate_configuration,
    _warn_if_truncated,
    SEARCH_RESULT_CEILING,
    post_with_retry,
    get_with_retry,
    DEFAULT_COUNTRY,
    ENDPOINT_CONTACTS,
    ENDPOINT_COMPANIES,
    JSONAPI_TYPE,
)


# ─────────────────────────────────────────────
# _bool_config
# ─────────────────────────────────────────────
class TestBoolConfig:
    @pytest.mark.parametrize(
        "value,expected",
        [
            (True, True),
            (False, False),
            ("true", True),
            ("True", True),
            ("TRUE", True),
            ("1", True),
            ("yes", True),
            ("YES", True),
            ("false", False),
            ("no", False),
            ("0", False),
            ("", False),
            ("anything-else", False),
        ],
    )
    def test_values(self, value, expected):
        assert _bool_config({"key": value}, "key") is expected

    def test_missing_key_returns_false(self):
        assert _bool_config({}, "missing") is False


# ─────────────────────────────────────────────
# _list_config
# ─────────────────────────────────────────────
class TestListConfig:
    def test_empty_string(self):
        assert _list_config({"k": ""}, "k") == []

    def test_missing_key(self):
        assert _list_config({}, "k") == []

    def test_single_value(self):
        assert _list_config({"k": "alpha"}, "k") == ["alpha"]

    def test_comma_separated(self):
        assert _list_config({"k": "a,b,c"}, "k") == ["a", "b", "c"]

    def test_strips_whitespace(self):
        assert _list_config({"k": " a , b , c "}, "k") == ["a", "b", "c"]

    def test_drops_empty_entries(self):
        assert _list_config({"k": "a,,b,"}, "k") == ["a", "b"]


# ─────────────────────────────────────────────
# _fields_config
# ─────────────────────────────────────────────
class TestFieldsConfig:
    def test_blank_returns_defaults(self):
        defaults = ["x", "y"]
        assert _fields_config({"k": ""}, "k", defaults) == defaults

    def test_missing_returns_defaults(self):
        defaults = ["x", "y"]
        assert _fields_config({}, "k", defaults) == defaults

    def test_populated_overrides_defaults(self):
        assert _fields_config({"k": "a,b"}, "k", ["x"]) == ["a", "b"]

    def test_strips_whitespace(self):
        assert _fields_config({"k": " a , b "}, "k", []) == ["a", "b"]


# ─────────────────────────────────────────────
# build_body
# ─────────────────────────────────────────────
class TestBuildBody:
    def test_contacts_envelope(self):
        body = build_body(ENDPOINT_CONTACTS, {"country": "US"})
        assert body == {
            "data": {
                "type": JSONAPI_TYPE[ENDPOINT_CONTACTS],
                "attributes": {"country": "US"},
            }
        }

    def test_companies_envelope(self):
        body = build_body(ENDPOINT_COMPANIES, {})
        assert body["data"]["type"] == "CompanySearch"
        assert body["data"]["attributes"] == {}

    def test_every_endpoint_has_a_type(self):
        for endpoint in JSONAPI_TYPE:
            body = build_body(endpoint, {"a": 1})
            assert body["data"]["type"] == JSONAPI_TYPE[endpoint]


# ─────────────────────────────────────────────
# build_search_filter
# ─────────────────────────────────────────────
class TestBuildSearchFilter:
    def test_blank_defaults_to_united_states(self):
        assert build_search_filter({}) == {"country": DEFAULT_COUNTRY}

    def test_blank_string_defaults_to_united_states(self):
        assert build_search_filter({"countries": "   "}) == {"country": DEFAULT_COUNTRY}

    def test_single_country(self):
        assert build_search_filter({"countries": "Canada"}) == {"country": "Canada"}

    def test_strips_whitespace(self):
        assert build_search_filter({"countries": "  Germany  "}) == {"country": "Germany"}

    def test_multiple_countries_uses_first_and_warns(self):
        result = build_search_filter({"countries": "France, Spain, Italy"})
        assert result == {"country": "France"}


# ─────────────────────────────────────────────
# should_enrich
# ─────────────────────────────────────────────
class TestShouldEnrich:
    @pytest.mark.parametrize(
        "attrs",
        [
            {"hasEmail": True},
            {"hasEmail": False},
            {"hasDirectPhone": True},
            {},
        ],
    )
    def test_all_filter_always_true(self, attrs):
        assert should_enrich(attrs, "all") is True

    def test_has_email_filter(self):
        assert should_enrich({"hasEmail": True}, "has_email") is True
        assert should_enrich({"hasEmail": False}, "has_email") is False
        assert should_enrich({}, "has_email") is False

    def test_has_phone_filter_direct(self):
        assert should_enrich({"hasDirectPhone": True}, "has_phone") is True

    def test_has_phone_filter_mobile(self):
        assert should_enrich({"hasMobilePhone": True}, "has_phone") is True

    def test_has_phone_filter_neither(self):
        assert (
            should_enrich({"hasDirectPhone": False, "hasMobilePhone": False}, "has_phone") is False
        )

    def test_has_email_or_phone_filter(self):
        assert should_enrich({"hasEmail": True}, "has_email_or_phone") is True
        assert should_enrich({"hasMobilePhone": True}, "has_email_or_phone") is True
        assert should_enrich({}, "has_email_or_phone") is False

    def test_unknown_filter_returns_false(self):
        assert should_enrich({"hasEmail": True}, "garbage") is False


# ─────────────────────────────────────────────
# matches_mgmt_level
# ─────────────────────────────────────────────
class TestMatchesMgmtLevel:
    def test_empty_filter_matches_everything(self):
        assert matches_mgmt_level({"managementLevel": ["Director"]}, []) is True
        assert matches_mgmt_level({}, []) is True

    def test_list_match(self):
        assert matches_mgmt_level({"managementLevel": ["C-Level"]}, ["C-Level"]) is True

    def test_string_value_normalised_to_list(self):
        assert matches_mgmt_level({"managementLevel": "C-Level"}, ["C-Level"]) is True

    def test_case_insensitive(self):
        assert matches_mgmt_level({"managementLevel": ["c-level"]}, ["C-LEVEL"]) is True

    def test_no_match(self):
        assert matches_mgmt_level({"managementLevel": ["Manager"]}, ["C-Level"]) is False

    def test_no_substring_match(self):
        # Matching is exact (case-insensitive), NOT substring containment.
        # A configured "C" must not match "C-Level"...
        assert matches_mgmt_level({"managementLevel": ["C-Level"]}, ["C"]) is False
        # ...and configured "Manager" must not match "Non Manager" (which would
        # otherwise enrich extra contacts and spend credits).
        assert matches_mgmt_level({"managementLevel": ["Non Manager"]}, ["Manager"]) is False
        # Exact match still succeeds.
        assert matches_mgmt_level({"managementLevel": ["Non Manager"]}, ["Non Manager"]) is True


# ─────────────────────────────────────────────
# apply_incremental_filter
# ─────────────────────────────────────────────
class TestApplyIncrementalFilter:
    BASE = {"country": "United States"}

    def test_no_state_returns_base_unchanged(self):
        result = apply_incremental_filter(
            self.BASE, {}, {}, "contacts_last_updated", "lastUpdatedDateAfter"
        )
        assert result == self.BASE

    def test_state_adds_date_only_predicate(self):
        state = {"contacts_last_updated": "2026-05-01T00:00:00Z"}
        result = apply_incremental_filter(
            self.BASE, {}, state, "contacts_last_updated", "lastUpdatedDateAfter"
        )
        assert result == {
            "country": "United States",
            "lastUpdatedDateAfter": "2026-05-01",
        }

    def test_state_with_plain_date_passes_through(self):
        state = {"scoops_last_updated": "2026-04-15"}
        result = apply_incremental_filter(
            {}, {}, state, "scoops_last_updated", "publishedStartDate"
        )
        assert result == {"publishedStartDate": "2026-04-15"}

    def test_full_refresh_flag_overrides_state(self):
        state = {"contacts_last_updated": "2026-05-01T00:00:00Z"}
        result = apply_incremental_filter(
            self.BASE,
            {"full_refresh": "true"},
            state,
            "contacts_last_updated",
            "lastUpdatedDateAfter",
        )
        assert result == self.BASE
        assert "lastUpdatedDateAfter" not in result

    def test_does_not_mutate_base_filter(self):
        state = {"contacts_last_updated": "2026-05-01T00:00:00Z"}
        original_base = dict(self.BASE)
        apply_incremental_filter(
            self.BASE, {}, state, "contacts_last_updated", "lastUpdatedDateAfter"
        )
        assert self.BASE == original_base

    def test_different_api_field_per_endpoint(self):
        # news uses pageDateMin, intent uses signalStartDate, etc.
        state = {"news_last_updated": "2026-05-10T12:00:00Z"}
        result = apply_incremental_filter({}, {}, state, "news_last_updated", "pageDateMin")
        assert result == {"pageDateMin": "2026-05-10"}

    def test_missing_state_key_returns_base(self):
        state = {"companies_last_updated": "2026-05-01T00:00:00Z"}
        result = apply_incremental_filter(
            self.BASE, {}, state, "contacts_last_updated", "lastUpdatedDateAfter"
        )
        assert result == self.BASE

    def test_api_field_none_skips_predicate(self):
        # Companies endpoint has no incremental filter — state exists but
        # the predicate must NOT be added.
        state = {"companies_last_updated": "2026-05-01T00:00:00Z"}
        result = apply_incremental_filter(self.BASE, {}, state, "companies_last_updated", None)
        assert result == self.BASE


# ─────────────────────────────────────────────
# _max_cursor / _parse_iso_for_compare
# ─────────────────────────────────────────────
class TestMaxCursor:
    """
    Regression coverage for the cursor-advancement bug: string comparison on
    ISO timestamps gave wrong answers when records arrived with different
    timezone offsets. The fix parses both to timezone-aware datetimes.
    """

    def test_returns_current_when_candidate_none(self):
        assert _max_cursor("2026-05-01T00:00:00Z", None) == "2026-05-01T00:00:00Z"

    def test_returns_candidate_when_current_none(self):
        assert _max_cursor(None, "2026-05-01T00:00:00Z") == "2026-05-01T00:00:00Z"

    def test_returns_both_none(self):
        assert _max_cursor(None, None) is None

    def test_picks_later_z_suffixed(self):
        assert (
            _max_cursor("2026-05-01T00:00:00Z", "2026-05-02T00:00:00Z") == "2026-05-02T00:00:00Z"
        )

    def test_picks_later_with_mixed_offsets(self):
        # 10:00 +02:00 == 08:00 UTC, which is later than 07:00Z. String
        # comparison would pick "07:00Z" by lexicographic order — the parsed
        # datetime comparison correctly picks "+02:00".
        current = "2026-05-01T07:00:00Z"
        candidate = "2026-05-01T10:00:00+02:00"
        assert _max_cursor(current, candidate) == candidate

    def test_returns_original_string_unchanged(self):
        # Important: the value going back into state should round-trip the
        # exact source string, not a re-serialized datetime.
        result = _max_cursor("2026-05-01T00:00:00Z", "2026-05-02T12:34:56.789+00:00")
        assert result == "2026-05-02T12:34:56.789+00:00"

    def test_unparseable_candidate_keeps_current(self):
        assert _max_cursor("2026-05-01T00:00:00Z", "garbage") == "2026-05-01T00:00:00Z"


class TestParseIsoForCompare:
    def test_z_suffix(self):
        dt = _parse_iso_for_compare("2026-05-01T00:00:00Z")
        assert dt is not None
        assert dt.tzinfo is not None

    def test_explicit_offset(self):
        dt = _parse_iso_for_compare("2026-05-01T10:00:00+02:00")
        assert dt is not None
        assert dt.tzinfo is not None

    def test_bare_date_promoted_to_utc_midnight(self):
        dt = _parse_iso_for_compare("2026-05-01")
        assert dt is not None
        assert dt.year == 2026 and dt.month == 5 and dt.day == 1
        assert dt.hour == 0 and dt.tzinfo is not None

    def test_garbage_returns_none(self):
        assert _parse_iso_for_compare("nope") is None
        assert _parse_iso_for_compare(None) is None
        assert _parse_iso_for_compare("") is None


# ─────────────────────────────────────────────
# _safe_int / _safe_float / _safe_utc_datetime / _iso_to_yyyymmdd
# ─────────────────────────────────────────────
class TestSafeCoercion:
    @pytest.mark.parametrize(
        "value,expected",
        [
            (5, 5),
            ("5", 5),
            (5.9, 5),
            ("5.9", 5),
            (None, None),
            ("", None),
            ("abc", None),
        ],
    )
    def test_safe_int(self, value, expected):
        assert _safe_int(value) == expected

    @pytest.mark.parametrize(
        "value,expected",
        [
            (1.5, 1.5),
            ("1.5", 1.5),
            (2, 2.0),
            (None, None),
            ("", None),
            ("xyz", None),
        ],
    )
    def test_safe_float(self, value, expected):
        assert _safe_float(value) == expected

    def test_safe_utc_datetime_full_timestamp_passthrough(self):
        assert _safe_utc_datetime("2026-05-01T12:00:00Z") == "2026-05-01T12:00:00Z"

    def test_safe_utc_datetime_bare_date_promoted(self):
        assert _safe_utc_datetime("2026-05-01") == "2026-05-01T00:00:00Z"

    def test_safe_utc_datetime_invalid_returns_none(self):
        assert _safe_utc_datetime("") is None
        assert _safe_utc_datetime(None) is None
        assert _safe_utc_datetime(12345) is None

    def test_iso_to_yyyymmdd_truncates_time(self):
        assert _iso_to_yyyymmdd("2026-05-19T23:31:00Z") == "2026-05-19"

    def test_iso_to_yyyymmdd_passthrough_date(self):
        assert _iso_to_yyyymmdd("2026-05-19") == "2026-05-19"

    def test_iso_to_yyyymmdd_none(self):
        assert _iso_to_yyyymmdd(None) is None


# ─────────────────────────────────────────────
# HTTP retry logic — mocked transport (no real network)
# ─────────────────────────────────────────────
class TestHttpRetry:
    """
    Exercises post_with_retry / get_with_retry against mocked responses.

    Retry backoff sleeps are patched to no-ops so the suite stays fast — we
    assert on the request count and final status, not wall-clock timing.
    """

    URL = "https://api.zoominfo.example/test"

    @pytest.fixture(autouse=True)
    def _no_sleep(self, monkeypatch):
        # Avoid real exponential-backoff sleeps during retry tests.
        monkeypatch.setattr("connector.time.sleep", lambda *_a, **_k: None)

    @responses.activate
    def test_post_returns_immediately_on_200(self):
        responses.add(responses.POST, self.URL, json={"ok": True}, status=200)
        resp = post_with_retry(self.URL, headers={}, json_body={})
        assert resp.status_code == 200
        assert len(responses.calls) == 1

    @responses.activate
    def test_post_retries_on_429_then_succeeds(self):
        responses.add(responses.POST, self.URL, status=429)
        responses.add(responses.POST, self.URL, status=429)
        responses.add(responses.POST, self.URL, json={"ok": True}, status=200)
        resp = post_with_retry(self.URL, headers={}, json_body={})
        assert resp.status_code == 200
        assert len(responses.calls) == 3

    @responses.activate
    def test_post_retries_on_transient_5xx(self):
        responses.add(responses.POST, self.URL, status=503)
        responses.add(responses.POST, self.URL, json={"ok": True}, status=200)
        resp = post_with_retry(self.URL, headers={}, json_body={})
        assert resp.status_code == 200
        assert len(responses.calls) == 2

    @responses.activate
    def test_post_raises_after_exhausting_retries(self):
        # Every attempt returns a retryable status — should raise, not loop forever.
        for _ in range(10):
            responses.add(responses.POST, self.URL, status=500)
        with pytest.raises(RuntimeError):
            post_with_retry(self.URL, headers={}, json_body={})

    @responses.activate
    def test_post_does_not_retry_on_400(self):
        # 400 is not in RETRY_STATUS_CODES — returned to caller as-is for
        # endpoint-specific handling (e.g. the PFAPI0004 end-of-pages signal).
        responses.add(responses.POST, self.URL, status=400, body="PFAPI0004")
        resp = post_with_retry(self.URL, headers={}, json_body={})
        assert resp.status_code == 400
        assert len(responses.calls) == 1

    @responses.activate
    def test_get_retries_on_429_then_succeeds(self):
        responses.add(responses.GET, self.URL, status=429)
        responses.add(responses.GET, self.URL, json={"ok": True}, status=200)
        resp = get_with_retry(self.URL, headers={})
        assert resp.status_code == 200
        assert len(responses.calls) == 2


# ─────────────────────────────────────────────
# validate_configuration
# ─────────────────────────────────────────────
class TestValidateConfiguration:
    def test_passes_with_required_credentials(self):
        # Should not raise.
        validate_configuration({"client_id": "abc", "client_secret": "xyz"})

    def test_missing_client_id_raises(self):
        with pytest.raises(ValueError, match="client_id"):
            validate_configuration({"client_secret": "xyz"})

    def test_missing_client_secret_raises(self):
        with pytest.raises(ValueError, match="client_secret"):
            validate_configuration({"client_id": "abc"})

    def test_empty_credentials_raise(self):
        with pytest.raises(ValueError):
            validate_configuration({"client_id": "", "client_secret": ""})

    def test_invalid_enrich_filter_raises_when_enrichment_enabled(self):
        cfg = {
            "client_id": "abc",
            "client_secret": "xyz",
            "enrich_contacts": "true",
            "enrich_filter": "not_a_real_filter",
        }
        with pytest.raises(ValueError, match="enrich_filter"):
            validate_configuration(cfg)

    def test_invalid_enrich_filter_ignored_when_enrichment_disabled(self):
        # enrich_filter is only validated when enrich_contacts is on.
        cfg = {
            "client_id": "abc",
            "client_secret": "xyz",
            "enrich_filter": "not_a_real_filter",
        }
        validate_configuration(cfg)  # should not raise

    def test_too_many_intent_topics_raises(self):
        cfg = {
            "client_id": "abc",
            "client_secret": "xyz",
            "intent_topics": ",".join(f"topic{i}" for i in range(51)),
        }
        with pytest.raises(ValueError, match="intent_topics"):
            validate_configuration(cfg)


# ─────────────────────────────────────────────
# _warn_if_truncated — Search result ceiling detection
# ─────────────────────────────────────────────
class TestWarnIfTruncated:
    def test_warns_when_over_ceiling(self):
        assert _warn_if_truncated("/contacts/search", SEARCH_RESULT_CEILING + 1) is True

    def test_no_warn_at_ceiling(self):
        assert _warn_if_truncated("/contacts/search", SEARCH_RESULT_CEILING) is False

    def test_no_warn_under_ceiling(self):
        assert _warn_if_truncated("/contacts/search", 42) is False

    def test_handles_stringified_count(self):
        # ZoomInfo sometimes returns numbers as strings; _safe_int coerces.
        assert _warn_if_truncated("/contacts/search", str(SEARCH_RESULT_CEILING + 5)) is True

    def test_none_total_does_not_warn(self):
        assert _warn_if_truncated("/contacts/search", None) is False

    def test_unparseable_total_does_not_warn(self):
        assert _warn_if_truncated("/contacts/search", "lots") is False
