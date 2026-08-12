"""Tests for the Federal Register documents connector.

These target the hazards found while profiling the live API on 2026-07-29, not
generic coverage. The universal rules are inherited from the shared contract; the
functions below are what is specific to this source:

  - publication_date[gte] is INCLUSIVE and day-granular, and a single day carries
    dozens of documents. A date-only cursor would re-fetch or skip records, so the
    cursor is compound: (publication_date, document_number). document_number is the
    exact value the API sorts on within a day -- its search_after cursor decodes to
    [publication_date_epoch, document_number] -- so the ordering matches the server.
  - Because document_number uniquely orders documents within a day, a bounded run
    can stop on any single record and resume on the next. max_records_per_sync is a
    TRUE ceiling here, unlike a bare-timestamp cursor where it is a floor.
  - agencies is a list of objects; flattened to delimited id and name strings.
  - the API returns pages via next_page_url; the walk stops on its absence.

Run:  ~/venvs/connector-sdk-venv/bin/python -m pytest tests/ -v
"""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import connector as c  # noqa: E402
from connector_test_harness import ConnectorContract  # noqa: E402


# ----------------------------------------------------- the shared contract
class TestFederalRegisterContract(ConnectorContract):
    """Every universal connector rule, inherited rather than re-written."""

    module = c
    flatten = staticmethod(c.flatten_document)
    sample_record = None  # set below, after _document() is defined
    sparse_record = {"document_number": "2024-00001"}


# --------------------------------------------------------------- fixtures
def _document(publication_date="2024-01-02", document_number="2023-26792", **overrides):
    """Build a minimal API record, shaped like the real documents.json response."""
    base = {
        "document_number": document_number,
        "publication_date": publication_date,
        "type": "Notice",
        "title": "Notice of Solicitation of Applications",
        "abstract": "The Rural Business-Cooperative Service announces the availability of funds.",
        "excerpts": "The Rural Business-Cooperative Service announces...",
        "html_url": "https://www.federalregister.gov/documents/2024/01/02/2023-26792/notice",
        "pdf_url": "https://www.govinfo.gov/content/pkg/FR-2024-01-02/pdf/2023-26792.pdf",
        "public_inspection_pdf_url": "https://www.federalregister.gov/documents/full_text/pdf/2023-26792.pdf",
        "agencies": [
            {
                "raw_name": "DEPARTMENT OF AGRICULTURE",
                "name": "Agriculture Department",
                "id": 12,
                "url": "https://www.federalregister.gov/agencies/agriculture-department",
            }
        ],
    }
    base.update(overrides)
    return base


# ----------------------------------------------- nested agency flattening
def test_agencies_are_flattened_not_stringified():
    """The trap: agencies is a list of dicts. str() on it type checks, lints, and
    lands a stringified list in a scalar column."""
    row = c.flatten_document(
        _document(
            agencies=[
                {
                    "name": "Agriculture Department",
                    "raw_name": "DEPARTMENT OF AGRICULTURE",
                    "id": 12,
                },
                {"name": "Forest Service", "raw_name": "Forest Service", "id": 145},
            ]
        )
    )
    assert row["agency_ids"] == "12,145"
    assert row["agency_names"] == "Agriculture Department; Forest Service"
    assert "[" not in row["agency_ids"]
    assert "{" not in row["agency_names"]


def test_empty_agencies_become_null_not_empty_string():
    row = c.flatten_document(_document(agencies=[]))
    assert row["agency_ids"] is None
    assert row["agency_names"] is None


def test_agency_name_falls_back_to_raw_name():
    row = c.flatten_document(_document(agencies=[{"raw_name": "SOME BOARD", "id": 500}]))
    assert row["agency_names"] == "SOME BOARD"
    assert row["agency_ids"] == "500"


def test_agency_without_id_is_skipped_in_ids_only():
    row = c.flatten_document(_document(agencies=[{"name": "A", "id": 1}, {"name": "B"}]))
    assert row["agency_ids"] == "1"
    assert row["agency_names"] == "A; B"


# ----------------------------------------------- reserved-word rename (#8)
def test_type_is_renamed_to_document_type():
    """The API field is `type`. It is emitted as `document_type` because `type`
    reads ambiguously as a column and some warehouses refuse it as an identifier.
    The raw key must never reach the schema."""
    row = c.flatten_document(_document(type="Rule"))
    assert row["document_type"] == "Rule"
    assert "type" not in row


# ----------------------------------------------------------- URL building
def test_url_uses_inclusive_gte_oldest_order_and_pinned_fields():
    url = c.build_initial_url("2024-01-02", 100)
    assert "conditions%5Bpublication_date%5D%5Bgte%5D=2024-01-02" in url
    assert "order=oldest" in url
    assert "per_page=100" in url
    assert "fields%5B%5D=document_number" in url


# ----------------------------------------------------------- configuration
@pytest.mark.parametrize(
    "config",
    [
        {"page_size": "0"},
        {"page_size": "1001"},
        {"page_size": "abc"},
        {"max_records_per_sync": "-1"},
        {"max_records_per_sync": "lots"},
        {"initial_sync_start_date": "2024/01/02"},
        {"initial_sync_start_date": "not-a-date"},
    ],
)
def test_invalid_configuration_fails_fast(config):
    """Fail at configuration time, not as an HTTP error midway through a sync."""
    with pytest.raises(ValueError):
        c.validate_configuration(config)


def test_valid_and_empty_configurations_pass():
    c.validate_configuration({})  # every field is optional with a default
    c.validate_configuration(
        {
            "page_size": "1000",
            "max_records_per_sync": "500",
            "initial_sync_start_date": "2024-01-02",
        }
    )


# --------------------------------------------------------- sync behaviour
def test_cursor_walk_terminates_on_missing_next_page_url():
    """The walk stops when next_page_url is absent, not when a page is empty."""
    pages = [
        {"results": [_document()], "next_page_url": "u2"},
        {"results": [], "next_page_url": None},  # link absent -> stop
    ]
    with patch.object(c, "get_api_response", side_effect=pages) as api, patch.object(
        c.op, "upsert"
    ) as upsert, patch.object(c.op, "checkpoint"):
        c.update({}, {})
    assert api.call_count == 2
    assert upsert.call_count == 1


def test_inclusive_boundary_records_already_synced_are_skipped():
    """The gte lower bound re-serves the last synced day on resume. Documents at
    or before the compound cursor are exactly the ones already delivered."""
    page = {
        "results": [
            _document("2024-01-02", "2023-26792"),  # <= cursor, skip
            _document("2024-01-02", "2023-27783"),  # == cursor, skip
            _document("2024-01-02", "2023-27901"),  # > cursor, emit
            _document("2024-01-03", "2023-28000"),  # next day, emit
        ],
        "next_page_url": None,
    }
    state = {
        "last_publication_date": "2024-01-02",
        "last_document_number": "2023-27783",
    }
    with patch.object(c, "get_api_response", return_value=page), patch.object(
        c.op, "upsert"
    ) as upsert, patch.object(c.op, "checkpoint"):
        c.update({}, state)
    emitted = [x.kwargs["data"]["document_number"] for x in upsert.call_args_list]
    assert emitted == ["2023-27901", "2023-28000"]


def test_compound_cursor_resumes_mid_boundary_day():
    """The named boundary guard.

    An inclusive, day-granular lower bound with dozens of documents per day would
    deadlock a bare-timestamp cursor: a per-sync cap that stops mid-day would
    checkpoint that day, and the next sync would reopen it and stop in the same
    place forever. The compound (publication_date, document_number) cursor stops
    BETWEEN two documents on the same day and resumes on the next one.
    """
    day = [_document("2024-01-02", f"2023-{n:05d}") for n in range(26792, 26796)]  # 4 docs

    # Run 1: cap of 2 stops mid-day after the first two documents.
    page = {"results": list(day), "next_page_url": None}
    with patch.object(c, "get_api_response", return_value=page), patch.object(
        c.op, "upsert"
    ) as upsert, patch.object(c.op, "checkpoint") as checkpoint:
        c.update({"max_records_per_sync": "2"}, {})
    run1 = [x.kwargs["data"]["document_number"] for x in upsert.call_args_list]
    assert run1 == ["2023-26792", "2023-26793"]
    state = checkpoint.call_args.kwargs["state"]
    assert state == {
        "last_publication_date": "2024-01-02",
        "last_document_number": "2023-26793",
    }

    # Run 2: the inclusive bound re-serves the same day; the compound cursor skips
    # the two already delivered and resumes on the third, same day.
    with patch.object(c, "get_api_response", return_value=page), patch.object(
        c.op, "upsert"
    ) as upsert, patch.object(c.op, "checkpoint") as checkpoint:
        c.update({"max_records_per_sync": "2"}, state)
    run2 = [x.kwargs["data"]["document_number"] for x in upsert.call_args_list]
    assert run2 == ["2023-26794", "2023-26795"]


def test_max_records_is_a_true_ceiling_not_a_floor():
    """Because document_number uniquely orders a day, the cap stops on the exact
    record, not at the end of a group. Three documents, cap of 2, delivers two."""
    page = {
        "results": [
            _document("2024-01-02", "2023-26792"),
            _document("2024-01-02", "2023-26793"),
            _document("2024-01-02", "2023-26794"),
        ],
        "next_page_url": None,
    }
    with patch.object(c, "get_api_response", return_value=page), patch.object(
        c.op, "upsert"
    ) as upsert, patch.object(c.op, "checkpoint") as checkpoint:
        c.update({"max_records_per_sync": "2"}, {})
    assert upsert.call_count == 2
    assert checkpoint.call_args.kwargs["state"] == {
        "last_publication_date": "2024-01-02",
        "last_document_number": "2023-26793",
    }


def test_repeated_syncs_drain_all_records_without_duplication():
    """Repeated bounded syncs cover every document exactly once and the cursor
    never regresses -- the compound-cursor equivalent of a drain test."""
    # Built as one comprehension over (date, span) pairs rather than three lists
    # joined with `+`. Black splits a multi-line binary expression with the operator
    # at the START of each line, and repo CI selects W (W503: line break before
    # binary operator) -- so the two tools fight over that shape and CI loses.
    # No operator split, nothing to disagree about.
    spans = [
        ("2024-01-02", range(1, 4)),
        ("2024-01-03", range(4, 7)),
        ("2024-01-04", range(7, 10)),
    ]
    all_docs = [_document(date, f"2023-{n:05d}") for date, span in spans for n in span]

    def key(doc):
        return (doc["publication_date"], doc["document_number"])

    cursors, delivered = [], []
    state = {}
    for _ in range(5):
        last = (
            state.get("last_publication_date", "2024-01-02"),
            state.get("last_document_number", ""),
        )
        # The gte filter re-serves from the cursor's day onward; the connector
        # itself skips anything at or before the compound cursor.
        window = {
            "results": [d for d in all_docs if d["publication_date"] >= last[0]],
            "next_page_url": None,
        }
        with patch.object(c, "get_api_response", return_value=window), patch.object(
            c.op, "upsert"
        ) as upsert, patch.object(c.op, "checkpoint") as checkpoint:
            c.update({"max_records_per_sync": "2"}, state)
        delivered += [x.kwargs["data"]["document_number"] for x in upsert.call_args_list]
        state = checkpoint.call_args.kwargs["state"]
        cursors.append((state["last_publication_date"], state["last_document_number"]))

    assert cursors == sorted(cursors), f"cursor moved backwards: {cursors}"
    assert len(delivered) == len(set(delivered)) == 9, f"dup or missing: {delivered}"


def test_state_never_moves_backwards_on_an_empty_window():
    """An empty sync must re-checkpoint the SAME cursor rather than reset it."""
    with patch.object(
        c, "get_api_response", return_value={"results": [], "next_page_url": None}
    ), patch.object(c.op, "upsert"), patch.object(c.op, "checkpoint") as checkpoint:
        c.update(
            {},
            {
                "last_publication_date": "2024-01-02",
                "last_document_number": "2023-27901",
            },
        )
    assert checkpoint.call_args.kwargs["state"] == {
        "last_publication_date": "2024-01-02",
        "last_document_number": "2023-27901",
    }


def test_record_without_document_number_is_skipped():
    page = {
        "results": [{"publication_date": "2024-01-02", "type": "Notice"}, _document()],
        "next_page_url": None,
    }
    with patch.object(c, "get_api_response", return_value=page), patch.object(
        c.op, "upsert"
    ) as upsert, patch.object(c.op, "checkpoint"):
        c.update({}, {})
    assert upsert.call_count == 1


def test_record_without_publication_date_is_skipped():
    """A null publication_date must not reach the compound-cursor comparison.

    Raised by fivetran-ankitsilla on PR #55. It is not a style point: the cursor
    compares `(publication_date, document_number) <= (last_date, last_document)`,
    and in Python `None <= "2024-01-02"` raises TypeError -- so ONE record with a
    null publication_date aborts the whole sync, not just that record. The
    connector already guards document_number two lines earlier; the omission of
    its sibling was the bug. The value is also checkpointed into state, so a null
    that slipped through would poison the cursor for every later run.
    """
    page = {
        "results": [{"document_number": "2024-99999", "type": "Notice"}, _document()],
        "next_page_url": None,
    }
    with patch.object(c, "get_api_response", return_value=page), patch.object(
        c.op, "upsert"
    ) as upsert, patch.object(c.op, "checkpoint") as checkpoint:
        c.update({}, {})
    # The good record still lands, and the sync does not die on the null one.
    assert upsert.call_count == 1
    assert checkpoint.call_args.kwargs["state"]["last_publication_date"] is not None


def test_state_advances_to_the_last_document_seen():
    page = {
        "results": [
            _document("2024-01-02", "2023-26792"),
            _document("2024-01-05", "2024-00123"),
        ],
        "next_page_url": None,
    }
    with patch.object(c, "get_api_response", return_value=page), patch.object(
        c.op, "upsert"
    ), patch.object(c.op, "checkpoint") as checkpoint:
        c.update({}, {})
    assert checkpoint.call_args.kwargs["state"] == {
        "last_publication_date": "2024-01-05",
        "last_document_number": "2024-00123",
    }


# ------------------------------------------------------------ error handling
def test_client_error_fails_immediately_without_retrying():
    """A 400 means the request is wrong. Retrying only delays the failure."""
    import requests

    response = requests.Response()
    response.status_code = 400
    response._content = b'{"errors":[{"detail":"bad request"}]}'
    with patch.object(c.requests, "get", return_value=response) as get, patch.object(
        c.time, "sleep"
    ) as sleep:
        with pytest.raises(RuntimeError, match="rejected the request"):
            c.get_api_response("https://example.invalid")
    assert get.call_count == 1
    assert sleep.call_count == 0


def test_transient_error_retries_then_succeeds():
    import requests

    ok = requests.Response()
    ok.status_code = 200
    ok._content = b'{"results":[]}'
    with patch.object(
        c.requests,
        "get",
        side_effect=[requests.exceptions.ConnectionError("reset"), ok],
    ) as get, patch.object(c.time, "sleep") as sleep:
        assert c.get_api_response("https://example.invalid") == {"results": []}
    assert get.call_count == 2
    assert sleep.call_count == 1


def test_retries_are_bounded_and_raise():
    import requests

    with patch.object(
        c.requests, "get", side_effect=requests.exceptions.ConnectionError("down")
    ) as get, patch.object(c.time, "sleep"):
        with pytest.raises(RuntimeError, match="after 3 attempts"):
            c.get_api_response("https://example.invalid")
    assert get.call_count == c.__MAX_RETRIES


# The contract needs a realistic record; _document() is defined above.
TestFederalRegisterContract.sample_record = _document()
