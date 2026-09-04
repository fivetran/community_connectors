"""Record shaping for the Semantic Scholar connector.

Pure functions that turn a raw API paper object into the rows the destination
tables expect. No HTTP, no configuration, no state -- given the same record they
always produce the same rows, which is what makes the reserved-word renames,
the nested-object flattening and the null-author rule cheap to test directly.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For serialising the publicationTypes array into a single string column
import json

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log


def flatten_paper(record: dict, enrichment: dict | None = None) -> dict:
    """
    Flatten a raw Semantic Scholar paper record to the papers table shape.

    Renames 'year' -> 'publication_year' and 'abstract' -> 'paper_abstract'.
    Flattens externalIds (object) and openAccessPdf (object|null) to scalar columns.
    Serialises publicationTypes (array|null) to a JSON string.
    Authors are NOT included here -- they go to the paper_authors table.

    Args:
        record: raw paper object from the API
        enrichment: optional dict of cortex enrichment fields

    Returns:
        dict with exactly the columns declared in schema()['papers']
    """
    external_ids = record.get("externalIds") or {}
    pdf = record.get("openAccessPdf") or {}

    row = {
        "paper_id": record.get("paperId"),
        "title": record.get("title"),
        "publication_year": record.get("year"),
        "paper_abstract": record.get("abstract"),
        "reference_count": record.get("referenceCount"),
        "citation_count": record.get("citationCount"),
        "publication_date": record.get("publicationDate"),
        "publication_types": (
            json.dumps(record["publicationTypes"]) if record.get("publicationTypes") else None
        ),
        "external_id_doi": external_ids.get("DOI"),
        "external_id_arxiv": external_ids.get("ArXiv"),
        "external_id_mag": external_ids.get("MAG"),
        "external_id_pubmed": external_ids.get("PubMed"),
        "external_id_dblp": external_ids.get("DBLP"),
        "external_id_acl": external_ids.get("ACL"),
        "external_id_corpus_id": (
            str(external_ids["CorpusId"]) if external_ids.get("CorpusId") is not None else None
        ),
        "open_access_pdf_url": pdf.get("url"),
        "open_access_pdf_status": pdf.get("status"),
        "cortex_research_impact": None,
        "cortex_technical_domain": None,
        "cortex_accessibility_level": None,
        "cortex_model_used": None,
    }

    if enrichment:
        row.update(enrichment)

    # Stamp the primary key AFTER the enrichment merge. Relying on every enrichment
    # key carrying a cortex_ prefix is true today and one rename away from a model
    # response silently overwriting the primary key. Ordering is a guarantee; a
    # naming convention is a hope.
    row["paper_id"] = record.get("paperId")

    return row


def flatten_authors(record: dict) -> list[dict]:
    """
    Extract author rows from a raw paper record for the paper_authors JOIN table.

    Authors whose authorId is null are skipped -- a null primary key column would
    cause a SYNC FAILED at the destination. The Semantic Scholar API returns null
    authorId for authors who do not have a Semantic Scholar profile.

    Args:
        record: raw paper object from the API

    Returns:
        list of dicts, one per author with a non-null authorId
    """
    paper_id = record.get("paperId")
    authors = record.get("authors") or []
    rows = []
    for author in authors:
        author_id = author.get("authorId")
        if author_id is None:
            log.info(
                f"Skipping author with null authorId for paper {paper_id}: "
                f"name={author.get('name')!r}"
            )
            continue
        rows.append(
            {
                "paper_id": paper_id,
                "author_id": author_id,
                "author_name": author.get("name"),
            }
        )
    return rows
